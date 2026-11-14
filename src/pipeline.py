"""
pipeline.py —— 长任务图（LangGraph StateGraph）

五步流水线：fetch_sources → extract_facts → dedupe_merge → write_report → notify_stakeholders

两条链路口径：
- write_report / notify_stakeholders 是真实外部副作用（发布报告、发通知），
  统一走 SideEffectLog 幂等注册表：键已存在则跳过真实写。
- 故障注入由 Faults 闭包传入；crash 注入点在真实外写之后，
  模拟「写成功但进程挂了」——最恶劣的现场，恢复重跑必须靠幂等拦住重复写。

批次计数 BatchCounter 是进程级闭包：恢复批次新建图时传 start=1，
节点记录 node_runs 时读当前批次；不放进 state（state 是检查点内容）。
"""
from langgraph.graph import StateGraph, START, END

from .faults import Faults
from .seeds import load_sources
from .side_effects import SideEffectLog
from .state import ReportState

TOPIC = "流程控制与可靠性 · 机制调研"


class BatchCounter:
    """执行批次计数：0=首次运行，1=第一次恢复（worker 传 --batch 决定起点）。"""

    def __init__(self, start: int = 0):
        self.n = start

    def current(self) -> int:
        return self.n


def _external_write(log: SideEffectLog, run_id: str, node: str, op: str, args: dict) -> bool:
    """真实外写统一入口：先过幂等注册表，再记外部系统写日志。

    返回 True=执行了真实写；False=被幂等拦截（跳过真实写）。
    """
    key = f"{node}:{run_id}"
    if log.try_record(run_id, key, node, op, args):
        log.record_external_write(run_id, node, op, args)
        return True
    log.mark_dup(run_id, key, node)
    return False


def _mark(state: ReportState, name: str) -> dict:
    return {"done_steps": [*state.get("done_steps", []), name]}


def summarise_prompt(source: dict) -> str:
    return (
        "用一句话概括以下材料的工程要点（只讲机制，不要举例子）：\n"
        f"标题：{source['title']}\n内容：{source['text']}"
    )


def rule_summary(source: dict) -> str:
    """规则摘要：取首句。selftest 与离线场景用，保证零网络可跑。"""
    text = source["text"].strip()
    first = text.split("。")[0].strip()
    return first + "。" if not first.endswith("。") else first


def build_report(topic: str, facts: list) -> str:
    lines = [f"# 调研报告：{topic}", "", "## 关键工程要点"]
    for f in facts:
        lines.append(f"- [{f['source_id']}] {f['summary']}")
    lines += ["", "## 结论", "检查点负责「从哪继续」，幂等负责「不重复写」；二者缺一不可。"]
    return "\n".join(lines)


def build_graph(
    *,
    log: SideEffectLog,
    faults: Faults,
    counter: BatchCounter,
    use_llm: bool = False,
    model=None,
):
    """构建未编译的图。编译由调用方决定 checkpointer（含 durability config）。"""

    def fetch_sources(state: ReportState):
        run_id = state["run_id"]
        log.record_node(run_id, counter.current(), "fetch_sources")
        faults.raise_at("fetch_sources")
        return {
            **_mark(state, "fetch_sources"),
            "sources": load_sources(state.get("topic", TOPIC)),
        }

    def extract_facts(state: ReportState):
        run_id = state["run_id"]
        log.record_node(run_id, counter.current(), "extract_facts")
        faults.raise_at("extract_facts")
        sources = state.get("sources", [])
        if use_llm and model is not None:
            facts = []
            for s in sources:
                resp = model.invoke([{"role": "user", "content": summarise_prompt(s)}])
                facts.append({"source_id": s["id"], "summary": resp.content.strip()})
        else:
            facts = [{"source_id": s["id"], "summary": rule_summary(s)} for s in sources]
        return {**_mark(state, "extract_facts"), "facts": facts}

    def dedupe_merge(state: ReportState):
        run_id = state["run_id"]
        log.record_node(run_id, counter.current(), "dedupe_merge")
        faults.raise_at("dedupe_merge")
        merged = {}
        for f in state.get("facts", []):
            merged.setdefault(f["source_id"], f["summary"])
        facts = [{"source_id": k, "summary": v} for k, v in merged.items()]
        return {**_mark(state, "dedupe_merge"), "facts": facts}

    def write_report(state: ReportState):
        run_id = state["run_id"]
        log.record_node(run_id, counter.current(), "write_report")
        faults.raise_at("write_report")
        faults.write_fail_at("write_report")
        topic = state.get("topic", TOPIC)
        facts = state.get("facts", [])
        report = build_report(topic, facts)
        _external_write(log, run_id, "write_report", "publish_report",
                        {"topic": topic, "sections": len(facts)})
        faults.crash_after_side_effect("write_report")
        return {**_mark(state, "write_report"), "report": report}

    def notify_stakeholders(state: ReportState):
        run_id = state["run_id"]
        log.record_node(run_id, counter.current(), "notify_stakeholders")
        faults.raise_at("notify_stakeholders")
        faults.write_fail_at("notify_stakeholders")
        _external_write(log, run_id, "notify_stakeholders", "send_notification",
                        {"topic": state.get("topic", TOPIC), "channel": "email"})
        faults.crash_after_side_effect("notify_stakeholders")
        return _mark(state, "notify_stakeholders")

    builder = StateGraph(ReportState)
    builder.add_node("fetch_sources", fetch_sources)
    builder.add_node("extract_facts", extract_facts)
    builder.add_node("dedupe_merge", dedupe_merge)
    builder.add_node("write_report", write_report)
    builder.add_node("notify_stakeholders", notify_stakeholders)
    builder.add_edge(START, "fetch_sources")
    builder.add_edge("fetch_sources", "extract_facts")
    builder.add_edge("extract_facts", "dedupe_merge")
    builder.add_edge("dedupe_merge", "write_report")
    builder.add_edge("write_report", "notify_stakeholders")
    builder.add_edge("notify_stakeholders", END)
    return builder
