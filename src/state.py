"""
state.py —— 长任务状态定义

一个 run = 一份调研报告生成任务（fetch → extract → dedupe → write → notify）。
run_id 贯穿全批次（含恢复），是幂等键与副作用统计的聚合主键。
执行批次（0=首次、1=恢复）由 pipeline.BatchCounter 闭包维护，不进 state：
state 是检查点内容，批次是运行期信息，混进去会把恢复语义弄乱。
"""
from typing import TypedDict


class ReportState(TypedDict, total=False):
    topic: str
    sources: list
    facts: list
    report: str
    done_steps: list
    error: str
    run_id: str


def initial_state(topic: str, run_id: str) -> ReportState:
    return ReportState(
        topic=topic,
        sources=[],
        facts=[],
        report="",
        done_steps=[],
        error=None,
        run_id=run_id,
    )


ALL_STEPS = ["fetch_sources", "extract_facts", "dedupe_merge", "write_report", "notify_stakeholders"]
