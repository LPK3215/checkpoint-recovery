"""
runner.py —— 场景运行器（对照实验与命令行共用）

一个场景 = 独立 thread_id + 独立检查点文件 + 独立 run_id；分批次执行：
- batch 0：注入故障执行（可能崩溃 / 抛异常）
- batch 1：恢复批次——enhanced 用 invoke(None) 从检查点续跑，
  baseline 无检查点，恢复 = 整条任务从头重跑。

指标全部来自 SideEffectLog（node_runs / external_writes / dup_blocks）+ 计时：
- redone_steps    恢复批次实际执行的节点数（「重做步骤数」）
- external_writes 外部系统实际收到的写次数
- dup_side_effects = external_writes - 期望写次数（完整跑完应为 2）
- intercepted_dups 被幂等注册表拦下的重复写次数（dup 的预警信号）
"""
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

from langgraph.checkpoint.sqlite import SqliteSaver

from .config import ROOT, build_model
from .faults import Faults
from .pipeline import TOPIC, BatchCounter, build_graph
from .side_effects import SideEffectLog
from .state import ALL_STEPS, initial_state

EXPECTED_WRITES = 2  # 完整跑完：publish_report + send_notification


def new_run_id() -> str:
    return f"run-{uuid.uuid4().hex[:10]}"


def _cfg(thread_id: str, durability: str = "sync") -> dict:
    return {"configurable": {"thread_id": thread_id, "__pregel_durability": durability}}


def _compile(checkpointer, log, faults_spec, batch, use_llm, model):
    return build_graph(
        log=log,
        faults=Faults(faults_spec),
        counter=BatchCounter(batch),
        use_llm=use_llm,
        model=model,
    ).compile(checkpointer=checkpointer)


def _invoke_inproc(mode, checkpoint_db, log, faults_spec, batch, run_id, thread_id,
                   use_llm, model, durability, on_event=None) -> dict:
    """同进程执行一个批次。enhanced 恢复批次 invoke(None)；baseline 恢复 = 整链重跑。

    on_event 存在时改用 graph.stream(stream_mode="updates") 逐节点发事件，
    最终状态用 get_state 取（语义与 invoke 等价）。
    """
    if mode == "enhanced":
        with SqliteSaver.from_conn_string(str(checkpoint_db)) as saver:
            graph = _compile(saver, log, faults_spec, batch, use_llm, model)
            cfg = _cfg(thread_id, durability)
            inp = None if batch >= 1 else initial_state(TOPIC, run_id)
            return _invoke_with_events(graph, inp, cfg, batch, on_event)
    graph = _compile(None, log, faults_spec, batch, use_llm, model)
    return _invoke_with_events(graph, initial_state(TOPIC, run_id),
                               {"configurable": {"thread_id": thread_id}}, batch, on_event)


def _invoke_with_events(graph, inp, cfg, batch, on_event) -> dict:
    if on_event is None:
        return graph.invoke(inp, cfg)
    for chunk in graph.stream(inp, cfg, stream_mode="updates"):
        for name, _out in chunk.items():
            if name.startswith("__"):
                continue
            on_event("node", {"batch": batch, "node": name})
    return graph.get_state(cfg).values


def _spawn_worker(mode, phase, batch, faults_spec, run_id, thread_id, checkpoint_db,
                  side_db, use_llm, durability) -> dict:
    """跨进程执行一个批次（崩溃场景用）。"""
    env = os.environ.copy()
    env["CR_FAULTS"] = json.dumps(faults_spec or {}, ensure_ascii=False)
    cmd = [
        sys.executable, "-m", "src._worker",
        "--mode", mode,
        "--phase", phase,
        "--thread-id", thread_id,
        "--batch", str(batch),
        "--run-id", run_id,
        "--side-db", str(side_db),
        "--use-llm", "1" if use_llm else "0",
        "--durability", durability,
    ]
    if checkpoint_db:
        cmd += ["--checkpoint-db", str(checkpoint_db)]
    t0 = time.perf_counter()
    p = subprocess.Popen(
        cmd, cwd=str(ROOT), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, encoding="utf-8", errors="replace",
    )
    out, err = p.communicate(timeout=300)
    payload = None
    for line in out.splitlines():
        if line.startswith("WORKER_JSON:"):
            payload = json.loads(line[len("WORKER_JSON:"):])
    return {
        "exit_code": p.returncode,
        "payload": payload,
        "stdout": out,
        "stderr": err,
        "elapsed_ms": int((time.perf_counter() - t0) * 1000),
    }


def run_scenario(mode: str, scenario: str, faults_spec: dict | None,
                 side_db, checkpoint_root, use_llm: bool = False, model=None,
                 durability: str = "sync", on_event=None) -> dict:
    """跑一个场景，返回指标 dict。on_event(kind, data) 为可选事件回调（演示端 SSE 用）。"""
    run_id = new_run_id()
    thread_id = f"th-{run_id}"
    checkpoint_db = Path(checkpoint_root) / f"{mode}-{scenario}.ckpt.db"
    if checkpoint_db.exists():
        checkpoint_db.unlink()  # 干净起点：对照实验之间互不污染

    log = SideEffectLog(side_db, idempotent=(mode == "enhanced"))
    log.hard_reset(run_id)

    t0 = time.perf_counter()
    failed = False
    error = None
    result = None
    resumed_from = None
    if faults_spec and faults_spec.get("crash"):
        # 崩溃场景必须跨进程（同进程 os._exit 会杀死调用方）
        if on_event:
            on_event("batch_start", {"batch": 0, "run_id": run_id, "thread_id": thread_id,
                                      "cross_process": True})
        r1 = _spawn_worker(mode, "run", 0, faults_spec, run_id, thread_id,
                           checkpoint_db if mode == "enhanced" else None,
                           log.db_path, use_llm, durability)
        failed = r1["exit_code"] != 0
        error = None if failed else "crash 未触发"
        if on_event:
            on_event("worker_exit", {"phase": "run", "exit_code": r1["exit_code"],
                                        "payload": r1["payload"]})
        if on_event:
            on_event("batch_start", {"batch": 1, "run_id": run_id, "thread_id": thread_id,
                                      "cross_process": True, "recover": True})
        r2 = _spawn_worker(mode, "resume", 1, {}, run_id, thread_id,
                           checkpoint_db if mode == "enhanced" else None,
                           log.db_path, use_llm, durability)
        if on_event:
            on_event("worker_exit", {"phase": "resume", "exit_code": r2["exit_code"],
                                        "payload": r2["payload"]})
        payload = r2["payload"] or {}
        resumed_from = payload.get("resumed_from")
        result = {"done_steps": payload.get("done_steps", [])}
        if r2["exit_code"] != 0:
            error = f"resume 失败: {r2['stderr'][-400:]}"
    else:
        if on_event:
            on_event("batch_start", {"batch": 0, "run_id": run_id, "thread_id": thread_id})
        try:
            result = _invoke_inproc(mode, checkpoint_db, log, faults_spec, 0,
                                    run_id, thread_id, use_llm, model, durability, on_event)
        except Exception as e:
            failed = True
            error = repr(e)
            result = {"done_steps": []}
        if on_event:
            on_event("batch_end", {"batch": 0, "failed": failed, "error": error})
        if failed:
            t1 = time.perf_counter()
            if on_event:
                on_event("batch_start", {"batch": 1, "run_id": run_id, "thread_id": thread_id,
                                          "recover": True})
            try:
                result = _invoke_inproc(mode, checkpoint_db, log, None, 1,
                                        run_id, thread_id, use_llm, model, durability, on_event)
            except Exception as e:
                error = f"恢复失败: {repr(e)}"
            if on_event:
                on_event("batch_end", {"batch": 1, "error": error})
            elapsed_recover = (time.perf_counter() - t1) * 1000
        else:
            elapsed_recover = 0.0
    elapsed = (time.perf_counter() - t0) * 1000

    st = log.stats(run_id, EXPECTED_WRITES)
    log.close()  # Windows 下不关连接会锁住 side.db，临时目录删不掉
    done = list(result.get("done_steps", []))
    metrics = {
        "mode": mode,
        "scenario": scenario,
        "faults": faults_spec,
        "run_id": run_id,
        "recovered": set(ALL_STEPS) <= set(done) if done else False,
        "done_steps": done,
        "total_steps": len(ALL_STEPS),
        "redone_steps": st["redone_steps"],
        "external_writes": st["external_writes"],
        "expected_writes": st["expected_writes"],
        "dup_side_effects": st["dup_side_effects"],
        "intercepted_dups": st["intercepted_dups"],
        "elapsed_ms": int(elapsed),
        "failed_first": failed,
        "resumed_from": resumed_from,
        "error": error,
    }
    if not failed:
        metrics["recovered"] = True
    return metrics
