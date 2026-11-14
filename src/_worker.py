"""
_worker.py —— subprocess 运行器（跨进程崩溃恢复用）

四种组合：
- enhanced + run:     checkpointer=SqliteSaver, faults=CR_FAULTS, invoke(init)
- enhanced + resume:  checkpointer=SqliteSaver, faults=None,     invoke(None)   从检查点续跑
- baseline + run:     checkpointer=None,       faults=CR_FAULTS, invoke(init)
- baseline + resume:  checkpointer=None,       faults=None,     invoke(init)   整链重跑

crash 注入点在真实外写之后（faults.crash_after_side_effect）：
write_report 发布完成 → os._exit(9)。durability=sync 已把前一步检查点落盘，
恢复批次同 thread_id 新进程 invoke(None) 续跑；重启进程本身不共享内存。

stdout 最后一行 WORKER_JSON:{...}；crash 时进程被杀无该行，父进程按 exit code 9 判定。
"""
import argparse
import json
import sys
import time

from .faults import Faults
from .pipeline import TOPIC, BatchCounter, build_graph
from .side_effects import SideEffectLog
from .state import initial_state


def _parse():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["enhanced", "baseline"], required=True)
    ap.add_argument("--phase", choices=["run", "resume"], required=True)
    ap.add_argument("--thread-id", required=True)
    ap.add_argument("--batch", type=int, required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--checkpoint-db", default="")
    ap.add_argument("--side-db", required=True)
    ap.add_argument("--use-llm", default="0")
    ap.add_argument("--durability", default="sync")
    return ap.parse_args()


def main():
    args = _parse()
    use_llm = args.use_llm == "1"
    model = None
    if use_llm:
        from .config import build_model

        model = build_model()

    # 故障只注入一次：恢复批次环境正常（真实故障是一次性事件）
    faults = Faults.from_env() if args.phase == "run" else Faults(None)
    log = SideEffectLog(args.side_db, idempotent=(args.mode == "enhanced"))
    counter = BatchCounter(args.batch)

    checkpoint_db = args.checkpoint_db if args.mode == "enhanced" and args.checkpoint_db else None

    t0 = time.perf_counter()
    payload = {
        "mode": args.mode,
        "phase": args.phase,
        "run_id": args.run_id,
        "ok": False,
        "done_steps": [],
        "resumed_from": None,
        "error": None,
    }
    try:
        from langgraph.checkpoint.sqlite import SqliteSaver

        if checkpoint_db:
            with SqliteSaver.from_conn_string(checkpoint_db) as saver:
                result, resumed_from = _invoke(saver, args, faults, log, counter, model, use_llm)
        else:
            result, resumed_from = _invoke(None, args, faults, log, counter, model, use_llm)
        payload["ok"] = True
        payload["done_steps"] = list(result.get("done_steps", []))
        payload["resumed_from"] = resumed_from
    except Exception as e:
        payload["error"] = repr(e)
    payload["elapsed_ms"] = int((time.perf_counter() - t0) * 1000)
    log.close()
    print("WORKER_JSON:" + json.dumps(payload, ensure_ascii=False))


def _invoke(checkpointer, args, faults, log, counter, model, use_llm):
    graph = build_graph(
        log=log, faults=faults, counter=counter, use_llm=use_llm, model=model
    ).compile(checkpointer=checkpointer)
    if checkpointer is not None:
        cfg = {
            "configurable": {
                "thread_id": args.thread_id,
                "__pregel_durability": args.durability,
            }
        }
    else:
        cfg = {"configurable": {"thread_id": args.thread_id}}
    if args.phase == "resume" and checkpointer is not None:
        try:
            # 恢复：invoke(None) 从检查点续跑；传 dict 会被当成新输入重跑 START
            return graph.invoke(None, cfg), "checkpoint"
        except Exception:
            # 无检查点可续（durability=exit + 硬杀 = 中间态全丢，只剩从头重跑）
            return graph.invoke(initial_state(TOPIC, args.run_id), cfg), "scratch"
    # 首次运行；baseline 的「恢复」也是整链重跑（无检查点可续）
    return graph.invoke(initial_state(TOPIC, args.run_id), cfg), None


if __name__ == "__main__":
    main()
