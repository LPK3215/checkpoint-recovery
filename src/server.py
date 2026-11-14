"""
server.py —— FastAPI + SSE 断点恢复观测台

    python -m src.server     # http://127.0.0.1:8622

接口：
- POST /api/run     body {"mode": "enhanced"|"baseline", "scenario": "clean|raise|write_fail|crash", "llm": bool}
                   SSE 事件流：batch_start / node / batch_end / worker_exit / metrics / error
- GET  /api/state   当前运行的状态：node_runs（每批每步）、external_writes、dup_blocks、检查点数
- GET  /            单页前端
"""
import json
import queue
import threading
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from .config import DATA_DIR, ROOT, build_model
from .runner import run_scenario
from .side_effects import SideEffectLog

SCENARIOS = {
    "clean": None,
    "raise": {"raise": "dedupe_merge"},
    "write_fail": {"write_fail": "notify_stakeholders"},
    "crash": {"crash": "write_report"},
}

app = FastAPI(title="checkpoint-recovery 观测台")
_lock = threading.Lock()
_current = {"run_id": None, "side_db": None, "checkpoint_db": None,
            "mode": None, "scenario": None, "metrics": None}


class RunBody(BaseModel):
    mode: str = "enhanced"
    scenario: str = "clean"
    llm: bool = False


@app.get("/")
def index():
    return FileResponse(str(ROOT / "static" / "index.html"))


@app.post("/api/run")
def run(body: RunBody):
    side_db = DATA_DIR / "server-side.db"
    q: queue.Queue = queue.Queue()
    run_ref = {}

    def on_event(kind: str, data: dict):
        q.put({"kind": kind, "data": data})

    def worker():
        try:
            model = build_model() if body.llm else None
            metrics = run_scenario(
                body.mode, body.scenario, SCENARIOS.get(body.scenario),
                side_db=side_db, checkpoint_root=DATA_DIR,
                use_llm=body.llm, model=model, on_event=on_event,
            )
            run_ref["run_id"] = metrics["run_id"]
            with _lock:
                _current.update({
                    "run_id": metrics["run_id"],
                    "side_db": str(side_db),
                    "checkpoint_db": str(DATA_DIR / f"{body.mode}-{body.scenario}.ckpt.db"),
                    "mode": body.mode,
                    "scenario": body.scenario,
                    "metrics": metrics,
                })
            q.put({"kind": "metrics", "data": metrics})
        except Exception as e:
            q.put({"kind": "error", "data": {"error": repr(e)}})
        finally:
            q.put(None)

    threading.Thread(target=worker, daemon=True).start()

    def gen():
        yield f"event: started\ndata: {json.dumps({'mode': body.mode, 'scenario': body.scenario, 'llm': body.llm}, ensure_ascii=False)}\n\n"
        while True:
            item = q.get()
            if item is None:
                break
            yield f"event: {item['kind']}\ndata: {json.dumps(item['data'], ensure_ascii=False)}\n\n"
        yield "event: done\ndata: {}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/api/state")
def state():
    with _lock:
        run_id = _current.get("run_id")
        side_db = _current.get("side_db")
        checkpoint_db = _current.get("checkpoint_db")
        metrics = _current.get("metrics")
    if not run_id:
        return {"run_id": None, "node_runs": [], "external_writes": [], "dup_blocks": [],
                "checkpoints": None, "metrics": None}
    log = SideEffectLog(side_db, idempotent=True)
    node_runs = [
        {"batch": r[0], "node": r[1], "status": r[2], "at": r[3]}
        for r in log.conn.execute(
            "SELECT batch, node, status, created_at FROM node_runs WHERE run_id=? ORDER BY id",
            (run_id,),
        )
    ]
    external_writes = [
        {"node": r[0], "op": r[1], "at": r[2]}
        for r in log.conn.execute(
            "SELECT node, op, created_at FROM external_writes WHERE run_id=? ORDER BY id",
            (run_id,),
        )
    ]
    dup_blocks = [
        {"node": r[0], "at": r[1]}
        for r in log.conn.execute(
            "SELECT node, created_at FROM dup_blocks WHERE run_id=? ORDER BY id", (run_id,)
        )
    ]
    log.close()
    ckpt = None
    if checkpoint_db and Path(checkpoint_db).exists():
        from langgraph.checkpoint.sqlite import SqliteSaver

        with SqliteSaver.from_conn_string(checkpoint_db) as saver:
            ckpt = len(list(saver.list({"configurable": {"thread_id": "th-" + run_id}})))
    return {"run_id": run_id, "node_runs": node_runs, "external_writes": external_writes,
            "dup_blocks": dup_blocks, "checkpoints": ckpt, "metrics": metrics}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8622, log_level="warning")
