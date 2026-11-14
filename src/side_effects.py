"""
side_effects.py —— 幂等副作用注册表与外部系统写日志

真实外写（发布报告、发通知）统一走 SideEffectLog：
- try_record(run_id, key, ...)：INSERT OR IGNORE；键已存在 → 返回 False，调用方跳过真实写。
  这一条是「重复副作用次数 = 0」的全部来源。
- external_writes 表记外部系统实际收到的每一次写，用于算 dup_side_effects
  （外部写次数 - 期望写次数）。重复写是真实损失，拦截记录只是证据。
- node_runs 表记每一步每次执行（含恢复批次 batch），用于「重做步骤数」指标。

idempotent=False 时退化为朴素模式（总是执行、不拦截），给基线对照用。
"""
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

CST = timezone(timedelta(hours=8))

_SCHEMA = """
CREATE TABLE IF NOT EXISTS side_effects (
    run_id TEXT NOT NULL,
    key TEXT NOT NULL,
    node TEXT NOT NULL,
    op TEXT NOT NULL,
    args TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, key)
);
CREATE TABLE IF NOT EXISTS dup_blocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    key TEXT NOT NULL,
    node TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS external_writes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    node TEXT NOT NULL,
    op TEXT NOT NULL,
    args TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS node_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    batch INTEGER NOT NULL,
    node TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


def _ts() -> str:
    return datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")


class SideEffectLog:
    def __init__(self, db_path, idempotent: bool = True):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.idempotent = idempotent
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(_SCHEMA)
        self.conn.commit()

    # ---------- 幂等注册表 ----------

    def try_record(self, run_id: str, key: str, node: str, op: str, args=None) -> bool:
        """尝试登记副作用；返回 True=新登记（应执行真实写），False=已存在（拦截）。"""
        args_json = json.dumps(args, ensure_ascii=False, sort_keys=True) if args is not None else None
        if not self.idempotent:
            return True
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO side_effects(run_id, key, node, op, args, created_at) VALUES(?,?,?,?,?,?)",
            (run_id, key, node, op, args_json, _ts()),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def mark_dup(self, run_id: str, key: str, node: str):
        self.conn.execute(
            "INSERT INTO dup_blocks(run_id, key, node, created_at) VALUES(?,?,?,?)",
            (run_id, key, node, _ts()),
        )
        self.conn.commit()

    # ---------- 外部系统写日志 ----------

    def record_external_write(self, run_id: str, node: str, op: str, args=None):
        args_json = json.dumps(args, ensure_ascii=False, sort_keys=True) if args is not None else None
        self.conn.execute(
            "INSERT INTO external_writes(run_id, node, op, args, created_at) VALUES(?,?,?,?,?)",
            (run_id, node, op, args_json, _ts()),
        )
        self.conn.commit()

    # ---------- 节点执行日志 ----------

    def record_node(self, run_id: str, batch: int, node: str, status: str = "start"):
        self.conn.execute(
            "INSERT INTO node_runs(run_id, batch, node, status, created_at) VALUES(?,?,?,?,?)",
            (run_id, batch, node, status, _ts()),
        )
        self.conn.commit()

    # ---------- 查询与统计 ----------

    def stats(self, run_id: str, expected_writes: int = 2) -> dict:
        cur = self.conn.execute(
            "SELECT COUNT(*) FROM external_writes WHERE run_id=?", (run_id,)
        )
        external_writes = cur.fetchone()[0]
        cur = self.conn.execute("SELECT COUNT(*) FROM dup_blocks WHERE run_id=?", (run_id,))
        intercepted = cur.fetchone()[0]
        cur = self.conn.execute(
            "SELECT batch, COUNT(*) FROM node_runs WHERE run_id=? GROUP BY batch ORDER BY batch",
            (run_id,),
        )
        by_batch = {int(b): int(n) for b, n in cur.fetchall()}
        redone = sum(n for b, n in by_batch.items() if b >= 1)
        return {
            "external_writes": external_writes,
            "expected_writes": expected_writes,
            "dup_side_effects": max(0, external_writes - expected_writes),
            "intercepted_dups": intercepted,
            "node_runs_by_batch": by_batch,
            "redone_steps": redone,
        }

    def reset(self, run_id: str):
        """清掉一个 run 的三张表数据（重复实验前用）。node_runs 保留。"""
        for t in ("side_effects", "dup_blocks", "external_writes"):
            self.conn.execute(f"DELETE FROM {t} WHERE run_id=?", (run_id,))
        self.conn.commit()

    def hard_reset(self, run_id: str):
        for t in ("side_effects", "dup_blocks", "external_writes", "node_runs"):
            self.conn.execute(f"DELETE FROM {t} WHERE run_id=?", (run_id,))
        self.conn.commit()

    def close(self):
        self.conn.close()
