"""
selftest.py —— 机制自测：检查点持久化 -> 故障注入 -> 恢复 -> 幂等断言

不依赖 LLM（extract_facts 走规则摘要），纯机制层。全部跑：
    python -m src.selftest

11 个用例，每个用例独立临时库（SQLite + 检查点），互不干扰：
 1. test_pipeline_clean         无故障全流程：5 步、外部写 2、dup=0、redone=0
 2. test_side_effect_idempotent  幂等注册表：同键重复记录被拦截并留痕
 3. test_side_effect_naive_mode  朴素模式（无幂等）：同键放行，baseline 语义
 4. test_resume_after_raise      同进程异常恢复：只重跑失败节点及后续（3 步）
 5. test_baseline_redo_all       基线对照：失败后整链重跑（5 步全做）
 6. test_crash_resume_enhanced   增强 + 写后崩溃：外部写不重复（dup=0），幂等拦截 ≥1
 7. test_crash_baseline_dup      基线 + 写后崩溃：外部写重复（dup=1，报告发布两次）
 8. test_durability_exit_loses   exit 档崩溃后检查点全无，恢复重做整链
 9. test_thread_id_isolation     检查点主键是 thread_id：新线程从零开始
10. test_worker_protocol        跨进程 worker 协议：crash 时 exit 9 无 JSON；resume 回 JSON
11. test_metrics_fields         指标字段齐全（runner 汇总完整性）
"""
import json
import shutil
import sys
import tempfile
import traceback
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from langgraph.checkpoint.sqlite import SqliteSaver

from .faults import Faults  # noqa: E402
from .pipeline import TOPIC, BatchCounter, build_graph  # noqa: E402
from .runner import EXPECTED_WRITES, _spawn_worker, run_scenario, new_run_id  # noqa: E402
from .side_effects import SideEffectLog  # noqa: E402
from .state import ALL_STEPS, initial_state  # noqa: E402

CASES = []


def case(fn):
    CASES.append(fn)
    return fn


@case
def test_pipeline_clean(tmp):
    """无故障：一次跑完，副作用恰好 2 次。"""
    m = run_scenario("enhanced", "clean", None, tmp / "side.db", tmp)
    assert m["recovered"] is True, f"未恢复: {m}"
    assert m["done_steps"] == ALL_STEPS, f"步骤不全: {m['done_steps']}"
    assert m["external_writes"] == EXPECTED_WRITES
    assert m["dup_side_effects"] == 0
    assert m["redone_steps"] == 0
    assert m["intercepted_dups"] == 0


@case
def test_side_effect_idempotent(tmp):
    """幂等注册表：同键第二次被拦截。"""
    log = SideEffectLog(tmp / "side.db", idempotent=True)
    ok1 = log.try_record("r1", "write_report:r1", "write_report", "publish_report", {"topic": "t"})
    ok2 = log.try_record("r1", "write_report:r1", "write_report", "publish_report", {"topic": "t"})
    assert ok1 is True and ok2 is False, "同键重复写未被拦截"
    log.mark_dup("r1", "write_report:r1", "write_report")
    st = log.stats("r1")
    assert st["intercepted_dups"] == 1
    log.close()


@case
def test_side_effect_naive_mode(tmp):
    """朴素模式：无幂等拦截（baseline 语义）。"""
    log = SideEffectLog(tmp / "side.db", idempotent=False)
    ok1 = log.try_record("r1", "k", "write_report", "publish_report")
    ok2 = log.try_record("r1", "k", "write_report", "publish_report")
    assert ok1 is True and ok2 is True, "朴素模式不应拦截"
    assert log.stats("r1")["dup_side_effects"] == 0  # 计数不含拦截
    log.close()


@case
def test_resume_after_raise(tmp):
    """同进程异常恢复：只重跑失败节点及其后续（3 步），不是整链。"""
    m = run_scenario("enhanced", "raise", {"raise": "dedupe_merge"}, tmp / "side.db", tmp)
    assert m["failed_first"] is True, "首次运行应失败"
    assert m["recovered"] is True
    assert m["redone_steps"] == 3, f"应只重做 dedupe_merge 及其后续 3 步，实际 {m['redone_steps']}"
    assert m["external_writes"] == EXPECTED_WRITES
    assert m["dup_side_effects"] == 0


@case
def test_baseline_redo_all(tmp):
    """基线：无检查点，失败后整链重跑（5 步全做）。"""
    m = run_scenario("baseline", "raise", {"raise": "dedupe_merge"}, tmp / "side.db", tmp)
    assert m["failed_first"] is True
    assert m["recovered"] is True
    assert m["redone_steps"] == len(ALL_STEPS), f"baseline 应整链重做，实际 {m['redone_steps']}"
    assert m["external_writes"] == EXPECTED_WRITES  # 首次没写到；重跑写完


@case
def test_crash_resume_enhanced(tmp):
    """增强 + 写后崩溃：报告只发布一次，重复被幂等拦截。"""
    m = run_scenario("enhanced", "crash", {"crash": "write_report"}, tmp / "side.db", tmp)
    assert m["failed_first"] is True, "崩溃应被检测到"
    assert m["recovered"] is True
    assert m["external_writes"] == EXPECTED_WRITES, f"外部写应恰好 2 次，实际 {m['external_writes']}"
    assert m["dup_side_effects"] == 0, "重复副作用必须为 0"
    assert m["intercepted_dups"] >= 1, f"应拦截住重复写，实际 {m['intercepted_dups']}"


@case
def test_crash_baseline_dup(tmp):
    """基线 + 写后崩溃：报告被发布两次（真实损失），dup=1。"""
    m = run_scenario("baseline", "crash", {"crash": "write_report"}, tmp / "side.db", tmp)
    assert m["failed_first"] is True
    assert m["recovered"] is True
    assert m["external_writes"] == 3, f"baseline 整链重跑后外部写 3 次，实际 {m['external_writes']}"
    assert m["dup_side_effects"] == 1, "报告发布两次，重复副作用 = 1"
    assert m["redone_steps"] == len(ALL_STEPS)


@case
def test_durability_exit_loses(tmp):
    """durability=exit：崩溃后只剩 START 快照，恢复整链重做。"""
    m = run_scenario("enhanced", "crash-exit", {"crash": "write_report"},
                     tmp / "side.db", tmp, durability="exit")
    assert m["recovered"] is True
    assert m["redone_steps"] == len(ALL_STEPS), f"exit 档无中间快照，应整链重做，实际 {m['redone_steps']}"
    # 对照：同场景 sync 档只重做 2 步
    m2 = run_scenario("enhanced", "crash-sync", {"crash": "write_report"}, tmp / "side.db", tmp)
    assert m2["redone_steps"] < len(ALL_STEPS), f"sync 档应少重做，实际 {m2['redone_steps']}"


@case
def test_thread_id_isolation(tmp):
    """检查点主键是 thread_id：新线程从零开始，不串旧状态。"""
    import os
    os.environ["CR_FAULTS"] = json.dumps({"crash": "write_report"})
    try:
        log = SideEffectLog(tmp / "side.db", idempotent=True)
        r1 = _spawn_worker("enhanced", "run", 0, {"crash": "write_report"}, "run-th", "th-A",
                           tmp / "a.ckpt.db", tmp / "side.db", False, "sync")
        assert r1["exit_code"] == 9, f"worker1 应以 9 退出，实际 {r1['exit_code']}"
        log.hard_reset("run-th")

        # 新 thread：从零跑通，不受 th-A 检查点影响
        r2 = _spawn_worker("enhanced", "run", 0, {}, "run-th", "th-B",
                           tmp / "b.ckpt.db", tmp / "side.db", False, "sync")
        assert r2["exit_code"] == 0, f"新线程应一次跑通: {r2['stderr'][-300:]}"
        payload = r2["payload"]
        assert payload["ok"] is True
        assert payload["done_steps"] == ALL_STEPS, f"新线程步骤不全: {payload['done_steps']}"
        log.close()
    finally:
        os.environ.pop("CR_FAULTS", None)


@case
def test_worker_protocol(tmp):
    """跨进程 worker 协议：crash 无 JSON（exit 9）；resume 有 JSON。"""
    log = SideEffectLog(tmp / "side.db", idempotent=True)
    r1 = _spawn_worker("enhanced", "run", 0, {"crash": "write_report"}, "run-w", "th-W",
                       tmp / "w.ckpt.db", tmp / "side.db", False, "sync")
    assert r1["exit_code"] == 9, f"crash worker 应被硬杀，实际 {r1['exit_code']}"
    assert r1["payload"] is None, "crash 时不应有 WORKER_JSON"
    r2 = _spawn_worker("enhanced", "resume", 1, {}, "run-w", "th-W",
                       tmp / "w.ckpt.db", tmp / "side.db", False, "sync")
    assert r2["exit_code"] == 0, f"resume 应成功: {r2['stderr'][-300:]}"
    assert r2["payload"] is not None and r2["payload"]["ok"] is True
    assert r2["payload"]["done_steps"] == ALL_STEPS
    log.close()


@case
def test_metrics_fields(tmp):
    """runner 指标字段齐全，语义正确。"""
    required = {"mode", "scenario", "recovered", "done_steps", "total_steps",
                "redone_steps", "external_writes", "expected_writes",
                "dup_side_effects", "intercepted_dups", "elapsed_ms",
                "failed_first", "error"}
    m = run_scenario("enhanced", "clean", None, tmp / "side.db", tmp)
    missing = required - set(m)
    assert not missing, f"指标缺字段: {missing}"
    assert m["expected_writes"] == EXPECTED_WRITES
    # 顺带验证检查点快照真实落盘
    ckpt = tmp / "enhanced-clean.ckpt.db"
    assert ckpt.exists(), "检查点文件未生成"
    with SqliteSaver.from_conn_string(str(ckpt)) as saver:
        snaps = list(saver.list({"configurable": {"thread_id": "th-" + m["run_id"]}}))
    assert len(snaps) >= len(ALL_STEPS), f"检查点份数异常: {len(snaps)}"


def main():
    print("checkpoint-recovery 机制自测（临时库，不写 data/）\n")
    fails = []
    for fn in CASES:
        tmp = Path(tempfile.mkdtemp(prefix="cr-test-"))
        try:
            fn(tmp)
            print(f"PASS  {fn.__name__}")
        except AssertionError as e:
            fails.append(fn.__name__)
            print(f"FAIL  {fn.__name__}: {e}")
        except Exception as e:
            fails.append(fn.__name__)
            print(f"ERROR {fn.__name__}: {type(e).__name__}: {e}")
            traceback.print_exc()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    total = len(CASES)
    print(f"\n{total - len(fails)}/{total} passed")
    if fails:
        print("失败用例：" + ", ".join(fails))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
