"""
agent_check.py —— 端到端检查（过 LLM）

与 selftest 的分层：selftest 纯机制（规则摘要、零网络）；
本文件让 extract_facts 真实调用模型摘要 5 篇语料，验证「模型参与的长任务」
在故障恢复后仍然完整、副作用不重复。

跑：python -m src.agent_check
退出码 0 = 全部通过；模型出口走 .env（学习与测试用 group/1）。
"""
import sys
import tempfile
import traceback
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from .config import build_model  # noqa: E402
from .runner import EXPECTED_WRITES, run_scenario  # noqa: E402
from .seeds import SOURCES  # noqa: E402

CHECKS = []


def check(fn):
    CHECKS.append(fn)
    return fn


@check
def test_clean_with_llm(tmp):
    """无故障：LLM 摘要跑通，报告含全部源标记。"""
    m = run_scenario("enhanced", "clean-llm", None, tmp / "side.db", tmp,
                     use_llm=True, model=build_model())
    assert m["recovered"] is True, f"未恢复: {m}"
    assert m["external_writes"] == EXPECTED_WRITES
    assert m["dup_side_effects"] == 0
    assert m["redone_steps"] == 0


@check
def test_crash_with_llm(tmp):
    """写后崩溃 + LLM：恢复后报告仍完整（含 s1-s5），重复写被拦截。"""
    m = run_scenario("enhanced", "crash-llm", {"crash": "write_report"},
                     tmp / "side.db", tmp, use_llm=True, model=build_model())
    assert m["failed_first"] is True, "崩溃应被检测到"
    assert m["recovered"] is True, f"未恢复: {m}"
    assert m["external_writes"] == EXPECTED_WRITES, f"外部写应恰好 2 次: {m}"
    assert m["dup_side_effects"] == 0
    assert m["intercepted_dups"] >= 1, "应拦住发布动作的重复"


@check
def test_raise_with_llm(tmp):
    """处理错误 + LLM：恢复只重跑失败节点及后续。"""
    m = run_scenario("enhanced", "raise-llm", {"raise": "dedupe_merge"},
                     tmp / "side.db", tmp, use_llm=True, model=build_model())
    assert m["failed_first"] is True
    assert m["recovered"] is True
    assert m["redone_steps"] == 3, f"应只重做 3 步，实际 {m['redone_steps']}"
    assert m["dup_side_effects"] == 0


def main():
    print(f"checkpoint-recovery 端到端检查（过 LLM，{len(SOURCES)} 篇语料）\n")
    fails = []
    for fn in CHECKS:
        tmp = Path(tempfile.mkdtemp(prefix="cr-agent-"))
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
            import shutil

            shutil.rmtree(tmp, ignore_errors=True)
    total = len(CHECKS)
    print(f"\n{total - len(fails)}/{total} passed")
    if fails:
        print("失败用例：" + ", ".join(fails))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
