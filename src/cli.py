"""
cli.py —— 命令行入口

    python -m src.cli demo           六步演示（规则摘要，零网络）
    python -m src.cli experiment     对照实验（默认 3 次重复，出表存 runs/）
    python -m src.cli selftest       机制自测（同 selftest.py）
    python -m src.cli demo --llm     演示走 LLM 摘要（需本地模型出口）
"""
import argparse
import shutil
import sys
import tempfile
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from .experiment import run_experiment  # noqa: E402
from .runner import EXPECTED_WRITES, run_scenario  # noqa: E402
from .selftest import main as selftest_main  # noqa: E402


def _fmt_metrics(m: dict) -> str:
    return (
        f"recovered={m['recovered']}  redone={m['redone_steps']}/{m['total_steps']}  "
        f"ext_writes={m['external_writes']}/{m['expected_writes']}  "
        f"dup={m['dup_side_effects']}  intercepted={m['intercepted_dups']}  "
        f"elapsed={m['elapsed_ms']}ms"
    )


def demo(use_llm: bool = False):
    """六步演示：clean → raise 恢复 → crash 恢复 → baseline 对照 → exit 档对照。"""
    model = None
    if use_llm:
        from .config import build_model

        model = build_model()

    tmp = Path(tempfile.mkdtemp(prefix="cr-demo-"))
    try:
        print("checkpoint-recovery 演示\n" + "=" * 60)

        print("\n[1/5] clean：无故障一次跑完（5 步）")
        m1 = run_scenario("enhanced", "demo-clean", None, tmp / "side.db", tmp,
                          use_llm=use_llm, model=model)
        print("      " + _fmt_metrics(m1))

        print("\n[2/5] raise@dedupe_merge：处理错误 → 恢复只重跑失败节点及后续")
        m2 = run_scenario("enhanced", "demo-raise", {"raise": "dedupe_merge"},
                          tmp / "side.db", tmp, use_llm=use_llm, model=model)
        print("      首次失败:", m2["failed_first"], "| " + _fmt_metrics(m2))

        print("\n[3/5] crash@write_report：写成功后硬杀进程 → 新进程续跑，重复写被幂等拦截")
        m3 = run_scenario("enhanced", "demo-crash", {"crash": "write_report"},
                          tmp / "side.db", tmp, use_llm=use_llm, model=model)
        print("      首次崩溃:", m3["failed_first"], "| " + _fmt_metrics(m3))

        print("\n[4/5] baseline + crash：无检查点无幂等 → 整链重跑，报告被发布两次")
        m4 = run_scenario("baseline", "demo-crash-base", {"crash": "write_report"},
                          tmp / "side.db", tmp, use_llm=use_llm, model=model)
        print("      首次崩溃:", m4["failed_first"], "| " + _fmt_metrics(m4))

        print("\n[5/5] durability=exit + crash：中间态全丢 → 恢复只能从头重跑")
        m5 = run_scenario("enhanced", "demo-exit", {"crash": "write_report"},
                          tmp / "side.db", tmp, use_llm=use_llm, model=model,
                          durability="exit")
        print("      首次崩溃:", m5["failed_first"], "| " + _fmt_metrics(m5))

        print("\n" + "=" * 60)
        print("对照结论：")
        print(f"  · 增强模式崩溃恢复：红做 {m3['redone_steps']} 步、重复写 {m3['dup_side_effects']} 次")
        print(f"  · 基线整链重跑：红做 {m4['redone_steps']} 步、重复写 {m4['dup_side_effects']} 次（真实损失）")
        print(f"  · exit 档无快照：红做 {m5['redone_steps']} 步（前功尽弃，但不重复写）")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser(prog="checkpoint-recovery")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p_demo = sub.add_parser("demo", help="六步演示")
    p_demo.add_argument("--llm", action="store_true", help="extract_facts 走 LLM 摘要")
    p_exp = sub.add_parser("experiment", help="对照实验")
    p_exp.add_argument("--repeats", type=int, default=3)
    sub.add_parser("selftest", help="机制自测")
    args = ap.parse_args()

    if args.cmd == "demo":
        demo(use_llm=args.llm)
    elif args.cmd == "experiment":
        run_experiment(repeats=args.repeats)
    elif args.cmd == "selftest":
        sys.exit(selftest_main())


if __name__ == "__main__":
    main()
