"""
experiment.py —— 基线 vs 增强对照实验（D11）

四场景 × 两模式：
- clean：无故障
- raise@dedupe_merge：节点处理错误（可恢复）
- write_fail@notify_stakeholders：外部写超时（可恢复）
- crash@write_report：外部写成功后硬杀进程（最恶劣现场，跨进程）

- baseline：无检查点 + 无幂等（朴素做法）
- enhanced：SqliteSaver 检查点（durability=sync）+ 幂等副作用注册表

四项指标：恢复成功率、重做步骤数、重复副作用次数、恢复耗时。
产出：终端对照表 + runs/<ts>-experiment/{result.json, report.md}。
"""
import json
import sys
import tempfile
from pathlib import Path

from .config import RUNS_DIR, now_str
from .runner import run_scenario

SCENARIOS = [
    ("clean", None),
    ("raise@dedupe_merge", {"raise": "dedupe_merge"}),
    ("write_fail@notify", {"write_fail": "notify_stakeholders"}),
    ("crash@write_report", {"crash": "write_report"}),
]

MODES = ["baseline", "enhanced"]


def run_experiment(repeats: int = 3, out_root: Path | None = None) -> dict:
    if out_root is None:
        out_root = RUNS_DIR / f"{now_str()}-experiment"
    out_root = Path(out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    rows = []
    for mode in MODES:
        for scenario, faults in SCENARIOS:
            for i in range(repeats):
                with tempfile.TemporaryDirectory(prefix="cr-exp-", ignore_cleanup_errors=True) as tmp:
                    metrics = run_scenario(
                        mode, scenario, faults,
                        side_db=Path(tmp) / "side.db",
                        checkpoint_root=Path(tmp),
                    )
                rows.append(metrics)
                print(f"  [{mode:8s}] {scenario:24s} #{i + 1} "
                      f"recovered={metrics['recovered']} redone={metrics['redone_steps']}/{metrics['total_steps']} "
                      f"ext_writes={metrics['external_writes']} dup={metrics['dup_side_effects']} "
                      f"intercepted={metrics['intercepted_dups']} {metrics['elapsed_ms']}ms")

    result = {
        "experiment": "D11 长任务断点恢复对照",
        "repeats": repeats,
        "modes": MODES,
        "scenarios": [{"name": n, "faults": f} for n, f in SCENARIOS],
        "rows": rows,
    }
    with open(out_root / "result.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    report = _render_report(result)
    with open(out_root / "report.md", "w", encoding="utf-8") as f:
        f.write(report)
    print()
    print(report)
    print(f"\n结果已存：{out_root / 'result.json'}")
    return result


def _agg(rows: list, mode: str, scenario: str) -> dict:
    sel = [r for r in rows if r["mode"] == mode and r["scenario"] == scenario]
    if not sel:
        return {}
    n = len(sel)
    return {
        "recovered": sum(1 for r in sel if r["recovered"]) / n,
        "redone_steps": sum(r["redone_steps"] for r in sel) / n,
        "external_writes": sum(r["external_writes"] for r in sel) / n,
        "dup_side_effects": sum(r["dup_side_effects"] for r in sel) / n,
        "intercepted_dups": sum(r["intercepted_dups"] for r in sel) / n,
        "elapsed_ms": sum(r["elapsed_ms"] for r in sel) / n,
    }


def _render_report(result: dict) -> str:
    rows = result["rows"]
    lines = [
        "# D11 断点恢复对照实验",
        "",
        f"重复次数：{result['repeats']}　｜　enhanced = SqliteSaver(durability=sync) + 幂等副作用注册表；baseline = 无检查点 + 无幂等",
        "",
        "| 模式 | 场景 | 恢复成功率 | 重做步骤(均) | 外部写(均) | 重复副作用(均) | 幂等拦截(均) | 耗时ms(均) |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for mode in result["modes"]:
        for scen in [s["name"] for s in result["scenarios"]]:
            a = _agg(rows, mode, scen)
            lines.append(
                f"| {mode} | {scen} | {a['recovered']:.0%} | {a['redone_steps']:.1f} | "
                f"{a['external_writes']:.1f} | {a['dup_side_effects']:.1f} | "
                f"{a['intercepted_dups']:.1f} | {a['elapsed_ms']:.0f} |"
            )
    lines += ["", "## 读表要点", ""]
    lines += [
        "- 重做步骤：baseline 恢复 = 整链重跑（恒 5）；enhanced 只重跑失败节点及其后续。",
        "- 重复副作用：crash@write_report 场景 baseline 必为 1（报告被发布两次）；"
        "enhanced 靠幂等拦截为 0。",
        "- 幂等拦截：enhanced 在 crash 场景的 intercepted_dups 应 ≥ 1——没有它 dup 就是真实损失。",
        "- 耗时：enhanced 每步写检查点，clean 场景会略慢；崩溃/超时场景因为少重做而更快。",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--repeats", type=int, default=3)
    args = ap.parse_args()
    run_experiment(repeats=args.repeats)
