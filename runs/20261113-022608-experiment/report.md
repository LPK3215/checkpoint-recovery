# D11 断点恢复对照实验

重复次数：3　｜　enhanced = SqliteSaver(durability=sync) + 幂等副作用注册表；baseline = 无检查点 + 无幂等

| 模式 | 场景 | 恢复成功率 | 重做步骤(均) | 外部写(均) | 重复副作用(均) | 幂等拦截(均) | 耗时ms(均) |
|---|---|---|---|---|---|---|---|
| baseline | clean | 100% | 0.0 | 2.0 | 0.0 | 0.0 | 11 |
| baseline | raise@dedupe_merge | 100% | 5.0 | 2.0 | 0.0 | 0.0 | 15 |
| baseline | write_fail@notify | 100% | 5.0 | 3.0 | 1.0 | 0.0 | 16 |
| baseline | crash@write_report | 100% | 5.0 | 3.0 | 1.0 | 0.0 | 4144 |
| enhanced | clean | 100% | 0.0 | 2.0 | 0.0 | 0.0 | 34 |
| enhanced | raise@dedupe_merge | 100% | 3.0 | 2.0 | 0.0 | 0.0 | 50 |
| enhanced | write_fail@notify | 100% | 1.0 | 2.0 | 0.0 | 0.0 | 43 |
| enhanced | crash@write_report | 100% | 2.0 | 2.0 | 0.0 | 1.0 | 3891 |

## 读表要点

- 重做步骤：baseline 恢复 = 整链重跑（恒 5）；enhanced 只重跑失败节点及其后续。
- 重复副作用：crash@write_report 场景 baseline 必为 1（报告被发布两次）；enhanced 靠幂等拦截为 0。
- 幂等拦截：enhanced 在 crash 场景的 intercepted_dups 应 ≥ 1——没有它 dup 就是真实损失。
- 耗时：enhanced 每步写检查点，clean 场景会略慢；崩溃/超时场景因为少重做而更快。