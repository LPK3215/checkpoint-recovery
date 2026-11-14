"""
faults.py —— 故障注入

spec 形如：{"crash": "write_report", "raise": "dedupe_merge", "write_fail": "notify_stakeholders"}
三种注入对应总纲 11.1 的故障注入四个点中最主要的三种：

- crash：节点内、真实外写执行之后 os._exit(9)。模拟「写成功了但进程挂了」——
  这是最恶劣的现场：外部已收写、本地无完成记录，恢复重跑必须靠幂等拦住重复写。
  只能在 subprocess 里用（同进程会杀死调用方）。
- raise：节点开头抛 RuntimeError，模拟处理错误（可被上层捕获、走恢复路径）。
- write_fail：节点内、外部写之前抛 TimeoutError，模拟外写超时（下游抖动）。

spec 经环境变量 CR_FAULTS（JSON）传给 subprocess worker，保证父子进程注入一致。
"""
import json
import os


class Faults:
    def __init__(self, spec: dict | None = None):
        spec = spec or {}
        self.crash = spec.get("crash")
        self.raise_node = spec.get("raise")
        self.write_fail = spec.get("write_fail")

    @classmethod
    def from_env(cls):
        raw = os.environ.get("CR_FAULTS", "")
        return cls(json.loads(raw) if raw.strip() else None)

    def as_json(self) -> str:
        return json.dumps(
            {"crash": self.crash, "raise": self.raise_node, "write_fail": self.write_fail},
            ensure_ascii=False,
        )

    def raise_at(self, node: str):
        """节点开头注入：处理错误。"""
        if self.raise_node == node:
            raise RuntimeError(f"注入故障：{node} 节点处理失败")

    def write_fail_at(self, node: str):
        """外部写之前注入：写超时。"""
        if self.write_fail == node:
            raise TimeoutError(f"注入故障：{node} 外部写超时")

    def crash_after_side_effect(self, node: str):
        """真实外写之后注入：硬杀进程。"""
        if self.crash == node:
            # 硬杀：不写文件、不 flush，模拟 SIGKILL；检查点依赖 durability=sync 已落盘。
            os._exit(9)
