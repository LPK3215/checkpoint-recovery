"""
config.py —— 路径、环境变量与模型装配

模型走 OpenAI 兼容接口（本机喜鹊网关），换模型只改 .env。
"""
import io
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
RUNS_DIR = ROOT / "runs"

CST = timezone(timedelta(hours=8))


def now_str() -> str:
    """Asia/Shanghai 时间戳（秒级），用于运行目录与记录。"""
    return datetime.now(CST).strftime("%Y%m%d-%H%M%S")


def load_env(path=None):
    """从项目 .env 读配置；已存在的环境变量不覆盖。"""
    if path is None:
        path = ROOT / ".env"
    path = Path(path)
    if not path.exists():
        return None
    with io.open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())
    return path


def build_model(model=None, temperature=0.2):
    from langchain_openai import ChatOpenAI

    load_env()
    return ChatOpenAI(
        model=model or os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
        api_key=os.environ.get("OPENAI_API_KEY", ""),
        base_url=os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1"),
        temperature=temperature,
    )
