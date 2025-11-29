from loguru import logger
from pathlib import Path

# Create logs directory at project root
LOG_PATH = Path(__file__).resolve().parents[1] / "logs"
LOG_PATH.mkdir(exist_ok=True)

logger.add(
    LOG_PATH / "risk_pipeline.log",
    rotation="1 MB",
    retention=5,
    level="INFO",
    enqueue=True,
    backtrace=True,
    diagnose=False,
)

__all__ = ["logger"]
