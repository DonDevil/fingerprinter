from loguru import logger
import sys

def configure_logging(level: str = "INFO"):
    logger.remove()
    logger.add(
        sink=sys.stdout,
        level=level,
        colorize=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{module}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    )
