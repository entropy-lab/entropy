import sys
import logging
from logging import StreamHandler, Formatter, INFO, WARN, ERROR

logger = logging.getLogger("entropy")
logger.setLevel(INFO)
_handler = StreamHandler(sys.stdout)
_handler.setFormatter(Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(_handler)

__all__ = ["INFO", "WARN", "ERROR", "logger"]
