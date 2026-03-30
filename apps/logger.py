import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from apps.config import settings


def setup_logging():
    """
    Configure application-wide logging.
    Call this once at application startup (in main.py).
    """

    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    logger = logging.getLogger()
    logger.setLevel(log_level)

    if logger.hasHandlers():
        logger.handlers.clear()

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    console_format = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(console_format)

    logger.addHandler(console_handler)

    logger.info("Logging initialized")