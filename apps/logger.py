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

    # Root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Prevent duplicate handlers (important if reloaded)
    if logger.hasHandlers():
        logger.handlers.clear()

    # ===== Console Handler (recommended for systemd) =====
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    console_format = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(console_format)

    logger.addHandler(console_handler)

    # ===== Optional File Logging =====
    # Uncomment if you want file logs in addition to journalctl
    """
    log_dir = Path(settings.output_directory)
    log_dir.mkdir(parents=True, exist_ok=True)

    file_handler = RotatingFileHandler(
        log_dir / "app.log",
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(console_format)

    logger.addHandler(file_handler)
    """

    logger.info("Logging initialized")