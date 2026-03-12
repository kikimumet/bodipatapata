import logging
import sys

from apps.config import settings
from apps.logger import setup_logging


def validate_startup():
    """
    Perform basic startup validation checks.
    Fail fast if something is misconfigured.
    """
    required_fields = [
        settings.kafka_bootstrap_servers,
        settings.kafka_topic,
        settings.output_directory,
    ]

    if not all(required_fields):
        raise ValueError("Missing required configuration values")

    logging.getLogger(__name__).info(
        f"Starting application in {settings.environment} mode"
    )


def main():
    try:
        # Initialize logging first
        setup_logging()

        # Validate configuration
        validate_startup()

    except Exception:
        logging.exception("Fatal startup error")
        sys.exit(1)


if __name__ == "__main__":
    main()