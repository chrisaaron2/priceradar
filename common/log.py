"""Consistent logging setup for scripts and Airflow tasks."""

import logging
import os

_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def get_logger(name: str) -> logging.Logger:
    """Return a module logger, configuring the root logger once if nothing else has."""
    if not logging.getLogger().handlers:
        logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format=_FORMAT)
    return logging.getLogger(name)
