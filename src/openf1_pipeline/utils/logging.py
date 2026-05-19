"""
Simple logging configuration for pipeline modules.
"""

from __future__ import annotations

import logging
import sys

_CONFIGURED = False
_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _configure_root() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    if not root.handlers:
        root.addHandler(handler)
    _CONFIGURED = True


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Return a logger with a single stream handler (no duplicates)."""
    _configure_root()
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = True
    return logger
