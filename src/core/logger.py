"""
Logging setup for TDB bot using Loguru.

Provides structured logging to console and rotating file.
"""

from __future__ import annotations

import sys
from pathlib import Path

from loguru import logger

from src.core.config import Config


def setup_logger(config: Config) -> None:
    """
    Configure loguru with console + file sinks.

    Call once at startup. After this, just ``from loguru import logger``
    anywhere in the codebase and use ``logger.info(...)``, etc.
    """
    # Remove default handler
    logger.remove()

    # Console sink
    logger.add(
        sys.stdout,
        level=config.log_level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{module}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        ),
        colorize=True,
    )

    # File sink — rotating logs
    log_dir = config.log_dir
    log_dir.mkdir(parents=True, exist_ok=True)

    logger.add(
        log_dir / "tdb.log",
        level=config.log_level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{line} | {message}",
        rotation="10 MB",
        retention="30 days",
        compression="zip",
    )

    # Separate trade log
    logger.add(
        log_dir / "trades.log",
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {message}",
        rotation="10 MB",
        retention="30 days",
        filter=lambda record: record["extra"].get("trade", False),
    )

    logger.info("Logger initialized — level={}", config.log_level)
