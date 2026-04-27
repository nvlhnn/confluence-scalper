"""
TDB — Trading Daily Bot (Entry Point)

Momentum Confluence Scalper targeting $5-10/day on $50 balance.

Usage:
    python main.py              # Run with default config
    python main.py --testnet    # Force testnet mode
    python main.py --live       # Force live mode (requires confirmation)
"""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.core.config import Config
from src.core.logger import setup_logger
from src.core.bot import Bot


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="TDB — Momentum Confluence Scalper",
    )
    parser.add_argument(
        "--testnet",
        action="store_true",
        help="Force testnet mode",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Force live trading mode",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to config YAML file",
    )
    parser.add_argument(
        "--env",
        type=str,
        default=None,
        help="Path to .env file",
    )
    return parser.parse_args()


async def main() -> None:
    """Main entry point."""
    args = parse_args()

    # Load config
    config = Config(env_path=args.env, config_path=args.config)
    setup_logger(config)

    from loguru import logger

    # Override mode from CLI
    if args.testnet:
        os.environ["BINANCE_TESTNET"] = "true"
        os.environ["BOT_MODE"] = "testnet"
    elif args.live:
        os.environ["BINANCE_TESTNET"] = "false"
        os.environ["BOT_MODE"] = "live"

        # Safety confirmation for live mode
        logger.warning("=" * 60)
        logger.warning("⚠️  LIVE TRADING MODE — REAL MONEY AT RISK")
        logger.warning("=" * 60)
        confirm = input("Type 'CONFIRM' to proceed with live trading: ")
        if confirm != "CONFIRM":
            logger.info("Live trading cancelled.")
            return

    # Check API keys
    if not config.binance_api_key or config.binance_api_key == "your_api_key_here":
        logger.error("Binance API key not set. Copy .env.example to .env and fill in your keys.")
        return

    mode = config.bot_mode
    logger.info("Starting TDB in {} mode...", mode.upper())

    # Create and start bot
    bot = Bot(config)

    # Handle graceful shutdown
    loop = asyncio.get_event_loop()

    def shutdown_handler():
        logger.info("Shutdown signal received...")
        bot.is_running = False

    # Register signal handlers (Unix-style)
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, shutdown_handler)
    except NotImplementedError:
        # Windows doesn't support add_signal_handler
        pass

    try:
        await bot.start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        await bot.shutdown()
    except Exception as e:
        logger.critical("Fatal error: {}", e)
        await bot.shutdown()
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
