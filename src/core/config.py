"""
Configuration loader for TDB bot.

Priority: ENV vars > YAML file > Defaults.
Loads settings from config/settings.yaml and .env file.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


# Project root directory
ROOT_DIR = Path(__file__).resolve().parent.parent.parent


def _deep_get(data: dict, *keys: str, default: Any = None) -> Any:
    """Safely traverse nested dicts."""
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key, default)
        else:
            return default
    return data if data is not None else default


class Config:
    """
    Centralized configuration.

    Loads all settings from config/settings.yaml and merges
    with environment variables (env vars take priority).
    """

    def __init__(
        self,
        env_path: str | Path | None = None,
        config_path: str | Path | None = None,
    ) -> None:
        # Load .env
        env_file = Path(env_path) if env_path else ROOT_DIR / ".env"
        if env_file.exists():
            load_dotenv(env_file)

        # Load YAML settings
        yaml_file = Path(config_path) if config_path else ROOT_DIR / "config" / "settings.yaml"
        self._settings: dict = {}
        if yaml_file.exists():
            with open(yaml_file, "r", encoding="utf-8") as f:
                self._settings = yaml.safe_load(f) or {}

    # ── Helpers ────────────────────────────────────────────

    def get(self, *keys: str, default: Any = None) -> Any:
        """Get a nested config value. e.g. config.get('risk', 'leverage', 'base')"""
        return _deep_get(self._settings, *keys, default=default)

    # ── Binance ────────────────────────────────────────────

    @property
    def binance_api_key(self) -> str:
        return os.getenv("BINANCE_API_KEY", "")

    @property
    def binance_api_secret(self) -> str:
        return os.getenv("BINANCE_API_SECRET", "")

    @property
    def is_testnet(self) -> bool:
        return os.getenv("BINANCE_TESTNET", "true").lower() == "true"

    # ── Bot Mode ───────────────────────────────────────────

    @property
    def bot_mode(self) -> str:
        return os.getenv("BOT_MODE", "testnet").lower()

    # ── Telegram ───────────────────────────────────────────

    @property
    def telegram_bot_token(self) -> str:
        return os.getenv("TELEGRAM_BOT_TOKEN", "")

    @property
    def telegram_chat_id(self) -> str:
        return os.getenv("TELEGRAM_CHAT_ID", "")

    @property
    def telegram_enabled(self) -> bool:
        env = os.getenv("TELEGRAM_ENABLED")
        if env is not None:
            return env.lower() == "true"
        return self.get("notifications", "telegram", "enabled", default=False)

    # ── Strategy ───────────────────────────────────────────

    @property
    def timeframes(self) -> dict:
        return self.get("strategy", "timeframes", default={
            "entry": "5m", "regime": "15m", "trend": "1h"
        })

    @property
    def regime_config(self) -> dict:
        return self.get("strategy", "regime", default={})

    @property
    def trend_config(self) -> dict:
        return self.get("strategy", "trend", default={})

    @property
    def divergence_config(self) -> dict:
        return self.get("strategy", "divergence", default={})

    @property
    def levels_config(self) -> dict:
        return self.get("strategy", "levels", default={})

    @property
    def volume_config(self) -> dict:
        return self.get("strategy", "volume", default={})

    @property
    def candle_config(self) -> dict:
        return self.get("strategy", "candles", default={})

    @property
    def scoring_config(self) -> dict:
        return self.get("strategy", "scoring", default={})

    # ── Screening ──────────────────────────────────────────

    @property
    def screening_config(self) -> dict:
        return self.get("screening", default={})

    @property
    def max_active_coins(self) -> int:
        return self.get("screening", "dynamic", "max_active_coins", default=30)

    @property
    def rescreen_interval_hours(self) -> int:
        return self.get("screening", "dynamic", "rescreen_interval_hours", default=4)

    @property
    def blacklist(self) -> list[str]:
        return self.get("screening", "blacklist", default=[])

    @property
    def whitelist(self) -> list[str]:
        return self.get("screening", "whitelist", default=["BTCUSDT", "ETHUSDT"])

    # ── Execution ──────────────────────────────────────────

    @property
    def execution_config(self) -> dict:
        return self.get("execution", default={})

    # ── Risk ───────────────────────────────────────────────

    @property
    def risk_config(self) -> dict:
        return self.get("risk", default={})

    @property
    def risk_per_trade_pct(self) -> float:
        return self.get("risk", "position", "risk_per_trade_pct", default=2.0) / 100.0

    @property
    def max_margin_pct(self) -> float:
        return self.get("risk", "position", "max_margin_pct", default=25.0) / 100.0

    @property
    def base_leverage(self) -> int:
        return self.get("risk", "leverage", "base", default=20)

    @property
    def min_leverage(self) -> int:
        return self.get("risk", "leverage", "min", default=10)

    @property
    def max_leverage(self) -> int:
        return self.get("risk", "leverage", "max", default=25)

    @property
    def max_open_positions(self) -> int:
        return self.get("risk", "limits", "max_open_positions", default=2)

    @property
    def max_daily_trades(self) -> int:
        return self.get("risk", "limits", "max_daily_trades", default=5)

    @property
    def margin_type(self) -> str:
        return self.get("risk", "margin_type", default="ISOLATED")

    # ── Database ───────────────────────────────────────────

    @property
    def db_path(self) -> Path:
        rel = self.get("database", "path", default="data/tdb.db")
        return ROOT_DIR / rel

    # ── Logging ────────────────────────────────────────────

    @property
    def log_level(self) -> str:
        return self.get("logging", "level", default="INFO")

    @property
    def log_dir(self) -> Path:
        rel = self.get("logging", "file", "path", default="logs/")
        return ROOT_DIR / rel
