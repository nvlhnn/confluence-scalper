"""
SQLite database manager for TDB bot.

Handles schema creation, trade persistence, signal logging,
daily P&L tracking, and coin scan history.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

from loguru import logger

from src.core.config import Config
from src.data.models import Signal, Trade


class Database:
    """SQLite persistence layer."""

    def __init__(self, config: Config) -> None:
        self._db_path = config.db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection | None = None

    # ── Connection ─────────────────────────────────────────

    def connect(self) -> None:
        """Open database connection and create schema."""
        self._conn = sqlite3.connect(
            str(self._db_path),
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._create_schema()
        logger.info("Database connected — {}", self._db_path)

    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("Database closed")

    @contextmanager
    def _cursor(self) -> Generator[sqlite3.Cursor, None, None]:
        """Context manager for cursor with auto-commit."""
        assert self._conn is not None, "Database not connected"
        cursor = self._conn.cursor()
        try:
            yield cursor
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    # ── Schema ─────────────────────────────────────────────

    def _create_schema(self) -> None:
        """Create all tables if they don't exist."""
        assert self._conn is not None
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS trades (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                status TEXT NOT NULL,
                entry_price REAL,
                exit_price REAL,
                stop_loss REAL,
                take_profit REAL,
                position_size REAL,
                margin_used REAL,
                leverage INTEGER,
                pnl REAL DEFAULT 0,
                fees REAL DEFAULT 0,
                net_pnl REAL DEFAULT 0,
                confluence_score INTEGER,
                quality TEXT,
                regime TEXT,
                entry_order_id TEXT,
                stop_order_id TEXT,
                tp_order_id TEXT,
                opened_at INTEGER,
                closed_at INTEGER,
                close_reason TEXT,
                metadata TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS daily_pnl (
                date TEXT PRIMARY KEY,
                starting_balance REAL,
                ending_balance REAL,
                total_pnl REAL,
                total_fees REAL,
                net_pnl REAL,
                trades_count INTEGER,
                wins INTEGER,
                losses INTEGER,
                win_rate REAL,
                best_trade REAL,
                worst_trade REAL,
                max_drawdown_pct REAL
            );

            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                confluence_score INTEGER,
                quality TEXT,
                regime TEXT,
                taken INTEGER DEFAULT 0,
                rejected_reason TEXT,
                layer_scores TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS coin_scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_time TIMESTAMP NOT NULL,
                total_pairs_scanned INTEGER,
                pairs_passed_filter INTEGER,
                selected_coins TEXT NOT NULL,
                scores TEXT NOT NULL,
                coins_added TEXT,
                coins_removed TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS bot_state (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
            CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
            CREATE INDEX IF NOT EXISTS idx_trades_opened ON trades(opened_at);
            CREATE INDEX IF NOT EXISTS idx_signals_created ON signals(created_at);
        """)

    # ── Trade CRUD ─────────────────────────────────────────

    def save_trade(self, trade: Trade) -> None:
        """Insert or update a trade."""
        signal = trade.signal
        with self._cursor() as cur:
            cur.execute("""
                INSERT OR REPLACE INTO trades (
                    id, symbol, direction, status,
                    entry_price, exit_price, stop_loss, take_profit,
                    position_size, margin_used, leverage,
                    pnl, fees, net_pnl,
                    confluence_score, quality, regime,
                    entry_order_id, stop_order_id, tp_order_id,
                    opened_at, closed_at, close_reason, metadata
                ) VALUES (
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?
                )
            """, (
                trade.id,
                signal.symbol if signal else "",
                signal.direction if signal else "",
                trade.status,
                trade.entry_fill_price,
                trade.exit_fill_price,
                signal.stop_loss if signal else 0,
                signal.take_profit if signal else 0,
                trade.position_size,
                trade.margin_used,
                trade.leverage,
                trade.pnl,
                trade.fees,
                trade.net_pnl,
                signal.confluence_score if signal else 0,
                signal.quality if signal else "",
                signal.regime if signal else "",
                trade.entry_order_id,
                trade.stop_order_id,
                trade.tp_order_id,
                trade.opened_at,
                trade.closed_at,
                trade.close_reason,
                json.dumps(signal.metadata) if signal else "{}",
            ))

    def get_open_trades(self) -> list[dict[str, Any]]:
        """Get all trades with status OPEN or PENDING."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM trades WHERE status IN ('OPEN', 'PENDING') "
                "ORDER BY opened_at DESC"
            )
            return [dict(row) for row in cur.fetchall()]

    def get_trades_today(self) -> list[dict[str, Any]]:
        """Get all trades from today (UTC)."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM trades WHERE date(created_at) = ? "
                "ORDER BY opened_at DESC",
                (today,),
            )
            return [dict(row) for row in cur.fetchall()]

    # ── Signal Logging ─────────────────────────────────────

    def log_signal(
        self,
        signal: Signal,
        taken: bool = False,
        reason: str = "",
    ) -> None:
        """Log a signal for analysis (taken or rejected)."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO signals (
                    symbol, direction, confluence_score, quality,
                    regime, taken, rejected_reason, layer_scores
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal.symbol,
                signal.direction,
                signal.confluence_score,
                signal.quality,
                signal.regime,
                1 if taken else 0,
                reason,
                json.dumps(signal.metadata.get("layer_scores", {})),
            ))

    # ── Coin Scan Logging ──────────────────────────────────

    def log_scan(
        self,
        selected_coins: list[str],
        scores: dict[str, Any],
        total_scanned: int = 0,
        passed_filter: int = 0,
        added: set[str] | None = None,
        removed: set[str] | None = None,
    ) -> None:
        """Log a coin scan result."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO coin_scans (
                    scan_time, total_pairs_scanned, pairs_passed_filter,
                    selected_coins, scores, coins_added, coins_removed
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now(timezone.utc).isoformat(),
                total_scanned,
                passed_filter,
                json.dumps(selected_coins),
                json.dumps(scores),
                json.dumps(list(added)) if added else "[]",
                json.dumps(list(removed)) if removed else "[]",
            ))

    # ── Daily P&L ──────────────────────────────────────────

    def update_daily_pnl(
        self,
        date: str,
        starting_balance: float,
        ending_balance: float,
    ) -> None:
        """Update or create daily P&L record."""
        trades = self._get_trades_for_date(date)

        total_pnl = sum(t["pnl"] for t in trades)
        total_fees = sum(t["fees"] for t in trades)
        net_pnl = sum(t["net_pnl"] for t in trades)
        wins = sum(1 for t in trades if t["net_pnl"] > 0)
        losses = sum(1 for t in trades if t["net_pnl"] <= 0)
        trade_count = len(trades)
        win_rate = (wins / trade_count * 100) if trade_count > 0 else 0
        best = max((t["net_pnl"] for t in trades), default=0)
        worst = min((t["net_pnl"] for t in trades), default=0)

        with self._cursor() as cur:
            cur.execute("""
                INSERT OR REPLACE INTO daily_pnl (
                    date, starting_balance, ending_balance,
                    total_pnl, total_fees, net_pnl,
                    trades_count, wins, losses, win_rate,
                    best_trade, worst_trade, max_drawdown_pct
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                date, starting_balance, ending_balance,
                total_pnl, total_fees, net_pnl,
                trade_count, wins, losses, win_rate,
                best, worst, 0.0,
            ))

    def _get_trades_for_date(self, date: str) -> list[dict[str, Any]]:
        """Get closed trades for a specific date."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM trades WHERE status = 'CLOSED' "
                "AND date(created_at) = ?",
                (date,),
            )
            return [dict(row) for row in cur.fetchall()]

    # ── Bot State ──────────────────────────────────────────

    def set_state(self, key: str, value: str) -> None:
        """Persist a key-value state."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT OR REPLACE INTO bot_state (key, value, updated_at)
                VALUES (?, ?, ?)
            """, (key, value, datetime.now(timezone.utc).isoformat()))

    def get_state(self, key: str, default: str = "") -> str:
        """Retrieve a persisted state value."""
        with self._cursor() as cur:
            cur.execute("SELECT value FROM bot_state WHERE key = ?", (key,))
            row = cur.fetchone()
            return row["value"] if row else default

    # ── Stats ──────────────────────────────────────────────

    def get_daily_realized_pnl(self) -> float:
        """Sum of net P&L for all closed trades today."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self._cursor() as cur:
            cur.execute(
                "SELECT COALESCE(SUM(net_pnl), 0) as total "
                "FROM trades WHERE status = 'CLOSED' "
                "AND date(created_at) = ?",
                (today,),
            )
            return cur.fetchone()["total"]

    def get_trade_count_today(self) -> int:
        """Number of trades opened today."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self._cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) as cnt FROM trades "
                "WHERE date(created_at) = ?",
                (today,),
            )
            return cur.fetchone()["cnt"]
