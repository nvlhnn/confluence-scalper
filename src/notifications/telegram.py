"""
Telegram notification system.

Sends trade alerts, daily summaries, circuit breaker warnings,
and heartbeat messages to the configured Telegram chat.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from loguru import logger

from src.core.config import Config
from src.data.models import PositionState, Signal, Trade


class TelegramNotifier:
    """Sends formatted messages to Telegram."""

    def __init__(self, config: Config) -> None:
        self._cfg = config
        self._enabled = config.telegram_enabled
        self._bot = None

    async def initialize(self) -> None:
        """Initialize Telegram bot if enabled."""
        if not self._enabled:
            logger.info("Telegram notifications disabled")
            return

        token = self._cfg.telegram_bot_token
        if not token:
            logger.warning("Telegram token not set — disabling notifications")
            self._enabled = False
            return

        try:
            from telegram import Bot
            self._bot = Bot(token=token)
            me = await self._bot.get_me()
            logger.info("Telegram bot connected: @{}", me.username)
        except Exception as e:
            logger.error("Failed to initialize Telegram: {}", e)
            self._enabled = False

    async def _send(self, text: str) -> None:
        """Send a message to the configured chat."""
        if not self._enabled or not self._bot:
            return
        try:
            chat_id = self._cfg.telegram_chat_id
            await self._bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error("Telegram send failed: {}", e)

    # ── Signal Messages ────────────────────────────────────

    async def signal_detected(self, signal: Signal) -> None:
        """Notify about a new signal."""
        scores = signal.metadata.get("layer_scores", {})
        msg = (
            f"🔍 <b>Signal Detected</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>{signal.direction}</b> {signal.symbol}\n"
            f"Score: <b>{signal.confluence_score}/13</b> ({signal.quality})\n"
            f"Regime: {signal.regime}\n"
            f"\n"
            f"📊 Layer Breakdown:\n"
            f"  Regime:     {scores.get('regime', 0)}/2\n"
            f"  Trend:      {scores.get('trend', 0)}/2\n"
            f"  Divergence: {scores.get('divergence', 0)}/3\n"
            f"  Level:      {scores.get('level', 0)}/3\n"
            f"  Volume:     {scores.get('volume', 0)}/2\n"
            f"  Candle:     {scores.get('candle', 0)}/1\n"
            f"\n"
            f"RSI: {signal.metadata.get('rsi', 0):.1f} | "
            f"ADX: {signal.metadata.get('adx', 0):.1f}"
        )
        await self._send(msg)

    async def position_opened(self, trade: Trade) -> None:
        """Notify about a new position."""
        signal = trade.signal
        if not signal:
            return

        direction_emoji = "📈" if signal.direction == "LONG" else "📉"
        msg = (
            f"{direction_emoji} <b>Position Opened</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>{signal.direction}</b> {signal.symbol}\n"
            f"Entry: ${trade.entry_fill_price:,.4f}\n"
            f"SL:    ${signal.stop_loss:,.4f}\n"
            f"TP:    ${signal.take_profit:,.4f}\n"
            f"\n"
            f"Size:     ${trade.position_size:,.2f}\n"
            f"Margin:   ${trade.margin_used:,.2f}\n"
            f"Leverage: {trade.leverage}x\n"
            f"Score:    {signal.confluence_score}/13 ({signal.quality})"
        )
        await self._send(msg)

    async def position_closed(self, trade: Trade, reason: str = "") -> None:
        """Notify about a closed position."""
        signal = trade.signal
        if not signal:
            return

        if trade.net_pnl >= 0:
            emoji = "✅"
            pnl_str = f"+${trade.net_pnl:.2f}"
        else:
            emoji = "❌"
            pnl_str = f"-${abs(trade.net_pnl):.2f}"

        close_reason = trade.close_reason or reason
        msg = (
            f"{emoji} <b>Position Closed</b> — {close_reason}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"{signal.direction} {signal.symbol}\n"
            f"Entry:  ${trade.entry_fill_price:,.4f}\n"
            f"Exit:   ${trade.exit_fill_price:,.4f}\n"
            f"P&L:    <b>{pnl_str}</b> (fees: ${trade.fees:.4f})\n"
            f"Reason: {close_reason}"
        )
        await self._send(msg)

    async def stop_updated(self, symbol: str, new_stop: float, reason: str) -> None:
        """Notify about stop loss update."""
        emoji = "🔄" if reason == "BREAKEVEN" else "📊"
        msg = (
            f"{emoji} <b>Stop Updated</b> — {reason}\n"
            f"{symbol} → SL: ${new_stop:,.4f}"
        )
        await self._send(msg)

    # ── Scanner Messages ───────────────────────────────────

    async def coin_rotation(
        self,
        added: set[str],
        removed: set[str],
        active: list[str],
    ) -> None:
        """Notify about coin lineup change."""
        msg = (
            f"🔄 <b>Coin Rotation</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
        )
        if added:
            msg += f"➕ Added: {', '.join(added)}\n"
        if removed:
            msg += f"➖ Removed: {', '.join(removed)}\n"
        msg += f"\n📋 Active ({len(active)}): {', '.join(active[:10])}"
        if len(active) > 10:
            msg += f" +{len(active) - 10} more"
        await self._send(msg)

    # ── Circuit Breaker Messages ───────────────────────────

    async def circuit_breaker(self, reason: str) -> None:
        """Notify about circuit breaker trigger."""
        msg = (
            f"🛑 <b>CIRCUIT BREAKER</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"Trading PAUSED\n"
            f"Reason: {reason}\n"
            f"\n"
            f"⚠️ Manual review required before resuming."
        )
        await self._send(msg)

    # ── Daily Summary ──────────────────────────────────────

    async def daily_summary(self, stats: dict[str, Any]) -> None:
        """Send end-of-day summary."""
        pnl = stats.get("net_pnl", 0)
        pnl_emoji = "🟢" if pnl >= 0 else "🔴"
        pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"

        msg = (
            f"📋 <b>Daily Summary</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"{pnl_emoji} Net P&L: <b>{pnl_str}</b>\n"
            f"Trades:   {stats.get('trades', 0)}\n"
            f"Wins:     {stats.get('wins', 0)}\n"
            f"Losses:   {stats.get('losses', 0)}\n"
            f"Win Rate: {stats.get('win_rate', 0):.0f}%\n"
            f"Fees:     ${stats.get('fees', 0):.4f}\n"
            f"\n"
            f"💰 Balance: ${stats.get('balance', 0):.2f}\n"
            f"📈 Peak:    ${stats.get('peak', 0):.2f}"
        )
        await self._send(msg)

    # ── System Messages ────────────────────────────────────

    async def heartbeat(self, balance: float, open_positions: int) -> None:
        """Hourly heartbeat status."""
        msg = (
            f"🔧 <b>Heartbeat</b> — "
            f"{datetime.now(timezone.utc).strftime('%H:%M UTC')}\n"
            f"Balance: ${balance:.2f} | "
            f"Open: {open_positions} positions"
        )
        await self._send(msg)

    async def bot_started(self, mode: str, balance: float, coins: int) -> None:
        """Bot startup notification."""
        msg = (
            f"🚀 <b>TDB Bot Started</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"Mode:    {mode.upper()}\n"
            f"Balance: ${balance:.2f}\n"
            f"Coins:   {coins} active\n"
            f"\n"
            f"Strategy: MCS (Momentum Confluence Scalper)"
        )
        await self._send(msg)

    async def error_alert(self, error: str) -> None:
        """Send error notification."""
        msg = (
            f"⚠️ <b>Error</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"<code>{error[:500]}</code>"
        )
        await self._send(msg)
