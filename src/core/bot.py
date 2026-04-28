"""
Main bot orchestrator — runs the 3-tier async event system.

Tier 1: Coin Scanner     — every 4 hours
Tier 2: Signal Checker   — every 5-minute candle close
Tier 3: Position Monitor — every 30 seconds (when positions open)
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone

from loguru import logger

from src.core.config import Config
from src.data.candle_cache import CandleCache
from src.data.models import PositionState, Signal, Trade
from src.database.db import Database
from src.exchange.binance_client import BinanceClient
from src.notifications.telegram import TelegramNotifier
from src.risk.risk_manager import RiskManager
from src.strategy.confluence import ConfluenceScorer
from src.strategy.engine import IndicatorEngine
from src.strategy.screener import CoinScanner


class Bot:
    """
    TDB Bot — Momentum Confluence Scalper.

    Coordinates all subsystems and runs the 3-tier async event loop.
    """

    def __init__(self, config: Config) -> None:
        self.config = config
        self.is_running = False

        # Subsystems
        self.client = BinanceClient(config)
        self.db = Database(config)
        self.notifier = TelegramNotifier(config)
        self.risk_manager = RiskManager(config, self.db)
        self.screener = CoinScanner(config, self.client)
        self.indicator_engine = IndicatorEngine(config)
        self.confluence = ConfluenceScorer(config)
        self.candle_cache = CandleCache()

        # Position tracking
        self.open_positions: list[PositionState] = []
        self.pending_entries: list[Trade] = []  # Unfilled limit entries

        # Timing
        self._last_heartbeat = 0.0
        self._heartbeat_interval = 3600  # 1 hour

    # ── Lifecycle ──────────────────────────────────────────

    async def start(self) -> None:
        """Initialize all systems and start the event loop."""
        logger.info("=" * 60)
        logger.info("TDB Bot — Momentum Confluence Scalper")
        logger.info("=" * 60)

        # Connect subsystems
        self.db.connect()
        await self.client.connect()
        await self.notifier.initialize()

        # Initialize risk manager
        balance = await self.client.get_balance()
        self.risk_manager.initialize(balance)

        # Recover open positions from exchange
        await self._recover_positions()

        # Initial coin scan
        logger.info("Running initial coin scan...")
        active_coins = await self.screener.scan()
        logger.info("Active coins ({}): {}", len(active_coins), active_coins)

        # Log scan to DB
        scores = self.screener.get_scores()
        self.db.log_scan(
            selected_coins=active_coins,
            scores={s: {"score": c.score, "atr": c.atr_pct, "vol": c.volume_24h}
                    for s, c in scores.items()},
            total_scanned=0,
            passed_filter=len(active_coins),
        )

        # Notify
        await self.notifier.bot_started(
            mode=self.config.bot_mode,
            balance=balance,
            coins=len(active_coins),
        )

        self.is_running = True

        # Run the 3-tier system
        try:
            await asyncio.gather(
                self._tier1_coin_scanner(),
                self._tier2_signal_checker(),
                self._tier3_position_monitor(),
                self._heartbeat_loop(),
            )
        except asyncio.CancelledError:
            logger.info("Bot shutting down...")
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        """Graceful shutdown."""
        self.is_running = False
        logger.info("Shutting down...")
        await self.client.close()
        self.db.close()
        logger.info("Bot stopped.")

    # ── Tier 1: Coin Scanner ──────────────────────────────

    async def _tier1_coin_scanner(self) -> None:
        """Scan all futures pairs every 4 hours."""
        interval = self.config.rescreen_interval_hours * 3600

        # Wait before first re-scan (initial scan already done in start())
        await asyncio.sleep(interval)

        while self.is_running:
            try:
                logger.info("━━━ TIER 1: Coin Scanner ━━━")
                old_coins = set(self.screener.active_coins)

                new_coins = await self.screener.scan()
                new_set = set(new_coins)

                added = new_set - old_coins
                removed = old_coins - new_set

                if added or removed:
                    await self.notifier.coin_rotation(added, removed, new_coins)

                # Log scan
                scores = self.screener.get_scores()
                self.db.log_scan(
                    selected_coins=new_coins,
                    scores={s: {"score": c.score} for s, c in scores.items()},
                    added=added,
                    removed=removed,
                )

            except Exception as e:
                logger.error("Tier 1 error: {}", e)
                await self.notifier.error_alert(f"Scanner error: {e}")

            await asyncio.sleep(interval)

    # ── Tier 2: Signal Checker ────────────────────────────

    async def _tier2_signal_checker(self) -> None:
        """Check for signals on every 5-minute candle close."""
        while self.is_running:
            try:
                # Wait for next candle close
                await self._wait_for_candle_close()

                active_coins = self.screener.active_coins
                if not active_coins:
                    logger.debug("No active coins — skipping cycle")
                    continue

                logger.info(
                    "━━━ TIER 2: Signal Check ({} coins) ━━━",
                    len(active_coins),
                )

                # Fetch candle data (with smart caching)
                candle_data = await self.candle_cache.update(
                    active_coins, self.client,
                )

                # Process each coin
                all_signals: list[Signal] = []
                checked_count = 0
                skipped_count = 0
                for symbol in active_coins:
                    data = candle_data.get(symbol, {})
                    candles_5m = data.get("5m", [])
                    candles_15m = data.get("15m", [])
                    candles_1h = data.get("1h", [])

                    if not candles_5m or not candles_15m or not candles_1h:
                        skipped_count += 1
                        continue

                    checked_count += 1

                    # Calculate indicators
                    ind_set, rsi_hist, price_hist = self.indicator_engine.calculate(
                        symbol, candles_5m, candles_15m, candles_1h,
                    )

                    # Run confluence
                    signal = self.confluence.evaluate(ind_set, rsi_hist, price_hist)

                    if signal is not None:
                        signal.timestamp = int(time.time() * 1000)
                        all_signals.append(signal)
                        logger.info(
                            "  {} {} — score {}/13 ({})",
                            signal.direction, signal.symbol,
                            signal.confluence_score, signal.quality,
                        )

                if not all_signals:
                    logger.info(
                        "Signal cycle complete — checked={} skipped={} signals=0",
                        checked_count, skipped_count,
                    )
                    continue

                # Sort by score (best first)
                all_signals.sort(
                    key=lambda s: s.confluence_score, reverse=True,
                )

                # Validate and execute
                taken_count = 0
                rejected_count = 0
                failed_count = 0
                for signal in all_signals:
                    approved, reason = self.risk_manager.validate(signal)

                    if approved:
                        trade = await self._execute_signal(signal)
                        if trade:
                            taken_count += 1
                            self.db.log_signal(signal, taken=True)
                        else:
                            failed_count += 1
                            self.db.log_signal(signal, taken=False, reason="Execution failed")
                    else:
                        rejected_count += 1
                        logger.debug(
                            "  {} {} rejected: {}",
                            signal.direction, signal.symbol, reason,
                        )
                        self.db.log_signal(signal, taken=False, reason=reason)

                logger.info(
                    "Signal cycle complete — checked={} skipped={} signals={} taken={} rejected={} failed={}",
                    checked_count, skipped_count, len(all_signals),
                    taken_count, rejected_count, failed_count,
                )

            except Exception as e:
                logger.error("Tier 2 error: {}", e)
                await asyncio.sleep(10)

    # ── Tier 3: Position Monitor ──────────────────────────

    async def _tier3_position_monitor(self) -> None:
        """Monitor pending entries and open positions every 30 seconds."""
        while self.is_running:
            try:
                # Check pending entry fills first
                await self._check_pending_entries()

                if not self.open_positions:
                    await asyncio.sleep(5)
                    continue

                for pos in list(self.open_positions):
                    trade = pos.trade
                    signal = trade.signal
                    if not signal:
                        continue

                    # Fetch current price
                    try:
                        ticker = await self.client.fetch_ticker(signal.symbol)
                        current_price = float(ticker.get("last", 0))
                    except Exception:
                        continue

                    pos.current_price = current_price
                    pos.bars_held += 1

                    # Calculate unrealized P&L
                    if signal.direction == "LONG":
                        pos.unrealized_pnl = (
                            (current_price - trade.entry_fill_price)
                            / trade.entry_fill_price * trade.position_size
                        )
                    else:
                        pos.unrealized_pnl = (
                            (trade.entry_fill_price - current_price)
                            / trade.entry_fill_price * trade.position_size
                        )

                    # R:R ratio
                    stop_dist = abs(trade.entry_fill_price - signal.stop_loss)
                    if stop_dist > 0:
                        if signal.direction == "LONG":
                            pos.unrealized_rr = (
                                (current_price - trade.entry_fill_price) / stop_dist
                            )
                        else:
                            pos.unrealized_rr = (
                                (trade.entry_fill_price - current_price) / stop_dist
                            )

                    # ── Exit management ──
                    await self._manage_exit(pos)

                await asyncio.sleep(30)

            except Exception as e:
                logger.error("Tier 3 error: {}", e)
                await asyncio.sleep(5)

    async def _manage_exit(self, pos: PositionState) -> None:
        """Manage trailing stop and time-based exit for a position."""
        trade = pos.trade
        signal = trade.signal
        if not signal:
            return

        exec_cfg = self.config.execution_config
        trail_cfg = exec_cfg.get("trailing", {})

        # ── Breakeven ──
        be_rr = trail_cfg.get("breakeven_at_rr", 1.5)
        if pos.unrealized_rr >= be_rr and pos.trailing_stop == 0:
            buffer = trail_cfg.get("breakeven_buffer_pct", 0.05) / 100.0
            if signal.direction == "LONG":
                new_stop = trade.entry_fill_price * (1 + buffer)
            else:
                new_stop = trade.entry_fill_price * (1 - buffer)

            pos.trailing_stop = new_stop
            # Update stop on exchange
            try:
                # Cancel old stop
                if trade.stop_order_id:
                    await self.client.cancel_order(signal.symbol, trade.stop_order_id)

                # Place new stop
                sl_side = "sell" if signal.direction == "LONG" else "buy"
                amount = trade.position_size / pos.current_price
                new_id = await self.client.place_stop_loss(
                    signal.symbol, sl_side, amount, new_stop,
                )
                trade.stop_order_id = new_id

                logger.info(
                    "  {} SL → breakeven ${:.4f}",
                    signal.symbol, new_stop,
                )
                await self.notifier.stop_updated(signal.symbol, new_stop, "BREAKEVEN")
            except Exception as e:
                logger.error("Failed to update SL: {}", e)

        # ── Trailing stop ──
        trail_rr = trail_cfg.get("activate_at_rr", 2.0)
        if pos.unrealized_rr >= trail_rr and pos.trailing_stop > 0:
            atr_val = signal.metadata.get("atr", 0)
            if atr_val > 0:
                new_stop = None
                if signal.direction == "LONG":
                    candidate = pos.current_price - atr_val
                    if candidate > pos.trailing_stop:
                        new_stop = candidate
                else:
                    candidate = pos.current_price + atr_val
                    if candidate < pos.trailing_stop:
                        new_stop = candidate

                if new_stop is not None:
                    try:
                        if trade.stop_order_id:
                            await self.client.cancel_order(signal.symbol, trade.stop_order_id)
                        sl_side = "sell" if signal.direction == "LONG" else "buy"
                        amount = self.client.format_amount(
                            signal.symbol, trade.position_size / pos.current_price,
                        )
                        new_id = await self.client.place_stop_loss(
                            signal.symbol, sl_side, amount, new_stop,
                        )
                        trade.stop_order_id = new_id
                        pos.trailing_stop = new_stop
                        logger.info("  {} trailing SL → ${:.4f}", signal.symbol, new_stop)
                        await self.notifier.stop_updated(signal.symbol, new_stop, "TRAILING")
                    except Exception as e:
                        logger.error("Failed to update trailing SL: {}", e)

        # ── Time-based exit ──
        time_cfg = exec_cfg.get("time_stop", {})
        if time_cfg.get("enabled", True):
            max_bars = time_cfg.get("max_bars", 15)
            min_move = time_cfg.get("min_move_pct", 0.3) / 100.0

            if pos.bars_held >= max_bars * 6:  # bars_held counts every 30s
                price_move = abs(
                    pos.current_price - trade.entry_fill_price
                ) / trade.entry_fill_price
                if price_move < min_move:
                    logger.info(
                        "  {} time exit — {}bars, only {:.2f}% move",
                        signal.symbol, pos.bars_held, price_move * 100,
                    )
                    await self._close_position(pos, "TIME")

    # ── Trade Execution ────────────────────────────────────

    async def _execute_signal(self, signal: Signal) -> Trade | None:
        """Execute a signal: set leverage, place entry order only.

        SL/TP are placed after entry fill confirmation (see _check_pending_entries).
        """
        try:
            balance = await self.client.get_balance()

            # Check combined pending + open against limits
            max_pos = self.config.max_open_positions
            if len(self.open_positions) + len(self.pending_entries) >= max_pos:
                logger.warning("Position limit reached (open={}, pending={})",
                               len(self.open_positions), len(self.pending_entries))
                return None

            # Calculate position size
            sizing = self.risk_manager.calculate_position_size(
                balance, signal, signal.metadata.get("atr", 0),
            )

            if sizing["position_size"] < self.config.risk_config.get(
                "position", {}
            ).get("min_order_value", 5.0):
                logger.warning("Position size too small: ${:.2f}", sizing["position_size"])
                return None

            # Set leverage and margin type
            await self.client.set_margin_type(signal.symbol, self.config.margin_type)
            await self.client.set_leverage(signal.symbol, sizing["leverage"])

            # Use ccxt precision methods
            amount = self.client.format_amount(
                signal.symbol, sizing["position_size"] / signal.entry_price,
            )
            min_amount = self.client.get_min_amount(signal.symbol)
            if amount < min_amount:
                logger.warning(
                    "{} amount {:.6f} < min {:.6f}",
                    signal.symbol, amount, min_amount,
                )
                return None

            entry_price = self.client.format_price(signal.symbol, signal.entry_price)

            # ── Place entry order only — SL/TP after fill ──
            entry_side = "buy" if signal.direction == "LONG" else "sell"

            entry_id = await self.client.place_limit_order(
                signal.symbol, entry_side, amount, entry_price,
            )

            # Create trade as PENDING (no SL/TP yet)
            trade = Trade(
                signal=signal,
                entry_order_id=entry_id,
                status="PENDING",
                entry_fill_price=0.0,  # Set on fill
                position_size=sizing["position_size"],
                margin_used=sizing["margin_required"],
                leverage=sizing["leverage"],
                opened_at=int(time.time() * 1000),
            )

            # Track as pending — NOT in open_positions or risk_manager yet
            self.pending_entries.append(trade)
            self.db.save_trade(trade)

            logger.info(
                "📝 {} {} — entry limit ${:.4f} size=${:.2f} lev={}x (pending fill)",
                signal.direction, signal.symbol,
                entry_price, sizing["position_size"], sizing["leverage"],
            )

            return trade

        except Exception as e:
            logger.error("Execution failed for {}: {}", signal.symbol, e)
            await self.notifier.error_alert(f"Execution failed: {signal.symbol} — {e}")
            return None

    async def _check_pending_entries(self) -> None:
        """Check if pending entry orders have been filled, cancelled, or timed out."""
        entry_timeout = self.config.execution_config.get("entry_timeout_seconds", 300)

        for trade in list(self.pending_entries):
            signal = trade.signal
            if not signal:
                self.pending_entries.remove(trade)
                continue

            try:
                order = await self.client.get_order(signal.symbol, trade.entry_order_id)
                status = order.get("status", "").lower()

                if status == "closed":  # Filled
                    fill_price = float(order.get("average", 0) or order.get("price", 0))
                    trade.entry_fill_price = fill_price
                    trade.status = "OPEN"

                    # Now place SL/TP with reduceOnly
                    sl_side = "sell" if signal.direction == "LONG" else "buy"
                    amount = self.client.format_amount(
                        signal.symbol, trade.position_size / fill_price,
                    )
                    sl_price = self.client.format_price(signal.symbol, signal.stop_loss)
                    tp_price = self.client.format_price(signal.symbol, signal.take_profit)

                    sl_id = await self.client.place_stop_loss(
                        signal.symbol, sl_side, amount, sl_price,
                    )
                    tp_id = await self.client.place_take_profit(
                        signal.symbol, sl_side, amount, tp_price,
                    )
                    trade.stop_order_id = sl_id
                    trade.tp_order_id = tp_id

                    # Promote to open position
                    self.pending_entries.remove(trade)
                    self.risk_manager.add_open_position(trade)
                    self.open_positions.append(PositionState(trade=trade))
                    self.db.save_trade(trade)

                    await self.notifier.position_opened(trade)
                    logger.info(
                        "✅ {} {} filled @ ${:.4f} — SL/TP placed",
                        signal.direction, signal.symbol, fill_price,
                    )

                elif status in ("canceled", "cancelled", "expired", "rejected"):
                    self.pending_entries.remove(trade)
                    trade.status = "CANCELLED"
                    trade.close_reason = status.upper()
                    self.db.save_trade(trade)
                    logger.info("Entry {} for {} {}", status, signal.direction, signal.symbol)

                else:
                    # Check timeout
                    age_s = (time.time() * 1000 - trade.opened_at) / 1000
                    if age_s > entry_timeout:
                        await self.client.cancel_order(signal.symbol, trade.entry_order_id)
                        self.pending_entries.remove(trade)
                        trade.status = "CANCELLED"
                        trade.close_reason = "TIMEOUT"
                        self.db.save_trade(trade)
                        logger.info(
                            "Entry timed out ({:.0f}s): {} {}",
                            age_s, signal.direction, signal.symbol,
                        )

            except Exception as e:
                logger.error("Error checking pending entry {}: {}", trade.entry_order_id, e)

    async def _close_position(self, pos: PositionState, reason: str) -> None:
        """Close a position at market."""
        trade = pos.trade
        signal = trade.signal
        if not signal:
            return

        try:
            # Cancel existing SL/TP
            if trade.stop_order_id:
                await self.client.cancel_order(signal.symbol, trade.stop_order_id)
            if trade.tp_order_id:
                await self.client.cancel_order(signal.symbol, trade.tp_order_id)

            # Market close
            close_side = "sell" if signal.direction == "LONG" else "buy"
            amount = trade.position_size / pos.current_price
            await self.client.place_market_order(signal.symbol, close_side, amount)

            # Update trade
            trade.status = "CLOSED"
            trade.exit_fill_price = pos.current_price
            trade.pnl = pos.unrealized_pnl
            trade.fees = trade.position_size * 0.0004 * 2  # Estimate
            trade.net_pnl = trade.pnl - trade.fees
            trade.closed_at = int(time.time() * 1000)
            trade.close_reason = reason

            self.db.save_trade(trade)
            self.risk_manager.remove_open_position(trade.id)
            self.risk_manager.record_trade_result(trade)
            self.open_positions.remove(pos)

            await self.notifier.position_closed(trade, reason)

            logger.info(
                "Position closed: {} {} — P&L: ${:.2f} ({})",
                signal.direction, signal.symbol, trade.net_pnl, reason,
            )

        except Exception as e:
            logger.error("Failed to close {}: {}", signal.symbol, e)

    # ── Recovery ───────────────────────────────────────────

    async def _recover_positions(self) -> None:
        """Recover open positions after restart — reconstruct full state."""
        try:
            positions = await self.client.get_positions()
            if not positions:
                logger.info("No open positions to recover")
                return

            logger.info("Recovering {} open positions...", len(positions))

            for p in positions:
                try:
                    symbol = p.get("symbol") or ""
                    side = (p.get("side") or "").lower()
                    contracts = abs(float(p.get("contracts") or 0))
                    entry_price = float(p.get("entryPrice") or p.get("entry_price") or 0)
                    notional = abs(float(p.get("notional") or 0))
                    leverage = int(float(p.get("leverage") or self.config.base_leverage))
                except (TypeError, ValueError) as e:
                    logger.warning("Skipping malformed recovered position: {} ({})", p, e)
                    continue

                if contracts <= 0 or not symbol or entry_price <= 0:
                    continue

                direction = "LONG" if side == "long" else "SHORT"

                # Build minimal signal for recovered position
                signal = Signal(
                    symbol=symbol,
                    direction=direction,
                    entry_price=entry_price,
                    stop_loss=0.0,
                    take_profit=0.0,
                    confluence_score=0,
                    quality="RECOVERED",
                    regime="UNKNOWN",
                    timestamp=int(time.time() * 1000),
                )

                trade = Trade(
                    signal=signal,
                    status="OPEN",
                    entry_fill_price=entry_price,
                    position_size=notional if notional > 0 else contracts * entry_price,
                    leverage=leverage,
                    opened_at=int(time.time() * 1000),
                )

                # Try to find existing SL/TP orders on exchange
                try:
                    open_orders = await self.client.get_open_orders(symbol)
                    for order in open_orders:
                        otype = order.get("type", "").lower()
                        if "stop" in otype and "profit" not in otype:
                            trade.stop_order_id = str(order.get("id", ""))
                            signal.stop_loss = float(order.get("stopPrice", 0) or 0)
                        elif "profit" in otype:
                            trade.tp_order_id = str(order.get("id", ""))
                            signal.take_profit = float(order.get("stopPrice", 0) or 0)
                except Exception:
                    logger.warning("Could not fetch open orders for {}", symbol)

                pos = PositionState(trade=trade)
                self.open_positions.append(pos)
                self.risk_manager.add_open_position(trade)

                logger.info(
                    "  Recovered: {} {} — ${:.2f} @ ${:.4f} SL={} TP={}",
                    direction, symbol, trade.position_size, entry_price,
                    f"${signal.stop_loss:.4f}" if signal.stop_loss else "none",
                    f"${signal.take_profit:.4f}" if signal.take_profit else "none",
                )

            logger.info("Recovery complete: {} positions restored", len(self.open_positions))

        except Exception as e:
            logger.warning("Position recovery failed: {}", e)

    # ── Timing ─────────────────────────────────────────────

    @staticmethod
    async def _wait_for_candle_close(interval_minutes: int = 5) -> None:
        """
        Sleep until next 5-minute candle close.

        Adds a 2-second buffer for API data availability.
        """
        now = datetime.now(timezone.utc)
        minutes_past = now.minute % interval_minutes
        seconds_to_close = (
            (interval_minutes - minutes_past) * 60
            - now.second
            - now.microsecond / 1_000_000
        )

        if seconds_to_close <= 0:
            seconds_to_close += interval_minutes * 60

        wait_time = seconds_to_close + 2.0
        next_close = now.strftime("%H:%M:%S")
        logger.debug("Waiting {:.0f}s for next candle close...", wait_time)

        await asyncio.sleep(wait_time)

    # ── Heartbeat ──────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        """Send hourly heartbeat."""
        while self.is_running:
            try:
                now = time.time()
                if now - self._last_heartbeat >= self._heartbeat_interval:
                    balance = await self.client.get_balance()
                    await self.notifier.heartbeat(
                        balance, len(self.open_positions),
                    )
                    self._last_heartbeat = now

                    # Update balance tracking
                    self.risk_manager.update_balance(balance)
            except Exception as e:
                logger.error("Heartbeat error: {}", e)

            await asyncio.sleep(60)  # Check every minute
