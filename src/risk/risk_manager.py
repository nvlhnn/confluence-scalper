"""
Risk manager — validates trades against all risk rules.

Enforces position sizing, circuit breakers, correlation guards,
and daily limits before any trade is executed.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from loguru import logger

from src.core.config import Config
from src.data.models import Signal, Trade
from src.database.db import Database


class RiskManager:
    """Validates trades against all risk rules and sizes positions."""

    def __init__(self, config: Config, db: Database) -> None:
        self._cfg = config
        self._db = db

        # Circuit breaker state
        self._daily_loss_triggered = False
        self._drawdown_triggered = False
        self._consecutive_losses = 0
        self._cooldown_until: datetime | None = None
        self._peak_balance = 0.0
        self._day_start_balance = 0.0

        # Track open positions locally
        self._open_positions: list[Trade] = []

    # ── Public API ─────────────────────────────────────────

    def initialize(self, balance: float) -> None:
        """Set initial balance state."""
        self._peak_balance = balance
        self._day_start_balance = balance
        logger.info("Risk manager initialized — balance: ${:.2f}", balance)

    def validate(self, signal: Signal) -> tuple[bool, str]:
        """
        Validate a signal against all risk rules.

        Returns (approved, reason).
        """
        # 1. Circuit breakers
        ok, reason = self._check_circuit_breakers()
        if not ok:
            return False, reason

        # 2. Position limits
        ok, reason = self._check_position_limits()
        if not ok:
            return False, reason

        # 3. Daily trade limit
        trade_count = self._db.get_trade_count_today()
        if trade_count >= self._cfg.max_daily_trades:
            return False, f"Daily trade limit ({self._cfg.max_daily_trades}) reached"

        # 4. Duplicate symbol check
        for pos in self._open_positions:
            if pos.signal and pos.signal.symbol == signal.symbol:
                return False, f"Already have position in {signal.symbol}"

        # 5. Correlation check
        ok, reason = self._check_correlation(signal)
        if not ok:
            return False, reason

        return True, "OK"

    def calculate_position_size(
        self,
        balance: float,
        signal: Signal,
        atr: float,
    ) -> dict:
        """
        Calculate position size from risk parameters.

        The position size is derived from how much we're willing to LOSE,
        not from available margin.
        """
        risk_pct = self._cfg.risk_per_trade_pct
        max_margin_pct = self._cfg.max_margin_pct

        # Apply signal quality multiplier
        quality_mult = signal.size_multiplier

        # Apply regime multiplier
        regime_mult = {
            "TRENDING": 1.0,
            "RANGING": 1.0,
            "VOLATILE": 0.5,
            "SQUEEZE": 0.75,
        }.get(signal.regime, 1.0)

        # Risk amount
        risk_amount = balance * risk_pct * quality_mult * regime_mult

        # Stop distance
        stop_dist_pct = abs(signal.entry_price - signal.stop_loss) / signal.entry_price
        if stop_dist_pct == 0:
            return {"position_size": 0, "margin_required": 0, "risk_amount": 0,
                    "leverage": self._cfg.base_leverage}

        # Position size
        position_size = risk_amount / stop_dist_pct

        # Dynamic leverage
        leverage = self._calculate_leverage(signal.regime, atr, signal.entry_price)

        # Margin required
        margin_required = position_size / leverage

        # Cap margin
        max_margin = balance * max_margin_pct
        if margin_required > max_margin:
            margin_required = max_margin
            position_size = margin_required * leverage
            risk_amount = position_size * stop_dist_pct

        return {
            "position_size": round(position_size, 2),
            "margin_required": round(margin_required, 2),
            "risk_amount": round(risk_amount, 4),
            "risk_pct_actual": round((risk_amount / balance) * 100, 2) if balance > 0 else 0,
            "leverage": leverage,
            "stop_distance_pct": round(stop_dist_pct * 100, 3),
        }

    def record_trade_result(self, trade: Trade) -> None:
        """Update circuit breaker state after a trade closes."""
        if trade.net_pnl > 0:
            self._consecutive_losses = 0
        else:
            self._consecutive_losses += 1
            max_consec = self._cfg.risk_config.get(
                "circuit_breakers", {}
            ).get("consecutive_losses", {}).get("max_consecutive", 4)
            if self._consecutive_losses >= max_consec:
                cooldown = self._cfg.risk_config.get(
                    "circuit_breakers", {}
                ).get("consecutive_losses", {}).get("cooldown_minutes", 120)
                self._cooldown_until = datetime.now(timezone.utc) + timedelta(minutes=cooldown)
                logger.warning(
                    "Consecutive loss breaker: {} losses, cooldown {}m",
                    self._consecutive_losses, cooldown,
                )

    def update_balance(self, balance: float) -> None:
        """Update peak balance and check drawdown."""
        if balance > self._peak_balance:
            self._peak_balance = balance

        if self._peak_balance > 0:
            drawdown_pct = (self._peak_balance - balance) / self._peak_balance * 100
            max_dd = self._cfg.risk_config.get(
                "circuit_breakers", {}
            ).get("drawdown", {}).get("max_drawdown_pct", 20.0)
            if drawdown_pct >= max_dd:
                self._drawdown_triggered = True
                logger.critical(
                    "DRAWDOWN BREAKER: {:.1f}% drawdown (max {:.1f}%)",
                    drawdown_pct, max_dd,
                )

    def add_open_position(self, trade: Trade) -> None:
        """Track a newly opened position."""
        self._open_positions.append(trade)

    def remove_open_position(self, trade_id: str) -> None:
        """Remove a closed position from tracking."""
        self._open_positions = [p for p in self._open_positions if p.id != trade_id]

    def reset_daily(self, balance: float) -> None:
        """Reset daily limits at UTC midnight."""
        self._daily_loss_triggered = False
        self._day_start_balance = balance
        logger.info("Daily risk reset — starting balance: ${:.2f}", balance)

    # ── Internal Checks ────────────────────────────────────

    def _check_circuit_breakers(self) -> tuple[bool, str]:
        """Check all circuit breakers."""
        # Daily loss
        cb_cfg = self._cfg.risk_config.get("circuit_breakers", {})

        if cb_cfg.get("daily_loss", {}).get("enabled", True):
            daily_pnl = self._db.get_daily_realized_pnl()
            max_loss = cb_cfg["daily_loss"].get("max_loss_pct", 6.0) / 100.0
            if daily_pnl < 0 and abs(daily_pnl) >= self._day_start_balance * max_loss:
                return False, f"Daily loss limit: ${abs(daily_pnl):.2f}"

        # Drawdown
        if self._drawdown_triggered:
            return False, "Max drawdown exceeded — manual review required"

        # Consecutive losses
        if self._cooldown_until:
            if datetime.now(timezone.utc) < self._cooldown_until:
                remaining = int((self._cooldown_until - datetime.now(timezone.utc)).total_seconds() / 60)
                return False, f"Consecutive loss cooldown ({remaining}m remaining)"
            else:
                self._cooldown_until = None
                self._consecutive_losses = 0

        return True, "OK"

    def _check_position_limits(self) -> tuple[bool, str]:
        """Check position count limits."""
        max_pos = self._cfg.max_open_positions
        if len(self._open_positions) >= max_pos:
            return False, f"Max positions ({max_pos}) reached"
        return True, "OK"

    def _check_correlation(self, signal: Signal) -> tuple[bool, str]:
        """Check correlation rules."""
        corr_cfg = self._cfg.risk_config.get("correlation", {})
        if not corr_cfg.get("enabled", True):
            return True, "OK"

        groups = corr_cfg.get("groups", {})

        # Find signal's group
        signal_group = None
        for group_name, symbols in groups.items():
            if signal.symbol in symbols:
                signal_group = group_name
                break

        # Check existing positions
        same_dir_count = 0
        for pos in self._open_positions:
            if not pos.signal:
                continue

            # Same correlation group + same direction
            if signal_group:
                for group_name, symbols in groups.items():
                    if (group_name == signal_group
                            and pos.signal.symbol in symbols
                            and pos.signal.direction == signal.direction):
                        return False, f"Correlated position in {group_name}"

            # Count same direction
            if pos.signal.direction == signal.direction:
                same_dir_count += 1

        max_same = self._cfg.risk_config.get("limits", {}).get("max_same_direction", 2)
        if same_dir_count >= max_same:
            return False, f"Max same-direction ({max_same}) reached"

        return True, "OK"

    def _calculate_leverage(
        self, regime: str, atr: float, price: float,
    ) -> int:
        """Calculate dynamic leverage based on regime and volatility."""
        if not self._cfg.risk_config.get("leverage", {}).get("dynamic", True):
            return self._cfg.base_leverage

        base = self._cfg.base_leverage
        min_lev = self._cfg.min_leverage
        max_lev = self._cfg.max_leverage

        # Regime multiplier
        regime_mult = {
            "TRENDING": 1.0,
            "RANGING": 1.2,
            "SQUEEZE": 0.8,
            "VOLATILE": 0.5,
            "DEAD": 0.0,
        }.get(regime, 1.0)

        # Volatility multiplier (ATR% inverse)
        atr_pct = (atr / price * 100) if price > 0 else 0.5
        if atr_pct > 0.8:
            vol_mult = 0.5
        elif atr_pct > 0.5:
            vol_mult = 0.75
        elif atr_pct > 0.3:
            vol_mult = 1.0
        else:
            vol_mult = 1.25

        leverage = int(base * regime_mult * vol_mult)
        return max(min_lev, min(max_lev, leverage))
