"""
5-layer confluence scorer.

Evaluates all 5 layers for a single coin and produces a
confluence score (0-13) with quality tier.
"""

from __future__ import annotations

import numpy as np

from src.core.config import Config
from src.data.models import IndicatorSet, Signal
from src.indicators.indicators import find_swing_highs, find_swing_lows
from src.strategy.regime import classify_regime


class ConfluenceScorer:
    """Score a trading setup through the 5-layer confluence system."""

    def __init__(self, config: Config) -> None:
        self._cfg = config

    def evaluate(
        self,
        ind: IndicatorSet,
        rsi_history: np.ndarray,
        price_history: np.ndarray,
    ) -> Signal | None:
        """
        Run all 5 layers and produce a Signal if score >= minimum.

        Args:
            ind: Pre-calculated indicator values for one symbol.
            rsi_history: Recent RSI(7) values on 5m (last 30+).
            price_history: Recent close prices on 5m (last 30+).

        Returns:
            Signal if confluence score meets threshold, else None.
        """
        # ── Layer 1: Regime ──
        regime_name, regime_score = self._score_regime(ind)

        # Early exit — don't trade dead markets
        if regime_name == "DEAD":
            return None

        # ── Layer 2: Trend Bias ──
        trend_bias, trend_score = self._score_trend(ind)

        # Early exit — conflicted trend
        if trend_bias == "NONE":
            return None

        # ── Layer 3: RSI Divergence ──
        div_type, div_score, div_strength = self._score_divergence(
            trend_bias, rsi_history, price_history, ind.rsi,
        )

        # Divergence is a strong bonus, not a hard gate.
        # Signals without divergence can still pass if other layers are strong.

        # ── Layer 4: Level Confluence ──
        level_score, near_levels = self._score_levels(ind)

        # ── Layer 5: Volume ──
        volume_score = self._score_volume(ind)

        # ── Layer 5b: Candle Pattern ──
        candle_score = 1 if ind.candle_pattern != "NONE" else 0

        # ── Total Score ──
        total = regime_score + trend_score + div_score + level_score + volume_score + candle_score

        # Hard requirements — only the essentials gate signals
        scoring_cfg = self._cfg.scoring_config
        min_score = scoring_cfg.get("min_score", 7)

        if trend_score < 2:
            return self._rejected(ind, total, "No trend alignment", regime_name)
        if regime_score < 1:
            return self._rejected(ind, total, "Bad regime", regime_name)
        if level_score < 1:
            return self._rejected(ind, total, "No level confluence", regime_name)
        if volume_score < 1:
            return self._rejected(ind, total, "No volume confirmation", regime_name)
        if total < min_score:
            return self._rejected(ind, total, f"Score {total} < {min_score}", regime_name)

        # Determine quality tier
        quality, size_mult = self._quality_tier(total)

        # Calculate entry/SL/TP
        entry, sl, tp = self._calculate_levels(ind, trend_bias)

        return Signal(
            symbol=ind.symbol,
            direction=trend_bias,
            entry_price=entry,
            stop_loss=sl,
            take_profit=tp,
            confluence_score=total,
            quality=quality,
            regime=regime_name,
            timestamp=0,  # Set by caller
            size_multiplier=size_mult,
            metadata={
                "layer_scores": {
                    "regime": regime_score,
                    "trend": trend_score,
                    "divergence": div_score,
                    "level": level_score,
                    "volume": volume_score,
                    "candle": candle_score,
                },
                "regime_name": regime_name,
                "trend_bias": trend_bias,
                "divergence_type": div_type,
                "divergence_strength": div_strength,
                "near_levels": near_levels,
                "volume_ratio": ind.volume_ratio,
                "candle_pattern": ind.candle_pattern,
                "rsi": ind.rsi,
                "adx": ind.adx,
                "atr": ind.atr,
            },
        )

    # ── Layer Scorers ──────────────────────────────────────

    def _score_regime(self, ind: IndicatorSet) -> tuple[str, int]:
        """Layer 1: Regime detection."""
        cfg = self._cfg.regime_config.get("thresholds", {})
        return classify_regime(ind.adx, ind.bb_width_percentile, cfg)

    def _score_trend(self, ind: IndicatorSet) -> tuple[str, int]:
        """Layer 2: Trend direction from multi-TF EMAs."""
        price = ind.current_price
        above_1h = price > ind.ema_200_1h
        above_15m = price > ind.ema_50_15m

        if above_1h and above_15m:
            return "LONG", 2
        elif not above_1h and not above_15m:
            return "SHORT", 2
        else:
            return "NONE", 0

    def _score_divergence(
        self,
        bias: str,
        rsi_history: np.ndarray,
        price_history: np.ndarray,
        current_rsi: float,
    ) -> tuple[str, int, float]:
        """Layer 3: RSI divergence detection."""
        cfg = self._cfg.divergence_config
        oversold = cfg.get("oversold_threshold", 25)
        overbought = cfg.get("overbought_threshold", 75)
        lookback = cfg.get("swing_lookback", 14)
        min_dist = cfg.get("min_swing_distance", 3)

        if len(rsi_history) < lookback or len(price_history) < lookback:
            return "NONE", 0, 0.0

        if bias == "LONG":
            swings = find_swing_lows(price_history, order=min_dist)
            if len(swings) < 2:
                return "NONE", 0, 0.0

            prev_idx, curr_idx = swings[-2], swings[-1]

            price_lower = price_history[curr_idx] < price_history[prev_idx]
            rsi_higher = rsi_history[curr_idx] > rsi_history[prev_idx]
            rsi_extreme = min(rsi_history[prev_idx], rsi_history[curr_idx]) < oversold

            if price_lower and rsi_higher and rsi_extreme:
                strength = abs(rsi_history[curr_idx] - rsi_history[prev_idx])
                score = 3 if strength > 10 else 2
                return "BULLISH_DIVERGENCE", score, strength

        elif bias == "SHORT":
            swings = find_swing_highs(price_history, order=min_dist)
            if len(swings) < 2:
                return "NONE", 0, 0.0

            prev_idx, curr_idx = swings[-2], swings[-1]

            price_higher = price_history[curr_idx] > price_history[prev_idx]
            rsi_lower = rsi_history[curr_idx] < rsi_history[prev_idx]
            rsi_extreme = max(rsi_history[prev_idx], rsi_history[curr_idx]) > overbought

            if price_higher and rsi_lower and rsi_extreme:
                strength = abs(rsi_history[prev_idx] - rsi_history[curr_idx])
                score = 3 if strength > 10 else 2
                return "BEARISH_DIVERGENCE", score, strength

        return "NONE", 0, 0.0

    def _score_levels(self, ind: IndicatorSet) -> tuple[int, list[dict]]:
        """Layer 4: Price proximity to key levels."""
        cfg = self._cfg.levels_config
        proximity = cfg.get("proximity_pct", 0.15) / 100.0  # Convert to ratio

        price = ind.current_price
        if price <= 0:
            return 0, []

        levels = {}
        if cfg.get("vwap_enabled", True) and ind.vwap > 0:
            levels["vwap"] = ind.vwap
        if cfg.get("ema_50_5m_enabled", True) and ind.ema_50_5m > 0:
            levels["ema_50_5m"] = ind.ema_50_5m
        if cfg.get("prev_session_levels", True):
            if ind.prev_session_high > 0:
                levels["prev_high"] = ind.prev_session_high
            if ind.prev_session_low > 0:
                levels["prev_low"] = ind.prev_session_low

        near = []
        for name, level in levels.items():
            dist = abs(price - level) / price
            if dist <= proximity:
                near.append({"name": name, "level": level, "distance_pct": dist * 100})

        score = min(len(near), 3)  # Cap at 3
        return score, near

    def _score_volume(self, ind: IndicatorSet) -> int:
        """Layer 5: Volume spike confirmation."""
        cfg = self._cfg.volume_config
        spike = cfg.get("spike_multiplier", 1.5)
        strong = cfg.get("strong_spike_multiplier", 2.0)

        ratio = ind.volume_ratio
        if ratio >= strong:
            return 2
        elif ratio >= spike:
            return 1
        return 0

    # ── Helpers ────────────────────────────────────────────

    def _quality_tier(self, score: int) -> tuple[str, float]:
        """Map total score to quality tier and size multiplier."""
        cfg = self._cfg.scoring_config
        if score >= cfg.get("a_plus_threshold", 11):
            return "A_PLUS", 1.0
        elif score >= cfg.get("a_threshold", 9):
            return "A", 1.0
        elif score >= cfg.get("b_threshold", 8):
            return "B", cfg.get("b_size_multiplier", 0.75)
        return "REJECTED", 0.0

    def _calculate_levels(
        self, ind: IndicatorSet, direction: str,
    ) -> tuple[float, float, float]:
        """Calculate entry price, stop loss, and take profit."""
        cfg = self._cfg.execution_config
        price = ind.current_price
        atr_val = ind.atr

        # Stop loss distance
        sl_cfg = cfg.get("stop_loss", {})
        atr_mult = sl_cfg.get("atr_multiplier", 1.5)
        max_stop = sl_cfg.get("max_stop_pct", 2.0) / 100.0
        min_stop = sl_cfg.get("min_stop_pct", 0.2) / 100.0

        stop_dist = atr_val * atr_mult
        stop_pct = stop_dist / price if price > 0 else 0

        # Clamp stop distance
        stop_pct = max(min_stop, min(max_stop, stop_pct))
        stop_dist = price * stop_pct

        # R:R ratio
        rr = cfg.get("take_profit", {}).get("rr_ratio", 2.0)

        # Entry buffer
        buffer_pct = cfg.get("limit_price_buffer_pct", 0.02) / 100.0

        if direction == "LONG":
            entry = price * (1 - buffer_pct)
            sl = entry - stop_dist
            tp = entry + stop_dist * rr
        else:
            entry = price * (1 + buffer_pct)
            sl = entry + stop_dist
            tp = entry - stop_dist * rr

        return round(entry, 8), round(sl, 8), round(tp, 8)

    def _rejected(
        self, ind: IndicatorSet, score: int, reason: str, regime: str,
    ) -> Signal | None:
        """
        Return None — signal is rejected.
        
        The caller (signal generator) will log rejected signals
        separately via database.log_signal().
        """
        return None
