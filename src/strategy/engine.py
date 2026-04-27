"""
Indicator engine — calculates all indicators for a symbol.

Bridges raw candle data → IndicatorSet used by the confluence scorer.
"""

from __future__ import annotations

import numpy as np

from src.core.config import Config
from src.data.models import Candle, IndicatorSet
from src.indicators import indicators as ind


class IndicatorEngine:
    """Calculate all indicators needed for the 5-layer confluence system."""

    def __init__(self, config: Config) -> None:
        self._cfg = config

    def calculate(
        self,
        symbol: str,
        candles_5m: list[Candle],
        candles_15m: list[Candle],
        candles_1h: list[Candle],
    ) -> tuple[IndicatorSet, np.ndarray, np.ndarray]:
        """
        Calculate all indicators for one symbol.

        Returns:
            Tuple of (IndicatorSet, rsi_history, price_history)
            rsi_history and price_history are the raw arrays for
            divergence detection in the confluence scorer.
        """
        # Extract arrays from candles
        close_5m = np.array([c.close for c in candles_5m])
        high_5m = np.array([c.high for c in candles_5m])
        low_5m = np.array([c.low for c in candles_5m])
        volume_5m = np.array([c.volume for c in candles_5m])

        close_15m = np.array([c.close for c in candles_15m])
        high_15m = np.array([c.high for c in candles_15m])
        low_15m = np.array([c.low for c in candles_15m])

        close_1h = np.array([c.close for c in candles_1h])

        current_price = close_5m[-1] if len(close_5m) > 0 else 0.0

        # ── Layer 1: Regime indicators (15m) ──
        regime_cfg = self._cfg.regime_config
        adx_period = regime_cfg.get("adx_period", 14)
        bb_period = regime_cfg.get("bb_period", 20)
        bb_std = regime_cfg.get("bb_std_dev", 2.0)
        bb_lookback = regime_cfg.get("bb_width_lookback", 100)

        adx_vals = ind.adx(high_15m, low_15m, close_15m, adx_period)
        adx_current = self._last_valid(adx_vals)

        bb_width = ind.bollinger_bandwidth(close_15m, bb_period, bb_std)
        bb_pct = ind.percentile_rank(bb_width, bb_lookback)

        # ── Layer 2: Trend indicators ──
        trend_cfg = self._cfg.trend_config
        ema_slow = trend_cfg.get("ema_slow_period", 200)
        ema_fast = trend_cfg.get("ema_fast_period", 50)

        ema_200_1h_arr = ind.ema(close_1h, ema_slow)
        ema_200_1h = self._last_valid(ema_200_1h_arr)

        ema_50_15m_arr = ind.ema(close_15m, ema_fast)
        ema_50_15m = self._last_valid(ema_50_15m_arr)

        # ── Layer 3: RSI (5m) ──
        div_cfg = self._cfg.divergence_config
        rsi_period = div_cfg.get("rsi_period", 7)

        rsi_vals = ind.rsi(close_5m, rsi_period)
        rsi_current = self._last_valid(rsi_vals)

        # ── Layer 4: Levels (5m) ──
        ema_50_5m_arr = ind.ema(close_5m, 50)
        ema_50_5m = self._last_valid(ema_50_5m_arr)

        vwap_vals = ind.vwap(high_5m, low_5m, close_5m, volume_5m)
        vwap_current = self._last_valid(vwap_vals)

        # Previous session high/low (use 1h candles, last 24 bars = ~1 day)
        if len(candles_1h) >= 24:
            prev_highs = high_5m[-288:] if len(high_5m) >= 288 else high_5m  # ~24h of 5m
            prev_lows = low_5m[-288:] if len(low_5m) >= 288 else low_5m
            prev_high = float(np.max(prev_highs[:-1])) if len(prev_highs) > 1 else 0.0
            prev_low = float(np.min(prev_lows[:-1])) if len(prev_lows) > 1 else 0.0
        else:
            prev_high = 0.0
            prev_low = 0.0

        # ── Layer 5: Volume (5m) ──
        vol_cfg = self._cfg.volume_config
        vol_sma_period = vol_cfg.get("sma_period", 20)

        vol_sma = ind.volume_sma(volume_5m, vol_sma_period)
        vol_sma_current = self._last_valid(vol_sma)
        current_vol = volume_5m[-1] if len(volume_5m) > 0 else 0.0
        vol_ratio = current_vol / vol_sma_current if vol_sma_current > 0 else 0.0

        # ── Layer 5b: Candle patterns (5m) ──
        candle_pattern = self._detect_candle_pattern(candles_5m)

        # ── Execution: ATR (5m) ──
        exec_cfg = self._cfg.execution_config
        atr_period = exec_cfg.get("stop_loss", {}).get("atr_period", 14)
        atr_vals = ind.atr(high_5m, low_5m, close_5m, atr_period)
        atr_current = self._last_valid(atr_vals)

        # Build result
        indicator_set = IndicatorSet(
            symbol=symbol,
            # Layer 1
            adx=adx_current,
            bb_width=self._last_valid(bb_width),
            bb_width_percentile=bb_pct,
            # Layer 2
            ema_200_1h=ema_200_1h,
            ema_50_15m=ema_50_15m,
            # Layer 3
            rsi=rsi_current,
            # Layer 4
            vwap=vwap_current,
            ema_50_5m=ema_50_5m,
            prev_session_high=prev_high,
            prev_session_low=prev_low,
            # Layer 5
            current_volume=current_vol,
            volume_sma_20=vol_sma_current,
            volume_ratio=vol_ratio,
            # Layer 5b
            candle_pattern=candle_pattern,
            # Execution
            atr=atr_current,
            current_price=current_price,
        )

        return indicator_set, rsi_vals, close_5m

    def _detect_candle_pattern(self, candles: list[Candle]) -> str:
        """Detect reversal candlestick pattern from last 2 candles."""
        if len(candles) < 2:
            return "NONE"

        prev = candles[-2]
        curr = candles[-1]

        curr_body = abs(curr.close - curr.open)
        prev_body = abs(prev.close - prev.open)
        curr_range = curr.high - curr.low

        if curr_range == 0 or curr_body == 0:
            return "NONE"

        # ── Bullish patterns ──
        # Bullish engulfing
        if (prev.close < prev.open and curr.close > curr.open
                and curr_body > prev_body
                and curr.close > prev.open and curr.open < prev.close):
            return "BULLISH_ENGULFING"

        # Hammer
        lower_wick = (min(curr.open, curr.close) - curr.low)
        upper_wick = (curr.high - max(curr.open, curr.close))
        if (curr.close > curr.open
                and lower_wick > curr_body * 2
                and upper_wick < curr_body * 0.3):
            return "HAMMER"

        # ── Bearish patterns ──
        # Bearish engulfing
        if (prev.close > prev.open and curr.close < curr.open
                and curr_body > prev_body
                and curr.open > prev.close and curr.close < prev.open):
            return "BEARISH_ENGULFING"

        # Shooting star
        if (curr.close < curr.open
                and upper_wick > curr_body * 2
                and lower_wick < curr_body * 0.3):
            return "SHOOTING_STAR"

        return "NONE"

    @staticmethod
    def _last_valid(arr: np.ndarray) -> float:
        """Get the last non-NaN value from an array."""
        if len(arr) == 0:
            return 0.0
        valid = arr[~np.isnan(arr)]
        return float(valid[-1]) if len(valid) > 0 else 0.0
