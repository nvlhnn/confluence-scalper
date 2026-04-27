"""
Technical indicators — pure numpy calculations.

All functions are stateless: arrays in, arrays/values out.
No side effects, easily testable.
"""

from __future__ import annotations

import numpy as np


# ── EMA ────────────────────────────────────────────────────


def ema(prices: np.ndarray, period: int) -> np.ndarray:
    """
    Exponential Moving Average.

    Uses the standard smoothing factor: alpha = 2 / (period + 1).
    First value is seeded with SMA of the first ``period`` values.
    """
    if len(prices) < period:
        return np.full_like(prices, np.nan)

    alpha = 2.0 / (period + 1)
    result = np.empty_like(prices, dtype=float)
    result[:period - 1] = np.nan
    result[period - 1] = np.mean(prices[:period])

    for i in range(period, len(prices)):
        result[i] = alpha * prices[i] + (1 - alpha) * result[i - 1]

    return result


# ── RSI ────────────────────────────────────────────────────


def rsi(prices: np.ndarray, period: int = 14) -> np.ndarray:
    """
    Relative Strength Index using Wilder's smoothing.

    Returns array of RSI values (0-100). First ``period`` values are NaN.
    """
    if len(prices) < period + 1:
        return np.full_like(prices, np.nan)

    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    result = np.full(len(prices), np.nan)

    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])

    if avg_loss == 0:
        result[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        result[period] = 100.0 - (100.0 / (1.0 + rs))

    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

        if avg_loss == 0:
            result[i + 1] = 100.0
        else:
            rs = avg_gain / avg_loss
            result[i + 1] = 100.0 - (100.0 / (1.0 + rs))

    return result


# ── ADX ────────────────────────────────────────────────────


def adx(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    period: int = 14,
) -> np.ndarray:
    """
    Average Directional Index.

    Returns ADX values. Uses Wilder's smoothing for +DI, -DI, and ADX.
    """
    n = len(close)
    if n < period * 2:
        return np.full(n, np.nan)

    # True Range
    tr = np.empty(n)
    tr[0] = high[0] - low[0]
    for i in range(1, n):
        tr[i] = max(
            high[i] - low[i],
            abs(high[i] - close[i - 1]),
            abs(low[i] - close[i - 1]),
        )

    # Directional Movement
    plus_dm = np.zeros(n)
    minus_dm = np.zeros(n)
    for i in range(1, n):
        up = high[i] - high[i - 1]
        down = low[i - 1] - low[i]
        if up > down and up > 0:
            plus_dm[i] = up
        if down > up and down > 0:
            minus_dm[i] = down

    # Wilder's smoothing
    def wilder_smooth(data: np.ndarray, p: int) -> np.ndarray:
        result = np.full(len(data), np.nan)
        result[p] = np.sum(data[1:p + 1])
        for i in range(p + 1, len(data)):
            result[i] = result[i - 1] - result[i - 1] / p + data[i]
        return result

    smooth_tr = wilder_smooth(tr, period)
    smooth_plus = wilder_smooth(plus_dm, period)
    smooth_minus = wilder_smooth(minus_dm, period)

    # +DI / -DI
    plus_di = np.where(smooth_tr > 0, 100.0 * smooth_plus / smooth_tr, 0.0)
    minus_di = np.where(smooth_tr > 0, 100.0 * smooth_minus / smooth_tr, 0.0)

    # DX
    di_sum = plus_di + minus_di
    dx = np.where(di_sum > 0, 100.0 * np.abs(plus_di - minus_di) / di_sum, 0.0)

    # ADX — smoothed DX
    result = np.full(n, np.nan)
    # First ADX = average of first `period` valid DX values
    start = period * 2
    if start < n:
        valid_dx = dx[period:start]
        valid_dx = valid_dx[~np.isnan(valid_dx)]
        if len(valid_dx) > 0:
            result[start - 1] = np.mean(valid_dx)
            for i in range(start, n):
                if not np.isnan(result[i - 1]) and not np.isnan(dx[i]):
                    result[i] = (result[i - 1] * (period - 1) + dx[i]) / period

    return result


# ── ATR ────────────────────────────────────────────────────


def atr(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    period: int = 14,
) -> np.ndarray:
    """
    Average True Range using Wilder's smoothing.
    """
    n = len(close)
    if n < period + 1:
        return np.full(n, np.nan)

    tr = np.empty(n)
    tr[0] = high[0] - low[0]
    for i in range(1, n):
        tr[i] = max(
            high[i] - low[i],
            abs(high[i] - close[i - 1]),
            abs(low[i] - close[i - 1]),
        )

    result = np.full(n, np.nan)
    result[period] = np.mean(tr[1:period + 1])

    for i in range(period + 1, n):
        result[i] = (result[i - 1] * (period - 1) + tr[i]) / period

    return result


# ── VWAP ───────────────────────────────────────────────────


def vwap(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    volume: np.ndarray,
) -> np.ndarray:
    """
    Volume-Weighted Average Price.

    Calculates cumulative VWAP from the start of the data.
    For daily VWAP, pass only candles from current session.
    """
    typical = (high + low + close) / 3.0
    cum_tp_vol = np.cumsum(typical * volume)
    cum_vol = np.cumsum(volume)

    return np.where(cum_vol > 0, cum_tp_vol / cum_vol, 0.0)


# ── Bollinger Bandwidth ───────────────────────────────────


def bollinger_bandwidth(
    close: np.ndarray,
    period: int = 20,
    std_dev: float = 2.0,
) -> np.ndarray:
    """
    Bollinger Bandwidth = (Upper - Lower) / Middle.

    Returns bandwidth as a ratio. Higher = more volatile.
    """
    n = len(close)
    result = np.full(n, np.nan)

    for i in range(period - 1, n):
        window = close[i - period + 1:i + 1]
        mid = np.mean(window)
        if mid == 0:
            continue
        std = np.std(window, ddof=0)
        upper = mid + std_dev * std
        lower = mid - std_dev * std
        result[i] = (upper - lower) / mid

    return result


# ── Volume SMA ─────────────────────────────────────────────


def volume_sma(volume: np.ndarray, period: int = 20) -> np.ndarray:
    """Simple moving average of volume."""
    n = len(volume)
    result = np.full(n, np.nan)
    for i in range(period - 1, n):
        result[i] = np.mean(volume[i - period + 1:i + 1])
    return result


# ── Swing Point Detection ─────────────────────────────────


def find_swing_lows(
    prices: np.ndarray,
    order: int = 3,
) -> list[int]:
    """
    Find swing low indices.

    A swing low at index i means prices[i] is lower than
    the ``order`` bars on each side.
    """
    swings = []
    for i in range(order, len(prices) - order):
        is_low = True
        for j in range(1, order + 1):
            if prices[i] >= prices[i - j] or prices[i] >= prices[i + j]:
                is_low = False
                break
        if is_low:
            swings.append(i)
    return swings


def find_swing_highs(
    prices: np.ndarray,
    order: int = 3,
) -> list[int]:
    """
    Find swing high indices.

    A swing high at index i means prices[i] is higher than
    the ``order`` bars on each side.
    """
    swings = []
    for i in range(order, len(prices) - order):
        is_high = True
        for j in range(1, order + 1):
            if prices[i] <= prices[i - j] or prices[i] <= prices[i + j]:
                is_high = False
                break
        if is_high:
            swings.append(i)
    return swings


# ── Percentile Rank ────────────────────────────────────────


def percentile_rank(values: np.ndarray, lookback: int = 100) -> float:
    """
    Percentile rank of the last value relative to the last ``lookback`` values.

    Returns a value between 0 and 100.
    """
    if len(values) < 2:
        return 50.0

    window = values[-lookback:]
    current = values[-1]

    if np.isnan(current):
        return 50.0

    valid = window[~np.isnan(window)]
    if len(valid) == 0:
        return 50.0

    count_below = np.sum(valid < current)
    return float(count_below / len(valid) * 100.0)
