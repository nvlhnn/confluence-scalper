"""
Market regime classifier.

Layer 1 of the 5-layer confluence system.
Determines if market is trending, ranging, squeezing, volatile, or dead.
"""

from __future__ import annotations

from src.core.config import Config


def classify_regime(
    adx_value: float,
    bb_width_percentile: float,
    config: dict | None = None,
) -> tuple[str, int]:
    """
    Classify market regime and return regime name + score.

    Args:
        adx_value: Current ADX(14) on 15m chart.
        bb_width_percentile: Percentile rank of BB width over last 100 bars.
        config: Optional regime config overrides.

    Returns:
        Tuple of (regime_name, score).
        Regime names: TRENDING, RANGING, SQUEEZE, VOLATILE, DEAD.
        Scores: -1 to 2.
    """
    cfg = config or {}
    dead_adx = cfg.get("dead_adx", 15)
    ranging_adx = cfg.get("ranging_adx", 20)
    trending_adx = cfg.get("trending_adx", 25)
    volatile_adx = cfg.get("volatile_adx", 30)
    squeeze_bb = cfg.get("squeeze_bb_percentile", 20)
    volatile_bb = cfg.get("volatile_bb_percentile", 80)

    # DEAD — no movement at all
    if adx_value < dead_adx and bb_width_percentile < 10:
        return "DEAD", -1

    # SQUEEZE — low ADX + tight BBs → breakout building
    if adx_value < ranging_adx and bb_width_percentile < squeeze_bb:
        return "SQUEEZE", 0

    # VOLATILE — high ADX + wide BBs → dangerous
    if adx_value > volatile_adx and bb_width_percentile > volatile_bb:
        return "VOLATILE", 1

    # TRENDING — strong directional movement
    if adx_value >= trending_adx:
        return "TRENDING", 2

    # RANGING — low ADX, normal BBs
    return "RANGING", 1
