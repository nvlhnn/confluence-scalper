"""
Candle data cache with smart multi-timeframe refresh.

Minimizes API calls by only fetching higher timeframes
when their candles actually close.
"""

from __future__ import annotations

from loguru import logger

from src.data.models import Candle
from src.exchange.binance_client import BinanceClient


class CandleCache:
    """
    Smart candle cache for multi-timeframe data.

    5m candles:  Fetched every cycle (primary timeframe)
    15m candles: Fetched every 3rd cycle (15 min / 5 min = 3)
    1H candles:  Fetched every 12th cycle (60 min / 5 min = 12)

    Between refreshes, cached data is used — the higher-TF candle
    hasn't closed yet, so indicator values haven't changed.
    """

    def __init__(self) -> None:
        self._cache: dict[str, dict[str, list[Candle]]] = {}
        self._cycle_count = 0

    @property
    def cycle_count(self) -> int:
        return self._cycle_count

    async def update(
        self,
        symbols: list[str],
        client: BinanceClient,
    ) -> dict[str, dict[str, list[Candle]]]:
        """
        Fetch candle data with smart caching.

        Returns dict: {symbol: {'5m': [...], '15m': [...], '1h': [...]}}
        """
        self._cycle_count += 1

        fetch_15m = self._cycle_count % 3 == 0 or self._cycle_count == 1
        fetch_1h = self._cycle_count % 12 == 0 or self._cycle_count == 1

        for symbol in symbols:
            if symbol not in self._cache:
                # New symbol — fetch all timeframes
                self._cache[symbol] = {"5m": [], "15m": [], "1h": []}
                fetch_15m_sym = True
                fetch_1h_sym = True
            else:
                fetch_15m_sym = fetch_15m
                fetch_1h_sym = fetch_1h

            # Always fetch 5m
            candles_5m = await client.fetch_candles(symbol, "5m", limit=200)
            if candles_5m:
                self._cache[symbol]["5m"] = candles_5m

            # 15m — every 3rd cycle or new symbol
            if fetch_15m_sym:
                candles_15m = await client.fetch_candles(symbol, "15m", limit=100)
                if candles_15m:
                    self._cache[symbol]["15m"] = candles_15m

            # 1H — every 12th cycle or new symbol
            if fetch_1h_sym:
                candles_1h = await client.fetch_candles(symbol, "1h", limit=200)
                if candles_1h:
                    self._cache[symbol]["1h"] = candles_1h

        # Remove symbols no longer in active list
        stale = set(self._cache.keys()) - set(symbols)
        for s in stale:
            del self._cache[s]

        if self._cycle_count % 12 == 0:
            logger.debug(
                "Cache: full refresh (cycle {}), {} symbols",
                self._cycle_count, len(symbols),
            )

        return self._cache

    def get(self, symbol: str) -> dict[str, list[Candle]]:
        """Get cached candle data for a symbol."""
        return self._cache.get(symbol, {"5m": [], "15m": [], "1h": []})

    def clear(self) -> None:
        """Clear all cached data."""
        self._cache.clear()
        self._cycle_count = 0
