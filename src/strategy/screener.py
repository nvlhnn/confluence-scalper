"""
Dynamic coin scanner.

Scans ALL Binance USDT-M perpetual futures pairs, filters by
volume/spread/ATR, and ranks them to select the top 30 coins.
"""

from __future__ import annotations

import asyncio
from typing import Any

import numpy as np
from loguru import logger

from src.core.config import Config
from src.data.models import CoinScore
from src.exchange.binance_client import BinanceClient


class CoinScanner:
    """Dynamically scans and ranks all Binance Futures coins."""

    def __init__(self, config: Config, client: BinanceClient) -> None:
        self._cfg = config
        self._client = client
        self.active_coins: list[str] = []
        self._scores: dict[str, CoinScore] = {}

    async def scan(self) -> list[str]:
        """
        Full scan: fetch all pairs → filter → rank → select top N.

        Returns list of selected symbol names.
        """
        screening_cfg = self._cfg.screening_config
        dyn_cfg = screening_cfg.get("dynamic", {})
        filters = dyn_cfg.get("filters", {})
        ranking = dyn_cfg.get("ranking", {})
        blacklist = set(screening_cfg.get("blacklist", []))
        whitelist = set(screening_cfg.get("whitelist", []))
        max_coins = dyn_cfg.get("max_active_coins", 30)

        # ── Step 1: Fetch all available futures pairs ──
        exchange_info = await self._client.fetch_exchange_info()
        all_symbols_info = exchange_info.get("symbols", [])

        contract_type = filters.get("contract_type", "PERPETUAL")
        quote = filters.get("quote_currency", "USDT")

        all_symbols = [
            s["symbol"] for s in all_symbols_info
            if s.get("contractType") == contract_type
            and s.get("quoteAsset") == quote
            and s.get("status") == "TRADING"
            and s["symbol"] not in blacklist
        ]
        total_scanned = len(all_symbols)
        logger.info("Scanner: {} USDT-M perpetual pairs found", total_scanned)

        # ── Step 2: Fetch tickers (one batch call) ──
        tickers = await self._client.fetch_all_tickers()

        # Build reverse map: raw symbol (e.g. BTCUSDT) → ticker data
        # ccxt fetch_tickers() keys by unified symbol (e.g. BTC/USDT:USDT),
        # but our symbol list uses raw Binance format from fapiPublicGetExchangeInfo.
        ticker_by_raw: dict[str, dict] = {}
        for _unified_sym, ticker_data in tickers.items():
            raw_id = ticker_data.get("info", {}).get("symbol", "")
            if raw_id:
                ticker_by_raw[raw_id] = ticker_data

        # ── Step 3: Volume & spread filter ──
        min_volume = filters.get("min_24h_volume", 50_000_000)
        max_spread = filters.get("max_spread_pct", 0.05) / 100.0
        min_price = filters.get("min_price", 0.001)

        volume_filtered: list[dict] = []
        for symbol in all_symbols:
            ticker = ticker_by_raw.get(symbol)
            if not ticker:
                continue

            quote_vol = float(ticker.get("quoteVolume", 0) or 0)
            if quote_vol < min_volume:
                continue

            last_price = float(ticker.get("last", 0) or 0)
            if last_price < min_price:
                continue

            bid = float(ticker.get("bid", 0) or 0)
            ask = float(ticker.get("ask", 0) or 0)
            mid = (bid + ask) / 2 if bid > 0 and ask > 0 else last_price
            spread_pct = (ask - bid) / mid if mid > 0 else 999
            if spread_pct > max_spread:
                continue

            volume_filtered.append({
                "symbol": symbol,
                "volume_24h": quote_vol,
                "spread_pct": spread_pct * 100,  # Store as %
                "price": last_price,
            })

        logger.info("Scanner: {} passed volume/spread filter", len(volume_filtered))

        # ── Step 4: Calculate ATR% for filtered coins ──
        min_atr = filters.get("min_atr_pct", 0.15)
        max_atr = filters.get("max_atr_pct", 5.0)

        scored: list[CoinScore] = []

        # Fetch candles in batches to avoid rate limits
        batch_size = 10
        for i in range(0, len(volume_filtered), batch_size):
            batch = volume_filtered[i:i + batch_size]
            tasks = [
                self._fetch_atr_pct(coin["symbol"])
                for coin in batch
            ]
            atr_results = await asyncio.gather(*tasks, return_exceptions=True)

            for coin, atr_pct in zip(batch, atr_results):
                if isinstance(atr_pct, Exception):
                    continue
                if atr_pct < min_atr or atr_pct > max_atr:
                    continue

                scored.append(CoinScore(
                    symbol=coin["symbol"],
                    score=0.0,  # Calculated next
                    volume_24h=coin["volume_24h"],
                    atr_pct=atr_pct,
                    spread_pct=coin["spread_pct"],
                    price=coin["price"],
                ))

            # Small delay between batches
            if i + batch_size < len(volume_filtered):
                await asyncio.sleep(0.5)

        if not scored:
            logger.warning("Scanner: no coins passed all filters!")
            return self.active_coins  # Keep existing if scan fails

        passed_filter = len(scored)
        logger.info("Scanner: {} passed ATR filter", passed_filter)

        # ── Step 5: Normalize and score ──
        max_vol = max(c.volume_24h for c in scored)
        max_atr_val = max(c.atr_pct for c in scored)
        spreads = [c.spread_pct for c in scored]
        min_spread = min(spreads)
        max_spread_val = max(spreads)
        spread_range = max_spread_val - min_spread

        vol_w = ranking.get("volatility_weight", 0.4)
        volume_w = ranking.get("volume_weight", 0.4)
        spread_w = ranking.get("spread_weight", 0.2)

        for coin in scored:
            norm_atr = coin.atr_pct / max_atr_val if max_atr_val > 0 else 0
            norm_vol = coin.volume_24h / max_vol if max_vol > 0 else 0
            norm_spread_inv = (
                (1 - (coin.spread_pct - min_spread) / spread_range)
                if spread_range > 0 else 1
            )
            coin.score = vol_w * norm_atr + volume_w * norm_vol + spread_w * norm_spread_inv

        # ── Step 6: Sort and select ──
        scored.sort(key=lambda x: x.score, reverse=True)

        selected: list[CoinScore] = []
        selected_symbols: set[str] = set()

        # Always include whitelisted coins
        for coin in scored:
            if coin.symbol in whitelist:
                selected.append(coin)
                selected_symbols.add(coin.symbol)

        # Fill remaining from top-ranked
        for coin in scored:
            if len(selected) >= max_coins:
                break
            if coin.symbol not in selected_symbols:
                selected.append(coin)
                selected_symbols.add(coin.symbol)

        # Update state
        old_coins = set(self.active_coins)
        new_coins = [c.symbol for c in selected]
        self.active_coins = new_coins
        self._scores = {c.symbol: c for c in selected}

        added = set(new_coins) - old_coins
        removed = old_coins - set(new_coins)

        if added or removed:
            logger.info(
                "Scanner: coins rotated — added={}, removed={}",
                added or "none", removed or "none",
            )

        # Log top 5
        for i, coin in enumerate(selected[:5]):
            logger.info(
                "  #{} {} — score={:.3f} vol=${:.0f}M ATR={:.2f}% spread={:.3f}%",
                i + 1, coin.symbol, coin.score,
                coin.volume_24h / 1_000_000,
                coin.atr_pct, coin.spread_pct,
            )

        return new_coins

    def get_scores(self) -> dict[str, CoinScore]:
        """Get current coin scores."""
        return self._scores

    async def _fetch_atr_pct(self, symbol: str) -> float:
        """Fetch 5m candles and calculate ATR%."""
        candles = await self._client.fetch_candles(symbol, "5m", limit=20)
        if len(candles) < 15:
            return 0.0

        highs = np.array([c.high for c in candles])
        lows = np.array([c.low for c in candles])
        closes = np.array([c.close for c in candles])

        # Simple ATR calculation
        trs = []
        for i in range(1, len(candles)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
            trs.append(tr)

        if not trs:
            return 0.0

        atr = np.mean(trs[-14:])
        price = closes[-1]
        return float(atr / price * 100) if price > 0 else 0.0
