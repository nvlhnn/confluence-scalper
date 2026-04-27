"""
Binance Futures API client.

Wraps ccxt for async exchange operations: fetching candles,
placing orders, managing positions, and querying account state.
"""

from __future__ import annotations

import asyncio
from typing import Any

import ccxt.async_support as ccxt
from loguru import logger

from src.core.config import Config
from src.data.models import Candle


class BinanceClient:
    """Async Binance Futures (USDT-M) client via ccxt."""

    def __init__(self, config: Config) -> None:
        self._cfg = config
        self._exchange: ccxt.binanceusdm | None = None
        self._exchange_info: dict | None = None  # Cached
        self._raw_to_unified: dict[str, str] = {}  # Raw symbol → unified symbol map

    # ── Connection ─────────────────────────────────────────

    async def connect(self) -> None:
        """Initialize exchange connection."""
        opts: dict[str, Any] = {
            "apiKey": self._cfg.binance_api_key,
            "secret": self._cfg.binance_api_secret,
            "enableRateLimit": True,
            "options": {
                "defaultType": "future",
                "adjustForTimeDifference": True,
            },
        }

        if self._cfg.is_testnet:
            opts["sandbox"] = True

        self._exchange = ccxt.binanceusdm(opts)

        if self._cfg.is_testnet:
            self._exchange.set_sandbox_mode(True)

        # Test connectivity
        await self._exchange.load_markets()
        balance = await self.get_balance()
        mode = "TESTNET" if self._cfg.is_testnet else "LIVE"
        logger.info("Binance {} connected — balance: ${:.2f}", mode, balance)

        # Build raw → unified symbol map for screener use
        self._build_symbol_map()

    def _build_symbol_map(self) -> None:
        """Build mapping from raw Binance symbol (e.g. BTCUSDT) to ccxt unified (e.g. BTC/USDT:USDT)."""
        if not self._exchange:
            return
        self._raw_to_unified = {}
        for unified, market in self._exchange.markets.items():
            raw_id = market.get("id", "")
            if raw_id:
                self._raw_to_unified[raw_id] = unified

    def raw_to_unified(self, raw_symbol: str) -> str | None:
        """Convert a raw Binance symbol to ccxt unified symbol."""
        return self._raw_to_unified.get(raw_symbol)

    async def close(self) -> None:
        """Close exchange connection."""
        if self._exchange:
            await self._exchange.close()
            self._exchange = None

    # ── Market Data ────────────────────────────────────────

    async def fetch_candles(
        self,
        symbol: str,
        interval: str = "5m",
        limit: int = 200,
    ) -> list[Candle]:
        """Fetch OHLCV candles for a symbol."""
        assert self._exchange is not None
        try:
            ohlcv = await self._exchange.fetch_ohlcv(
                symbol, timeframe=interval, limit=limit,
            )
            return [
                Candle(
                    timestamp=int(bar[0]),
                    open=float(bar[1]),
                    high=float(bar[2]),
                    low=float(bar[3]),
                    close=float(bar[4]),
                    volume=float(bar[5]),
                    symbol=symbol,
                    interval=interval,
                )
                for bar in ohlcv
            ]
        except Exception as e:
            logger.error("Failed to fetch candles for {}: {}", symbol, e)
            return []

    async def fetch_exchange_info(self) -> dict:
        """
        Get exchange info (all symbols, contract types, etc.).
        Cached after first call — rarely changes.
        """
        if self._exchange_info is not None:
            return self._exchange_info

        assert self._exchange is not None
        self._exchange_info = await self._exchange.fapiPublicGetExchangeInfo()
        return self._exchange_info

    async def fetch_all_tickers(self) -> dict[str, dict]:
        """
        Fetch 24hr ticker data for ALL symbols in one call.
        Returns dict keyed by symbol.
        """
        assert self._exchange is not None
        tickers = await self._exchange.fetch_tickers()
        return tickers

    async def fetch_ticker(self, symbol: str) -> dict:
        """Fetch single symbol ticker."""
        assert self._exchange is not None
        return await self._exchange.fetch_ticker(symbol)

    # ── Account ────────────────────────────────────────────

    async def get_balance(self) -> float:
        """Get available USDT balance."""
        assert self._exchange is not None
        balance = await self._exchange.fetch_balance()
        return float(balance.get("USDT", {}).get("free", 0))

    async def get_total_balance(self) -> float:
        """Get total USDT balance (including margin)."""
        assert self._exchange is not None
        balance = await self._exchange.fetch_balance()
        return float(balance.get("USDT", {}).get("total", 0))

    async def get_positions(self) -> list[dict]:
        """Get all open positions."""
        assert self._exchange is not None
        positions = await self._exchange.fetch_positions()
        return [p for p in positions if float(p.get("contracts", 0)) > 0]

    async def get_open_orders(self, symbol: str | None = None) -> list[dict]:
        """Get open orders, optionally filtered by symbol."""
        assert self._exchange is not None
        if symbol:
            return await self._exchange.fetch_open_orders(symbol)
        return await self._exchange.fetch_open_orders()

    # ── Trading ────────────────────────────────────────────

    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """Set leverage for a symbol."""
        assert self._exchange is not None
        try:
            await self._exchange.set_leverage(leverage, symbol)
            logger.debug("Leverage set: {} = {}x", symbol, leverage)
            return True
        except Exception as e:
            logger.error("Failed to set leverage for {}: {}", symbol, e)
            return False

    async def set_margin_type(self, symbol: str, margin_type: str = "ISOLATED") -> bool:
        """Set margin type (ISOLATED or CROSSED)."""
        assert self._exchange is not None
        try:
            await self._exchange.set_margin_mode(margin_type.lower(), symbol)
            return True
        except Exception as e:
            # Binance returns error if already set — that's OK
            if "No need to change" in str(e):
                return True
            logger.error("Failed to set margin type for {}: {}", symbol, e)
            return False

    async def place_limit_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        price: float,
    ) -> str:
        """Place a limit order. Returns order ID."""
        assert self._exchange is not None
        order = await self._exchange.create_order(
            symbol=symbol,
            type="limit",
            side=side.lower(),
            amount=amount,
            price=price,
        )
        logger.info(
            "Limit order placed: {} {} {} @ {} — id={}",
            side, amount, symbol, price, order["id"],
        )
        return str(order["id"])

    async def place_market_order(
        self,
        symbol: str,
        side: str,
        amount: float,
    ) -> str:
        """Place a market order. Returns order ID."""
        assert self._exchange is not None
        order = await self._exchange.create_order(
            symbol=symbol,
            type="market",
            side=side.lower(),
            amount=amount,
        )
        logger.info(
            "Market order placed: {} {} {} — id={}",
            side, amount, symbol, order["id"],
        )
        return str(order["id"])

    async def place_stop_loss(
        self,
        symbol: str,
        side: str,
        amount: float,
        stop_price: float,
    ) -> str:
        """Place a server-side stop-loss order with reduceOnly. Returns order ID."""
        assert self._exchange is not None
        order = await self._exchange.create_order(
            symbol=symbol,
            type="stop_market",
            side=side.lower(),
            amount=amount,
            params={"stopPrice": stop_price, "reduceOnly": True},
        )
        logger.info(
            "Stop loss placed: {} {} {} @ {} — id={} (reduceOnly)",
            side, amount, symbol, stop_price, order["id"],
        )
        return str(order["id"])

    async def place_take_profit(
        self,
        symbol: str,
        side: str,
        amount: float,
        price: float,
    ) -> str:
        """Place a take-profit order with reduceOnly. Returns order ID."""
        assert self._exchange is not None
        order = await self._exchange.create_order(
            symbol=symbol,
            type="take_profit_market",
            side=side.lower(),
            amount=amount,
            params={"stopPrice": price, "reduceOnly": True},
        )
        logger.info(
            "Take profit placed: {} {} {} @ {} — id={} (reduceOnly)",
            side, amount, symbol, price, order["id"],
        )
        return str(order["id"])

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Cancel an open order."""
        assert self._exchange is not None
        try:
            await self._exchange.cancel_order(order_id, symbol)
            logger.info("Order cancelled: {} — {}", symbol, order_id)
            return True
        except Exception as e:
            logger.error("Failed to cancel order {}: {}", order_id, e)
            return False

    async def get_order(self, symbol: str, order_id: str) -> dict:
        """Get order details."""
        assert self._exchange is not None
        return await self._exchange.fetch_order(order_id, symbol)

    # ── Symbol Info ────────────────────────────────────────

    async def get_symbol_info(self, symbol: str) -> dict:
        """Get symbol trading rules (tick size, lot size, etc.)."""
        assert self._exchange is not None
        markets = self._exchange.markets
        return markets.get(symbol, {})

    def get_min_amount(self, symbol: str) -> float:
        """Get minimum order quantity for a symbol."""
        assert self._exchange is not None
        market = self._exchange.markets.get(symbol, {})
        limits = market.get("limits", {}).get("amount", {})
        return float(limits.get("min", 0.001))

    def get_price_precision(self, symbol: str) -> int:
        """Get price decimal precision for a symbol."""
        assert self._exchange is not None
        market = self._exchange.markets.get(symbol, {})
        return int(market.get("precision", {}).get("price", 2))

    def get_amount_precision(self, symbol: str) -> int:
        """Get amount decimal precision for a symbol."""
        assert self._exchange is not None
        market = self._exchange.markets.get(symbol, {})
        return int(market.get("precision", {}).get("amount", 3))

    def format_price(self, symbol: str, price: float) -> float:
        """Format price using ccxt's built-in precision handling."""
        assert self._exchange is not None
        return float(self._exchange.price_to_precision(symbol, price))

    def format_amount(self, symbol: str, amount: float) -> float:
        """Format amount using ccxt's built-in precision handling."""
        assert self._exchange is not None
        return float(self._exchange.amount_to_precision(symbol, amount))
