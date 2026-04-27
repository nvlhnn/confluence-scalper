# TDB — Trading Daily Bot

> **Momentum Confluence Scalper (MCS)** — A high-leverage crypto futures scalping bot
> targeting $5-10/day on $50 capital via Binance Futures.

---

## Overview

TDB is an automated crypto futures trading bot that uses a **5-layer confluence system**
to identify high-probability scalping entries on 5-minute charts. It combines regime detection,
trend filtering, RSI divergence, VWAP anchoring, and volume confirmation to achieve a
**55-65% win rate with 2:1+ risk-reward ratio**.

### Key Specs

| Parameter | Value |
|-----------|-------|
| Exchange | Binance Futures (USDT-M) |
| Starting Capital | $50 |
| Leverage | 10-25x (dynamic) |
| Timeframes | 5m (entry), 15m (regime), 1H (trend) |
| Trades per Day | 2-4 |
| Target Daily Profit | $5-10 (10-20% ROI) |
| Risk per Trade | 2% of balance |
| Max Simultaneous Positions | 2 |

### Project Structure

```
f:\trading\tdb\
├── docs/                    # Documentation
│   ├── README.md            # This file
│   ├── STRATEGY.md          # Full strategy specification
│   ├── ARCHITECTURE.md      # System architecture & data flow
│   ├── RISK_MANAGEMENT.md   # Risk rules & position sizing
│   ├── CONFIGURATION.md     # Config & environment variables
│   └── DEPLOYMENT.md        # VPS deployment guide
├── config/
│   └── settings.yaml        # All bot parameters (YAML)
├── src/                     # Source code
│   ├── core/                # Core engine
│   │   ├── bot.py           # Main 3-tier async orchestrator
│   │   ├── config.py        # Config loader (YAML + .env)
│   │   └── logger.py        # Loguru logging setup
│   ├── data/                # Data layer
│   │   ├── candle_cache.py  # Smart multi-TF candle cache
│   │   └── models.py        # Candle, Signal, Trade, IndicatorSet
│   ├── indicators/          # Technical indicators
│   │   └── indicators.py    # EMA, RSI, ADX, ATR, VWAP, BB (numpy)
│   ├── strategy/            # Strategy engine
│   │   ├── regime.py        # Market regime classifier (Layer 1)
│   │   ├── confluence.py    # 5-layer confluence scorer
│   │   ├── engine.py        # Indicator engine (candles → IndicatorSet)
│   │   └── screener.py      # Dynamic coin scanner (200+ pairs)
│   ├── risk/                # Risk management
│   │   └── risk_manager.py  # Sizing, circuit breakers, correlation
│   ├── exchange/            # Exchange integration
│   │   └── binance_client.py # Async Binance Futures via ccxt
│   ├── notifications/       # Alerting
│   │   └── telegram.py      # Telegram bot notifications
│   └── database/            # Persistence
│       └── db.py            # SQLite (trades, signals, scans, P&L)
├── .env.example             # Environment variable template
├── .gitignore
├── requirements.txt
└── main.py                  # Entry point (CLI)
```

### Quick Start

```bash
# 1. Install dependencies
cd f:\trading\tdb
pip install -r requirements.txt

# 2. Configure API keys
copy .env.example .env
# Edit .env with your Binance API keys

# 3. Run on testnet first
python main.py --testnet

# 4. Run live (after testing — requires confirmation)
python main.py --live
```

### Documentation Index

| Document | Description |
|----------|-------------|
| [STRATEGY.md](./STRATEGY.md) | Complete MCS strategy specification |
| [ARCHITECTURE.md](./ARCHITECTURE.md) | System architecture, data flow, module interactions |
| [RISK_MANAGEMENT.md](./RISK_MANAGEMENT.md) | Position sizing, risk rules, circuit breakers |
| [CONFIGURATION.md](./CONFIGURATION.md) | All config parameters & environment variables |
| [DEPLOYMENT.md](./DEPLOYMENT.md) | VPS deployment, monitoring, maintenance |
