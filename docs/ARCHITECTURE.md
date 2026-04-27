# System Architecture — TDB Bot

## 1. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        TDB — Trading Daily Bot                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────┐    ┌──────────────┐    ┌────────────────────────┐     │
│  │ Data Feed │───▶│ Indicator    │───▶│ Strategy Engine         │    │
│  │ (REST+WS) │    │ Calculator   │    │ (5-Layer Confluence)    │    │
│  └──────────┘    └──────────────┘    └───────────┬────────────┘    │
│                                                   │                  │
│                                                   ▼                  │
│  ┌──────────┐    ┌──────────────┐    ┌────────────────────────┐    │
│  │ Telegram  │◀───│ Position     │◀───│ Risk Manager           │    │
│  │ Notifier  │    │ Monitor      │    │ (Sizing + Rules)       │    │
│  └──────────┘    └──────┬───────┘    └───────────┬────────────┘    │
│                          │                        │                  │
│                          ▼                        ▼                  │
│                  ┌──────────────┐    ┌────────────────────────┐     │
│                  │ SQLite DB    │    │ Order Executor         │     │
│                  │ (Trades/P&L) │    │ (Binance Futures API)  │     │
│                  └──────────────┘    └────────────────────────┘     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. Component Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                        main.py                               │
│                   (Entry Point + CLI)                         │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                     src/core/bot.py                           │
│                  (Main Orchestrator)                          │
│                                                              │
│  Responsibilities:                                           │
│  • Initialize all components                                 │
│  • Run main event loop (every 5m candle close)               │
│  • Coordinate data → indicators → strategy → risk → orders  │
│  • Handle graceful shutdown                                  │
│  • Health monitoring                                         │
└─────┬──────────┬──────────┬──────────┬──────────┬───────────┘
      │          │          │          │          │
      ▼          ▼          ▼          ▼          ▼
┌─────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌─────────┐
│  DATA   │ │INDICAT.│ │STRATEGY│ │  RISK  │ │EXCHANGE │
│  LAYER  │ │ LAYER  │ │ LAYER  │ │ LAYER  │ │ LAYER   │
└─────────┘ └────────┘ └────────┘ └────────┘ └─────────┘
```

---

## 3. Module Specifications

### 3.1 Data Layer (`src/data/`)

#### `feed.py` — Market Data Feed
```
Responsibilities:
├── Connect to Binance Futures REST API for historical candles
├── Connect to Binance WebSocket for real-time price updates
├── Manage multiple symbol subscriptions
├── Handle connection drops and auto-reconnect
└── Emit candle close events to the bot orchestrator

Key Methods:
├── async fetch_candles(symbol, interval, limit) → list[Candle]
├── async subscribe_klines(symbols, interval, callback)
├── async subscribe_ticker(symbols, callback)
└── async close()

Data Refresh Schedule:
├── 5m candles:  Fetched every 5 minutes (primary)
├── 15m candles: Fetched every 15 minutes (regime)
├── 1H candles:  Fetched every 1 hour (trend)
└── Ticker:      Real-time via WebSocket (position monitoring)
```

#### `candles.py` — Candle Manager
```
Responsibilities:
├── Cache candle data in memory (rolling window)
├── Convert raw API data to Candle objects
├── Provide candle data to indicator calculations
└── Handle missing candles (gap detection)

Cache Sizes:
├── 5m:  200 candles (~16.7 hours)
├── 15m: 100 candles (~25 hours)
└── 1H:  200 candles (~8.3 days)
```

#### `models.py` — Data Models
```python
@dataclass
class Candle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    symbol: str
    interval: str

@dataclass
class Signal:
    symbol: str
    direction: str          # 'LONG' or 'SHORT'
    entry_price: float
    stop_loss: float
    take_profit: float
    confluence_score: int
    quality: str            # 'A_PLUS', 'A', 'B'
    regime: str
    timestamp: int
    metadata: dict          # Layer breakdown, indicators, etc.

@dataclass 
class Trade:
    id: str
    signal: Signal
    entry_order_id: str
    stop_order_id: str
    tp_order_id: str
    status: str             # 'PENDING', 'OPEN', 'CLOSED', 'CANCELLED'
    entry_fill_price: float
    exit_fill_price: float
    position_size: float
    margin_used: float
    leverage: int
    pnl: float
    fees: float
    opened_at: int
    closed_at: int

@dataclass
class PositionState:
    trade: Trade
    current_price: float
    unrealized_pnl: float
    unrealized_rr: float
    bars_held: int
    trailing_stop: float
```

---

### 3.2 Indicator Layer (`src/indicators/`)

All indicator modules follow the same pattern:

```python
# Pure functions — no side effects, easily testable
# Input: numpy arrays or lists of floats
# Output: numpy arrays or single values

def ema(prices: np.ndarray, period: int) -> np.ndarray: ...
def rsi(prices: np.ndarray, period: int) -> np.ndarray: ...
def adx(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int) -> np.ndarray: ...
def atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int) -> np.ndarray: ...
def vwap(high: np.ndarray, low: np.ndarray, close: np.ndarray, volume: np.ndarray) -> np.ndarray: ...
def bollinger_bandwidth(close: np.ndarray, period: int, std_dev: float) -> np.ndarray: ...
def volume_sma(volume: np.ndarray, period: int) -> np.ndarray: ...
```

**Design Decision:** Use pure numpy calculations instead of pandas-ta for:
1. **Speed** — numpy is 3-5× faster for simple calculations
2. **Control** — exact implementation matches our spec
3. **No dependency bloat** — pandas-ta pulls in many dependencies

---

### 3.3 Strategy Layer (`src/strategy/`)

#### `regime.py` — Market Regime Classifier
```
Input:  15m candle data (ADX, BB Width)
Output: Regime enum ('TRENDING', 'RANGING', 'SQUEEZE', 'VOLATILE', 'DEAD')
```

#### `confluence.py` — 5-Layer Confluence Scorer
```
Input:  All indicator values for a symbol
Output: Confluence score (0-13) + quality tier + layer breakdown
```

#### `signals.py` — Signal Generator
```
Input:  Confluence results for all screened coins
Output: List of Signal objects ranked by score
```

#### `screener.py` — Dynamic Coin Scanner
```
Responsibilities:
├── Fetch ALL USDT-M perpetual futures from Binance exchangeInfo
├── Apply multi-criteria filters (volume, spread, ATR%, contract type)
├── Normalize and score using weighted ranking formula
├── Select top 30 coins (with whitelist/blacklist support)
├── Detect coin rotation (new coins in/out) and notify
└── Update WebSocket subscriptions when active coins change

Key Methods:
├── async scan_all_futures() → list[CoinScore]
├── async refresh() → None (called every 4 hours)
├── get_active_coins() → list[str]
└── get_coin_scores() → dict[str, float]

Data Flow:
├── Input:  Binance exchangeInfo + 24hr tickers + 5m candles
├── Pool:   ALL ~200+ USDT-M perpetual pairs
├── Filter: ~30-60 pass volume/spread/ATR criteria
├── Output: Top 30 ranked by weighted score
└── Run:    Every 4 hours (configurable)

API Cost Per Scan:
├── exchangeInfo:  1 call (cached 24h)
├── 24hr tickers:  1 call (batch, returns all)
├── 5m candles:    ~50 calls (only volume-filtered coins)
└── Total:         ~52 calls — well within 1200/min limit
```

---

### 3.4 Risk Layer (`src/risk/`)

#### `position_sizer.py`
```
Input:  Current balance, signal quality, ATR, entry/stop prices
Output: Position size (USDT), margin required, leverage to use

Formula:
  risk_amount = balance × risk_pct (2%)
  stop_distance = abs(entry - stop) / entry
  position_size = risk_amount / stop_distance
  margin = position_size / leverage
  
  # Cap margin at 25% of balance per trade
  if margin > balance * 0.25:
      position_size = balance * 0.25 * leverage
```

#### `risk_manager.py`
```
Checks (ALL must pass before opening a trade):
├── Open positions < max_positions (2)
├── Daily realized loss < daily_loss_limit (6% of starting balance)
├── Account drawdown < max_drawdown (20%)
├── Available margin >= margin_required
├── No existing position in same symbol
├── No highly correlated position (e.g., BTC + ETH same direction)
└── Daily trade count < max_daily_trades (5)
```

#### `drawdown.py`
```
Tracks:
├── Peak balance (high-water mark)
├── Current drawdown percentage
├── Drawdown duration (bars since peak)
├── Circuit breaker state
└── Recovery tracking
```

---

### 3.5 Exchange Layer (`src/exchange/`)

#### `binance_client.py`
```
Wrapper around Binance Futures API via ccxt library.

Methods:
├── async connect(testnet: bool = True)
├── async get_balance() → float
├── async get_position(symbol) → dict
├── async get_all_positions() → list[dict]
├── async place_limit_order(symbol, side, amount, price) → str
├── async place_market_order(symbol, side, amount) → str
├── async place_stop_loss(symbol, side, amount, stop_price) → str
├── async place_take_profit(symbol, side, amount, price) → str
├── async cancel_order(symbol, order_id) → bool
├── async set_leverage(symbol, leverage) → bool
├── async set_margin_type(symbol, margin_type='ISOLATED') → bool
├── async get_ticker(symbol) → dict
├── async fetch_exchange_info() → dict           # All futures pairs metadata
├── async fetch_all_tickers() → dict[str, dict]  # 24hr stats for ALL pairs
└── async fetch_symbol_info(symbol) → dict        # Tick size, lot size, etc.

Design:
├── Always use ISOLATED margin (never cross)
├── Set leverage per symbol before first trade
├── Retry logic: 3 retries with exponential backoff
├── Rate limiting: Max 10 requests/second
└── All orders have client_order_id for tracking
```

#### `order_manager.py`
```
Responsibilities:
├── Convert Signal → set of orders (entry + SL + TP)
├── Track order states (NEW, FILLED, CANCELLED, EXPIRED)
├── Handle partial fills
├── Cancel unfilled limit orders after timeout (3 candles)
└── Reconcile local state with exchange state
```

#### `position_monitor.py`
```
Responsibilities:
├── Poll open positions every 30 seconds
├── Calculate unrealized P&L
├── Manage trailing stop updates
├── Detect and handle manual interventions
├── Trigger time-based exits
└── Emit position state changes to Telegram notifier
```

---

### 3.6 Notification Layer (`src/notifications/`)

#### `telegram.py`
```
Messages:
├── 🔍 Signal detected (score, quality, direction, coin)
├── 📈 Position opened (entry, SL, TP, size, leverage)
├── 🔄 Stop moved to breakeven
├── 📊 Trailing stop updated
├── ✅ Position closed — profit (+$X.XX, +X.X%)
├── ❌ Position closed — loss (-$X.XX, -X.X%)
├── ⏰ Position closed — time exit
├── 📋 Daily summary (trades, P&L, win rate, balance)
├── 🛑 Circuit breaker triggered (reason)
└── 🔧 Bot status (heartbeat every 1 hour)
```

---

### 3.7 Database Layer (`src/database/`)

#### SQLite Schema

```sql
-- Trades table
CREATE TABLE trades (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL,           -- 'LONG' or 'SHORT'
    status TEXT NOT NULL,              -- 'PENDING', 'OPEN', 'CLOSED', 'CANCELLED'
    entry_price REAL,
    exit_price REAL,
    stop_loss REAL,
    take_profit REAL,
    position_size REAL,
    margin_used REAL,
    leverage INTEGER,
    pnl REAL DEFAULT 0,
    fees REAL DEFAULT 0,
    net_pnl REAL DEFAULT 0,
    confluence_score INTEGER,
    quality TEXT,
    regime TEXT,
    entry_order_id TEXT,
    stop_order_id TEXT,
    tp_order_id TEXT,
    opened_at INTEGER,
    closed_at INTEGER,
    close_reason TEXT,                 -- 'TP', 'SL', 'TRAIL', 'TIME', 'MANUAL'
    metadata TEXT,                     -- JSON blob with full signal details
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily P&L table
CREATE TABLE daily_pnl (
    date TEXT PRIMARY KEY,             -- YYYY-MM-DD
    starting_balance REAL,
    ending_balance REAL,
    total_pnl REAL,
    total_fees REAL,
    net_pnl REAL,
    trades_count INTEGER,
    wins INTEGER,
    losses INTEGER,
    win_rate REAL,
    best_trade REAL,
    worst_trade REAL,
    max_drawdown_pct REAL
);

-- Signals log (for analysis)
CREATE TABLE signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL,
    confluence_score INTEGER,
    quality TEXT,
    regime TEXT,
    taken INTEGER DEFAULT 0,          -- 1 if trade was opened
    rejected_reason TEXT,              -- Why it was skipped
    layer_scores TEXT,                 -- JSON breakdown
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bot state (for recovery after restart)
CREATE TABLE bot_state (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Coin scan history (track which coins are selected and why)
CREATE TABLE coin_scans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_time TIMESTAMP NOT NULL,
    total_pairs_scanned INTEGER,       -- Total USDT-M pairs on Binance
    pairs_passed_filter INTEGER,       -- How many passed volume/spread/ATR
    selected_coins TEXT NOT NULL,       -- JSON array of selected symbols
    scores TEXT NOT NULL,               -- JSON object {symbol: score_details}
    coins_added TEXT,                   -- Coins new this scan (vs previous)
    coins_removed TEXT,                 -- Coins dropped this scan (vs previous)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## 4. Event Loop & Timing Architecture

### The 3-Tier System

The bot runs **3 concurrent async tasks**, each at a different frequency:

```
┌─────────────────────────────────────────────────────────────────────┐
│                     CONCURRENT ASYNC TASKS                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  TIER 1: COIN SCANNER          ← Runs every 4 HOURS                │
│  ├── Scans ALL 200+ futures pairs from Binance                     │
│  ├── Filters → ~30-60 pass                                         │
│  ├── Ranks → selects top 30                                        │
│  ├── Updates active coin list                                      │
│  └── Updates WebSocket subscriptions                               │
│       Cost: ~52 API calls | Duration: ~15-30 seconds               │
│                                                                     │
│  TIER 2: SIGNAL CHECKER        ← Runs every 5 MINUTES              │
│  ├── Triggers on 5m candle close (not polling!)                    │
│  ├── Checks ONLY the 30 active coins (not 200+)                   │
│  ├── Calculates indicators for each                                │
│  ├── Runs 5-layer confluence → generates signals                   │
│  ├── Validates through risk manager                                │
│  └── Executes approved trades                                      │
│       Cost: ~15-20 API calls | Duration: ~3-5 seconds              │
│                                                                     │
│  TIER 3: POSITION MONITOR      ← Runs every 30 SECONDS            │
│  ├── Only active when positions are open (otherwise idle)          │
│  ├── Uses WebSocket for real-time price (no API calls!)            │
│  ├── Calculates unrealized P&L                                     │
│  ├── Manages trailing stop logic                                   │
│  ├── Checks time-based exits                                       │
│  └── Updates stop/TP orders on Binance if needed                   │
│       Cost: 0-2 API calls | Duration: <1 second                    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Why NOT Loop Every Second?

| Approach | API Calls/Day | Issues |
|----------|--------------|--------|
| ❌ 1s loop × 200 coins | 17,280,000 | Rate limited instantly, banned |
| ❌ 1s loop × 30 coins | 2,592,000 | Wasteful, indicators don't change sub-candle |
| ❌ 30s loop × 30 coins | 86,400 | Still wasteful for candle-based indicators |
| ✅ **5m candle close × 30 coins** | **~8,640** | **Efficient — indicators only update on close** |

**Key insight:** All our indicators (RSI, EMA, ADX, VWAP, Volume) are calculated on
**closed candles**. Checking mid-candle is pointless — the values will change before
the candle closes. We wait for the candle to close, THEN check.

### Detailed Flow: What Happens Each Cycle

```
TIME: 14:04:59 — Waiting...
TIME: 14:05:00 — 5m candle closes → TRIGGER

  ┌─────── TIER 2: Signal Check (30 coins, ~15 seconds) ─────┐
  │                                                           │
  │  14:05:00  Fetch 5m candles for 30 active coins          │
  │            (30 API calls, parallel via asyncio.gather)    │
  │                                                           │
  │  14:05:03  Check: is it also a 15m close? (every 3rd)    │
  │            If yes → also fetch 15m + 1H candles           │
  │            If no  → use cached 15m + 1H data             │
  │                                                           │
  │  14:05:05  Calculate indicators for all 30 coins:        │
  │            ├── BTCUSDT:  ADX=27, RSI=23, Vol=1.8x ✓     │
  │            ├── ETHUSDT:  ADX=19, RSI=45, Vol=0.9x ✗     │
  │            ├── WIFUSDT:  ADX=31, RSI=18, Vol=2.1x ✓     │
  │            ├── ... (27 more coins checked)               │
  │            └── TIAUSDT:  ADX=28, RSI=77, Vol=2.3x ✓     │
  │                                                           │
  │  14:05:02  Run 5-layer confluence on BTCUSDT:            │
  │            ├── Layer 1 (Regime):     TRENDING ✅ +2       │
  │            ├── Layer 2 (Trend):      LONG ✅ +2           │
  │            ├── Layer 3 (Divergence): Bullish ✅ +2        │
  │            ├── Layer 4 (Level):      At VWAP ✅ +2        │
  │            ├── Layer 5 (Volume):     1.8x ✅ +1           │
  │            └── Layer 5b (Candle):    Hammer ✅ +1          │
  │            Score: 10/13 → Quality: A → SIGNAL!           │
  │                                                           │
  │  14:05:03  Run 5-layer on WIFUSDT:                       │
  │            Score: 6/13 → REJECTED (trend conflicted)     │
  │                                                           │
  │  14:05:03  Risk Manager validates BTCUSDT signal:        │
  │            ├── Open positions < 2?        ✅              │
  │            ├── Daily loss limit ok?       ✅              │
  │            ├── No correlated position?    ✅              │
  │            └── Margin available?          ✅              │
  │                                                           │
  │  14:05:04  Execute: Place limit order for BTCUSDT LONG   │
  │            Entry: $67,450, SL: $67,115, TP: $68,120     │
  │            → Telegram: "📈 LONG BTCUSDT, Score 10/13"   │
  │                                                           │
  └───────────────────────────────────────────────────────────┘

TIME: 14:05:04 — Done. Sleep until next 5m close (14:10:00)

  ┌─── TIER 3: Position Monitor (runs independently) ────────┐
  │                                                           │
  │  While position is open:                                  │
  │  Every 30 seconds via WebSocket price feed:              │
  │                                                           │
  │  14:05:30  BTCUSDT @ $67,465 → P&L: +$0.04 (0.02 R:R)  │
  │  14:06:00  BTCUSDT @ $67,490 → P&L: +$0.12 (0.06 R:R)  │
  │  14:06:30  BTCUSDT @ $67,520 → P&L: +$0.21 (0.10 R:R)  │
  │  ...                                                      │
  │  14:35:00  BTCUSDT @ $67,950 → P&L: +$1.48 (1.50 R:R)  │
  │            → Move SL to breakeven ($67,483)              │
  │            → Telegram: "🔄 SL → breakeven"              │
  │  ...                                                      │
  │  14:55:00  BTCUSDT @ $68,120 → TP HIT!                  │
  │            → P&L: +$1.98 (2.0 R:R)                      │
  │            → Telegram: "✅ BTCUSDT +$1.98"              │
  │                                                           │
  └───────────────────────────────────────────────────────────┘
```

### Candle Close Detection

We do NOT poll for candle closes. We calculate the exact timestamp:

```python
async def wait_for_candle_close(interval_minutes: int = 5):
    """
    Sleep until the next 5-minute candle closes.
    
    Candle closes happen at exact multiples of 5 minutes:
    00:00, 00:05, 00:10, ... 23:55 UTC
    
    We add a 2-second buffer to ensure the candle data is
    available on Binance's API (slight processing delay).
    """
    now = datetime.utcnow()
    
    # Next candle close time
    minutes_past = now.minute % interval_minutes
    seconds_to_close = (
        (interval_minutes - minutes_past) * 60
        - now.second
        - now.microsecond / 1_000_000
    )
    
    if seconds_to_close <= 0:
        seconds_to_close += interval_minutes * 60
    
    # Add 2s buffer for API data availability
    wait_time = seconds_to_close + 2.0
    
    await asyncio.sleep(wait_time)
```

### Multi-Timeframe Data Caching

Not every 5m cycle needs fresh 15m and 1H data:

```python
class CandleCache:
    """
    Smart caching to minimize API calls.
    
    5m candles:  Fetch every cycle (primary timeframe)
    15m candles: Fetch every 3rd cycle (15 min / 5 min = 3)
    1H candles:  Fetch every 12th cycle (60 min / 5 min = 12)
    
    Between fetches, use cached data — the higher-TF candle
    hasn't closed yet, so data hasn't changed.
    """
    def __init__(self):
        self.cache = {}       # {symbol: {interval: [candles]}}
        self.cycle_count = 0
    
    async def update(self, symbols: list[str], feed) -> dict:
        self.cycle_count += 1
        
        for symbol in symbols:
            # Always fetch 5m
            self.cache.setdefault(symbol, {})
            self.cache[symbol]['5m'] = await feed.fetch_candles(
                symbol, '5m', limit=200
            )
            
            # Fetch 15m every 3rd cycle
            if self.cycle_count % 3 == 0:
                self.cache[symbol]['15m'] = await feed.fetch_candles(
                    symbol, '15m', limit=100
                )
            
            # Fetch 1H every 12th cycle
            if self.cycle_count % 12 == 0:
                self.cache[symbol]['1h'] = await feed.fetch_candles(
                    symbol, '1h', limit=200
                )
        
        return self.cache
```

### API Call Budget Per Day

```
TIER 1 — Coin Scanner (every 4h = 6 scans/day):
├── exchangeInfo:  1 × 1 = 1 call (cached, fetched once)
├── 24hr tickers:  1 × 6 = 6 calls
├── 5m candles:    50 × 6 = 300 calls
└── Subtotal:      ~307 calls/day

TIER 2 — Signal Checker (every 5m = 288 cycles/day):
├── 5m candles:    30 × 288 = 8,640 calls
├── 15m candles:   30 × 96 = 2,880 calls (every 3rd cycle)
├── 1H candles:    30 × 24 = 720 calls (every 12th cycle)
└── Subtotal:      ~12,240 calls/day

TIER 3 — Position Monitor:
├── WebSocket:     0 calls (real-time stream)
├── Order updates: ~10 calls/day (SL moves, exits)
└── Subtotal:      ~10 calls/day

TOTAL:             ~12,557 calls/day
BINANCE LIMIT:     1,200 calls/MINUTE = 1,728,000/day
USAGE:             0.73% of limit ✅ — still very safe
```

### Implementation: Concurrent Task Runner

```python
async def run_bot(config: Config):
    """
    Launch all 3 tiers as concurrent async tasks.
    """
    bot = Bot(config)
    await bot.initialize()
    
    # Launch all tiers concurrently
    await asyncio.gather(
        tier1_coin_scanner(bot),      # Every 4 hours
        tier2_signal_checker(bot),    # Every 5 minutes
        tier3_position_monitor(bot),  # Every 30 seconds (when positions open)
    )


async def tier1_coin_scanner(bot: Bot):
    """TIER 1: Scan all futures every 4 hours."""
    while bot.is_running:
        try:
            new_coins = await bot.screener.scan_all_futures()
            old_coins = bot.screener.active_coins
            
            if set(new_coins) != set(old_coins):
                # Coin lineup changed — update subscriptions
                added = set(new_coins) - set(old_coins)
                removed = set(old_coins) - set(new_coins)
                
                await bot.data_feed.update_subscriptions(new_coins)
                await bot.notifier.coin_rotation(added, removed, new_coins)
                
                bot.screener.active_coins = new_coins
                bot.db.log_scan(new_coins, added, removed)
            
            bot.logger.info(f"Scanner: active coins = {new_coins}")
        except Exception as e:
            bot.logger.error(f"Scanner error: {e}")
        
        await asyncio.sleep(4 * 3600)  # 4 hours


async def tier2_signal_checker(bot: Bot):
    """TIER 2: Check signals on every 5m candle close."""
    while bot.is_running:
        try:
            await wait_for_candle_close(interval_minutes=5)
            
            active_coins = bot.screener.active_coins
            if not active_coins:
                continue
            
            # Fetch candle data (with smart caching)
            candle_data = await bot.candle_cache.update(
                active_coins, bot.data_feed
            )
            
            # Process each coin through 5-layer confluence
            all_signals = []
            for symbol in active_coins:
                indicators = bot.indicator_engine.calculate(
                    candle_data[symbol]
                )
                signal = bot.strategy.evaluate_single(symbol, indicators)
                
                if signal and signal.quality != 'REJECTED':
                    all_signals.append(signal)
            
            # Sort by score (best first)
            all_signals.sort(key=lambda s: s.confluence_score, reverse=True)
            
            # Validate and execute top signals
            for signal in all_signals:
                approved, reason = bot.risk_manager.validate(signal)
                if approved:
                    trade = await bot.order_manager.execute_signal(signal)
                    if trade:
                        bot.db.save_trade(trade)
                        await bot.notifier.signal_opened(trade)
                else:
                    bot.db.log_signal(signal, taken=False, reason=reason)
            
        except Exception as e:
            bot.logger.error(f"Signal checker error: {e}")


async def tier3_position_monitor(bot: Bot):
    """TIER 3: Monitor open positions via WebSocket."""
    while bot.is_running:
        try:
            open_positions = bot.position_monitor.open_positions
            
            if not open_positions:
                # No positions — sleep longer, nothing to monitor
                await asyncio.sleep(5)
                continue
            
            for position in open_positions:
                # Price comes from WebSocket (real-time, no API call)
                current_price = bot.data_feed.get_latest_price(
                    position.trade.signal.symbol
                )
                
                # Update position state
                position.current_price = current_price
                position.unrealized_pnl = calculate_pnl(position)
                position.bars_held += 1  # Tracked separately
                
                # Exit management
                action = bot.strategy.manage_exit(position)
                
                if action['action'] == 'TRAIL':
                    await bot.order_manager.update_stop(
                        position, action['new_stop']
                    )
                    await bot.notifier.stop_updated(position, action)
                
                elif action['action'] == 'BREAKEVEN':
                    await bot.order_manager.update_stop(
                        position, action['new_stop']
                    )
                    await bot.notifier.breakeven_hit(position)
                
                elif action['action'] in ('STOP_OUT', 'TP_HIT'):
                    # Binance handles this server-side via SL/TP orders
                    # We just detect and log it
                    await bot.position_monitor.close_position(position)
                    await bot.notifier.position_closed(position, action)
            
            await asyncio.sleep(30)  # Check every 30 seconds
            
        except Exception as e:
            bot.logger.error(f"Monitor error: {e}")
            await asyncio.sleep(5)
```

---

## 5. Error Handling & Recovery

### Connection Failures
```
WebSocket disconnect:
├── Auto-reconnect with exponential backoff (1s, 2s, 4s, 8s, max 60s)
├── After 5 failed attempts → switch to REST-only mode
├── Alert via Telegram
└── Continue operation (REST is reliable fallback)

REST API failure:
├── Retry 3 times with 1s delay
├── If all retries fail → skip this cycle
├── Log error
└── Continue with next candle close
```

### Bot Crash Recovery
```
On startup:
├── Load bot_state from SQLite
├── Check for any open positions on exchange
├── Reconcile local state with exchange state
├── Resume position monitoring for any open trades
└── Continue normal operation
```

### Order Failures
```
Order rejected:
├── Log the rejection reason
├── If INSUFFICIENT_BALANCE → reduce position size by 50% and retry once
├── If PRICE_FILTER → adjust price to valid tick size
├── If MAX_ORDERS → cancel oldest pending orders first
└── If unknown → skip and alert
```

---

## 6. Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Language | Python | 3.11+ | Core runtime |
| Exchange API | ccxt | Latest | Binance Futures wrapper |
| Async I/O | asyncio | Built-in | Non-blocking event loop |
| WebSocket | websockets | Latest | Real-time data stream |
| Math | numpy | Latest | Indicator calculations |
| Database | SQLite | Built-in | Trade persistence |
| HTTP | aiohttp | Latest | Async HTTP client |
| Telegram | python-telegram-bot | Latest | Notifications |
| Config | python-dotenv | Latest | Environment variables |
| Scheduling | APScheduler | Latest | Periodic tasks |
| Logging | loguru | Latest | Structured logging |
