# Configuration — TDB Bot

## 1. Environment Variables (`.env`)

```env
# ──────────────────────────────────────────────────────────
# BINANCE API
# ──────────────────────────────────────────────────────────
BINANCE_API_KEY=your_api_key_here
BINANCE_API_SECRET=your_api_secret_here
BINANCE_TESTNET=true

# ──────────────────────────────────────────────────────────
# TELEGRAM NOTIFICATIONS
# ──────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
TELEGRAM_ENABLED=true

# ──────────────────────────────────────────────────────────
# BOT MODE
# ──────────────────────────────────────────────────────────
BOT_MODE=testnet
# Options: testnet, live, backtest
```

---

## 2. Strategy Configuration (`config/strategy.yaml`)

```yaml
# ──────────────────────────────────────────────────────────
# STRATEGY PARAMETERS
# ──────────────────────────────────────────────────────────
strategy:
  name: "MCS"  # Momentum Confluence Scalper

  # Timeframes
  timeframes:
    entry: "5m"
    regime: "15m"
    trend: "1h"

  # Layer 1: Regime Detection
  regime:
    adx_period: 14
    bb_period: 20
    bb_std_dev: 2.0
    bb_width_lookback: 100        # Bars for percentile calculation
    thresholds:
      trending_adx: 25            # ADX above this = trending
      ranging_adx: 20             # ADX below this = ranging
      dead_adx: 15                # ADX below this = dead market
      squeeze_bb_percentile: 20   # BB width below this percentile = squeeze
      volatile_bb_percentile: 80  # BB width above this percentile = volatile
      volatile_adx: 30            # ADX above this + high BB = volatile

  # Layer 2: Trend Direction
  trend:
    ema_slow_period: 200          # 1H chart — primary trend
    ema_fast_period: 50           # 15m chart — secondary trend

  # Layer 3: RSI Divergence
  divergence:
    rsi_period: 7                 # Shorter period for 5m sensitivity
    oversold_threshold: 25        # More extreme than standard 30
    overbought_threshold: 75      # More extreme than standard 70
    swing_lookback: 14            # Bars to look back for swing points
    min_swing_distance: 3         # Minimum bars between swings

  # Layer 4: Level Confluence
  levels:
    vwap_enabled: true
    ema_50_5m_enabled: true
    prev_session_levels: true
    proximity_pct: 0.15           # 0.15% proximity to level

  # Layer 5: Volume
  volume:
    sma_period: 20
    spike_multiplier: 1.5         # Volume > 1.5x average
    strong_spike_multiplier: 2.0  # Volume > 2x average (extra score)

  # Layer 5b: Candle Patterns
  candles:
    engulfing_enabled: true
    hammer_enabled: true
    shooting_star_enabled: true
    body_to_wick_ratio: 2.0       # Wick must be >= 2x body for hammer/star

  # Signal Scoring
  scoring:
    min_score: 8                  # Minimum total score to trade
    a_plus_threshold: 11          # A+ quality
    a_threshold: 9                # A quality
    b_threshold: 8                # B quality (reduced size)
    b_size_multiplier: 0.75       # Position size reduction for B setups

# ──────────────────────────────────────────────────────────
# COIN SCREENING (Dynamic Scanner)
# ──────────────────────────────────────────────────────────
screening:
  # Mode: 'dynamic' scans ALL Binance Futures pairs automatically
  #        'static' uses only the whitelist below
  mode: "dynamic"

  # Dynamic scanning — fetches ALL USDT-M futures from Binance
  dynamic:
    scan_pool: "all"              # 'all' = every USDT-M pair, 'top50' = top 50 by volume, 'top30' = top 30
    rescreen_interval_hours: 4    # Re-rank coins every 4 hours
    max_active_coins: 30          # Top N coins to actively monitor after ranking

    # Filters (coins must pass ALL to be considered)
    filters:
      min_24h_volume: 50000000    # $50M minimum 24h volume
      max_spread_pct: 0.05        # 0.05% max bid-ask spread
      min_atr_pct: 0.15           # Min ATR% on 5m (enough movement for scalping)
      max_atr_pct: 5.0            # Max ATR% (avoid extreme volatility / illiquid coins)
      min_price: 0.001            # Skip dust-priced coins with rounding issues
      quote_currency: "USDT"      # Only USDT-margined futures
      contract_type: "PERPETUAL"  # Only perpetual contracts (no delivery)

    # Ranking formula: how to score & rank coins that pass filters
    # Final score = (volatility_weight × norm_atr) + (volume_weight × norm_volume)
    #             + (spread_weight × norm_spread_inverse)
    ranking:
      volatility_weight: 0.4      # Higher ATR% = more scalping opportunity
      volume_weight: 0.4          # Higher volume = better fills, less slippage
      spread_weight: 0.2          # Tighter spread = lower hidden cost

  # Blacklist — NEVER trade these (regardless of ranking)
  blacklist:
    - "1000SHIBUSDT"              # Rounding issues with leveraged sizing
    - "1000PEPEUSDT"              # Same — use PEPEUSDT instead
    - "USDCUSDT"                  # Stablecoin — no volatility
    - "FDUSDUSDT"                 # Stablecoin

  # Whitelist — ALWAYS include these in scan (bypass volume filter)
  # Useful to guarantee BTC/ETH are always scanned even during low-vol periods
  whitelist:
    - "BTCUSDT"
    - "ETHUSDT"

# ──────────────────────────────────────────────────────────
# ENTRY & EXIT
# ──────────────────────────────────────────────────────────
execution:
  # Entry
  order_type: "limit"             # 'limit' or 'market'
  limit_price_buffer_pct: 0.02    # 0.02% buffer for better fills
  order_timeout_candles: 3        # Cancel after 3 unfilled candles

  # Exit — Stop Loss
  stop_loss:
    method: "atr"                 # 'atr' or 'fixed_pct'
    atr_multiplier: 1.5           # SL = ATR(14) × 1.5
    atr_period: 14
    max_stop_pct: 2.0             # Max stop distance 2%
    min_stop_pct: 0.2             # Min stop distance 0.2%

  # Exit — Take Profit
  take_profit:
    rr_ratio: 2.0                 # 2:1 R:R
    min_rr_ratio: 1.5             # Absolute minimum R:R

  # Exit — Trailing Stop
  trailing:
    activate_at_rr: 1.5           # Start trailing after 1.5:1
    breakeven_at_rr: 1.5          # Move SL to breakeven at 1.5:1
    breakeven_buffer_pct: 0.05    # Small buffer above breakeven
    trail_by_atr: true            # Trail by ATR
    trail_atr_multiplier: 1.0     # Trailing ATR multiplier

  # Exit — Time Stop
  time_stop:
    enabled: true
    max_bars: 15                  # Exit after 15 bars (75 min on 5m)
    min_move_pct: 0.3             # Minimum expected move in that time
```

---

## 3. Risk Configuration (`config/risk.yaml`)

```yaml
# ──────────────────────────────────────────────────────────
# RISK MANAGEMENT
# ──────────────────────────────────────────────────────────
risk:
  # Position Sizing
  position:
    risk_per_trade_pct: 2.0       # 2% of balance per trade
    max_margin_pct: 25.0          # Max 25% of balance as margin per trade
    min_order_value: 5.0          # Minimum order size ($5)

  # Leverage
  leverage:
    base: 20                      # Default leverage
    min: 10                       # Minimum allowed
    max: 25                       # Maximum allowed
    dynamic: true                 # Adjust based on volatility/regime

  # Account Limits
  limits:
    max_open_positions: 2
    max_daily_trades: 5
    max_same_direction: 2         # Max positions in same direction

  # Circuit Breakers
  circuit_breakers:
    daily_loss:
      enabled: true
      max_loss_pct: 6.0           # 6% of day's starting balance
      reset_time_utc: "00:00"     # Reset at UTC midnight

    drawdown:
      enabled: true
      max_drawdown_pct: 20.0      # 20% from peak balance
      auto_resume: false          # Requires manual reset

    consecutive_losses:
      enabled: true
      max_consecutive: 4
      cooldown_minutes: 120       # 2 hour cooldown

  # Margin Type
  margin_type: "ISOLATED"         # NEVER use CROSS

  # Correlation
  correlation:
    enabled: true
    groups:
      btc: [BTCUSDT]
      eth: [ETHUSDT]
      alt_l1: [SOLUSDT, SUIUSDT]
      meme: [DOGEUSDT, PEPEUSDT]
```

---

## 4. Notification Configuration

```yaml
# ──────────────────────────────────────────────────────────
# NOTIFICATIONS
# ──────────────────────────────────────────────────────────
notifications:
  telegram:
    enabled: true
    messages:
      signal_detected: true       # New signal found
      position_opened: true       # Trade entered
      position_closed: true       # Trade exited
      stop_moved: true            # SL/TP updated
      daily_summary: true         # End of day report
      circuit_breaker: true       # Breaker triggered
      error_alert: true           # Bot errors
      heartbeat: true             # Hourly status
    heartbeat_interval_minutes: 60
    daily_summary_time_utc: "23:59"
```

---

## 5. Logging Configuration

```yaml
# ──────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────
logging:
  level: "INFO"                   # DEBUG, INFO, WARNING, ERROR
  console: true
  file:
    enabled: true
    path: "logs/"
    rotation: "10 MB"             # Rotate after 10MB
    retention: "30 days"          # Keep 30 days of logs
    format: "{time:YYYY-MM-DD HH:mm:ss} | {level} | {module}:{line} | {message}"
  
  # Separate log for trades
  trade_log:
    enabled: true
    path: "logs/trades.log"
```

---

## 6. Database Configuration

```yaml
# ──────────────────────────────────────────────────────────
# DATABASE
# ──────────────────────────────────────────────────────────
database:
  path: "data/tdb.db"
  backup:
    enabled: true
    interval_hours: 24
    max_backups: 7
    path: "data/backups/"
```

---

## 7. Backtest Configuration

```yaml
# ──────────────────────────────────────────────────────────
# BACKTESTING
# ──────────────────────────────────────────────────────────
backtest:
  start_date: "2024-01-01"
  end_date: "2025-04-27"
  initial_balance: 50.0
  symbols:
    - BTCUSDT
    - ETHUSDT
    - SOLUSDT
  data_source: "binance"          # Fetch from Binance API
  data_cache_dir: "data/historical/"
  
  # Realistic simulation
  slippage_pct: 0.02              # 0.02% slippage per order
  maker_fee_pct: 0.02             # Binance maker fee
  taker_fee_pct: 0.04             # Binance taker fee
  
  # Output
  report_dir: "backtest/reports/"
  save_trades: true
  generate_charts: true
```

---

## 8. Configuration Loading

All config is loaded through a single `Config` class:

```python
class Config:
    """
    Centralized configuration loader.
    Priority: ENV vars > YAML files > Defaults
    """
    def __init__(self, env_path='.env', config_dir='config/'):
        load_dotenv(env_path)
        self.strategy = self._load_yaml(f'{config_dir}/strategy.yaml')
        self.risk = self._load_yaml(f'{config_dir}/risk.yaml')
        # ... etc
    
    @property
    def binance_api_key(self) -> str:
        return os.getenv('BINANCE_API_KEY', '')
    
    @property  
    def is_testnet(self) -> bool:
        return os.getenv('BINANCE_TESTNET', 'true').lower() == 'true'
    
    # ... more properties
```

---

## 9. Parameter Quick Reference

### Most Likely to Tune

| Parameter | Location | Default | Range | Impact |
|-----------|----------|---------|-------|--------|
| RSI period | strategy.divergence.rsi_period | 7 | 5-14 | Sensitivity of divergence |
| RSI thresholds | strategy.divergence.*_threshold | 25/75 | 20-30/70-80 | Signal frequency |
| ATR multiplier (stop) | execution.stop_loss.atr_multiplier | 1.5 | 1.0-2.5 | Stop tightness |
| R:R ratio | execution.take_profit.rr_ratio | 2.0 | 1.5-3.0 | Target vs stop |
| Risk per trade | risk.position.risk_per_trade_pct | 2.0 | 1.0-3.0 | Position size |
| Volume spike multiplier | strategy.volume.spike_multiplier | 1.5 | 1.2-2.5 | Filter strictness |
| Min confluence score | strategy.scoring.min_score | 8 | 7-10 | Signal frequency |
| ADX trending threshold | strategy.regime.thresholds.trending_adx | 25 | 20-30 | Regime sensitivity |

### DO NOT Change Without Backtesting

| Parameter | Why |
|-----------|-----|
| Max open positions (2) | Correlated risk management |
| Margin type (ISOLATED) | Account protection |
| Daily loss limit (6%) | Survival |
| Max drawdown (20%) | Survival |
