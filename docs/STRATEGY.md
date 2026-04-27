# Strategy Specification — Momentum Confluence Scalper (MCS)

## 1. Strategy Philosophy

The MCS strategy is built on one core principle:

> **No single indicator has edge on low timeframes.
> Edge comes from CONFLUENCE — multiple independent signals confirming the same trade.**

Each of the 5 layers acts as a filter. Individually, each filter removes 30-40% of false
signals. Combined: 0.7^5 = only ~17% of false signals survive all 5 filters.

---

## 2. The 5-Layer Confluence System

### Layer Overview

```
Layer 1: REGIME DETECTION    → Should I be trading at all right now?
Layer 2: TREND DIRECTION     → Which direction should I trade?
Layer 3: MOMENTUM TRIGGER    → Is there an exhaustion/reversal signal?
Layer 4: LEVEL CONFLUENCE    → Is price at a significant level?
Layer 5: VOLUME CONFIRMATION → Is there real participation behind this move?
```

Each layer produces a score. A trade is only taken when ALL layers agree.

---

### Layer 1: Market Regime Detection (ADX + Bollinger Bandwidth)

**Purpose:** Determine if the market is trending, ranging, or in a squeeze. This decides
WHICH sub-strategy to use and WHETHER to trade at all.

**Timeframe:** 15-minute chart

**Indicators:**
- ADX(14) — Average Directional Index
- Bollinger Bandwidth(20, 2.0) — measures volatility

**Regime Classification:**

| Regime | ADX Value | BB Width | Action |
|--------|-----------|----------|--------|
| TRENDING | ADX > 25 | Normal | ✅ Trade momentum setups |
| RANGING | ADX < 20 | Normal | ✅ Trade mean reversion setups |
| SQUEEZE | ADX < 20 | < 20th percentile of last 100 bars | ⚠️ Prepare for breakout, don't enter yet |
| VOLATILE | ADX > 30 | > 80th percentile of last 100 bars | ⚠️ Reduce position size by 50% |
| DEAD | ADX < 15 | < 10th percentile | ❌ Do NOT trade |

**Implementation:**

```python
def classify_regime(adx_value: float, bb_width: float, bb_width_percentile: float) -> str:
    """
    Classify market regime based on ADX and Bollinger Bandwidth.
    
    Args:
        adx_value: Current ADX(14) value on 15m chart
        bb_width: Current Bollinger Bandwidth (upper - lower) / middle
        bb_width_percentile: Percentile rank of current BB width over last 100 bars
    
    Returns:
        One of: 'TRENDING', 'RANGING', 'SQUEEZE', 'VOLATILE', 'DEAD'
    """
    if adx_value < 15 and bb_width_percentile < 10:
        return 'DEAD'
    elif adx_value < 20 and bb_width_percentile < 20:
        return 'SQUEEZE'
    elif adx_value > 30 and bb_width_percentile > 80:
        return 'VOLATILE'
    elif adx_value > 25:
        return 'TRENDING'
    else:
        return 'RANGING'
```

---

### Layer 2: Trend Direction (200 EMA + 50 EMA)

**Purpose:** Establish a directional bias. ONLY trade in the direction of the trend.

**Timeframes:**
- 1-hour chart: 200 EMA → Primary trend bias
- 15-minute chart: 50 EMA → Secondary trend confirmation

**Rules:**

| 1H 200 EMA | 15m 50 EMA | Bias | Allowed Trades |
|-------------|-----------|------|----------------|
| Price ABOVE | Price ABOVE | BULLISH | LONG only |
| Price BELOW | Price BELOW | BEARISH | SHORT only |
| Price ABOVE | Price BELOW | CONFLICTED | ❌ No trade |
| Price BELOW | Price ABOVE | CONFLICTED | ❌ No trade |

**Edge Justification:** Counter-trend trades on 5m charts lose 65%+ of the time
(sourced from multiple backtests). This single filter eliminates ~40% of losing trades.

**Implementation:**

```python
def get_trend_bias(
    price: float,
    ema_200_1h: float,
    ema_50_15m: float
) -> str:
    """
    Determine directional bias from multi-timeframe EMAs.
    
    Returns:
        'LONG', 'SHORT', or 'NONE' (conflicted)
    """
    above_1h = price > ema_200_1h
    above_15m = price > ema_50_15m
    
    if above_1h and above_15m:
        return 'LONG'
    elif not above_1h and not above_15m:
        return 'SHORT'
    else:
        return 'NONE'  # Conflicted — skip
```

---

### Layer 3: Momentum Trigger (RSI Divergence)

**Purpose:** Identify momentum exhaustion within the trend — catch pullback entries,
not chase breakouts.

**Timeframe:** 5-minute chart

**Indicator:** RSI(7) — shorter period for faster signal on 5m

**Signal Types:**

#### Bullish Divergence (for LONG entries)
```
Price: Makes a LOWER low
RSI:   Makes a HIGHER low
→ Momentum is strengthening even though price dipped
→ Buyers are stepping in — high probability bounce
```

#### Bearish Divergence (for SHORT entries)
```
Price: Makes a HIGHER high
RSI:   Makes a LOWER high
→ Momentum is weakening even though price went up
→ Sellers are stepping in — high probability drop
```

**RSI Extremes Required:**
- For bullish divergence: RSI must touch below 25 (oversold extreme)
- For bearish divergence: RSI must touch above 75 (overbought extreme)
- Standard 30/70 is TOO LOOSE for crypto — adjusted thresholds reduce false signals

**Divergence Detection Algorithm:**

```python
def detect_divergence(
    prices: list[float],       # Last 20 close prices on 5m
    rsi_values: list[float],   # Last 20 RSI(7) values on 5m
    lookback: int = 14,        # Bars to look back for swing points
    trend_bias: str = 'LONG'   # From Layer 2
) -> dict | None:
    """
    Detect RSI divergence.
    
    Steps:
    1. Find the two most recent swing lows (for bullish) or swing highs (for bearish)
    2. Compare price direction vs RSI direction
    3. Confirm RSI reached extreme zone (< 25 or > 75)
    
    Returns:
        Dict with divergence info or None if no divergence found
    """
    if trend_bias == 'LONG':
        # Find two most recent swing lows
        swing_lows = find_swing_lows(prices, lookback)
        if len(swing_lows) < 2:
            return None
        
        prev_low, curr_low = swing_lows[-2], swing_lows[-1]
        
        # Price: lower low, RSI: higher low
        price_lower = prices[curr_low] < prices[prev_low]
        rsi_higher = rsi_values[curr_low] > rsi_values[prev_low]
        rsi_extreme = min(rsi_values[prev_low], rsi_values[curr_low]) < 25
        
        if price_lower and rsi_higher and rsi_extreme:
            return {
                'type': 'BULLISH_DIVERGENCE',
                'rsi_at_signal': rsi_values[curr_low],
                'price_at_signal': prices[curr_low],
                'strength': abs(rsi_values[curr_low] - rsi_values[prev_low])
            }
    
    elif trend_bias == 'SHORT':
        # Find two most recent swing highs
        swing_highs = find_swing_highs(prices, lookback)
        if len(swing_highs) < 2:
            return None
        
        prev_high, curr_high = swing_highs[-2], swing_highs[-1]
        
        # Price: higher high, RSI: lower high
        price_higher = prices[curr_high] > prices[prev_high]
        rsi_lower = rsi_values[curr_high] < rsi_values[prev_high]
        rsi_extreme = max(rsi_values[prev_high], rsi_values[curr_high]) > 75
        
        if price_higher and rsi_lower and rsi_extreme:
            return {
                'type': 'BEARISH_DIVERGENCE',
                'rsi_at_signal': rsi_values[curr_high],
                'price_at_signal': prices[curr_high],
                'strength': abs(rsi_values[prev_high] - rsi_values[curr_high])
            }
    
    return None
```

---

### Layer 4: Level Confluence (VWAP + EMA Levels)

**Purpose:** Ensure entry happens at a significant price level where institutional
orders cluster, increasing bounce probability.

**Timeframe:** 5-minute chart

**Levels Checked:**
1. **VWAP** — Volume-Weighted Average Price (daily reset)
2. **50 EMA on 5m** — Dynamic support/resistance
3. **Previous session high/low** — Structural levels

**Proximity Rule:** Price must be within **0.15%** of at least one level.

```python
def check_level_confluence(
    price: float,
    vwap: float,
    ema_50_5m: float,
    prev_session_high: float,
    prev_session_low: float,
    proximity_pct: float = 0.0015  # 0.15%
) -> dict:
    """
    Check if price is near significant levels.
    
    Returns:
        Dict with level info and confluence score
    """
    levels = {
        'vwap': vwap,
        'ema_50': ema_50_5m,
        'prev_high': prev_session_high,
        'prev_low': prev_session_low,
    }
    
    near_levels = []
    for name, level in levels.items():
        distance_pct = abs(price - level) / price
        if distance_pct <= proximity_pct:
            near_levels.append({
                'name': name,
                'level': level,
                'distance_pct': distance_pct
            })
    
    return {
        'has_confluence': len(near_levels) > 0,
        'near_levels': near_levels,
        'score': len(near_levels)  # More levels = stronger confluence
    }
```

---

### Layer 5: Volume Confirmation

**Purpose:** Validate that real market participants are behind the move, not just
noise or thin-liquidity wicks.

**Timeframe:** 5-minute chart

**Rules:**
- Current bar volume must be > **1.5×** the 20-period volume SMA
- For divergence setups: Volume should spike on the reversal candle
- Volume confirms that the signal isn't just a random wick in thin liquidity

```python
def check_volume_confirmation(
    current_volume: float,
    volume_sma_20: float,
    multiplier: float = 1.5
) -> dict:
    """
    Check if current volume confirms the signal.
    
    Returns:
        Dict with volume confirmation status
    """
    volume_ratio = current_volume / volume_sma_20 if volume_sma_20 > 0 else 0
    
    return {
        'confirmed': volume_ratio >= multiplier,
        'volume_ratio': round(volume_ratio, 2),
        'current': current_volume,
        'average': volume_sma_20
    }
```

---

## 3. Candlestick Confirmation (Bonus Filter)

**Purpose:** Time the exact entry with a reversal candlestick pattern.

**Patterns Accepted:**
- **Bullish Engulfing** — Current candle body fully covers previous candle body (bullish)
- **Bearish Engulfing** — Current candle body fully covers previous candle body (bearish)
- **Hammer / Inverted Hammer** — Long wick rejection at support
- **Shooting Star** — Long upper wick rejection at resistance

**This is Layer 5b — the final confirmation before entry.**

```python
def detect_entry_candle(
    candles: list[dict],  # Last 3 candles: [prev_prev, prev, current]
    bias: str             # 'LONG' or 'SHORT'
) -> dict:
    """
    Detect reversal candlestick patterns.
    
    Each candle dict: {open, high, low, close, volume}
    """
    prev = candles[-2]
    curr = candles[-1]
    
    curr_body = abs(curr['close'] - curr['open'])
    prev_body = abs(prev['close'] - prev['open'])
    curr_range = curr['high'] - curr['low']
    
    if bias == 'LONG':
        # Bullish engulfing
        is_engulfing = (
            prev['close'] < prev['open'] and    # Previous is bearish
            curr['close'] > curr['open'] and     # Current is bullish
            curr_body > prev_body and            # Body fully covers
            curr['close'] > prev['open'] and     # Close above prev open
            curr['open'] < prev['close']         # Open below prev close
        )
        
        # Hammer (long lower wick, small body at top)
        lower_wick = curr['open'] - curr['low'] if curr['close'] > curr['open'] \
                     else curr['close'] - curr['low']
        is_hammer = (
            curr['close'] > curr['open'] and     # Bullish candle
            lower_wick > curr_body * 2 and       # Lower wick >= 2x body
            curr_range > 0 and
            (curr['high'] - curr['close']) < curr_body * 0.3  # Small upper wick
        )
        
        return {
            'pattern': 'BULLISH_ENGULFING' if is_engulfing else 
                       'HAMMER' if is_hammer else 'NONE',
            'confirmed': is_engulfing or is_hammer
        }
    
    elif bias == 'SHORT':
        # Bearish engulfing
        is_engulfing = (
            prev['close'] > prev['open'] and
            curr['close'] < curr['open'] and
            curr_body > prev_body and
            curr['open'] > prev['close'] and
            curr['close'] < prev['open']
        )
        
        # Shooting star (long upper wick, small body at bottom)
        upper_wick = curr['high'] - curr['open'] if curr['close'] < curr['open'] \
                     else curr['high'] - curr['close']
        is_shooting_star = (
            curr['close'] < curr['open'] and
            upper_wick > curr_body * 2 and
            curr_range > 0 and
            (curr['close'] - curr['low']) < curr_body * 0.3
        )
        
        return {
            'pattern': 'BEARISH_ENGULFING' if is_engulfing else
                       'SHOOTING_STAR' if is_shooting_star else 'NONE',
            'confirmed': is_engulfing or is_shooting_star
        }
    
    return {'pattern': 'NONE', 'confirmed': False}
```

---

## 4. Signal Scoring System

Each layer produces a score. The total confluence score determines trade quality.

| Layer | Max Score | Required | Description |
|-------|----------|----------|-------------|
| 1. Regime | 2 | ≥ 1 | TRENDING=2, RANGING=1, SQUEEZE=0, DEAD=-1 |
| 2. Trend Bias | 2 | = 2 | Both EMAs aligned=2, One aligned=1, Conflicted=0 |
| 3. RSI Divergence | 3 | ≥ 2 | Strong div=3, Normal div=2, Weak div=1, None=0 |
| 4. Level Confluence | 3 | ≥ 1 | At VWAP+EMA=3, At VWAP=2, At EMA=1, None=0 |
| 5. Volume | 2 | ≥ 1 | Vol > 2x avg=2, Vol > 1.5x=1, Below=0 |
| 5b. Candle Pattern | 1 | ≥ 0 | Engulfing=1, Hammer/Star=1, None=0 |
| **TOTAL** | **13** | **≥ 8** | **Minimum score to open a trade** |

### Trade Quality Tiers

| Score | Quality | Action |
|-------|---------|--------|
| 11-13 | 🔥 A+ Setup | Full position size |
| 9-10 | ✅ A Setup | Full position size |
| 8 | ⚠️ B Setup | 75% position size |
| 6-7 | ❌ C Setup | DO NOT trade |
| < 6 | ❌ D Setup | DO NOT trade |

```python
def calculate_confluence_score(
    regime_score: int,
    trend_score: int,
    divergence_score: int,
    level_score: int,
    volume_score: int,
    candle_score: int
) -> dict:
    """
    Calculate total confluence score and determine trade quality.
    """
    total = (regime_score + trend_score + divergence_score + 
             level_score + volume_score + candle_score)
    
    # Hard requirements
    if trend_score < 2:
        return {'score': total, 'quality': 'REJECTED', 'reason': 'No trend alignment'}
    if divergence_score < 2:
        return {'score': total, 'quality': 'REJECTED', 'reason': 'No divergence signal'}
    if regime_score < 1:
        return {'score': total, 'quality': 'REJECTED', 'reason': 'Bad regime'}
    if level_score < 1:
        return {'score': total, 'quality': 'REJECTED', 'reason': 'No level confluence'}
    if volume_score < 1:
        return {'score': total, 'quality': 'REJECTED', 'reason': 'No volume confirmation'}
    
    # Quality tiers
    if total >= 11:
        quality = 'A_PLUS'
        size_multiplier = 1.0
    elif total >= 9:
        quality = 'A'
        size_multiplier = 1.0
    elif total >= 8:
        quality = 'B'
        size_multiplier = 0.75
    else:
        quality = 'REJECTED'
        size_multiplier = 0.0
    
    return {
        'score': total,
        'quality': quality,
        'size_multiplier': size_multiplier,
        'breakdown': {
            'regime': regime_score,
            'trend': trend_score,
            'divergence': divergence_score,
            'level': level_score,
            'volume': volume_score,
            'candle': candle_score
        }
    }
```

---

## 5. Entry Execution

Once all 5 layers confirm and score ≥ 8:

### Entry Type: Limit Order (Preferred)

```
For LONG:
  Entry Price = Current price - (0.02% buffer)     # Slightly below for better fill
  Stop Loss   = Entry - ATR(14) × 1.5              # ATR-based stop
  Take Profit = Entry + (Stop Distance × 2.0)      # 2:1 R:R minimum

For SHORT:
  Entry Price = Current price + (0.02% buffer)
  Stop Loss   = Entry + ATR(14) × 1.5
  Take Profit = Entry - (Stop Distance × 2.0)
```

### Limit Order Timeout
- If limit order not filled within **3 candles (15 minutes)**, cancel the order
- The setup has expired — don't chase

### Why Limit Over Market
- Maker fee: 0.02% vs Taker fee: 0.04%
- Saves $0.08 per round trip on $200 position
- At 4 trades/day: saves $0.32/day = $9.60/month

---

## 6. Exit Management

### Stop Loss (Non-Negotiable)
- **Method:** ATR-based — `ATR(14) × 1.5` on the 5m chart
- **Type:** Server-side stop-loss order on Binance (survives API disconnect)
- **Adjustment:** NEVER widen a stop loss. Only trail in profit direction.

### Take Profit
- **Initial TP:** Set at 2× stop distance (2:1 R:R)
- **After 1.5:1 reached:** Move stop to breakeven + 0.05%
- **After 2:1 reached:** Trail stop by ATR(14) for potential extra gains

### Trailing Stop Logic

```python
def manage_exit(
    entry_price: float,
    current_price: float,
    stop_loss: float,
    take_profit: float,
    atr: float,
    direction: str  # 'LONG' or 'SHORT'
) -> dict:
    """
    Dynamic exit management with trailing stop.
    """
    stop_distance = abs(entry_price - stop_loss)
    
    if direction == 'LONG':
        unrealized_rr = (current_price - entry_price) / stop_distance
        
        if unrealized_rr >= 2.0:
            # Trail stop by ATR
            new_stop = current_price - atr
            return {'action': 'TRAIL', 'new_stop': max(new_stop, stop_loss)}
        elif unrealized_rr >= 1.5:
            # Move stop to breakeven + small buffer
            breakeven_stop = entry_price + (entry_price * 0.0005)
            return {'action': 'BREAKEVEN', 'new_stop': max(breakeven_stop, stop_loss)}
        elif current_price <= stop_loss:
            return {'action': 'STOP_OUT', 'exit_price': stop_loss}
        elif current_price >= take_profit:
            return {'action': 'TP_HIT', 'exit_price': take_profit}
        else:
            return {'action': 'HOLD', 'new_stop': stop_loss}
    
    elif direction == 'SHORT':
        unrealized_rr = (entry_price - current_price) / stop_distance
        
        if unrealized_rr >= 2.0:
            new_stop = current_price + atr
            return {'action': 'TRAIL', 'new_stop': min(new_stop, stop_loss)}
        elif unrealized_rr >= 1.5:
            breakeven_stop = entry_price - (entry_price * 0.0005)
            return {'action': 'BREAKEVEN', 'new_stop': min(breakeven_stop, stop_loss)}
        elif current_price >= stop_loss:
            return {'action': 'STOP_OUT', 'exit_price': stop_loss}
        elif current_price <= take_profit:
            return {'action': 'TP_HIT', 'exit_price': take_profit}
        else:
            return {'action': 'HOLD', 'new_stop': stop_loss}
```

### Time-Based Exit
- If position shows **no significant move** (< 0.3% in direction) after **15 candles (75 min)**, exit at market
- Dead trades tie up margin and prevent better setups

---

## 7. Dynamic Coin Scanner

The bot does NOT use a hardcoded coin list. Instead, it **dynamically scans ALL Binance
USDT-M perpetual futures pairs** and automatically selects the best coins to trade.

### Why Dynamic Scanning?

- **200+ perpetual futures** available on Binance — manually picking 5-8 misses opportunities
- Market leaders rotate — SOL dominates for weeks, then DOGE takes over
- Static lists miss breakout coins with sudden volume/volatility spikes
- Dynamic scanning ensures we're ALWAYS trading the most favorable conditions

### Scanning Flow (Runs Every 4 Hours)

```
Step 1: FETCH — Get all USDT-M perpetual futures from Binance
│       └── API: GET /fapi/v1/exchangeInfo → ~200+ symbols
│
Step 2: FILTER — Remove unsuitable pairs
│       ├── Remove non-PERPETUAL contracts (delivery futures)
│       ├── Remove non-USDT quote (BUSD pairs, etc.)
│       ├── Remove blacklisted symbols (stablecoins, problematic pairs)
│       ├── Remove 24h volume < $50M (low liquidity)
│       ├── Remove spread > 0.05% (too expensive to trade)
│       ├── Remove ATR% < 0.15% on 5m (not enough movement)
│       └── Remove ATR% > 5.0% on 5m (too dangerous / illiquid)
│       └── Result: ~30-60 coins typically pass
│
Step 3: RANK — Score remaining coins
│       ├── Volatility Score (40% weight): normalize ATR% across all candidates
│       ├── Volume Score (40% weight): normalize 24h volume
│       ├── Spread Score (20% weight): inverse normalized spread (tighter = better)
│       └── Final Score = weighted sum of normalized scores
│
Step 4: SELECT — Pick top N coins
│       ├── Always include whitelisted coins (BTC, ETH) if they pass filters
│       ├── Fill remaining slots from top-ranked coins
│       └── Output: Top 30 coins for this cycle
│
Step 5: NOTIFY — Log coin rotation
        ├── Log selected coins + scores to database
        ├── Send Telegram message if coin lineup changed
        └── Update WebSocket subscriptions for new coins
```

### Implementation

```python
async def scan_all_futures(
    exchange_client,
    config: dict
) -> list[dict]:
    """
    Scan ALL Binance USDT-M perpetual futures and return ranked list.
    
    This replaces static PRIMARY_COINS / SECONDARY_COINS lists.
    The bot now automatically discovers and trades the best opportunities
    across the ENTIRE futures market.
    """
    # ── Step 1: Fetch all available futures pairs ──
    exchange_info = await exchange_client.fetch_exchange_info()
    all_symbols = [
        s['symbol'] for s in exchange_info['symbols']
        if s['contractType'] == 'PERPETUAL'
        and s['quoteAsset'] == 'USDT'
        and s['status'] == 'TRADING'
    ]
    # Typically returns 200+ symbols
    
    # ── Step 2: Remove blacklisted ──
    blacklist = config.get('blacklist', [])
    candidates = [s for s in all_symbols if s not in blacklist]
    
    # ── Step 3: Fetch market data for all candidates (batch) ──
    # Use /fapi/v1/ticker/24hr for all pairs in one call
    tickers = await exchange_client.fetch_all_tickers()
    
    # ── Step 4: Fetch 5m ATR for volatility measurement ──
    # For efficiency, first filter by volume, then fetch ATR only for survivors
    volume_filtered = []
    min_volume = config.get('min_24h_volume', 50_000_000)
    
    for symbol in candidates:
        ticker = tickers.get(symbol, {})
        volume_24h = float(ticker.get('quoteVolume', 0))
        if volume_24h >= min_volume:
            spread_pct = _calculate_spread(ticker)
            if spread_pct <= config.get('max_spread_pct', 0.05):
                volume_filtered.append({
                    'symbol': symbol,
                    'volume_24h': volume_24h,
                    'spread_pct': spread_pct,
                    'price': float(ticker.get('lastPrice', 0)),
                })
    
    # ── Step 5: Calculate ATR% for volume-filtered coins ──
    # Fetch 5m candles (last 20 bars) for ATR calculation
    scored_coins = []
    for coin in volume_filtered:
        candles_5m = await exchange_client.fetch_candles(
            coin['symbol'], '5m', limit=20
        )
        atr_pct = calculate_atr_percent(candles_5m)
        
        if atr_pct < config.get('min_atr_pct', 0.15):
            continue  # Not volatile enough for scalping
        if atr_pct > config.get('max_atr_pct', 5.0):
            continue  # Too volatile / dangerous
        
        coin['atr_pct'] = atr_pct
        scored_coins.append(coin)
    
    # ── Step 6: Normalize and score ──
    if not scored_coins:
        return []
    
    # Normalize each metric to 0-1 range
    max_vol = max(c['volume_24h'] for c in scored_coins)
    max_atr = max(c['atr_pct'] for c in scored_coins)
    min_spread = min(c['spread_pct'] for c in scored_coins)
    max_spread = max(c['spread_pct'] for c in scored_coins)
    
    vol_weight = config.get('volatility_weight', 0.4)
    volume_weight = config.get('volume_weight', 0.4)
    spread_weight = config.get('spread_weight', 0.2)
    
    for coin in scored_coins:
        norm_atr = coin['atr_pct'] / max_atr if max_atr > 0 else 0
        norm_vol = coin['volume_24h'] / max_vol if max_vol > 0 else 0
        
        # Spread is inverse — lower spread = higher score
        spread_range = max_spread - min_spread
        norm_spread_inv = (1 - (coin['spread_pct'] - min_spread) / spread_range
                          if spread_range > 0 else 1)
        
        coin['score'] = (
            vol_weight * norm_atr +
            volume_weight * norm_vol +
            spread_weight * norm_spread_inv
        )
    
    # ── Step 7: Sort and select top N ──
    scored_coins.sort(key=lambda x: x['score'], reverse=True)
    
    # Always include whitelisted coins if they passed filters
    whitelist = config.get('whitelist', ['BTCUSDT', 'ETHUSDT'])
    max_coins = config.get('max_active_coins', 30)
    
    selected = []
    # First: add whitelisted coins that passed filters
    for coin in scored_coins:
        if coin['symbol'] in whitelist:
            selected.append(coin)
    
    # Then: fill remaining slots from top-ranked
    for coin in scored_coins:
        if len(selected) >= max_coins:
            break
        if coin['symbol'] not in [s['symbol'] for s in selected]:
            selected.append(coin)
    
    return selected


def calculate_atr_percent(candles: list[dict], period: int = 14) -> float:
    """Calculate ATR as a percentage of price (for cross-coin comparison)."""
    if len(candles) < period + 1:
        return 0.0
    
    true_ranges = []
    for i in range(1, len(candles)):
        high = candles[i]['high']
        low = candles[i]['low']
        prev_close = candles[i-1]['close']
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        true_ranges.append(tr)
    
    atr = sum(true_ranges[-period:]) / period
    current_price = candles[-1]['close']
    return (atr / current_price) * 100 if current_price > 0 else 0.0
```

### Example Scanner Output

```
🔍 Dynamic Scan Results (2026-04-27 18:00 UTC)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Scanned:  217 USDT-M perpetual pairs
Filtered: 47 passed volume/spread/ATR filters
Selected: Top 30

Rank │ Symbol     │ Score │ Volume 24h   │ ATR%  │ Spread
─────┼────────────┼───────┼──────────────┼───────┼────────
  1  │ BTCUSDT    │ 0.92  │ $28.4B       │ 0.31% │ 0.01%
  2  │ ETHUSDT    │ 0.87  │ $12.1B       │ 0.38% │ 0.01%
  3  │ WIFUSDT    │ 0.78  │ $890M        │ 0.82% │ 0.02%
  4  │ SOLUSDT    │ 0.74  │ $3.2B        │ 0.45% │ 0.01%
  5  │ PEPEUSDT   │ 0.71  │ $1.8B        │ 0.67% │ 0.02%

↻ Next scan: 2026-04-27 22:00 UTC
```

### Rate Limiting Consideration

```
Scanning 200+ pairs requires careful API usage:
├── exchangeInfo:        1 call (cached for 24h, rarely changes)
├── 24hr tickers:        1 call (returns ALL pairs at once)
├── 5m candles per coin: ~50 calls (only for volume-filtered coins)
├── Total:               ~52 API calls per scan
├── Binance limit:       1200 requests/minute
└── Well within limits ✅
```

---

## 8. Complete Signal Flow

```
Every 5-minute candle close:
│
├── 1. Fetch latest candles (5m, 15m, 1H) for all screened coins
│
├── 2. For each coin:
│   │
│   ├── 2a. Calculate regime (ADX + BB Width on 15m)
│   │   └── If DEAD → skip coin
│   │
│   ├── 2b. Determine trend bias (200 EMA 1H + 50 EMA 15m)
│   │   └── If CONFLICTED → skip coin
│   │
│   ├── 2c. Check RSI divergence (RSI(7) on 5m)
│   │   └── If no divergence → skip coin
│   │
│   ├── 2d. Check level proximity (VWAP + 50 EMA 5m)
│   │   └── If not near any level → skip coin
│   │
│   ├── 2e. Check volume spike (volume vs 20-SMA on 5m)
│   │   └── If volume too low → skip coin
│   │
│   ├── 2f. Check candlestick pattern (engulfing/hammer on 5m)
│   │
│   ├── 2g. Calculate confluence score
│   │   └── If score < 8 → skip coin
│   │
│   └── 2h. SIGNAL GENERATED → Pass to Risk Manager
│
├── 3. Risk Manager validates:
│   ├── Max open positions not exceeded?
│   ├── Daily loss limit not hit?
│   ├── No correlated position already open?
│   ├── Sufficient margin available?
│   └── If all pass → calculate position size
│
├── 4. Order Executor:
│   ├── Place limit entry order
│   ├── Place server-side stop loss
│   ├── Place take profit order
│   └── Start position monitor
│
└── 5. Position Monitor:
    ├── Track P&L every 30 seconds
    ├── Manage trailing stop
    ├── Check time-based exit
    └── Send Telegram updates on state changes
```

---

## 9. Edge Decay & Adaptation

Strategies lose edge over time. Built-in safeguards:

| Metric | Threshold | Action |
|--------|-----------|--------|
| Rolling 20-trade win rate | < 40% | ⚠️ Alert — review parameters |
| Rolling 20-trade profit factor | < 1.0 | 🛑 Pause bot — losing money |
| Daily loss limit hit 3 days in row | N/A | 🛑 Pause bot for 48h, review |
| Monthly return < 0% | N/A | Full strategy review required |

### Automatic Parameter Adjustment
- Every 50 trades, recalculate optimal RSI thresholds (20-30 range)
- Every 100 trades, review ATR multiplier for stops (1.0-2.0 range)
- Log all parameter changes for audit trail
