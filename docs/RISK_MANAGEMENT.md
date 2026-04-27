# Risk Management — TDB Bot

> **This is the most critical document in the entire project.**
> With $50 and 20x leverage, a single unmanaged trade can wipe the account.
> Risk management is not a feature — it IS the strategy.

---

## 1. Core Risk Parameters

### Account-Level Rules

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| **Max risk per trade** | 2% of current balance | Survive 15+ consecutive losses before 30% drawdown |
| **Max simultaneous positions** | 2 | Avoid correlated liquidation events |
| **Max daily loss** | 6% of day's starting balance | Circuit breaker — stop trading for the day |
| **Max drawdown** | 20% from peak balance | Pause bot, require manual review before resuming |
| **Max daily trades** | 5 | Prevent overtrading (fees kill thin edges) |
| **Margin type** | ISOLATED (never CROSS) | Limit loss to margin used, protect rest of balance |
| **Max margin per trade** | 25% of current balance | Never risk more than quarter of account on one trade |

### Position-Level Rules

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| **Stop loss** | ALWAYS — ATR(14) × 1.5 | Non-negotiable, server-side on Binance |
| **Minimum R:R** | 2.0:1 | Ensures positive expectancy even at 45% win rate |
| **Leverage range** | 10x - 25x (dynamic) | Based on volatility regime |
| **Order type** | Limit preferred | Save 50% on fees vs market orders |
| **Order timeout** | 3 candles (15 min) | Don't chase stale setups |
| **Time stop** | 15 candles (75 min) | Exit dead trades that tie up margin |

---

## 2. Dynamic Position Sizing

### The Formula

```python
def calculate_position_size(
    balance: float,         # Current account balance
    risk_pct: float,        # 0.02 (2%)
    entry_price: float,     # Planned entry price
    stop_loss: float,       # Planned stop loss price
    leverage: int,          # Dynamic leverage
    quality_multiplier: float,  # From signal quality (0.75 or 1.0)
    regime_multiplier: float    # From regime (0.5, 0.75, or 1.0)
) -> dict:
    """
    Calculate position size based on risk parameters.
    
    The key insight: position size is derived from RISK, not from
    available margin. We decide how much to LOSE first, then calculate
    the position size that produces that exact loss at the stop.
    """
    # 1. Maximum dollar risk for this trade
    risk_amount = balance * risk_pct * quality_multiplier * regime_multiplier
    
    # 2. Stop distance as a percentage
    stop_distance_pct = abs(entry_price - stop_loss) / entry_price
    
    # 3. Position size that produces risk_amount loss at stop
    position_size = risk_amount / stop_distance_pct
    
    # 4. Margin required
    margin_required = position_size / leverage
    
    # 5. Cap margin at 25% of balance
    max_margin = balance * 0.25
    if margin_required > max_margin:
        margin_required = max_margin
        position_size = margin_required * leverage
        # Recalculate actual risk
        risk_amount = position_size * stop_distance_pct
    
    return {
        'position_size': round(position_size, 2),
        'margin_required': round(margin_required, 2),
        'risk_amount': round(risk_amount, 2),
        'risk_pct_actual': round((risk_amount / balance) * 100, 2),
        'stop_distance_pct': round(stop_distance_pct * 100, 3),
        'leverage': leverage
    }
```

### Worked Example

```
Balance:     $50.00
Risk:        2% = $1.00
Entry:       $67,500.00 (BTCUSDT)
Stop Loss:   $67,162.50 (0.5% away)
Leverage:    20x

Position Size = $1.00 / 0.005 = $200.00
Margin Required = $200.00 / 20 = $10.00
Actual Risk = $200 × 0.5% = $1.00 ✓

If stop is hit: Lose $1.00 (2% of $50)
If TP is hit (2:1 R:R at 1.0%): Gain $2.00 (4% of $50)
```

---

## 3. Dynamic Leverage

Leverage is NOT fixed. It adjusts based on market conditions.

```python
def calculate_leverage(
    regime: str,
    atr_pct: float,         # ATR as % of price
    base_leverage: int = 20  # Default
) -> int:
    """
    Adjust leverage based on volatility and regime.
    
    Higher volatility → Lower leverage (wider stops)
    Lower volatility → Higher leverage (tighter stops)
    """
    # Regime adjustment
    regime_multiplier = {
        'TRENDING': 1.0,     # Normal leverage
        'RANGING': 1.2,      # Can use slightly more (tighter stops)
        'SQUEEZE': 0.8,      # Preparing for breakout — be cautious
        'VOLATILE': 0.5,     # Half leverage in volatile conditions
        'DEAD': 0.0,         # No trading
    }.get(regime, 1.0)
    
    # Volatility adjustment (inverse)
    # Low ATR → more leverage, High ATR → less leverage
    if atr_pct > 0.8:       # Very volatile
        vol_multiplier = 0.5
    elif atr_pct > 0.5:     # Moderate
        vol_multiplier = 0.75
    elif atr_pct > 0.3:     # Normal
        vol_multiplier = 1.0
    else:                    # Low vol
        vol_multiplier = 1.25
    
    leverage = int(base_leverage * regime_multiplier * vol_multiplier)
    
    # Clamp to 10-25x range
    return max(10, min(25, leverage))
```

---

## 4. Correlation Guard

Prevents opening positions that would double down on the same market move.

### Correlation Groups

```python
CORRELATION_GROUPS = {
    'BTC_GROUP': ['BTCUSDT'],
    'ETH_GROUP': ['ETHUSDT'],
    'ALT_L1': ['SOLUSDT', 'SUIUSDT'],          # L1 chains move together
    'MEME': ['DOGEUSDT', 'PEPEUSDT'],           # Meme coins correlate
    'DEFI': ['LINKUSDT', 'AAVEUSDT'],           # DeFi tokens
}

def check_correlation(
    new_signal: Signal,
    open_positions: list[Trade]
) -> tuple[bool, str]:
    """
    Check if new signal conflicts with existing positions.
    
    Rules:
    1. No two positions in the same symbol
    2. No two positions in the same correlation group with same direction
    3. Max 2 positions in the same direction (LONG or SHORT)
    """
    for position in open_positions:
        # Same symbol
        if new_signal.symbol == position.signal.symbol:
            return False, f"Already have position in {new_signal.symbol}"
        
        # Same correlation group, same direction
        new_group = get_group(new_signal.symbol)
        pos_group = get_group(position.signal.symbol)
        if (new_group and new_group == pos_group and 
            new_signal.direction == position.signal.direction):
            return False, f"Correlated position exists in {pos_group}"
    
    # Max same-direction positions
    same_dir_count = sum(
        1 for p in open_positions 
        if p.signal.direction == new_signal.direction
    )
    if same_dir_count >= 2:
        return False, f"Already have {same_dir_count} {new_signal.direction} positions"
    
    return True, "OK"
```

---

## 5. Circuit Breakers

### Daily Loss Circuit Breaker

```python
class DailyLossBreaker:
    """
    Stops all trading if daily loss limit is hit.
    Resets at UTC midnight.
    """
    def __init__(self, max_loss_pct: float = 0.06):
        self.max_loss_pct = max_loss_pct
        self.starting_balance = 0.0
        self.realized_pnl_today = 0.0
        self.is_triggered = False
    
    def reset(self, current_balance: float):
        """Called at start of each trading day (UTC midnight)."""
        self.starting_balance = current_balance
        self.realized_pnl_today = 0.0
        self.is_triggered = False
    
    def record_trade(self, pnl: float):
        """Called after each trade closes."""
        self.realized_pnl_today += pnl
        
        loss_pct = abs(self.realized_pnl_today) / self.starting_balance
        if self.realized_pnl_today < 0 and loss_pct >= self.max_loss_pct:
            self.is_triggered = True
    
    def can_trade(self) -> tuple[bool, str]:
        if self.is_triggered:
            return False, f"Daily loss limit hit: ${abs(self.realized_pnl_today):.2f}"
        return True, "OK"
```

### Drawdown Circuit Breaker

```python
class DrawdownBreaker:
    """
    Pauses bot if drawdown from peak exceeds threshold.
    Requires manual review to resume.
    """
    def __init__(self, max_drawdown_pct: float = 0.20):
        self.max_drawdown_pct = max_drawdown_pct
        self.peak_balance = 0.0
        self.is_triggered = False
    
    def update(self, current_balance: float):
        if current_balance > self.peak_balance:
            self.peak_balance = current_balance
        
        drawdown = (self.peak_balance - current_balance) / self.peak_balance
        if drawdown >= self.max_drawdown_pct:
            self.is_triggered = True
    
    def can_trade(self) -> tuple[bool, str]:
        if self.is_triggered:
            dd_pct = ((self.peak_balance - self.current_balance) / 
                      self.peak_balance * 100)
            return False, f"Max drawdown breached: {dd_pct:.1f}%"
        return True, "OK"
```

### Consecutive Loss Breaker

```python
class ConsecutiveLossBreaker:
    """
    Pauses trading after N consecutive losses.
    Gives time for market conditions to change.
    """
    def __init__(self, max_consecutive: int = 4, cooldown_minutes: int = 120):
        self.max_consecutive = max_consecutive
        self.cooldown_minutes = cooldown_minutes
        self.consecutive_losses = 0
        self.cooldown_until = None
    
    def record_trade(self, is_win: bool):
        if is_win:
            self.consecutive_losses = 0
        else:
            self.consecutive_losses += 1
            if self.consecutive_losses >= self.max_consecutive:
                self.cooldown_until = datetime.utcnow() + timedelta(
                    minutes=self.cooldown_minutes
                )
    
    def can_trade(self) -> tuple[bool, str]:
        if self.cooldown_until and datetime.utcnow() < self.cooldown_until:
            remaining = (self.cooldown_until - datetime.utcnow()).seconds // 60
            return False, f"Cooling down after {self.max_consecutive} losses ({remaining}m left)"
        elif self.cooldown_until:
            # Cooldown expired
            self.cooldown_until = None
            self.consecutive_losses = 0
        return True, "OK"
```

---

## 6. Risk Validation Flow

```
Signal generated with score ≥ 8
│
├── 1. Circuit Breakers
│   ├── Daily loss limit hit?            → REJECT
│   ├── Max drawdown exceeded?           → REJECT
│   └── Consecutive loss cooldown?       → REJECT
│
├── 2. Position Limits
│   ├── Open positions >= 2?             → REJECT
│   ├── Daily trade count >= 5?          → REJECT
│   └── Same symbol already open?        → REJECT
│
├── 3. Correlation Check
│   ├── Correlated position exists?      → REJECT
│   └── Too many same-direction?         → REJECT
│
├── 4. Position Sizing
│   ├── Calculate risk amount (2% × multipliers)
│   ├── Calculate position size from stop distance
│   ├── Cap margin at 25% of balance
│   └── Sufficient available margin?     → REJECT if not
│
├── 5. Sanity Checks
│   ├── Position size > minimum order ($5)?
│   ├── Stop loss within valid range?
│   ├── Entry price reasonable vs current price?
│   └── Fees < 15% of expected profit?   → WARN if not
│
└── ALL PASSED → Execute trade
```

---

## 7. Survival Math

### How Long Can We Survive a Losing Streak?

```
Starting balance: $50
Risk per trade: 2% ($1.00)

After 5 consecutive losses:
  $50 × (0.98)^5 = $45.10  → Down 9.8% — manageable

After 10 consecutive losses:
  $50 × (0.98)^10 = $40.69 → Down 18.6% — circuit breaker triggers at 20%

After 15 consecutive losses:
  $50 × (0.98)^15 = $36.72 → Down 26.6% — would be stopped earlier by breaker

Probability of 10 consecutive losses at 55% win rate:
  (0.45)^10 = 0.034% → 1 in ~3,000 chance
  
Probability of 5 consecutive losses at 55% win rate:
  (0.45)^5 = 1.8% → 1 in ~55 chance (will happen eventually)
```

### Break-Even Analysis

```
With 2:1 R:R and fees of $0.16/trade:

Required win rate for break-even:
  Win × $2.00 - Loss × $1.00 - Fees × $0.16 = 0
  W × 2 - (1-W) × 1 = 0.16
  2W - 1 + W = 0.16
  3W = 1.16
  W = 38.7%

Our target win rate: 55-65%
Safety margin: 16-26 percentage points above break-even ✓
```

---

## 8. Recovery Protocol

### After Drawdown Circuit Breaker Triggers

```
1. Bot automatically:
   ├── Closes all pending orders (not open positions)
   ├── Sends Telegram alert with full stats
   └── Enters PAUSED state

2. Manual review (YOU must do this):
   ├── Check last 20 trades — what went wrong?
   ├── Is the market regime unusual? (black swan, major news)
   ├── Are the strategy parameters still valid?
   └── Decide: resume, adjust parameters, or wait

3. To resume:
   ├── python main.py --reset-breaker
   ├── Bot resumes with reduced position size (50%) for first 10 trades
   └── Gradually returns to full size if win rate normalizes
```
