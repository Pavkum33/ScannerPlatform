# Marubozu → Doji Pattern Detection Logic (Latest Version)

## Pattern Overview
Detects failed breakout patterns where a Doji candle breaks above a Marubozu's high but closes back inside the Marubozu's body, indicating rejection.

## Current Thresholds

### 1. Marubozu Candle (First Candle)
- **Body Percentage**: ≥ 80% of total range
  - Formula: `body_pct = |close - open| / (high - low) * 100`
  - Must be ≥ 80%

- **Body Move Filter**: User configurable (default 3%)
  - Formula: `body_move_pct = |close - open| / open * 100`
  - Must be ≥ user_input (e.g., 3%)

### 2. Doji Candle (Second Candle)
- **Body Percentage**: < 20% of total range
  - Formula: `body_pct = |close - open| / (high - low) * 100`
  - Must be < 20%

### 3. Pattern Requirements

#### Consecutive Periods
- Candles MUST be from consecutive trading periods
- **Daily**: Next trading day
- **Weekly**: Consecutive ISO weeks (handles year boundaries)
- **Monthly**: Consecutive calendar months

#### Breakout Condition
- Doji.high > Marubozu.high
- Simple check: Doji high must break above Marubozu high

#### Rejection Condition
- Doji must close INSIDE Marubozu body
- **Bullish Marubozu**:
  - `Marubozu.open < Doji.close < Marubozu.close`
- **Bearish Marubozu**:
  - `Marubozu.close < Doji.close < Marubozu.open`

## Code Implementation

### Pattern Detector Class
```python
class PatternDetector:
    def __init__(self,
                 marubozu_threshold: float = 0.8,   # 80% body of range
                 doji_threshold: float = 0.20):      # 20% body of range (stricter)
```

### Key Validation Logic
```python
def matches_marubozu_doji(self, c1: Candle, c2: Candle):
    # 1. Check Marubozu (≥80% body)
    if c1.body_pct < 80:
        return False

    # 2. Check Doji (<20% body)
    if c2.body_pct >= 20:
        return False

    # 3. Check breakout
    if c2.high <= c1.high:
        return False

    # 4. Check rejection (closes inside body)
    if c1.is_bullish:
        closes_inside = c1.open < c2.close < c1.close
    else:
        closes_inside = c1.close < c2.close < c1.open

    if not closes_inside:
        return False

    return True
```

## Weekly Aggregation Rules (TradingView-style)

### ISO Week Aggregation
- Uses ISO 8601 week numbering
- Week starts on Monday, ends on Sunday
- Handles year boundaries correctly

### OHLC Calculation
- **Open**: First trading day's open of the week
- **Close**: Last trading day's close of the week
- **High**: Maximum high across all days in the week
- **Low**: Minimum low across all days in the week
- **Volume**: Sum of all daily volumes

### Consecutive Week Validation
```python
# Check if weeks are consecutive using ISO calendar
current_iso = current_date.isocalendar()
next_iso = next_date.isocalendar()

is_consecutive = (
    (current_iso.year == next_iso.year and next_iso.week == current_iso.week + 1) or
    (current_iso.year == next_iso.year - 1 and current_iso.week >= 52 and next_iso.week == 1)
)
```

## Example Patterns

### Valid Pattern Example (TCS)
- **Week 1 (Apr 25, 2025)**: Bullish Marubozu
  - Open: 3290.1, Close: 3448.0
  - Body: 81.52% of range ✓
  - Move: 4.80% ✓

- **Week 2 (May 2, 2025)**: Doji
  - Open: 3435.0, Close: 3444.7
  - Body: 6.74% of range ✓ (< 20%)
  - High: 3509.9 (breaks 3477.8) ✓
  - Closes inside Marubozu body ✓

### Rejected Pattern Example (MCX)
- **Week 1 (Aug 1, 2025)**: Bearish Marubozu
  - Body: 87.44% of range ✓
  - Move: 5.29% ✓

- **Week 2 (Aug 8, 2025)**: Failed Doji
  - Body: 23.98% of range ✗ (exceeds 20% threshold)
  - Pattern REJECTED

## Output Format
```json
{
  "symbol": "TCS",
  "timeframe": "1W",
  "pattern_direction": "bullish",
  "marubozu": {
    "date": "2025-04-25",
    "body_pct_of_range": 81.52,
    "body_move_pct": 4.80
  },
  "doji": {
    "date": "2025-05-02",
    "body_pct_of_range": 6.74
  },
  "breakout_amount": 32.1,
  "rejection_strength": 45.31
}
```

## Summary of Current Logic

1. **Doji Threshold**: 20% (stricter than original 25%)
2. **Breakout Validation**: Simple check - Doji.high > Marubozu.high
3. **Body Check**: Doji must close inside Marubozu body (no tolerance)
4. **No Volume Requirements**
5. **No Price Filters**

Clean, simple logic that focuses on the core pattern requirements.