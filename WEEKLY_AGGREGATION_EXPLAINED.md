# Weekly Pattern Detection - TradingView Standard

## How Weekly Aggregation Works

### 1. ISO Week Standard (Same as TradingView)
- Uses ISO 8601 week numbering system
- Week starts on **Monday**, ends on **Sunday**
- Week numbers range from 1 to 52/53 per year
- Handles year boundaries correctly

### 2. OHLC Aggregation for Weekly Candles

For each ISO week, we aggregate daily data as follows:
- **Open**: First trading day's OPEN of the week
- **High**: Maximum HIGH across all days in the week
- **Low**: Minimum LOW across all days in the week
- **Close**: Last trading day's CLOSE of the week
- **Volume**: Sum of all daily volumes in the week

### 3. Pattern Detection Process

```
Week N (Marubozu) → Week N+1 (Doji)
```

The scanner checks **consecutive weekly candles**:
1. First weekly candle is checked for Marubozu pattern
2. Next consecutive weekly candle is checked for Doji pattern
3. Both must be from consecutive ISO weeks

### 4. Example Pattern

**TCS Pattern Found:**
- **Week 17 (Apr 21-27, 2025)**: Marubozu candle
  - Last trading day: April 25, 2025
  - Body: 81.52% of range ✓
  - Move: 4.80% ✓

- **Week 18 (Apr 28-May 4, 2025)**: Doji candle
  - Last trading day: May 2, 2025
  - Body: 6.74% of range ✓
  - High breaks Week 17's high ✓
  - Closes inside Week 17's body ✓

### 5. UI Display

The UI now shows:
- **Signal Date**: The Doji week's ending date with week number
  - Example: "May 2, 2025 (Week 18)"

- **Pattern Span**: Shows the week progression
  - Example: "(Week 17) → (Week 18)"

This makes it clear that we're comparing consecutive weekly candles, not individual days.

### 6. Verification

To verify this matches TradingView:
1. Open TradingView chart for the symbol
2. Switch to Weekly timeframe (W)
3. Look at the two consecutive weekly candles
4. The aggregated OHLC should match exactly

### Key Points
- ✅ Uses TradingView-standard ISO week aggregation
- ✅ Checks consecutive weekly candles only
- ✅ Shows week numbers in UI for clarity
- ✅ Pattern spans exactly 2 consecutive weeks