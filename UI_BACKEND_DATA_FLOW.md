# UI to Backend Data Flow - Weekly Pattern Detection

## Summary: System Works Correctly ✅

The backend **IS** correctly handling UI-passed data for weekly timeframes. The confusion was around the UI labeling.

## Data Flow Analysis

### 1. UI Input (Updated with Clear Labels)
```javascript
// User selects in UI:
- Timeframe: "1W" (Weekly)
- History Periods: 60 (now clearly labeled as "weeks")
- Min Body Move: 4%
```

### 2. UI JavaScript Sends
```javascript
const params = {
    timeframe: "1W",        // Selected timeframe
    min_body_move: "4",     // Minimum body move %
    days_back: "60",        // Actually means "periods" not days
    symbol_group: "fno"     // Symbol group
};
```

### 3. Backend API Receives (app.py)
```python
timeframe = data.get('timeframe', '1D')           # Gets "1W"
min_body_move = float(data.get('min_body_move', 4))    # Gets 4.0
days_back = int(data.get('days_back', 30))        # Gets 60
```

### 4. Scanner Engine Processes (scanner_engine.py)
```python
# Correctly treats days_back as "history periods"
results = scanner_engine.scan(
    symbols=symbols,
    timeframe="1W",         # Weekly timeframe
    history=60,             # 60 PERIODS (weeks, not days)
    min_body_move_pct=4.0
)
```

### 5. Days Calculation (Automatic Conversion)
```python
def _calculate_days_back(self, timeframe: str, history: int) -> int:
    if timeframe == '1W':
        return (history * 7) + 30    # 60 weeks * 7 days + 30 buffer = 450 days
    elif timeframe == '1M':
        return (history * 30) + 60   # 12 months * 30 days + 60 buffer = 420 days
```

### 6. Data Fetching & Aggregation
1. **Fetch**: 450 days of daily data from DHAN API
2. **Aggregate**: Daily → Weekly using TradingView ISO week standards
3. **Result**: ~60 weekly candles for pattern detection
4. **Detection**: Check consecutive weekly candles for Marubozu→Doji patterns

## UI Improvements Made

### Before (Confusing):
```html
<label>Days Back</label>
<span class="unit">days</span>
<small>Historical data range to analyze</small>
```

### After (Clear):
```html
<label>History Periods</label>
<span class="unit" id="historyUnit">weeks</span>
<small id="historyHelp">Number of weeks to analyze (fetches ~7 days per week)</small>
```

### Dynamic Updates:
- **Daily (1D)**: Shows "30 days"
- **Weekly (1W)**: Shows "60 weeks"
- **Monthly (1M)**: Shows "12 months"

## Week Calculation for UI Input

When user enters **60** for weekly timeframe:

1. **Backend receives**: `history=60`
2. **Interprets as**: 60 weeks (periods)
3. **Calculates days**: `60 * 7 + 30 = 450 days`
4. **Fetches**: 450 days of daily OHLC data
5. **Aggregates**: Into ~60 weekly candles
6. **Scans**: Consecutive weekly pairs for patterns

## Validation: System Working Correctly ✅

The recent scan results confirm correct operation:
- **TCS**: Week 17 → Week 18 (consecutive weeks) ✓
- **ICICIBANK**: Week 42 → Week 43 (consecutive weeks) ✓
- **All patterns**: Use proper ISO week aggregation ✓

The backend correctly handles UI data - the issue was just unclear labeling in the UI, which has now been fixed.