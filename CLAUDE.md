# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview
Production-ready Marubozu → Doji pattern scanner with web UI dashboard. Detects failed breakout patterns across Daily, Weekly, and Monthly timeframes using DHANHQ v2 API and SQLite database storage.

## Key Commands

### Running the Application
```bash
# Start web server (main entry point)
python app.py
# Access UI at http://localhost:5000

# Run CLI scanner (legacy)
python run_scanner.py --timeframe 1D --history 30 --min-body-move-pct 4

# Generate weekly/monthly aggregations from database
cd database && python generate_aggregations.py
```

### Database Operations
```bash
# Setup SQLite database with F&O symbols
cd database && python sqlite_setup.py

# Daily EOD update (run after market close)
cd database && python daily_eod_update.py

# Monitor data loading progress
cd database && python monitor_progress.py
```

## Architecture Overview

### Two-Mode Operation
1. **Live Scan Mode**: Real-time API calls to DHAN for fresh patterns (Daily only)
   - Triggered by "Start Full Scan" or "Today's Signals" buttons
   - Uses `scanner/sqlite_scanner_engine.py` → `scanner/dhan_client.py`
   - Adjustable parameters via UI (min body move %)

2. **Database Mode**: Pre-calculated patterns from SQLite (All timeframes)
   - Triggered by "Load All Patterns" button
   - Uses `database/pattern_scanner.db` with fixed thresholds (2% move, 80% body)
   - Includes Weekly/Monthly aggregations

### Database Schema
- **SQLite Database**: `database/pattern_scanner.db`
- **Key Tables**:
  - `symbols`: F&O stock list (154 symbols)
  - `daily_ohlc`: Daily candle data
  - `aggregated_ohlc`: Weekly/Monthly aggregations
  - `detected_patterns`: Stored pattern results
  - `pattern_types`: Pattern definitions

### Pattern Detection Thresholds
- **Marubozu**: Body ≥ 80% of range, Move ≥ user_input% (live) or 2% (database)
- **Doji**: Body < 25% of range
- **Breakout**: Doji.high > Marubozu.high
- **Rejection**: Doji closes inside Marubozu body

### API Endpoints
- `POST /api/scan`: Start live scan with parameters
- `GET /api/scan/status`: Check scan progress
- `GET /api/results/latest`: Get most recent scan results
- `GET /api/results/today`: Filter for today's signals only
- `GET /api/results/database`: Load all patterns from database (Daily/Weekly/Monthly)
- `GET /api/export/{format}`: Export results (json/csv/excel)

## Data Flow

### Live Scanning Flow
```
UI Button → app.py → /api/scan → sqlite_scanner_engine.py → dhan_client.py → DHAN API
                                        ↓
                              pattern_detector.py (detection logic)
                                        ↓
                              Results → UI (not saved to DB)
```

### Database Pattern Flow
```
UI Button → app.py → /api/results/database → SQLite Query → detected_patterns table
                                                    ↓
                                            Format & Return → UI
```

### Weekly/Monthly Generation
```
generate_aggregations.py → Read daily_ohlc → Aggregate by week/month → aggregated_ohlc
                                                    ↓
                                          Pattern detection → detected_patterns
```

## Important Implementation Details

### Timeframe Handling
- **Daily (1D)**: Direct from DHAN API or daily_ohlc table
- **Weekly**: ISO week aggregation, period_start/period_end in database
- **Monthly**: Calendar month aggregation, same structure as weekly

### Frontend Date Display
- Daily patterns use `date` field
- Weekly/Monthly use `period_end` field with special formatting
- JavaScript handles both formats in `formatDateWithWeek()` and `formatPatternSpan()`

### API Credentials
- Client ID: 1106283829
- Access Token: Stored in `scanner/config.py`
- Rate limits: 3 calls/second, batch size 20 symbols

### Performance Optimizations
- Parallel fetching with ThreadPoolExecutor (5 workers)
- SQLite for instant pattern loading (no API calls)
- Batch processing for DHAN API calls
- Progress tracking via status endpoint

## Common Development Tasks

### Adding New Pattern Types
1. Define pattern logic in `scanner/pattern_detector.py`
2. Add to `pattern_types` table in database
3. Update `generate_aggregations.py` for weekly/monthly detection
4. Update UI display logic if needed

### Refreshing Database Patterns
```bash
# Regenerate all weekly/monthly patterns with current thresholds
cd database && python generate_aggregations.py
```

### Debugging Pattern Detection
```bash
# Check patterns in database
python check_patterns.py

# Test specific symbol
python test_pattern.py
```

## UI Button Functionality

- **"Start Full Scan"**: Live API scan, uses slider value, Daily only
- **"Today's Signals"**: Live scan filtered for yesterday→today patterns
- **"Load All Patterns"**: Database patterns, all timeframes, fixed thresholds

## Edge Cases Handled
- Division by zero (skip if high == low)
- Missing data (skip symbols with insufficient candles)
- API rate limits (automatic retry with backoff)
- Binary data types in SQLite (converted to proper types)
- Different date field names between daily and aggregated data