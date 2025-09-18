# Marubozu → Doji Pattern Scanner (DHANHQ v2)

A production-ready pattern scanner that detects **Marubozu → Doji (failed breakout)** patterns across Daily, Weekly, and Monthly timeframes using DHANHQ v2 historical data.

## 🎯 Pattern Logic

The scanner detects a specific two-candle pattern:

1. **Marubozu Candle** (Strong directional move)
   - Body ≥ 80% of total range
   - Can be bullish or bearish
   - Additional filter: Body move ≥ user-specified % (default 4%)

2. **Doji Candle** (Indecision/Rejection)
   - Body < 25% of total range
   - High breaks above Marubozu high
   - Close falls back inside Marubozu body (rejection)

This pattern often indicates a failed breakout and potential reversal.

## ✨ Features

- ✅ **Multi-timeframe support**: Daily (1D), Weekly (1W), Monthly (1M)
- ✅ **TradingView-style aggregation** for weekly/monthly candles
- ✅ **Parallel batch processing** for fast scanning of 100s of symbols
- ✅ **Configurable thresholds** and filters
- ✅ **No caching** - fetches fresh data every run
- ✅ **Production-ready** with error handling and retries
- ✅ **Both CLI and programmatic usage**

## 📁 Project Structure

```
scratch-dhan-prod/
├── scanner/
│   ├── config.py            # DHAN API credentials
│   ├── dhan_client.py       # DHAN API client with batching
│   ├── aggregator.py        # Timeframe aggregation utilities
│   ├── pattern_detector.py  # Pattern detection logic
│   └── scanner_engine.py    # Main scanning orchestrator
├── run_scanner.py           # CLI entry point
├── security_id_list.csv    # List of symbols to scan
├── requirements.txt         # Python dependencies
├── README.md               # This file
└── CLAUDE.md              # Development documentation
```

## 🚀 Quick Start

### 1. Installation

```bash
# Clone or download the project
cd scratch-dhan-prod

# Install dependencies
pip install -r requirements.txt
```

### 2. Basic Usage

```bash
# Scan daily timeframe with default settings
python run_scanner.py --timeframe 1D --history 30

# Scan weekly timeframe with 4% minimum move
python run_scanner.py --timeframe 1W --history 20 --min-body-move-pct 4

# Scan specific symbols
python run_scanner.py --symbols RELIANCE TCS INFY --timeframe 1D

# Save results as CSV
python run_scanner.py --timeframe 1W --output results.csv --format csv
```

## 📊 Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--timeframe` | Timeframe: 1D, 1W, or 1M | 1D |
| `--history` | Number of periods to scan | 20 |
| `--min-body-move-pct` | Minimum Marubozu body move % | 4.0 |
| `--symbols-file` | CSV file with symbols | security_id_list.csv |
| `--symbols` | Specific symbols to scan | None |
| `--output` | Output file path | results.json |
| `--format` | Output format: json or csv | json |
| `--no-summary` | Skip console output | False |
| `--debug` | Enable debug logging | False |

## 📝 Symbol List Format

Create `security_id_list.csv` with symbols in the first column:

```csv
Symbol
RELIANCE
TCS
INFY
HDFCBANK
ICICIBANK
```

## 📤 Output Format

### JSON Output
```json
{
  "results": [
    {
      "symbol": "RELIANCE",
      "timeframe": "1W",
      "pattern_direction": "bullish",
      "marubozu": {
        "date": "2025-01-10",
        "open": 2500,
        "high": 2610,
        "low": 2495,
        "close": 2600,
        "body_pct_of_range": 86.96,
        "body_move_pct": 4.0
      },
      "doji": {
        "date": "2025-01-17",
        "open": 2605,
        "high": 2625,
        "low": 2550,
        "close": 2580,
        "body_pct_of_range": 12.5
      },
      "breakout_amount": 15,
      "rejection_strength": 66.67,
      "notes": "Doji high broke Marubozu high but closed inside Marubozu body"
    }
  ],
  "statistics": {
    "total_symbols_requested": 50,
    "symbols_scanned": 50,
    "symbols_with_data": 48,
    "symbols_with_patterns": 5,
    "total_patterns_found": 7,
    "scan_duration_seconds": 12.34
  }
}
```

## 🔧 Programmatic Usage

```python
from scanner.dhan_client import DhanClient
from scanner.scanner_engine import ScannerEngine

# Initialize
client = DhanClient()
engine = ScannerEngine(client)

# Scan single symbol
results = engine.scan_single(
    symbol="RELIANCE",
    timeframe="1W",
    history=20,
    min_body_move_pct=4.0
)

# Scan multiple symbols
symbols = ["RELIANCE", "TCS", "INFY"]
results = engine.scan(
    symbols=symbols,
    timeframe="1D",
    history=30,
    min_body_move_pct=4.0
)

# Access results
patterns = results['results']
stats = results['statistics']
```

## ⚙️ Configuration

### API Credentials
Edit `scanner/config.py` to update DHAN credentials:
```python
config.dhan.client_id = "YOUR_CLIENT_ID"
config.dhan.access_token = "YOUR_ACCESS_TOKEN"
```

**Security Note**: For production, use environment variables:
```python
import os
config.dhan.client_id = os.getenv("DHAN_CLIENT_ID")
config.dhan.access_token = os.getenv("DHAN_ACCESS_TOKEN")
```

### Pattern Thresholds
Modify thresholds when initializing the scanner:
```python
engine = ScannerEngine(
    client,
    marubozu_threshold=0.8,  # 80% body of range
    doji_threshold=0.25       # 25% body of range
)
```

## 🎛️ Performance Tuning

### Batch Size
Edit `scanner/dhan_client.py`:
```python
batch_size = 20  # Symbols per batch
```

### Concurrency
Edit `scanner/dhan_client.py`:
```python
ThreadPoolExecutor(max_workers=5)  # Parallel workers
```

### Rate Limiting
Edit `scanner/dhan_client.py`:
```python
@rate_limit(calls_per_second=3)
```

## 🧪 Testing

Run a quick test with a few symbols:
```bash
# Test with 3 symbols
python run_scanner.py --symbols RELIANCE TCS INFY --timeframe 1D --history 10

# Enable debug mode
python run_scanner.py --timeframe 1W --debug
```

## 📈 Timeframe Aggregation

The scanner uses TradingView-style aggregation:

- **Weekly**: ISO week (Monday-Sunday)
  - Open = First day's open
  - Close = Last day's close
  - High = Highest high
  - Low = Lowest low

- **Monthly**: Calendar month
  - Same aggregation rules as weekly

## ⚠️ Edge Cases Handled

1. **Division by zero**: Skips candles where high == low
2. **Missing data**: Skips symbols with insufficient candles
3. **Non-consecutive periods**: Validates calendar continuity
4. **API failures**: Retries with exponential backoff
5. **Rate limiting**: Respects DHAN API limits

## 🔍 Troubleshooting

### No patterns found
- Reduce `--min-body-move-pct` (try 2 or 3)
- Increase `--history` to scan more periods
- Check if symbols have sufficient trading data

### API errors
- Verify credentials in `scanner/config.py`
- Check internet connection
- Ensure DHAN API access is active

### Slow performance
- Reduce batch size if hitting rate limits
- Decrease number of symbols per run
- Use weekly/monthly timeframes (less data)

## 📝 License

This project is for educational and trading purposes. Use at your own risk.

## 🤝 Support

For issues or questions:
- Check `CLAUDE.md` for technical details
- Review error logs with `--debug` flag
- Ensure all dependencies are installed

---

**Note**: This scanner is designed for NSE equity stocks. Ensure you have proper DHAN API access and understand the risks involved in trading based on technical patterns.