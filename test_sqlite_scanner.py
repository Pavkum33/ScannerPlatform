"""
Test SQLite Scanner with All 154 F&O Symbols
Shows the massive speed improvement!
"""

import time
import json
from scanner.sqlite_scanner_engine import SQLiteScannerEngine

def test_full_scan():
    """Test scanning all F&O symbols"""

    print("=" * 60)
    print("TESTING SQLITE SCANNER - FULL F&O SCAN")
    print("=" * 60)

    # Initialize scanner
    scanner = SQLiteScannerEngine("database/pattern_scanner.db")

    # Get all available symbols
    symbols = scanner.get_available_symbols()
    print(f"\nSymbols available: {len(symbols)}")

    # Test different timeframes
    timeframes = [
        ("1D", 30, "Daily (30 days)"),
        ("1W", 20, "Weekly (20 weeks)"),
        ("1M", 12, "Monthly (12 months)")
    ]

    for timeframe, history, description in timeframes:
        print(f"\n" + "-" * 60)
        print(f"SCANNING: {description}")
        print("-" * 60)

        start_time = time.time()

        # Run scan
        results = scanner.scan(
            symbols=symbols,
            timeframe=timeframe,
            history=history,
            min_body_move_pct=4.0
        )

        elapsed = time.time() - start_time

        # Display results
        print(f"Symbols scanned:     {results['statistics']['symbols_scanned']}")
        print(f"Successful scans:    {results['statistics']['successful_scans']}")
        print(f"Skipped (no data):   {results['statistics'].get('skipped_no_data', 0)}")
        print(f"Patterns found:      {results['statistics']['patterns_found']}")
        print(f"Scan duration:       {elapsed:.2f} seconds")
        print(f"Speed:               {len(symbols)/elapsed:.1f} symbols/second")

        # Show patterns if found
        if results['results']:
            print(f"\nPATTERNS FOUND:")
            for i, pattern in enumerate(results['results'][:10], 1):  # Show first 10
                print(f"{i}. {pattern['symbol']} - {pattern['pattern_direction']} "
                      f"(Marubozu: {pattern['marubozu']['date']}, "
                      f"Doji: {pattern['doji']['date']})")

            if len(results['results']) > 10:
                print(f"... and {len(results['results']) - 10} more patterns")

    print("\n" + "=" * 60)
    print("PERFORMANCE COMPARISON")
    print("=" * 60)

    # Compare with API-based scanner estimates
    api_time_estimate = len(symbols) * 2  # ~2 seconds per symbol with API
    sqlite_time = elapsed

    print(f"API Scanner (estimated):  {api_time_estimate:.0f} seconds ({api_time_estimate/60:.1f} minutes)")
    print(f"SQLite Scanner (actual):  {sqlite_time:.2f} seconds")
    print(f"Speed improvement:        {api_time_estimate/sqlite_time:.0f}x faster!")
    print(f"Time saved:               {(api_time_estimate - sqlite_time)/60:.1f} minutes")

def test_today_signals():
    """Test finding today's signals"""

    print("\n" + "=" * 60)
    print("TESTING TODAY'S SIGNALS")
    print("=" * 60)

    scanner = SQLiteScannerEngine("database/pattern_scanner.db")
    symbols = scanner.get_available_symbols()

    # Look for patterns in last 2 days (yesterday Marubozu → today Doji)
    results = scanner.scan(
        symbols=symbols,
        timeframe="1D",
        history=2,  # Only last 2 days
        min_body_move_pct=4.0
    )

    from datetime import datetime, timedelta
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    today_patterns = []
    for pattern in results.get('results', []):
        try:
            doji_date = datetime.strptime(pattern['doji']['date'], '%Y-%m-%d').date()
            marubozu_date = datetime.strptime(pattern['marubozu']['date'], '%Y-%m-%d').date()

            # Check if it's yesterday → today pattern
            if doji_date == today and marubozu_date == yesterday:
                today_patterns.append(pattern)
        except:
            pass

    if today_patterns:
        print(f"\nTODAY'S SIGNALS FOUND: {len(today_patterns)}")
        for i, pattern in enumerate(today_patterns, 1):
            print(f"{i}. {pattern['symbol']} - {pattern['pattern_direction']}")
            print(f"   Marubozu: {pattern['marubozu']['date']} "
                  f"(Move: {pattern['marubozu']['body_move_pct']}%)")
            print(f"   Doji: {pattern['doji']['date']} "
                  f"(Body: {pattern['doji']['body_pct']}%)")
    else:
        print("\nNo today's signals found (this is normal - patterns are rare)")

if __name__ == "__main__":
    # Check database status first
    scanner = SQLiteScannerEngine("database/pattern_scanner.db")
    completeness = scanner.check_data_completeness()

    print("DATABASE STATUS")
    print("-" * 30)
    print(f"Total symbols:     {completeness['total_symbols']}")
    print(f"Symbols with data: {completeness['symbols_with_data']}")
    print(f"Total records:     {completeness['total_records']}")
    print(f"Database size:     {completeness['database_size']}")

    if completeness['symbols_with_data'] < completeness['total_symbols']:
        print("\nWarning: Not all symbols have data. Run data loading script first.")
    else:
        print("\nDatabase fully loaded. Ready for scanning!")

        # Run tests
        test_full_scan()
        test_today_signals()