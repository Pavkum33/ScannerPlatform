"""
Debug weekly pattern detection to understand the issue
"""

import pandas as pd
from scanner.dhan_client import DhanClient
from scanner.scanner_engine import ScannerEngine
from scanner.aggregator import aggregate_to_weekly, check_consecutive_periods

# Initialize
client = DhanClient()
engine = ScannerEngine(client)

# Test with specific symbols that showed issues
test_symbols = ['TCS', 'CANFINHOME']

print("Testing Weekly Pattern Detection")
print("=" * 80)

for symbol in test_symbols:
    print(f"\nAnalyzing {symbol}:")
    print("-" * 40)

    # Fetch daily data
    days_back = 450  # Get enough data for weekly aggregation
    data = client.get_batch_historical_data([symbol], days_back, timeframe='1D')

    if symbol not in data or data[symbol].empty:
        print(f"No data for {symbol}")
        continue

    # Aggregate to weekly
    df = data[symbol]
    df = df.rename(columns={'timestamp': 'date'})
    df_weekly = aggregate_to_weekly(df)

    print(f"Total weeks: {len(df_weekly)}")

    # Check consecutive periods
    consecutive_pairs = check_consecutive_periods(df_weekly, '1W')
    print(f"Consecutive week pairs found: {len(consecutive_pairs)}")

    # Show first few weekly candles
    print("\nFirst 10 weekly candles:")
    for i in range(min(10, len(df_weekly))):
        row = df_weekly.iloc[i]
        date = pd.to_datetime(row['date'])
        iso = date.isocalendar()
        print(f"  Week {i}: {date.date()} (Year {iso.year}, Week {iso.week})")

    # Check which pairs are consecutive
    print("\nChecking week continuity:")
    for i in range(min(9, len(df_weekly)-1)):
        current = pd.to_datetime(df_weekly.iloc[i]['date'])
        next_week = pd.to_datetime(df_weekly.iloc[i+1]['date'])

        curr_iso = current.isocalendar()
        next_iso = next_week.isocalendar()

        is_consecutive = (
            (curr_iso.year == next_iso.year and next_iso.week == curr_iso.week + 1) or
            (curr_iso.year == next_iso.year - 1 and curr_iso.week >= 52 and next_iso.week == 1)
        )

        print(f"  Weeks {i}->{i+1}: Y{curr_iso.year}W{curr_iso.week} -> Y{next_iso.year}W{next_iso.week} = {is_consecutive}")

    # Show which pairs are in the consecutive list
    if consecutive_pairs:
        print(f"\nConsecutive pairs identified: {consecutive_pairs[:5]}...")

    # Now run the actual scanner
    print("\nRunning scanner on this symbol...")
    results = engine.scan_single(symbol, timeframe='1W', history=60, min_body_move_pct=3)

    patterns_found = results['results']
    print(f"Patterns found: {len(patterns_found)}")

    for pattern in patterns_found:
        m_date = pattern['marubozu']['date']
        d_date = pattern['doji']['date']
        print(f"  Pattern: {m_date} -> {d_date}")

        # Check if these dates are actually consecutive weeks
        m_pd = pd.to_datetime(m_date)
        d_pd = pd.to_datetime(d_date)
        m_iso = m_pd.isocalendar()
        d_iso = d_pd.isocalendar()

        is_consecutive = (
            (m_iso.year == d_iso.year and d_iso.week == m_iso.week + 1) or
            (m_iso.year == d_iso.year - 1 and m_iso.week >= 52 and d_iso.week == 1)
        )

        print(f"    Week numbers: Y{m_iso.year}W{m_iso.week} -> Y{d_iso.year}W{d_iso.week}")
        print(f"    Are consecutive? {is_consecutive}")
        if not is_consecutive:
            print(f"    ⚠️ WARNING: NON-CONSECUTIVE WEEKS MATCHED!")

print("\n" + "=" * 80)
print("CONCLUSION: The issue is that non-consecutive weeks are being matched as patterns!")
print("This violates the requirement: 'Candles must be consecutive time periods'")