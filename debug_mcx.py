"""
Debug MCX weekly pattern detection to understand the issue
"""

import pandas as pd
from datetime import datetime, timedelta
from scanner.dhan_client import DhanClient
from scanner.scanner_engine import ScannerEngine
from scanner.aggregator import aggregate_to_weekly

# Initialize
client = DhanClient()
engine = ScannerEngine(client)

print("MCX WEEKLY PATTERN INVESTIGATION")
print("=" * 80)

# Fetch MCX data for analysis
print("\n1. Fetching MCX daily data for weekly aggregation...")
days_back = 450  # Get enough data for weekly analysis
data = client.get_batch_historical_data(['MCX'], days_back, timeframe='1D')

if 'MCX' not in data or data['MCX'].empty:
    print("[ERROR] No data found for MCX")
    exit()

# Get daily data
df_daily = data['MCX']
df_daily = df_daily.rename(columns={'timestamp': 'date'})
print(f"   Daily candles fetched: {len(df_daily)}")

# Aggregate to weekly
print("\n2. Aggregating to weekly...")
df_weekly = aggregate_to_weekly(df_daily)
print(f"   Weekly candles created: {len(df_weekly)}")

# Look for data around August 8, 2025
print("\n3. Searching for weeks around August 8, 2025...")
target_date = pd.to_datetime('2025-08-08')

# Find the week containing August 8
df_weekly['date_pd'] = pd.to_datetime(df_weekly['date'])
august_weeks = df_weekly[
    (df_weekly['date_pd'] >= target_date - timedelta(days=14)) &
    (df_weekly['date_pd'] <= target_date + timedelta(days=14))
].copy()

print(f"\nWeekly candles around August 8, 2025:")
print("-" * 60)
for idx, row in august_weeks.iterrows():
    date = row['date_pd']
    iso = date.isocalendar()
    body = abs(row['close'] - row['open'])
    range_val = row['high'] - row['low']
    body_pct = (body / range_val * 100) if range_val > 0 else 0
    body_move = (body / row['open'] * 100) if row['open'] > 0 else 0
    direction = "BULLISH" if row['close'] > row['open'] else "BEARISH"

    print(f"\nWeek ending {date.date()} (Year {iso.year}, Week {iso.week}):")
    print(f"  OHLC: O={row['open']:.2f}, H={row['high']:.2f}, L={row['low']:.2f}, C={row['close']:.2f}")
    print(f"  Body: {body:.2f} ({body_pct:.2f}% of range)")
    print(f"  Body Move: {body_move:.2f}%")
    print(f"  Direction: {direction}")
    print(f"  Volume: {row['volume']:.0f}")

# Now check consecutive week pairs
print("\n" + "=" * 80)
print("4. Checking for Marubozu->Doji patterns in this period...")
print("-" * 60)

# Get indices for checking consecutive pairs
august_indices = august_weeks.index.tolist()
if len(august_indices) >= 2:
    for i in range(len(august_indices) - 1):
        idx1 = august_indices[i]
        idx2 = august_indices[i + 1]

        week1 = df_weekly.loc[idx1]
        week2 = df_weekly.loc[idx2]

        date1 = pd.to_datetime(week1['date'])
        date2 = pd.to_datetime(week2['date'])
        iso1 = date1.isocalendar()
        iso2 = date2.isocalendar()

        # Check if consecutive
        is_consecutive = (
            (iso1.year == iso2.year and iso2.week == iso1.week + 1) or
            (iso1.year == iso2.year - 1 and iso1.week >= 52 and iso2.week == 1)
        )

        print(f"\nWeek Pair: {date1.date()} -> {date2.date()}")
        print(f"  ISO: Y{iso1.year}W{iso1.week} -> Y{iso2.year}W{iso2.week}")
        print(f"  Consecutive? {is_consecutive}")

        if is_consecutive:
            # Check pattern
            body1 = abs(week1['close'] - week1['open'])
            range1 = week1['high'] - week1['low']
            body_pct1 = (body1 / range1 * 100) if range1 > 0 else 0
            body_move1 = (body1 / week1['open'] * 100) if week1['open'] > 0 else 0

            body2 = abs(week2['close'] - week2['open'])
            range2 = week2['high'] - week2['low']
            body_pct2 = (body2 / range2 * 100) if range2 > 0 else 0

            print(f"\n  Week 1 Analysis:")
            print(f"    Body%: {body_pct1:.2f}% (Marubozu if >= 80%)")
            print(f"    Body Move%: {body_move1:.2f}%")
            print(f"    Is Marubozu? {body_pct1 >= 80}")

            print(f"\n  Week 2 Analysis:")
            print(f"    Body%: {body_pct2:.2f}% (Doji if < 25%)")
            print(f"    Is Doji? {body_pct2 < 25}")

            if body_pct1 >= 80 and body_pct2 < 25:
                print(f"\n  [PATTERN CHECK] Potential Marubozu->Doji pattern!")
                print(f"    Week 2 High ({week2['high']:.2f}) > Week 1 High ({week1['high']:.2f})? {week2['high'] > week1['high']}")

                if week1['close'] > week1['open']:  # Bullish Marubozu
                    inside = week1['open'] < week2['close'] < week1['close']
                    print(f"    Bullish: Week 2 Close ({week2['close']:.2f}) inside Week 1 body ({week1['open']:.2f}-{week1['close']:.2f})? {inside}")
                else:  # Bearish Marubozu
                    inside = week1['close'] < week2['close'] < week1['open']
                    print(f"    Bearish: Week 2 Close ({week2['close']:.2f}) inside Week 1 body ({week1['close']:.2f}-{week1['open']:.2f})? {inside}")

# Now run the actual scanner to see what it finds
print("\n" + "=" * 80)
print("5. Running scanner to see what it detects...")
print("-" * 60)

results = engine.scan_single('MCX', timeframe='1W', history=60, min_body_move_pct=3.0)
patterns = results['results']

if patterns:
    print(f"\n[SCANNER FOUND] {len(patterns)} pattern(s):")
    for pattern in patterns:
        print(f"\n  Pattern: {pattern['pattern_direction'].upper()}")
        print(f"    Marubozu: {pattern['marubozu']['date']} (Body Move: {pattern['marubozu']['body_move_pct']:.2f}%)")
        print(f"    Doji: {pattern['doji']['date']}")
        print(f"    Marubozu Body%: {pattern['marubozu']['body_pct_of_range']:.2f}%")
        print(f"    Doji Body%: {pattern['doji']['body_pct_of_range']:.2f}%")

        # Check if this is the August 8 pattern
        if pattern['doji']['date'] == '2025-08-08':
            print(f"\n    [WARNING] This is the August 8 pattern from UI!")
            print(f"    UI shows Body Move: 5.29%, Scanner shows: {pattern['marubozu']['body_move_pct']:.2f}%")
else:
    print("\n[SCANNER] No patterns found")

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)