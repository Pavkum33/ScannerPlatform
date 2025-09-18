"""
Check exact week boundaries and daily candles that form each weekly candle
"""

import pandas as pd
from datetime import datetime
from scanner.dhan_client import DhanClient

# Initialize
client = DhanClient()

print("WEEK BOUNDARY ANALYSIS FOR MCX")
print("=" * 80)

# Fetch MCX daily data
print("\nFetching daily data...")
data = client.get_batch_historical_data(['MCX'], days_back=450, timeframe='1D')

if 'MCX' not in data or data['MCX'].empty:
    print("[ERROR] No data found for MCX")
    exit()

df = data['MCX'].copy()
df = df.rename(columns={'timestamp': 'date'})
df['date'] = pd.to_datetime(df['date'])

# Focus on July-August 2025
start_date = pd.to_datetime('2025-07-20')
end_date = pd.to_datetime('2025-08-15')

july_aug_data = df[(df['date'] >= start_date) & (df['date'] <= end_date)].copy()

print(f"\nDaily candles from {start_date.date()} to {end_date.date()}:")
print("-" * 80)

# Group by ISO week and show which days form each week
july_aug_data['iso_year'] = july_aug_data['date'].dt.isocalendar().year
july_aug_data['iso_week'] = july_aug_data['date'].dt.isocalendar().week
july_aug_data['weekday'] = july_aug_data['date'].dt.day_name()

for (year, week), week_data in july_aug_data.groupby(['iso_year', 'iso_week']):
    print(f"\n[ISO Week {year}-W{week}]")
    print("-" * 40)

    # Show all daily candles in this week
    for idx, row in week_data.iterrows():
        print(f"  {row['date'].date()} ({row['weekday'][:3]}): "
              f"O={row['open']:.2f}, H={row['high']:.2f}, "
              f"L={row['low']:.2f}, C={row['close']:.2f}")

    # Calculate weekly candle from these days
    weekly_open = week_data.iloc[0]['open']
    weekly_close = week_data.iloc[-1]['close']
    weekly_high = week_data['high'].max()
    weekly_low = week_data['low'].min()
    weekly_volume = week_data['volume'].sum()

    body = abs(weekly_close - weekly_open)
    range_val = weekly_high - weekly_low
    body_pct = (body / range_val * 100) if range_val > 0 else 0
    body_move = (body / weekly_open * 100) if weekly_open > 0 else 0

    print(f"\n  WEEKLY CANDLE:")
    print(f"    Open: {weekly_open:.2f} (from {week_data.iloc[0]['date'].date()})")
    print(f"    Close: {weekly_close:.2f} (from {week_data.iloc[-1]['date'].date()})")
    print(f"    High: {weekly_high:.2f}")
    print(f"    Low: {weekly_low:.2f}")
    print(f"    Body: {body:.2f} ({body_pct:.2f}% of range)")
    print(f"    Body Move: {body_move:.2f}%")
    print(f"    Direction: {'BULLISH' if weekly_close > weekly_open else 'BEARISH'}")

    # Check if this could be Marubozu or Doji
    if body_pct >= 80:
        print(f"    [MARUBOZU] Body% >= 80%")
    elif body_pct < 25:
        print(f"    [DOJI] Body% < 25%")

print("\n" + "=" * 80)
print("KEY FINDINGS:")
print("-" * 80)

# Check Week 31 and 32 specifically
week31_data = july_aug_data[(july_aug_data['iso_year'] == 2025) & (july_aug_data['iso_week'] == 31)]
week32_data = july_aug_data[(july_aug_data['iso_year'] == 2025) & (july_aug_data['iso_week'] == 32)]

if not week31_data.empty and not week32_data.empty:
    print("\nWeek 31 (Marubozu candidate):")
    print(f"  Trading days: {len(week31_data)}")
    print(f"  Date range: {week31_data.iloc[0]['date'].date()} to {week31_data.iloc[-1]['date'].date()}")

    print("\nWeek 32 (Doji candidate):")
    print(f"  Trading days: {len(week32_data)}")
    print(f"  Date range: {week32_data.iloc[0]['date'].date()} to {week32_data.iloc[-1]['date'].date()}")

    print("\nAre they consecutive ISO weeks? YES (W31 -> W32)")

print("\n" + "=" * 80)