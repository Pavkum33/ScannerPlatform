"""
Analyze all patterns found to identify potential issues
"""

import json
import pandas as pd

# Load results
with open('fno_scan_results.json', 'r') as f:
    data = json.load(f)

results = data['results']

print(f"Total patterns found: {len(results)}")
print("=" * 80)

for i, result in enumerate(results, 1):
    symbol = result['symbol']
    direction = result['pattern_direction']

    m = result['marubozu']
    d = result['doji']

    print(f"\n{i}. {symbol} ({direction.upper()})")
    print("-" * 40)

    # Marubozu details
    print(f"Marubozu ({m['date']}):")
    print(f"  OHLC: O={m['open']:.2f}, H={m['high']:.2f}, L={m['low']:.2f}, C={m['close']:.2f}")
    print(f"  Body: {abs(m['close'] - m['open']):.2f}")
    print(f"  Range: {m['high'] - m['low']:.2f}")
    print(f"  Body%: {m['body_pct_of_range']:.2f}%")
    print(f"  Move%: {m['body_move_pct']:.2f}%")

    # Doji details
    print(f"Doji ({d['date']}):")
    print(f"  OHLC: O={d['open']:.2f}, H={d['high']:.2f}, L={d['low']:.2f}, C={d['close']:.2f}")
    print(f"  Body: {abs(d['close'] - d['open']):.2f}")
    print(f"  Range: {d['high'] - d['low']:.2f}")
    print(f"  Body%: {d['body_pct_of_range']:.2f}%")

    # Pattern validation
    print(f"\nPattern Validation:")

    # 1. Marubozu check
    marubozu_valid = m['body_pct_of_range'] >= 80
    print(f"  [CHECK] Marubozu body% >= 80%: {marubozu_valid}")

    # 2. Doji check
    doji_valid = d['body_pct_of_range'] < 25
    print(f"  [CHECK] Doji body% < 25%: {doji_valid}")

    # 3. Breakout check
    breakout = d['high'] > m['high']
    print(f"  [CHECK] Doji high ({d['high']:.2f}) > Marubozu high ({m['high']:.2f}): {breakout}")

    # 4. Close inside body check
    if direction == 'bullish':
        inside = m['open'] < d['close'] < m['close']
        print(f"  [CHECK] Doji close ({d['close']:.2f}) inside bullish body ({m['open']:.2f}-{m['close']:.2f}): {inside}")
    else:
        inside = m['close'] < d['close'] < m['open']
        print(f"  [CHECK] Doji close ({d['close']:.2f}) inside bearish body ({m['close']:.2f}-{m['open']:.2f}): {inside}")

    # Overall validity
    all_valid = marubozu_valid and doji_valid and breakout and inside
    if not all_valid:
        print(f"  [WARNING] PATTERN ISSUE DETECTED!")
    else:
        print(f"  [OK] Pattern is VALID")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

# Check date continuity
print("\nDate Continuity Check:")
for result in results:
    m_date = pd.to_datetime(result['marubozu']['date'])
    d_date = pd.to_datetime(result['doji']['date'])
    days_diff = (d_date - m_date).days

    if result['timeframe'] == '1D':
        expected_diff = 1
    elif result['timeframe'] == '1W':
        expected_diff = 7
    else:
        expected_diff = 30  # Approximate for monthly

    if days_diff != expected_diff and result['timeframe'] == '1D':
        print(f"  [WARNING] {result['symbol']}: {days_diff} days between candles (expected 1 for daily)")

# Pattern distribution
print(f"\nPattern Distribution:")
bullish = sum(1 for r in results if r['pattern_direction'] == 'bullish')
bearish = sum(1 for r in results if r['pattern_direction'] == 'bearish')
print(f"  Bullish: {bullish}")
print(f"  Bearish: {bearish}")

# Body move % distribution
print(f"\nMarubozu Body Move % Distribution:")
moves = [r['marubozu']['body_move_pct'] for r in results]
print(f"  Min: {min(moves):.2f}%")
print(f"  Max: {max(moves):.2f}%")
print(f"  Avg: {sum(moves)/len(moves):.2f}%")