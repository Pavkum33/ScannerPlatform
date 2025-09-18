"""
Verify weekly pattern detection and compare with UI results
"""

import json
from scanner.dhan_client import DhanClient
from scanner.scanner_engine import ScannerEngine

# Initialize
client = DhanClient()
engine = ScannerEngine(client)

# Test with the symbols you mentioned seeing wrong results for
problematic_symbols = ['TCS', 'ICICIBANK', 'HINDUNILVR', 'GRASIM', 'EICHERMOT',
                      'CUMMINSIND', 'INDIGO', 'CANFINHOME']

print("WEEKLY PATTERN VERIFICATION")
print("=" * 80)
print("\nTesting symbols that showed wrong patterns in UI...")
print("-" * 80)

# Run scan for F&O symbols with weekly timeframe
print("\nRunning weekly scan with same parameters as UI...")
print("Timeframe: 1W")
print("Min Body Move: 3%")
print("History: 60 weeks")
print()

all_patterns = []

for symbol in problematic_symbols:
    print(f"\nScanning {symbol}...")
    results = engine.scan_single(
        symbol=symbol,
        timeframe='1W',
        history=60,
        min_body_move_pct=3.0
    )

    patterns = results['results']
    if patterns:
        for pattern in patterns:
            all_patterns.append(pattern)
            print(f"  [FOUND] Pattern detected:")
            print(f"    Direction: {pattern['pattern_direction']}")
            print(f"    Marubozu: {pattern['marubozu']['date']} (Move: {pattern['marubozu']['body_move_pct']:.2f}%)")
            print(f"    Doji: {pattern['doji']['date']}")
            print(f"    Dates: {pattern['marubozu']['date']} -> {pattern['doji']['date']}")
    else:
        print(f"  [NONE] No patterns found")

print("\n" + "=" * 80)
print("COMPARISON WITH UI RESULTS")
print("=" * 80)

ui_results = {
    'TCS': {'date': '2025-05-02', 'move': 4.80},
    'ICICIBANK': {'date': '2025-05-23', 'move': 3.16},
    'HINDUNILVR': {'date': '2025-07-18', 'move': 7.63},
    'GRASIM': {'date': '2025-04-25', 'move': 3.20},
    'EICHERMOT': {'date': '2025-07-04', 'move': 3.00},
    'CUMMINSIND': {'date': '2025-07-18', 'move': 5.85},
    'INDIGO': {'date': '2025-02-28', 'move': 8.02},
    'CANFINHOME': [
        {'date': '2025-03-28', 'move': 12.05},
        {'date': '2025-06-06', 'move': 5.96}
    ]
}

print("\nUI shows these results (from your message):")
for symbol, data in ui_results.items():
    if isinstance(data, list):
        for d in data:
            print(f"  {symbol}: Signal {d['date']}, Move {d['move']}%")
    else:
        print(f"  {symbol}: Signal {data['date']}, Move {data['move']}%")

print("\nBackend found these patterns:")
for pattern in all_patterns:
    print(f"  {pattern['symbol']}: Signal {pattern['doji']['date']}, Move {pattern['marubozu']['body_move_pct']:.2f}%")

print("\n" + "=" * 80)
print("DISCREPANCIES")
print("=" * 80)

# Check for mismatches
backend_dict = {}
for p in all_patterns:
    symbol = p['symbol']
    if symbol not in backend_dict:
        backend_dict[symbol] = []
    backend_dict[symbol].append({
        'date': p['doji']['date'],
        'move': p['marubozu']['body_move_pct']
    })

for symbol in problematic_symbols:
    ui_data = ui_results.get(symbol)
    backend_data = backend_dict.get(symbol, [])

    if not ui_data and not backend_data:
        continue

    print(f"\n{symbol}:")

    if isinstance(ui_data, list):
        ui_dates = [d['date'] for d in ui_data]
        backend_dates = [d['date'] for d in backend_data]
    else:
        ui_dates = [ui_data['date']] if ui_data else []
        backend_dates = [d['date'] for d in backend_data]

    if set(ui_dates) != set(backend_dates):
        print(f"  [WARNING] DATE MISMATCH!")
        print(f"    UI shows: {ui_dates}")
        print(f"    Backend found: {backend_dates}")
    else:
        print(f"  [OK] Dates match")

# Save results for debugging
with open('weekly_verification.json', 'w') as f:
    json.dump({
        'backend_results': all_patterns,
        'ui_results': ui_results,
        'analysis': {
            'total_backend_patterns': len(all_patterns),
            'symbols_with_patterns': len(backend_dict)
        }
    }, f, indent=2, default=str)

print(f"\nResults saved to weekly_verification.json")