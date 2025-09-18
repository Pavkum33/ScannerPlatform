"""
Debug symbol mapping to understand why only 57/158 symbols have data
"""

import pandas as pd
from scanner.dhan_client import DhanClient

# Initialize client
client = DhanClient()

print("SYMBOL MAPPING ANALYSIS")
print("=" * 80)

# Load our F&O symbols
print("\n1. Loading our F&O symbols list...")
with open('fno_symbols_corrected.csv', 'r') as f:
    our_symbols = [line.strip() for line in f.readlines()[1:] if line.strip()]

print(f"   Our F&O list has: {len(our_symbols)} symbols")

# Load DHAN's equity mapping
print("\n2. Loading DHAN's equity instruments...")
equity_mapping = client.load_equity_instruments()
print(f"   DHAN has: {len(equity_mapping)} equity symbols")

# Check which symbols are available vs missing
print("\n3. Checking symbol availability...")
available_symbols = []
missing_symbols = []

for symbol in our_symbols:
    if symbol in equity_mapping:
        available_symbols.append(symbol)
    else:
        missing_symbols.append(symbol)

print(f"\n[OK] Available in DHAN: {len(available_symbols)} symbols")
print(f"[MISSING] Missing from DHAN: {len(missing_symbols)} symbols")

# Show missing symbols
if missing_symbols:
    print(f"\nMISSING SYMBOLS ({len(missing_symbols)}):")
    print("-" * 40)
    for i, symbol in enumerate(missing_symbols, 1):
        print(f"{i:3}. {symbol}")

# Show first few available symbols
print(f"\nAVAILABLE SYMBOLS (first 20 of {len(available_symbols)}):")
print("-" * 40)
for i, symbol in enumerate(available_symbols[:20], 1):
    sec_id = equity_mapping[symbol]
    print(f"{i:3}. {symbol} -> {sec_id}")

# Check for potential name variations in DHAN's data
print(f"\n4. Looking for potential name variations...")
print("-" * 40)

# Get all DHAN symbols that contain our missing symbols as substrings
dhan_symbols = list(equity_mapping.keys())

for missing in missing_symbols[:10]:  # Check first 10 missing symbols
    matches = [s for s in dhan_symbols if missing in s or s in missing]
    if matches:
        print(f"{missing} -> Possible matches: {matches[:3]}")

# Create a corrected symbol file
print(f"\n5. Creating corrected F&O symbols file...")
corrected_symbols = available_symbols.copy()

# Add any obvious corrections here
symbol_corrections = {
    # Add corrections as we find them
    # 'OLD_NAME': 'NEW_NAME'
}

for old_name, new_name in symbol_corrections.items():
    if new_name in equity_mapping and old_name in missing_symbols:
        corrected_symbols.append(new_name)
        print(f"   Added correction: {old_name} -> {new_name}")

# Save corrected symbol list
with open('fno_symbols_corrected.csv', 'w') as f:
    f.write('Symbol\n')
    for symbol in sorted(corrected_symbols):
        f.write(f'{symbol}\n')

print(f"\n[OK] Created fno_symbols_corrected.csv with {len(corrected_symbols)} valid symbols")

print(f"\nSUMMARY:")
print(f"- Original F&O list: {len(our_symbols)} symbols")
print(f"- Available in DHAN: {len(available_symbols)} symbols")
print(f"- Missing from DHAN: {len(missing_symbols)} symbols")
print(f"- Success rate: {len(available_symbols)/len(our_symbols)*100:.1f}%")

print(f"\nRun scanner with: --symbols-file fno_symbols_corrected.csv")