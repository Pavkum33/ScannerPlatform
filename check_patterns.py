import sqlite3
import ast

conn = sqlite3.connect('database/pattern_scanner.db')
cursor = conn.cursor()

# Get patterns for all timeframes
cursor.execute("""
    SELECT dp.*, s.symbol, pt.pattern_name
    FROM detected_patterns dp
    JOIN symbols s ON dp.symbol_id = s.symbol_id
    JOIN pattern_types pt ON dp.pattern_type_id = pt.pattern_type_id
    WHERE dp.timeframe IN ('WEEKLY', 'MONTHLY')
    LIMIT 5
""")

patterns = cursor.fetchall()
print(f"Found {len(patterns)} weekly/monthly patterns")

for pattern in patterns[:2]:
    print(f"\nPattern: {pattern[3]} - {pattern[12]}")  # timeframe - symbol
    pattern_data = pattern[7]  # pattern_data column
    if pattern_data:
        try:
            # Check if it's empty or None
            if pattern_data and pattern_data != 'None':
                print(f"Pattern data (first 200 chars): {pattern_data[:200]}")
                # Try to parse it
                pattern_dict = ast.literal_eval(pattern_data)
                print(f"Parsed successfully: {pattern_dict.get('symbol', 'Unknown')} - {pattern_dict.get('timeframe', 'Unknown')}")
            else:
                print(f"Pattern data is empty or None: {pattern_data}")
        except Exception as e:
            print(f"Error parsing: {e}")
            print(f"Raw data: {pattern_data[:500]}")

conn.close()