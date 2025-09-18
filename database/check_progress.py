"""
Simple Progress Checker for Database Loading
"""

import sqlite3
import os
from datetime import datetime

def check_progress():
    """Check and display current progress"""
    db_path = "pattern_scanner.db"

    if not os.path.exists(db_path):
        print("Database not found. Setup may not have started yet.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("=" * 60)
    print("DATA LOADING PROGRESS")
    print("=" * 60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Get statistics
    cursor.execute("SELECT COUNT(*) FROM symbols WHERE is_active = 1")
    total_symbols = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT symbol_id) FROM daily_ohlc")
    loaded_symbols = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM daily_ohlc")
    total_records = cursor.fetchone()[0]

    # Calculate progress
    progress_pct = (loaded_symbols / total_symbols * 100) if total_symbols > 0 else 0
    pending = total_symbols - loaded_symbols

    # Display
    print(f"Total Symbols:    {total_symbols}")
    print(f"Loaded:           {loaded_symbols}")
    print(f"Pending:          {pending}")
    print(f"Progress:         {progress_pct:.1f}%")
    print(f"Total Records:    {total_records:,}")
    print()

    # Progress bar (simple version)
    bar_length = 50
    filled = int(bar_length * progress_pct / 100)
    bar = '#' * filled + '-' * (bar_length - filled)
    print(f"[{bar}] {progress_pct:.1f}%")
    print()

    # Show recent symbols
    cursor.execute("""
        SELECT s.symbol, COUNT(d.ohlc_id) as days
        FROM symbols s
        JOIN daily_ohlc d ON s.symbol_id = d.symbol_id
        GROUP BY s.symbol
        ORDER BY MAX(d.ohlc_id) DESC
        LIMIT 10
    """)

    recent = cursor.fetchall()
    if recent:
        print("Recently Loaded Symbols:")
        print("-" * 30)
        for symbol, days in recent:
            print(f"  {symbol:15} {days} days")
    print()

    # Estimate remaining time
    if loaded_symbols > 0 and progress_pct < 100:
        # Assume 30 seconds per symbol average
        est_minutes = (pending * 30) / 60
        print(f"Estimated Time Remaining: {est_minutes:.0f} minutes")
    elif progress_pct >= 100:
        print("DATA LOADING COMPLETE!")

    # Database size
    db_size_mb = os.path.getsize(db_path) / (1024 * 1024)
    print(f"Database Size: {db_size_mb:.1f} MB")

    conn.close()

    print("\n" + "=" * 60)

if __name__ == "__main__":
    check_progress()