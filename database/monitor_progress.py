"""
Database Progress Monitor
Shows real-time statistics of data loading
"""

import sqlite3
import os
import time
from datetime import datetime
from tabulate import tabulate
import sys

def clear_screen():
    """Clear console screen"""
    os.system('cls' if os.name == 'nt' else 'clear')

def get_db_stats(db_path="pattern_scanner.db"):
    """Get current database statistics"""
    if not os.path.exists(db_path):
        return None

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    stats = {}

    try:
        # Total symbols
        cursor.execute("SELECT COUNT(*) FROM symbols WHERE is_active = 1")
        stats['total_symbols'] = cursor.fetchone()[0]

        # Symbols with data
        cursor.execute("""
            SELECT COUNT(DISTINCT symbol_id)
            FROM daily_ohlc
        """)
        stats['symbols_with_data'] = cursor.fetchone()[0]

        # Total records
        cursor.execute("SELECT COUNT(*) FROM daily_ohlc")
        stats['total_records'] = cursor.fetchone()[0]

        # Average days per symbol
        cursor.execute("""
            SELECT AVG(days_count)
            FROM (
                SELECT COUNT(*) as days_count
                FROM daily_ohlc
                GROUP BY symbol_id
            )
        """)
        result = cursor.fetchone()[0]
        stats['avg_days_per_symbol'] = round(result) if result else 0

        # Recent symbols loaded
        cursor.execute("""
            SELECT s.symbol, COUNT(d.ohlc_id) as days, MAX(d.created_at) as last_update
            FROM symbols s
            JOIN daily_ohlc d ON s.symbol_id = d.symbol_id
            GROUP BY s.symbol
            ORDER BY last_update DESC
            LIMIT 10
        """)
        stats['recent_symbols'] = cursor.fetchall()

        # Symbols without data
        cursor.execute("""
            SELECT s.symbol
            FROM symbols s
            LEFT JOIN daily_ohlc d ON s.symbol_id = d.symbol_id
            WHERE d.ohlc_id IS NULL AND s.is_active = 1
        """)
        stats['pending_symbols'] = [row[0] for row in cursor.fetchall()]

        # Database size
        stats['db_size_mb'] = os.path.getsize(db_path) / (1024 * 1024)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

    return stats

def display_progress(stats):
    """Display progress in a nice format"""
    if not stats:
        print("Database not found. Waiting for setup to start...")
        return

    clear_screen()

    print("=" * 80)
    print("DATABASE LOADING PROGRESS MONITOR")
    print("=" * 80)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Overall progress
    progress_pct = (stats['symbols_with_data'] / stats['total_symbols'] * 100) if stats['total_symbols'] > 0 else 0

    print("OVERALL PROGRESS")
    print("-" * 40)
    print(f"Total Symbols: {stats['total_symbols']}")
    print(f"Symbols Loaded: {stats['symbols_with_data']}")
    print(f"Symbols Pending: {stats['total_symbols'] - stats['symbols_with_data']}")
    print(f"Progress: {progress_pct:.1f}%")

    # Progress bar
    bar_length = 50
    filled = int(bar_length * progress_pct / 100)
    bar = '█' * filled + '░' * (bar_length - filled)
    print(f"[{bar}] {progress_pct:.1f}%")
    print()

    # Data statistics
    print("DATA STATISTICS")
    print("-" * 40)
    print(f"Total Records: {stats['total_records']:,}")
    print(f"Avg Days/Symbol: {stats['avg_days_per_symbol']}")
    print(f"Database Size: {stats['db_size_mb']:.1f} MB")
    print()

    # Recently loaded symbols
    if stats['recent_symbols']:
        print("RECENTLY LOADED SYMBOLS (Last 10)")
        print("-" * 40)
        headers = ['Symbol', 'Days', 'Last Update']
        table_data = [[s[0], s[1], s[2][:19] if s[2] else 'N/A'] for s in stats['recent_symbols']]
        print(tabulate(table_data, headers=headers, tablefmt='simple'))
        print()

    # Pending symbols (show first 10)
    if stats['pending_symbols']:
        print(f"PENDING SYMBOLS ({len(stats['pending_symbols'])} remaining)")
        print("-" * 40)
        print(", ".join(stats['pending_symbols'][:20]))
        if len(stats['pending_symbols']) > 20:
            print(f"... and {len(stats['pending_symbols']) - 20} more")
        print()

    # Estimated time remaining
    if stats['symbols_with_data'] > 0 and progress_pct < 100:
        # Assume 30 seconds per symbol (conservative)
        remaining_symbols = stats['total_symbols'] - stats['symbols_with_data']
        est_minutes = (remaining_symbols * 30) / 60
        print(f"ESTIMATED TIME REMAINING: {est_minutes:.0f} minutes")
    elif progress_pct >= 100:
        print("✅ DATA LOADING COMPLETE!")

    print("\n" + "=" * 80)

def monitor_continuous(refresh_interval=10):
    """Continuously monitor progress"""
    print("Starting continuous monitoring...")
    print(f"Refreshing every {refresh_interval} seconds")
    print("Press Ctrl+C to stop")
    time.sleep(2)

    try:
        while True:
            stats = get_db_stats()
            display_progress(stats)

            # Check if complete
            if stats and stats['symbols_with_data'] >= stats['total_symbols']:
                print("\n🎉 DATA LOADING COMPLETE!")
                print(f"Successfully loaded {stats['symbols_with_data']} symbols")
                print(f"Total records: {stats['total_records']:,}")
                break

            time.sleep(refresh_interval)

    except KeyboardInterrupt:
        print("\n\nMonitoring stopped.")
        sys.exit(0)

def quick_check():
    """Quick one-time check"""
    stats = get_db_stats()
    if stats:
        display_progress(stats)

        # Show summary
        print("\nQUICK SUMMARY:")
        print(f"  - {stats['symbols_with_data']}/{stats['total_symbols']} symbols loaded")
        print(f"  - {stats['total_records']:,} total records")
        print(f"  - {stats['db_size_mb']:.1f} MB database size")
    else:
        print("Database not found or not initialized yet.")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Monitor database loading progress')
    parser.add_argument('--once', action='store_true', help='Check once and exit')
    parser.add_argument('--interval', type=int, default=10, help='Refresh interval in seconds (default: 10)')

    args = parser.parse_args()

    if args.once:
        quick_check()
    else:
        monitor_continuous(args.interval)