"""
PHASE 2: Daily Operations and EOD Updates
==========================================
Run this daily at 4 PM to update today's data

Features:
1. EOD data update (only today's candle)
2. Smart scanning with DB
3. Performance monitoring
4. Auto-maintenance

Expected time: 30 seconds for daily update
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
import schedule
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_manager import DatabaseManager
from database.smart_data_manager import SmartDataManager, DailyUpdateJob
from scanner.dhan_client import DhanClient
from scanner.enhanced_scanner_engine import EnhancedScannerEngine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class Phase2Operations:
    """Phase 2: Daily operations with smart data management"""

    def __init__(self):
        # Initialize components
        logger.info("Initializing Phase 2 Daily Operations...")

        self.db = DatabaseManager()
        self.dhan = DhanClient()
        self.smart_manager = SmartDataManager(self.db, self.dhan)
        self.scanner = EnhancedScannerEngine(self.dhan, self.db)
        self.updater = DailyUpdateJob(self.smart_manager)

        logger.info("✅ All components initialized")

    def run_eod_update(self):
        """Run end-of-day data update (4 PM)"""
        logger.info("\n" + "=" * 60)
        logger.info("🌅 RUNNING EOD UPDATE")
        logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)

        start_time = time.time()

        try:
            # Get symbols to update
            symbols = self.db.get_active_symbols(fno_only=True)
            logger.info(f"📊 Updating {len(symbols)} symbols with today's data...")

            success_count = 0
            error_count = 0
            api_calls = 0

            # Process each symbol
            for symbol_data in symbols:
                symbol = symbol_data['symbol']
                security_id = symbol_data['dhan_security_id']

                if not security_id:
                    continue

                try:
                    # Check if we already have today's data
                    latest = self.db.get_latest_update_date(symbol)
                    today = datetime.now().date()

                    if latest and latest == today:
                        logger.debug(f"✓ {symbol}: Already up-to-date")
                        success_count += 1
                        continue

                    # Fetch ONLY today's candle
                    df = self.dhan.get_historical_data(
                        security_id,
                        days_back=1,  # Only today!
                        timeframe="1D"
                    )
                    api_calls += 1

                    if not df.empty:
                        # Store in database
                        self.smart_manager._store_to_database(symbol, df)
                        success_count += 1
                        logger.info(f"✅ {symbol}: Today's data updated")
                    else:
                        logger.warning(f"⚠️  {symbol}: No data for today")

                except Exception as e:
                    error_count += 1
                    logger.error(f"❌ {symbol}: {e}")

                # Rate limiting
                if api_calls % 10 == 0:
                    time.sleep(1)  # Small delay every 10 calls

            # Update aggregates
            logger.info("\n📊 Updating weekly/monthly aggregates...")
            self.db.update_aggregated_data()

            # Refresh materialized views
            logger.info("🔄 Refreshing materialized views...")
            self.db.refresh_materialized_views()

            elapsed = time.time() - start_time

            # Summary
            logger.info("\n" + "=" * 60)
            logger.info("✅ EOD UPDATE COMPLETE!")
            logger.info(f"   Time taken: {elapsed:.1f} seconds")
            logger.info(f"   Symbols updated: {success_count}")
            logger.info(f"   Errors: {error_count}")
            logger.info(f"   API calls made: {api_calls}")
            logger.info("=" * 60)

            return True

        except Exception as e:
            logger.error(f"EOD update failed: {e}")
            return False

    def run_smart_scan(self, timeframe="1D", min_body_move_pct=4.0):
        """Run pattern scan using smart data manager"""
        logger.info("\n" + "=" * 60)
        logger.info("🔍 RUNNING SMART SCAN (DB-POWERED)")
        logger.info(f"Timeframe: {timeframe}, Min Body Move: {min_body_move_pct}%")
        logger.info("=" * 60)

        start_time = time.time()

        # Get symbols
        symbols = self.db.get_active_symbols(fno_only=True)
        symbol_list = [s['symbol'] for s in symbols]

        logger.info(f"📊 Scanning {len(symbol_list)} symbols...")

        # Run scan
        results = self.scanner.scan(
            symbols=symbol_list,
            timeframe=timeframe,
            history=30 if timeframe == "1D" else 20 if timeframe == "1W" else 12,
            min_body_move_pct=min_body_move_pct
        )

        elapsed = time.time() - start_time

        # Show results
        logger.info("\n" + "=" * 60)
        logger.info("✅ SCAN COMPLETE!")
        logger.info(f"   Time taken: {elapsed:.1f} seconds")
        logger.info(f"   Patterns found: {len(results['results'])}")
        logger.info(f"   DB hit rate: {results['statistics'].get('db_hit_rate', 'N/A')}")
        logger.info(f"   API calls: {results['statistics'].get('api_calls_made', 0)}")
        logger.info(f"   Speed: {len(symbol_list)/elapsed:.1f} symbols/second")
        logger.info("=" * 60)

        # Show patterns
        if results['results']:
            logger.info("\n📈 PATTERNS FOUND:")
            for i, pattern in enumerate(results['results'][:10], 1):  # Show first 10
                logger.info(f"{i}. {pattern['symbol']} - "
                          f"{pattern['pattern_direction']} "
                          f"(Marubozu: {pattern['marubozu']['date']}, "
                          f"Doji: {pattern['doji']['date']})")

        return results

    def run_today_signals(self):
        """Get today's signals (yesterday Marubozu → today Doji)"""
        logger.info("\n" + "=" * 60)
        logger.info("📅 TODAY'S SIGNALS")
        logger.info("=" * 60)

        # Get symbols
        symbols = self.db.get_active_symbols(fno_only=True)
        symbol_list = [s['symbol'] for s in symbols]

        # Run today scan
        results = self.scanner.run_today_scan(symbol_list)

        # Show results
        if results['results']:
            logger.info(f"✅ Found {len(results['results'])} today's signals:")
            for i, pattern in enumerate(results['results'], 1):
                logger.info(f"{i}. {pattern['symbol']} - {pattern['pattern_direction']}")
        else:
            logger.info("No signals for today")

        return results

    def check_database_health(self):
        """Check database health and statistics"""
        logger.info("\n" + "=" * 60)
        logger.info("🏥 DATABASE HEALTH CHECK")
        logger.info("=" * 60)

        # Get statistics
        stats = self.db.get_database_stats()
        logger.info(f"📊 Database Statistics:")
        logger.info(f"   - Total symbols: {stats.get('total_symbols', 0)}")
        logger.info(f"   - Daily records: {stats.get('total_daily_records', 0)}")
        logger.info(f"   - Total patterns: {stats.get('total_patterns', 0)}")
        logger.info(f"   - Patterns today: {stats.get('patterns_today', 0)}")
        logger.info(f"   - Database size: {stats.get('database_size', 'N/A')}")

        # Check data freshness
        freshness = self.db.check_data_freshness()
        logger.info(f"\n📅 Data Freshness:")
        logger.info(f"   - Up-to-date: {freshness.get('current_symbols', 0)} symbols")
        logger.info(f"   - One day old: {freshness.get('one_day_old', 0)} symbols")
        logger.info(f"   - Stale: {freshness.get('stale_symbols', 0)} symbols")
        logger.info(f"   - Newest data: {freshness.get('newest_data', 'N/A')}")
        logger.info(f"   - Oldest data: {freshness.get('oldest_data', 'N/A')}")

        return stats

    def schedule_daily_jobs(self):
        """Schedule daily automated jobs"""
        logger.info("\n📅 Setting up scheduled jobs...")

        # Schedule EOD update at 4:00 PM
        schedule.every().day.at("16:00").do(self.run_eod_update)

        # Schedule health check at 5:00 PM
        schedule.every().day.at("17:00").do(self.check_database_health)

        logger.info("✅ Scheduled jobs:")
        logger.info("   - EOD Update: 4:00 PM daily")
        logger.info("   - Health Check: 5:00 PM daily")

        logger.info("\n⏰ Scheduler running... (Ctrl+C to stop)")

        # Run scheduler
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute

    def run_interactive_menu(self):
        """Interactive menu for manual operations"""
        while True:
            print("\n" + "=" * 60)
            print("PHASE 2: DAILY OPERATIONS MENU")
            print("=" * 60)
            print("1. Run EOD Update (fetch today's data)")
            print("2. Run Smart Scan (daily)")
            print("3. Run Smart Scan (weekly)")
            print("4. Run Smart Scan (monthly)")
            print("5. Get Today's Signals")
            print("6. Check Database Health")
            print("7. Start Automated Scheduler")
            print("8. Exit")
            print("-" * 60)

            choice = input("Select option (1-8): ")

            if choice == '1':
                self.run_eod_update()
            elif choice == '2':
                self.run_smart_scan("1D")
            elif choice == '3':
                self.run_smart_scan("1W")
            elif choice == '4':
                self.run_smart_scan("1M")
            elif choice == '5':
                self.run_today_signals()
            elif choice == '6':
                self.check_database_health()
            elif choice == '7':
                self.schedule_daily_jobs()
            elif choice == '8':
                print("Goodbye!")
                break
            else:
                print("Invalid option, please try again")


def main():
    """Main entry point"""
    logger.info("=" * 60)
    logger.info("PHASE 2: DAILY OPERATIONS")
    logger.info("=" * 60)

    # Check if Phase 1 is complete
    try:
        db = DatabaseManager()
        stats = db.get_database_stats()

        if stats.get('total_daily_records', 0) == 0:
            logger.error("❌ Database is empty!")
            logger.error("Please run setup_phase1.py first to load historical data")
            return

        logger.info(f"✅ Database ready: {stats.get('total_daily_records', 0)} records found")

    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        logger.error("Please run setup_phase1.py first")
        return

    # Start Phase 2 operations
    ops = Phase2Operations()
    ops.run_interactive_menu()


if __name__ == "__main__":
    main()