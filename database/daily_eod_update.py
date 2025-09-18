"""
Daily EOD Update Script
Run this after market close (4 PM) to update today's data
Only fetches TODAY's candle for each symbol (minimal API usage)
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
import schedule

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.sqlite_db_manager import SQLiteDBManager
from scanner.dhan_client import DhanClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EODUpdater:
    """End-of-Day data updater"""

    def __init__(self, db_path="pattern_scanner.db"):
        self.db = SQLiteDBManager(db_path)
        self.dhan = DhanClient()

    def update_todays_data(self, force_days=None):
        """Update today's data for all symbols

        Args:
            force_days: Override days_back (useful when today's data not yet available)
        """

        logger.info("=" * 60)
        logger.info("EOD UPDATE - FETCHING LATEST DATA")
        logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)

        # Get all active symbols
        symbols = self.db.get_active_symbols(fno_only=True)
        logger.info(f"Updating {len(symbols)} symbols...")

        success_count = 0
        error_count = 0
        already_updated = 0
        new_records = 0
        start_time = time.time()

        # Determine how many days to fetch
        days_to_fetch = force_days or 2  # Default to 2 days to ensure we get latest available

        for symbol_data in symbols:
            symbol = symbol_data['symbol']
            security_id = symbol_data['dhan_security_id']

            if not security_id:
                logger.warning(f"{symbol}: No DHAN security ID")
                error_count += 1
                continue

            try:
                # Get latest date in database
                latest = self.db.get_latest_update_date(symbol)

                # Fetch recent data
                logger.info(f"Updating {symbol}...")
                df = self.dhan.get_historical_data(
                    security_id,
                    days_back=days_to_fetch,
                    timeframe="1D"
                )

                if df.empty:
                    logger.warning(f"{symbol}: No data available")
                    error_count += 1
                    continue

                # Store in database
                records = []
                dates_added = []
                for _, row in df.iterrows():
                    candle_date = row['timestamp'].date() if hasattr(row['timestamp'], 'date') else row['timestamp']

                    # Skip if we already have this date
                    if latest and candle_date <= latest:
                        continue

                    records.append({
                        'symbol_id': symbol_data['symbol_id'],
                        'trade_date': str(candle_date),
                        'open': float(row['open']),
                        'high': float(row['high']),
                        'low': float(row['low']),
                        'close': float(row['close']),
                        'volume': int(row.get('volume', 0))
                    })
                    dates_added.append(str(candle_date))

                if records:
                    self.db.bulk_insert_daily_ohlc(records)
                    success_count += 1
                    new_records += len(records)
                    logger.info(f"  SUCCESS: {symbol} updated with {len(records)} new record(s) for {', '.join(dates_added)}")
                else:
                    already_updated += 1
                    logger.debug(f"  {symbol}: Already up-to-date")

            except Exception as e:
                error_count += 1
                logger.error(f"  ERROR {symbol}: {e}")

            # Small delay to avoid rate limiting
            time.sleep(0.5)

        # Summary
        elapsed = time.time() - start_time
        total = len(symbols)

        logger.info("\n" + "=" * 60)
        logger.info("EOD UPDATE COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Total symbols:      {total}")
        logger.info(f"Already updated:    {already_updated}")
        logger.info(f"Newly updated:      {success_count}")
        logger.info(f"New records added:  {new_records}")
        logger.info(f"Errors:             {error_count}")
        logger.info(f"Time taken:         {elapsed/60:.1f} minutes")
        logger.info(f"API calls made:     {total - already_updated}")

        # Update aggregates
        logger.info("\nUpdating weekly/monthly aggregates...")
        self.db.update_aggregated_data()
        logger.info("Aggregates updated")

        # Show database stats
        stats = self.db.get_database_stats()
        logger.info(f"\nDatabase Statistics:")
        logger.info(f"  Total records: {stats['total_daily_records']:,}")
        logger.info(f"  Database size: {stats['database_size']}")

        return success_count > 0

    def check_update_needed(self) -> bool:
        """Check if update is needed"""

        # Get latest data date
        latest = self.db.get_latest_update_date()

        if not latest:
            logger.info("No data in database, update needed")
            return True

        today = datetime.now().date()

        # Check if it's a weekday
        if today.weekday() > 4:  # Weekend
            logger.info("Weekend - no update needed")
            return False

        # Check if we already have today's data
        if latest >= today:
            logger.info(f"Data already up-to-date (latest: {latest})")
            return False

        # Check if market hours are over (after 3:30 PM)
        now = datetime.now()
        if now.hour < 15 or (now.hour == 15 and now.minute < 30):
            logger.info("Market still open, wait until 3:30 PM")
            return False

        logger.info(f"Update needed (latest data: {latest}, today: {today})")
        return True

    def schedule_daily_update(self):
        """Schedule automatic daily updates at 4 PM"""

        logger.info("Scheduling daily EOD update at 4:00 PM...")

        # Schedule job
        schedule.every().weekday.at("16:00").do(self.update_todays_data)

        logger.info("Scheduler started. Press Ctrl+C to stop")
        logger.info("Update will run Monday-Friday at 4:00 PM")

        # Keep running
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute


def main():
    """Main entry point"""

    import argparse

    parser = argparse.ArgumentParser(description='EOD Data Updater')
    parser.add_argument('--now', action='store_true', help='Run update now')
    parser.add_argument('--check', action='store_true', help='Check if update needed')
    parser.add_argument('--schedule', action='store_true', help='Schedule daily updates')

    args = parser.parse_args()

    updater = EODUpdater()

    if args.check:
        if updater.check_update_needed():
            print("Update needed")
        else:
            print("No update needed")

    elif args.schedule:
        updater.schedule_daily_update()

    else:  # Default or --now
        if args.now or updater.check_update_needed():
            updater.update_todays_data()
        else:
            print("No update needed. Use --now to force update")


if __name__ == "__main__":
    main()