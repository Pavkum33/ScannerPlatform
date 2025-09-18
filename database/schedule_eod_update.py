"""
Schedule EOD Update Script
Runs daily EOD updates automatically at 4 PM on weekdays
Can be run as a background service
"""

import os
import sys
import time
import logging
import schedule
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.daily_eod_update import EODUpdater

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('eod_update_schedule.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def run_eod_update():
    """Run the daily EOD update"""
    try:
        logger.info("=" * 60)
        logger.info("SCHEDULED EOD UPDATE STARTING")
        logger.info("=" * 60)

        updater = EODUpdater()

        # Check if it's a weekday
        if datetime.now().weekday() > 4:  # Weekend
            logger.info("Weekend - skipping update")
            return

        # Check if update is needed
        if updater.check_update_needed():
            logger.info("Running scheduled update...")
            updater.update_todays_data()
            logger.info("Scheduled update completed successfully")
        else:
            logger.info("Data already up-to-date, skipping")

    except Exception as e:
        logger.error(f"Error in scheduled update: {e}")


def main():
    """Main entry point for scheduler"""

    logger.info("=" * 60)
    logger.info("EOD UPDATE SCHEDULER")
    logger.info("=" * 60)
    logger.info("Schedule: Monday-Friday at 4:00 PM IST")
    logger.info("Press Ctrl+C to stop")
    logger.info("=" * 60)

    # Schedule the job
    schedule.every().monday.at("16:00").do(run_eod_update)
    schedule.every().tuesday.at("16:00").do(run_eod_update)
    schedule.every().wednesday.at("16:00").do(run_eod_update)
    schedule.every().thursday.at("16:00").do(run_eod_update)
    schedule.every().friday.at("16:00").do(run_eod_update)

    # Also check at 6 PM in case 4 PM data wasn't available
    schedule.every().monday.at("18:00").do(run_eod_update)
    schedule.every().tuesday.at("18:00").do(run_eod_update)
    schedule.every().wednesday.at("18:00").do(run_eod_update)
    schedule.every().thursday.at("18:00").do(run_eod_update)
    schedule.every().friday.at("18:00").do(run_eod_update)

    logger.info("Scheduler started. Waiting for scheduled time...")

    # Keep running
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Check every minute

            # Log status every hour
            if datetime.now().minute == 0:
                logger.info(f"Scheduler active. Next job: {schedule.idle_seconds()} seconds")

        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
            break
        except Exception as e:
            logger.error(f"Scheduler error: {e}")
            time.sleep(60)  # Continue after error


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Schedule EOD Updates')
    parser.add_argument('--test', action='store_true', help='Test run the update now')

    args = parser.parse_args()

    if args.test:
        logger.info("Running test update...")
        run_eod_update()
    else:
        main()