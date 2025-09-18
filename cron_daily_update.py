#!/usr/bin/env python
"""
Daily EOD Update Script for Render Cron Job
Runs as a scheduled job to update the database with latest market data
"""

import os
import sys
import logging

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Run daily EOD update"""
    logger.info("=" * 60)
    logger.info("RENDER CRON JOB: Daily EOD Update Starting")
    logger.info("=" * 60)

    try:
        # Import and run the update
        from database.daily_eod_update import EODUpdater

        updater = EODUpdater()

        # Check if update is needed
        if updater.check_update_needed():
            logger.info("Update needed. Starting EOD data fetch...")
            success = updater.update_todays_data()

            if success:
                logger.info("Daily EOD update completed successfully!")
            else:
                logger.warning("Daily EOD update completed with some warnings")
        else:
            logger.info("Data is already up-to-date. No update needed.")

        logger.info("=" * 60)
        logger.info("RENDER CRON JOB: Completed")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Cron job failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()