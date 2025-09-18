"""
SQLite Setup - Phase 1: Load Historical Data
No PostgreSQL installation required!
"""

import os
import sys
import time
import pandas as pd
from datetime import datetime, timedelta
import logging

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


def setup_and_load_data():
    """Setup SQLite database and load historical data"""

    logger.info("=" * 60)
    logger.info("SQLITE SETUP - NO INSTALLATION REQUIRED!")
    logger.info("=" * 60)
    logger.info("This will:")
    logger.info("1. Create SQLite database (instant)")
    logger.info("2. Load F&O symbols")
    logger.info("3. Fetch 1 year of historical data")
    logger.info("Expected time: 60-90 minutes")
    logger.info("=" * 60)

    start_time = time.time()

    # Initialize components
    logger.info("\nStep 1: Initializing database...")
    db = SQLiteDBManager("pattern_scanner.db")
    logger.info("Database created: pattern_scanner.db")

    logger.info("\nStep 2: Initializing DHAN client...")
    dhan = DhanClient()

    # Load symbols
    logger.info("\nStep 3: Loading F&O symbols...")
    df = pd.read_csv('../fno_symbols_corrected.csv')
    symbols_list = df['Symbol'].tolist()

    # Get DHAN mapping
    equity_mapping = dhan.load_equity_instruments()

    # Prepare symbol data
    symbols = []
    for symbol_name in symbols_list:
        dhan_security_id = equity_mapping.get(symbol_name, '')
        symbols.append({
            'symbol': symbol_name,
            'exchange': 'NSE',
            'instrument_type': 'EQUITY',
            'is_fno': True,
            'dhan_security_id': str(dhan_security_id) if dhan_security_id else ''
        })

    # Insert symbols
    count = db.upsert_symbols(symbols)
    logger.info(f"Loaded {count} symbols into database")

    # Get symbols with valid DHAN IDs
    valid_symbols = db.get_active_symbols(fno_only=True)
    valid_symbols = [s for s in valid_symbols if s['dhan_security_id']]
    logger.info(f"Found {len(valid_symbols)} symbols with valid DHAN IDs")

    # Load historical data
    logger.info("\n" + "=" * 60)
    logger.info("Step 4: Loading Historical Data")
    logger.info("This is the longest step (60-90 minutes)")
    logger.info("=" * 60)

    success_count = 0
    error_count = 0
    failed_symbols = []

    # Process in small batches with retries
    batch_size = 3  # Small batch for safety
    total_batches = (len(valid_symbols) + batch_size - 1) // batch_size

    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(valid_symbols))
        batch = valid_symbols[start_idx:end_idx]

        logger.info(f"\nBatch {batch_idx + 1}/{total_batches}")
        logger.info(f"Processing: {[s['symbol'] for s in batch]}")

        for symbol_data in batch:
            symbol = symbol_data['symbol']
            security_id = symbol_data['dhan_security_id']

            # Multiple retry attempts
            retry_count = 0
            max_retries = 5
            data_fetched = False

            while retry_count < max_retries and not data_fetched:
                try:
                    if retry_count > 0:
                        logger.info(f"  Retry {retry_count}/{max_retries} for {symbol}")
                        time.sleep(5 * retry_count)  # Exponential backoff

                    # Fetch data
                    logger.info(f"  Fetching {symbol}...")
                    df = dhan.get_historical_data(
                        security_id,
                        days_back=365,
                        timeframe="1D"
                    )

                    if df.empty or len(df) < 200:
                        logger.warning(f"  {symbol}: Insufficient data ({len(df)} days)")
                        retry_count += 1
                        continue

                    # Store in database
                    records = []
                    for _, row in df.iterrows():
                        records.append({
                            'symbol_id': symbol_data['symbol_id'],
                            'trade_date': row['timestamp'].strftime('%Y-%m-%d') if hasattr(row['timestamp'], 'strftime') else str(row['timestamp'])[:10],
                            'open': float(row['open']),
                            'high': float(row['high']),
                            'low': float(row['low']),
                            'close': float(row['close']),
                            'volume': int(row.get('volume', 0))
                        })

                    inserted = db.bulk_insert_daily_ohlc(records)
                    success_count += 1
                    data_fetched = True
                    logger.info(f"  SUCCESS: {symbol} - {len(records)} days stored")

                except Exception as e:
                    retry_count += 1
                    logger.error(f"  ERROR {symbol}: {e}")

                    if retry_count >= max_retries:
                        failed_symbols.append(symbol)
                        error_count += 1

            # Delay between symbols
            time.sleep(2)

        # Delay between batches
        if batch_idx < total_batches - 1:
            logger.info("Waiting 10 seconds before next batch...")
            time.sleep(10)

        # Progress update
        elapsed = time.time() - start_time
        logger.info(f"\nProgress: {success_count}/{len(valid_symbols)} completed")
        logger.info(f"Time elapsed: {elapsed/60:.1f} minutes")

    # Final summary
    total_time = time.time() - start_time
    logger.info("\n" + "=" * 60)
    logger.info("SETUP COMPLETE!")
    logger.info(f"Total time: {total_time/60:.1f} minutes")
    logger.info(f"Success: {success_count} symbols")
    logger.info(f"Errors: {error_count} symbols")

    if failed_symbols:
        logger.info(f"\nFailed symbols: {failed_symbols}")

    # Show database stats
    stats = db.get_database_stats()
    logger.info(f"\nDatabase Statistics:")
    logger.info(f"  Total symbols: {stats['total_symbols']}")
    logger.info(f"  Total records: {stats['total_daily_records']}")
    logger.info(f"  Database size: {stats['database_size']}")
    logger.info("=" * 60)

    return success_count > 0


if __name__ == "__main__":
    setup_and_load_data()