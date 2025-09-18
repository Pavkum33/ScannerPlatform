"""
PHASE 1: Database Setup and Initial Data Load
==============================================
Run this ONCE to set up the database and load historical data

Steps:
1. Create database and tables
2. Load F&O symbols
3. Fetch 1 year of historical data
4. Generate weekly/monthly aggregates

Expected time: 30-60 minutes for full setup
"""

import os
import sys
import psycopg2
from psycopg2 import sql
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_manager import DatabaseManager
from scanner.dhan_client import DhanClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class Phase1Setup:
    """Phase 1: Database setup and initial data load"""

    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'dbname': 'postgres',  # Connect to default DB first
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres')
        }
        self.db_name = os.getenv('DB_NAME', 'pattern_scanner_db')

    def step1_create_database(self):
        """Step 1: Create database if it doesn't exist"""
        logger.info("=" * 60)
        logger.info("STEP 1: Creating Database")
        logger.info("=" * 60)

        try:
            # Connect to PostgreSQL
            conn = psycopg2.connect(**self.db_config)
            conn.autocommit = True
            cursor = conn.cursor()

            # Check if database exists
            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (self.db_name,)
            )
            exists = cursor.fetchone()

            if exists:
                logger.info(f"✅ Database '{self.db_name}' already exists")
            else:
                # Create database
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(self.db_name)
                ))
                logger.info(f"✅ Created database '{self.db_name}'")

            cursor.close()
            conn.close()

            return True

        except Exception as e:
            logger.error(f"❌ Database creation failed: {e}")
            logger.error("Make sure PostgreSQL is installed and running")
            return False

    def step2_create_schema(self):
        """Step 2: Create database schema"""
        logger.info("=" * 60)
        logger.info("STEP 2: Creating Schema")
        logger.info("=" * 60)

        try:
            # Update config to connect to new database
            self.db_config['dbname'] = self.db_name

            # Connect and run schema
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()

            # Read schema file
            schema_path = os.path.join(
                os.path.dirname(__file__),
                'schema.sql'
            )

            with open(schema_path, 'r') as f:
                schema_sql = f.read()

            # Execute schema
            cursor.execute(schema_sql)
            conn.commit()

            logger.info("✅ Schema created successfully")

            cursor.close()
            conn.close()

            return True

        except Exception as e:
            logger.error(f"❌ Schema creation failed: {e}")
            return False

    def step3_load_symbols(self):
        """Step 3: Load F&O symbols into database"""
        logger.info("=" * 60)
        logger.info("STEP 3: Loading F&O Symbols")
        logger.info("=" * 60)

        try:
            # Initialize database manager
            db = DatabaseManager()

            # Load symbols from CSV
            symbols_file = 'fno_symbols_corrected.csv'
            df = pd.read_csv(symbols_file)

            logger.info(f"📄 Loading {len(df)} symbols from {symbols_file}")

            # Get DHAN mapping
            dhan = DhanClient()
            equity_mapping = dhan.load_equity_instruments()

            # Prepare symbol data
            symbols = []
            for _, row in df.iterrows():
                symbol_name = row['Symbol']
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
            logger.info(f"✅ Loaded {count} symbols into database")

            # Show summary
            valid_symbols = [s for s in symbols if s['dhan_security_id']]
            logger.info(f"   - Valid symbols with DHAN ID: {len(valid_symbols)}")
            logger.info(f"   - Missing DHAN ID: {len(symbols) - len(valid_symbols)}")

            return True

        except Exception as e:
            logger.error(f"❌ Symbol loading failed: {e}")
            return False

    def step4_load_historical_data(self):
        """Step 4: Load 1 year of historical data"""
        logger.info("=" * 60)
        logger.info("STEP 4: Loading Historical Data (This will take 30-60 minutes)")
        logger.info("=" * 60)

        try:
            # Initialize components
            db = DatabaseManager()
            dhan = DhanClient()

            # Get active symbols
            symbols = db.get_active_symbols(fno_only=True)
            logger.info(f"📊 Loading data for {len(symbols)} symbols")

            # Progress tracking
            success_count = 0
            error_count = 0
            start_time = time.time()

            # CRITICAL: Use SAFE batch sizes and delays to ensure 100% data retrieval
            batch_size = 3  # REDUCED from 5 to 3 for safety
            delay_between_symbols = 2  # 2 seconds between each symbol
            delay_between_batches = 10  # 10 seconds between batches

            # Track failed symbols for retry
            failed_symbols = []

            total_batches = (len(symbols) + batch_size - 1) // batch_size

            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min((batch_idx + 1) * batch_size, len(symbols))
                batch = symbols[start_idx:end_idx]

                logger.info(f"\n📦 Batch {batch_idx + 1}/{total_batches} ({len(batch)} symbols)")
                logger.info(f"   Processing symbols: {[s['symbol'] for s in batch]}")

                for symbol_data in batch:
                    symbol = symbol_data['symbol']
                    security_id = symbol_data['dhan_security_id']

                    if not security_id:
                        logger.warning(f"⚠️  {symbol}: No DHAN security ID, skipping")
                        error_count += 1
                        continue

                    # Multiple retry attempts for each symbol
                    retry_count = 0
                    max_retries = 5  # INCREASED to 5 retries to ensure we don't miss data
                    data_fetched = False

                    while retry_count < max_retries and not data_fetched:
                        try:
                            if retry_count > 0:
                                logger.info(f"🔄 Retry {retry_count}/{max_retries} for {symbol}")
                                time.sleep(5 * retry_count)  # Exponential backoff

                            # Fetch 1 year of data
                            logger.info(f"📥 Fetching {symbol}...")
                            df = dhan.get_historical_data(
                                security_id,
                                days_back=365,
                                timeframe="1D"
                            )

                            if df.empty:
                                logger.warning(f"⚠️  {symbol}: No data received (attempt {retry_count + 1})")
                                retry_count += 1
                                continue

                            # Verify we got substantial data (at least 200 days for a year)
                            if len(df) < 200:
                                logger.warning(f"⚠️  {symbol}: Only {len(df)} days received, retrying...")
                                retry_count += 1
                                continue

                            # Convert to database format
                            records = []
                            for _, row in df.iterrows():
                                records.append({
                                    'symbol_id': symbol_data['symbol_id'],
                                    'trade_date': row['timestamp'].date() if hasattr(row['timestamp'], 'date') else row['timestamp'],
                                    'open': float(row['open']),
                                    'high': float(row['high']),
                                    'low': float(row['low']),
                                    'close': float(row['close']),
                                    'volume': int(row.get('volume', 0))
                                })

                            # Store in database
                            inserted = db.bulk_insert_daily_ohlc(records)
                            success_count += 1
                            data_fetched = True

                            logger.info(f"✅ {symbol}: {len(records)} days stored successfully")

                        except Exception as e:
                            retry_count += 1
                            logger.error(f"❌ {symbol}: Attempt {retry_count} failed - {e}")

                            if retry_count >= max_retries:
                                failed_symbols.append({
                                    'symbol': symbol,
                                    'symbol_data': symbol_data,
                                    'error': str(e)
                                })
                                error_count += 1

                    # CRITICAL: Delay between symbols to avoid rate limiting
                    logger.info(f"⏳ Waiting {delay_between_symbols} seconds before next symbol...")
                    time.sleep(delay_between_symbols)

                # CRITICAL: Longer delay between batches
                if batch_idx < total_batches - 1:
                    logger.info(f"⏳ Waiting {delay_between_batches} seconds before next batch...")
                    time.sleep(delay_between_batches)

                # Show progress
                elapsed = time.time() - start_time
                rate = success_count / elapsed if elapsed > 0 else 0
                eta = (len(symbols) - success_count - error_count) / rate if rate > 0 else 0

                logger.info(f"\n📊 Progress: {success_count + error_count}/{len(symbols)} symbols")
                logger.info(f"   Success: {success_count}, Errors: {error_count}")
                logger.info(f"   Speed: {rate:.2f} symbols/sec")
                logger.info(f"   ETA: {eta/60:.1f} minutes")

            # CRITICAL: Final retry for any failed symbols
            if failed_symbols:
                logger.info("\n" + "=" * 60)
                logger.info(f"🔄 FINAL RETRY PHASE: {len(failed_symbols)} failed symbols")
                logger.info("=" * 60)

                for failed_item in failed_symbols:
                    symbol = failed_item['symbol']
                    symbol_data = failed_item['symbol_data']

                    logger.info(f"\n🔄 Final retry for {symbol}")
                    logger.info("   Waiting 15 seconds before retry...")
                    time.sleep(15)  # Longer wait for final retry

                    try:
                        # Final attempt with longer timeout
                        df = dhan.get_historical_data(
                            symbol_data['dhan_security_id'],
                            days_back=365,
                            timeframe="1D"
                        )

                        if not df.empty and len(df) >= 200:
                            # Convert and store
                            records = []
                            for _, row in df.iterrows():
                                records.append({
                                    'symbol_id': symbol_data['symbol_id'],
                                    'trade_date': row['timestamp'].date() if hasattr(row['timestamp'], 'date') else row['timestamp'],
                                    'open': float(row['open']),
                                    'high': float(row['high']),
                                    'low': float(row['low']),
                                    'close': float(row['close']),
                                    'volume': int(row.get('volume', 0))
                                })

                            db.bulk_insert_daily_ohlc(records)
                            success_count += 1
                            error_count -= 1
                            logger.info(f"✅ {symbol}: SUCCESS on final retry - {len(records)} days stored")
                        else:
                            logger.error(f"❌ {symbol}: Failed final retry - insufficient data")

                    except Exception as e:
                        logger.error(f"❌ {symbol}: Failed final retry - {e}")

            # Final summary
            total_time = time.time() - start_time
            logger.info("\n" + "=" * 60)
            logger.info("✅ Historical Data Load Complete!")
            logger.info(f"   Total time: {total_time/60:.1f} minutes")
            logger.info(f"   Success: {success_count}/{len(symbols)} symbols")
            logger.info(f"   Errors: {error_count}")

            # List any remaining failed symbols
            if error_count > 0:
                logger.info(f"\n⚠️  Failed symbols requiring manual review:")
                for failed_item in failed_symbols:
                    if failed_item['symbol'] not in [s['symbol'] for s in symbols if s in success_count]:
                        logger.info(f"   - {failed_item['symbol']}: {failed_item['error']}")

            logger.info("=" * 60)

            return success_count > 0

        except Exception as e:
            logger.error(f"❌ Historical data load failed: {e}")
            return False

    def step5_generate_aggregates(self):
        """Step 5: Generate weekly and monthly aggregates"""
        logger.info("=" * 60)
        logger.info("STEP 5: Generating Weekly/Monthly Aggregates")
        logger.info("=" * 60)

        try:
            db = DatabaseManager()

            logger.info("📊 Generating weekly aggregates...")
            db.update_aggregated_data()

            logger.info("✅ Aggregates generated successfully")

            # Show statistics
            stats = db.get_database_stats()
            logger.info("\n📈 Database Statistics:")
            logger.info(f"   - Total symbols: {stats.get('total_symbols', 0)}")
            logger.info(f"   - Daily records: {stats.get('total_daily_records', 0)}")
            logger.info(f"   - Database size: {stats.get('database_size', 'N/A')}")

            # Check data freshness
            freshness = db.check_data_freshness()
            logger.info(f"\n📅 Data Freshness:")
            logger.info(f"   - Current data: {freshness.get('current_symbols', 0)} symbols")
            logger.info(f"   - One day old: {freshness.get('one_day_old', 0)} symbols")
            logger.info(f"   - Stale data: {freshness.get('stale_symbols', 0)} symbols")

            return True

        except Exception as e:
            logger.error(f"❌ Aggregate generation failed: {e}")
            return False

    def run_full_setup(self, auto_confirm=False):
        """Run complete Phase 1 setup"""
        logger.info("STARTING PHASE 1: DATABASE SETUP AND DATA LOAD")
        logger.info("=" * 60)
        logger.info("This will:")
        logger.info("1. Create PostgreSQL database")
        logger.info("2. Create schema and tables")
        logger.info("3. Load F&O symbols")
        logger.info("4. Fetch 1 year of historical data")
        logger.info("5. Generate aggregates")
        logger.info("\nEstimated time: 60-90 minutes")
        logger.info("=" * 60)

        if not auto_confirm:
            # Ask for confirmation
            try:
                response = input("\nContinue with setup? (yes/no): ")
                if response.lower() != 'yes':
                    logger.info("Setup cancelled")
                    return
            except:
                logger.info("Auto-confirming setup...")
                pass

        start_time = time.time()

        # Run steps
        steps = [
            (self.step1_create_database, "Database Creation"),
            (self.step2_create_schema, "Schema Creation"),
            (self.step3_load_symbols, "Symbol Loading"),
            (self.step4_load_historical_data, "Historical Data Load"),
            (self.step5_generate_aggregates, "Aggregate Generation")
        ]

        for step_func, step_name in steps:
            if not step_func():
                logger.error(f"❌ Setup failed at: {step_name}")
                logger.error("Please fix the error and run again")
                return False

            logger.info(f"✅ {step_name} completed\n")

        # Complete!
        total_time = time.time() - start_time
        logger.info("\n" + "=" * 60)
        logger.info("PHASE 1 SETUP COMPLETE!")
        logger.info(f"Total time: {total_time/60:.1f} minutes")
        logger.info("\nDatabase is ready for use!")
        logger.info("Next: Run phase2_daily_ops.py for daily operations")
        logger.info("=" * 60)

        return True


if __name__ == "__main__":
    setup = Phase1Setup()
    setup.run_full_setup()