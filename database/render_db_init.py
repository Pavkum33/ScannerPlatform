"""
Render Database Initialization Script
Sets up SQLite database with ACTUAL historical data from DHAN API
Includes pattern detection for Daily, Weekly, and Monthly timeframes
"""

import os
import sys
import sqlite3
import pandas as pd
import json
import logging
from datetime import datetime, timedelta
import time

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

def initialize_render_database():
    """Initialize database for Render deployment with ACTUAL data from DHAN API"""

    logger.info("="*60)
    logger.info("RENDER DATABASE INITIALIZATION WITH REAL DATA")
    logger.info("="*60)

    # Initialize components
    logger.info("\nInitializing database and DHAN client...")
    db = SQLiteDBManager("pattern_scanner.db")
    dhan_client = DhanClient()

    try:
        # Step 1: Load symbols
        logger.info("\nStep 1: Loading F&O symbols...")
        df = pd.read_csv('../fno_symbols_corrected.csv')
        symbols_list = df['Symbol'].tolist()

        # Get DHAN mapping
        equity_mapping = dhan_client.load_equity_instruments()

        # Prepare and insert symbols
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

        count = db.upsert_symbols(symbols)
        logger.info(f"Loaded {count} symbols into database")

        # Get symbols with valid DHAN IDs
        valid_symbols = db.get_active_symbols(fno_only=True)
        valid_symbols = [s for s in valid_symbols if s['dhan_security_id']]
        logger.info(f"Found {len(valid_symbols)} symbols with valid DHAN IDs")

        # Step 2: Load historical data from DHAN API
        logger.info("\n" + "="*60)
        logger.info("Step 2: Loading ACTUAL Historical Data from DHAN")
        logger.info("This will take 60-90 minutes for full data")
        logger.info("="*60)

        success_count = 0
        error_count = 0
        failed_symbols = []

        # Process in small batches
        batch_size = 3
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

                        # Fetch data from DHAN API
                        logger.info(f"  Fetching {symbol}...")
                        df = dhan_client.get_historical_data(
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

        logger.info(f"\nData loading complete: {success_count} success, {error_count} errors")

        # Step 3: Generate aggregations
        logger.info("\nStep 3: Generating weekly/monthly aggregations...")
        generate_aggregations_from_db(db)

        # Step 4: Detect patterns
        logger.info("\nStep 4: Detecting patterns across all timeframes...")
        detect_all_patterns_from_db(db)

        # Step 5: Verify database
        logger.info("\nStep 5: Verifying database...")
        stats = db.get_database_stats()
        logger.info(f"  Total symbols: {stats['total_symbols']}")
        logger.info(f"  Total daily records: {stats['total_daily_records']}")
        logger.info(f"  Database size: {stats['database_size']}")

        # Get pattern counts
        conn = sqlite3.connect('pattern_scanner.db')
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM detected_patterns WHERE timeframe = '1D'")
        daily_patterns = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM detected_patterns WHERE timeframe = 'WEEKLY'")
        weekly_patterns = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM detected_patterns WHERE timeframe = 'MONTHLY'")
        monthly_patterns = cursor.fetchone()[0]

        logger.info(f"  Daily patterns: {daily_patterns}")
        logger.info(f"  Weekly patterns: {weekly_patterns}")
        logger.info(f"  Monthly patterns: {monthly_patterns}")

        conn.close()

        logger.info("\n" + "="*60)
        logger.info("DATABASE INITIALIZATION COMPLETE!")
        logger.info("="*60)

    except Exception as e:
        logger.error(f"Error during initialization: {e}")
        raise

def generate_aggregations_from_db(db):
    """Generate weekly and monthly aggregations from daily OHLC data"""
    conn = sqlite3.connect('pattern_scanner.db')

    try:
        # Get all daily data
        logger.info("Fetching daily OHLC data...")
        daily_df = pd.read_sql_query("""
            SELECT symbol_id, trade_date as date, open, high, low, close, volume
            FROM daily_ohlc
            ORDER BY symbol_id, trade_date
        """, conn)

        if daily_df.empty:
            logger.error("No daily data found in database!")
            return

        logger.info(f"Found {len(daily_df)} daily records for {daily_df['symbol_id'].nunique()} symbols")

        # Convert date to datetime
        daily_df['date'] = pd.to_datetime(daily_df['date'])

        # Clear existing aggregated data
        cursor = conn.cursor()
        cursor.execute("DELETE FROM aggregated_ohlc")
        conn.commit()

        aggregated_records = []

        # Process each symbol
        for symbol_id in daily_df['symbol_id'].unique():
            symbol_data = daily_df[daily_df['symbol_id'] == symbol_id].copy()
            symbol_data = symbol_data.sort_values('date')

            # Weekly aggregation (ISO week)
            symbol_data['week'] = symbol_data['date'].dt.to_period('W')
            weekly = symbol_data.groupby('week').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'date': 'last'
            }).reset_index(drop=True)

            # Add weekly records
            for idx, row in weekly.iterrows():
                week_data = symbol_data[symbol_data['week'] == symbol_data['week'].unique()[idx]]
                period_start = week_data['date'].min()
                period_end = week_data['date'].max()
                trading_days = len(week_data)

                aggregated_records.append({
                    'symbol_id': int(symbol_id),
                    'timeframe': 'WEEKLY',
                    'period_start': period_start.strftime('%Y-%m-%d'),
                    'period_end': period_end.strftime('%Y-%m-%d'),
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': int(row['volume']) if pd.notna(row['volume']) else 0,
                    'trading_days': int(trading_days)
                })

            # Monthly aggregation
            symbol_data['month'] = symbol_data['date'].dt.to_period('M')
            monthly = symbol_data.groupby('month').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'date': 'last'
            }).reset_index(drop=True)

            # Add monthly records
            for idx, row in monthly.iterrows():
                month_data = symbol_data[symbol_data['month'] == symbol_data['month'].unique()[idx]]
                period_start = month_data['date'].min()
                period_end = month_data['date'].max()
                trading_days = len(month_data)

                aggregated_records.append({
                    'symbol_id': int(symbol_id),
                    'timeframe': 'MONTHLY',
                    'period_start': period_start.strftime('%Y-%m-%d'),
                    'period_end': period_end.strftime('%Y-%m-%d'),
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': int(row['volume']) if pd.notna(row['volume']) else 0,
                    'trading_days': int(trading_days)
                })

        # Insert all aggregated records
        if aggregated_records:
            logger.info(f"Inserting {len(aggregated_records)} aggregated records...")
            cursor.executemany("""
                INSERT INTO aggregated_ohlc (symbol_id, timeframe, period_start, period_end, open, high, low, close, volume, trading_days)
                VALUES (:symbol_id, :timeframe, :period_start, :period_end, :open, :high, :low, :close, :volume, :trading_days)
            """, aggregated_records)
            conn.commit()

            # Get counts by timeframe
            cursor.execute("""
                SELECT timeframe, COUNT(*) as count
                FROM aggregated_ohlc
                GROUP BY timeframe
            """)
            counts = cursor.fetchall()
            for timeframe, count in counts:
                logger.info(f"Generated {count} {timeframe} aggregations")

        logger.info("Aggregation generation completed successfully!")

    except Exception as e:
        logger.error(f"Error generating aggregations: {e}")
        raise
    finally:
        conn.close()

def detect_all_patterns_from_db(db):
    """Detect Marubozu->Doji patterns on all timeframes"""
    conn = sqlite3.connect('pattern_scanner.db')

    try:
        cursor = conn.cursor()

        # Get or create pattern type
        cursor.execute("SELECT pattern_type_id FROM pattern_types WHERE pattern_name = 'MARUBOZU_DOJI'")
        result = cursor.fetchone()

        if not result:
            cursor.execute("""
                INSERT INTO pattern_types (pattern_name, pattern_category, description, candles_required)
                VALUES ('MARUBOZU_DOJI', 'REVERSAL', 'Marubozu followed by Doji - failed breakout pattern', 2)
            """)
            pattern_type_id = cursor.lastrowid
        else:
            pattern_type_id = result[0]

        # Clear existing patterns
        cursor.execute("DELETE FROM detected_patterns")
        conn.commit()

        detected_patterns = []

        # Detect daily patterns
        logger.info("Detecting daily patterns...")
        daily_df = pd.read_sql_query("""
            SELECT d.*, s.symbol
            FROM daily_ohlc d
            JOIN symbols s ON d.symbol_id = s.symbol_id
            ORDER BY d.symbol_id, d.trade_date
        """, conn)

        daily_df['date'] = pd.to_datetime(daily_df['trade_date'])

        for symbol_id in daily_df['symbol_id'].unique():
            symbol_data = daily_df[daily_df['symbol_id'] == symbol_id].sort_values('date').reset_index(drop=True)
            symbol_name = symbol_data.iloc[0]['symbol']

            for i in range(len(symbol_data) - 1):
                candle1 = symbol_data.iloc[i]
                candle2 = symbol_data.iloc[i + 1]

                # Calculate body percentages
                body1 = abs(candle1['close'] - candle1['open'])
                range1 = candle1['high'] - candle1['low']

                if range1 == 0:
                    continue

                body1_pct = (body1 / range1) * 100
                body1_move_pct = (body1 / candle1['open']) * 100

                body2 = abs(candle2['close'] - candle2['open'])
                range2 = candle2['high'] - candle2['low']

                if range2 == 0:
                    continue

                body2_pct = (body2 / range2) * 100

                # Check for Marubozu (body >= 80% of range, move >= 2%)
                if body1_pct >= 80 and body1_move_pct >= 2:
                    # Check for Doji (body < 25% of range)
                    if body2_pct < 25:
                        # Check breakout and rejection
                        if candle2['high'] > candle1['high']:
                            is_bullish = candle1['close'] > candle1['open']

                            if is_bullish:
                                closed_inside = candle1['open'] < candle2['close'] < candle1['close']
                            else:
                                closed_inside = candle1['close'] < candle2['close'] < candle1['open']

                            if closed_inside:
                                pattern_data = {
                                    'symbol': symbol_name,
                                    'timeframe': '1D',
                                    'pattern_direction': 'BULLISH' if is_bullish else 'BEARISH',
                                    'marubozu': {
                                        'date': candle1['trade_date'],
                                        'open': float(candle1['open']),
                                        'high': float(candle1['high']),
                                        'low': float(candle1['low']),
                                        'close': float(candle1['close']),
                                        'body_pct': round(body1_pct, 2),
                                        'body_move_pct': round(body1_move_pct, 2)
                                    },
                                    'doji': {
                                        'date': candle2['trade_date'],
                                        'open': float(candle2['open']),
                                        'high': float(candle2['high']),
                                        'low': float(candle2['low']),
                                        'close': float(candle2['close']),
                                        'body_pct': round(body2_pct, 2)
                                    }
                                }

                                detected_patterns.append({
                                    'symbol_id': int(symbol_id),
                                    'pattern_type_id': pattern_type_id,
                                    'pattern_date': candle2['trade_date'],
                                    'timeframe': '1D',
                                    'pattern_direction': 'BULLISH' if is_bullish else 'BEARISH',
                                    'pattern_data': json.dumps(pattern_data),
                                    'confidence_score': 85.0
                                })

        logger.info(f"Found {len(detected_patterns)} daily patterns")

        # Detect patterns in aggregated data
        for timeframe in ['WEEKLY', 'MONTHLY']:
            logger.info(f"Detecting {timeframe} patterns...")

            df = pd.read_sql_query(f"""
                SELECT a.*, s.symbol
                FROM aggregated_ohlc a
                JOIN symbols s ON a.symbol_id = s.symbol_id
                WHERE a.timeframe = '{timeframe}'
                ORDER BY a.symbol_id, a.period_end
            """, conn)

            if df.empty:
                logger.warning(f"No {timeframe} aggregated data found")
                continue

            for symbol_id in df['symbol_id'].unique():
                symbol_data = df[df['symbol_id'] == symbol_id].reset_index(drop=True)
                symbol_name = symbol_data.iloc[0]['symbol']

                for i in range(len(symbol_data) - 1):
                    candle1 = symbol_data.iloc[i]
                    candle2 = symbol_data.iloc[i + 1]

                    body1 = abs(candle1['close'] - candle1['open'])
                    range1 = candle1['high'] - candle1['low']

                    if range1 == 0:
                        continue

                    body1_pct = (body1 / range1) * 100
                    body1_move_pct = (body1 / candle1['open']) * 100

                    body2 = abs(candle2['close'] - candle2['open'])
                    range2 = candle2['high'] - candle2['low']

                    if range2 == 0:
                        continue

                    body2_pct = (body2 / range2) * 100

                    if body1_pct >= 80 and body1_move_pct >= 2:
                        if body2_pct < 25:
                            if candle2['high'] > candle1['high']:
                                is_bullish = candle1['close'] > candle1['open']

                                if is_bullish:
                                    closed_inside = candle1['open'] < candle2['close'] < candle1['close']
                                else:
                                    closed_inside = candle1['close'] < candle2['close'] < candle1['open']

                                if closed_inside:
                                    pattern_data = {
                                        'symbol': symbol_name,
                                        'timeframe': timeframe,
                                        'pattern_direction': 'BULLISH' if is_bullish else 'BEARISH',
                                        'marubozu': {
                                            'period_start': candle1['period_start'],
                                            'period_end': candle1['period_end'],
                                            'open': float(candle1['open']),
                                            'high': float(candle1['high']),
                                            'low': float(candle1['low']),
                                            'close': float(candle1['close']),
                                            'body_pct': round(body1_pct, 2),
                                            'body_move_pct': round(body1_move_pct, 2)
                                        },
                                        'doji': {
                                            'period_start': candle2['period_start'],
                                            'period_end': candle2['period_end'],
                                            'open': float(candle2['open']),
                                            'high': float(candle2['high']),
                                            'low': float(candle2['low']),
                                            'close': float(candle2['close']),
                                            'body_pct': round(body2_pct, 2)
                                        }
                                    }

                                    detected_patterns.append({
                                        'symbol_id': int(symbol_id),
                                        'pattern_type_id': pattern_type_id,
                                        'pattern_date': candle2['period_end'],
                                        'timeframe': timeframe,
                                        'pattern_direction': 'BULLISH' if is_bullish else 'BEARISH',
                                        'pattern_data': json.dumps(pattern_data),
                                        'confidence_score': 85.0
                                    })

            logger.info(f"Found {len([p for p in detected_patterns if p['timeframe'] == timeframe])} {timeframe} patterns")

        # Insert all detected patterns
        if detected_patterns:
            logger.info(f"Inserting {len(detected_patterns)} total patterns...")
            cursor.executemany("""
                INSERT INTO detected_patterns
                (symbol_id, pattern_type_id, pattern_date, timeframe, pattern_direction, pattern_data, confidence_score)
                VALUES (:symbol_id, :pattern_type_id, :pattern_date, :timeframe, :pattern_direction, :pattern_data, :confidence_score)
            """, detected_patterns)
            conn.commit()
        else:
            logger.info("No patterns detected")

    except Exception as e:
        logger.error(f"Error detecting patterns: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    initialize_render_database()