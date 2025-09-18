"""
PostgreSQL Database Manager for Pattern Scanner
Handles all database operations including data updates, pattern storage, and queries
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor, execute_batch
import pandas as pd
from datetime import datetime, timedelta
import json
import logging
from typing import Dict, List, Optional, Tuple
from contextlib import contextmanager
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager:
    """PostgreSQL database manager for pattern scanner"""

    def __init__(self, connection_string: str = None):
        """Initialize database connection"""
        if connection_string:
            self.connection_string = connection_string
        else:
            # Build from environment variables
            self.connection_string = (
                f"host={os.getenv('DB_HOST', 'localhost')} "
                f"port={os.getenv('DB_PORT', '5432')} "
                f"dbname={os.getenv('DB_NAME', 'pattern_scanner_db')} "
                f"user={os.getenv('DB_USER', 'postgres')} "
                f"password={os.getenv('DB_PASSWORD', '')}"
            )

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = psycopg2.connect(self.connection_string)
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    @contextmanager
    def get_cursor(self, dict_cursor: bool = True):
        """Context manager for database cursors"""
        with self.get_connection() as conn:
            cursor_factory = RealDictCursor if dict_cursor else None
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
            finally:
                cursor.close()

    # =====================================================
    # SYMBOL MANAGEMENT
    # =====================================================

    def upsert_symbols(self, symbols: List[Dict]) -> int:
        """Insert or update symbols in database"""
        query = """
            INSERT INTO symbols (symbol, exchange, instrument_type, is_fno, dhan_security_id)
            VALUES (%(symbol)s, %(exchange)s, %(instrument_type)s, %(is_fno)s, %(dhan_security_id)s)
            ON CONFLICT (symbol) DO UPDATE SET
                exchange = EXCLUDED.exchange,
                instrument_type = EXCLUDED.instrument_type,
                is_fno = EXCLUDED.is_fno,
                dhan_security_id = EXCLUDED.dhan_security_id,
                updated_at = CURRENT_TIMESTAMP
        """

        with self.get_cursor(dict_cursor=False) as cursor:
            execute_batch(cursor, query, symbols)
            return cursor.rowcount

    def get_active_symbols(self, fno_only: bool = False) -> List[Dict]:
        """Get list of active symbols"""
        query = """
            SELECT symbol_id, symbol, dhan_security_id
            FROM symbols
            WHERE is_active = TRUE
        """
        if fno_only:
            query += " AND is_fno = TRUE"
        query += " ORDER BY symbol"

        with self.get_cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    # =====================================================
    # OHLC DATA MANAGEMENT
    # =====================================================

    def bulk_insert_daily_ohlc(self, data: List[Dict]) -> int:
        """Bulk insert daily OHLC data"""
        query = """
            INSERT INTO daily_ohlc (symbol_id, trade_date, open, high, low, close, volume)
            VALUES (%(symbol_id)s, %(trade_date)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
            ON CONFLICT (symbol_id, trade_date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
        """

        with self.get_cursor(dict_cursor=False) as cursor:
            execute_batch(cursor, query, data, page_size=1000)
            return cursor.rowcount

    def get_ohlc_data(self, symbol: str, start_date: str, end_date: str, timeframe: str = '1D') -> pd.DataFrame:
        """Get OHLC data for a symbol"""
        if timeframe == '1D':
            query = """
                SELECT d.trade_date as date, d.open, d.high, d.low, d.close, d.volume,
                       d.body_pct, d.change_pct, d.is_bullish
                FROM daily_ohlc d
                JOIN symbols s ON d.symbol_id = s.symbol_id
                WHERE s.symbol = %s AND d.trade_date BETWEEN %s AND %s
                ORDER BY d.trade_date
            """
        else:
            query = """
                SELECT a.period_start as date, a.open, a.high, a.low, a.close, a.volume,
                       a.body_pct, a.change_pct, a.is_bullish, a.trading_days
                FROM aggregated_ohlc a
                JOIN symbols s ON a.symbol_id = s.symbol_id
                WHERE s.symbol = %s AND a.timeframe = %s
                    AND a.period_start BETWEEN %s AND %s
                ORDER BY a.period_start
            """

        with self.get_connection() as conn:
            if timeframe == '1D':
                df = pd.read_sql_query(query, conn, params=(symbol, start_date, end_date))
            else:
                df = pd.read_sql_query(query, conn, params=(symbol, timeframe, start_date, end_date))

            return df

    def get_latest_update_date(self, symbol: str = None) -> Optional[datetime]:
        """Get the latest data update date for a symbol or all symbols"""
        query = """
            SELECT MAX(d.trade_date) as latest_date
            FROM daily_ohlc d
        """
        if symbol:
            query += """
                JOIN symbols s ON d.symbol_id = s.symbol_id
                WHERE s.symbol = %s
            """

        with self.get_cursor() as cursor:
            cursor.execute(query, (symbol,) if symbol else ())
            result = cursor.fetchone()
            return result['latest_date'] if result and result['latest_date'] else None

    # =====================================================
    # PATTERN MANAGEMENT
    # =====================================================

    def save_pattern(self, pattern: Dict) -> int:
        """Save a detected pattern to database"""
        # First, check if pattern already exists
        check_query = """
            SELECT pattern_exists(
                (SELECT symbol_id FROM symbols WHERE symbol = %(symbol)s),
                (SELECT pattern_type_id FROM pattern_types WHERE pattern_name = %(pattern_type)s),
                %(pattern_date)s::date,
                %(timeframe)s
            )
        """

        with self.get_cursor() as cursor:
            cursor.execute(check_query, pattern)
            if cursor.fetchone()['pattern_exists']:
                logger.info(f"Pattern already exists: {pattern['symbol']} - {pattern['pattern_type']} on {pattern['pattern_date']}")
                return 0

        # Insert pattern
        insert_query = """
            INSERT INTO detected_patterns (
                pattern_type_id, symbol_id, timeframe, pattern_date,
                pattern_direction, confidence_score, pattern_data,
                breakout_level, stop_loss_level, target_level
            )
            VALUES (
                (SELECT pattern_type_id FROM pattern_types WHERE pattern_name = %(pattern_type)s),
                (SELECT symbol_id FROM symbols WHERE symbol = %(symbol)s),
                %(timeframe)s, %(pattern_date)s, %(pattern_direction)s,
                %(confidence_score)s, %(pattern_data)s::jsonb,
                %(breakout_level)s, %(stop_loss_level)s, %(target_level)s
            )
            RETURNING pattern_id
        """

        with self.get_cursor() as cursor:
            cursor.execute(insert_query, pattern)
            pattern_id = cursor.fetchone()['pattern_id']

            # Insert pattern candles if provided
            if 'candles' in pattern:
                candle_query = """
                    INSERT INTO pattern_candles (
                        pattern_id, candle_position, trade_date, candle_type,
                        open, high, low, close, volume, body_pct, change_pct, notes
                    )
                    VALUES (
                        %s, %(position)s, %(date)s, %(type)s,
                        %(open)s, %(high)s, %(low)s, %(close)s,
                        %(volume)s, %(body_pct)s, %(change_pct)s, %(notes)s
                    )
                """
                for candle in pattern['candles']:
                    candle['pattern_id'] = pattern_id
                    cursor.execute(candle_query, (pattern_id,), candle)

            return pattern_id

    def get_patterns(self, symbol: str = None, pattern_type: str = None,
                    start_date: str = None, limit: int = 100) -> List[Dict]:
        """Get detected patterns with filters"""
        query = """
            SELECT
                s.symbol,
                pt.pattern_name,
                dp.pattern_direction,
                dp.pattern_date,
                dp.timeframe,
                dp.confidence_score,
                dp.pattern_data,
                dp.breakout_level,
                dp.stop_loss_level,
                dp.target_level,
                dp.scan_timestamp
            FROM detected_patterns dp
            JOIN symbols s ON dp.symbol_id = s.symbol_id
            JOIN pattern_types pt ON dp.pattern_type_id = pt.pattern_type_id
            WHERE dp.is_active = TRUE
        """

        params = []
        if symbol:
            query += " AND s.symbol = %s"
            params.append(symbol)
        if pattern_type:
            query += " AND pt.pattern_name = %s"
            params.append(pattern_type)
        if start_date:
            query += " AND dp.pattern_date >= %s"
            params.append(start_date)

        query += " ORDER BY dp.pattern_date DESC, dp.scan_timestamp DESC LIMIT %s"
        params.append(limit)

        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()

    def get_todays_patterns(self) -> List[Dict]:
        """Get patterns detected today (yesterday Marubozu → today Doji)"""
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)

        query = """
            SELECT
                s.symbol,
                pt.pattern_name,
                dp.pattern_direction,
                dp.pattern_date,
                dp.timeframe,
                dp.confidence_score,
                dp.pattern_data,
                dp.breakout_level,
                dp.stop_loss_level,
                dp.target_level
            FROM detected_patterns dp
            JOIN symbols s ON dp.symbol_id = s.symbol_id
            JOIN pattern_types pt ON dp.pattern_type_id = pt.pattern_type_id
            WHERE dp.pattern_date = %s
                AND dp.is_active = TRUE
                AND pt.pattern_name = 'Marubozu-Doji'
                AND dp.pattern_data->>'marubozu_date' = %s
        ORDER BY s.symbol
        """

        with self.get_cursor() as cursor:
            cursor.execute(query, (today, yesterday.isoformat()))
            return cursor.fetchall()

    # =====================================================
    # DATA UPDATE STRATEGY
    # =====================================================

    def update_daily_data(self, symbols: List[str], data_fetcher) -> Dict:
        """Update daily OHLC data for symbols"""
        start_time = datetime.now()
        results = {
            'symbols_updated': 0,
            'records_inserted': 0,
            'errors': []
        }

        try:
            # Log update start
            self.log_update_start('daily_ohlc', 'incremental')

            # Get symbol IDs
            symbol_map = {}
            with self.get_cursor() as cursor:
                cursor.execute(
                    "SELECT symbol_id, symbol FROM symbols WHERE symbol = ANY(%s)",
                    (symbols,)
                )
                for row in cursor.fetchall():
                    symbol_map[row['symbol']] = row['symbol_id']

            # Determine date range for update
            latest_date = self.get_latest_update_date()
            if latest_date:
                start_date = latest_date - timedelta(days=1)  # Overlap by 1 day
            else:
                start_date = datetime.now() - timedelta(days=365)  # Get 1 year if no data

            # Fetch and insert data
            for symbol in symbols:
                if symbol not in symbol_map:
                    logger.warning(f"Symbol {symbol} not found in database")
                    continue

                try:
                    # Fetch data from API
                    df = data_fetcher(symbol, start_date, datetime.now())

                    if df.empty:
                        logger.warning(f"No data for {symbol}")
                        continue

                    # Prepare data for insertion
                    records = []
                    for _, row in df.iterrows():
                        records.append({
                            'symbol_id': symbol_map[symbol],
                            'trade_date': row['date'],
                            'open': row['open'],
                            'high': row['high'],
                            'low': row['low'],
                            'close': row['close'],
                            'volume': row.get('volume', 0)
                        })

                    # Bulk insert
                    inserted = self.bulk_insert_daily_ohlc(records)
                    results['records_inserted'] += inserted
                    results['symbols_updated'] += 1

                    logger.info(f"Updated {symbol}: {inserted} records")

                except Exception as e:
                    error_msg = f"Error updating {symbol}: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)

            # Update aggregated data
            self.update_aggregated_data()

            # Log update completion
            self.log_update_complete('daily_ohlc', results['records_inserted'])

        except Exception as e:
            logger.error(f"Update failed: {e}")
            self.log_update_failed('daily_ohlc', str(e))
            raise

        results['duration'] = (datetime.now() - start_time).total_seconds()
        return results

    def update_aggregated_data(self):
        """Update weekly and monthly aggregated data"""
        queries = {
            '1W': """
                INSERT INTO aggregated_ohlc (symbol_id, timeframe, period_start, period_end,
                    open, high, low, close, volume, trading_days)
                SELECT
                    symbol_id,
                    '1W' as timeframe,
                    date_trunc('week', trade_date)::date as period_start,
                    (date_trunc('week', trade_date) + interval '6 days')::date as period_end,
                    (array_agg(open ORDER BY trade_date))[1] as open,
                    MAX(high) as high,
                    MIN(low) as low,
                    (array_agg(close ORDER BY trade_date DESC))[1] as close,
                    SUM(volume) as volume,
                    COUNT(*) as trading_days
                FROM daily_ohlc
                WHERE trade_date >= CURRENT_DATE - INTERVAL '3 months'
                GROUP BY symbol_id, date_trunc('week', trade_date)
                ON CONFLICT (symbol_id, timeframe, period_start) DO UPDATE SET
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    trading_days = EXCLUDED.trading_days
            """,
            '1M': """
                INSERT INTO aggregated_ohlc (symbol_id, timeframe, period_start, period_end,
                    open, high, low, close, volume, trading_days)
                SELECT
                    symbol_id,
                    '1M' as timeframe,
                    date_trunc('month', trade_date)::date as period_start,
                    (date_trunc('month', trade_date) + interval '1 month - 1 day')::date as period_end,
                    (array_agg(open ORDER BY trade_date))[1] as open,
                    MAX(high) as high,
                    MIN(low) as low,
                    (array_agg(close ORDER BY trade_date DESC))[1] as close,
                    SUM(volume) as volume,
                    COUNT(*) as trading_days
                FROM daily_ohlc
                WHERE trade_date >= CURRENT_DATE - INTERVAL '1 year'
                GROUP BY symbol_id, date_trunc('month', trade_date)
                ON CONFLICT (symbol_id, timeframe, period_start) DO UPDATE SET
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    trading_days = EXCLUDED.trading_days
            """
        }

        with self.get_cursor(dict_cursor=False) as cursor:
            for timeframe, query in queries.items():
                cursor.execute(query)
                logger.info(f"Updated {cursor.rowcount} {timeframe} aggregated records")

    def log_update_start(self, table_name: str, update_type: str):
        """Log the start of a data update"""
        query = """
            INSERT INTO data_update_log (table_name, update_type, status)
            VALUES (%s, %s, 'in_progress')
        """
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute(query, (table_name, update_type))

    def log_update_complete(self, table_name: str, records_affected: int):
        """Log successful completion of data update"""
        query = """
            UPDATE data_update_log
            SET status = 'completed',
                records_affected = %s,
                completed_at = CURRENT_TIMESTAMP
            WHERE table_name = %s
                AND status = 'in_progress'
                AND update_id = (
                    SELECT MAX(update_id) FROM data_update_log
                    WHERE table_name = %s AND status = 'in_progress'
                )
        """
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute(query, (records_affected, table_name, table_name))

    def log_update_failed(self, table_name: str, error_message: str):
        """Log failed data update"""
        query = """
            UPDATE data_update_log
            SET status = 'failed',
                error_message = %s,
                completed_at = CURRENT_TIMESTAMP
            WHERE table_name = %s
                AND status = 'in_progress'
                AND update_id = (
                    SELECT MAX(update_id) FROM data_update_log
                    WHERE table_name = %s AND status = 'in_progress'
                )
        """
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute(query, (error_message, table_name, table_name))

    # =====================================================
    # SCAN OPERATIONS
    # =====================================================

    def save_scan_result(self, config_id: int, scan_type: str, results: Dict) -> int:
        """Save scan results to history"""
        query = """
            INSERT INTO scan_history (
                config_id, scan_type, symbols_scanned,
                patterns_found, execution_time_ms, scan_results
            )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            RETURNING scan_id
        """

        with self.get_cursor() as cursor:
            cursor.execute(query, (
                config_id,
                scan_type,
                results.get('symbols_scanned', 0),
                results.get('patterns_found', 0),
                results.get('execution_time_ms', 0),
                json.dumps(results)
            ))
            return cursor.fetchone()['scan_id']

    def refresh_materialized_views(self):
        """Refresh materialized views for performance"""
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY latest_patterns")
            logger.info("Refreshed materialized views")

    # =====================================================
    # HEALTH CHECKS
    # =====================================================

    def check_data_freshness(self) -> Dict:
        """Check data freshness for all symbols"""
        query = """
            SELECT
                COUNT(*) as total_symbols,
                COUNT(CASE WHEN latest_date = CURRENT_DATE THEN 1 END) as current_symbols,
                COUNT(CASE WHEN latest_date = CURRENT_DATE - 1 THEN 1 END) as one_day_old,
                COUNT(CASE WHEN latest_date < CURRENT_DATE - 1 THEN 1 END) as stale_symbols,
                MIN(latest_date) as oldest_data,
                MAX(latest_date) as newest_data
            FROM (
                SELECT s.symbol_id, MAX(d.trade_date) as latest_date
                FROM symbols s
                LEFT JOIN daily_ohlc d ON s.symbol_id = d.symbol_id
                WHERE s.is_active = TRUE
                GROUP BY s.symbol_id
            ) symbol_dates
        """

        with self.get_cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchone()

    def get_database_stats(self) -> Dict:
        """Get database statistics"""
        stats = {}

        queries = {
            'total_symbols': "SELECT COUNT(*) as count FROM symbols WHERE is_active = TRUE",
            'total_daily_records': "SELECT COUNT(*) as count FROM daily_ohlc",
            'total_patterns': "SELECT COUNT(*) as count FROM detected_patterns",
            'patterns_today': """
                SELECT COUNT(*) as count FROM detected_patterns
                WHERE DATE(scan_timestamp) = CURRENT_DATE
            """,
            'database_size': """
                SELECT pg_size_pretty(pg_database_size(current_database())) as size
            """
        }

        with self.get_cursor() as cursor:
            for key, query in queries.items():
                cursor.execute(query)
                result = cursor.fetchone()
                stats[key] = result.get('count', result.get('size'))

        return stats


# =====================================================
# UTILITY FUNCTIONS
# =====================================================

def initialize_database(db_manager: DatabaseManager, symbol_file: str = 'fno_symbols_corrected.csv'):
    """Initialize database with symbols"""
    import pandas as pd

    # Load symbols from CSV
    df = pd.read_csv(symbol_file)

    # Prepare symbol data
    symbols = []
    for _, row in df.iterrows():
        symbols.append({
            'symbol': row['Symbol'],
            'exchange': 'NSE',
            'instrument_type': 'EQUITY',
            'is_fno': True,
            'dhan_security_id': row.get('SecurityId', '')
        })

    # Insert symbols
    count = db_manager.upsert_symbols(symbols)
    logger.info(f"Initialized {count} symbols in database")

    return count


def schedule_daily_update(db_manager: DatabaseManager, dhan_client):
    """Schedule daily data updates (run after market close)"""
    from datetime import time
    import schedule

    def update_job():
        logger.info("Starting scheduled daily update...")

        # Get active F&O symbols
        symbols = db_manager.get_active_symbols(fno_only=True)
        symbol_list = [s['symbol'] for s in symbols]

        # Update data
        results = db_manager.update_daily_data(
            symbol_list,
            lambda sym, start, end: dhan_client.get_historical_data(sym, start, end)
        )

        logger.info(f"Daily update completed: {results}")

        # Refresh views
        db_manager.refresh_materialized_views()

    # Schedule at 4:30 PM (after market close)
    schedule.every().day.at("16:30").do(update_job)

    logger.info("Daily update scheduled for 4:30 PM")


if __name__ == "__main__":
    # Test database connection
    db = DatabaseManager()
    stats = db.get_database_stats()
    print("Database Stats:", stats)