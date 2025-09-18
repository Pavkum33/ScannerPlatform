"""
SQLite Database Manager for Pattern Scanner
No installation required - Works immediately!
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import json
import logging
from typing import Dict, List, Optional
import os
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SQLiteDBManager:
    """SQLite database manager - No installation required!"""

    def __init__(self, db_path: str = "pattern_scanner.db"):
        """Initialize SQLite database"""
        self.db_path = db_path
        self.init_database()

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Enable column access by name
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

    def init_database(self):
        """Initialize database schema"""
        schema = """
        -- Symbols table
        CREATE TABLE IF NOT EXISTS symbols (
            symbol_id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT UNIQUE NOT NULL,
            exchange TEXT DEFAULT 'NSE',
            instrument_type TEXT DEFAULT 'EQUITY',
            is_fno INTEGER DEFAULT 1,
            dhan_security_id TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Daily OHLC data
        CREATE TABLE IF NOT EXISTS daily_ohlc (
            ohlc_id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol_id INTEGER NOT NULL,
            trade_date DATE NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume INTEGER DEFAULT 0,
            body_size REAL,
            range_size REAL,
            body_pct REAL,
            change_pct REAL,
            is_bullish INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (symbol_id) REFERENCES symbols(symbol_id),
            UNIQUE(symbol_id, trade_date)
        );

        -- Aggregated OHLC
        CREATE TABLE IF NOT EXISTS aggregated_ohlc (
            agg_id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol_id INTEGER NOT NULL,
            timeframe TEXT NOT NULL,
            period_start DATE NOT NULL,
            period_end DATE NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume INTEGER DEFAULT 0,
            trading_days INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (symbol_id) REFERENCES symbols(symbol_id),
            UNIQUE(symbol_id, timeframe, period_start)
        );

        -- Pattern types
        CREATE TABLE IF NOT EXISTS pattern_types (
            pattern_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern_name TEXT UNIQUE NOT NULL,
            pattern_category TEXT NOT NULL,
            candles_required INTEGER DEFAULT 2,
            description TEXT,
            is_active INTEGER DEFAULT 1
        );

        -- Detected patterns
        CREATE TABLE IF NOT EXISTS detected_patterns (
            pattern_id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern_type_id INTEGER NOT NULL,
            symbol_id INTEGER NOT NULL,
            timeframe TEXT DEFAULT '1D',
            pattern_date DATE NOT NULL,
            pattern_direction TEXT,
            confidence_score REAL DEFAULT 100.0,
            pattern_data TEXT,
            breakout_level REAL,
            stop_loss_level REAL,
            target_level REAL,
            scan_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (pattern_type_id) REFERENCES pattern_types(pattern_type_id),
            FOREIGN KEY (symbol_id) REFERENCES symbols(symbol_id)
        );

        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_daily_ohlc_symbol_date
            ON daily_ohlc(symbol_id, trade_date DESC);
        CREATE INDEX IF NOT EXISTS idx_daily_ohlc_date
            ON daily_ohlc(trade_date DESC);
        CREATE INDEX IF NOT EXISTS idx_patterns_symbol_date
            ON detected_patterns(symbol_id, pattern_date DESC);
        CREATE INDEX IF NOT EXISTS idx_patterns_scan_timestamp
            ON detected_patterns(scan_timestamp DESC);
        """

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executescript(schema)

            # Insert default pattern types
            cursor.execute("""
                INSERT OR IGNORE INTO pattern_types (pattern_name, pattern_category, candles_required, description)
                VALUES
                ('Marubozu-Doji', 'reversal', 2, 'Strong move followed by indecision'),
                ('Hammer', 'reversal', 1, 'Bullish reversal at bottom'),
                ('Shooting Star', 'reversal', 1, 'Bearish reversal at top'),
                ('Bullish Engulfing', 'reversal', 2, 'Bullish reversal pattern'),
                ('Bearish Engulfing', 'reversal', 2, 'Bearish reversal pattern')
            """)

        logger.info(f"Database initialized: {self.db_path}")

    def upsert_symbols(self, symbols: List[Dict]) -> int:
        """Insert or update symbols"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            count = 0

            for symbol_data in symbols:
                cursor.execute("""
                    INSERT OR REPLACE INTO symbols (symbol, exchange, instrument_type, is_fno, dhan_security_id)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    symbol_data['symbol'],
                    symbol_data.get('exchange', 'NSE'),
                    symbol_data.get('instrument_type', 'EQUITY'),
                    1 if symbol_data.get('is_fno', True) else 0,
                    symbol_data.get('dhan_security_id', '')
                ))
                count += 1

            return count

    def get_active_symbols(self, fno_only: bool = False) -> List[Dict]:
        """Get list of active symbols"""
        query = """
            SELECT symbol_id, symbol, dhan_security_id
            FROM symbols
            WHERE is_active = 1
        """
        if fno_only:
            query += " AND is_fno = 1"
        query += " ORDER BY symbol"

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)

            results = []
            for row in cursor.fetchall():
                results.append({
                    'symbol_id': row[0],
                    'symbol': row[1],
                    'dhan_security_id': row[2]
                })

            return results

    def bulk_insert_daily_ohlc(self, data: List[Dict]) -> int:
        """Bulk insert daily OHLC data"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            count = 0

            for record in data:
                # Calculate derived fields
                body_size = abs(record['close'] - record['open'])
                range_size = record['high'] - record['low']
                body_pct = (body_size / range_size * 100) if range_size > 0 else 0
                change_pct = ((record['close'] - record['open']) / record['open'] * 100) if record['open'] > 0 else 0
                is_bullish = 1 if record['close'] > record['open'] else 0

                cursor.execute("""
                    INSERT OR REPLACE INTO daily_ohlc
                    (symbol_id, trade_date, open, high, low, close, volume,
                     body_size, range_size, body_pct, change_pct, is_bullish)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    record['symbol_id'],
                    record['trade_date'],
                    record['open'],
                    record['high'],
                    record['low'],
                    record['close'],
                    record.get('volume', 0),
                    body_size,
                    range_size,
                    body_pct,
                    change_pct,
                    is_bullish
                ))
                count += 1

            return count

    def get_ohlc_data(self, symbol: str, start_date, end_date, timeframe: str = '1D') -> pd.DataFrame:
        """Get OHLC data for a symbol"""
        if timeframe == '1D':
            query = """
                SELECT d.trade_date as date, d.open, d.high, d.low, d.close, d.volume,
                       d.body_pct, d.change_pct, d.is_bullish
                FROM daily_ohlc d
                JOIN symbols s ON d.symbol_id = s.symbol_id
                WHERE s.symbol = ? AND d.trade_date BETWEEN ? AND ?
                ORDER BY d.trade_date
            """
            params = (symbol, start_date, end_date)
        else:
            query = """
                SELECT a.period_start as date, a.open, a.high, a.low, a.close, a.volume,
                       a.trading_days
                FROM aggregated_ohlc a
                JOIN symbols s ON a.symbol_id = s.symbol_id
                WHERE s.symbol = ? AND a.timeframe = ?
                    AND a.period_start BETWEEN ? AND ?
                ORDER BY a.period_start
            """
            params = (symbol, timeframe, start_date, end_date)

        with self.get_connection() as conn:
            df = pd.read_sql_query(query, conn, params=params)
            return df

    def get_latest_update_date(self, symbol: str = None) -> Optional[datetime]:
        """Get the latest data update date"""
        query = "SELECT MAX(trade_date) as latest_date FROM daily_ohlc"
        params = ()

        if symbol:
            query += " WHERE symbol_id = (SELECT symbol_id FROM symbols WHERE symbol = ?)"
            params = (symbol,)

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            result = cursor.fetchone()

            if result and result[0]:
                return datetime.strptime(result[0], '%Y-%m-%d').date()
            return None

    def save_pattern(self, pattern: Dict) -> int:
        """Save a detected pattern"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Get symbol_id
            cursor.execute("SELECT symbol_id FROM symbols WHERE symbol = ?", (pattern['symbol'],))
            symbol_result = cursor.fetchone()
            if not symbol_result:
                return 0
            symbol_id = symbol_result[0]

            # Get pattern_type_id
            cursor.execute("SELECT pattern_type_id FROM pattern_types WHERE pattern_name = ?",
                          (pattern.get('pattern_type', 'Marubozu-Doji'),))
            pattern_type_result = cursor.fetchone()
            if not pattern_type_result:
                return 0
            pattern_type_id = pattern_type_result[0]

            # Insert pattern
            cursor.execute("""
                INSERT INTO detected_patterns
                (pattern_type_id, symbol_id, timeframe, pattern_date,
                 pattern_direction, confidence_score, pattern_data,
                 breakout_level, stop_loss_level, target_level)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pattern_type_id,
                symbol_id,
                pattern.get('timeframe', '1D'),
                pattern['pattern_date'],
                pattern.get('pattern_direction'),
                pattern.get('confidence_score', 100.0),
                json.dumps(pattern.get('pattern_data', {})),
                pattern.get('breakout_level'),
                pattern.get('stop_loss_level'),
                pattern.get('target_level')
            ))

            return cursor.lastrowid

    def update_aggregated_data(self):
        """Update weekly and monthly aggregates"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Weekly aggregation
            cursor.execute("""
                INSERT OR REPLACE INTO aggregated_ohlc
                (symbol_id, timeframe, period_start, period_end, open, high, low, close, volume, trading_days)
                SELECT
                    symbol_id,
                    '1W' as timeframe,
                    DATE(trade_date, 'weekday 0', '-6 days') as period_start,
                    DATE(trade_date, 'weekday 0') as period_end,
                    (SELECT open FROM daily_ohlc d2
                     WHERE d2.symbol_id = d1.symbol_id
                     AND DATE(d2.trade_date, 'weekday 0', '-6 days') = DATE(d1.trade_date, 'weekday 0', '-6 days')
                     ORDER BY d2.trade_date LIMIT 1) as open,
                    MAX(high) as high,
                    MIN(low) as low,
                    (SELECT close FROM daily_ohlc d3
                     WHERE d3.symbol_id = d1.symbol_id
                     AND DATE(d3.trade_date, 'weekday 0', '-6 days') = DATE(d1.trade_date, 'weekday 0', '-6 days')
                     ORDER BY d3.trade_date DESC LIMIT 1) as close,
                    SUM(volume) as volume,
                    COUNT(*) as trading_days
                FROM daily_ohlc d1
                WHERE trade_date >= DATE('now', '-3 months')
                GROUP BY symbol_id, DATE(trade_date, 'weekday 0', '-6 days')
            """)

            logger.info(f"Updated {cursor.rowcount} weekly aggregates")

    def get_database_stats(self) -> Dict:
        """Get database statistics"""
        stats = {}

        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Total symbols
            cursor.execute("SELECT COUNT(*) FROM symbols WHERE is_active = 1")
            stats['total_symbols'] = cursor.fetchone()[0]

            # Total daily records
            cursor.execute("SELECT COUNT(*) FROM daily_ohlc")
            stats['total_daily_records'] = cursor.fetchone()[0]

            # Total patterns
            cursor.execute("SELECT COUNT(*) FROM detected_patterns")
            stats['total_patterns'] = cursor.fetchone()[0]

            # Patterns today
            cursor.execute("""
                SELECT COUNT(*) FROM detected_patterns
                WHERE DATE(scan_timestamp) = DATE('now')
            """)
            stats['patterns_today'] = cursor.fetchone()[0]

            # Database size
            stats['database_size'] = f"{os.path.getsize(self.db_path) / (1024*1024):.1f} MB"

        return stats

    def check_data_freshness(self) -> Dict:
        """Check data freshness for all symbols"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT
                    COUNT(DISTINCT symbol_id) as total_symbols,
                    SUM(CASE WHEN latest_date = DATE('now') THEN 1 ELSE 0 END) as current_symbols,
                    SUM(CASE WHEN latest_date = DATE('now', '-1 day') THEN 1 ELSE 0 END) as one_day_old,
                    SUM(CASE WHEN latest_date < DATE('now', '-1 day') THEN 1 ELSE 0 END) as stale_symbols,
                    MIN(latest_date) as oldest_data,
                    MAX(latest_date) as newest_data
                FROM (
                    SELECT symbol_id, MAX(trade_date) as latest_date
                    FROM daily_ohlc
                    GROUP BY symbol_id
                ) symbol_dates
            """)

            result = cursor.fetchone()

            return {
                'total_symbols': result[0] or 0,
                'current_symbols': result[1] or 0,
                'one_day_old': result[2] or 0,
                'stale_symbols': result[3] or 0,
                'oldest_data': result[4],
                'newest_data': result[5]
            }