"""
SQLite-based Scanner Engine
100x faster scanning using local database
Preserves ALL pattern detection logic from original scanner
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.sqlite_db_manager import SQLiteDBManager
from scanner.dhan_client import DhanClient

logger = logging.getLogger(__name__)


class SQLiteScannerEngine:
    """
    Fast scanner using SQLite database
    Drop-in replacement for original ScannerEngine
    """

    def __init__(self, db_path: str = "database/pattern_scanner.db"):
        """Initialize SQLite scanner"""
        self.db = SQLiteDBManager(db_path)
        self.dhan = None  # Only initialize if needed for updates
        logger.info("SQLite Scanner Engine initialized")

    def scan(self, symbols: List[str], timeframe: str = "1D",
            history: int = 30, min_body_move_pct: float = 4.0) -> Dict:
        """
        Scan for patterns using database
        EXACT SAME INTERFACE as original scanner

        Args:
            symbols: List of symbols to scan
            timeframe: "1D", "1W", or "1M"
            history: Number of periods to look back
            min_body_move_pct: Minimum body movement % for Marubozu

        Returns:
            Dict with results in SAME FORMAT as original
        """
        logger.info(f"Starting SQLite scan: {len(symbols)} symbols")
        start_time = datetime.now()

        results = []
        successful_symbols = 0
        failed_symbols = []
        skipped_no_data = []

        # Calculate date range
        end_date = datetime.now().date()
        if timeframe == "1D":
            start_date = end_date - timedelta(days=history)
        elif timeframe == "1W":
            start_date = end_date - timedelta(weeks=history)
        else:  # 1M
            start_date = end_date - timedelta(days=history * 30)

        for symbol in symbols:
            try:
                # Get data from database (INSTANT!)
                df = self.db.get_ohlc_data(symbol, start_date, end_date, timeframe)

                if df.empty or len(df) < 2:
                    skipped_no_data.append(symbol)
                    continue

                # Convert date column to timestamp for compatibility
                df['timestamp'] = pd.to_datetime(df['date'])

                # EXACT SAME PATTERN DETECTION AS ORIGINAL
                patterns = self._detect_marubozu_doji_patterns(
                    df, symbol, min_body_move_pct
                )

                if patterns:
                    for pattern in patterns:
                        pattern['symbol'] = symbol
                        pattern['timeframe'] = timeframe
                        results.append(pattern)

                        # Store in database
                        self._store_pattern(pattern)

                successful_symbols += 1

            except Exception as e:
                logger.error(f"Error scanning {symbol}: {e}")
                failed_symbols.append(symbol)

        # Calculate statistics
        elapsed = (datetime.now() - start_time).total_seconds()

        response = {
            "results": results,
            "statistics": {
                "symbols_scanned": len(symbols),
                "successful_scans": successful_symbols,
                "failed_scans": len(failed_symbols),
                "skipped_no_data": len(skipped_no_data),
                "patterns_found": len(results),
                "scan_duration_seconds": elapsed,
                "scan_timestamp": datetime.now().isoformat(),
                "timeframe": timeframe,
                "history_periods": history,
                "min_body_move_pct": min_body_move_pct,
                "data_source": "SQLite Database",
                "avg_time_per_symbol": elapsed / len(symbols) if symbols else 0
            },
            "failed_symbols": failed_symbols,
            "skipped_symbols": skipped_no_data
        }

        logger.info(f"SQLite scan complete: {len(results)} patterns in {elapsed:.2f}s")
        logger.info(f"Speed: {len(symbols)/elapsed:.1f} symbols/second")

        return response

    def _detect_marubozu_doji_patterns(self, df: pd.DataFrame, symbol: str,
                                      min_body_move_pct: float) -> List[Dict]:
        """
        EXACT COPY OF PATTERN DETECTION LOGIC FROM ORIGINAL
        NO CHANGES - Preserves all detection rules

        Pattern Rules:
        1. Marubozu: Body% >= 80% AND Body Move% >= min_body_move_pct
        2. Doji: Body% < 25%
        3. Doji high must break Marubozu high
        4. Doji must close inside Marubozu body
        """
        patterns = []

        # Need at least 2 candles
        if len(df) < 2:
            return patterns

        # Iterate through candles
        for i in range(len(df) - 1):
            candle1 = df.iloc[i]  # Potential Marubozu
            candle2 = df.iloc[i + 1]  # Potential Doji

            # ============================================
            # MARUBOZU DETECTION - EXACT SAME LOGIC
            # ============================================
            c1_body = abs(candle1['close'] - candle1['open'])
            c1_range = candle1['high'] - candle1['low']

            if c1_range == 0:  # Skip if no range
                continue

            c1_body_pct = (c1_body / c1_range) * 100

            # Check if it's a Marubozu (body >= 80% of range)
            if c1_body_pct < 80:
                continue

            # Calculate body move percentage (user filter)
            c1_body_move_pct = (c1_body / candle1['open']) * 100

            # Apply user's minimum body move filter
            if c1_body_move_pct < min_body_move_pct:
                continue

            # Determine Marubozu direction
            is_bullish_marubozu = candle1['close'] > candle1['open']

            # ============================================
            # DOJI DETECTION - EXACT SAME LOGIC
            # ============================================
            c2_body = abs(candle2['close'] - candle2['open'])
            c2_range = candle2['high'] - candle2['low']

            if c2_range == 0:  # Skip if no range
                continue

            c2_body_pct = (c2_body / c2_range) * 100

            # Check if it's a Doji (body < 25% of range)
            if c2_body_pct >= 25:
                continue

            # ============================================
            # BREAKOUT & REJECTION - EXACT SAME LOGIC
            # ============================================
            # Check if Doji high breaks Marubozu high
            if candle2['high'] <= candle1['high']:
                continue

            # Check if Doji closes inside Marubozu body
            if is_bullish_marubozu:
                # Bullish Marubozu: open < close
                closes_inside = (candle1['open'] < candle2['close'] < candle1['close'])
            else:
                # Bearish Marubozu: close < open
                closes_inside = (candle1['close'] < candle2['close'] < candle1['open'])

            if not closes_inside:
                continue

            # ============================================
            # PATTERN FOUND - SAME OUTPUT FORMAT
            # ============================================
            pattern = {
                'pattern_type': 'marubozu_doji',
                'pattern_direction': 'bullish' if is_bullish_marubozu else 'bearish',
                'marubozu': {
                    'date': candle1['timestamp'].strftime('%Y-%m-%d'),
                    'open': float(candle1['open']),
                    'high': float(candle1['high']),
                    'low': float(candle1['low']),
                    'close': float(candle1['close']),
                    'volume': int(candle1.get('volume', 0)),
                    'body_pct': round(c1_body_pct, 2),
                    'body_move_pct': round(c1_body_move_pct, 2)
                },
                'doji': {
                    'date': candle2['timestamp'].strftime('%Y-%m-%d'),
                    'open': float(candle2['open']),
                    'high': float(candle2['high']),
                    'low': float(candle2['low']),
                    'close': float(candle2['close']),
                    'volume': int(candle2.get('volume', 0)),
                    'body_pct': round(c2_body_pct, 2)
                },
                'scan_timestamp': datetime.now().isoformat()
            }

            patterns.append(pattern)

        return patterns

    def _aggregate_to_weekly(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        EXACT COPY from dhan_client.py - DO NOT MODIFY
        Aggregate daily data to weekly using TradingView-style rules
        """
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date').sort_index()

        # ISO week aggregation
        df['year'] = df.index.isocalendar().year
        df['week'] = df.index.isocalendar().week
        grouped = df.groupby(['year', 'week'])

        rows = []
        for (y, w), g in grouped:
            g = g.sort_index()
            row = {
                'date': g.index[-1],  # Last trading day of week
                'open': g.iloc[0]['open'],
                'high': g['high'].max(),
                'low': g['low'].min(),
                'close': g.iloc[-1]['close'],
                'volume': g['volume'].sum() if 'volume' in g.columns else 0
            }
            rows.append(row)

        return pd.DataFrame(rows).sort_values('date').reset_index(drop=True)

    def _aggregate_to_monthly(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        EXACT COPY from dhan_client.py - DO NOT MODIFY
        Aggregate daily data to monthly using TradingView-style rules
        """
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date').sort_index()

        # Calendar month aggregation
        df['year'] = df.index.year
        df['month'] = df.index.month
        grouped = df.groupby(['year', 'month'])

        rows = []
        for (y, m), g in grouped:
            g = g.sort_index()
            row = {
                'date': g.index[-1],  # Last trading day of month
                'open': g.iloc[0]['open'],
                'high': g['high'].max(),
                'low': g['low'].min(),
                'close': g.iloc[-1]['close'],
                'volume': g['volume'].sum() if 'volume' in g.columns else 0
            }
            rows.append(row)

        return pd.DataFrame(rows).sort_values('date').reset_index(drop=True)

    def _store_pattern(self, pattern: Dict):
        """Store detected pattern in database"""
        try:
            db_pattern = {
                'symbol': pattern['symbol'],
                'pattern_type': 'Marubozu-Doji',
                'pattern_date': pattern['doji']['date'],
                'pattern_direction': pattern['pattern_direction'],
                'timeframe': pattern.get('timeframe', '1D'),
                'confidence_score': 100.0,
                'pattern_data': pattern,
                'breakout_level': pattern['marubozu']['high'],
                'stop_loss_level': pattern['marubozu']['low']
            }
            self.db.save_pattern(db_pattern)
        except Exception as e:
            logger.error(f"Error storing pattern: {e}")

    def get_available_symbols(self) -> List[str]:
        """Get list of symbols with data in database"""
        symbols = self.db.get_active_symbols(fno_only=True)
        return [s['symbol'] for s in symbols if s.get('symbol')]

    def check_data_completeness(self) -> Dict:
        """Check how complete the database is"""
        stats = self.db.get_database_stats()
        freshness = self.db.check_data_freshness()

        return {
            "total_symbols": stats.get('total_symbols', 0),
            "symbols_with_data": freshness.get('total_symbols', 0),
            "total_records": stats.get('total_daily_records', 0),
            "database_size": stats.get('database_size', 'N/A'),
            "data_current": freshness.get('current_symbols', 0),
            "data_stale": freshness.get('stale_symbols', 0),
            "oldest_data": freshness.get('oldest_data'),
            "newest_data": freshness.get('newest_data')
        }


# Quick test
if __name__ == "__main__":
    scanner = SQLiteScannerEngine()

    # Check data completeness
    completeness = scanner.check_data_completeness()
    print(f"Database Status:")
    print(f"  Symbols with data: {completeness['symbols_with_data']}/{completeness['total_symbols']}")
    print(f"  Total records: {completeness['total_records']}")
    print(f"  Database size: {completeness['database_size']}")

    # Get available symbols
    symbols = scanner.get_available_symbols()
    print(f"\nAvailable symbols: {len(symbols)}")

    if symbols:
        # Test scan with first 5 symbols
        test_symbols = symbols[:5]
        print(f"\nTesting scan with: {test_symbols}")

        results = scanner.scan(
            symbols=test_symbols,
            timeframe="1D",
            history=30,
            min_body_move_pct=4.0
        )

        print(f"\nScan Results:")
        print(f"  Duration: {results['statistics']['scan_duration_seconds']:.2f} seconds")
        print(f"  Patterns found: {results['statistics']['patterns_found']}")
        print(f"  Speed: {len(test_symbols)/results['statistics']['scan_duration_seconds']:.1f} symbols/sec")