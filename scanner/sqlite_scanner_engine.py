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
        # Always initialize DHAN client for live data fetching
        self.dhan = DhanClient()
        # Pre-load and cache symbol-to-security-ID mappings (constant data)
        logger.info("Loading symbol-to-security-ID mappings...")
        self.symbol_mapping = self.dhan.load_equity_instruments()
        logger.info(f"Cached {len(self.symbol_mapping)} symbol mappings for fast lookup")
        logger.info("SQLite Scanner Engine initialized with live API support")

    def scan(self, symbols: List[str], timeframe: str = "1D",
            history: int = 30, min_body_move_pct: float = 4.0) -> Dict:
        """
        Live scan using DHAN API with multi-timeframe support and retries

        Args:
            symbols: List of symbols to scan
            timeframe: "1D", "1W", or "1M"
            history: Number of periods to look back
            min_body_move_pct: Minimum body movement % for Marubozu

        Returns:
            Dict with results including ALL timeframes when requested
        """
        logger.info(f"Starting LIVE DHAN API scan: {len(symbols)} symbols, timeframe: {timeframe}")
        start_time = datetime.now()

        # Determine which timeframes to scan
        if timeframe == "ALL":
            timeframes_to_scan = ["1D", "1W", "1M"]
        else:
            timeframes_to_scan = [timeframe]

        all_results = []
        total_successful = 0
        total_failed = []
        total_skipped = []

        # Scan each timeframe
        for tf in timeframes_to_scan:
            logger.info(f"Scanning {tf} timeframe...")

            tf_results = []
            successful_symbols = 0
            failed_symbols = []
            skipped_no_data = []

            # Calculate history for this timeframe
            if tf == "1D":
                tf_history = history
            elif tf == "1W":
                tf_history = max(10, history // 7)  # At least 10 weeks
            else:  # 1M
                tf_history = max(6, history // 30)  # At least 6 months

            for symbol in symbols:
                try:
                    # Always fetch Daily data first (DHAN API only provides Daily)
                    daily_df = self._fetch_with_retries(symbol, "1D", max(tf_history * 7, 50))

                    if daily_df is None or daily_df.empty or len(daily_df) < 2:
                        skipped_no_data.append(f"{symbol}({tf})")
                        continue

                    # Aggregate Daily data to target timeframe using DhanClient aggregation methods
                    if tf == "1D":
                        df = daily_df
                    elif tf == "1W":
                        df = self.dhan._aggregate_to_weekly(daily_df)
                    elif tf == "1M":
                        df = self.dhan._aggregate_to_monthly(daily_df)
                    else:
                        df = daily_df

                    if df is None or df.empty or len(df) < 2:
                        skipped_no_data.append(f"{symbol}({tf})")
                        continue

                    # Add timestamp column for pattern detection
                    if 'timestamp' not in df.columns:
                        df['timestamp'] = pd.to_datetime(df['date'])

                    # Detect patterns on aggregated timeframe data
                    patterns = self._detect_marubozu_doji_patterns(
                        df, symbol, min_body_move_pct
                    )

                    if patterns:
                        for pattern in patterns:
                            pattern['symbol'] = symbol
                            pattern['timeframe'] = tf
                            tf_results.append(pattern)

                    successful_symbols += 1

                except Exception as e:
                    logger.error(f"Error scanning {symbol}({tf}): {e}")
                    failed_symbols.append(f"{symbol}({tf})")

            # Add timeframe results to overall results
            all_results.extend(tf_results)
            total_successful += successful_symbols
            total_failed.extend(failed_symbols)
            total_skipped.extend(skipped_no_data)

            logger.info(f"{tf} scan: {len(tf_results)} patterns, {successful_symbols} successful")

        # Calculate statistics
        elapsed = (datetime.now() - start_time).total_seconds()

        response = {
            "results": all_results,
            "statistics": {
                "symbols_scanned": len(symbols),
                "timeframes_scanned": timeframes_to_scan,
                "successful_scans": total_successful,
                "failed_scans": len(total_failed),
                "skipped_no_data": len(total_skipped),
                "patterns_found": len(all_results),
                "scan_duration_seconds": elapsed,
                "scan_timestamp": datetime.now().isoformat(),
                "timeframe": timeframe,
                "history_periods": history,
                "min_body_move_pct": min_body_move_pct,
                "data_source": "LIVE DHAN API",
                "avg_time_per_symbol": elapsed / len(symbols) if symbols else 0
            },
            "failed_symbols": total_failed,
            "skipped_symbols": total_skipped
        }

        logger.info(f"LIVE scan complete: {len(all_results)} patterns in {elapsed:.2f}s")
        logger.info(f"Speed: {len(symbols)/elapsed:.1f} symbols/second")

        return response

    def _fetch_with_retries(self, symbol: str, timeframe: str, history: int, max_retries: int = 5) -> pd.DataFrame:
        """
        Fetch data from DHAN API with retries and linear 2-second backoff
        Uses cached symbol-to-security-ID mapping for fast lookup
        """
        import time

        # Look up security ID from cached mapping (constant data)
        security_id = self.symbol_mapping.get(symbol)
        if not security_id:
            logger.error(f"Security ID not found for symbol: {symbol}")
            return None

        for attempt in range(max_retries):
            try:
                # Use security ID (not symbol name) for DHAN API call
                df = self.dhan.get_historical_data(security_id, history, timeframe)
                if df is not None and not df.empty:
                    return df
                else:
                    logger.warning(f"Empty data for {symbol}({timeframe}) on attempt {attempt + 1}")

            except Exception as e:
                logger.warning(f"API error for {symbol}({timeframe}) attempt {attempt + 1}: {e}")

            # Linear 2-second backoff
            if attempt < max_retries - 1:
                sleep_time = 2
                logger.info(f"Retrying {symbol}({timeframe}) in {sleep_time}s...")
                time.sleep(sleep_time)

        logger.error(f"Failed to fetch {symbol}({timeframe}) after {max_retries} attempts")
        return None

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