"""
Main scanner engine that orchestrates data fetching, aggregation, and pattern detection
"""

import pandas as pd
import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta

from .dhan_client import DhanClient
from .aggregator import TimeframeAggregator, check_consecutive_periods
from .pattern_detector import PatternDetector, Candle

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ScannerEngine:
    """
    Main engine for scanning patterns across multiple symbols and timeframes
    """

    def __init__(self,
                 dhan_client: Optional[DhanClient] = None,
                 marubozu_threshold: float = 0.8,
                 doji_threshold: float = 0.20):
        """
        Initialize scanner engine

        Args:
            dhan_client: DHAN API client instance
            marubozu_threshold: Threshold for Marubozu detection (0.8 = 80%)
            doji_threshold: Threshold for Doji detection (0.20 = 20%, made more strict)
        """
        self.client = dhan_client or DhanClient()
        self.detector = PatternDetector(marubozu_threshold, doji_threshold)
        self.aggregator = TimeframeAggregator()

    def _calculate_days_back(self, timeframe: str, history: int) -> int:
        """
        Calculate how many days of data to fetch based on timeframe and history

        Args:
            timeframe: '1D', '1W', or '1M'
            history: Number of periods requested

        Returns:
            Number of days to fetch
        """
        if timeframe == '1D':
            # For daily, fetch exact number of days plus buffer
            return history + 10

        elif timeframe == '1W':
            # For weekly, assume 5 trading days per week plus buffer
            return (history * 7) + 30

        elif timeframe == '1M':
            # For monthly, assume 22 trading days per month plus buffer
            return (history * 30) + 60

        else:
            return history

    def _to_candle_list(self, df: pd.DataFrame) -> List[Candle]:
        """
        Convert DataFrame to list of Candle objects

        Args:
            df: DataFrame with OHLC data

        Returns:
            List of Candle objects
        """
        candles = []
        for _, row in df.iterrows():
            candle = Candle(
                date=pd.Timestamp(row['date']).date() if pd.notna(row['date']) else None,
                open=float(row['open']),
                high=float(row['high']),
                low=float(row['low']),
                close=float(row['close']),
                volume=float(row.get('volume', 0))
            )
            candles.append(candle)
        return candles

    def scan_symbol(self,
                    symbol: str,
                    df: pd.DataFrame,
                    timeframe: str,
                    min_body_move_pct: float,
                    history: int) -> List[Dict]:
        """
        Scan a single symbol for patterns

        Args:
            symbol: Symbol name
            df: Daily OHLC DataFrame
            timeframe: '1D', '1W', or '1M'
            min_body_move_pct: Minimum body move % filter
            history: Number of periods used

        Returns:
            List of pattern matches
        """
        results = []

        try:
            # Aggregate data if needed
            df_agg = self.aggregator.aggregate(df, timeframe)
            if df_agg.empty or len(df_agg) < 2:
                logger.debug(f"{symbol}: Insufficient data after aggregation")
                return results

            # Convert to candles
            candles = self._to_candle_list(df_agg)

            # Find consecutive periods
            if timeframe in ['1W', '1M']:
                consecutive_pairs = check_consecutive_periods(df_agg, timeframe)
            else:
                # For daily, all adjacent pairs are consecutive
                consecutive_pairs = [(i, i+1) for i in range(len(candles)-1)]

            # Check each consecutive pair for pattern
            for idx1, idx2 in consecutive_pairs:
                c1 = candles[idx1]
                c2 = candles[idx2]

                # Check pattern
                matched, details = self.detector.matches_marubozu_doji(c1, c2)
                if not matched:
                    continue

                # Apply minimum body move filter
                if not self.detector.filter_by_min_body_move(c1, min_body_move_pct):
                    logger.debug(f"{symbol}: Marubozu body move {c1.body_move_pct:.2f}% < {min_body_move_pct}%")
                    continue

                # Pattern found and passes filters!
                # Format result based on timeframe to match database structure
                if timeframe in ['1W', '1M']:
                    # For weekly/monthly patterns, we need to provide period spans like database
                    # Get the original DataFrame to find period start dates
                    c1_row = df_agg.iloc[idx1]
                    c2_row = df_agg.iloc[idx2]

                    if timeframe == '1W':
                        # For weekly, calculate the week start for each candle
                        c1_date = pd.to_datetime(c1.date)
                        c2_date = pd.to_datetime(c2.date)

                        # Calculate Monday of each week (ISO week start)
                        c1_monday = c1_date - pd.Timedelta(days=c1_date.weekday())
                        c2_monday = c2_date - pd.Timedelta(days=c2_date.weekday())

                        marubozu_period_start = c1_monday.strftime('%Y-%m-%d')
                        marubozu_period_end = str(c1.date)
                        doji_period_start = c2_monday.strftime('%Y-%m-%d')
                        doji_period_end = str(c2.date)

                        result = {
                            'symbol': symbol,
                            'timeframe': timeframe,
                            'pattern_direction': details.get('direction'),
                            'marubozu': {
                                'period_start': marubozu_period_start,
                                'period_end': marubozu_period_end,
                                'open': c1.open,
                                'high': c1.high,
                                'low': c1.low,
                                'close': c1.close,
                                'volume': c1.volume,
                                'body_pct': round(details.get('marubozu_body_pct'), 2),
                                'body_move_pct': round(details.get('marubozu_body_move_pct'), 2)
                            },
                            'doji': {
                                'period_start': doji_period_start,
                                'period_end': doji_period_end,
                                'open': c2.open,
                                'high': c2.high,
                                'low': c2.low,
                                'close': c2.close,
                                'volume': c2.volume,
                                'body_pct': round(details.get('doji_body_pct'), 2)
                            },
                        }
                    elif timeframe == '1M':
                        # For monthly, calculate month start for each candle
                        c1_date = pd.to_datetime(c1.date)
                        c2_date = pd.to_datetime(c2.date)

                        # First day of each month
                        c1_month_start = c1_date.replace(day=1)
                        c2_month_start = c2_date.replace(day=1)

                        marubozu_period_start = c1_month_start.strftime('%Y-%m-%d')
                        marubozu_period_end = str(c1.date)
                        doji_period_start = c2_month_start.strftime('%Y-%m-%d')
                        doji_period_end = str(c2.date)

                        result = {
                            'symbol': symbol,
                            'timeframe': timeframe,
                            'pattern_direction': details.get('direction'),
                            'marubozu': {
                                'period_start': marubozu_period_start,
                                'period_end': marubozu_period_end,
                                'open': c1.open,
                                'high': c1.high,
                                'low': c1.low,
                                'close': c1.close,
                                'volume': c1.volume,
                                'body_pct': round(details.get('marubozu_body_pct'), 2),
                                'body_move_pct': round(details.get('marubozu_body_move_pct'), 2)
                            },
                            'doji': {
                                'period_start': doji_period_start,
                                'period_end': doji_period_end,
                                'open': c2.open,
                                'high': c2.high,
                                'low': c2.low,
                                'close': c2.close,
                                'volume': c2.volume,
                                'body_pct': round(details.get('doji_body_pct'), 2)
                            },
                        }
                else:
                    # For daily patterns, keep existing format
                    result = {
                        'symbol': symbol,
                        'timeframe': timeframe,
                        'pattern_direction': details.get('direction'),
                        'marubozu': {
                            'date': str(c1.date),
                            'open': c1.open,
                            'high': c1.high,
                            'low': c1.low,
                            'close': c1.close,
                            'volume': c1.volume,
                            'body_pct_of_range': details.get('marubozu_body_pct'),
                            'body_move_pct': details.get('marubozu_body_move_pct')
                        },
                        'doji': {
                            'date': str(c2.date),
                            'open': c2.open,
                            'high': c2.high,
                            'low': c2.low,
                            'close': c2.close,
                            'volume': c2.volume,
                            'body_pct_of_range': details.get('doji_body_pct')
                        },
                    }

                # Add common properties for all timeframes
                result.update({
                    'notes': 'Doji high broke Marubozu high but closed inside Marubozu body',
                    'breakout_amount': details.get('breakout_amount'),
                    'rejection_strength': details.get('rejection_strength'),
                    'source': 'DHANHQ_v2',
                    'data_range_used': f'last {history} {timeframe}',
                    'scan_timestamp': datetime.now().isoformat()
                })

                results.append(result)
                logger.info(f"✅ Pattern found: {symbol} {timeframe} {details.get('direction')} "
                           f"on {c1.date} → {c2.date}")

        except Exception as e:
            logger.error(f"Error scanning {symbol}: {e}")

        return results

    def scan(self,
             symbols: List[str],
             timeframe: str = '1D',
             history: int = 20,
             min_body_move_pct: float = 4.0) -> Dict:
        """
        Scan multiple symbols for patterns

        Args:
            symbols: List of symbols to scan
            timeframe: '1D', '1W', or '1M'
            history: Number of periods to check
            min_body_move_pct: Minimum body move % filter

        Returns:
            Dictionary with results and statistics
        """
        logger.info(f"Starting scan: {len(symbols)} symbols, timeframe={timeframe}, "
                   f"history={history}, min_body_move={min_body_move_pct}%")

        start_time = datetime.now()

        # Calculate how many days of data to fetch
        days_back = self._calculate_days_back(timeframe, history)
        logger.info(f"Fetching {days_back} days of daily data for aggregation")

        # Fetch data for all symbols in batches
        all_data = self.client.get_batch_historical_data(symbols, days_back, timeframe='1D')

        # Scan each symbol
        all_results = []
        symbols_scanned = 0
        symbols_with_data = 0
        symbols_with_patterns = 0

        for symbol, df in all_data.items():
            symbols_scanned += 1

            if df is None or df.empty:
                logger.warning(f"{symbol}: No data available")
                continue

            symbols_with_data += 1

            # Scan for patterns
            results = self.scan_symbol(symbol, df, timeframe, min_body_move_pct, history)

            if results:
                symbols_with_patterns += 1
                all_results.extend(results)

        # Calculate statistics
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        statistics = {
            'total_symbols_requested': len(symbols),
            'symbols_scanned': symbols_scanned,
            'symbols_with_data': symbols_with_data,
            'symbols_with_patterns': symbols_with_patterns,
            'total_patterns_found': len(all_results),
            'scan_duration_seconds': round(duration, 2),
            'scan_timestamp': end_time.isoformat(),
            'parameters': {
                'timeframe': timeframe,
                'history': history,
                'min_body_move_pct': min_body_move_pct,
                'marubozu_threshold': self.detector.marubozu_threshold,
                'doji_threshold': self.detector.doji_threshold
            }
        }

        logger.info(f"Scan complete: {len(all_results)} patterns found in {duration:.2f}s")
        logger.info(f"Statistics: {symbols_with_patterns}/{symbols_with_data} symbols with patterns")

        return {
            'results': all_results,
            'statistics': statistics
        }

    def scan_single(self,
                    symbol: str,
                    timeframe: str = '1D',
                    history: int = 20,
                    min_body_move_pct: float = 4.0) -> Dict:
        """
        Convenience method to scan a single symbol

        Args:
            symbol: Symbol to scan
            timeframe: '1D', '1W', or '1M'
            history: Number of periods to check
            min_body_move_pct: Minimum body move % filter

        Returns:
            Dictionary with results and statistics
        """
        return self.scan([symbol], timeframe, history, min_body_move_pct)