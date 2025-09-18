"""
Generate weekly and monthly aggregations from daily OHLC data.
"""
import sqlite3
import pandas as pd
from datetime import datetime
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_aggregations():
    """Generate weekly and monthly aggregations from daily data."""
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
                'date': 'last'  # Use last trading day of week as date
            }).reset_index(drop=True)

            # Add weekly records
            for idx, row in weekly.iterrows():
                # Get the week's start and end dates
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
                'date': 'last'  # Use last trading day of month as date
            }).reset_index(drop=True)

            # Add monthly records
            for idx, row in monthly.iterrows():
                # Get the month's start and end dates
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

def detect_patterns_on_aggregated():
    """Detect Marubozu->Doji patterns on aggregated data."""
    conn = sqlite3.connect('pattern_scanner.db')

    try:
        # Get pattern type ID for Marubozu->Doji
        cursor = conn.cursor()
        cursor.execute("SELECT pattern_type_id FROM pattern_types WHERE pattern_name = 'MARUBOZU_DOJI'")
        result = cursor.fetchone()

        if not result:
            # Create pattern type if it doesn't exist
            cursor.execute("""
                INSERT INTO pattern_types (pattern_name, pattern_category, description, candles_required)
                VALUES ('MARUBOZU_DOJI', 'REVERSAL', 'Marubozu followed by Doji - failed breakout pattern', 2)
            """)
            pattern_type_id = cursor.lastrowid
        else:
            pattern_type_id = result[0]

        # Clear existing weekly/monthly patterns
        cursor.execute("DELETE FROM detected_patterns WHERE timeframe IN ('WEEKLY', 'MONTHLY')")
        conn.commit()

        detected_patterns = []

        for timeframe in ['WEEKLY', 'MONTHLY']:
            logger.info(f"Detecting patterns for {timeframe} timeframe...")

            # Get aggregated data
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

            # Group by symbol
            for symbol_id in df['symbol_id'].unique():
                symbol_data = df[df['symbol_id'] == symbol_id].sort_values('period_end').reset_index(drop=True)
                symbol_name = symbol_data.iloc[0]['symbol']

                # Need at least 2 candles
                if len(symbol_data) < 2:
                    continue

                # Check last two candles
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
                                # Check if doji closed inside marubozu body
                                is_bullish = candle1['close'] > candle1['open']

                                if is_bullish:
                                    closed_inside = candle1['open'] < candle2['close'] < candle1['close']
                                else:
                                    closed_inside = candle1['close'] < candle2['close'] < candle1['open']

                                if closed_inside:
                                    # Pattern found!
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
                                            'body_pct': float(body1_pct),
                                            'body_move_pct': float(body1_move_pct)
                                        },
                                        'doji': {
                                            'period_start': candle2['period_start'],
                                            'period_end': candle2['period_end'],
                                            'open': float(candle2['open']),
                                            'high': float(candle2['high']),
                                            'low': float(candle2['low']),
                                            'close': float(candle2['close']),
                                            'body_pct': float(body2_pct)
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

                                    logger.info(f"Found pattern: {symbol_name} ({timeframe}) - {pattern_data['pattern_direction']}")

        # Insert detected patterns
        if detected_patterns:
            logger.info(f"Inserting {len(detected_patterns)} patterns...")
            cursor.executemany("""
                INSERT INTO detected_patterns (symbol_id, pattern_type_id, pattern_date, timeframe, pattern_direction, pattern_data, confidence_score)
                VALUES (:symbol_id, :pattern_type_id, :pattern_date, :timeframe, :pattern_direction, :pattern_data, :confidence_score)
            """, detected_patterns)
            conn.commit()

            # Get final counts
            cursor.execute("""
                SELECT timeframe, COUNT(*) as count
                FROM detected_patterns
                WHERE timeframe IN ('WEEKLY', 'MONTHLY')
                GROUP BY timeframe
            """)
            counts = cursor.fetchall()
            for timeframe, count in counts:
                logger.info(f"Detected {count} {timeframe} patterns")
        else:
            logger.info("No weekly/monthly patterns detected")

    except Exception as e:
        logger.error(f"Error detecting patterns: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    logger.info("Starting aggregation generation...")
    generate_aggregations()

    logger.info("\nDetecting patterns on aggregated data...")
    detect_patterns_on_aggregated()

    logger.info("\nProcess completed!")