"""
Timeframe aggregation utilities for converting daily data to weekly/monthly
Following TradingView-style aggregation rules
"""

import pandas as pd
from datetime import datetime
from typing import Optional

def validate_ohlc(row: dict) -> bool:
    """
    Validate OHLC data sanity
    Ensures: low <= open/close <= high
    """
    if row['low'] > row['high']:
        return False
    if not (row['low'] <= row['open'] <= row['high']):
        return False
    if not (row['low'] <= row['close'] <= row['high']):
        return False
    return True

def aggregate_to_weekly(df: pd.DataFrame, min_days: int = 1) -> pd.DataFrame:
    """
    Aggregate daily data to weekly using TradingView rules

    Args:
        df: DataFrame with daily OHLC data
        min_days: Minimum trading days required in a week

    Returns:
        Weekly aggregated DataFrame
    """
    df = df.copy()

    # Ensure we have a date column
    if 'timestamp' in df.columns:
        df['date'] = pd.to_datetime(df['timestamp'])
    elif 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    else:
        raise ValueError("DataFrame must have 'date' or 'timestamp' column")

    df = df.set_index('date').sort_index()

    # ISO week aggregation for calendar consistency
    df['year'] = df.index.isocalendar().year
    df['week'] = df.index.isocalendar().week
    grouped = df.groupby(['year', 'week'])

    rows = []
    for (y, w), g in grouped:
        # Skip weeks with insufficient trading days
        if len(g) < min_days:
            continue

        g = g.sort_index()
        row = {
            'date': g.index[-1],  # Last trading day of week
            'open': float(g.iloc[0]['open']),  # First day's open
            'high': float(g['high'].max()),    # Highest high
            'low': float(g['low'].min()),      # Lowest low
            'close': float(g.iloc[-1]['close']), # Last day's close
            'volume': float(g['volume'].sum()) if 'volume' in g.columns else 0,
            'days': len(g),  # Number of trading days
            'year': y,
            'week': w
        }

        if validate_ohlc(row):
            rows.append(row)

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows).sort_values('date').reset_index(drop=True)
    # Keep only essential columns
    return result[['date', 'open', 'high', 'low', 'close', 'volume', 'days']]

def aggregate_to_monthly(df: pd.DataFrame, min_days: int = 1) -> pd.DataFrame:
    """
    Aggregate daily data to monthly using TradingView rules

    Args:
        df: DataFrame with daily OHLC data
        min_days: Minimum trading days required in a month

    Returns:
        Monthly aggregated DataFrame
    """
    df = df.copy()

    # Ensure we have a date column
    if 'timestamp' in df.columns:
        df['date'] = pd.to_datetime(df['timestamp'])
    elif 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    else:
        raise ValueError("DataFrame must have 'date' or 'timestamp' column")

    df = df.set_index('date').sort_index()

    # Calendar month aggregation
    df['year'] = df.index.year
    df['month'] = df.index.month
    grouped = df.groupby(['year', 'month'])

    rows = []
    for (y, m), g in grouped:
        # Skip months with insufficient trading days
        if len(g) < min_days:
            continue

        g = g.sort_index()
        row = {
            'date': g.index[-1],  # Last trading day of month
            'open': float(g.iloc[0]['open']),  # First day's open
            'high': float(g['high'].max()),    # Highest high
            'low': float(g['low'].min()),      # Lowest low
            'close': float(g.iloc[-1]['close']), # Last day's close
            'volume': float(g['volume'].sum()) if 'volume' in g.columns else 0,
            'days': len(g),  # Number of trading days
            'year': y,
            'month': m
        }

        if validate_ohlc(row):
            rows.append(row)

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows).sort_values('date').reset_index(drop=True)
    # Keep only essential columns
    return result[['date', 'open', 'high', 'low', 'close', 'volume', 'days']]

def check_consecutive_periods(df: pd.DataFrame, timeframe: str) -> list:
    """
    Find consecutive period pairs in the DataFrame

    Args:
        df: Aggregated DataFrame with date column
        timeframe: '1W' for weekly, '1M' for monthly

    Returns:
        List of tuples (index1, index2) for consecutive periods
    """
    consecutive_pairs = []

    if len(df) < 2:
        return consecutive_pairs

    for i in range(len(df) - 1):
        current_date = pd.to_datetime(df.iloc[i]['date'])
        next_date = pd.to_datetime(df.iloc[i + 1]['date'])

        if timeframe == '1W':
            # Check if weeks are consecutive (ISO week)
            current_week = current_date.isocalendar()
            next_week = next_date.isocalendar()

            # Handle year boundary
            if (current_week.year == next_week.year and
                next_week.week == current_week.week + 1):
                consecutive_pairs.append((i, i + 1))
            elif (current_week.year == next_week.year - 1 and
                  current_week.week >= 52 and next_week.week == 1):
                consecutive_pairs.append((i, i + 1))

        elif timeframe == '1M':
            # Check if months are consecutive
            if (current_date.year == next_date.year and
                next_date.month == current_date.month + 1):
                consecutive_pairs.append((i, i + 1))
            elif (current_date.year == next_date.year - 1 and
                  current_date.month == 12 and next_date.month == 1):
                consecutive_pairs.append((i, i + 1))

        else:  # Daily - all pairs are consecutive
            consecutive_pairs.append((i, i + 1))

    return consecutive_pairs

class TimeframeAggregator:
    """
    Utility class for aggregating data to different timeframes
    """

    def __init__(self, min_days_weekly: int = 1, min_days_monthly: int = 1):
        self.min_days_weekly = min_days_weekly
        self.min_days_monthly = min_days_monthly

    def aggregate(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """
        Aggregate data to specified timeframe

        Args:
            df: Daily OHLC DataFrame
            timeframe: '1D', '1W', or '1M'

        Returns:
            Aggregated DataFrame
        """
        if timeframe == '1D':
            # No aggregation needed for daily
            result = df.copy()
            if 'timestamp' in result.columns:
                result = result.rename(columns={'timestamp': 'date'})
            return result

        elif timeframe == '1W':
            return aggregate_to_weekly(df, self.min_days_weekly)

        elif timeframe == '1M':
            return aggregate_to_monthly(df, self.min_days_monthly)

        else:
            raise ValueError(f"Unknown timeframe: {timeframe}. Use '1D', '1W', or '1M'")