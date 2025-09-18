"""
DHAN API Client for fetching NSE equity data
Production-ready, no caching, uses batching + parallel fetching.
"""

import requests
import pandas as pd
from dhanhq import dhanhq
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import time
import logging
import concurrent.futures
from functools import wraps

from .config import config  # uses your injected credentials

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ===================== DECORATORS =====================

def rate_limit(calls_per_second: int = 3):
    """Simple rate limiting decorator"""
    min_interval = 1.0 / calls_per_second
    last_called = [0.0]

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            elapsed = time.time() - last_called[0]
            wait = min_interval - elapsed
            if wait > 0:
                time.sleep(wait)
            last_called[0] = time.time()
            return func(*args, **kwargs)
        return wrapper
    return decorator


def retry_on_failure(retries: int = 3, delay: float = 1.0):
    """Retry decorator with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt < retries:
                        wait = delay * attempt
                        logger.warning(f"Retry {attempt}/{retries} for {func.__name__} due to {e}, waiting {wait}s")
                        time.sleep(wait)
                    else:
                        logger.error(f"Max retries reached for {func.__name__}: {e}")
                        raise
        return wrapper
    return decorator

# ===================== MAIN CLIENT =====================

class DhanClient:
    """DHAN API client with batching & concurrency"""
    def __init__(self):
        self.client_id = config.dhan.client_id
        self.access_token = config.dhan.access_token
        self.dhan = dhanhq(self.client_id, self.access_token)
        self._equity_mapping = None
        self._fno_instruments = None
        self._test_connection()

    def _test_connection(self):
        """Test DHAN API connection"""
        try:
            self.dhan.get_positions()
            logger.info("✅ DHAN API connection successful")
        except Exception as e:
            logger.error(f"❌ DHAN API connection failed: {e}")
            raise

    # ------------------- Instruments -------------------

    @rate_limit()
    @retry_on_failure()
    def load_fno_instruments(self) -> pd.DataFrame:
        """Load F&O instruments from DHAN CSV"""
        if self._fno_instruments is not None:
            return self._fno_instruments
        logger.info("Loading F&O instruments from DHAN CSV...")
        url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        try:
            df = pd.read_csv(url, dtype=str, low_memory=False)
            self._fno_instruments = df
            logger.info(f"Loaded {len(df)} instruments")
            return df
        except Exception as e:
            logger.error(f"Failed to load F&O instruments: {e}")
            raise

    @rate_limit()
    @retry_on_failure()
    def load_equity_instruments(self) -> Dict[str, str]:
        """Create symbol -> securityId mapping for equities"""
        if self._equity_mapping is not None:
            return self._equity_mapping
        logger.info("Loading equity instruments via DHAN API...")
        try:
            # Try fetching from CSV first (more reliable)
            url = "https://images.dhan.co/api-data/api-scrip-master.csv"
            df = pd.read_csv(url, dtype=str, low_memory=False)

            # Filter for NSE equities
            equity_df = df[
                (df['SEM_EXM_EXCH_ID'] == 'NSE') &  # NSE exchange
                (df['SEM_INSTRUMENT_NAME'] == 'EQUITY')
            ]

            mapping = {}
            for _, row in equity_df.iterrows():
                symbol = row.get('SEM_TRADING_SYMBOL')
                sec_id = row.get('SEM_SMST_SECURITY_ID')
                if symbol and sec_id:
                    mapping[str(symbol)] = str(sec_id)

            logger.info(f"Loaded {len(mapping)} equity instruments")
            self._equity_mapping = mapping
            return mapping
        except Exception as e:
            logger.error(f"Failed to load equity instruments: {e}")
            raise

    # ------------------- Historical Data -------------------

    @rate_limit()
    @retry_on_failure()
    def get_historical_data(self, security_id: str, days_back: int = 30, timeframe: str = "1D") -> pd.DataFrame:
        """Fetch daily historical data using DHAN SDK (adds +1 day to timestamps)

        Note: DHAN API may not have today's data immediately after market close.
        We use tomorrow as to_date to ensure we get the latest available EOD data.
        """
        # Use tomorrow as to_date to get today's data if available
        to_date = datetime.now() + timedelta(days=1)
        from_date = to_date - timedelta(days=days_back + 1)
        try:
            response = self.dhan.historical_daily_data(
                security_id=str(security_id),
                exchange_segment="NSE_EQ",
                instrument_type="EQUITY",
                from_date=from_date.strftime("%Y-%m-%d"),
                to_date=to_date.strftime("%Y-%m-%d"),
                expiry_code=0
            )
            if response.get("status") != "success":
                return pd.DataFrame()

            data = response.get("data", {})
            timestamps = pd.to_datetime(data.get("timestamp", []), unit='s', utc=True).tz_localize(None)
            # Add 1 day for correct trading date alignment
            timestamps = timestamps + pd.Timedelta(days=1)

            df = pd.DataFrame({
                "timestamp": timestamps,
                "open": data.get("open", []),
                "high": data.get("high", []),
                "low": data.get("low", []),
                "close": data.get("close", []),
                "volume": data.get("volume", [])
            })
            return df.sort_values("timestamp").reset_index(drop=True)
        except Exception as e:
            logger.error(f"Error fetching historical data for {security_id}: {e}")
            return pd.DataFrame()

    def get_batch_historical_data(self, symbols: List[str], days_back: int = 30, timeframe: str = "1D") -> Dict[str, pd.DataFrame]:
        """Fetch historical data for multiple symbols in parallel batches"""
        mapping = self.load_equity_instruments()
        valid = [(s, mapping.get(s)) for s in symbols if mapping.get(s)]
        results = {}

        batch_size = 10  # Reduced from 20 to ease API pressure
        total_batches = (len(valid) + batch_size - 1) // batch_size
        start = time.time()
        failed_symbols = []  # Track failed symbols for individual retry

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:  # Reduced from 5 to 3
            for i in range(total_batches):
                start_idx, end_idx = i * batch_size, min((i+1) * batch_size, len(valid))
                batch = valid[start_idx:end_idx]
                logger.info(f"Batch {i+1}/{total_batches}: fetching {len(batch)} symbols...")

                future_map = {
                    executor.submit(self.get_historical_data, sec_id, days_back, timeframe): (sym, sec_id)
                    for sym, sec_id in batch
                }
                for fut in concurrent.futures.as_completed(future_map):
                    sym, sec_id = future_map[fut]
                    try:
                        df = fut.result()
                        if len(df) > 0:
                            results[sym] = df
                            logger.info(f"✅ {sym}: {len(df)} candles fetched")
                        else:
                            logger.warning(f"❌ {sym}: no data")
                            failed_symbols.append((sym, sec_id))
                    except Exception as e:
                        logger.error(f"Error for {sym}: {e}")
                        failed_symbols.append((sym, sec_id))

                # Add delay between batches to reduce API pressure
                if i < total_batches - 1:  # Don't delay after the last batch
                    logger.info(f"Waiting 2 seconds before next batch...")
                    time.sleep(2)

        # Individual retry for failed symbols with longer delays
        if failed_symbols:
            logger.info(f"Retrying {len(failed_symbols)} failed symbols individually...")
            for sym, sec_id in failed_symbols:
                try:
                    time.sleep(1)  # 1 second delay between individual retries
                    df = self.get_historical_data(sec_id, days_back, timeframe)
                    if len(df) > 0:
                        results[sym] = df
                        logger.info(f"✅ {sym}: {len(df)} candles fetched on retry")
                    else:
                        logger.warning(f"❌ {sym}: no data on retry")
                except Exception as e:
                    logger.error(f"Retry failed for {sym}: {e}")

        logger.info(f"Fetched data for {len(results)}/{len(valid)} symbols in {time.time()-start:.2f}s")
        return results

    # ------------------- Aggregation Methods -------------------

    def _aggregate_to_weekly(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate daily data to weekly using TradingView-style rules"""
        df = df.copy()
        if 'timestamp' in df.columns:
            df['date'] = pd.to_datetime(df['timestamp'])
        elif 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])

        df = df.set_index('date').sort_index()

        # ISO week aggregation (year-week) for calendar consistency
        df['year'] = df.index.isocalendar().year
        df['week'] = df.index.isocalendar().week
        grouped = df.groupby(['year', 'week'])

        rows = []
        for (y, w), g in grouped:
            g = g.sort_index()
            row = {
                'date': g.index[-1],  # Last trading day of week
                'open': g.iloc[0]['open'],  # First day's open
                'high': g['high'].max(),    # Highest high
                'low': g['low'].min(),      # Lowest low
                'close': g.iloc[-1]['close'], # Last day's close
                'volume': g['volume'].sum(),  # Total volume
                'days': len(g)               # Number of trading days
            }
            rows.append(row)

        return pd.DataFrame(rows).sort_values('date').reset_index(drop=True)

    def _aggregate_to_monthly(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate daily data to monthly using TradingView-style rules"""
        df = df.copy()
        if 'timestamp' in df.columns:
            df['date'] = pd.to_datetime(df['timestamp'])
        elif 'date' in df.columns:
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
                'open': g.iloc[0]['open'],  # First day's open
                'high': g['high'].max(),    # Highest high
                'low': g['low'].min(),      # Lowest low
                'close': g.iloc[-1]['close'], # Last day's close
                'volume': g['volume'].sum(),  # Total volume
                'days': len(g)               # Number of trading days
            }
            rows.append(row)

        return pd.DataFrame(rows).sort_values('date').reset_index(drop=True)