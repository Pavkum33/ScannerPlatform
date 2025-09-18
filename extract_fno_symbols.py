"""
Extract unique F&O symbols from DHAN master file
"""

import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_fno_symbols():
    """Extract unique F&O equity symbols from DHAN master CSV"""
    try:
        # Read the DHAN master file
        logger.info("Reading DHAN master file...")
        df = pd.read_csv('security_id_list.csv', dtype=str, low_memory=False)

        # Filter for NSE F&O equities
        # Look for NSE exchange (SEM_EXM_EXCH_ID = 1) and F&O instruments
        nse_df = df[df['SEM_EXM_EXCH_ID'] == '1']

        # Filter for FUTSTK and OPTSTK (stock futures and options)
        fno_df = nse_df[nse_df['SEM_INSTRUMENT_NAME'].isin(['FUTSTK', 'OPTSTK', 'FUTIDX', 'OPTIDX'])]

        # Get unique symbols
        unique_symbols = fno_df['SM_SYMBOL_NAME'].dropna().unique()

        # Remove index symbols (NIFTY, BANKNIFTY, etc) - keep only stocks
        stock_symbols = [s for s in unique_symbols if s not in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']]

        logger.info(f"Found {len(stock_symbols)} unique F&O stock symbols")

        # Create DataFrame and save
        symbols_df = pd.DataFrame({'Symbol': sorted(stock_symbols)})
        symbols_df.to_csv('fno_symbols.csv', index=False)
        logger.info(f"Saved {len(stock_symbols)} F&O symbols to fno_symbols.csv")

        # Print first 10 symbols as sample
        print("\nSample F&O symbols:")
        for symbol in stock_symbols[:10]:
            print(f"  - {symbol}")

        return stock_symbols

    except Exception as e:
        logger.error(f"Error extracting F&O symbols: {e}")

        # Fallback to common F&O stocks
        fallback_symbols = [
            'RELIANCE', 'TCS', 'INFY', 'HDFCBANK', 'ICICIBANK', 'SBIN', 'AXISBANK',
            'KOTAKBANK', 'LT', 'ITC', 'HINDUNILVR', 'BHARTIARTL', 'WIPRO', 'HCLTECH',
            'BAJFINANCE', 'TATAMOTORS', 'MARUTI', 'TITAN', 'SUNPHARMA', 'ASIANPAINT',
            'ULTRACEMCO', 'NTPC', 'POWERGRID', 'ONGC', 'TATASTEEL', 'JSWSTEEL',
            'ADANIPORTS', 'ADANIENT', 'GRASIM', 'INDUSINDBK', 'DIVISLAB', 'DRREDDY',
            'CIPLA', 'COALINDIA', 'HINDALCO', 'BRITANNIA', 'TECHM', 'M&M', 'NESTLEIND',
            'BAJAJFINSV', 'APOLLOHOSP', 'TATACONSUM', 'BPCL', 'EICHERMOT', 'HEROMOTOCO',
            'PIDILITIND', 'SIEMENS', 'HAVELLS', 'DABUR', 'PAGEIND', 'MUTHOOTFIN',
            'PEL', 'VOLTAS', 'BATAINDIA', 'BERGEPAINT', 'CANBK', 'LICHSGFIN',
            'GODREJCP', 'GODREJPROP', 'SRF', 'MINDTREE', 'SAIL', 'AUROPHARMA',
            'BIOCON', 'COLPAL', 'MCDOWELL-N', 'DLF', 'BALKRISIND', 'ESCORTS',
            'GLENMARK', 'GMRINFRA', 'IDFCFIRSTB', 'NATIONALUM', 'PFC', 'RECLTD',
            'TATAPOWER', 'TORNTPHARM', 'VEDL', 'ZEEL', 'AMBUJACEM', 'ACC',
            'BANDHANBNK', 'BANKBARODA', 'BHARATFORG', 'BOSCHLTD', 'CHOLAFIN',
            'CONCOR', 'CUMMINSIND', 'FEDERALBNK', 'GAIL', 'ICICIPRULI', 'IDEA',
            'INDIGO', 'INDUSTOWER', 'INFRATEL', 'IOC', 'JINDALSTEL', 'JUBLFOOD',
            'L&TFH', 'LUPIN', 'MANAPPURAM', 'MARICO', 'MOTHERSUMI', 'MRF',
            'NAUKRI', 'PNB', 'PETRONET', 'RBLBANK', 'SHREECEM', 'SRTRANSFIN',
            'UBL', 'UPL', 'CADILAHC', 'IPCALAB', 'AUBANK', 'RAMCOCEM', 'TVSMOTOR',
            'BHARATFORGE', 'ASHOKLEY', 'EXIDEIND', 'BHEL', 'MGL', 'OFSS',
            'GRANULES', 'POLYCAB', 'IRCTC', 'IGL', 'DIXON', 'LTTS', 'LAURUSLABS',
            'PIIND', 'DEEPAKNTR', 'COFORGE', 'ALKEM', 'RAIN', 'CHAMBLFERT'
        ]

        # Save fallback symbols
        symbols_df = pd.DataFrame({'Symbol': fallback_symbols[:100]})  # Top 100 F&O stocks
        symbols_df.to_csv('fno_symbols.csv', index=False)
        logger.info(f"Using fallback list: Saved {len(fallback_symbols[:100])} F&O symbols to fno_symbols.csv")

        return fallback_symbols[:100]

if __name__ == '__main__':
    symbols = extract_fno_symbols()
    print(f"\nTotal F&O stocks: {len(symbols)}")