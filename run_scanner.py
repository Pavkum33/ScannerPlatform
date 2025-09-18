#!/usr/bin/env python3
"""
CLI entry point for Marubozu → Doji pattern scanner
"""

import argparse
import json
import pandas as pd
import sys
from pathlib import Path
import logging
from datetime import datetime

from scanner.dhan_client import DhanClient
from scanner.scanner_engine import ScannerEngine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def load_symbols(file_path: str) -> list:
    """
    Load symbols from CSV file

    Args:
        file_path: Path to CSV file with symbols

    Returns:
        List of symbol strings
    """
    try:
        # Try to read CSV
        df = pd.read_csv(file_path, header=0)

        # Get first column (assume it contains symbols)
        if df.empty:
            logger.error(f"Empty CSV file: {file_path}")
            return []

        col = df.columns[0]
        symbols = df[col].astype(str).tolist()

        # Remove any NaN or empty values
        symbols = [s for s in symbols if s and s != 'nan']

        logger.info(f"Loaded {len(symbols)} symbols from {file_path}")
        return symbols

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return []
    except Exception as e:
        logger.error(f"Failed to load symbols file: {e}")
        return []


def save_results(results: dict, output_path: str, format: str = 'json'):
    """
    Save scan results to file

    Args:
        results: Scan results dictionary
        output_path: Path to save results
        format: Output format ('json' or 'csv')
    """
    try:
        if format == 'json':
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            logger.info(f"Results saved to {output_path}")

        elif format == 'csv':
            # Convert results to DataFrame for CSV export
            if results['results']:
                df_results = pd.json_normalize(results['results'])
                df_results.to_csv(output_path, index=False)
                logger.info(f"Results saved to {output_path}")
            else:
                logger.info("No results to save")

    except Exception as e:
        logger.error(f"Failed to save results: {e}")


def print_summary(results: dict):
    """
    Print summary of scan results

    Args:
        results: Scan results dictionary
    """
    stats = results.get('statistics', {})
    patterns = results.get('results', [])

    print("\n" + "="*60)
    print("SCAN SUMMARY")
    print("="*60)
    print(f"Symbols scanned:     {stats.get('symbols_scanned', 0)}")
    print(f"Symbols with data:   {stats.get('symbols_with_data', 0)}")
    print(f"Patterns found:      {stats.get('total_patterns_found', 0)}")
    print(f"Scan duration:       {stats.get('scan_duration_seconds', 0):.2f} seconds")
    print("="*60)

    if patterns:
        print("\nPATTERN DETAILS:")
        print("-"*60)
        for pattern in patterns[:10]:  # Show first 10 patterns
            print(f"\nSymbol: {pattern['symbol']}")
            print(f"Direction: {pattern['pattern_direction']}")
            print(f"Marubozu Date: {pattern['marubozu']['date']}")
            print(f"  - Body % of Range: {pattern['marubozu']['body_pct_of_range']:.2f}%")
            print(f"  - Body Move %: {pattern['marubozu']['body_move_pct']:.2f}%")
            print(f"Doji Date: {pattern['doji']['date']}")
            print(f"  - Body % of Range: {pattern['doji']['body_pct_of_range']:.2f}%")
            print(f"  - Breakout Amount: {pattern.get('breakout_amount', 'N/A')}")
            print(f"  - Rejection Strength: {pattern.get('rejection_strength', 'N/A')}%")

        if len(patterns) > 10:
            print(f"\n... and {len(patterns) - 10} more patterns")


def main():
    """Main CLI function"""
    parser = argparse.ArgumentParser(
        description='Marubozu → Doji Pattern Scanner for NSE Stocks',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scan daily timeframe
  python run_scanner.py --timeframe 1D --history 30 --min-body-move-pct 4

  # Scan weekly timeframe
  python run_scanner.py --timeframe 1W --history 20 --min-body-move-pct 4

  # Scan specific symbols
  python run_scanner.py --symbols RELIANCE TCS INFY --timeframe 1W

  # Save as CSV
  python run_scanner.py --timeframe 1D --output results.csv --format csv
        """
    )

    # Required arguments
    parser.add_argument(
        '--timeframe',
        default='1D',
        choices=['1D', '1W', '1M'],
        help='Timeframe for pattern detection (default: 1D)'
    )

    # Optional arguments
    parser.add_argument(
        '--history',
        type=int,
        default=20,
        help='Number of periods to look back (default: 20)'
    )

    parser.add_argument(
        '--min-body-move-pct',
        type=float,
        default=4.0,
        help='Minimum body move percentage for Marubozu filter (default: 4.0)'
    )

    parser.add_argument(
        '--symbols-file',
        default='security_id_list.csv',
        help='CSV file containing symbols to scan (default: security_id_list.csv)'
    )

    parser.add_argument(
        '--symbols',
        nargs='+',
        help='Specific symbols to scan (overrides --symbols-file)'
    )

    parser.add_argument(
        '--output',
        default='results.json',
        help='Output file path (default: results.json)'
    )

    parser.add_argument(
        '--format',
        choices=['json', 'csv'],
        default='json',
        help='Output format (default: json)'
    )

    parser.add_argument(
        '--no-summary',
        action='store_true',
        help='Skip printing summary to console'
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )

    args = parser.parse_args()

    # Set logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Load symbols
    if args.symbols:
        symbols = args.symbols
        logger.info(f"Using {len(symbols)} symbols from command line")
    else:
        symbols = load_symbols(args.symbols_file)
        if not symbols:
            logger.error("No symbols to scan. Please provide symbols via --symbols or --symbols-file")
            sys.exit(1)

    # Initialize scanner
    try:
        logger.info("Initializing DHAN client and scanner engine...")
        client = DhanClient()
        engine = ScannerEngine(client)
    except Exception as e:
        logger.error(f"Failed to initialize scanner: {e}")
        sys.exit(1)

    # Run scan
    logger.info(f"Starting scan with parameters:")
    logger.info(f"  Timeframe: {args.timeframe}")
    logger.info(f"  History: {args.history} periods")
    logger.info(f"  Min body move: {args.min_body_move_pct}%")
    logger.info(f"  Symbols: {len(symbols)} total")

    try:
        results = engine.scan(
            symbols=symbols,
            timeframe=args.timeframe,
            history=args.history,
            min_body_move_pct=args.min_body_move_pct
        )
    except Exception as e:
        logger.error(f"Scan failed: {e}")
        sys.exit(1)

    # Save results
    output_path = args.output
    if args.format == 'csv' and not output_path.endswith('.csv'):
        output_path = output_path.replace('.json', '.csv')

    save_results(results, output_path, args.format)

    # Print summary
    if not args.no_summary:
        print_summary(results)

    # Exit with appropriate code
    if results['statistics']['total_patterns_found'] > 0:
        sys.exit(0)  # Success, patterns found
    else:
        logger.info("No patterns found matching the criteria")
        sys.exit(0)  # Success, but no patterns


if __name__ == '__main__':
    main()