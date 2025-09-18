"""
Flask Web Application for Pattern Scanner Platform
Professional Trading Analytics Dashboard
"""

from flask import Flask, render_template, jsonify, request, send_file
from flask_cors import CORS
import json
import pandas as pd
from datetime import datetime, timedelta
import threading
import time
import os
from scanner.dhan_client import DhanClient
from scanner.scanner_engine import ScannerEngine
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Disable browser caching for static files during development
@app.after_request
def add_header(response):
    if request.endpoint == 'static':
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
    return response

# Global variables for caching and state management
scan_cache = {}
scan_in_progress = False
current_scan_status = {"progress": 0, "message": "Ready"}

# Clear cache on startup to avoid showing stale data
def clear_cache():
    """Clear all cached scan results and reset scan state"""
    global scan_cache, current_scan_status, scan_in_progress
    scan_cache.clear()
    scan_in_progress = False
    current_scan_status = {"progress": 0, "message": "Ready"}
    logger.info("Cache cleared and scan state reset")

# Clear cache immediately on startup
clear_cache()

# Initialize scanner components
try:
    # Try to use SQLite scanner first (fast!)
    from scanner.sqlite_scanner_engine import SQLiteScannerEngine
    from database.sqlite_db_manager import SQLiteDBManager

    # Check if database exists
    import os
    if os.path.exists("database/pattern_scanner.db"):
        scanner_engine = SQLiteScannerEngine("database/pattern_scanner.db")
        logger.info("SQLite Scanner Engine initialized (100x faster!)")

        # Check completeness
        completeness = scanner_engine.check_data_completeness()
        logger.info(f"Database has {completeness['symbols_with_data']}/{completeness['total_symbols']} symbols")
    else:
        # Fallback to API-based scanner
        dhan_client = DhanClient()
        scanner_engine = ScannerEngine(dhan_client)
        logger.info("API-based Scanner engine initialized")

except Exception as e:
    logger.error(f"Failed to initialize scanner: {e}")
    # Final fallback
    try:
        dhan_client = DhanClient()
        scanner_engine = ScannerEngine(dhan_client)
        logger.info("Fallback to API scanner")
    except:
        scanner_engine = None

def load_symbols(symbol_type="fno"):
    """Load symbols from CSV file"""
    try:
        if symbol_type == "fno":
            df = pd.read_csv('fno_symbols_corrected.csv')
        elif symbol_type == "nifty50":
            df = pd.read_csv('nse_symbols.csv')
        else:
            df = pd.read_csv('fno_symbols_corrected.csv')  # Default to corrected F&O

        return df['Symbol'].tolist()
    except Exception as e:
        logger.error(f"Error loading symbols: {e}")
        return []

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')

@app.route('/api/scan', methods=['POST'])
def scan():
    """API endpoint to trigger pattern scan"""
    global scan_in_progress, current_scan_status

    if scan_in_progress:
        return jsonify({"error": "Scan already in progress"}), 400

    if not scanner_engine:
        return jsonify({"error": "Scanner not initialized"}), 500

    data = request.json

    # Extract parameters
    timeframe = data.get('timeframe', '1D')
    min_body_move = float(data.get('min_body_move', 4))
    days_back = int(data.get('days_back', 30))
    symbol_group = data.get('symbol_group', 'fno')
    custom_symbols = data.get('custom_symbols', '')

    # Get symbols
    if custom_symbols:
        symbols = [s.strip().upper() for s in custom_symbols.split(',') if s.strip()]
    else:
        symbols = load_symbols(symbol_group)

    if not symbols:
        return jsonify({"error": "No symbols to scan"}), 400

    # Start scan in background
    def run_scan():
        global scan_in_progress, current_scan_status, scan_cache
        scan_in_progress = True
        current_scan_status = {"progress": 0, "message": f"Starting scan of {len(symbols)} symbols..."}

        try:
            # Run the scan
            results = scanner_engine.scan(
                symbols=symbols,
                timeframe=timeframe,
                history=days_back,
                min_body_move_pct=min_body_move
            )

            # Cache results
            scan_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            scan_cache[scan_id] = results

            # Update status
            current_scan_status = {
                "progress": 100,
                "message": f"Scan complete: {len(results.get('results', []))} patterns found",
                "scan_id": scan_id,
                "results": results
            }

        except Exception as e:
            logger.error(f"Scan error: {e}")
            current_scan_status = {
                "progress": 0,
                "message": f"Scan failed: {str(e)}",
                "error": True
            }
        finally:
            scan_in_progress = False

    # Start background thread
    thread = threading.Thread(target=run_scan)
    thread.start()

    return jsonify({"message": "Scan started", "status": current_scan_status})

@app.route('/api/scan/status')
def scan_status():
    """Get current scan status"""
    return jsonify(current_scan_status)

@app.route('/api/results/latest')
def get_latest_results():
    """Get latest scan results"""
    if scan_cache:
        latest_id = max(scan_cache.keys())
        return jsonify(scan_cache[latest_id])
    return jsonify({"results": [], "statistics": {}})

# Global variable to track EOD update status
eod_update_status = {'status': 'idle'}

@app.route('/api/eod-update', methods=['POST'])
def start_eod_update():
    """Start EOD update process"""
    import threading
    from database.daily_eod_update import EODUpdater

    global eod_update_status

    def run_update():
        global eod_update_status
        eod_update_status = {
            'status': 'running',
            'progress': 0,
            'symbols_updated': 0,
            'new_records': 0,
            'message': 'Starting EOD update...'
        }

        try:
            updater = EODUpdater()
            # Run the update and capture results
            success = updater.update_todays_data()

            # Get stats from database
            stats = updater.db.get_database_stats()

            eod_update_status = {
                'status': 'completed',
                'progress': 100,
                'symbols_updated': 154,  # Total symbols
                'new_records': stats.get('total_daily_records', 0),
                'message': 'EOD update completed successfully'
            }
        except Exception as e:
            eod_update_status = {
                'status': 'error',
                'progress': 0,
                'message': f'Error: {str(e)}'
            }

    # Check if update already running
    if eod_update_status.get('status') == 'running':
        return jsonify({'status': 'error', 'message': 'Update already in progress'}), 400

    # Start update in background thread
    thread = threading.Thread(target=run_update)
    thread.daemon = True
    thread.start()

    return jsonify({'status': 'started', 'message': 'EOD update started'})

@app.route('/api/eod-update/status')
def get_eod_update_status():
    """Get EOD update status"""
    global eod_update_status
    return jsonify(eod_update_status)

@app.route('/api/results/database')
def get_database_patterns():
    """Get patterns from database for all timeframes"""
    import sqlite3

    conn = sqlite3.connect('database/pattern_scanner.db')
    cursor = conn.cursor()

    # Get patterns for all timeframes
    cursor.execute("""
        SELECT dp.*, s.symbol, pt.pattern_name
        FROM detected_patterns dp
        JOIN symbols s ON dp.symbol_id = s.symbol_id
        JOIN pattern_types pt ON dp.pattern_type_id = pt.pattern_type_id
        ORDER BY dp.timeframe, dp.pattern_date DESC
    """)

    patterns = cursor.fetchall()

    # Format results by timeframe
    results_by_timeframe = {
        '1D': [],
        'WEEKLY': [],
        'MONTHLY': []
    }

    for pattern in patterns:
        pattern_data = pattern[7]  # pattern_data column

        # Parse the pattern_data string to dict
        try:
            if pattern_data:
                # Try JSON first, fallback to ast.literal_eval for old data
                try:
                    pattern_dict = json.loads(pattern_data)
                except:
                    import ast
                    pattern_dict = ast.literal_eval(pattern_data)

                # Add to appropriate timeframe
                timeframe = pattern[3]  # timeframe column
                if timeframe in results_by_timeframe:
                    results_by_timeframe[timeframe].append(pattern_dict)
        except:
            continue

    # Count patterns by timeframe
    statistics = {
        'daily_patterns': len(results_by_timeframe['1D']),
        'weekly_patterns': len(results_by_timeframe['WEEKLY']),
        'monthly_patterns': len(results_by_timeframe['MONTHLY']),
        'total_patterns': len(patterns),
        'timestamp': datetime.now().isoformat()
    }

    conn.close()

    return jsonify({
        'results': results_by_timeframe,
        'statistics': statistics
    })

@app.route('/api/results/today')
def get_todays_signals():
    """Get today's signals only (yesterday Marubozu → today Doji)"""
    from datetime import datetime, timedelta

    if not scan_cache:
        return jsonify({"results": [], "statistics": {}, "message": "No scan results available"})

    # Get today and yesterday dates
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    # Convert to string format used in results
    today_str = today.strftime('%Y-%m-%d')
    yesterday_str = yesterday.strftime('%Y-%m-%d')

    # Get latest results
    latest_id = max(scan_cache.keys())
    latest_results = scan_cache[latest_id]

    # Filter for yesterday→today patterns only
    todays_patterns = []
    for result in latest_results.get('results', []):
        marubozu_date = result.get('marubozu', {}).get('date', '')
        doji_date = result.get('doji', {}).get('date', '')

        # Check if pattern is yesterday Marubozu → today Doji
        if marubozu_date == yesterday_str and doji_date == today_str:
            todays_patterns.append(result)

    # Create response with filtered results
    response = {
        "results": todays_patterns,
        "statistics": {
            "scan_timestamp": latest_results.get('statistics', {}).get('scan_timestamp'),
            "total_patterns_found": len(todays_patterns),
            "filter_applied": f"Yesterday ({yesterday_str}) Marubozu → Today ({today_str}) Doji",
            "original_total_patterns": len(latest_results.get('results', [])),
            "symbols_scanned": latest_results.get('statistics', {}).get('symbols_scanned', 0)
        },
        "message": f"Found {len(todays_patterns)} today's signals" if todays_patterns else "No today's signals found"
    }

    return jsonify(response)

@app.route('/api/cache/clear', methods=['POST'])
def clear_cache_endpoint():
    """Clear all cached scan results and reset scan state"""
    clear_cache()
    return jsonify({"message": "Cache cleared and scan state reset", "status": "success"})

@app.route('/api/scan/reset', methods=['POST'])
def reset_scan_state():
    """Reset scan state if stuck"""
    global scan_in_progress, current_scan_status
    scan_in_progress = False
    current_scan_status = {"progress": 0, "message": "Ready"}
    logger.info("Scan state reset manually")
    return jsonify({"message": "Scan state reset successfully", "status": "success"})

@app.route('/api/results/<scan_id>')
def get_results(scan_id):
    """Get specific scan results"""
    if scan_id in scan_cache:
        return jsonify(scan_cache[scan_id])
    return jsonify({"error": "Scan not found"}), 404

@app.route('/api/export/<format>')
def export_results(format):
    """Export results in different formats"""
    try:
        if not scan_cache:
            return jsonify({"error": "No results to export"}), 400

        latest_id = max(scan_cache.keys())
        results = scan_cache[latest_id]

        if format == 'json':
            filename = f'scan_results_{latest_id}.json'
            with open(filename, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            return send_file(filename, as_attachment=True)

        elif format == 'csv':
            filename = f'scan_results_{latest_id}.csv'
            df = pd.json_normalize(results['results'])
            df.to_csv(filename, index=False)
            return send_file(filename, as_attachment=True)

        elif format == 'excel':
            filename = f'scan_results_{latest_id}.xlsx'
            df = pd.json_normalize(results['results'])
            df.to_excel(filename, index=False)
            return send_file(filename, as_attachment=True)

        else:
            return jsonify({"error": "Invalid format"}), 400

    except Exception as e:
        logger.error(f"Export error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/symbols/<group>')
def get_symbols(group):
    """Get available symbols for a group"""
    symbols = load_symbols(group)
    return jsonify({"symbols": symbols, "count": len(symbols)})

@app.route('/api/stats')
def get_stats():
    """Get scanner statistics"""
    stats = {
        "total_scans": len(scan_cache),
        "scanner_status": "online" if scanner_engine else "offline",
        "last_scan": max(scan_cache.keys()) if scan_cache else None,
        "patterns_today": 0
    }

    # Count today's patterns
    if scan_cache:
        latest_results = scan_cache[max(scan_cache.keys())]
        today = datetime.now().date()
        for pattern in latest_results.get('results', []):
            pattern_date = datetime.fromisoformat(pattern.get('scan_timestamp', '')).date()
            if pattern_date == today:
                stats['patterns_today'] += 1

    return jsonify(stats)

@app.route('/api/scanners')
def get_scanners():
    """Get list of available scanners (for future multi-scanner support)"""
    scanners = [
        {
            "id": "marubozu_doji",
            "name": "Marubozu + Doji",
            "description": "Two-candle patterns",
            "status": "active",
            "icon": "📊"
        },
        {
            "id": "hammer_shooting",
            "name": "Hammer & Shooting",
            "description": "Reversal patterns",
            "status": "soon",
            "icon": "🔨"
        },
        {
            "id": "engulfing",
            "name": "Engulfing",
            "description": "Engulfing patterns",
            "status": "soon",
            "icon": "📈"
        },
        {
            "id": "doji_variations",
            "name": "Doji Variations",
            "description": "All doji types",
            "status": "soon",
            "icon": "⭐"
        },
        {
            "id": "three_patterns",
            "name": "Three Patterns",
            "description": "Soldiers & Crows",
            "status": "soon",
            "icon": "🎯"
        }
    ]
    return jsonify(scanners)

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)

    logger.info("Starting Scanner Platform on http://localhost:5000")
    app.run(host='127.0.0.1', port=5000, debug=True)