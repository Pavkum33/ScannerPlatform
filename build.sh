#!/usr/bin/env bash
# Build script for Render deployment

set -o errexit

echo "==================================="
echo "Starting Render Build Process"
echo "==================================="

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Create database directory if it doesn't exist
mkdir -p database

# Initialize the database if it doesn't exist
if [ ! -f database/pattern_scanner.db ]; then
    echo "==================================="
    echo "Initializing SQLite Database"
    echo "==================================="

    cd database

    # Run database setup
    echo "1. Setting up database schema and F&O symbols..."
    python sqlite_setup.py

    # Load initial historical data (last 30 days)
    echo "2. Loading initial historical data..."
    python run_setup.py

    # Generate weekly/monthly aggregations
    echo "3. Generating weekly/monthly patterns..."
    python generate_aggregations.py

    cd ..
    echo "==================================="
    echo "Database initialized successfully!"
    echo "==================================="
else
    echo "Database already exists. Skipping initialization."

    # Run daily update if database exists
    echo "Running daily EOD update..."
    cd database
    python daily_eod_update.py --check
    cd ..
fi

echo "==================================="
echo "Build completed successfully!"
echo "==================================="