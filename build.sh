#!/usr/bin/env bash
# Build script for Render deployment

set -o errexit

# Install Python dependencies
pip install -r requirements.txt

# Create database directory if it doesn't exist
mkdir -p database

# Initialize the database if it doesn't exist
if [ ! -f database/pattern_scanner.db ]; then
    echo "Initializing database..."
    cd database
    python sqlite_setup.py
    cd ..
    echo "Database initialized successfully"
else
    echo "Database already exists"
fi

echo "Build completed successfully"