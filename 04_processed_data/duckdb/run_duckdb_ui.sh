#!/bin/bash

echo "🦆 Starting DuckDB UI Server"
echo "============================"

cd /Users/benweiss/Code/narr_extractor/04_processed_data/duckdb/databases

# Kill any existing DuckDB processes
pkill -f "duckdb.*recovery_directory" 2>/dev/null

# Start DuckDB with UI
echo "Starting DuckDB with UI extension..."
duckdb recovery_directory.duckdb -cmd "INSTALL ui; LOAD ui; CALL start_ui_server();" -interactive

echo "If the UI didn't start, try running:"
echo "  duckdb recovery_directory.duckdb"
echo "Then in DuckDB shell:"
echo "  INSTALL ui;"
echo "  LOAD ui;"
echo "  CALL start_ui_server();"