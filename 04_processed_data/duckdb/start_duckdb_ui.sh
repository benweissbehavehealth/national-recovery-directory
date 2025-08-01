#!/bin/bash
# Start DuckDB UI Server

cd /Users/benweiss/Code/narr_extractor/04_processed_data/duckdb/databases

echo "🦆 Starting DuckDB UI Server..."
echo "================================"
echo "Database: recovery_directory.duckdb"
echo "URL: http://localhost:4213/"
echo "Press Ctrl+C to stop"
echo "================================"
echo ""

# Start DuckDB with UI and keep it running
duckdb recovery_directory.duckdb << 'EOF'
INSTALL ui;
LOAD ui;
CALL start_ui_server();
-- Keep the session alive
SELECT 'UI is running at http://localhost:4213/';
SELECT 'Press Ctrl+C to stop';
-- This will keep DuckDB running
.timer on
SELECT 1;
EOF