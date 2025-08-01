#!/bin/bash

echo "🦆 Starting DuckDB UI Server"
echo "==========================="
echo ""
echo "This will open DuckDB with the UI server running."
echo "IMPORTANT: Do NOT close this window or the UI will stop!"
echo ""
echo "Press Enter to continue..."
read

cd /Users/benweiss/Code/narr_extractor/04_processed_data/duckdb/databases

# Start DuckDB and keep it running
duckdb recovery_directory.duckdb << 'EOF'
.echo on
INSTALL ui;
LOAD ui;
CALL start_ui_server();
.echo off

-- Keep the session alive
SELECT '✅ UI is running at http://localhost:4213/';
SELECT '⚠️  Keep this window open!';
SELECT 'Press Ctrl+D to stop the UI server';

-- This will keep DuckDB running interactively
.shell echo "Keeping DuckDB alive..."
EOF

# If we get here, start interactive mode
duckdb recovery_directory.duckdb