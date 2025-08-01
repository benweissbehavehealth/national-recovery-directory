#!/bin/bash
# Launch DuckDB UI for Recovery Directory

echo "🦆 Launching DuckDB UI..."
echo "================================"
echo "Database: recovery_directory.duckdb"
echo ""

# Check if duckdb is installed
if ! command -v duckdb &> /dev/null; then
    echo "❌ DuckDB CLI not found!"
    echo ""
    echo "Install DuckDB:"
    echo "  macOS: brew install duckdb"
    echo "  Linux: wget https://github.com/duckdb/duckdb/releases/download/v1.2.1/duckdb_cli-linux-amd64.zip"
    echo ""
    exit 1
fi

# Launch UI
echo "Starting web UI at http://localhost:3000"
echo "Press Ctrl+C to stop"
echo ""

cd "$(dirname "$0")/databases"
duckdb -ui recovery_directory.duckdb