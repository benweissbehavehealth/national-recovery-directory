#!/bin/bash

echo "🦆 MotherDuck Migration Launcher"
echo "================================"
echo ""
echo "This will push your local DuckDB database to MotherDuck cloud."
echo ""
echo "Prerequisites:"
echo "1. MotherDuck account created"
echo "2. Authentication token ready"
echo "3. Local database not locked (close DuckDB UI if running)"
echo ""
echo "Press Enter to continue or Ctrl+C to cancel..."
read

# Check if DuckDB UI is running
if lsof -i :4213 > /dev/null 2>&1; then
    echo "⚠️  WARNING: DuckDB UI is running on port 4213"
    echo "This may cause database lock issues."
    echo ""
    read -p "Do you want to continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Migration cancelled."
        exit 1
    fi
fi

# Check if local database exists
if [ ! -f "databases/recovery_directory.duckdb" ]; then
    echo "❌ Error: Local database not found at databases/recovery_directory.duckdb"
    exit 1
fi

# Check if migration script exists
if [ ! -f "scripts/migration/push_to_motherduck.py" ]; then
    echo "❌ Error: Migration script not found"
    exit 1
fi

echo "🚀 Starting migration..."
echo ""

# Run the migration script
python3 scripts/migration/push_to_motherduck.py

# Check exit code
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Migration completed successfully!"
    echo ""
    echo "Your database is now available in MotherDuck at:"
    echo "md:narr_directory"
    echo ""
    echo "You can connect using:"
    echo "duckdb md:narr_directory?motherduck_token=YOUR_TOKEN"
else
    echo ""
    echo "❌ Migration failed. Check the logs above for details."
    exit 1
fi 