#!/usr/bin/env python3
"""
Simple DuckDB UI starter
"""

import subprocess
import os
import sys

# Change to database directory
db_dir = os.path.join(os.path.dirname(__file__), 'databases')
os.chdir(db_dir)

print("🦆 Starting DuckDB UI...")
print("=" * 50)
print(f"Directory: {os.getcwd()}")
print("Database: recovery_directory.duckdb")
print("=" * 50)

# Run DuckDB with UI flag
try:
    # Run DuckDB UI
    process = subprocess.Popen(
        ['duckdb', 'recovery_directory.duckdb', '-ui'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True
    )
    
    # Read initial output
    for i in range(5):  # Read first 5 lines
        line = process.stdout.readline()
        if line:
            print(line.strip())
            if "localhost:4213" in line:
                print("\n✅ DuckDB UI is running!")
                print("🌐 Open http://localhost:4213/ in your browser")
                break
    
    print("\n⏹️  Press Ctrl+C to stop")
    print("=" * 50)
    
    # Wait for process
    process.wait()
    
except KeyboardInterrupt:
    print("\n\nStopping DuckDB UI...")
    process.terminate()
    print("✅ Stopped")
except Exception as e:
    print(f"❌ Error: {e}")