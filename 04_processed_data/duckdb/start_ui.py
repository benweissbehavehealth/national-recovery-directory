#!/usr/bin/env python3
"""Start DuckDB UI and keep it running"""

import subprocess
import time
import webbrowser
import os

print("🦆 Starting DuckDB UI...")
print("=" * 50)

# Change to database directory
os.chdir(os.path.join(os.path.dirname(__file__), 'databases'))

# Start DuckDB UI
process = subprocess.Popen(
    ['duckdb', 'recovery_directory.duckdb', '-ui'],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True
)

# Wait a moment for the server to start
time.sleep(2)

# Check if UI started successfully
if process.poll() is None:
    print("✅ DuckDB UI started successfully!")
    print("🌐 Opening http://localhost:4213/ in your browser...")
    webbrowser.open('http://localhost:4213/')
    
    print("\n📊 Database Info:")
    print("- Total Organizations: 41,097")
    print("- Database Size: 24.26 MB")
    print("- Tables: organizations, services, networks, etc.")
    print("- Views: state_summary, data_quality_dashboard, etc.")
    
    print("\n💡 Try these queries in the UI:")
    print("1. SELECT organization_type, COUNT(*) FROM organizations GROUP BY organization_type")
    print("2. SELECT * FROM state_summary ORDER BY organization_count DESC LIMIT 10")
    print("3. SELECT * FROM data_quality_dashboard")
    
    print("\n⏹️  Press Ctrl+C to stop the UI server")
    
    try:
        # Keep the process running
        process.wait()
    except KeyboardInterrupt:
        print("\n\n👋 Stopping DuckDB UI...")
        process.terminate()
        process.wait()
        print("✅ UI stopped successfully")
else:
    print("❌ Failed to start DuckDB UI")
    stderr = process.stderr.read()
    if stderr:
        print(f"Error: {stderr}")