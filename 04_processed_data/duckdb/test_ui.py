#!/usr/bin/env python3
"""
Test DuckDB UI connection
"""

import duckdb
import requests
import time
from pathlib import Path

def test_ui():
    db_path = Path(__file__).parent / "databases" / "recovery_directory.duckdb"
    
    print("🦆 Testing DuckDB UI Setup")
    print("=" * 50)
    
    # Connect to database
    conn = duckdb.connect(str(db_path))
    
    try:
        # Check if UI extension is installed
        extensions = conn.execute("SELECT extension_name, installed, loaded FROM duckdb_extensions() WHERE extension_name = 'ui'").fetchall()
        print(f"UI Extension Status: {extensions}")
        
        if not extensions or not extensions[0][1]:
            print("Installing UI extension...")
            conn.execute("INSTALL ui")
        
        # Load UI extension
        print("Loading UI extension...")
        conn.execute("LOAD ui")
        
        # Check if UI server is already running
        try:
            response = requests.get("http://localhost:4213/", timeout=2)
            if response.status_code == 200:
                print("✅ UI server is already running at http://localhost:4213/")
                return
        except:
            pass
        
        # Start UI server
        print("Starting UI server...")
        result = conn.execute("CALL start_ui_server()").fetchone()
        print(f"Server response: {result}")
        
        # Wait a moment
        time.sleep(2)
        
        # Test connection
        try:
            response = requests.get("http://localhost:4213/", timeout=5)
            if response.status_code == 200:
                print("✅ UI server is accessible at http://localhost:4213/")
            else:
                print(f"❌ UI server returned status code: {response.status_code}")
        except Exception as e:
            print(f"❌ Could not connect to UI server: {e}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        # Don't close connection to keep UI running
        print("\nKeeping connection open for UI...")
        try:
            while True:
                time.sleep(10)
        except KeyboardInterrupt:
            print("\nClosing connection...")
            conn.close()

if __name__ == "__main__":
    test_ui()