#!/usr/bin/env python3
"""
Fix and start DuckDB UI
"""

import duckdb
import time
import subprocess
import os
from pathlib import Path

def main():
    db_path = Path(__file__).parent / "databases" / "recovery_directory.duckdb"
    
    print("🦆 Fixing DuckDB UI...")
    print("=" * 50)
    
    # First, ensure UI extension is installed
    conn = duckdb.connect(str(db_path))
    
    try:
        # Check if UI extension is installed
        result = conn.execute("SELECT * FROM duckdb_extensions() WHERE extension_name = 'ui'").fetchone()
        if result:
            print(f"✓ UI Extension found: {result}")
        else:
            print("Installing UI extension...")
            conn.execute("INSTALL ui")
            print("✓ UI Extension installed")
        
        # Load the extension
        conn.execute("LOAD ui")
        print("✓ UI Extension loaded")
        
        # Try to start UI server
        result = conn.execute("CALL start_ui_server()").fetchone()
        print(f"✓ UI Server Response: {result}")
        
        # Keep connection alive
        print("\n🌐 DuckDB UI should be available at: http://localhost:4213/")
        print("⏹️  Press Ctrl+C to stop\n")
        
        while True:
            # Keep connection alive with periodic queries
            conn.execute("SELECT 1").fetchone()
            time.sleep(10)
            
    except KeyboardInterrupt:
        print("\n\nStopping UI server...")
        try:
            conn.execute("CALL stop_ui_server()")
        except:
            pass
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nTrying alternative method...")
        
        # Alternative: Run duckdb directly
        conn.close()
        os.chdir(str(db_path.parent))
        print(f"Changed to directory: {os.getcwd()}")
        print("Running: duckdb recovery_directory.duckdb -ui")
        
        try:
            subprocess.run(['duckdb', 'recovery_directory.duckdb', '-ui'])
        except Exception as e2:
            print(f"❌ Alternative method failed: {e2}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()