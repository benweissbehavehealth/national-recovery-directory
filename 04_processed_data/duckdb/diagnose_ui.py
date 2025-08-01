#!/usr/bin/env python3
"""
Diagnose DuckDB UI issues
"""

import duckdb
import subprocess
import sys
from pathlib import Path

def diagnose():
    print("🔍 Diagnosing DuckDB UI Issues")
    print("=" * 50)
    
    # Check DuckDB version
    try:
        result = subprocess.run(['duckdb', '--version'], capture_output=True, text=True)
        print(f"✓ DuckDB Version: {result.stdout.strip()}")
    except:
        print("❌ DuckDB not found in PATH")
        return
    
    # Connect to database
    db_path = Path(__file__).parent / "databases" / "recovery_directory.duckdb"
    print(f"\n✓ Database Path: {db_path}")
    
    try:
        conn = duckdb.connect(str(db_path))
        print("✓ Database connection successful")
        
        # Check available extensions
        print("\n📦 Available Extensions:")
        extensions = conn.execute("SELECT * FROM duckdb_extensions()").fetchall()
        for ext in extensions:
            if ext[0] == 'ui' or 'ui' in str(ext):
                print(f"  UI Extension: {ext}")
        
        # Try to install UI extension
        print("\n🔧 Installing UI Extension...")
        try:
            conn.execute("INSTALL ui")
            print("✓ UI extension installed")
        except Exception as e:
            print(f"⚠️  Install message: {e}")
        
        # Try to load UI extension
        print("\n🔧 Loading UI Extension...")
        try:
            conn.execute("LOAD ui")
            print("✓ UI extension loaded")
        except Exception as e:
            print(f"❌ Failed to load UI: {e}")
            return
        
        # Check if start_ui_server function exists
        print("\n🔍 Checking UI Functions...")
        try:
            functions = conn.execute("SELECT function_name FROM duckdb_functions() WHERE function_name LIKE '%ui%'").fetchall()
            print(f"UI-related functions: {functions}")
        except:
            pass
        
        # Try to start UI server
        print("\n🚀 Starting UI Server...")
        try:
            result = conn.execute("CALL start_ui_server()").fetchone()
            print(f"✓ Server response: {result}")
            
            # Keep connection alive
            print("\n✅ UI should be running at http://localhost:4213/")
            print("Press Ctrl+C to stop")
            
            import time
            while True:
                time.sleep(10)
                
        except KeyboardInterrupt:
            print("\nStopping...")
        except Exception as e:
            print(f"❌ Failed to start UI server: {e}")
            
            # Try alternative approaches
            print("\n🔄 Trying alternative method...")
            conn.close()
            
            # Try direct CLI approach
            print("Running: duckdb recovery_directory.duckdb -ui")
            subprocess.run([
                'duckdb', 
                str(db_path), 
                '-ui'
            ])
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    diagnose()