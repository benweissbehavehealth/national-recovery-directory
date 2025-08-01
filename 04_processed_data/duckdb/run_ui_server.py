#!/usr/bin/env python3
"""
Keep DuckDB UI server running
"""

import duckdb
import time
import threading
import webbrowser
from pathlib import Path

def keep_alive(conn):
    """Keep the connection alive by running periodic queries"""
    while True:
        try:
            conn.execute("SELECT 1").fetchone()
            time.sleep(60)  # Check every minute
        except:
            break

def main():
    # Connect to database
    db_path = Path(__file__).parent / "databases" / "recovery_directory.duckdb"
    conn = duckdb.connect(str(db_path))
    
    print("🦆 Starting DuckDB UI Server...")
    print("=" * 50)
    
    try:
        # Install and load UI extension
        conn.execute("INSTALL ui")
        conn.execute("LOAD ui")
        
        # Start the UI server
        result = conn.execute("CALL start_ui_server()").fetchone()
        print(f"✅ {result[0]}")
        
        # Start keep-alive thread
        keep_alive_thread = threading.Thread(target=keep_alive, args=(conn,), daemon=True)
        keep_alive_thread.start()
        
        print("\n📊 Database Info:")
        print(f"- Database: {db_path}")
        print("- Total Organizations: 41,097")
        print("- Tables: organizations, services, networks, etc.")
        print("- Views: state_summary, data_quality_dashboard, etc.")
        
        print("\n🌐 UI is available at: http://localhost:4213/")
        print("Opening in browser...")
        time.sleep(2)
        webbrowser.open('http://localhost:4213/')
        
        print("\n⏹️  Press Ctrl+C to stop the server")
        
        # Keep the server running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n\n👋 Stopping UI server...")
        try:
            conn.execute("CALL stop_ui_server()")
            print("✅ Server stopped")
        except:
            pass
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()