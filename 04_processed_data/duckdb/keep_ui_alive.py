#!/usr/bin/env python3
"""
Keep DuckDB UI connection alive
"""

import duckdb
import time
import signal
import sys
import threading
from pathlib import Path

class DuckDBUIServer:
    def __init__(self):
        self.db_path = Path(__file__).parent / "databases" / "recovery_directory.duckdb"
        self.conn = None
        self.running = True
        
    def signal_handler(self, sig, frame):
        print('\n\nShutting down UI server...')
        self.running = False
        sys.exit(0)
        
    def keep_alive(self):
        """Keep the connection alive with periodic queries"""
        while self.running:
            try:
                if self.conn:
                    # Execute a simple query to keep connection active
                    self.conn.execute("SELECT 1").fetchone()
            except:
                pass
            time.sleep(30)  # Check every 30 seconds
    
    def start(self):
        print("🦆 DuckDB UI Server - Connection Keeper")
        print("=" * 50)
        print(f"Database: {self.db_path}")
        print("=" * 50)
        
        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        
        try:
            # Connect to database
            self.conn = duckdb.connect(str(self.db_path))
            print("✓ Connected to database")
            
            # Check if UI extension is installed
            ui_status = self.conn.execute("SELECT installed, loaded FROM duckdb_extensions() WHERE extension_name = 'ui'").fetchone()
            
            if not ui_status or not ui_status[0]:
                print("Installing UI extension...")
                self.conn.execute("INSTALL ui")
                
            # Load UI extension
            print("Loading UI extension...")
            self.conn.execute("LOAD ui")
            print("✓ UI extension loaded")
            
            # Start UI server
            print("\nStarting UI server...")
            result = self.conn.execute("CALL start_ui_server()").fetchone()
            print(f"✓ {result[0]}")
            
            print("\n" + "="*50)
            print("🌐 DuckDB UI is running at: http://localhost:4213/")
            print("="*50)
            print("\n⚠️  IMPORTANT: Keep this window open!")
            print("The UI will stop working if you close this window.")
            print("\n⏹️  Press Ctrl+C to stop the server")
            print("="*50)
            
            # Start keep-alive thread
            keep_alive_thread = threading.Thread(target=self.keep_alive, daemon=True)
            keep_alive_thread.start()
            
            # Main loop - just sleep to keep process alive
            while self.running:
                time.sleep(1)
                
        except Exception as e:
            print(f"❌ Error: {e}")
            sys.exit(1)
        finally:
            if self.conn:
                try:
                    self.conn.execute("CALL stop_ui_server()")
                except:
                    pass
                self.conn.close()

def main():
    server = DuckDBUIServer()
    server.start()

if __name__ == "__main__":
    main()