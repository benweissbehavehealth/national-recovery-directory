#!/usr/bin/env python3
"""
DuckDB UI Server - Keeps connection alive
"""

import duckdb
import time
import signal
import sys
from pathlib import Path

class DuckDBUIServer:
    def __init__(self):
        self.db_path = Path(__file__).parent / "databases" / "recovery_directory.duckdb"
        self.conn = None
        self.running = True
        
    def signal_handler(self, sig, frame):
        print('\n\nShutting down UI server...')
        self.running = False
        if self.conn:
            try:
                self.conn.execute("CALL stop_ui_server()")
                print("✓ UI server stopped")
            except:
                pass
            self.conn.close()
        sys.exit(0)
        
    def start(self):
        print("🦆 DuckDB UI Server")
        print("=" * 50)
        print(f"Database: {self.db_path}")
        print("=" * 50)
        
        # Set up signal handler
        signal.signal(signal.SIGINT, self.signal_handler)
        
        try:
            # Connect to database
            self.conn = duckdb.connect(str(self.db_path))
            
            # Install and load UI extension
            print("Loading UI extension...")
            try:
                self.conn.execute("INSTALL ui")
            except:
                pass  # Already installed
                
            self.conn.execute("LOAD ui")
            print("✓ UI extension loaded")
            
            # Start UI server
            result = self.conn.execute("CALL start_ui_server()").fetchone()
            print(f"✓ {result[0]}")
            
            print("\n" + "="*50)
            print("🌐 DuckDB UI is running at: http://localhost:4213/")
            print("="*50)
            print("\nDatabase Statistics:")
            
            # Show some stats
            stats = self.conn.execute("""
                SELECT 
                    (SELECT COUNT(*) FROM organizations) as total_orgs,
                    (SELECT COUNT(*) FROM data_sources) as total_sources,
                    (SELECT COUNT(*) FROM certification_authorities) as total_authorities
            """).fetchone()
            
            print(f"  • Organizations: {stats[0]:,}")
            print(f"  • Data Sources: {stats[1]}")
            print(f"  • Certification Authorities: {stats[2]}")
            
            print("\nUseful queries to try in the UI:")
            print("  • SELECT * FROM data_source_summary;")
            print("  • SELECT * FROM organization_data_freshness LIMIT 20;")
            print("  • SELECT * FROM certification_authority_summary;")
            
            print("\n⏹️  Press Ctrl+C to stop the server")
            print("="*50)
            
            # Keep the connection alive
            while self.running:
                # Periodic query to keep connection active
                self.conn.execute("SELECT 1").fetchone()
                time.sleep(30)  # Check every 30 seconds
                
        except Exception as e:
            print(f"❌ Error: {e}")
            if self.conn:
                self.conn.close()
            sys.exit(1)

def main():
    server = DuckDBUIServer()
    server.start()

if __name__ == "__main__":
    main()