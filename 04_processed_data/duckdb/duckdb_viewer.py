#!/usr/bin/env python3
"""
Interactive DuckDB Viewer for Recovery Directory
"""

import duckdb
import pandas as pd
from pathlib import Path

def main():
    # Connect to database
    db_path = Path(__file__).parent / "databases" / "recovery_directory.duckdb"
    conn = duckdb.connect(str(db_path))
    
    print("🦆 DuckDB Recovery Directory Viewer")
    print("=" * 50)
    print(f"Database: {db_path}")
    print("41,097 organizations loaded")
    print("=" * 50)
    
    # Show available tables and views
    print("\nAvailable Tables:")
    tables = conn.execute("SHOW TABLES").fetchall()
    for i, (table,) in enumerate(tables, 1):
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {i}. {table} ({count:,} rows)")
    
    print("\nAvailable Views:")
    views = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW'").fetchall()
    for i, (view,) in enumerate(views, 1):
        print(f"  {i}. {view}")
    
    print("\nExample Queries:")
    print("1. SELECT organization_type, COUNT(*) FROM organizations GROUP BY organization_type")
    print("2. SELECT * FROM state_summary LIMIT 10")
    print("3. SELECT * FROM data_quality_dashboard")
    print("4. SELECT name, address_city, address_state FROM organizations WHERE address_state = 'CA' LIMIT 20")
    
    while True:
        print("\n" + "-" * 50)
        query = input("\nEnter SQL query (or 'quit' to exit, 'help' for examples): ").strip()
        
        if query.lower() == 'quit':
            break
        elif query.lower() == 'help':
            print("\nExample queries:")
            print("- Organization overview: SELECT organization_type, COUNT(*) as count FROM organizations GROUP BY organization_type ORDER BY count DESC")
            print("- State summary: SELECT * FROM state_summary ORDER BY organization_count DESC LIMIT 10")
            print("- Search by state: SELECT * FROM organizations WHERE address_state = 'TX' LIMIT 20")
            print("- Data quality: SELECT * FROM data_quality_dashboard")
            continue
        elif query == '1':
            query = "SELECT organization_type, COUNT(*) as count FROM organizations GROUP BY organization_type ORDER BY count DESC"
        elif query == '2':
            query = "SELECT * FROM state_summary LIMIT 10"
        elif query == '3':
            query = "SELECT * FROM data_quality_dashboard"
        elif query == '4':
            query = "SELECT name, address_city, address_state FROM organizations WHERE address_state = 'CA' LIMIT 20"
        
        if not query:
            continue
            
        try:
            # Execute query
            result = conn.execute(query).fetchdf()
            
            # Display results
            print(f"\nResults ({len(result)} rows):")
            print(result.to_string(index=False, max_rows=50))
            
            # Offer to export
            if len(result) > 0:
                export = input("\nExport results? (csv/json/no): ").lower()
                if export == 'csv':
                    filename = f"export_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    result.to_csv(filename, index=False)
                    print(f"Exported to {filename}")
                elif export == 'json':
                    filename = f"export_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.json"
                    result.to_json(filename, orient='records', indent=2)
                    print(f"Exported to {filename}")
                    
        except Exception as e:
            print(f"Error: {e}")
    
    conn.close()
    print("\nGoodbye!")

if __name__ == "__main__":
    main()