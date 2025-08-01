#!/usr/bin/env python3
"""
Reset data lineage tables for fresh migration
"""

import duckdb
from pathlib import Path

def main():
    # Connect to database
    db_path = Path(__file__).parent.parent.parent.parent.parent / "04_processed_data" / "duckdb" / "databases" / "recovery_directory.duckdb"
    conn = duckdb.connect(str(db_path))
    
    print("🔄 Resetting data lineage tables...")
    
    # Drop views first
    views = [
        'certification_authority_summary',
        'organization_data_freshness', 
        'data_source_summary'
    ]
    
    for view in views:
        try:
            conn.execute(f"DROP VIEW IF EXISTS {view}")
            print(f"✓ Dropped view {view}")
        except:
            pass
    
    # Drop tables in reverse dependency order
    tables = [
        'organization_certifications_tracked',
        'certification_types',
        'certification_authorities',
        'organization_type_history',
        'organization_types',
        'data_confirmations',
        'organization_lineage',
        'extraction_runs',
        'data_sources'
    ]
    
    for table in tables:
        try:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
            print(f"✓ Dropped table {table}")
        except:
            pass
    
    # Drop sequences
    sequences = [
        'extraction_runs_id_seq',
        'organization_lineage_id_seq',
        'data_confirmations_id_seq',
        'organization_type_history_id_seq',
        'organization_certifications_tracked_id_seq'
    ]
    
    for seq in sequences:
        try:
            conn.execute(f"DROP SEQUENCE IF EXISTS {seq}")
            print(f"✓ Dropped sequence {seq}")
        except:
            pass
    
    conn.close()
    print("✅ Reset complete")

if __name__ == "__main__":
    main()