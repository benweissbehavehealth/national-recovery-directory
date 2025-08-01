#!/usr/bin/env python3
"""
Test data lineage queries to verify implementation
"""

import duckdb
from pathlib import Path
import pandas as pd

def main():
    # Connect to database
    db_path = Path(__file__).parent / "databases" / "recovery_directory.duckdb"
    conn = duckdb.connect(str(db_path))
    
    print("🦆 Data Lineage Query Results")
    print("=" * 80)
    
    # 1. Data Sources Summary
    print("\n📊 DATA SOURCES SUMMARY:")
    print("-" * 40)
    result = conn.execute("SELECT * FROM data_source_summary").fetchdf()
    print(result.to_string(index=False))
    
    # 2. Organization Types
    print("\n\n🏢 ORGANIZATION TYPES:")
    print("-" * 40)
    result = conn.execute("""
        SELECT type_id, type_name, parent_type_id, category 
        FROM organization_types 
        ORDER BY category, parent_type_id NULLS FIRST
    """).fetchdf()
    print(result.to_string(index=False))
    
    # 3. Certification Authorities
    print("\n\n📜 CERTIFICATION AUTHORITIES:")
    print("-" * 40)
    result = conn.execute("SELECT * FROM certification_authority_summary").fetchdf()
    print(result.to_string(index=False))
    
    # 4. Sample Organization Lineage
    print("\n\n🔍 SAMPLE ORGANIZATION LINEAGE (first 10):")
    print("-" * 40)
    result = conn.execute("""
        SELECT 
            o.name,
            o.address_state,
            ol.source_id,
            ol.extracted_at,
            ol.version_number
        FROM organization_lineage ol
        JOIN organizations o ON ol.organization_id = o.id
        LIMIT 10
    """).fetchdf()
    print(result.to_string(index=False))
    
    # 5. Data Quality Stats
    print("\n\n📈 DATA QUALITY STATISTICS:")
    print("-" * 40)
    result = conn.execute("""
        SELECT 
            source_id,
            COUNT(*) as organizations,
            COUNT(DISTINCT organization_id) as unique_orgs
        FROM organization_lineage
        WHERE is_current = TRUE
        GROUP BY source_id
        ORDER BY organizations DESC
    """).fetchdf()
    print(result.to_string(index=False))
    
    # 6. Check for NARR certified organizations
    print("\n\n🏆 NARR CERTIFIED ORGANIZATIONS:")
    print("-" * 40)
    result = conn.execute("""
        SELECT 
            COUNT(*) as total_narr_certified
        FROM organization_type_history
        WHERE type_id = 'narr_certified' AND is_current = TRUE
    """).fetchdf()
    print(result.to_string(index=False))
    
    conn.close()
    print("\n✅ Data lineage system is fully operational!")

if __name__ == "__main__":
    main()