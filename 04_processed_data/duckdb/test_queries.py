#!/usr/bin/env python3
"""Test DuckDB queries and performance"""

import duckdb
import time
from pathlib import Path

# Connect to database
db_path = Path(__file__).parent / "databases" / "recovery_directory.duckdb"
conn = duckdb.connect(str(db_path))

print("=== DUCKDB QUERY TESTS ===\n")

# Test queries
queries = [
    ("Total Organizations", "SELECT COUNT(*) as total FROM organizations"),
    
    ("Organizations by Type", """
        SELECT organization_type, COUNT(*) as count 
        FROM organizations 
        GROUP BY organization_type 
        ORDER BY count DESC
    """),
    
    ("Top 10 States by Organization Count", """
        SELECT address_state, COUNT(*) as count 
        FROM organizations 
        WHERE address_state IS NOT NULL 
        GROUP BY address_state 
        ORDER BY count DESC 
        LIMIT 10
    """),
    
    ("California Peer Support Services", """
        SELECT COUNT(*) as count 
        FROM organizations 
        WHERE address_state = 'CA' 
        AND services LIKE '%peer support%'
    """),
    
    ("Data Quality Overview", """
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN has_complete_address THEN 1 ELSE 0 END) as complete_addresses,
            SUM(CASE WHEN has_contact_info THEN 1 ELSE 0 END) as has_contact,
            ROUND(AVG(data_quality_score), 3) as avg_quality_score
        FROM organizations
    """),
    
    ("NARR Certified Organizations", """
        SELECT 
            organization_type,
            COUNT(*) as total,
            SUM(CASE WHEN is_narr_certified THEN 1 ELSE 0 END) as narr_certified
        FROM organizations
        WHERE organization_type IN ('narr_residences', 'oxford_houses')
        GROUP BY organization_type
    """),
    
    ("Services Overview", """
        SELECT 
            service_name, 
            COUNT(*) as providers 
        FROM services s
        JOIN organization_services os ON s.id = os.service_id
        GROUP BY service_name
        ORDER BY providers DESC
    """),
    
    ("Geographic Coverage", """
        SELECT 
            COUNT(DISTINCT address_state) as states_covered,
            COUNT(DISTINCT address_city) as cities_covered,
            COUNT(*) as total_organizations
        FROM organizations
        WHERE address_state IS NOT NULL
    """)
]

# Execute queries and measure performance
for query_name, query in queries:
    print(f"\n{query_name}:")
    print("-" * 50)
    
    start_time = time.time()
    result = conn.execute(query).fetchall()
    elapsed_ms = (time.time() - start_time) * 1000
    
    # Display results
    if len(result) > 0 and len(result[0]) == 1:
        # Single value result
        print(f"Result: {result[0][0]:,}")
    else:
        # Table result
        for row in result[:20]:  # Limit display to 20 rows
            if len(row) == 2:
                print(f"  {row[0]}: {row[1]:,}")
            else:
                print(f"  {row}")
    
    print(f"\nQuery time: {elapsed_ms:.2f}ms")

# Test database size
print("\n\n=== DATABASE STATISTICS ===")
print("-" * 50)

# Get file size
file_size_mb = db_path.stat().st_size / (1024 * 1024)
print(f"Database file size: {file_size_mb:.2f} MB")

# Get table sizes
table_sizes = conn.execute("""
    SELECT table_name, estimated_size 
    FROM duckdb_tables() 
    WHERE database_name = 'main'
    ORDER BY estimated_size DESC
""").fetchall()

print("\nTable sizes:")
for table, size in table_sizes:
    print(f"  {table}: {size:,} bytes")

# Close connection
conn.close()

print("\n=== TESTS COMPLETE ===")