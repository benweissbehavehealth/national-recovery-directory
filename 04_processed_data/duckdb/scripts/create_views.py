#!/usr/bin/env python3
"""Create analytical views for DuckDB"""

import duckdb
from pathlib import Path

# Connect to database
db_path = Path(__file__).parent.parent / "databases" / "recovery_directory.duckdb"
conn = duckdb.connect(str(db_path))

print("Creating analytical views...")

# Skip complex JSON view for now - DuckDB has different JSON syntax
print("Note: Skipping service_coverage view due to JSON complexity")

# Create the other views from the schema file
views_sql = """
-- State-level Organization Summary
CREATE OR REPLACE VIEW state_summary AS
SELECT 
    address_state,
    organization_type,
    COUNT(*) as organization_count,
    COUNT(DISTINCT address_city) as city_count,
    SUM(CASE WHEN is_narr_certified THEN 1 ELSE 0 END) as narr_certified_count,
    SUM(CASE WHEN has_complete_address THEN 1 ELSE 0 END) as complete_address_count,
    SUM(CASE WHEN has_contact_info THEN 1 ELSE 0 END) as has_contact_count,
    AVG(data_quality_score) as avg_quality_score
FROM organizations
WHERE address_state IS NOT NULL
GROUP BY address_state, organization_type
ORDER BY address_state, organization_count DESC;

-- Treatment Center Analysis
CREATE OR REPLACE VIEW treatment_center_summary AS
SELECT 
    address_state,
    level_of_care,
    facility_type,
    COUNT(*) as facility_count,
    COUNT(DISTINCT address_city) as city_coverage,
    COUNT(DISTINCT operator) as unique_operators
FROM organizations
WHERE organization_type = 'treatment_centers'
GROUP BY address_state, level_of_care, facility_type
ORDER BY address_state, facility_count DESC;

-- NARR vs Non-NARR Classification Summary
CREATE OR REPLACE VIEW certification_summary AS
SELECT 
    address_state,
    organization_type,
    COUNT(*) as total_orgs,
    SUM(CASE WHEN is_narr_certified THEN 1 ELSE 0 END) as narr_certified,
    SUM(CASE WHEN certification_type = 'oxford_house' THEN 1 ELSE 0 END) as oxford_houses,
    SUM(CASE WHEN certification_type NOT IN ('narr', 'oxford_house') OR certification_type IS NULL THEN 1 ELSE 0 END) as other_or_uncertified,
    ROUND(100.0 * SUM(CASE WHEN is_narr_certified THEN 1 ELSE 0 END) / COUNT(*), 2) as narr_percentage
FROM organizations
WHERE organization_type IN ('narr_residences', 'oxford_houses')
GROUP BY address_state, organization_type
ORDER BY address_state, total_orgs DESC;

-- Data Quality Dashboard
CREATE OR REPLACE VIEW data_quality_dashboard AS
SELECT 
    organization_type,
    COUNT(*) as total_records,
    SUM(CASE WHEN name IS NOT NULL AND name != '' THEN 1 ELSE 0 END) as has_name,
    SUM(CASE WHEN has_complete_address THEN 1 ELSE 0 END) as complete_address,
    SUM(CASE WHEN has_contact_info THEN 1 ELSE 0 END) as has_contact,
    SUM(CASE WHEN services IS NOT NULL AND json_array_length(services) > 0 THEN 1 ELSE 0 END) as has_services,
    AVG(data_quality_score) as avg_quality_score,
    ROUND(100.0 * SUM(CASE WHEN has_complete_address THEN 1 ELSE 0 END) / COUNT(*), 2) as address_completeness_pct,
    ROUND(100.0 * SUM(CASE WHEN has_contact_info THEN 1 ELSE 0 END) / COUNT(*), 2) as contact_completeness_pct
FROM organizations
GROUP BY organization_type
ORDER BY total_records DESC;

-- Recent Updates Summary
CREATE OR REPLACE VIEW recent_updates AS
SELECT 
    update_date,
    update_type,
    source,
    records_added,
    records_updated,
    records_deleted,
    status,
    duration_seconds,
    CASE 
        WHEN status = 'success' THEN '✅'
        WHEN status = 'partial' THEN '⚠️'
        ELSE '❌'
    END as status_icon
FROM update_history
ORDER BY update_date DESC
LIMIT 20;
"""

# Execute view creation
for statement in views_sql.split(';'):
    if statement.strip():
        try:
            conn.execute(statement)
            print("✓ Created view successfully")
        except Exception as e:
            print(f"✗ Error creating view: {e}")

# Test the views
print("\n=== TESTING VIEWS ===")

test_queries = [
    ("State Summary (Top 5)", "SELECT * FROM state_summary LIMIT 5"),
    ("Service Coverage Sample", "SELECT * FROM service_coverage WHERE address_state = 'CA' LIMIT 5"),
    ("Data Quality by Type", "SELECT * FROM data_quality_dashboard"),
    ("Certification Summary", "SELECT * FROM certification_summary WHERE address_state IN ('CA', 'TX', 'FL') LIMIT 10")
]

for view_name, query in test_queries:
    print(f"\n{view_name}:")
    print("-" * 50)
    try:
        result = conn.execute(query).fetchdf()
        print(result.to_string(index=False, max_rows=10))
    except Exception as e:
        print(f"Error: {e}")

conn.close()
print("\n✅ Views created successfully!")