# 🦆 DuckDB Recovery Directory

## Quick Start

### 1. View Data with DuckDB UI (Recommended)
```bash
# Install DuckDB if needed
brew install duckdb  # macOS

# Launch the UI
./launch_ui.sh

# Or manually:
duckdb -ui databases/recovery_directory.duckdb
```

The UI will open at http://localhost:3000 with:
- Interactive SQL notebooks
- Schema browser
- Query results visualization

### 2. Use Beekeeper Studio (GUI)
Download from: https://www.beekeeperstudio.io/
- Connect to: `databases/recovery_directory.duckdb`
- Visual table editing
- Export to CSV/Excel/JSON

### 3. VS Code Extensions
Install:
- **DuckDB SQL Tools** by RandomFractalsInc
- **SQLTools DuckDB Driver** by Evidence

Then connect to the database file.

### 4. Python Access
```python
import duckdb
conn = duckdb.connect('databases/recovery_directory.duckdb')

# Query example
df = conn.execute("""
    SELECT address_state, COUNT(*) as count 
    FROM organizations 
    GROUP BY address_state 
    ORDER BY count DESC 
    LIMIT 10
""").fetchdf()

print(df)
```

## Database Contents

- **41,097 organizations** across 5 types
- **132 states/territories**
- **6,725 cities**
- **24.26 MB** file size

## Common Queries

### Organization Overview
```sql
-- Count by type
SELECT organization_type, COUNT(*) as count 
FROM organizations 
GROUP BY organization_type 
ORDER BY count DESC;

-- Top states
SELECT * FROM state_summary 
ORDER BY organization_count DESC 
LIMIT 10;

-- Data quality
SELECT * FROM data_quality_dashboard;
```

### Search Examples
```sql
-- Find California treatment centers
SELECT * FROM organizations 
WHERE organization_type = 'treatment_centers' 
AND address_state = 'CA'
LIMIT 100;

-- Find organizations with peer support
SELECT name, address_city, address_state, services
FROM organizations
WHERE services LIKE '%peer support%';

-- NARR certified residences
SELECT * FROM certification_summary
WHERE narr_certified > 0;
```

## Views Available

1. **state_summary** - Organizations by state and type
2. **treatment_center_summary** - Treatment facility analysis  
3. **certification_summary** - NARR vs non-NARR breakdown
4. **data_quality_dashboard** - Quality metrics by type
5. **recent_updates** - Update history

## Export Data

### To CSV
```sql
COPY (SELECT * FROM organizations WHERE address_state = 'CA') 
TO 'california_orgs.csv' (HEADER, DELIMITER ',');
```

### To Parquet
```sql
COPY organizations TO 'organizations.parquet' (FORMAT PARQUET);
```

### To JSON
```sql
COPY (SELECT * FROM state_summary) 
TO 'state_summary.json' (FORMAT JSON, ARRAY true);
```

## MotherDuck Cloud Sync

When ready for cloud deployment:
```python
import duckdb

# Connect to MotherDuck
cloud = duckdb.connect('md:recovery_directory?token=YOUR_TOKEN')

# Copy local to cloud
local = duckdb.connect('databases/recovery_directory.duckdb')
local.execute("ATTACH 'md:recovery_directory' AS cloud")
local.execute("CREATE TABLE cloud.organizations AS SELECT * FROM main.organizations")
```

---

For more information, see the [DuckDB documentation](https://duckdb.org/docs/)