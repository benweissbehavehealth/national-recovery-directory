# 🦆 MotherDuck Migration Guide

## Overview

This guide will help you push your local DuckDB database (41,097 organizations) to MotherDuck cloud for:
- **Cloud access** from anywhere
- **Team collaboration** 
- **Automatic backups**
- **Scalable performance**
- **Cost-effective storage** (~$25/month estimated)

## Prerequisites

1. **MotherDuck Account**: Sign up at https://motherduck.com
2. **Authentication Token**: Get your token from MotherDuck dashboard
3. **Local Database**: Your `recovery_directory.duckdb` (24.26 MB)

## Quick Migration

### Option 1: Automated Script (Recommended)

```bash
# Navigate to DuckDB directory
cd /Users/benweiss/Code/narr_extractor/04_processed_data/duckdb

# Run the migration launcher
./push_to_motherduck.sh
```

The script will:
- ✅ Check prerequisites
- ✅ Detect if DuckDB UI is running
- ✅ Migrate all tables and views
- ✅ Verify data integrity
- ✅ Generate migration report

### Option 2: Manual Migration

```bash
# Set your MotherDuck token
export MOTHERDUCK_TOKEN="your_token_here"

# Run migration script
python3 scripts/migration/push_to_motherduck.py
```

### Option 3: Direct Command Line

```bash
# Connect to MotherDuck
duckdb md:narr_directory?motherduck_token=YOUR_TOKEN

# In DuckDB CLI:
ATTACH 'databases/recovery_directory.duckdb' AS local;
CREATE TABLE organizations AS SELECT * FROM local.organizations;
CREATE TABLE data_lineage AS SELECT * FROM local.data_lineage;
# ... repeat for all tables
```

## What Gets Migrated

### Tables (41,097 organizations)
- `organizations` - Main organization data
- `data_lineage` - Source tracking and lineage
- `certification_authorities` - NARR affiliates
- `organization_types` - Type classifications
- `data_sources` - Source systems
- `temporal_tracking` - Update history

### Views
- `state_summary` - Organizations by state
- `certification_summary` - NARR vs non-NARR
- `data_quality_dashboard` - Quality metrics
- `recent_updates` - Recent changes

## Post-Migration

### 1. Verify Migration
```sql
-- Check total organizations
SELECT COUNT(*) FROM organizations;

-- Verify data lineage
SELECT * FROM data_lineage_summary;

-- Test sample queries
SELECT organization_type, COUNT(*) 
FROM organizations 
GROUP BY organization_type;
```

### 2. Update Connection Strings

**Python:**
```python
import duckdb
conn = duckdb.connect('md:narr_directory?motherduck_token=YOUR_TOKEN')
```

**CLI:**
```bash
duckdb md:narr_directory?motherduck_token=YOUR_TOKEN
```

**DBeaver/Beekeeper:**
- Driver: DuckDB
- URL: `md:narr_directory`
- Properties: `motherduck_token=YOUR_TOKEN`

### 3. Configure Sync (Optional)

For ongoing synchronization:
```python
# Attach both databases
local = duckdb.connect('databases/recovery_directory.duckdb')
cloud = duckdb.connect('md:narr_directory?motherduck_token=YOUR_TOKEN')

# Sync changes
local.execute("ATTACH 'md:narr_directory' AS cloud")
local.execute("CREATE TABLE cloud.new_data AS SELECT * FROM main.recent_updates")
```

## Cost Estimation

Based on your 41,097 organizations:
- **Storage**: ~0.5 GB = $0.025/month
- **Compute**: ~100 hours/month = $25.00/month
- **Total**: ~$25.03/month

## Troubleshooting

### Database Locked Error
```bash
# Stop DuckDB UI first
pkill -f "keep_ui_alive.py"
# or close the terminal running the UI
```

### Authentication Failed
- Verify token is correct
- Check MotherDuck account status
- Ensure token has proper permissions

### Migration Fails
- Check network connectivity
- Verify local database integrity
- Review migration logs in `scripts/migration/`

### Partial Migration
- Run verification step
- Check which tables failed
- Re-run migration for specific tables

## Security Best Practices

1. **Token Management**
   - Store token in environment variables
   - Never commit tokens to version control
   - Rotate tokens regularly

2. **Access Control**
   - Use MotherDuck's built-in access controls
   - Limit database sharing to necessary users
   - Monitor access logs

3. **Data Protection**
   - Enable encryption at rest
   - Use SSL connections
   - Regular security audits

## Performance Optimization

1. **Query Optimization**
   - Use appropriate indexes
   - Partition large tables
   - Cache frequently used queries

2. **Cost Management**
   - Monitor query usage
   - Use auto-suspend features
   - Archive old data

## Support

- **MotherDuck Docs**: https://motherduck.com/docs
- **Migration Logs**: Check `scripts/migration/migration_report_*.txt`
- **DuckDB Community**: https://duckdb.org/community

---

**Next Steps**: After migration, update your applications to use the cloud database and enjoy the benefits of cloud-based analytics! 