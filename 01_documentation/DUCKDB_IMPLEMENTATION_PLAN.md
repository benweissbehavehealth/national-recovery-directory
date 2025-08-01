# 🦆 DuckDB Implementation Plan - National Recovery Support Services Directory

**Date**: July 31, 2025  
**Total Organizations**: 41,091+  
**Current Storage**: 70MB JSON  
**Target Storage**: ~17MB DuckDB  

---

## 📋 EXECUTIVE SUMMARY

This plan outlines the migration from JSON-based storage to DuckDB for the National Recovery Support Services Directory, preparing for future MotherDuck cloud deployment. The implementation will consolidate 41,091+ organizations into a high-performance analytical database optimized for BehaveHealth platform integration.

---

## 🎯 PROJECT GOALS

1. **Consolidate** all organization data into single DuckDB database
2. **Optimize** query performance to <100ms for 95% of queries
3. **Enable** complex network analysis and geographic clustering
4. **Prepare** for seamless MotherDuck cloud migration
5. **Maintain** data quality and integrity during migration
6. **Establish** automated update and backup processes

---

## 🏗️ IMPLEMENTATION PHASES

### PHASE 1: Database Design & Schema Creation (Day 1)

#### 1.1 Create Directory Structure
```bash
04_processed_data/duckdb/
├── databases/
│   ├── recovery_directory.duckdb     # Main production database
│   └── recovery_directory_dev.duckdb # Development database
├── schemas/
│   ├── 01_core_tables.sql           # Table definitions
│   ├── 02_indexes.sql               # Performance indexes
│   ├── 03_views.sql                 # Analytical views
│   └── 04_constraints.sql           # Data integrity rules
├── scripts/
│   ├── migration/
│   │   ├── json_to_duckdb.py       # Main migration script
│   │   └── validate_migration.py    # Data validation
│   ├── maintenance/
│   │   ├── backup_manager.py        # Automated backups
│   │   └── update_scheduler.py      # Monthly updates
│   └── analytics/
│       ├── performance_tuner.py     # Query optimization
│       └── network_analyzer.py      # Referral analysis
└── exports/
    ├── parquet/                     # Parquet format exports
    ├── json/                        # JSON format exports
    └── motherduck/                  # Cloud-ready exports
```

#### 1.2 Core Schema Design

```sql
-- Main Organizations Table
CREATE TABLE organizations (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    organization_type VARCHAR NOT NULL,
    
    -- Address fields
    address_street VARCHAR,
    address_city VARCHAR,
    address_state VARCHAR(2),
    address_zip VARCHAR(10),
    latitude DOUBLE,
    longitude DOUBLE,
    geohash VARCHAR(12),
    
    -- Contact fields
    phone VARCHAR,
    email VARCHAR,
    website VARCHAR,
    
    -- Service data (using JSON for flexibility)
    services JSON,
    certifications JSON,
    demographics JSON,
    capacity JSON,
    
    -- Metadata
    data_source VARCHAR,
    extraction_date DATE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Classification
    is_narr_certified BOOLEAN DEFAULT FALSE,
    certification_type VARCHAR,
    
    -- Network analysis
    operator VARCHAR,
    funding_sources JSON
);

-- Services Lookup Table
CREATE TABLE services (
    id INTEGER PRIMARY KEY,
    service_name VARCHAR UNIQUE NOT NULL,
    service_category VARCHAR,
    description TEXT
);

-- Organization Services Junction
CREATE TABLE organization_services (
    organization_id VARCHAR,
    service_id INTEGER,
    PRIMARY KEY (organization_id, service_id),
    FOREIGN KEY (organization_id) REFERENCES organizations(id),
    FOREIGN KEY (service_id) REFERENCES services(id)
);

-- Network Relationships
CREATE TABLE networks (
    id INTEGER PRIMARY KEY,
    org1_id VARCHAR NOT NULL,
    org2_id VARCHAR NOT NULL,
    relationship_type VARCHAR,
    score DOUBLE,
    factors JSON,
    FOREIGN KEY (org1_id) REFERENCES organizations(id),
    FOREIGN KEY (org2_id) REFERENCES organizations(id)
);

-- Geographic Clusters
CREATE TABLE geographic_clusters (
    cluster_id VARCHAR PRIMARY KEY,
    cluster_name VARCHAR,
    state VARCHAR(2),
    city VARCHAR,
    centroid_lat DOUBLE,
    centroid_lon DOUBLE,
    organization_count INTEGER
);
```

### PHASE 2: Data Migration (Day 2-3)

#### 2.1 Migration Script Components

```python
# json_to_duckdb.py - Key components

import duckdb
import json
import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
import logging

class DuckDBMigrator:
    def __init__(self, db_path: str):
        self.conn = duckdb.connect(db_path)
        self.setup_logging()
        
    def migrate_all_sources(self):
        """Main migration orchestrator"""
        sources = {
            'narr_residences': 'master_directory.json',
            'rccs': 'recovery_community_centers_master.json',
            'rcos': 'recovery_organizations_master.json',
            'treatment_centers': 'treatment_centers_master.json'
        }
        
        for org_type, filename in sources.items():
            self.migrate_source(org_type, filename)
            
    def migrate_source(self, org_type: str, filename: str):
        """Migrate single data source"""
        # Load JSON data
        data = self.load_json_data(filename)
        
        # Transform to DataFrame
        df = self.transform_to_dataframe(data, org_type)
        
        # Batch insert with progress tracking
        self.batch_insert(df, batch_size=10000)
        
        # Create indexes after bulk load
        self.create_indexes()
        
    def create_geospatial_index(self):
        """Create geohash index for location queries"""
        self.conn.execute("""
            UPDATE organizations 
            SET geohash = encode_geohash(latitude, longitude, 8)
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """)
```

#### 2.2 Data Validation Framework

```python
# validate_migration.py - Key validation checks

def validate_migration(conn):
    """Comprehensive validation of migrated data"""
    
    checks = {
        'total_count': validate_record_counts,
        'data_integrity': validate_required_fields,
        'geographic_coverage': validate_state_distribution,
        'service_consistency': validate_service_data,
        'network_integrity': validate_relationships
    }
    
    results = {}
    for check_name, check_func in checks.items():
        results[check_name] = check_func(conn)
        
    return results

def validate_record_counts(conn):
    """Ensure all records migrated"""
    expected = {
        'narr_residences': 404,
        'rccs': 207,
        'rcos': 184,
        'treatment_centers': 40296
    }
    
    actual = conn.execute("""
        SELECT organization_type, COUNT(*) as count
        FROM organizations
        GROUP BY organization_type
    """).fetchdf()
    
    return compare_counts(expected, actual)
```

### PHASE 3: Performance Optimization (Day 4)

#### 3.1 Indexing Strategy

```sql
-- Primary indexes for common queries
CREATE INDEX idx_state ON organizations(address_state);
CREATE INDEX idx_city_state ON organizations(address_state, address_city);
CREATE INDEX idx_org_type ON organizations(organization_type);
CREATE INDEX idx_geohash ON organizations(geohash);

-- Service search optimization
CREATE INDEX idx_services_gin ON organizations USING GIN(services);

-- Network analysis indexes
CREATE INDEX idx_network_org1 ON networks(org1_id);
CREATE INDEX idx_network_org2 ON networks(org2_id);
CREATE INDEX idx_network_score ON networks(score DESC);

-- Certification tracking
CREATE INDEX idx_narr_certified ON organizations(is_narr_certified) 
WHERE is_narr_certified = true;
```

#### 3.2 Materialized Views for Analytics

```sql
-- State-level aggregations
CREATE MATERIALIZED VIEW state_summary AS
SELECT 
    address_state,
    organization_type,
    COUNT(*) as org_count,
    COUNT(DISTINCT address_city) as city_count,
    SUM(CASE WHEN is_narr_certified THEN 1 ELSE 0 END) as narr_certified_count
FROM organizations
GROUP BY address_state, organization_type;

-- Service availability view
CREATE MATERIALIZED VIEW service_coverage AS
SELECT 
    s.service_name,
    o.address_state,
    COUNT(DISTINCT o.id) as provider_count
FROM organizations o
CROSS JOIN LATERAL json_array_elements_text(o.services) as service
JOIN services s ON s.service_name = service
GROUP BY s.service_name, o.address_state;

-- Network density view
CREATE MATERIALIZED VIEW network_density AS
SELECT 
    o.address_state,
    o.address_city,
    COUNT(DISTINCT n.id) as connection_count,
    AVG(n.score) as avg_connection_strength
FROM organizations o
JOIN networks n ON o.id IN (n.org1_id, n.org2_id)
GROUP BY o.address_state, o.address_city;
```

### PHASE 4: MotherDuck Preparation (Day 5)

#### 4.1 Cloud Compatibility Checklist

- [x] No custom types or extensions specific to local DuckDB
- [x] All data types compatible with MotherDuck
- [x] Proper UTF-8 encoding for all text fields
- [x] No local file dependencies in queries
- [x] Indexes designed for distributed queries
- [x] Partition-friendly schema design

#### 4.2 MotherDuck Migration Script

```python
# motherduck_sync.py

import duckdb
import os
from datetime import datetime

def sync_to_motherduck():
    """Sync local DuckDB to MotherDuck cloud"""
    
    # Connect to local database
    local_conn = duckdb.connect('recovery_directory.duckdb')
    
    # Connect to MotherDuck
    md_token = os.environ.get('MOTHERDUCK_TOKEN')
    cloud_conn = duckdb.connect(f'md:recovery_directory?token={md_token}')
    
    # Export tables to Parquet for efficient transfer
    tables = ['organizations', 'services', 'networks', 'geographic_clusters']
    
    for table in tables:
        print(f"Syncing {table} to MotherDuck...")
        
        # Export to Parquet
        local_conn.execute(f"""
            COPY {table} TO 'temp_{table}.parquet' (FORMAT PARQUET)
        """)
        
        # Import to MotherDuck
        cloud_conn.execute(f"""
            CREATE OR REPLACE TABLE {table} AS 
            SELECT * FROM read_parquet('temp_{table}.parquet')
        """)
        
    # Recreate indexes in cloud
    create_cloud_indexes(cloud_conn)
    
    print(f"Sync completed at {datetime.now()}")
```

### PHASE 5: Testing & Validation (Day 6)

#### 5.1 Performance Benchmarks

```python
# performance_benchmarks.py

benchmark_queries = {
    'geographic_search': """
        SELECT * FROM organizations 
        WHERE address_state = 'CA' 
        AND services @> '["peer support"]'
        LIMIT 100
    """,
    
    'network_analysis': """
        SELECT o1.name, o2.name, n.score
        FROM networks n
        JOIN organizations o1 ON n.org1_id = o1.id
        JOIN organizations o2 ON n.org2_id = o2.id
        WHERE n.score > 0.7
        ORDER BY n.score DESC
        LIMIT 50
    """,
    
    'aggregation_query': """
        SELECT 
            address_state,
            organization_type,
            COUNT(*) as count,
            COUNT(DISTINCT json_extract(services, '$[*]')) as service_variety
        FROM organizations
        GROUP BY address_state, organization_type
        ORDER BY count DESC
    """
}

def run_benchmarks(conn):
    """Execute benchmark queries and measure performance"""
    results = {}
    
    for query_name, query in benchmark_queries.items():
        start = time.time()
        conn.execute(query).fetchall()
        elapsed = (time.time() - start) * 1000  # milliseconds
        
        results[query_name] = {
            'time_ms': elapsed,
            'passes_target': elapsed < 100  # 100ms target
        }
        
    return results
```

#### 5.2 Data Quality Checks

```sql
-- Completeness checks
SELECT 
    COUNT(*) as total,
    COUNT(name) as has_name,
    COUNT(address_state) as has_state,
    COUNT(phone) as has_phone,
    COUNT(services) as has_services
FROM organizations;

-- Duplicate detection
SELECT name, address_street, COUNT(*) as duplicate_count
FROM organizations
GROUP BY name, address_street
HAVING COUNT(*) > 1;

-- Network integrity
SELECT COUNT(*) as orphaned_relationships
FROM networks n
WHERE NOT EXISTS (SELECT 1 FROM organizations o WHERE o.id = n.org1_id)
   OR NOT EXISTS (SELECT 1 FROM organizations o WHERE o.id = n.org2_id);
```

### PHASE 6: Production Deployment (Day 7)

#### 6.1 Backup Strategy

```python
# backup_manager.py - Automated backup system

class BackupManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.backup_dir = Path('backups')
        self.backup_dir.mkdir(exist_ok=True)
        
    def create_backup(self, backup_type: str = 'daily'):
        """Create timestamped backup"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_name = f"recovery_directory_{backup_type}_{timestamp}"
        
        # Create Parquet backup (efficient compression)
        conn = duckdb.connect(self.db_path)
        conn.execute(f"""
            EXPORT DATABASE '{self.backup_dir}/{backup_name}' (
                FORMAT PARQUET,
                COMPRESSION ZSTD
            )
        """)
        
        # Create metadata file
        self.save_backup_metadata(backup_name)
        
        # Clean old backups based on retention policy
        self.cleanup_old_backups(backup_type)
        
    def restore_backup(self, backup_name: str):
        """Restore from backup"""
        conn = duckdb.connect(self.db_path)
        conn.execute(f"""
            IMPORT DATABASE '{self.backup_dir}/{backup_name}'
        """)
```

#### 6.2 Update Scheduler

```python
# update_scheduler.py - Monthly update automation

class UpdateScheduler:
    def __init__(self, db_conn):
        self.conn = db_conn
        self.update_sources = {
            'oxford_vacancies': self.update_oxford_houses,
            'samhsa_quarterly': self.update_samhsa_data,
            'network_analysis': self.update_network_scores
        }
        
    def run_monthly_updates(self):
        """Execute all monthly update tasks"""
        results = {}
        
        for source_name, update_func in self.update_sources.items():
            try:
                count = update_func()
                results[source_name] = {'status': 'success', 'records': count}
            except Exception as e:
                results[source_name] = {'status': 'error', 'message': str(e)}
                
        # Log results
        self.log_update_results(results)
        
        # Refresh materialized views
        self.refresh_materialized_views()
        
        # Create backup after updates
        BackupManager(self.conn).create_backup('post_update')
```

---

## 📊 PERFORMANCE TARGETS & METRICS

### Query Performance SLAs
- **Simple lookups**: <10ms (ID-based queries)
- **Geographic searches**: <50ms (state/city filtering)
- **Service matching**: <100ms (JSON array searches)
- **Network analysis**: <200ms (multi-table joins)
- **Aggregations**: <500ms (full table scans)

### Storage Efficiency
- **Current JSON**: 70MB
- **Target DuckDB**: 17MB (75% reduction)
- **Parquet backups**: 10MB (85% compression)
- **Query cache**: 5MB (common results)

### Scalability Metrics
- **Concurrent users**: 50+ simultaneous queries
- **Write throughput**: 10,000 records/second
- **Read throughput**: 100,000 records/second
- **Network queries**: 1M+ relationship calculations/minute

---

## 🚀 MOTHERDUCK READINESS

### Pre-Migration Checklist
- [ ] Create MotherDuck account
- [ ] Generate API tokens
- [ ] Test small dataset sync
- [ ] Validate query compatibility
- [ ] Set up monitoring
- [ ] Configure backup sync
- [ ] Test dual execution mode

### Migration Commands
```bash
# Initial sync to MotherDuck
python scripts/motherduck_sync.py --initial

# Enable dual execution
export MOTHERDUCK_TOKEN=your_token
duckdb -c "ATTACH 'md:recovery_directory' AS cloud"

# Query both local and cloud
duckdb -c "SELECT * FROM local.organizations 
           UNION ALL 
           SELECT * FROM cloud.organizations"
```

### Cost Estimates
- **Storage**: ~$0.05/month (17MB compressed)
- **Compute**: ~$0.50/month (estimated query volume)
- **Total**: Well within free tier limits

---

## 📝 MAINTENANCE & OPERATIONS

### Daily Tasks
- Monitor query performance
- Check data quality alerts
- Review error logs

### Weekly Tasks
- Run data validation suite
- Update materialized views
- Performance tuning review

### Monthly Tasks
- Execute update pipelines
- Refresh Oxford House data
- Network analysis recalculation
- Create monthly backup

### Quarterly Tasks
- Update SAMHSA dataset
- Schema optimization review
- MotherDuck sync validation

---

## 🎯 SUCCESS CRITERIA

1. **Migration Complete**: All 41,091 organizations in DuckDB
2. **Performance Met**: 95% of queries under 100ms
3. **Quality Assured**: Zero data loss, 100% validation pass
4. **Cloud Ready**: Successful MotherDuck test sync
5. **Automated**: Update and backup processes scheduled
6. **Documented**: Complete operational runbooks

---

## 🚨 RISK MITIGATION

### Technical Risks
- **Data Loss**: Mitigated by comprehensive backups
- **Performance Issues**: Addressed by indexing strategy
- **Cloud Migration**: Tested with staging environment

### Operational Risks
- **Update Failures**: Automated rollback procedures
- **Query Bottlenecks**: Performance monitoring alerts
- **Schema Changes**: Version control and migration scripts

---

## 📅 TIMELINE SUMMARY

- **Day 1**: Schema design and structure creation
- **Day 2-3**: Data migration and validation
- **Day 4**: Performance optimization
- **Day 5**: MotherDuck preparation
- **Day 6**: Testing and benchmarking
- **Day 7**: Production deployment

**Total Duration**: 7 days from start to production-ready DuckDB implementation

---

*This plan ensures a smooth transition from JSON to DuckDB while maintaining data integrity, optimizing performance, and preparing for seamless MotherDuck cloud deployment.*