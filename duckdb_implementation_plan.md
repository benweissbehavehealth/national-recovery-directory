# DuckDB Implementation Plan for National Recovery Support Services Directory

## Executive Summary
This plan outlines the implementation of a DuckDB-based data management system for the National Recovery Support Services Directory containing 41,091+ organizations. The design prioritizes performance, scalability, and seamless migration to MotherDuck cloud services.

## 1. Database Schema Design

### Core Tables Structure

#### 1.1 Organizations Master Table
```sql
CREATE TABLE organizations (
    -- Primary Identifiers
    org_id VARCHAR PRIMARY KEY,
    org_type VARCHAR NOT NULL CHECK (org_type IN ('narr_residence', 'recovery_center', 'recovery_organization', 'treatment_center')),
    
    -- Basic Information
    facility_name VARCHAR NOT NULL,
    dba_names VARCHAR[],
    
    -- Location Data
    address_line1 VARCHAR,
    address_line2 VARCHAR,
    city VARCHAR,
    state VARCHAR(2),
    zip_code VARCHAR(10),
    county VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE,
    
    -- Contact Information
    phone VARCHAR,
    fax VARCHAR,
    website VARCHAR,
    email VARCHAR,
    
    -- Metadata
    data_source VARCHAR,
    extraction_date TIMESTAMP,
    last_updated TIMESTAMP,
    
    -- Geospatial Index (for MotherDuck compatibility)
    geohash VARCHAR GENERATED ALWAYS AS (
        CASE 
            WHEN latitude IS NOT NULL AND longitude IS NOT NULL 
            THEN ST_GeoHash(ST_Point(longitude, latitude), 12)
            ELSE NULL 
        END
    ) STORED
);

-- Create indexes
CREATE INDEX idx_org_state ON organizations(state);
CREATE INDEX idx_org_type ON organizations(org_type);
CREATE INDEX idx_org_geohash ON organizations(geohash);
CREATE INDEX idx_org_city_state ON organizations(city, state);
```

#### 1.2 Treatment Centers Specific Table
```sql
CREATE TABLE treatment_centers (
    org_id VARCHAR PRIMARY KEY REFERENCES organizations(org_id),
    
    -- Licensing & Certification
    license_numbers VARCHAR[],
    state_license VARCHAR,
    federal_certification VARCHAR,
    accreditations VARCHAR[],
    
    -- Service Levels
    level_of_care VARCHAR CHECK (level_of_care IN ('outpatient', 'residential', 'inpatient')),
    
    -- Treatment Services (as separate boolean columns for performance)
    standard_outpatient BOOLEAN DEFAULT false,
    intensive_outpatient BOOLEAN DEFAULT false,
    partial_hospitalization BOOLEAN DEFAULT false,
    medication_assisted_treatment BOOLEAN DEFAULT false,
    opioid_treatment_program BOOLEAN DEFAULT false,
    
    -- Therapy Types
    individual_therapy BOOLEAN DEFAULT false,
    group_therapy BOOLEAN DEFAULT false,
    family_therapy BOOLEAN DEFAULT false,
    cognitive_behavioral_therapy BOOLEAN DEFAULT false,
    
    -- Demographics Served
    serves_adolescents BOOLEAN DEFAULT false,
    serves_adults BOOLEAN DEFAULT false,
    serves_seniors BOOLEAN DEFAULT false,
    minimum_age INTEGER,
    maximum_age INTEGER,
    
    -- Payment Options
    accepts_medicaid BOOLEAN DEFAULT false,
    accepts_medicare BOOLEAN DEFAULT false,
    accepts_private_insurance BOOLEAN DEFAULT false,
    accepts_cash_self_payment BOOLEAN DEFAULT false,
    sliding_fee_scale BOOLEAN DEFAULT false,
    
    -- Capacity Metrics
    outpatient_capacity INTEGER,
    residential_capacity INTEGER,
    inpatient_capacity INTEGER,
    current_census INTEGER,
    
    -- Quality Metrics
    quality_score DOUBLE,
    last_inspection_date DATE,
    accreditation_status VARCHAR
);

-- Create bitmap indexes for boolean fields
CREATE INDEX idx_tc_mat ON treatment_centers(medication_assisted_treatment);
CREATE INDEX idx_tc_medicaid ON treatment_centers(accepts_medicaid);
CREATE INDEX idx_tc_level ON treatment_centers(level_of_care);
```

#### 1.3 NARR Residences Table
```sql
CREATE TABLE narr_residences (
    org_id VARCHAR PRIMARY KEY REFERENCES organizations(org_id),
    
    -- NARR Specific
    certification_level VARCHAR,
    capacity INTEGER,
    affiliate_organization VARCHAR,
    affiliate_website VARCHAR,
    
    -- Specializations
    specializations VARCHAR[],
    gender_specific VARCHAR,
    
    -- Operations
    operating_since DATE,
    certification_date DATE,
    certification_expiry DATE
);
```

#### 1.4 Recovery Centers Table
```sql
CREATE TABLE recovery_centers (
    org_id VARCHAR PRIMARY KEY REFERENCES organizations(org_id),
    
    -- Center Information
    trade_names VARCHAR[],
    certification_status VARCHAR,
    
    -- Services Array
    services VARCHAR[],
    
    -- Funding & Operations
    funding_sources VARCHAR[],
    annual_budget DECIMAL(12,2),
    annual_reach INTEGER,
    
    -- Target Populations
    target_populations VARCHAR[],
    
    -- Hours & Leadership
    operating_hours STRUCT(
        monday VARCHAR,
        tuesday VARCHAR,
        wednesday VARCHAR,
        thursday VARCHAR,
        friday VARCHAR,
        saturday VARCHAR,
        sunday VARCHAR
    ),
    leadership VARCHAR,
    
    -- Social Media
    social_media STRUCT(
        facebook VARCHAR,
        twitter VARCHAR,
        instagram VARCHAR,
        linkedin VARCHAR
    )
);
```

#### 1.5 Service Codes Lookup Table
```sql
CREATE TABLE service_codes (
    code VARCHAR PRIMARY KEY,
    category VARCHAR NOT NULL,
    description VARCHAR NOT NULL,
    is_active BOOLEAN DEFAULT true
);

-- Junction table for many-to-many relationship
CREATE TABLE organization_services (
    org_id VARCHAR REFERENCES organizations(org_id),
    service_code VARCHAR REFERENCES service_codes(code),
    PRIMARY KEY (org_id, service_code)
);
```

#### 1.6 Network Analysis Table (Pre-computed)
```sql
CREATE TABLE network_relationships (
    org_id_1 VARCHAR REFERENCES organizations(org_id),
    org_id_2 VARCHAR REFERENCES organizations(org_id),
    relationship_type VARCHAR,
    distance_miles DOUBLE,
    travel_time_minutes INTEGER,
    strength_score DOUBLE,
    
    PRIMARY KEY (org_id_1, org_id_2, relationship_type)
);

CREATE INDEX idx_network_distance ON network_relationships(distance_miles);
```

## 2. Data Migration Strategy

### 2.1 Migration Pipeline Architecture
```python
# migration_pipeline.py structure
class MigrationPipeline:
    """
    ETL Pipeline for JSON to DuckDB migration
    - Parallel processing for large files
    - Data validation and cleansing
    - Incremental loading support
    - Error handling and rollback
    """
```

### 2.2 Migration Steps
1. **Phase 1: Schema Creation**
   - Create DuckDB database with all tables
   - Set up constraints and indexes
   
2. **Phase 2: Data Extraction & Transformation**
   - Parse JSON files in parallel
   - Normalize data structures
   - Validate data integrity
   - Generate unique IDs where missing

3. **Phase 3: Loading Strategy**
   - Use COPY INTO for bulk loading
   - Implement batch processing (10,000 records/batch)
   - Monitor memory usage
   - Create checkpoints for recovery

4. **Phase 4: Post-Migration**
   - Verify row counts
   - Run data quality checks
   - Build statistics
   - Create materialized views

## 3. Indexing & Optimization Strategy

### 3.1 Primary Indexes
- Geospatial: GeoHash-based indexing for proximity queries
- Service-based: Bitmap indexes for boolean service flags
- Text search: Full-text indexes on names and addresses
- Temporal: Date-based partitioning for historical data

### 3.2 Materialized Views for Common Queries
```sql
-- Geographic service availability
CREATE MATERIALIZED VIEW mv_services_by_location AS
SELECT 
    state, 
    city,
    org_type,
    COUNT(*) as facility_count,
    ARRAY_AGG(DISTINCT services) as available_services
FROM organizations o
LEFT JOIN organization_services os ON o.org_id = os.org_id
GROUP BY state, city, org_type;

-- Treatment center capacity by state
CREATE MATERIALIZED VIEW mv_treatment_capacity AS
SELECT 
    o.state,
    tc.level_of_care,
    COUNT(*) as facility_count,
    SUM(tc.outpatient_capacity) as total_outpatient_capacity,
    SUM(tc.residential_capacity) as total_residential_capacity,
    AVG(tc.quality_score) as avg_quality_score
FROM organizations o
JOIN treatment_centers tc ON o.org_id = tc.org_id
GROUP BY o.state, tc.level_of_care;
```

## 4. Partitioning Strategy

### 4.1 Partitioning Scheme
```sql
-- Partition treatment centers by state and level of care
CREATE TABLE treatment_centers_partitioned (
    LIKE treatment_centers INCLUDING ALL
) PARTITION BY (state, level_of_care);

-- Create partitions for high-volume states
CREATE TABLE treatment_centers_ca_outpatient PARTITION OF treatment_centers_partitioned
FOR VALUES IN ('CA', 'outpatient');

CREATE TABLE treatment_centers_ca_residential PARTITION OF treatment_centers_partitioned
FOR VALUES IN ('CA', 'residential');

-- Continue for other high-volume state/level combinations
```

### 4.2 Partition Maintenance
- Monthly partition statistics update
- Automatic partition pruning for queries
- Archive old partitions to Parquet files

## 5. Directory Structure

```
/narr_extractor/
├── duckdb/
│   ├── database/
│   │   ├── narr_directory.duckdb         # Main database file
│   │   └── narr_directory.wal            # Write-ahead log
│   │
│   ├── schemas/
│   │   ├── 01_core_tables.sql           # Core table definitions
│   │   ├── 02_indexes.sql               # Index definitions
│   │   ├── 03_views.sql                 # View definitions
│   │   └── 04_procedures.sql            # Stored procedures
│   │
│   ├── migrations/
│   │   ├── 001_initial_schema.sql       # Initial schema
│   │   ├── 002_add_network_table.sql    # Incremental changes
│   │   └── migration_log.json           # Migration history
│   │
│   ├── scripts/
│   │   ├── json_to_duckdb.py           # Main migration script
│   │   ├── data_validator.py           # Data validation
│   │   ├── performance_tuner.py        # Performance optimization
│   │   └── backup_manager.py           # Backup automation
│   │
│   ├── backups/
│   │   ├── daily/                      # Daily backups
│   │   ├── weekly/                     # Weekly backups
│   │   └── exports/                    # Parquet exports
│   │
│   └── config/
│       ├── duckdb_config.json          # Database configuration
│       ├── migration_config.yaml       # Migration settings
│       └── motherduck_config.json      # Cloud settings
```

## 6. MotherDuck Readiness Checklist

### 6.1 Schema Compatibility
- [x] Use standard SQL types (no custom types)
- [x] Avoid local file references in tables
- [x] Use cloud-compatible spatial functions
- [x] Implement proper key constraints

### 6.2 Data Format Requirements
- [x] UTF-8 encoding for all text
- [x] ISO 8601 date/time formats
- [x] Consistent NULL handling
- [x] No local file dependencies

### 6.3 Performance Considerations
- [x] Partition large tables (treatment_centers)
- [x] Create appropriate indexes
- [x] Use columnar storage benefits
- [x] Implement query result caching

### 6.4 Migration Prerequisites
- [ ] Create MotherDuck account
- [ ] Configure authentication tokens
- [ ] Set up data sync policies
- [ ] Test network bandwidth
- [ ] Plan migration window

### 6.5 Cloud-Specific Features
```sql
-- MotherDuck-specific configuration
ATTACH 'md:narr_directory' AS cloud_db;

-- Enable cloud features
SET enable_object_cache = true;
SET memory_limit = '8GB';
SET threads = 4;
```

## 7. Performance Benchmarks & Optimization

### 7.1 Expected Query Performance
```sql
-- Geographic search (< 50ms)
SELECT * FROM organizations 
WHERE ST_DWithin(
    ST_Point(longitude, latitude),
    ST_Point(-122.4194, 37.7749),
    10000  -- 10km radius
);

-- Service availability (< 100ms)
SELECT COUNT(*) FROM treatment_centers
WHERE accepts_medicaid = true
  AND medication_assisted_treatment = true
  AND state = 'CA';

-- Network analysis (< 200ms with materialized view)
SELECT * FROM network_relationships
WHERE org_id_1 = 'ORG_12345'
  AND distance_miles < 25
ORDER BY strength_score DESC;
```

### 7.2 Optimization Techniques
1. **Column Pruning**: Only select needed columns
2. **Predicate Pushdown**: Filter early in queries
3. **Join Optimization**: Use appropriate join strategies
4. **Batch Processing**: Process large updates in batches
5. **Connection Pooling**: Reuse database connections

### 7.3 Monitoring Queries
```sql
-- Table statistics
SELECT table_name, estimated_size, row_count
FROM duckdb_tables;

-- Query performance
SELECT query, execution_time, rows_returned
FROM duckdb_queries
ORDER BY execution_time DESC
LIMIT 10;
```

## 8. Backup & Version Control Strategy

### 8.1 Backup Schedule
- **Continuous**: WAL archiving to S3/cloud storage
- **Daily**: Full database export to Parquet
- **Weekly**: Complete backup with schema
- **Monthly**: Archive to cold storage

### 8.2 Version Control
```bash
# Git integration for schema changes
git add schemas/*.sql
git commit -m "feat: Add service indexing"
git tag -a v1.2.0 -m "Added service indexing"
```

### 8.3 Recovery Procedures
1. Point-in-time recovery using WAL
2. Full restore from Parquet files
3. Incremental restore for specific tables
4. Cross-region disaster recovery

## 9. Implementation Timeline

### Phase 1: Foundation (Week 1)
- Set up DuckDB environment
- Create initial schema
- Develop migration scripts

### Phase 2: Migration (Week 2)
- Migrate NARR residences (404 records)
- Migrate recovery centers (207 records)
- Migrate recovery organizations (184 records)

### Phase 3: Scale (Week 3)
- Migrate treatment centers (40,296 records)
- Implement partitioning
- Create indexes and views

### Phase 4: Optimization (Week 4)
- Performance tuning
- Create materialized views
- Network analysis pre-computation

### Phase 5: Cloud Prep (Week 5)
- MotherDuck setup
- Test cloud sync
- Performance validation

## 10. Success Metrics

### Performance KPIs
- Query response time < 100ms for 95% of queries
- Bulk insert rate > 50,000 records/second
- Database size < 500MB (compressed)
- Cloud sync time < 5 minutes

### Data Quality Metrics
- 100% referential integrity
- < 0.1% data validation errors
- Complete geographic coverage
- Accurate service mappings

## Conclusion

This implementation plan provides a robust foundation for migrating your National Recovery Support Services Directory to DuckDB. The design prioritizes performance, scalability, and cloud readiness while maintaining data integrity and query efficiency. The modular approach allows for incremental implementation and testing, ensuring a smooth transition to production use.