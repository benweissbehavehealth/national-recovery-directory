# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# National Recovery Support Services Directory Extractor

## Project Overview
This project extracts and structures data for 17,000+ recovery support organizations nationwide, creating a comprehensive directory for integration with the BehaveHealth platform. Current progress: 795 organizations extracted (4.7% of target).

## Architecture & Data Flow

### Multi-Phase Extraction Pipeline
1. **Phase 1**: NARR Affiliates (29 state organizations) ✅ COMPLETE
2. **Phase 2**: NARR Certified Organizations (404 extracted) ✅ COMPLETE
3. **Phase 3**: Recovery Community Centers (207 extracted) ✅ COMPLETE
4. **Phase 4**: Recovery Community Organizations (184 extracted) ✅ COMPLETE
5. **Phase 5**: Outpatient Treatment Centers (8,000+ target) 🔄 PENDING
6. **Phase 6**: Residential Treatment Centers (4,000+ target) 🔄 PENDING
7. **Phase 7**: Inpatient Treatment Centers (3,000+ target) 🔄 PENDING

### Data Organization Structure
```
03_raw_data/
├── narr_affiliates/          # Original 29 affiliates data
├── narr_organizations/       # State-by-state certified residences
│   └── [state]/             # e.g., massachusetts/, texas/, georgia/
├── recovery_centers/         # RCCs by funding source/region
│   ├── samhsa_funded/
│   ├── regional/            # northeast, southeast, midwest, etc.
│   └── national_networks/
└── recovery_organizations/   # RCOs by category
    ├── national_networks/
    ├── state_coalitions/
    ├── specialized/         # Population-specific RCOs
    └── regional/           # Multi-state networks

04_processed_data/master_directories/
├── master_directory.json                    # NARR organizations
├── recovery_community_centers_master.json   # RCCs
└── recovery_organizations_master.json       # RCOs
```

### ID System & Schema
- **NARR Organizations**: `ORG_0001` to `ORG_XXXX`
- **Recovery Community Centers**: `RCC_0001` to `RCC_XXXX`
- **Recovery Community Organizations**: `RCO_0001` to `RCO_XXXX`
- **Treatment Centers** (future): `TX_OUT_XXXX`, `TX_RES_XXXX`, `TX_INP_XXXX`

## Common Development Commands

### Aggregation Scripts
```bash
cd 02_scripts/aggregation

# Aggregate NARR certified organizations
python3 aggregate_results.py

# Aggregate Recovery Community Centers
python3 aggregate_rccs_v2.py

# Aggregate Recovery Community Organizations  
python3 aggregate_rcos.py
```

### Dependencies
```bash
pip install -r 02_scripts/utilities/requirements.txt
```

## Extraction Methodology

### Web-Research-Analyst Agent Usage
When launching agents:
1. **Always use 7-15 concurrent agents** for parallel processing
2. **Specify clear geographic/organizational boundaries** per agent
3. **Request structured JSON output** matching existing schemas
4. **Include extraction reports** with success metrics
5. **Save to appropriate `03_raw_data/` subdirectory**

### Common Extraction Patterns
```python
# Example agent prompt structure
TARGET: [specific URL or organization]
EXTRACTION REQUIREMENTS:
- Organization name and all DBAs
- Complete contact information
- Services and specializations
- Certification/licensing details
- Geographic coverage
OUTPUT: JSON file saved to 03_raw_data/[category]/[subcategory]/[filename].json
TARGET: Find [X]+ organizations
```

### Technical Barriers & Solutions
- **GetHelp SDK widgets** (Arizona): Requires Selenium automation
- **JavaScript search interfaces** (Colorado): Use browser automation scripts
- **Dynamic content** (Texas TROHN): Implement AJAX interception
- **Member-only directories**: Direct contact via standardized templates

## Data Extraction Scripts

### Key Extractors in `02_scripts/extraction/`
- `mash_extractor.py`: Massachusetts MASH API integration (204 homes)
- `trohn_*.py`: Texas TROHN various approaches (partial success)
- `azrha_*.py`: Arizona AzRHA attempts (GetHelp widget barrier)
- `tier1_michigan_marr_code.py`: Michigan MARR operator extraction

### Aggregation Scripts in `02_scripts/aggregation/`
- `aggregate_results.py`: Processes NARR organizations with path updates
- `aggregate_rccs_v2.py`: Handles multiple RCC JSON structures
- `aggregate_rcos.py`: Extracts from various RCO data formats

## Current Extraction Status

### Completed Extractions (41,091 total organizations)
- **NARR Residences**: 404 certified residences
- **Recovery Community Centers**: 207 peer-run centers
- **Recovery Community Organizations**: 184 advocacy groups
- **Treatment Centers**: 40,296 facilities
  - Outpatient: 33,505
  - Residential: 4,946
  - Inpatient: 1,845

### Data Sources Integrated
- Complete SAMHSA dataset (16,501 facilities added)
- State licensing databases  
- Massachusetts MASH API
- Manual extraction from state websites

### Pending Technical Solutions
- Texas TROHN: 48 residences need browser automation
- Arizona AzRHA: GetHelp widget blocking 1,678+ beds data
- Colorado CARR: JavaScript search preventing extraction

## Data Quality Standards

### Required Fields
- Organization name and type
- Geographic location (address/state minimum)
- Contact method (phone/email/website)
- Services/programs offered
- Target populations
- Certification/licensing status

### Validation Checks
- Duplicate detection across sources
- State abbreviation standardization
- Phone number format validation
- Geographic region assignment
- Multi-state organization handling

## Advanced Analytics & Network Analysis (Phase 8)

### Clustering Analysis System
Located in `02_scripts/analysis/network_clustering_framework.py`

**Purpose**: Identify affiliation networks and referral partnerships through:
- Geographic proximity clustering
- Service overlap analysis  
- Name similarity patterns (franchise detection)
- Funding source connections
- Leadership overlap identification
- Certification network mapping

### NARR vs Non-NARR Classification
**Critical Distinction**: Clear classification system to prevent confusion
- NARR certified residences use specific indicators
- Non-NARR certifications include Oxford House, GARR, FARR, state licenses
- Confidence scoring for classification accuracy
- Real-time classification updates

### Oxford House Data Pipeline
Located in `02_scripts/extraction/oxford_house_pipeline.py`

**Features**:
- Real-time vacancy extraction from oxfordvacancies.com
- Standardized data format matching our schema
- Automatic deduplication with existing records
- Occupancy rate calculations
- State-by-state systematic coverage

### Referral Network Mapping
**Scoring Algorithm** considers:
- Geographic proximity (same city/county bonus)
- Complementary services (detox → sober living)
- Organization type diversity (RCC → treatment center)
- Certification compatibility
- Historical referral patterns

**Visualization Goals**:
- Interactive network graphs
- Heat maps of referral density
- Service gap identification
- Partnership opportunity highlights

## Recurring Search System

### Parallel Agent Architecture
Located in `02_scripts/recurring_search/`

**Capabilities**:
- Launch 10+ concurrent web-research-analyst agents
- State-by-state systematic coverage
- Automatic duplicate detection
- Standardized JSON output
- Monthly update scheduling

**Search Targets**:
- Additional Recovery Community Centers
- New Recovery Community Organizations
- Non-NARR recovery residences
- Emerging recovery support services

### Integration Pipeline
1. Agent results saved to `03_raw_data/recurring_search_results/`
2. Duplicate detection against master directories
3. Human review for quality assurance
4. Integration into master directories
5. Network analysis update triggers

## Data Storage Architecture - DuckDB & MotherDuck

### Local Development - DuckDB
**Purpose**: High-performance analytical database for 41,091+ organizations

**Structure**:
```
04_processed_data/duckdb/
├── recovery_directory.duckdb      # Main database (~17MB)
├── schemas/                       # Table definitions
│   ├── 01_core_tables.sql
│   ├── 02_indexes.sql
│   └── 03_views.sql
├── scripts/                       # Management scripts
│   ├── json_to_duckdb.py         # Migration from JSON
│   ├── performance_tuner.py      # Query optimization
│   └── backup_manager.py         # Backup automation
└── exports/                      # Parquet/JSON exports
```

**Schema Design**:
- **organizations**: Core table with all organization data
- **services**: Normalized service offerings
- **certifications**: Certification tracking
- **networks**: Referral partnerships (19,180 relationships)
- **geographic_index**: Geohash-based location indexing

**Performance Targets**:
- Query response time: <100ms for 95% of queries
- Concurrent users: 50+
- Data load time: <5 seconds

### Production Cloud - MotherDuck
**Purpose**: Collaborative cloud analytics with zero infrastructure

**Features**:
- **Dual Execution**: Queries run locally and in cloud automatically
- **Team Collaboration**: Shared SQL workspace
- **API Integration**: REST endpoints via URL queries
- **Cost**: Free tier covers our dataset (10GB storage + 10 compute hours)

**Migration Path**:
```python
# Connect to MotherDuck
import duckdb
conn = duckdb.connect('md:recovery_directory')

# Sync local to cloud
conn.execute("COPY organizations TO 'md:recovery_directory.organizations'")
```

### Common Queries
```sql
-- Find organizations by state and service
SELECT * FROM organizations
WHERE state = 'CA' 
AND services @> '["peer support"]'
ORDER BY geohash;

-- Network analysis query
SELECT o1.name, o2.name, n.score
FROM networks n
JOIN organizations o1 ON n.org1_id = o1.id
JOIN organizations o2 ON n.org2_id = o2.id
WHERE n.score > 0.7;

-- Geographic clustering
SELECT state, city, COUNT(*) as org_count
FROM organizations
GROUP BY CUBE(state, city)
ORDER BY org_count DESC;
```

### Data Quality & Updates
- **Automated Validation**: Check required fields, formats
- **Duplicate Detection**: Cross-source deduplication
- **Monthly Refresh**: Oxford vacancies, new organizations
- **Version Control**: Daily backups to Parquet format