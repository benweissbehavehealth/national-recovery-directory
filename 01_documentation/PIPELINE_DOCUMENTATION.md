# Pipeline Documentation - National Recovery Support Services Directory

## Overview
This document provides comprehensive documentation of all data extraction and processing pipelines in the National Recovery Support Services Directory project. It includes schema definitions, data flow diagrams, and storage locations for each pipeline.

## Table of Contents
1. [NARR Organizations Pipeline](#narr-organizations-pipeline)
2. [Recovery Community Centers (RCC) Pipeline](#recovery-community-centers-pipeline)
3. [Recovery Community Organizations (RCO) Pipeline](#recovery-community-organizations-pipeline)
4. [Treatment Centers Pipeline](#treatment-centers-pipeline)
5. [Oxford House Pipeline](#oxford-house-pipeline)
6. [Recurring Search Pipeline](#recurring-search-pipeline)
7. [Network Analysis Pipeline](#network-analysis-pipeline)

---

## 1. NARR Organizations Pipeline

### Purpose
Extracts and aggregates NARR-certified recovery residences from state affiliates.

### Data Sources
- State NARR affiliate websites
- APIs (Massachusetts MASH)
- Manual extraction from member directories

### Schema
```json
{
  "id": "ORG_0001",  // Unique identifier
  "name": "Organization Name",
  "dba_names": ["Alternative Name 1", "Alternative Name 2"],
  "narr_certified": true,
  "certification_level": "Level II",
  "address": {
    "street": "123 Recovery St",
    "city": "Boston",
    "state": "MA",
    "zip": "02101"
  },
  "contact": {
    "phone": "(555) 123-4567",
    "email": "contact@recovery.org",
    "website": "https://recovery.org"
  },
  "capacity": {
    "total_beds": 12,
    "gender": "Men"
  },
  "services": ["Peer support", "Recovery coaching"],
  "operator": "Operating Organization Name",
  "data_source": "Massachusetts MASH API",
  "extraction_date": "2025-07-31"
}
```

### Storage Locations
- **Raw Data**: `/03_raw_data/narr_organizations/[state]/`
- **Processed Data**: `/04_processed_data/master_directories/master_directory.json`
- **Scripts**: `/02_scripts/extraction/mash_extractor.py`

### Pipeline Flow
1. Extract from state sources → 2. Standardize format → 3. Aggregate by state → 4. Create master directory

---

## 2. Recovery Community Centers (RCC) Pipeline

### Purpose
Identifies and extracts peer-run recovery support centers nationwide.

### Data Sources
- SAMHSA RCC directories
- State funding reports
- Regional recovery networks
- Web searches

### Schema
```json
{
  "id": "RCC_0001",
  "name": "Recovery Community Center Name",
  "type": "recovery_community_center",
  "address": {
    "street": "456 Peer Support Ave",
    "city": "Phoenix",
    "state": "AZ",
    "zip": "85001"
  },
  "contact": {
    "phone": "(555) 234-5678",
    "website": "https://rcc-example.org",
    "email": "info@rcc-example.org"
  },
  "services": [
    "Peer support groups",
    "Recovery coaching",
    "Social activities",
    "Resource navigation"
  ],
  "hours": "Monday-Friday 9AM-5PM",
  "certifications": ["CAPRSS accredited"],
  "funding_sources": ["SAMHSA", "State grants"],
  "population_served": "Adults in recovery",
  "data_source": "SAMHSA Directory",
  "extraction_date": "2025-07-31"
}
```

### Storage Locations
- **Raw Data**: `/03_raw_data/recovery_centers/[region]/`
- **Processed Data**: `/04_processed_data/master_directories/recovery_community_centers_master.json`
- **Scripts**: `/02_scripts/aggregation/aggregate_rccs_v2.py`

### Pipeline Flow
1. Search by funding source → 2. Extract center details → 3. Categorize by region → 4. Aggregate and deduplicate

---

## 3. Recovery Community Organizations (RCO) Pipeline

### Purpose
Extracts advocacy and policy-focused recovery organizations.

### Data Sources
- Faces & Voices of Recovery directory
- State advocacy networks
- Coalition websites

### Schema
```json
{
  "id": "RCO_0001",
  "name": "Recovery Advocacy Organization",
  "type": "recovery_community_organization",
  "focus": "advocacy",
  "address": {
    "street": "789 Policy Blvd",
    "city": "Austin",
    "state": "TX",
    "zip": "78701"
  },
  "contact": {
    "phone": "(555) 345-6789",
    "website": "https://recovery-advocacy.org"
  },
  "mission": "Advocate for recovery-friendly policies",
  "advocacy_priorities": [
    "Criminal justice reform",
    "Healthcare access",
    "Anti-stigma campaigns"
  ],
  "geographic_scope": "Statewide",
  "key_programs": ["Policy advocacy", "Community organizing"],
  "membership_type": "Coalition of organizations",
  "data_source": "Organization website",
  "extraction_date": "2025-07-31"
}
```

### Storage Locations
- **Raw Data**: `/03_raw_data/recovery_organizations/[category]/`
- **Processed Data**: `/04_processed_data/master_directories/recovery_organizations_master.json`
- **Scripts**: `/02_scripts/aggregation/aggregate_rcos.py`

---

## 4. Treatment Centers Pipeline

### Purpose
Aggregates licensed addiction treatment facilities from SAMHSA and state databases.

### Data Sources
- SAMHSA Treatment Locator (complete CSV dataset)
- State licensing databases
- Massachusetts MASH API

### Schema
```json
{
  "id": "SAMHSA_OUT_000001",  // Prefix indicates source and level
  "name": "Treatment Center Name",
  "level_of_care": "outpatient|residential|inpatient",
  "facility_type": "SU|MH|SUMH",  // Substance Use, Mental Health, Both
  "address": {
    "street": "321 Treatment Way",
    "city": "Denver",
    "state": "CO",
    "zip": "80201",
    "county": "Denver County"
  },
  "contact": {
    "phone": "(555) 456-7890",
    "website": "https://treatment-center.org",
    "intake_phone": "(555) 456-7891"
  },
  "services": [
    "Detoxification",
    "Medication-Assisted Treatment",
    "Individual counseling",
    "Group therapy"
  ],
  "populations_served": ["Adults", "Adolescents"],
  "insurance_accepted": ["Medicaid", "Medicare", "Private insurance"],
  "certifications": ["CARF accredited", "State licensed"],
  "data_source": "SAMHSA Treatment Locator",
  "extraction_date": "2025-07-31"
}
```

### Storage Locations
- **Raw Data**: 
  - `/SAMHSA Directory/` (original CSV files)
  - `/03_raw_data/samhsa_complete_processed.json`
- **Processed Data**: `/04_processed_data/master_directories/treatment_centers_master.json`
- **Scripts**: 
  - `/02_scripts/extraction/process_complete_samhsa.py`
  - `/02_scripts/aggregation/integrate_complete_samhsa_optimized.py`

### Pipeline Flow
1. Process SAMHSA CSV files → 2. Extract service codes → 3. Standardize records → 4. Integrate with existing data → 5. Deduplicate

---

## 5. Oxford House Pipeline

### Purpose
Extracts complete Oxford House network data including all houses (with and without vacancies).

### Data Sources
- oxfordvacancies.com (primary source)
- State-by-state searches
- Complete directory listings

### Schema
```json
{
  "id": "OXFORD_TX_0001",
  "name": "Oxford House [Name]",
  "organization_type": "recovery_residence",
  "certification_type": "oxford_house",
  "is_narr_certified": false,
  "narr_classification": {
    "is_narr_certified": false,
    "certification_type": "oxford_house",
    "certification_details": ["Oxford House Charter"],
    "classification_confidence": 1.0,
    "last_classified": "2025-07-31"
  },
  "address": {
    "street": "123 Oxford Lane",
    "city": "Houston",
    "state": "TX",
    "zip": "77001"
  },
  "contact": {
    "phone": "(555) 567-8901",
    "application_process": "Call for interview"
  },
  "capacity": {
    "total_beds": 8,
    "current_vacancies": 2,
    "has_vacancy": true,
    "occupancy_rate": 75.0
  },
  "demographics": {
    "gender": "Men",
    "age_restrictions": "Adults (18+)"
  },
  "operational_details": {
    "established_date": "2020-01-15",
    "charter_status": "Active Oxford House Charter",
    "governance": "Democratic self-governance"
  },
  "services": [
    "Democratically run house",
    "Peer support environment",
    "Self-supporting through resident fees",
    "Drug and alcohol free environment",
    "No time limit on residency"
  ],
  "cost": {
    "structure": "Equal share of house expenses",
    "typical_range": "$100-150 per week",
    "includes": ["Rent", "Utilities", "Basic supplies"]
  },
  "data_source": {
    "source": "oxfordvacancies.com",
    "extraction_date": "2025-07-31",
    "data_type": "Complete network data"
  }
}
```

### Storage Locations
- **Raw Data**: `/03_raw_data/oxford_house_data/`
  - `oxford_house_complete_network_latest.json`
  - `oxford_house_vacancies_[timestamp].json`
- **Scripts**: 
  - `/02_scripts/extraction/oxford_house_complete_pipeline.py` (all houses)
  - `/02_scripts/extraction/oxford_house_pipeline_v2.py` (with pagination)
  - `/02_scripts/extraction/process_oxford_vacancies.py`

### Pipeline Flow
1. Extract houses with vacancies → 2. Extract complete directory → 3. State-by-state search → 4. Merge and deduplicate → 5. Standardize format

### Key Features
- **Pagination Support**: Handles multiple pages of results
- **Complete Coverage**: Captures both vacant and occupied houses
- **Deduplication**: Prevents duplicate entries across search methods
- **Real-time Updates**: Can be run monthly for current vacancy data

---

## 6. Recurring Search Pipeline

### Purpose
Systematically searches for new recovery organizations not in existing databases.

### Data Sources
- Web searches via web-research-analyst agents
- State-specific queries
- Organization type-specific searches

### Schema
```json
{
  "search_metadata": {
    "state": "California",
    "organization_type": "rcc|rco|non_narr_residences",
    "search_date": "2025-07-31",
    "agent_used": "web-research-analyst"
  },
  "new_organizations": [
    {
      "name": "New Recovery Center",
      "address": "Full address",
      "phone": "Phone number",
      "website": "URL",
      "services": ["List of services"],
      "description": "Organization description"
    }
  ],
  "duplicates_filtered": [
    // Organizations already in our database
  ]
}
```

### Storage Locations
- **Raw Results**: `/03_raw_data/recurring_search_results/[state]_[type]_[timestamp].json`
- **Summary Reports**: `/05_reports/recurring_search_summaries/`
- **Scripts**: 
  - `/02_scripts/recurring_search/recurring_recovery_search.py`
  - `/02_scripts/recurring_search/execute_recovery_search.py`

### Pipeline Flow
1. Load existing data for deduplication → 2. Launch parallel agents → 3. Extract organizations → 4. Filter duplicates → 5. Save new discoveries

### Search Targets
- Recovery Community Centers (RCCs)
- Recovery Community Organizations (RCOs)  
- Non-NARR certified recovery residences

---

## 7. Network Analysis Pipeline

### Purpose
Analyzes relationships and referral networks among recovery organizations.

### Data Sources
- All master directories
- Geographic data
- Service overlap analysis
- Name pattern matching

### Schema
```json
{
  "clusters": {
    "geographic_proximity": {
      "cluster_id": {
        "organizations": ["org1_id", "org2_id"],
        "centroid": {"lat": 0.0, "lng": 0.0},
        "radius_miles": 10
      }
    },
    "service_overlap": {
      "peer_support": ["org1_id", "org2_id", "org3_id"],
      "recovery_coaching": ["org4_id", "org5_id"]
    },
    "name_similarity": {
      "oxford_house": ["OXFORD_CA_0001", "OXFORD_CA_0002"],
      "recovery_cafe": ["RCC_0010", "RCC_0025"]
    }
  },
  "referral_networks": {
    "org1_id_org2_id": {
      "org1": "Organization 1 Name",
      "org2": "Organization 2 Name",
      "score": 0.85,
      "factors": ["same_city", "complementary_services"]
    }
  }
}
```

### Storage Locations
- **Analysis Results**: `/05_reports/network_analysis_report.json`
- **Scripts**: `/02_scripts/analysis/network_clustering_framework.py`

### Analysis Methods
1. **Geographic Clustering**: Groups organizations by proximity
2. **Service Analysis**: Identifies complementary services
3. **Name Pattern Matching**: Detects franchise/affiliate relationships
4. **Referral Scoring**: Calculates likelihood of partnerships

---

## Data Storage Summary

### Directory Structure
```
narr_extractor/
├── 03_raw_data/
│   ├── narr_organizations/        # State-specific NARR data
│   ├── recovery_centers/          # RCC raw extracts
│   ├── recovery_organizations/    # RCO raw extracts
│   ├── oxford_house_data/        # Oxford House extracts
│   ├── recurring_search_results/  # New organization discoveries
│   └── samhsa_complete_processed.json
│
├── 04_processed_data/
│   ├── master_directories/
│   │   ├── master_directory.json              # NARR organizations
│   │   ├── recovery_community_centers_master.json
│   │   ├── recovery_organizations_master.json
│   │   └── treatment_centers_master.json
│   └── comprehensive_recovery_directory.json   # Combined all sources
│
└── 05_reports/
    ├── FINAL_PROJECT_REPORT.md
    ├── network_analysis_report.json
    └── recurring_search_summaries/
```

## Pipeline Execution Order

1. **Initial Extraction**: NARR → RCCs → RCOs → Treatment Centers
2. **Supplemental Extraction**: Oxford Houses → Recurring Searches
3. **Analysis**: Network Clustering → Referral Mapping
4. **Final Output**: Comprehensive Recovery Directory

## Maintenance and Updates

### Monthly Updates
- Run Oxford House pipeline for current vacancies
- Execute recurring searches for new organizations
- Update SAMHSA data quarterly
- Refresh network analysis

### Quality Checks
- Duplicate detection across all pipelines
- Address standardization
- Phone number validation
- Website verification
- NARR vs non-NARR classification accuracy

---

*Last Updated: July 31, 2025*