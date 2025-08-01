# Proposed File Organization Structure

## Overview
This document outlines a proposed reorganization of the NARR Extractor project workspace to improve clarity, accessibility, and maintainability as we expand beyond NARR to include Recovery Community Centers and Treatment Centers.

## Proposed Directory Structure

```
/narr_extractor/
│
├── 📁 01_documentation/
│   ├── README.md                           # Main project overview
│   ├── CLAUDE.md                          # Development guide
│   ├── EXPANDED_PROJECT_SCOPE.md          # Full project scope
│   ├── EXTRACTION_METHODOLOGY.md          # Technical methodology
│   ├── PROPOSED_FILE_ORGANIZATION.md      # This document
│   └── project_plans/
│       ├── FINAL_EXECUTION_PLAN.md
│       ├── IMMEDIATE_REANALYSIS_NEEDED.md
│       └── phase2_plan.md
│
├── 📁 02_scripts/
│   ├── extraction/
│   │   ├── mash_extractor.py
│   │   ├── trohn_comprehensive_extractor.py
│   │   ├── michigan_marr_extractor.py
│   │   ├── azrha_extractor.py
│   │   └── colorado_carr_extractor.py
│   ├── aggregation/
│   │   ├── aggregate_results.py
│   │   └── merge_mash_to_master.py
│   └── utilities/
│       └── requirements.txt
│
├── 📁 03_raw_data/
│   ├── narr_affiliates/
│   │   ├── narr_affiliates.json          # Original 29 affiliates
│   │   ├── narr_affiliates.csv
│   │   ├── batch1_results.json
│   │   ├── batch2_results.json
│   │   └── batch3_results.json
│   ├── narr_organizations/
│   │   ├── massachusetts/
│   │   │   ├── mash_certified_homes.json
│   │   │   └── mash_extraction_report.md
│   │   ├── texas/
│   │   │   ├── trohn_comprehensive_results.json
│   │   │   └── trohn_operators_detailed.json
│   │   ├── georgia/
│   │   │   ├── garr_certified_organizations.json
│   │   │   └── garr_extraction_report.md
│   │   ├── michigan/
│   │   │   ├── michigan_marr_operators.json
│   │   │   └── michigan_extraction_summary.md
│   │   ├── pennsylvania/
│   │   │   └── pennsylvania_statewide_directory.json
│   │   ├── arizona/
│   │   │   └── azrha_extraction_report.json
│   │   └── colorado/
│   │       └── colorado_carr_extraction_report.json
│   ├── recovery_centers/
│   │   ├── samhsa_funded/
│   │   │   └── samhsa_recovery_community_centers.json
│   │   ├── regional/
│   │   │   ├── southwest_recovery_community_centers.json
│   │   │   └── west_coast_recovery_community_centers.json
│   │   └── national_networks/
│   │       ├── recovery_community_centers_directory.json
│   │       └── recovery_community_centers_research_summary.md
│   └── treatment_centers/
│       ├── outpatient/
│       ├── residential/
│       └── inpatient/
│
├── 📁 04_processed_data/
│   ├── master_directories/
│   │   ├── master_directory.json          # Current master
│   │   ├── master_directory_backup.json   # Previous version
│   │   └── aggregation_summary.json
│   └── analytics/
│       └── SUMMARY_REPORT.md
│
├── 📁 05_reports/
│   ├── extraction_logs/
│   │   └── extraction_log.md
│   ├── analysis/
│   │   └── behavehealth_analysis.md
│   └── data_quality/
│       └── (quality reports)
│
└── 📁 06_archive/
    └── legacy_files/
        ├── text.txt
        ├── *.html files
        └── (other temporary files)
```

## Key Organization Principles

### 1. **Numbered Folders** (01-06)
- Provides clear workflow progression
- Easy to navigate and understand project flow

### 2. **Separation by Purpose**
- **Documentation**: All project docs and plans
- **Scripts**: Reusable code organized by function
- **Raw Data**: Original extraction results by source
- **Processed Data**: Aggregated and cleaned datasets
- **Reports**: Analysis and quality reports
- **Archive**: Legacy and temporary files

### 3. **Geographic Organization**
- NARR organizations grouped by state
- Recovery centers grouped by region/funding source
- Treatment centers will follow similar pattern

### 4. **Clear Naming Conventions**
- State/region prefixes where applicable
- Descriptive filenames
- Consistent date formats (YYYY-MM-DD)
- Version control through dated backups

## Migration Benefits

1. **Improved Navigation**: Clear hierarchy makes finding files intuitive
2. **Scalability**: Structure supports adding new data types
3. **Collaboration**: Others can easily understand project organization
4. **Maintenance**: Easy to identify outdated files for archival
5. **Integration**: Standardized structure facilitates automated processing

## Implementation Steps

1. Create new directory structure
2. Move files to appropriate locations
3. Update scripts to reference new paths
4. Create symlinks for backward compatibility if needed
5. Update documentation with new file locations
6. Archive temporary/working files

## Future Considerations

- Add `07_api/` folder when building API endpoints
- Add `08_database/` for database schemas and migrations
- Consider git submodules for very large datasets
- Implement automated backup strategy

This organization will support the project's growth from ~400 organizations to 17,000+ while maintaining clarity and efficiency.