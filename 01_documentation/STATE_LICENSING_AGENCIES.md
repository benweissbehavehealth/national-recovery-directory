# State Licensing Agencies for Addiction Treatment & Behavioral Health

## Overview
Every state has regulatory agencies that license and monitor addiction treatment facilities, behavioral health centers, and mental health facilities. We need to integrate all 50 states' licensing databases into our data pipeline.

## Current Status
As of August 2025, we have:
- SAMHSA Treatment Locator (federal level)
- MASH API (Massachusetts only)
- Some state-specific extractions (Michigan MARR, Arizona AZRHA)

## Required State Agency Data Sources

### Priority 1: States with Downloadable Provider Lists
These states offer CSV/Excel downloads or APIs:

1. **California** - DHCS (Department of Health Care Services)
   - URL: https://www.dhcs.ca.gov/individuals/Pages/sud-licensed-certified.aspx
   - Data: Licensed residential and outpatient SUD facilities
   - Format: Excel download

2. **Texas** - HHSC (Health and Human Services Commission)
   - URL: https://www.hhs.texas.gov/providers/long-term-care-providers/search-texas-providers
   - Data: Licensed treatment facilities
   - Format: Searchable database with export

3. **Florida** - DCF (Department of Children and Families)
   - URL: https://www.myflfamilies.com/service-programs/samh/
   - Data: Licensed substance abuse providers
   - Format: Provider directory

4. **New York** - OASAS (Office of Addiction Services and Supports)
   - URL: https://webapps.oasas.ny.gov/providerDirectory/index.cfm
   - Data: All certified addiction treatment programs
   - Format: Searchable with export capability

5. **Pennsylvania** - DDAP (Department of Drug and Alcohol Programs)
   - URL: https://sais.health.pa.gov/commonpoc/content/publiccommonpoc/normalSearch.aspx
   - Data: Licensed treatment facilities
   - Format: Public search portal

### Priority 2: States Requiring Web Scraping
These states have searchable directories but no direct download:

6. **Illinois** - SUPR (Division of Substance Use Prevention and Recovery)
7. **Ohio** - MHAS (Department of Mental Health and Addiction Services)
8. **Georgia** - DBHDD (Department of Behavioral Health and Developmental Disabilities)
9. **North Carolina** - DMH/DD/SAS
10. **Michigan** - LARA (Licensing and Regulatory Affairs)

### Complete State List Structure
```json
{
  "state_agencies": {
    "AL": {
      "name": "Alabama Department of Mental Health",
      "abbreviation": "ADMH",
      "url": "https://mh.alabama.gov/",
      "data_access": "web_portal",
      "licenses": ["SUD", "Mental Health", "Residential"]
    },
    "AK": {
      "name": "Alaska Division of Behavioral Health",
      "abbreviation": "DBH",
      "url": "https://dhss.alaska.gov/dbh/",
      "data_access": "pdf_list",
      "licenses": ["Behavioral Health", "SUD"]
    }
    // ... all 50 states
  }
}
```

## NPI Database Integration

### What is NPI?
The National Provider Identifier (NPI) is a unique 10-digit identification number for healthcare providers in the US.

### Why We Need It
- Links providers across different databases
- Contains provider taxonomy codes (specialty types)
- Includes organizational hierarchies
- Updated monthly by CMS

### NPI Data Source
- **Download URL**: https://download.cms.gov/nppes/NPI_Files.html
- **Format**: CSV (5GB+ uncompressed)
- **Update Frequency**: Monthly
- **Key Fields**:
  - NPI
  - Provider Name (Individual/Organization)
  - Practice Address
  - Taxonomy Codes (provider specialties)
  - Organization relationships

### Relevant Taxonomy Codes for Our Use Case
```
261QR0405X - Residential Treatment Facility, Substance Abuse
324500000X - Substance Abuse Rehabilitation Facility
261QM0855X - Adolescent and Children Mental Health
101YA0400X - Addiction (Substance Use Disorder) Counselor
```

## Implementation Plan

### Phase 1: Infrastructure Setup
1. Create state agency tracking table in DuckDB
2. Build flexible extractor framework for different data formats
3. Set up NPI database import pipeline

### Phase 2: High-Volume States (Top 10 by population)
- California, Texas, Florida, New York, Pennsylvania
- Illinois, Ohio, Georgia, North Carolina, Michigan

### Phase 3: Remaining States
- Prioritize by data accessibility
- States with APIs/downloads first
- Web scraping states last

### Phase 4: Data Integration
- Match facilities across SAMHSA, state, and NPI databases
- Identify gaps and discrepancies
- Create unified facility registry

## Data Pipeline Architecture

```python
# Proposed structure
state_extractors/
├── __init__.py
├── base_state_extractor.py
├── states/
│   ├── ca_dhcs_extractor.py
│   ├── tx_hhsc_extractor.py
│   ├── fl_dcf_extractor.py
│   └── ... (one per state)
├── npi/
│   ├── npi_downloader.py
│   ├── npi_parser.py
│   └── npi_matcher.py
└── utils/
    ├── state_agency_registry.py
    └── license_type_mapper.py
```

## Success Metrics
- Coverage: All 50 states + DC
- Completeness: 95%+ of licensed facilities captured
- Freshness: Updated monthly
- Accuracy: Cross-validated with NPI database
- Deduplication: <5% duplicate rate across sources