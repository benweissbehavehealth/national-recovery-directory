# Inpatient/Hospital-Based Treatment Centers Data Directory

This directory contains data and resources for hospital-based and medically-managed inpatient addiction treatment facilities.

## Data Files

- `samhsa_inpatient_facilities.json` - Main data file for extracted inpatient facilities
- `samhsa_inpatient_facilities_summary.txt` - Summary statistics of extracted facilities

## Extraction Scripts Created

### 1. samhsa_inpatient_extractor.py
- **Purpose**: Direct API extraction from SAMHSA findtreatment.gov
- **Features**: 
  - Comprehensive data model for inpatient facilities
  - Service type filtering for hospital/inpatient services
  - State-by-state systematic extraction
  - Medicare data enhancement capability

### 2. samhsa_inpatient_web_extractor.py
- **Purpose**: Enhanced web scraping approach
- **Features**:
  - Browser simulation headers
  - Multiple search URL patterns
  - JSON extraction from page scripts
  - HTML parsing for facility cards
  - Deduplication by facility hash

### 3. multi_source_inpatient_extractor.py
- **Purpose**: Aggregate data from multiple sources
- **Features**:
  - State health department extractors
  - VA Medical Centers API integration
  - Local file processing (CSV/JSON)
  - Multi-source data merging
  - Comprehensive reporting

## Target Facility Types

### Service Types
- Hospital inpatient detoxification
- Medical detox units
- Psychiatric hospitals with addiction units
- Dual diagnosis inpatient
- Medical stabilization
- ASAM Level 4 facilities
- Acute care addiction units
- Emergency detox services

### Facility Categories
- General hospitals with addiction units
- Psychiatric hospitals
- Specialty addiction hospitals
- VA medical centers
- University medical centers
- Private hospital systems

## Data Sources Identified

### Primary Sources
1. **SAMHSA Treatment Locator** (findtreatment.gov)
   - Requires API access request
   - Web scraping as alternative

2. **CMS Provider Data**
   - Provider of Services (POS) files
   - Medicare-certified facilities
   - Quarterly updates

3. **State Directories**
   - California DHCS
   - New York OASAS
   - Texas HHSC
   - Florida DCF
   - Pennsylvania DDAP

### Secondary Sources
- VA Facilities API
- Hospital association directories
- SAMHSA N-SUMHSS dataset
- State Medicaid provider lists

## Data Fields Captured

### Core Information
- Facility name and IDs (NPI, Medicare)
- Complete address and contact details
- Geographic coordinates

### Clinical Services
- Detox capabilities (medical, standard)
- Psychiatric services
- Dual diagnosis treatment
- Emergency admission capability
- Medical stabilization

### Operational Details
- Bed capacity (total, detox, psychiatric)
- Medical staff credentials
- 24/7 nursing availability
- Average length of stay

### Insurance & Certifications
- Medicare/Medicaid acceptance
- Private insurance
- VA/TRICARE approval
- JCAHO/CARF accreditation
- State certifications

## Usage Instructions

### To Run Extractions
```bash
# Direct SAMHSA extraction
python3 samhsa_inpatient_extractor.py

# Web scraping approach
python3 samhsa_inpatient_web_extractor.py

# Multi-source extraction
python3 multi_source_inpatient_extractor.py
```

### To Process Results
The scripts automatically:
1. Deduplicate facilities by name/address
2. Merge data from multiple sources
3. Generate summary statistics
4. Save to JSON format

## Known Issues & Limitations

1. **API Access**: SAMHSA API requires authentication/access request
2. **Rate Limiting**: Web scraping must respect rate limits
3. **Data Currency**: Some sources update quarterly or annually
4. **Coverage Gaps**: Not all states have publicly accessible directories

## Recommendations

1. Submit API access requests to primary data sources
2. Implement selenium for JavaScript-heavy sites
3. Contact state agencies directly for bulk data
4. Cross-reference with Medicare provider databases
5. Validate facility types through direct contact

## Contact for Questions

For questions about this data collection effort, refer to the extraction report at:
`/05_reports/extraction_logs/inpatient_extraction_report.md`