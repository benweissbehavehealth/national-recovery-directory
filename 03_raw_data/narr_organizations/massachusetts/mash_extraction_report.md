# MASH Certified Recovery Homes Extraction Report

## Executive Summary
Successfully extracted **204 certified recovery homes** from Massachusetts Alliance for Sober Housing (MASH) - exceeding the expected 180+ homes. The extraction was completed using the MASH API endpoint discovered through website analysis.

## Extraction Details

### Data Source
- **Organization**: Massachusetts Alliance for Sober Housing (MASH)
- **Website**: https://mashsoberhousing.org/certified-residences/
- **API Endpoint**: https://management.mashsoberhousing.org/api/GetCertifiedHomes
- **Extraction Date**: July 30, 2025

### Results Summary
- **Total Homes Extracted**: 204 (113% of expected 180+)
- **Data Completeness**: High - all homes include basic information
- **Handicap Accessible Homes**: 5

### Breakdown by Service Type
- Male: 145 homes (71%)
- Female: 52 homes (25%)
- Co-ed: 7 homes (4%)

### Breakdown by Region
- Southeast: 73 homes (36%)
- Northeast: 39 homes (19%)
- Boston: 38 homes (19%)
- Central: 24 homes (12%)
- Metro West: 16 homes (8%)
- Western: 14 homes (7%)

## Data Fields Captured
For each certified home, the following information was extracted:
- Home name
- Complete address (street, city, state, zip)
- Service type (Male/Female/Co-ed)
- Bed capacity
- Region
- Languages spoken
- Website (if available)
- Handicap accessibility status
- Public contact information (name, phone, email)
- Weekly/monthly fees (where available)
- Certification dates

## Technical Implementation
- **Method**: Direct API access
- **Language**: Python 3
- **Libraries**: requests, json
- **Output Format**: JSON (both detailed and simplified versions)

## File Outputs
1. `mash_certified_homes.json` - Complete data with all fields
2. `mash_certified_homes_simplified.json` - Simplified view for easy reference
3. `mash_extractor.py` - Extraction script for future updates

## Key Findings
1. **Complete Coverage**: Successfully extracted all available homes from MASH database
2. **Gender Distribution**: Significant majority (71%) serve male populations
3. **Geographic Distribution**: Southeast Massachusetts has the highest concentration (36%)
4. **Contact Information**: Most homes have at least one public contact listed
5. **Website Presence**: Many homes lack dedicated websites

## Recommendations
1. Regular updates: Run the extraction script monthly to capture new certifications
2. Data validation: Cross-reference with MASH website for any updates
3. Integration: Merge this data with the master NARR directory
4. Enhancement: Consider geocoding addresses for mapping capabilities

## Success Metrics Achieved
✓ Extracted 204 homes (exceeded target of 180+)
✓ Captured all required data fields
✓ Created structured JSON output
✓ Documented extraction process
✓ Built reusable extraction tool