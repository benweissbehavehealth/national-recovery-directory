# NARR Master Directory Extractor

This project creates a comprehensive directory of all NARR-certified recovery residences by extracting data from NARR affiliates and their certified organizations.

## Project Overview

**Phase 1 - NARR Affiliates (COMPLETED)**
- **Source**: https://narronline.org/affiliates/#affiliates
- **Goal**: Extract comprehensive information about all 29 NARR state affiliates
- **Status**: ✅ Complete - 29 affiliates extracted with 97% data completeness

**Phase 2 - Certified Organizations (IN PROGRESS)**
- **Sources**: All 29 NARR affiliate websites 
- **Goal**: Extract all certified recovery residences from each affiliate's directory
- **Target**: Comprehensive analysis of each affiliate website (top to bottom)
- **Data Points**: Organization names, locations, contact info, certification details

**Phase 3 - Organization Research (PLANNED)**
- **Sources**: Individual certified organization websites
- **Goal**: Deep research on each certified organization
- **Target**: Populate master directory with detailed organizational profiles
- **Format**: Structure data for BehaveHealth directory format (similar to https://directory.behavehealth.com/virginia)

## Data Collection Process

### Phase 1: NARR Affiliates ✅
1. Navigate to the main affiliates page
2. Extract list of all affiliates
3. Visit each affiliate's "read more" page
4. Extract detailed information from individual pages

### Phase 2: Certified Organizations 🔄
1. Visit each of the 29 NARR affiliate websites
2. Perform comprehensive top-to-bottom analysis of all pages
3. Extract certified organization listings and directories
4. Collect location data for each certified organization
5. Structure data for master directory

### Phase 3: Organization Deep Research 📋
1. Research individual certified organization websites
2. Extract comprehensive organizational profiles
3. Structure data for BehaveHealth directory integration
4. Create searchable master directory

## Output Structure

### Current Deliverables (Phase 1)
- `narr_affiliates.json` - Complete affiliate data
- `narr_affiliates.csv` - Spreadsheet format
- `SUMMARY_REPORT.md` - Analysis report

### Planned Deliverables (Phase 2-3)
- `certified_organizations.json` - All certified recovery residences
- `organization_locations.json` - Geographic data for mapping
- `master_directory.json` - Complete directory structure
- `behavehealth_format.json` - Data formatted for BehaveHealth integration

## Status

- **Project Started**: 2025-07-30
- **Phase 1 Complete**: 2025-07-30 (29 NARR affiliates)
- **Phase 2 In Progress**: Comprehensive affiliate website analysis
- **Total Target**: 29 affiliate websites + hundreds of certified organizations