# NARR Directory Extraction Methodology

## Overview
This document provides a comprehensive methodology for extracting and maintaining the complete directory of NARR-certified recovery residences. This process should be executed monthly to ensure data freshness and capture new certifications.

## Phase 1: NARR Affiliate Analysis (Complete)

### Objective
Extract comprehensive information about all NARR state affiliates and their certified organizations.

### Current Status: COMPLETED
- **29 affiliates analyzed** across all NARR-affiliated states
- **Results stored in**: batch1_results.json, batch2_results.json, batch3_results.json
- **Total organizations initially identified**: 230+ certified recovery residences

## Phase 2: Comprehensive Organization Extraction (In Progress)

### Critical Gap Identified
Initial extraction was not exhaustive. Many affiliates have comprehensive directories that require deeper analysis to extract ALL certified organizations.

### Methodology for Complete Extraction

#### Tier 1: Affiliates with Public Directories (Immediate Access)
These affiliates have publicly accessible directories that can be systematically scraped:

1. **Texas TROHN** - 78 certified residences (COMPLETE)
2. **Georgia GARR** - 32 organizations across 3 levels (COMPLETE)  
3. **North Carolina NCARR** - 20 certified residences (COMPLETE)
4. **Alabama AARR** - 22 certified residences (COMPLETE)
5. **Florida FARR** - 9+ identified, likely more available
6. **Pennsylvania PARR** - WestPARR has 14, likely more statewide

**Action Plan**: Re-analyze these affiliates with focus on exhaustive extraction of ALL listed providers.

#### Tier 2: Affiliates with Dynamic/Interactive Directories
These require direct website interaction or API calls:

1. **Massachusetts MASH** - 180+ certified homes (dynamic directory)
2. **Michigan MARR** - Extensive operator list available
3. **Ohio ORH** - Directory under maintenance, requires direct contact
4. **Virginia VARR** - State mandated certification, comprehensive Excel lists available
5. **Colorado CARR** - Interactive system, requires direct contact
6. **Arizona AzRHA** - 1,678 beds represented, search-based system
7. **Washington WAQRR** - Interactive directory system
8. **Oklahoma OKARR** - Search-based portal

**Action Plan**: Develop automated scripts to interact with search interfaces or contact organizations directly.

#### Tier 3: Affiliates Requiring Direct Contact
These have limited public access and require organizational outreach:

1. **California CCAPP** - Member-only directory access
2. **New York NYSARR** - Member portal access required
3. **Connecticut CTARR** - Directory not publicly accessible
4. **Delaware FSARR** - No public directory, press releases only
5. **Indiana INARR** - Managed by state DMHA
6. **Kentucky KRHN** - Redirected to external system
7. **Illinois IAEC** - Limited website functionality
8. **Minnesota MASH** - Directory access limited
9. **Missouri MCRSP** - PDF directories not accessible
10. **Maine MARR** - Dynamic directory system

**Action Plan**: Systematic outreach campaign to request complete member directories.

### Monthly Update Process

#### Week 1: Tier 1 Re-Analysis
- Use web-research-analyst agents to comprehensively re-extract ALL organizations from public directories
- Focus on finding organizations missed in initial analysis
- Verify contact information and certification status

#### Week 2: Tier 2 Interactive Extraction  
- Develop scripts for dynamic directory interaction
- Contact organizations for API access or bulk data exports
- Use specialized tools for JavaScript-heavy sites

#### Week 3: Tier 3 Direct Outreach
- Send standardized requests to all Tier 3 affiliates
- Follow up on previous month's requests
- Build relationships with affiliate staff for ongoing access

#### Week 4: Data Aggregation and Validation
- Merge all new findings into master directory
- Cross-reference against previous data
- Validate contact information and certification status
- Generate monthly update report

## Standardized Data Structure

### Required Fields for Each Organization
```json
{
  "organization_id": "unique_identifier",
  "affiliate_state": "State Name",
  "affiliate_name": "NARR Affiliate Name",
  "extraction_date": "YYYY-MM-DD",
  "organization_name": "Primary Business Name",
  "trade_names": ["DBA names"],
  "addresses": [
    {
      "type": "primary|facility|mailing",
      "street": "Street Address",
      "city": "City",
      "state": "ST",
      "zip": "ZIP",
      "coordinates": {"lat": 0.0, "lng": 0.0}
    }
  ],
  "contact": {
    "phone_primary": "XXX-XXX-XXXX",
    "phone_secondary": "XXX-XXX-XXXX", 
    "email_primary": "email@domain.com",
    "website": "https://website.com"
  },
  "certification": {
    "narr_level": "Level I|II|III|IV",
    "certification_date": "YYYY-MM-DD",
    "expiration_date": "YYYY-MM-DD",
    "status": "Active|Pending|Expired"
  },
  "services": {
    "capacity": 0,
    "target_populations": ["Men", "Women", "Veterans", etc],
    "specializations": ["MAT", "Dual Diagnosis", etc],
    "service_types": ["Residential", "Transitional", etc]
  },
  "data_quality": {
    "completeness_score": 0.0-1.0,
    "last_verified": "YYYY-MM-DD",
    "source_reliability": "High|Medium|Low"
  }
}
```

## Tools and Resources

### Primary Tools
1. **Web Research Analyst Agents** - For comprehensive website analysis
2. **List Extraction Agents** - For directory-specific extraction
3. **Direct Contact Templates** - Standardized outreach messages
4. **Data Validation Scripts** - For contact verification

### Contact Templates

#### Initial Request Template
```
Subject: Request for Certified Recovery Residence Directory - Monthly Update

Dear [Affiliate Name] Team,

We are conducting comprehensive research to build a master directory of NARR-certified recovery residences to help individuals seeking recovery housing. 

Could you please provide:
1. Complete list of currently certified recovery residences
2. Contact information (address, phone, email, website)
3. Certification levels and specializations
4. Any recent additions or changes to your certified member list

This information will be used solely for recovery housing directory purposes and will be updated monthly to ensure accuracy.

Thank you for your support of recovery housing standards.

Best regards,
[Your name and organization]
```

#### Follow-up Template
```
Subject: Follow-up: Certified Recovery Residence Directory Request

Dear [Contact Name],

Following up on our request from [date] for your current certified recovery residence directory. We understand you may be busy, but this information is crucial for helping individuals find quality recovery housing.

Would it be possible to schedule a brief call to discuss how we can access this information on an ongoing basis?

Thank you for your time and commitment to recovery housing standards.

Best regards,
[Your name and organization]
```

## Quality Assurance Process

### Data Validation Steps
1. **Contact Verification** - Test phone numbers and email addresses
2. **Website Validation** - Verify URLs are active and current  
3. **Address Standardization** - Use USPS address validation
4. **Duplicate Detection** - Identify organizations listed by multiple affiliates
5. **Certification Cross-Reference** - Verify with NARR national database when possible

### Monthly Reporting
- Total organizations in database
- New organizations added this month
- Organizations that changed status
- Contact information updates
- Data quality metrics by affiliate
- Affiliates requiring follow-up

## Success Metrics

### Quantitative Goals
- **Coverage**: 90%+ of all NARR-certified organizations
- **Data Quality**: 85%+ complete contact information
- **Freshness**: 90%+ of data verified within 3 months
- **Growth**: 10%+ new organizations identified monthly

### Qualitative Goals
- Establish ongoing relationships with affiliate staff
- Develop automated extraction where possible
- Create reliable monthly update cycle
- Build comprehensive resource for recovery housing seekers

## File Organization

### Monthly Structure
```
/narr_extractor/
├── monthly_updates/
│   ├── 2025-07/
│   │   ├── tier1_results.json
│   │   ├── tier2_results.json  
│   │   ├── tier3_results.json
│   │   ├── monthly_report.md
│   │   └── data_quality_report.json
│   ├── 2025-08/
│   └── 2025-09/
├── master_directory.json
├── affiliate_contacts.json
└── extraction_templates/
```

This methodology ensures comprehensive, systematic extraction of ALL NARR-certified recovery residences with ongoing monthly updates to maintain data accuracy and completeness.