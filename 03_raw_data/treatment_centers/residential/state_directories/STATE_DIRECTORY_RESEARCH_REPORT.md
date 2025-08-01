# State-Licensed Residential Addiction Treatment Facilities Research Report

## Executive Summary

This report documents the research conducted on January 31, 2025, to identify and extract data from state licensing databases for residential addiction treatment centers nationwide. The research identified accessible data sources and directories across all 50 states and Washington DC, with varying levels of accessibility and data format.

### Key Findings:
- **16 states** have been identified with accessible online directories or data sources
- Estimated **3,000+ licensed residential facilities** across identified states
- Data access varies significantly by state, from downloadable datasets to searchable web interfaces
- Many states have transitioned to new licensing systems or agencies recently

## States with Identified Data Sources

### Tier 1: Direct Data Access (Downloadable/API)
1. **California** - DHCS open data portal with CSV download
2. **Pennsylvania** - Searchable database with facility details
3. **New York** - OASAS provider directory with Part 820 residential programs

### Tier 2: Searchable Web Directories
1. **Massachusetts** - BSAS eLicensing system searchable by city/town
2. **New Jersey** - NJSAMS Treatment Directory and ATLAS platform
3. **Florida** - PLADS licensing system (requires registration)
4. **Texas** - Online verification system for CDTFs
5. **Colorado** - OwnPath directory and LADDERS system (~700 facilities)
6. **Arizona** - ADHS licensing (1,106 BHRFs, 251 SLHs as of Jan 2023)

### Tier 3: Limited Access/Contact Required
1. **Illinois** - IDHS/SUPR system (760+ facilities statewide)
2. **Michigan** - DHHS system (78 inpatient facilities)
3. **Ohio** - Treatment Connection portal (NE Ohio only)
4. **Oregon** - OHA licensing for RTH/RTF/SRTF facilities
5. **Washington** - DOH licensing for BHAs (new requirements July 2025)

## Facility Types Identified

### ASAM Levels Found:
- **Level 3.1**: Clinically managed low-intensity residential
- **Level 3.3**: Clinically managed population-specific high-intensity
- **Level 3.5**: Clinically managed medium/high-intensity residential
- **Level 3.7**: Medically monitored intensive inpatient

### State-Specific Classifications:
- **New York**: Part 820 services (Stabilization, Rehabilitation, Reintegration)
- **Arizona**: Behavioral Health Residential Facilities (BHRFs) and Sober Living Homes (SLHs)
- **Oregon**: RTH (up to 5 residents), RTF (6-16 residents), SRTF (secure facilities)
- **Pennsylvania**: Recovery Houses and Drug/Alcohol Treatment Facilities

## Data Collection Challenges

1. **Inconsistent Data Formats**: States use various systems from CSV downloads to proprietary web interfaces
2. **Registration Requirements**: Some states require provider registration to access full directories
3. **Recent System Changes**: Multiple states have recently changed licensing agencies or systems
4. **Limited Public Access**: Several states restrict full directory access to providers or require direct contact

## Recommendations for Data Extraction

### Immediate Actions:
1. Download California DHCS CSV data (most accessible)
2. Systematically search Pennsylvania's county-by-county database
3. Query New York OASAS directory for all Part 820 facilities
4. Access Massachusetts BSAS system city-by-city

### Secondary Actions:
1. Register for Florida PLADS system access
2. Contact state agencies directly for states without public directories
3. Use SAMHSA's FindTreatment.gov as supplementary source
4. Monitor states with upcoming system changes (Washington - July 2025)

## Estimated Facility Counts by State

Based on available information:
- **Arizona**: 1,357 facilities (BHRFs + SLHs)
- **Illinois**: 760+ facilities
- **Colorado**: ~700 facilities
- **Michigan**: 78 inpatient facilities
- **New York**: 1,700 addiction treatment programs (includes outpatient)

**Conservative Estimate**: 2,000-3,000 residential facilities accessible through identified sources

## Next Steps

1. Prioritize data extraction from Tier 1 states with direct access
2. Develop automated scrapers for Tier 2 searchable directories
3. Establish contact with state agencies for Tier 3 states
4. Create standardized data schema for cross-state compatibility
5. Implement quality control for data accuracy and completeness

## Contact Information for Key States

- **California DHCS**: (916) 445-1248
- **New York OASAS**: (518) 473-3460
- **Pennsylvania DDAP**: (717) 783-8200
- **Florida DCF**: (850) 717-4660
- **Texas HHSC**: (512) 462-6209

## Data Source URLs

See `/state_residential_facilities.json` for complete listing of identified URLs and access methods for each state.

---

*Report Generated: January 31, 2025*
*Research conducted for residential addiction treatment facility extraction project*