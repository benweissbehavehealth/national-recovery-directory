# Expanded Project Scope: National Recovery Support Services Directory

## Vision
Create the most comprehensive national directory of recovery support services, expanding beyond NARR-certified recovery residences to include all major recovery support infrastructure.

## Service Categories

### 1. NARR-Certified Recovery Residences (Phase 2 - In Progress)
- **Definition**: Sober living homes certified by National Alliance for Recovery Residences
- **Certification Levels**: Level I (Peer-Run), Level II (Monitored), Level III (Supervised), Level IV (Service Provider)
- **Current Status**: 112 extracted, 800-1,200+ target
- **Sources**: 29 NARR state affiliates

### 2. Recovery Community Centers (Phase 3)
- **Definition**: Peer-run centers offering recovery support services, meetings, and activities
- **Key Services**: Peer support, recovery coaching, social activities, resource navigation
- **Target**: 200+ centers nationwide
- **Sources**: 
  - SAMHSA Recovery Community Services Program grantees
  - State behavioral health department directories
  - Association of Recovery Community Organizations (ARCO)
  - Faces & Voices of Recovery network

### 3. Recovery Community Organizations (Phase 4)
- **Definition**: Non-profit organizations mobilizing resources for recovery support
- **Key Services**: Advocacy, policy work, community organizing, recovery events
- **Target**: 300+ organizations
- **Sources**:
  - Faces & Voices of Recovery member directory
  - State recovery advocacy organizations
  - SAMHSA RCSP grantee lists
  - Young People in Recovery chapters

### 4. Licensed Addiction Treatment Centers - Outpatient (Phase 5)
- **Definition**: Licensed facilities providing non-residential treatment
- **Service Types**: 
  - Standard Outpatient (OP)
  - Intensive Outpatient (IOP)
  - Partial Hospitalization (PHP)
  - Medication-Assisted Treatment (MAT) clinics
- **Target**: 8,000+ facilities
- **Sources**: SAMHSA Treatment Locator, state licensing databases

### 5. Licensed Addiction Treatment Centers - Residential (Phase 6)
- **Definition**: 24-hour treatment facilities with overnight stays
- **Service Types**:
  - Short-term residential (30 days or less)
  - Long-term residential (more than 30 days)
  - Therapeutic communities
  - Modified therapeutic communities
- **Target**: 4,000+ facilities
- **Sources**: SAMHSA Treatment Locator, state licensing databases

### 6. Licensed Addiction Treatment Centers - Inpatient (Phase 7)
- **Definition**: Hospital-based or medically managed treatment
- **Service Types**:
  - Detoxification services
  - Hospital inpatient
  - Psychiatric units with addiction focus
  - Dual diagnosis treatment
- **Target**: 3,000+ facilities
- **Sources**: SAMHSA Treatment Locator, hospital databases, state licensing

## Data Collection Strategy

### Parallel Processing Approach
- Minimum 7 concurrent agents, up to 15 for major extractions
- Geographic distribution: Assign agents by region to ensure coverage
- Service type specialization: Dedicate agents to specific facility types

### Priority Extraction Order
1. **Immediate**: Complete NARR certified organizations (Phase 2)
2. **Week 2**: Begin Recovery Community Centers extraction
3. **Week 3**: Recovery Community Organizations
4. **Month 2**: Licensed treatment centers by level of care

### Data Standardization
Each entry will include:
- Unique identifier with service type prefix (RR_, RCC_, RCO_, TX_)
- Organization type and subtype
- Complete contact information
- Services offered with standardized taxonomy
- Licensing/certification information
- Insurance acceptance (where applicable)
- Capacity and availability
- Special populations served
- Peer support availability
- Geographic coordinates for mapping

## Integration Points

### BehaveHealth Directory Compatibility
- Match data structure to BehaveHealth's schema
- Include required fields for direct import
- Maintain data quality standards for integration

### Cross-Referencing Opportunities
- Many organizations operate multiple service types
- Create parent-child relationships in data
- Link affiliated organizations
- Track referral networks

## Quality Assurance

### Verification Priorities
1. Active license/certification status
2. Current contact information
3. Service accuracy
4. Insurance acceptance updates
5. Capacity/availability status

### Update Frequency
- NARR residences: Monthly
- Treatment centers: Quarterly (licensing changes)
- RCCs/RCOs: Semi-annually (more stable)

## Expected Outcomes

### By Service Type
- **Recovery Residences**: 1,200+ certified homes
- **Recovery Community Centers**: 200+ centers
- **Recovery Community Organizations**: 300+ organizations
- **Treatment Centers (all levels)**: 15,000+ facilities
- **Total Directory Entries**: 17,000+ organizations

### Geographic Coverage
- All 50 states plus DC
- Urban and rural representation
- Underserved area identification
- Service desert mapping

## Next Steps

1. Complete Phase 2 NARR re-extraction with 7+ parallel agents
2. Create directory structure for new service types
3. Develop extraction templates for each service category
4. Establish data validation protocols
5. Build relationships with national organizations for ongoing data access

This expanded scope transforms the project from a recovery residence directory into a comprehensive national recovery support infrastructure database.