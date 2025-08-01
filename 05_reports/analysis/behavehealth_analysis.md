# BehaveHealth Directory Format Analysis

## Reference Site
https://directory.behavehealth.com/virginia

## Data Structure Analysis

Based on the BehaveHealth directory format, we need to extract and structure the following data points for each certified recovery residence:

### Core Organization Information
- **Organization Name**: Primary business name
- **Trade Names/DBA**: Any alternative names used
- **Description**: Brief organizational overview
- **Mission Statement**: Organization's stated purpose

### Location & Contact Data
- **Primary Address**: Street address, city, state, ZIP
- **Service Areas**: Geographic areas served
- **Phone Numbers**: Primary, secondary, emergency
- **Email Addresses**: General contact, admissions, specific departments
- **Website URL**: Official organization website
- **Social Media**: Facebook, LinkedIn, etc.

### Service Information
- **Service Types**: 
  - Residential Treatment
  - Sober Living/Recovery Residences
  - Transitional Housing
  - Intensive Outpatient (IOP)
  - Outpatient Services
  
- **Specializations**:
  - Demographics (Men, Women, LGBTQ+, Youth, Adults, Seniors)
  - Populations (Veterans, First Responders, Professionals)
  - Conditions (Dual Diagnosis, Trauma, Co-occurring Disorders)
  - Treatment Approaches (12-Step, Non-12-Step, Faith-Based, Secular)

### Operational Details
- **Capacity**: Number of beds/residents
- **Admission Requirements**: Age limits, sobriety requirements, etc.
- **Insurance Accepted**: Private insurance, Medicaid, Medicare, self-pay
- **Payment Options**: Financial assistance, sliding scale, etc.
- **Languages**: Languages spoken by staff
- **Accessibility**: ADA compliance, accommodations

### Certification & Licensing
- **NARR Certification**: Level (I-IV), status, date
- **State Licenses**: License numbers and types
- **Accreditations**: Joint Commission, CARF, etc.
- **Affiliations**: Professional associations, networks

### Quality Indicators
- **Founded Date**: When organization was established
- **Experience**: Years in operation
- **Staff Credentials**: Licensed professionals on staff
- **Success Metrics**: Completion rates, outcomes data (if available)

## Data Extraction Strategy

### Primary Sources (NARR Affiliates)
1. **Certified Provider Directories**: Most affiliates maintain searchable databases
2. **Member Lists**: Public listings of certified organizations
3. **Resource Pages**: Contact information and service details

### Secondary Sources (Organization Websites)
1. **About Pages**: Mission, history, leadership
2. **Services Pages**: Detailed program descriptions
3. **Contact Pages**: Complete contact information
4. **Admissions Pages**: Requirements, process, insurance
5. **Resources Pages**: Additional services, community connections

### Data Validation Methods
1. **Cross-Reference**: Verify data across multiple sources
2. **Contact Verification**: Test phone numbers and email addresses
3. **Website Validation**: Ensure URLs are active and current
4. **Certification Check**: Verify NARR certification status

## Output Format Template

```json
{
  "organization_id": "unique-identifier",
  "basic_info": {
    "name": "Organization Name",
    "trade_names": ["Alternative Name 1", "Alternative Name 2"],
    "description": "Brief description of services",
    "mission_statement": "Organization's mission",
    "founded": "YYYY",
    "website": "https://example.com"
  },
  "contact": {
    "primary_address": {
      "street": "123 Main Street",
      "city": "Richmond",
      "state": "VA",
      "zip": "23219",
      "coordinates": {"lat": 37.5407, "lng": -77.4360}
    },
    "phone": {
      "primary": "804-555-0123",
      "admissions": "804-555-0124",
      "emergency": "804-555-0125"
    },
    "email": {
      "general": "info@example.com",
      "admissions": "admissions@example.com"
    },
    "social_media": {
      "facebook": "https://facebook.com/orgname",
      "linkedin": "https://linkedin.com/company/orgname"
    }
  },
  "services": {
    "primary_services": ["Sober Living", "Transitional Housing"],
    "specializations": {
      "demographics": ["Men", "Adults"],
      "populations": ["Veterans"],
      "conditions": ["Substance Use Disorder"],
      "approaches": ["12-Step", "Peer Support"]
    },
    "capacity": 24,
    "admission_requirements": [
      "18+ years old",
      "30+ days sobriety",
      "Criminal background check"
    ]
  },
  "operational": {
    "insurance_accepted": ["Private Insurance", "Medicaid", "Self-Pay"],
    "languages": ["English", "Spanish"],
    "accessibility": "ADA Compliant",
    "hours": "24/7 Residential"
  },
  "certifications": {
    "narr": {
      "level": "Level II",
      "status": "Active",
      "certification_date": "2024-01-15",
      "expiration_date": "2026-01-15"
    },
    "state_licenses": ["VA License #12345"],
    "accreditations": ["CARF Accredited"]
  },
  "quality_indicators": {
    "years_in_operation": 15,
    "staff_credentials": ["Licensed Clinical Social Workers", "Certified Addiction Counselors"],
    "success_metrics": {
      "completion_rate": "85%",
      "follow_up_data": "Available upon request"
    }
  }
}
```

## Integration Notes

This structured format will allow easy integration with the BehaveHealth directory system and provide comprehensive information for users seeking recovery housing options.