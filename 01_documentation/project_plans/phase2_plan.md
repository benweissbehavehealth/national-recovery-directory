# Phase 2: NARR Affiliate Website Analysis Plan

## Objective
Visit all 29 NARR affiliate websites and extract comprehensive data about their certified recovery residences to build a master directory.

## Approach
Use the web-research-analyst agent to perform systematic, top-to-bottom analysis of each affiliate website.

## Target Data Points

### Certified Organizations
- Organization name
- Business/Trade names
- Certification level (Level I-IV as per NARR standards)
- Certification status (Active, Pending, Expired)
- Certification date
- License numbers/IDs

### Location Information
- Physical addresses
- Service areas
- Multiple locations (if applicable)
- Geographic coordinates (if available)

### Contact Information
- Phone numbers
- Email addresses
- Website URLs
- Emergency contacts

### Service Details
- Capacity (number of beds/residents)
- Target populations served
- Services offered
- Specializations
- Admission criteria

### Operational Information
- Hours of operation
- Accessibility features
- Payment methods accepted
- Insurance accepted

## Affiliate Website Priority List

Based on the extracted affiliate data, here are the 29 websites to analyze:

### High Priority (Large States/Comprehensive Sites)
1. **California** - https://ccapp.us
2. **Florida** - https://www.farronline.org
3. **Texas** - https://trohn.org
4. **New York** - https://www.nysarr.org/
5. **Pennsylvania** - https://www.parronline.org
6. **Ohio** - https://www.ohiorecoveryhousing.org

### Medium Priority (Established Programs)
7. **Georgia** - https://www.thegarrnetwork.org
8. **Massachusetts** - https://mashsoberhousing.org
9. **Virginia** - https://varronline.org
10. **Michigan** - https://michiganarr.com
11. **North Carolina** - https://ncarr.org
12. **Washington** - https://www.waqrr.org

### Standard Priority (All Remaining States)
13. **Alabama** - https://aarronline.org/
14. **Arizona** - https://myazrha.org
15. **Colorado** - https://carrcolorado.org
16. **Connecticut** - https://ctrecoveryresidences.org
17. **Delaware** - https://fsarr.org
18. **Illinois** - https://www.iaecrecoveryillinois.org
19. **Indiana** - https://inarr.org
20. **Kentucky** - https://dbhdid.ky.gov/dbh/krhn.aspx
21. **Maine** - https://www.mainerecoveryresidences.com
22. **Minnesota** - https://www.mnsoberhomes.org
23. **Missouri** - https://mcrsp.org
24. **New Hampshire** - https://www.nhcorr.org
25. **Oklahoma** - https://okarr.org
26. **Oregon** - https://www.mhacbo.org
27. **Rhode Island** - https://ricares.org
28. **Tennessee** - https://www.tnarr.com
29. **Vermont** - https://vtarr.org
30. **West Virginia** - https://wvarr.org
31. **Wisconsin** - https://washcommunity.org/home-1

*Note: South Carolina (SCARR) has no active website*

## Data Structure Template

```json
{
  "affiliate_state": "State Name",
  "affiliate_name": "Organization Name",
  "extraction_date": "2025-07-30",
  "website_url": "https://example.org",
  "certified_organizations": [
    {
      "name": "Organization Name",
      "certification_id": "CERT-123",
      "certification_level": "Level II",
      "certification_status": "Active",
      "certification_date": "2024-01-15",
      "locations": [
        {
          "address": "123 Main St, City, State ZIP",
          "phone": "555-123-4567",
          "email": "contact@org.com",
          "capacity": 12,
          "services": ["Residential", "Transitional"],
          "specializations": ["Men", "Veterans"]
        }
      ],
      "website": "https://organization.com",
      "contact_info": {
        "primary_phone": "555-123-4567",
        "primary_email": "info@org.com"
      }
    }
  ]
}
```

## Quality Assurance
- Verify data accuracy through cross-referencing
- Check for duplicate organizations across states
- Validate contact information where possible
- Document any data limitations or missing information

## Expected Timeline
- **High Priority Sites**: 1-2 days per site (6 sites)
- **Medium Priority Sites**: 1 day per site (6 sites) 
- **Standard Priority Sites**: 0.5 days per site (16 sites)
- **Total Estimated Time**: 2-3 weeks for comprehensive analysis

## Next Steps
1. Begin with high-priority states (California, Florida, Texas)
2. Use web-research-analyst for systematic site analysis
3. Create standardized data extraction templates
4. Build aggregated master directory
5. Prepare for Phase 3 individual organization research