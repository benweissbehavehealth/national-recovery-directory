# Michigan MARR Operator Extraction Summary

## Extraction Results
- **Date**: July 30, 2025
- **Operators Extracted**: 47 of 71 claimed operators
- **Source**: https://michiganarr.com/full-operator-list
- **Output File**: michigan_marr_operators.json

## Key Statistics (from MARR website)
- Total Certified Operators: 71
- Total Recovery Residences: 322
- Total Beds: 2,669
  - Men's Beds: 1,649
  - Women's Beds: 479
  - Family Beds: 337
- Counties Served: 36 (including 5 in Upper Peninsula)

## Extraction Coverage by County
The 47 operators extracted cover facilities in the following counties:
- Baraga, Berrien, Calhoun, Charlevoix, Chippewa
- Delta, Dickinson, Emmet, Genesee, Grand Traverse
- Houghton, Ingham, Isabella, Jackson, Kalamazoo
- Kent, Livingston, Macomb, Marquette, Mason
- Mecosta, Menominee, Monroe, Muskegon, Oakland
- Otsego, Ottawa, Saginaw, St. Clair, St. Joseph
- Washtenaw, Wayne, Bay

## Gap Analysis
- **Missing Operators**: 24 operators (33.8% of total)
- **Possible Reasons**:
  1. Some operators may only be visible in the REC-CAP system
  2. The full operator list page may have pagination not visible in web fetch
  3. Some operators may be pending certification or recently added
  4. Member-only access may be required for complete directory

## Data Quality Notes
- Most operators have complete contact information (name, phone, email)
- Facility-level details vary in completeness
- All operators are MAT-friendly according to MARR standards
- Certification levels range from Level 2 to Level 4

## Recommendations for Complete Data
1. **Contact MARR Directly**:
   - REC-CAP Contact: Kelley Darin (kdarin@micharr.com)
   - Request access to complete operator database

2. **Access REC-CAP System**:
   - Accredited operators are required to join REC-CAP
   - May contain additional operator information

3. **Request API or Data Export**:
   - MARR uses Certemy for certification management
   - May have data export capabilities

## Next Steps
1. Contact MARR for complete operator list access
2. Integrate the 47 extracted operators into master directory
3. Schedule follow-up to obtain remaining 24 operators
4. Consider establishing ongoing data sharing agreement with MARR