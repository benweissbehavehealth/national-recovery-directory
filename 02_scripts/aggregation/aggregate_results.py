#!/usr/bin/env python3
"""
Aggregate all extraction results into updated master directory
"""

import json
import os
from datetime import datetime

def load_json(filepath):
    """Load JSON file if it exists"""
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            return json.load(f)
    return None

def aggregate_results():
    # Load existing master directory (from new file structure)
    master = load_json('../../04_processed_data/master_directories/master_directory.json')
    if not master:
        print("Master directory not found, creating new one")
        master = {
            "extraction_date": datetime.now().strftime("%Y-%m-%d"),
            "total_organizations_found": 0,
            "organizations_by_state": {}
        }
    
    # Keep track of current highest ID
    current_orgs = []
    for state_orgs in master.get('organizations_by_state', {}).values():
        current_orgs.extend(state_orgs)
    
    max_id = 0
    for org in current_orgs:
        if org.get('id', '').startswith('ORG_'):
            try:
                org_num = int(org['id'].split('_')[1])
                max_id = max(max_id, org_num)
            except:
                pass
    
    next_id = max_id + 1
    
    # Process Texas TROHN results
    print("Processing Texas TROHN results...")
    trohn_data = load_json('../../03_raw_data/narr_organizations/texas/trohn_comprehensive_results.json')
    if trohn_data:
        texas_orgs = []
        for operator, residences in trohn_data.get('residences_by_operator', {}).items():
            for res in residences:
                org = {
                    "id": f"ORG_{next_id:04d}",
                    "name": res.get('name', ''),
                    "state": "Texas",
                    "affiliate": "TROHN",
                    "affiliate_website": "https://trohn.org",
                    "operator": operator,
                    "phone": res.get('phone', ''),
                    "email": res.get('email', ''),
                    "website": res.get('website', ''),
                    "address": res.get('address', ''),
                    "city": res.get('location', ''),
                    "services": res.get('services', ''),
                    "certification_level": res.get('certification_level', ''),
                    "capacity": res.get('capacity', ''),
                    "specializations": res.get('specializations', []),
                    "data_source": "trohn_comprehensive_results.json",
                    "extraction_date": "2025-07-30"
                }
                texas_orgs.append(org)
                next_id += 1
        
        master['organizations_by_state']['Texas'] = texas_orgs
        print(f"Added {len(texas_orgs)} Texas organizations")
    
    # Process Georgia GARR results
    print("Processing Georgia GARR results...")
    garr_data = load_json('../../03_raw_data/narr_organizations/georgia/garr_certified_organizations.json')
    if garr_data:
        georgia_orgs = []
        for level_key, level_data in garr_data.get('certification_levels', {}).items():
            level_name = level_key.replace('_', ' ').title()
            for org_data in level_data.get('organizations', []):
                org = {
                    "id": f"ORG_{next_id:04d}",
                    "name": org_data.get('name', ''),
                    "state": "Georgia",
                    "affiliate": "GARR",
                    "affiliate_website": "https://www.thegarrnetwork.org",
                    "phone": org_data.get('phone', ''),
                    "email": org_data.get('email', ''),
                    "website": org_data.get('website', ''),
                    "address": org_data.get('address', ''),
                    "services": org_data.get('services', ''),
                    "certification_level": level_name,
                    "garr_tier": org_data.get('certification_level', ''),
                    "capacity": org_data.get('capacity', ''),
                    "specializations": org_data.get('specializations', []),
                    "demographics": org_data.get('demographics', ''),
                    "pricing": org_data.get('pricing', ''),
                    "faith_based": org_data.get('faith_based', False),
                    "data_source": "garr_certified_organizations.json",
                    "extraction_date": "2025-07-30"
                }
                georgia_orgs.append(org)
                next_id += 1
        
        master['organizations_by_state']['Georgia'] = georgia_orgs
        print(f"Added {len(georgia_orgs)} Georgia organizations")
    
    # Process Michigan MARR results
    print("Processing Michigan MARR results...")
    michigan_data = load_json('../../03_raw_data/narr_organizations/michigan/michigan_marr_operators.json')
    if michigan_data:
        michigan_orgs = []
        for operator in michigan_data.get('operators', []):
            # Create organization entry for each operator
            org = {
                "id": f"ORG_{next_id:04d}",
                "name": operator.get('name', ''),
                "state": "Michigan",
                "affiliate": "MARR",
                "affiliate_website": "https://michiganarr.com",
                "operator_type": "Recovery Residence Operator",
                "phone": operator.get('phone', ''),
                "email": operator.get('email', ''),
                "website": operator.get('website', ''),
                "address": operator.get('address', ''),
                "certification_level": operator.get('level', ''),
                "certification_number": operator.get('certification_number', ''),
                "counties_served": operator.get('counties', []),
                "housing_types": operator.get('housing_types', []),
                "capacity": {
                    "total_residences": operator.get('residences', 0),
                    "total_beds": operator.get('beds', 0),
                    "mens_beds": operator.get('mens_beds', 0),
                    "womens_beds": operator.get('womens_beds', 0),
                    "family_beds": operator.get('family_beds', 0)
                },
                "contact_person": operator.get('contact', ''),
                "data_source": "michigan_marr_operators.json",
                "extraction_date": "2025-07-30"
            }
            michigan_orgs.append(org)
            next_id += 1
        
        master['organizations_by_state']['Michigan'] = michigan_orgs
        print(f"Added {len(michigan_orgs)} Michigan operators")
    
    # Process Pennsylvania statewide results
    print("Processing Pennsylvania statewide results...")
    pa_data = load_json('../../03_raw_data/narr_organizations/pennsylvania/pennsylvania_statewide_directory.json')
    if pa_data:
        pennsylvania_orgs = []
        
        # Process organizations from certified_organizations section
        cert_orgs = pa_data.get('certified_organizations', {})
        for region, orgs_list in cert_orgs.items():
            if isinstance(orgs_list, list):
                for org_data in orgs_list:
                    contact = org_data.get('contact', {})
                    org = {
                        "id": f"ORG_{next_id:04d}",
                        "name": org_data.get('name', ''),
                        "state": "Pennsylvania",
                        "affiliate": "PARR",
                        "affiliate_website": "https://www.parronline.org",
                        "region": region.replace('_', ' ').title(),
                        "phone": contact.get('phone', ''),
                        "email": contact.get('email', ''),
                        "website": contact.get('website', ''),
                        "address": org_data.get('address', ''),
                        "county": org_data.get('county', ''),
                        "population_served": org_data.get('population_served', ''),
                        "houses": org_data.get('houses', ''),
                        "type": org_data.get('type', ''),
                        "contact_person": contact.get('name', ''),
                        "special_notes": org_data.get('special_notes', ''),
                        "data_source": "pennsylvania_statewide_directory.json",
                        "extraction_date": "2025-07-30"
                    }
                    pennsylvania_orgs.append(org)
                    next_id += 1
        
        # Merge with existing Pennsylvania organizations if any
        existing_pa = master['organizations_by_state'].get('Pennsylvania Parr', [])
        
        # Update the Pennsylvania entry with new comprehensive data
        master['organizations_by_state']['Pennsylvania'] = pennsylvania_orgs
        
        print(f"Added {len(pennsylvania_orgs)} Pennsylvania organizations")
    
    # Update totals
    total = 0
    for state_orgs in master['organizations_by_state'].values():
        total += len(state_orgs)
    
    master['total_organizations_found'] = total
    master['extraction_date'] = datetime.now().strftime("%Y-%m-%d")
    master['last_update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Update data quality summary
    master['data_quality_summary'] = {
        "comprehensive_directories": [
            "Massachusetts MASH (204 homes)",
            "Georgia GARR (44 organizations)",
            "Michigan MARR (47 operators)",
            "Pennsylvania PARR (35 organizations)"
        ],
        "partial_directories": [
            "Texas TROHN (30 of 78 residences)"
        ],
        "requires_direct_contact": [
            "Arizona AzRHA (GetHelp widget barrier)",
            "Colorado CARR (JavaScript search barrier)"
        ]
    }
    
    # Save updated master directory
    with open('../../04_processed_data/master_directories/master_directory_updated.json', 'w') as f:
        json.dump(master, f, indent=2)
    
    print(f"\nAggregation complete!")
    print(f"Total organizations in updated directory: {total}")
    print(f"Saved to: 04_processed_data/master_directories/master_directory_updated.json")
    
    # Generate summary report
    summary = {
        "aggregation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_organizations": total,
        "organizations_by_state": {
            state: len(orgs) for state, orgs in master['organizations_by_state'].items()
        },
        "new_additions": {
            "Texas": len(texas_orgs) if 'texas_orgs' in locals() else 0,
            "Georgia": len(georgia_orgs) if 'georgia_orgs' in locals() else 0,
            "Michigan": len(michigan_orgs) if 'michigan_orgs' in locals() else 0,
            "Pennsylvania": len(pennsylvania_orgs) if 'pennsylvania_orgs' in locals() else 0
        }
    }
    
    with open('../../04_processed_data/master_directories/aggregation_summary.json', 'w') as f:
        json.dump(summary, f, indent=2)
    
    return summary

if __name__ == "__main__":
    summary = aggregate_results()
    print("\nSummary by state:")
    for state, count in summary['organizations_by_state'].items():
        print(f"  {state}: {count} organizations")