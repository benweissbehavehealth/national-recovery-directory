#!/usr/bin/env python3
"""
Merge MASH certified homes into the master directory
"""

import json
from datetime import datetime

def load_json_file(filename):
    """Load JSON data from file"""
    with open(filename, 'r', encoding='utf-8') as f:
        return json.load(f)

def merge_mash_data():
    """Merge MASH homes into master directory"""
    
    # Load existing master directory
    master_data = load_json_file('master_directory.json')
    
    # Load MASH certified homes
    mash_data = load_json_file('mash_certified_homes.json')
    
    # Initialize Massachusetts in the master directory if not exists
    if 'Massachusetts Mash' not in master_data['organizations_by_state']:
        master_data['organizations_by_state']['Massachusetts Mash'] = []
    
    # Get current org count for ID generation
    current_org_count = master_data['total_organizations_found']
    
    # Process each MASH home
    mash_orgs = []
    for idx, home in enumerate(mash_data['certified_homes']):
        org_id = f"ORG_{(current_org_count + idx + 1):04d}"
        
        # Build full address
        address_parts = []
        if home['address']['street']:
            address_parts.append(home['address']['street'])
        if home['address'].get('street2'):
            address_parts.append(home['address']['street2'])
        if home['address']['city']:
            address_parts.append(home['address']['city'])
        if home['address']['state'] and home['address']['zip']:
            address_parts.append(f"{home['address']['state']} {home['address']['zip']}")
        
        full_address = ", ".join(address_parts) if address_parts else ""
        
        # Extract primary contact
        contact_phone = ""
        contact_email = ""
        if home['public_contacts']:
            contact = home['public_contacts'][0]
            contact_phone = contact.get('phone', '')
            contact_email = contact.get('email', '')
        
        # Create organization entry
        org_entry = {
            "id": org_id,
            "name": home['name'],
            "state": "Massachusetts Mash",
            "affiliate_website": "https://mashsoberhousing.org",
            "phone": contact_phone,
            "email": contact_email,
            "website": home['website'],
            "address": full_address,
            "services": f"{home['service_type']} recovery residence",
            "certification_level": "MASH Certified",
            "capacity": str(home['capacity']) if home['capacity'] else "",
            "specializations": [],
            "data_source": "mash_certified_homes.json",
            "data_quality": "Complete",
            "region": home['region'],
            "handicap_accessible": home['handicap_accessible'],
            "languages": home.get('languages', ''),
            "weekly_fee": str(home.get('weekly_fee', '')) if home.get('weekly_fee') else "",
            "monthly_fee": str(home.get('monthly_fee', '')) if home.get('monthly_fee') else ""
        }
        
        # Add specializations based on data
        if home['handicap_accessible']:
            org_entry['specializations'].append("Handicap Accessible")
        if home.get('languages'):
            org_entry['specializations'].append(f"Languages: {home['languages']}")
        
        mash_orgs.append(org_entry)
    
    # Update master directory
    master_data['organizations_by_state']['Massachusetts Mash'] = mash_orgs
    master_data['total_organizations_found'] = current_org_count + len(mash_orgs)
    master_data['extraction_date'] = datetime.now().strftime('%Y-%m-%d')
    
    # Update data quality summary
    if 'Massachusetts Mash' not in master_data['data_quality_summary']['comprehensive_directories']:
        master_data['data_quality_summary']['comprehensive_directories'].append('Massachusetts Mash')
    
    # Add MASH-specific summary
    master_data['mash_summary'] = {
        "total_homes": len(mash_orgs),
        "by_service_type": mash_data['summary']['by_service_type'],
        "by_region": mash_data['summary']['by_region'],
        "handicap_accessible_count": mash_data['summary']['handicap_accessible_count'],
        "extraction_date": mash_data['summary']['extraction_date']
    }
    
    # Save updated master directory
    with open('master_directory.json', 'w', encoding='utf-8') as f:
        json.dump(master_data, f, indent=2, ensure_ascii=False)
    
    print(f"Successfully merged {len(mash_orgs)} MASH certified homes into master directory")
    print(f"Total organizations in master directory: {master_data['total_organizations_found']}")
    
    # Create a summary report
    summary = {
        "merge_date": datetime.now().isoformat(),
        "mash_homes_added": len(mash_orgs),
        "previous_total": current_org_count,
        "new_total": master_data['total_organizations_found'],
        "states_with_data": list(master_data['organizations_by_state'].keys())
    }
    
    with open('mash_merge_summary.json', 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    return summary

if __name__ == "__main__":
    print("Merging MASH data into master directory...")
    print("="*50)
    
    summary = merge_mash_data()
    
    print("\nMerge Summary:")
    print(f"- MASH homes added: {summary['mash_homes_added']}")
    print(f"- Previous total: {summary['previous_total']}")
    print(f"- New total: {summary['new_total']}")
    print(f"- States with data: {len(summary['states_with_data'])}")
    
    print("\nMerge complete!")