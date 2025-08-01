#!/usr/bin/env python3
"""
Integrate SAMHSA data with existing treatment centers and update comprehensive directory
"""

import json
import os
from datetime import datetime
from pathlib import Path

def load_json(filepath):
    """Load JSON file if it exists"""
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            return json.load(f)
    return None

def integrate_samhsa_data():
    """Integrate SAMHSA data with existing treatment centers"""
    base_path = Path(__file__).parent.parent.parent
    
    # Load existing data
    print("Loading existing data...")
    samhsa_data = load_json(base_path / "03_raw_data" / "samhsa_facilities_processed.json")
    existing_treatment = load_json(base_path / "04_processed_data" / "master_directories" / "treatment_centers_master.json")
    
    if not samhsa_data:
        print("SAMHSA data not found!")
        return
    
    print(f"Loaded {len(samhsa_data['facilities'])} SAMHSA facilities")
    
    # Initialize combined data structure
    combined_facilities = []
    stats = {
        'existing_facilities': 0,
        'samhsa_facilities': len(samhsa_data['facilities']),
        'total_combined': 0,
        'by_level': {
            'outpatient': 0,
            'residential': 0, 
            'inpatient': 0
        },
        'by_state': {},
        'duplicates_removed': 0
    }
    
    # Add existing treatment centers if they exist
    if existing_treatment and 'all_facilities' in existing_treatment:
        print(f"Adding {len(existing_treatment['all_facilities'])} existing treatment facilities...")
        for facility in existing_treatment['all_facilities']:
            combined_facilities.append(facility)
            stats['existing_facilities'] += 1
            
            level = facility.get('level_of_care', 'outpatient')
            if level in stats['by_level']:
                stats['by_level'][level] += 1
            
            state = facility.get('state', 'Unknown')
            if state not in stats['by_state']:
                stats['by_state'][state] = {'total': 0, 'outpatient': 0, 'residential': 0, 'inpatient': 0}
            stats['by_state'][state]['total'] += 1
            if level in stats['by_state'][state]:
                stats['by_state'][state][level] += 1
    
    # Track facilities for deduplication (simple name + state matching)
    seen_facilities = set()
    for facility in combined_facilities:
        name = facility.get('name', '').lower().strip()
        state = facility.get('state', facility.get('address', {}).get('state', '')).lower().strip()
        seen_facilities.add(f"{name}|{state}")
    
    # Add SAMHSA facilities (avoiding duplicates)
    print("Integrating SAMHSA facilities...")
    for facility in samhsa_data['facilities']:
        name = facility.get('name', '').lower().strip()
        state = facility.get('address', {}).get('state', '').lower().strip()
        facility_key = f"{name}|{state}"
        
        # Check for potential duplicate
        if facility_key in seen_facilities:
            stats['duplicates_removed'] += 1
            continue
        
        seen_facilities.add(facility_key)
        
        # Standardize facility format to match existing structure
        standardized_facility = {
            'id': facility['id'],
            'name': facility['name'],
            'level_of_care': facility['level_of_care'],
            'address': facility['address']['street'],
            'city': facility['address']['city'],
            'state': facility['address']['state'],
            'zip': facility['address']['zip'],
            'county': facility['address'].get('county', ''),
            'phone': facility['contact']['phone'],
            'website': facility['contact']['website'],
            'services': facility['services'],
            'populations_served': facility.get('populations_served', []),
            'insurance_accepted': facility.get('insurance_accepted', []),
            'coordinates': {
                'latitude': facility['location'].get('latitude', ''),
                'longitude': facility['location'].get('longitude', '')
            },
            'data_source': 'SAMHSA Treatment Locator',
            'extraction_date': facility['extraction_date'],
            'facility_details': {
                'facility_type': facility.get('facility_type', ''),
                'mh_services': facility['raw_data'].get('mh_services', False),
                'sa_services': facility['raw_data'].get('sa_services', False),
                'detox_available': facility['raw_data'].get('detox', False),
                'methadone_available': facility['raw_data'].get('methadone', False),
                'otp_available': facility['raw_data'].get('otp', False)
            }
        }
        
        combined_facilities.append(standardized_facility)
        
        # Update statistics
        level = facility['level_of_care']
        stats['by_level'][level] += 1
        
        state = facility['address']['state']
        if state not in stats['by_state']:
            stats['by_state'][state] = {'total': 0, 'outpatient': 0, 'residential': 0, 'inpatient': 0}
        stats['by_state'][state]['total'] += 1
        stats['by_state'][state][level] += 1
    
    stats['total_combined'] = len(combined_facilities)
    
    # Create updated master directory
    updated_master = {
        'metadata': {
            'title': 'Enhanced Treatment Centers Master Directory',
            'description': 'Combined treatment facilities from multiple sources including SAMHSA',
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'sources': [
                'State licensing databases',
                'SAMHSA Treatment Locator',
                'Massachusetts MASH API',
                'Manual extraction from state websites'
            ]
        },
        'statistics': stats,
        'total_facilities': stats['total_combined'],
        'all_facilities': combined_facilities
    }
    
    # Save updated master directory
    output_path = base_path / "04_processed_data" / "master_directories" / "treatment_centers_master.json"
    with open(output_path, 'w') as f:
        json.dump(updated_master, f, indent=2)
    
    print(f"\n{'='*60}")
    print(f"SAMHSA INTEGRATION COMPLETE")
    print(f"{'='*60}")
    print(f"Existing Facilities: {stats['existing_facilities']:,}")
    print(f"SAMHSA Facilities Added: {stats['samhsa_facilities'] - stats['duplicates_removed']:,}")
    print(f"Duplicates Removed: {stats['duplicates_removed']:,}")
    print(f"Total Combined Facilities: {stats['total_combined']:,}")
    print(f"\nUpdated Breakdown by Level of Care:")
    print(f"  - Outpatient: {stats['by_level']['outpatient']:,}")
    print(f"  - Residential: {stats['by_level']['residential']:,}")
    print(f"  - Inpatient: {stats['by_level']['inpatient']:,}")
    print(f"\nStates/Territories Covered: {len(stats['by_state'])}")
    print(f"\nSaved to: {output_path}")
    
    return updated_master

if __name__ == "__main__":
    integrate_samhsa_data()