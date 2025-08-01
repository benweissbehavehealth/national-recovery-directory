#!/usr/bin/env python3
"""
Integrate complete SAMHSA dataset with existing treatment centers
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

def normalize_name(name):
    """Normalize facility name for comparison"""
    if not name:
        return ""
    return name.lower().strip().replace(',', '').replace('.', '').replace('-', ' ')

def is_duplicate(facility1, facility2):
    """Check if two facilities are duplicates using multiple criteria"""
    name1 = normalize_name(facility1.get('name', ''))
    name2 = normalize_name(facility2.get('name', ''))
    
    # Get states - handle different data structures
    state1 = ''
    if 'state' in facility1:
        state1 = str(facility1['state']).upper()
    elif 'address' in facility1 and isinstance(facility1['address'], dict):
        state1 = str(facility1['address'].get('state', '')).upper()
    
    state2 = ''
    if 'state' in facility2:
        state2 = str(facility2['state']).upper()
    elif 'address' in facility2 and isinstance(facility2['address'], dict):
        state2 = str(facility2['address'].get('state', '')).upper()
    
    # Must be same state
    if state1 != state2:
        return False
    
    # Exact name match
    if name1 == name2:
        return True
    
    # Check if one name contains the other (for different naming variations)
    if len(name1) > 10 and len(name2) > 10:
        if name1 in name2 or name2 in name1:
            return True
    
    return False

def integrate_complete_samhsa():
    """Integrate complete SAMHSA dataset with existing treatment centers"""
    base_path = Path(__file__).parent.parent.parent
    
    # Load data
    print("Loading complete SAMHSA dataset...")
    complete_samhsa = load_json(base_path / "03_raw_data" / "samhsa_complete_processed.json")
    
    print("Loading existing treatment data...")
    existing_treatment = load_json(base_path / "04_processed_data" / "master_directories" / "treatment_centers_master.json")
    
    if not complete_samhsa:
        print("Complete SAMHSA data not found!")
        return
    
    print(f"Loaded {len(complete_samhsa['facilities'])} SAMHSA facilities")
    
    # Initialize data structures
    all_facilities = []
    stats = {
        'original_existing': 0,
        'samhsa_total': len(complete_samhsa['facilities']),
        'samhsa_added': 0,
        'duplicates_removed': 0,
        'final_total': 0,
        'by_level': {
            'outpatient': 0,
            'residential': 0,
            'inpatient': 0
        },
        'by_state': {},
        'by_source': {
            'existing_sources': 0,
            'samhsa': 0
        }
    }
    
    # Add existing facilities (non-SAMHSA)
    if existing_treatment and 'all_facilities' in existing_treatment:
        print(f"Adding {len(existing_treatment['all_facilities'])} existing facilities...")
        for facility in existing_treatment['all_facilities']:
            # Skip if already from SAMHSA to avoid old SAMHSA duplicates
            if facility.get('data_source') == 'SAMHSA Treatment Locator':
                continue
                
            all_facilities.append(facility)
            stats['original_existing'] += 1
            stats['by_source']['existing_sources'] += 1
            
            # Update level counts
            level = facility.get('level_of_care', 'outpatient')
            if level in stats['by_level']:
                stats['by_level'][level] += 1
            
            # Update state counts
            state = facility.get('state', facility.get('address', {}).get('state', 'Unknown'))
            if state not in stats['by_state']:
                stats['by_state'][state] = {'total': 0, 'outpatient': 0, 'residential': 0, 'inpatient': 0}
            stats['by_state'][state]['total'] += 1
            if level in stats['by_state'][state]:
                stats['by_state'][state][level] += 1
    
    # Process SAMHSA facilities
    print("Integrating complete SAMHSA dataset...")
    samhsa_facilities_added = 0
    
    for idx, samhsa_facility in enumerate(complete_samhsa['facilities']):
        if idx % 2000 == 0 and idx > 0:
            print(f"  Processed {idx:,} SAMHSA facilities...")
        
        # Check for duplicates against existing facilities
        is_duplicate_found = False
        for existing_facility in all_facilities:
            if is_duplicate(samhsa_facility, existing_facility):
                stats['duplicates_removed'] += 1
                is_duplicate_found = True
                break
        
        if is_duplicate_found:
            continue
        
        # Standardize SAMHSA facility format
        standardized_facility = {
            'id': samhsa_facility['id'],
            'name': samhsa_facility['name'],
            'level_of_care': samhsa_facility['level_of_care'],
            'address': samhsa_facility['address']['street'],
            'city': samhsa_facility['address']['city'],
            'state': samhsa_facility['address']['state'],
            'zip': samhsa_facility['address']['zip'],
            'county': samhsa_facility['address'].get('county', ''),
            'phone': samhsa_facility['contact']['phone'],
            'website': samhsa_facility['contact']['website'],
            'services': samhsa_facility['services'],
            'populations_served': samhsa_facility.get('populations_served', []),
            'insurance_accepted': samhsa_facility.get('insurance_accepted', []),
            'coordinates': {
                'latitude': samhsa_facility['location'].get('latitude', ''),
                'longitude': samhsa_facility['location'].get('longitude', '')
            },
            'data_source': 'SAMHSA Treatment Locator (Complete)',
            'source_file': samhsa_facility.get('source_file', ''),
            'extraction_date': samhsa_facility['extraction_date'],
            'facility_details': {
                'facility_type': samhsa_facility.get('facility_type', ''),
                'mh_services': samhsa_facility['facility_details'].get('mh_services', False),
                'sa_services': samhsa_facility['facility_details'].get('sa_services', False),
                'co_occurring': samhsa_facility['facility_details'].get('co_occurring', False),
                'detox_available': samhsa_facility['facility_details'].get('detox_available', False),
                'methadone_available': samhsa_facility['facility_details'].get('methadone_available', False),
                'otp_available': samhsa_facility['facility_details'].get('otp_available', False),
                'hospital_inpatient': samhsa_facility['facility_details'].get('hospital_inpatient', False),
                'residential': samhsa_facility['facility_details'].get('residential', False),
                'halfway_house': samhsa_facility['facility_details'].get('halfway_house', False),
                'va_facility': samhsa_facility['facility_details'].get('va_facility', False),
                'fqhc': samhsa_facility['facility_details'].get('fqhc', False)
            }
        }
        
        all_facilities.append(standardized_facility)
        samhsa_facilities_added += 1
        stats['samhsa_added'] += 1
        stats['by_source']['samhsa'] += 1
        
        # Update statistics
        level = samhsa_facility['level_of_care']
        stats['by_level'][level] += 1
        
        state = samhsa_facility['address']['state']
        if state not in stats['by_state']:
            stats['by_state'][state] = {'total': 0, 'outpatient': 0, 'residential': 0, 'inpatient': 0}
        stats['by_state'][state]['total'] += 1
        stats['by_state'][state][level] += 1
    
    stats['final_total'] = len(all_facilities)
    
    # Create enhanced master directory
    enhanced_master = {
        'metadata': {
            'title': 'Complete Treatment Centers Master Directory',
            'description': 'Comprehensive treatment facilities from all sources including complete SAMHSA dataset',
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'sources': [
                'Complete SAMHSA Treatment Locator Dataset (both CSV files)',
                'State licensing databases',
                'Massachusetts MASH API',
                'Manual extraction from state websites'
            ],
            'samhsa_files_processed': complete_samhsa['metadata']['source_files'],
            'service_codes_included': complete_samhsa['metadata']['service_codes_loaded']
        },
        'statistics': stats,
        'total_facilities': stats['final_total'],
        'samhsa_service_codes': complete_samhsa.get('service_codes_reference', {}),
        'all_facilities': all_facilities
    }
    
    # Save enhanced master directory
    output_path = base_path / "04_processed_data" / "master_directories" / "treatment_centers_master.json"
    with open(output_path, 'w') as f:
        json.dump(enhanced_master, f, indent=2)
        
    # Save backup with timestamp
    backup_path = base_path / "04_processed_data" / "master_directories" / f"treatment_centers_master_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(backup_path, 'w') as f:
        json.dump(enhanced_master, f, indent=2)
    
    print(f"\n{'='*70}")
    print(f"COMPLETE SAMHSA INTEGRATION FINISHED")
    print(f"{'='*70}")
    print(f"Original Existing Facilities: {stats['original_existing']:,}")
    print(f"SAMHSA Facilities Available: {stats['samhsa_total']:,}")
    print(f"SAMHSA Facilities Added: {stats['samhsa_added']:,}")
    print(f"Duplicates Removed: {stats['duplicates_removed']:,}")
    print(f"Final Total Facilities: {stats['final_total']:,}")
    print(f"\nFinal Breakdown by Level of Care:")
    print(f"  - Outpatient: {stats['by_level']['outpatient']:,}")
    print(f"  - Residential: {stats['by_level']['residential']:,}")
    print(f"  - Inpatient: {stats['by_level']['inpatient']:,}")
    print(f"\nBy Data Source:")
    print(f"  - Existing Sources: {stats['by_source']['existing_sources']:,}")
    print(f"  - SAMHSA Complete: {stats['by_source']['samhsa']:,}")
    print(f"\nStates/Territories Covered: {len(stats['by_state'])}")
    
    # Show top 15 states
    top_states = sorted(stats['by_state'].items(), key=lambda x: x[1]['total'], reverse=True)[:15]
    print(f"\nTop 15 States by Total Facilities:")
    for state, counts in top_states:
        print(f"  - {state}: {counts['total']:,} facilities (OP: {counts['outpatient']:,}, RES: {counts['residential']:,}, INP: {counts['inpatient']:,})")
    
    print(f"\nSaved to: {output_path}")
    print(f"Backup saved to: {backup_path}")
    
    return enhanced_master

if __name__ == "__main__":
    integrate_complete_samhsa()