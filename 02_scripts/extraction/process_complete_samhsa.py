#!/usr/bin/env python3
"""
Process complete SAMHSA dataset including both CSV files and service codes
"""

import csv
import json
import os
from datetime import datetime
from pathlib import Path

def load_service_codes():
    """Load and parse service codes reference file"""
    base_path = Path(__file__).parent.parent.parent
    codes_path = base_path / "SAMHSA Directory" / "treatment_directory_service_codes.csv"
    
    service_codes = {}
    
    if codes_path.exists():
        with open(codes_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                code = row.get('service_code', '').strip()
                if code:
                    service_codes[code] = {
                        'category_code': row.get('category_code', ''),
                        'category_name': row.get('category_name', ''),
                        'service_name': row.get('service_name', ''),
                        'service_description': row.get('service_description', ''),
                        'sa_code': row.get('sa_code', '') == 'Y',
                        'mh_code': row.get('mh_code', '') == 'Y',
                        'both_code': row.get('both_code', '') == 'Y'
                    }
    
    return service_codes

def determine_level_of_care(row):
    """Determine level of care based on facility characteristics"""
    # Check for inpatient indicators
    if (row.get('hi') == '1' or  # Hospital inpatient
        row.get('hid') == '1' or  # Hospital inpatient detox
        row.get('hit') == '1' or  # Hospital inpatient treatment
        row.get('psyh') == '1' or  # Psychiatric hospital
        row.get('vamc') == '1'):   # VA Medical Center
        return 'inpatient'
    
    # Check for residential indicators  
    elif (row.get('res') == '1' or  # Residential
          row.get('rs') == '1' or   # Residential short-term
          row.get('rl') == '1' or   # Residential long-term
          row.get('rd') == '1' or   # Residential detox
          row.get('hh') == '1'):    # Halfway house
        return 'residential'
    
    # Default to outpatient for all others
    else:
        return 'outpatient'

def extract_detailed_services(row, service_codes):
    """Extract detailed services using service codes reference"""
    services = []
    
    # Core service mappings
    service_mappings = {
        'sa': 'Substance Abuse Treatment',
        'dt': 'Detoxification',
        'mh': 'Mental Health Treatment',
        'sumh': 'Co-occurring Disorders Treatment',
        'mm': 'Methadone Maintenance',
        'otp': 'Opioid Treatment Program',
        'moa': 'Medication-Assisted Treatment',
        'psy': 'Psychiatric Services',
        'op': 'Outpatient Treatment',
        'res': 'Residential Treatment',
        'hi': 'Hospital Inpatient',
        'ph': 'Partial Hospitalization',
        'hh': 'Transitional Housing/Halfway House',
        'cbt': 'Cognitive Behavioral Therapy',
        'dbt': 'Dialectical Behavior Therapy',
        'gt': 'Group Therapy',
        'cft': 'Couples/Family Therapy',
        'ipt': 'Individual Psychotherapy',
        'at': 'Activity Therapy',
        'ect': 'Electroconvulsive Therapy',
        'tele': 'Telemedicine/Telehealth',
        'peer': 'Peer Support Services',
        'cm': 'Case Management',
        'act': 'Assertive Community Treatment',
        'icm': 'Intensive Case Management'
    }
    
    # Extract services based on row data
    for code, service_name in service_mappings.items():
        if row.get(code) == '1':
            services.append(service_name)
    
    # Add specialized services
    if row.get('vamc') == '1':
        services.append('VA Medical Center Services')
    if row.get('fqhc') == '1':
        services.append('Federally Qualified Health Center')
    if row.get('cmhc') == '1':
        services.append('Community Mental Health Center')
    
    return services

def extract_populations_served(row):
    """Extract populations served from row data"""
    populations = []
    
    population_mappings = {
        'adlt': 'Adults',
        'ped': 'Children/Adolescents',
        'yad': 'Young Adults',
        'snr': 'Seniors',
        'fem': 'Women',
        'male': 'Men',
        'vet': 'Veterans',
        'cj': 'Criminal Justice Clients',
        'dv': 'Domestic Violence Survivors',
        'tay': 'Transition Age Youth',
        'trma': 'Trauma Survivors',
        'smi': 'Serious Mental Illness',
        'sed': 'Serious Emotional Disturbance',
        'alz': 'Alzheimer\'s/Dementia',
        'ptsd': 'PTSD',
        'tbi': 'Traumatic Brain Injury'
    }
    
    for code, population in population_mappings.items():
        if row.get(code) == '1':
            populations.append(population)
            
    return populations

def extract_insurance_accepted(row):
    """Extract insurance types accepted"""
    insurance = []
    
    insurance_mappings = {
        'pi': 'Private Insurance',
        'mc': 'Medicaid',
        'md': 'Medicare',
        'si': 'State Insurance',
        'mi': 'Military Insurance',
        'sf': 'Sliding Fee Scale',
        'pa': 'Payment Assistance',
        'ss': 'State-Sponsored Insurance',
        'wi': 'Workers Compensation',
        'vaf': 'VA Funds'
    }
    
    for code, insurance_type in insurance_mappings.items():
        if row.get(code) == '1':
            insurance.append(insurance_type)
            
    return insurance

def process_csv_file(csv_path, service_codes, existing_facilities, stats):
    """Process a single CSV file"""
    print(f"Processing: {csv_path.name}")
    
    facilities = []
    seen_facilities = set()
    
    # Add existing facilities to seen set
    for facility in existing_facilities:
        name = facility.get('name', '').lower().strip()
        state = facility.get('address', {}).get('state', '').lower().strip()
        seen_facilities.add(f"{name}|{state}")
    
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row_num, row in enumerate(reader, 1):
            if row_num % 2000 == 0:
                print(f"  Processed {row_num} rows from {csv_path.name}...")
            
            # Clean and extract basic info
            name = row.get('name1', '').strip()
            name2 = row.get('name2', '').strip()
            if name2:
                name = f"{name} - {name2}"
            
            address = row.get('street1', '').strip()
            address2 = row.get('street2', '').strip()
            if address2:
                address = f"{address}, {address2}"
                
            city = row.get('city', '').strip()
            state = row.get('state', '').strip()
            zip_code = row.get('zip', '').strip()
            phone = row.get('phone', '').strip()
            website = row.get('website', '').strip()
            
            # Skip if missing essential data
            if not name or not state:
                continue
            
            # Check for duplicates
            facility_key = f"{name.lower()}|{state.lower()}"
            if facility_key in seen_facilities:
                stats['duplicates_found'] += 1
                continue
            seen_facilities.add(facility_key)
            
            # Determine level of care
            level = determine_level_of_care(row)
            
            # Generate unique ID
            level_prefix = {
                'outpatient': 'SAMHSA_OUT',
                'residential': 'SAMHSA_RES', 
                'inpatient': 'SAMHSA_INP'
            }
            facility_id = f"{level_prefix[level]}_{len(facilities) + len(existing_facilities) + 1:06d}"
            
            # Build standardized facility record
            facility = {
                'id': facility_id,
                'name': name,
                'level_of_care': level,
                'facility_type': row.get('type_facility', ''),
                'address': {
                    'street': address,
                    'city': city,
                    'state': state,
                    'zip': zip_code,
                    'county': row.get('county', '')
                },
                'contact': {
                    'phone': phone,
                    'website': website if website.startswith('http') else '',
                    'intake_phone': row.get('intake1', '').strip(),
                    'intake_phone2': row.get('intake2', '').strip()
                },
                'location': {
                    'latitude': row.get('latitude', ''),
                    'longitude': row.get('longitude', '')
                },
                'services': extract_detailed_services(row, service_codes),
                'populations_served': extract_populations_served(row),
                'insurance_accepted': extract_insurance_accepted(row),
                'data_source': 'SAMHSA Treatment Locator',
                'source_file': csv_path.name,
                'extraction_date': datetime.now().strftime('%Y-%m-%d'),
                'facility_details': {
                    'mh_services': row.get('mh') == '1',
                    'sa_services': row.get('sa') == '1',
                    'co_occurring': row.get('sumh') == '1',
                    'detox_available': row.get('dt') == '1',
                    'methadone_available': row.get('mm') == '1',
                    'otp_available': row.get('otp') == '1',
                    'hospital_inpatient': row.get('hi') == '1',
                    'residential': row.get('res') == '1',
                    'halfway_house': row.get('hh') == '1',
                    'va_facility': row.get('vamc') == '1',
                    'fqhc': row.get('fqhc') == '1'
                }
            }
            
            facilities.append(facility)
            stats['total_facilities'] += 1
            stats['by_level'][level] += 1
            
            # Update state counts
            if state not in stats['by_state']:
                stats['by_state'][state] = {'total': 0, 'outpatient': 0, 'residential': 0, 'inpatient': 0}
            stats['by_state'][state]['total'] += 1
            stats['by_state'][state][level] += 1
    
    return facilities

def process_complete_samhsa():
    """Process complete SAMHSA dataset"""
    base_path = Path(__file__).parent.parent.parent
    samhsa_dir = base_path / "SAMHSA Directory"
    
    # Load service codes
    print("Loading service codes reference...")
    service_codes = load_service_codes()
    print(f"Loaded {len(service_codes)} service codes")
    
    # Initialize statistics
    stats = {
        'total_facilities': 0,
        'by_level': {'outpatient': 0, 'residential': 0, 'inpatient': 0},
        'by_state': {},
        'duplicates_found': 0,
        'files_processed': []
    }
    
    all_facilities = []
    
    # Process both main CSV files
    csv_files = [
        "FindTreament_Facility_listing_2025_07_31_190629.csv",
        "FindTreament_Facility_listing_2025_07_31_190717.csv"
    ]
    
    for csv_file in csv_files:
        csv_path = samhsa_dir / csv_file
        if csv_path.exists():
            facilities = process_csv_file(csv_path, service_codes, all_facilities, stats)
            all_facilities.extend(facilities)
            stats['files_processed'].append(csv_file)
            print(f"Added {len(facilities)} facilities from {csv_file}")
        else:
            print(f"Warning: {csv_file} not found")
    
    # Create output data structure
    output_data = {
        'metadata': {
            'title': 'Complete SAMHSA Treatment Facilities Directory',
            'description': 'Comprehensive dataset from SAMHSA FindTreatment.gov',
            'source_files': stats['files_processed'],
            'service_codes_loaded': len(service_codes),
            'extraction_date': datetime.now().strftime('%Y-%m-%d'),
            'processing_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        },
        'statistics': stats,
        'service_codes_reference': service_codes,
        'facilities': all_facilities
    }
    
    # Save to processed data directory
    output_path = base_path / "03_raw_data" / "samhsa_complete_processed.json"
    with open(output_path, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\n{'='*70}")
    print(f"COMPLETE SAMHSA PROCESSING FINISHED")
    print(f"{'='*70}")
    print(f"Files Processed: {', '.join(stats['files_processed'])}")
    print(f"Total Facilities Processed: {stats['total_facilities']:,}")
    print(f"Duplicates Removed: {stats['duplicates_found']:,}")
    print(f"Service Codes Loaded: {len(service_codes):,}")
    print(f"\nBreakdown by Level of Care:")
    print(f"  - Outpatient: {stats['by_level']['outpatient']:,}")
    print(f"  - Residential: {stats['by_level']['residential']:,}")
    print(f"  - Inpatient: {stats['by_level']['inpatient']:,}")
    print(f"\nStates/Territories: {len(stats['by_state'])}")
    
    # Show top 15 states
    top_states = sorted(stats['by_state'].items(), key=lambda x: x[1]['total'], reverse=True)[:15]
    print(f"\nTop 15 States by Facility Count:")
    for state, counts in top_states:
        print(f"  - {state}: {counts['total']:,} facilities (OP: {counts['outpatient']:,}, RES: {counts['residential']:,}, INP: {counts['inpatient']:,})")
    
    print(f"\nSaved to: {output_path}")
    
    return output_data

if __name__ == "__main__":
    process_complete_samhsa()