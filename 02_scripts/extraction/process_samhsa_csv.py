#!/usr/bin/env python3
"""
Process SAMHSA CSV directory and convert to standardized JSON format
"""

import csv
import json
import os
from datetime import datetime
from pathlib import Path

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
          row.get('rd') == '1'):    # Residential detox
        return 'residential'
    
    # Default to outpatient for all others
    else:
        return 'outpatient'

def extract_services(row):
    """Extract services from row data"""
    services = []
    
    # Substance abuse services
    if row.get('sa') == '1':
        services.append('Substance Abuse Treatment')
    if row.get('dt') == '1':
        services.append('Detoxification')
    if row.get('mm') == '1':
        services.append('Methadone Maintenance')
    if row.get('otp') == '1':
        services.append('Opioid Treatment Program')
    if row.get('moa') == '1':
        services.append('Medication-Assisted Treatment')
    
    # Mental health services
    if row.get('mh') == '1':
        services.append('Mental Health Treatment')
    if row.get('psy') == '1':
        services.append('Psychiatric Services')
    
    # Treatment modalities
    if row.get('op') == '1':
        services.append('Outpatient Treatment')
    if row.get('res') == '1':
        services.append('Residential Treatment')
    if row.get('hi') == '1':
        services.append('Hospital Inpatient')
    if row.get('ph') == '1':
        services.append('Partial Hospitalization')
    
    # Specialized services
    if row.get('cbt') == '1':
        services.append('Cognitive Behavioral Therapy')
    if row.get('dbt') == '1':
        services.append('Dialectical Behavior Therapy')
    if row.get('gt') == '1':
        services.append('Group Therapy')
    if row.get('cft') == '1':
        services.append('Couples/Family Therapy')
    
    return services

def extract_populations_served(row):
    """Extract populations served from row data"""
    populations = []
    
    if row.get('adlt') == '1':
        populations.append('Adults')
    if row.get('ped') == '1':
        populations.append('Adolescents')
    if row.get('yad') == '1':
        populations.append('Young Adults')
    if row.get('snr') == '1':
        populations.append('Seniors')
    if row.get('fem') == '1':
        populations.append('Women')
    if row.get('male') == '1':
        populations.append('Men')
    if row.get('vet') == '1':
        populations.append('Veterans')
    if row.get('cj') == '1':
        populations.append('Criminal Justice Clients')
    if row.get('dv') == '1':
        populations.append('Domestic Violence')
    if row.get('tay') == '1':
        populations.append('Transition Age Youth')
        
    return populations

def extract_insurance_accepted(row):
    """Extract insurance types accepted"""
    insurance = []
    
    if row.get('pi') == '1':
        insurance.append('Private Insurance')
    if row.get('mc') == '1':
        insurance.append('Medicaid')
    if row.get('md') == '1':
        insurance.append('Medicare')
    if row.get('si') == '1':
        insurance.append('State Insurance')
    if row.get('mi') == '1':
        insurance.append('Military Insurance')
    if row.get('sf') == '1':
        insurance.append('Sliding Fee Scale')
    if row.get('pa') == '1':
        insurance.append('Payment Assistance')
        
    return insurance

def process_samhsa_csv():
    """Process SAMHSA CSV file and convert to standardized format"""
    base_path = Path(__file__).parent.parent.parent
    csv_path = base_path / "SAMHSA Directory" / "FindTreament_Facility_listing_2025_07_31_190629.csv"
    
    if not csv_path.exists():
        print(f"CSV file not found: {csv_path}")
        return
    
    print(f"Processing SAMHSA CSV: {csv_path}")
    
    # Statistics tracking
    stats = {
        'total_facilities': 0,
        'by_level': {'outpatient': 0, 'residential': 0, 'inpatient': 0},
        'by_state': {},
        'duplicates_found': 0
    }
    
    facilities = []
    seen_facilities = set()  # Track duplicates by name + address
    
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row_num, row in enumerate(reader, 1):
            if row_num % 1000 == 0:
                print(f"Processed {row_num} rows...")
            
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
            facility_key = f"{name.lower()}|{address.lower()}|{city.lower()}"
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
            facility_id = f"{level_prefix[level]}_{len(facilities) + 1:06d}"
            
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
                    'intake_phone': row.get('intake1', '').strip()
                },
                'location': {
                    'latitude': row.get('latitude', ''),
                    'longitude': row.get('longitude', '')
                },
                'services': extract_services(row),
                'populations_served': extract_populations_served(row),
                'insurance_accepted': extract_insurance_accepted(row),
                'data_source': 'SAMHSA Treatment Locator',
                'extraction_date': datetime.now().strftime('%Y-%m-%d'),
                'raw_data': {
                    'mh_services': row.get('mh') == '1',
                    'sa_services': row.get('sa') == '1',
                    'detox': row.get('dt') == '1',
                    'methadone': row.get('mm') == '1',
                    'otp': row.get('otp') == '1'
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
    
    # Save processed data
    output_data = {
        'metadata': {
            'title': 'SAMHSA Treatment Facilities Directory',
            'description': 'Processed from SAMHSA FindTreatment.gov facility listing',
            'source_file': 'FindTreament_Facility_listing_2025_07_31_190629.csv',
            'extraction_date': datetime.now().strftime('%Y-%m-%d'),
            'processing_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        },
        'statistics': stats,
        'facilities': facilities
    }
    
    # Save to processed data directory
    output_path = base_path / "03_raw_data" / "samhsa_facilities_processed.json"
    with open(output_path, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\n{'='*60}")
    print(f"SAMHSA CSV PROCESSING COMPLETE")
    print(f"{'='*60}")
    print(f"Total Facilities Processed: {stats['total_facilities']:,}")
    print(f"Duplicates Removed: {stats['duplicates_found']:,}")
    print(f"\nBreakdown by Level of Care:")
    print(f"  - Outpatient: {stats['by_level']['outpatient']:,}")
    print(f"  - Residential: {stats['by_level']['residential']:,}")
    print(f"  - Inpatient: {stats['by_level']['inpatient']:,}")
    print(f"\nStates/Territories: {len(stats['by_state'])}")
    
    # Show top 10 states
    top_states = sorted(stats['by_state'].items(), key=lambda x: x[1]['total'], reverse=True)[:10]
    print(f"\nTop 10 States by Facility Count:")
    for state, counts in top_states:
        print(f"  - {state}: {counts['total']:,} facilities")
    
    print(f"\nSaved to: {output_path}")
    
    return output_data

if __name__ == "__main__":
    process_samhsa_csv()