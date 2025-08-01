#!/usr/bin/env python3
"""
Process real Oxford House vacancy data and integrate into our system
"""

import json
import os
from datetime import datetime
from pathlib import Path

def process_oxford_vacancies():
    """Process the extracted Oxford House vacancy data"""
    base_path = Path(__file__).parent.parent.parent
    
    # Load the vacancy data
    vacancy_file = base_path / "oxford_house_vacancies.json"
    with open(vacancy_file, 'r') as f:
        data = json.load(f)
    
    # Process each house
    processed_houses = []
    
    for idx, house in enumerate(data['houses_with_vacancies']):
        # Generate unique ID
        state_abbr = house['state']
        house_id = f"OXFORD_{state_abbr}_{idx+1:04d}"
        
        # Calculate occupancy
        total_beds = house['total_beds']
        vacancies = house['vacancies']
        occupied = total_beds - vacancies
        occupancy_rate = round((occupied / total_beds) * 100, 1) if total_beds > 0 else 0
        
        processed_house = {
            'id': house_id,
            'name': f"Oxford House {house['name']}",
            'organization_type': 'recovery_residence',
            'certification_type': 'oxford_house',
            'is_narr_certified': False,
            'narr_classification': {
                'is_narr_certified': False,
                'certification_type': 'oxford_house',
                'certification_details': ['Oxford House Charter'],
                'classification_confidence': 1.0,
                'last_classified': datetime.now().strftime('%Y-%m-%d')
            },
            'address': {
                'street': house['address'],
                'city': house['city'],
                'state': house['state'],
                'zip': house['zip']
            },
            'contact': {
                'phone': house['phone'],
                'contact_person': house['contact_first_name'],
                'application_process': house['application_info']
            },
            'capacity': {
                'total_beds': total_beds,
                'current_vacancies': vacancies,
                'occupancy_rate': occupancy_rate
            },
            'demographics': {
                'gender': house['gender'],
                'age_restrictions': 'Adults (18+)'
            },
            'requirements': house.get('requirements', []) + [
                'Desire to stop using alcohol and drugs',
                'Willing to pay equal share of house expenses', 
                'Willing to abide by Oxford House rules'
            ],
            'services': [
                'Democratically self-run house',
                'Peer support environment',
                'Self-supporting through resident fees',
                'Drug and alcohol free environment',
                'No time limit on residency',
                'Weekly house meetings',
                'Immediate expulsion for relapse'
            ],
            'cost': {
                'structure': 'Equal share of house expenses',
                'typical_range': '$100-150 per week',
                'includes': ['Rent', 'Utilities', 'Basic supplies']
            },
            'oxford_house_details': {
                'charter_type': 'Standard Oxford House Charter',
                'house_traditions': 'Oxford House traditions and principles',
                'governance': 'Democratic self-governance',
                'officers': 'Elected every 6 months'
            },
            'vacancy_status': {
                'has_vacancies': True,
                'vacancy_count': vacancies,
                'last_updated': data['extraction_date']
            },
            'data_source': {
                'source': 'oxfordvacancies.com',
                'extraction_date': data['extraction_date'],
                'data_freshness': 'Real-time vacancy data'
            }
        }
        
        processed_houses.append(processed_house)
    
    # Create comprehensive output
    output_data = {
        'metadata': {
            'source': 'Oxford House Vacancies - Real Data',
            'extraction_timestamp': data['extraction_date'],
            'processing_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_houses_with_vacancies': len(processed_houses),
            'states_covered': len(set(h['address']['state'] for h in processed_houses)),
            'total_vacancies': sum(h['capacity']['current_vacancies'] for h in processed_houses),
            'total_beds': sum(h['capacity']['total_beds'] for h in processed_houses),
            'pipeline_version': '1.1'
        },
        'summary_statistics': {
            'by_state': {},
            'by_gender': {},
            'average_occupancy_rate': 0
        },
        'houses': processed_houses
    }
    
    # Calculate summary statistics
    state_stats = {}
    gender_stats = {'Men': 0, 'Women': 0, 'Co-ed': 0}
    total_occupancy = 0
    
    for house in processed_houses:
        state = house['address']['state']
        if state not in state_stats:
            state_stats[state] = {
                'total_houses': 0,
                'total_vacancies': 0,
                'total_beds': 0,
                'average_occupancy': 0
            }
        
        state_stats[state]['total_houses'] += 1
        state_stats[state]['total_vacancies'] += house['capacity']['current_vacancies']
        state_stats[state]['total_beds'] += house['capacity']['total_beds']
        
        gender = house['demographics']['gender']
        if gender in gender_stats:
            gender_stats[gender] += 1
        
        total_occupancy += house['capacity']['occupancy_rate']
    
    # Calculate averages
    for state in state_stats:
        beds = state_stats[state]['total_beds']
        vacancies = state_stats[state]['total_vacancies']
        if beds > 0:
            occupied = beds - vacancies
            state_stats[state]['average_occupancy'] = round((occupied / beds) * 100, 1)
    
    output_data['summary_statistics']['by_state'] = state_stats
    output_data['summary_statistics']['by_gender'] = gender_stats
    output_data['summary_statistics']['average_occupancy_rate'] = round(
        total_occupancy / len(processed_houses), 1
    ) if processed_houses else 0
    
    # Save processed data
    output_dir = base_path / "03_raw_data" / "oxford_house_data"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = output_dir / f"oxford_house_processed_{timestamp}.json"
    
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    # Also save as latest
    latest_file = output_dir / "oxford_house_processed_latest.json"
    with open(latest_file, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    # Print summary
    print("=== OXFORD HOUSE VACANCY PROCESSING COMPLETE ===")
    print(f"\nProcessed {len(processed_houses)} houses with vacancies")
    print(f"Total vacancies: {output_data['metadata']['total_vacancies']}")
    print(f"Total beds: {output_data['metadata']['total_beds']}")
    print(f"Average occupancy: {output_data['summary_statistics']['average_occupancy_rate']}%")
    print(f"\nStates covered: {', '.join(sorted(state_stats.keys()))}")
    print(f"\nBreakdown by state:")
    for state, stats in sorted(state_stats.items()):
        print(f"  {state}: {stats['total_houses']} houses, {stats['total_vacancies']} vacancies, {stats['average_occupancy']}% occupancy")
    print(f"\nGender breakdown:")
    for gender, count in gender_stats.items():
        if count > 0:
            print(f"  {gender}: {count} houses")
    
    print(f"\nData saved to: {output_file}")
    print(f"Latest version: {latest_file}")
    
    return output_data

if __name__ == "__main__":
    process_oxford_vacancies()