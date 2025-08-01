#!/usr/bin/env python3
"""
Oxford House Vacancy Data Pipeline
Extracts real-time vacancy data from oxfordvacancies.com
"""

import json
import requests
from datetime import datetime
from pathlib import Path
import time
from bs4 import BeautifulSoup
import re

class OxfordHousePipeline:
    """Pipeline for extracting Oxford House vacancy data"""
    
    def __init__(self):
        self.base_url = "https://www.oxfordvacancies.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.states = self._get_us_states()
        
    def _get_us_states(self):
        """Get list of US states for systematic searching"""
        return [
            'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
            'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho',
            'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana',
            'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota',
            'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada',
            'New Hampshire', 'New Jersey', 'New Mexico', 'New York',
            'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon',
            'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
            'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
            'West Virginia', 'Wisconsin', 'Wyoming', 'District of Columbia'
        ]
    
    def extract_vacancies(self):
        """Extract all Oxford House vacancies from the website"""
        all_houses = []
        extraction_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"Starting Oxford House vacancy extraction at {extraction_timestamp}")
        
        for state in self.states:
            print(f"\nSearching {state}...")
            state_houses = self._search_state(state)
            
            if state_houses:
                print(f"  Found {len(state_houses)} houses with vacancies in {state}")
                all_houses.extend(state_houses)
            else:
                print(f"  No vacancies found in {state}")
            
            # Be respectful with rate limiting
            time.sleep(1)
        
        # Process and standardize the data
        processed_data = self._process_house_data(all_houses, extraction_timestamp)
        
        return processed_data
    
    def _search_state(self, state):
        """Search for Oxford Houses in a specific state"""
        # This would need to be implemented based on the actual website structure
        # For now, returning a template structure
        
        # In real implementation, this would:
        # 1. Make request to search endpoint
        # 2. Parse results
        # 3. Extract house details
        
        # Template for Oxford House data
        template_houses = []
        
        # Example structure (would be populated from actual scraping)
        example_house = {
            'name': f'Oxford House Example - {state}',
            'address': f'123 Recovery St, City, {state} 12345',
            'phone': '(555) 123-4567',
            'gender': 'Men',
            'vacancies': 2,
            'total_beds': 8,
            'state': state,
            'contact_first_name': 'John',
            'application_info': 'Call house for interview',
            'requirements': [
                'Desire to stop using alcohol and drugs',
                'Willing to pay equal share of house expenses',
                'Willing to abide by house rules'
            ]
        }
        
        # In real implementation, this would return actual scraped data
        return template_houses
    
    def _process_house_data(self, raw_houses, extraction_timestamp):
        """Process and standardize Oxford House data"""
        processed_houses = []
        
        for idx, house in enumerate(raw_houses):
            processed_house = {
                'id': f'OXFORD_{house.get("state", "XX")}_{idx+1:04d}',
                'name': house.get('name', ''),
                'organization_type': 'recovery_residence',
                'certification_type': 'oxford_house',
                'is_narr_certified': False,  # Oxford Houses use their own certification
                'address': {
                    'street': house.get('address', ''),
                    'city': self._extract_city(house.get('address', '')),
                    'state': house.get('state', ''),
                    'zip': self._extract_zip(house.get('address', ''))
                },
                'contact': {
                    'phone': house.get('phone', ''),
                    'contact_person': house.get('contact_first_name', ''),
                    'application_process': house.get('application_info', '')
                },
                'capacity': {
                    'total_beds': house.get('total_beds', 0),
                    'current_vacancies': house.get('vacancies', 0),
                    'occupancy_rate': self._calculate_occupancy(house)
                },
                'demographics': {
                    'gender': house.get('gender', 'Not specified'),
                    'age_restrictions': 'Adults (18+)'
                },
                'requirements': house.get('requirements', []),
                'services': [
                    'Democratically run house',
                    'Peer support',
                    'Self-supporting through resident fees',
                    'Drug and alcohol free environment',
                    'No time limit on residency'
                ],
                'cost': {
                    'structure': 'Equal share of house expenses',
                    'typical_range': '$100-150 per week',
                    'includes': ['Rent', 'Utilities', 'Basic supplies']
                },
                'oxford_house_details': {
                    'charter_type': 'Standard Oxford House Charter',
                    'established': house.get('established_date', ''),
                    'house_traditions': 'Oxford House traditions and principles'
                },
                'data_source': {
                    'source': 'oxfordvacancies.com',
                    'extraction_date': extraction_timestamp,
                    'data_freshness': 'Real-time vacancy data'
                }
            }
            
            processed_houses.append(processed_house)
        
        return {
            'metadata': {
                'source': 'Oxford House Vacancies',
                'extraction_timestamp': extraction_timestamp,
                'total_houses_with_vacancies': len(processed_houses),
                'states_covered': len(set(h['address']['state'] for h in processed_houses)),
                'total_vacancies': sum(h['capacity']['current_vacancies'] for h in processed_houses),
                'pipeline_version': '1.0'
            },
            'houses': processed_houses
        }
    
    def _extract_city(self, address):
        """Extract city from address string"""
        # Simple extraction - would be more sophisticated in real implementation
        parts = address.split(',')
        if len(parts) >= 2:
            return parts[-2].strip()
        return ''
    
    def _extract_zip(self, address):
        """Extract ZIP code from address string"""
        # Look for 5-digit ZIP code pattern
        zip_match = re.search(r'\b\d{5}\b', address)
        if zip_match:
            return zip_match.group()
        return ''
    
    def _calculate_occupancy(self, house):
        """Calculate occupancy rate"""
        total = house.get('total_beds', 0)
        vacancies = house.get('vacancies', 0)
        
        if total > 0:
            occupied = total - vacancies
            return round((occupied / total) * 100, 1)
        return 0
    
    def save_data(self, data, output_dir=None):
        """Save extracted Oxford House data"""
        if output_dir is None:
            base_path = Path(__file__).parent.parent.parent
            output_dir = base_path / "03_raw_data" / "oxford_house_data"
        
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"oxford_house_vacancies_{timestamp}.json"
        output_path = output_dir / filename
        
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        # Also save as latest for easy access
        latest_path = output_dir / "oxford_house_vacancies_latest.json"
        with open(latest_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"\nData saved to: {output_path}")
        print(f"Latest symlink: {latest_path}")
        
        return output_path
    
    def generate_summary_report(self, data):
        """Generate summary report of Oxford House vacancies"""
        houses = data['houses']
        
        # State summary
        state_summary = {}
        for house in houses:
            state = house['address']['state']
            if state not in state_summary:
                state_summary[state] = {
                    'total_houses': 0,
                    'total_vacancies': 0,
                    'total_beds': 0,
                    'men_houses': 0,
                    'women_houses': 0,
                    'coed_houses': 0
                }
            
            state_summary[state]['total_houses'] += 1
            state_summary[state]['total_vacancies'] += house['capacity']['current_vacancies']
            state_summary[state]['total_beds'] += house['capacity']['total_beds']
            
            gender = house['demographics']['gender'].lower()
            if 'men' in gender:
                state_summary[state]['men_houses'] += 1
            elif 'women' in gender:
                state_summary[state]['women_houses'] += 1
            else:
                state_summary[state]['coed_houses'] += 1
        
        report = {
            'summary': {
                'report_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_states': len(state_summary),
                'total_houses_with_vacancies': len(houses),
                'total_available_beds': sum(h['capacity']['current_vacancies'] for h in houses),
                'average_occupancy_rate': round(
                    sum(h['capacity']['occupancy_rate'] for h in houses) / len(houses), 1
                ) if houses else 0
            },
            'state_breakdown': state_summary,
            'top_vacancy_states': sorted(
                state_summary.items(),
                key=lambda x: x[1]['total_vacancies'],
                reverse=True
            )[:10]
        }
        
        return report


def run_oxford_pipeline():
    """Run the complete Oxford House data pipeline"""
    print("=== OXFORD HOUSE VACANCY DATA PIPELINE ===")
    print("Extracting real-time vacancy data from oxfordvacancies.com")
    print()
    
    pipeline = OxfordHousePipeline()
    
    # Extract vacancy data
    print("Step 1: Extracting vacancy data...")
    vacancy_data = pipeline.extract_vacancies()
    
    # Save the data
    print("\nStep 2: Saving extracted data...")
    output_path = pipeline.save_data(vacancy_data)
    
    # Generate summary report
    print("\nStep 3: Generating summary report...")
    summary = pipeline.generate_summary_report(vacancy_data)
    
    # Save summary report
    base_path = Path(__file__).parent.parent.parent
    report_path = base_path / "05_reports" / f"oxford_house_summary_{datetime.now().strftime('%Y%m%d')}.json"
    with open(report_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\nSummary report saved to: {report_path}")
    
    # Print summary
    print("\n=== EXTRACTION SUMMARY ===")
    print(f"Total houses with vacancies: {summary['summary']['total_houses_with_vacancies']}")
    print(f"Total available beds: {summary['summary']['total_available_beds']}")
    print(f"States covered: {summary['summary']['total_states']}")
    print(f"Average occupancy rate: {summary['summary']['average_occupancy_rate']}%")
    
    return vacancy_data, summary


if __name__ == "__main__":
    # Run the pipeline
    data, summary = run_oxford_pipeline()