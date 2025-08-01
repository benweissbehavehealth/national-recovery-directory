#!/usr/bin/env python3
"""
Oxford House Complete Data Pipeline - Extracts ALL houses (with and without vacancies)
Captures comprehensive Oxford House network data from oxfordvacancies.com
"""

import json
import requests
from datetime import datetime
from pathlib import Path
import time
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse, parse_qs

class OxfordHouseCompletePipeline:
    """Pipeline to extract ALL Oxford Houses - both with and without vacancies"""
    
    def __init__(self):
        self.base_url = "https://www.oxfordvacancies.com"
        self.search_url = "https://www.oxfordvacancies.com/search"
        self.all_houses_url = "https://www.oxfordvacancies.com/houses"  # URL for all houses
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.states = self._get_us_states()
        
    def _get_us_states(self):
        """Get list of US states with proper names for searching"""
        return {
            'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
            'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
            'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
            'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
            'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
            'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
            'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
            'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
            'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
            'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
            'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
            'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
            'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia'
        }
    
    def extract_complete_oxford_network(self):
        """Extract ALL Oxford Houses - with and without vacancies"""
        all_houses = []
        extraction_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"Starting COMPLETE Oxford House extraction at {extraction_timestamp}")
        print("This will extract ALL houses in the Oxford House network...")
        print()
        
        # Strategy 1: Extract houses WITH vacancies
        print("Phase 1: Extracting houses with current vacancies...")
        houses_with_vacancies = self._extract_houses_with_vacancies()
        print(f"  Found {len(houses_with_vacancies)} houses with vacancies")
        
        # Strategy 2: Extract ALL houses from directory/listings
        print("\nPhase 2: Extracting complete house directory...")
        all_directory_houses = self._extract_all_houses_directory()
        print(f"  Found {len(all_directory_houses)} total houses in directory")
        
        # Strategy 3: State-by-state comprehensive search
        print("\nPhase 3: State-by-state comprehensive search...")
        state_houses = self._extract_by_state_comprehensive()
        print(f"  Found {len(state_houses)} houses through state searches")
        
        # Merge all data sources
        print("\nPhase 4: Merging and deduplicating data...")
        all_houses = self._merge_all_sources(
            houses_with_vacancies, 
            all_directory_houses, 
            state_houses
        )
        
        # Process and standardize the data
        processed_data = self._process_complete_house_data(all_houses, extraction_timestamp)
        
        return processed_data
    
    def _extract_houses_with_vacancies(self):
        """Extract houses that currently have vacancies"""
        houses = []
        page = 1
        
        while True:
            try:
                params = {'vacancies': 'true', 'page': page}
                response = self.session.get(self.search_url, params=params, timeout=30)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    page_houses = self._parse_houses_from_page(soup, has_vacancy=True)
                    
                    if not page_houses:
                        break
                        
                    houses.extend(page_houses)
                    print(f"    Page {page}: {len(page_houses)} houses with vacancies")
                    
                    if not self._has_next_page(soup):
                        break
                        
                    page += 1
                    time.sleep(1)
                else:
                    break
                    
            except Exception as e:
                print(f"    Error extracting vacancies page {page}: {e}")
                break
        
        return houses
    
    def _extract_all_houses_directory(self):
        """Extract all houses from main directory (including those without vacancies)"""
        houses = []
        page = 1
        
        # Try different URLs that might list all houses
        directory_urls = [
            self.base_url + "/houses",
            self.base_url + "/directory",
            self.base_url + "/all-houses",
            self.search_url  # Search with no filters
        ]
        
        for url in directory_urls:
            try:
                while True:
                    params = {'page': page}
                    response = self.session.get(url, params=params, timeout=30)
                    
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        page_houses = self._parse_houses_from_page(soup, has_vacancy=None)
                        
                        if not page_houses:
                            break
                            
                        houses.extend(page_houses)
                        print(f"    Directory page {page}: {len(page_houses)} houses")
                        
                        if not self._has_next_page(soup):
                            break
                            
                        page += 1
                        time.sleep(1)
                    else:
                        break
                        
            except Exception as e:
                print(f"    Error with directory URL {url}: {e}")
                continue
                
            if houses:  # Found data, no need to try other URLs
                break
        
        return houses
    
    def _extract_by_state_comprehensive(self):
        """Extract houses state by state for comprehensive coverage"""
        all_state_houses = []
        
        for state_code, state_name in self.states.items():
            print(f"  Searching {state_name} ({state_code})...")
            
            # Search with multiple parameter combinations
            search_params = [
                {'state': state_code},
                {'state': state_name},
                {'location': state_name},
                {'q': f"Oxford House {state_name}"}
            ]
            
            state_houses = []
            for params in search_params:
                houses = self._extract_with_pagination(params)
                state_houses.extend(houses)
                
                if houses:  # Found results, might not need other search params
                    break
            
            # Deduplicate within state
            unique_state_houses = self._deduplicate_houses(state_houses)
            all_state_houses.extend(unique_state_houses)
            
            if unique_state_houses:
                print(f"    Found {len(unique_state_houses)} houses in {state_name}")
            
            time.sleep(1)  # Rate limiting
        
        return all_state_houses
    
    def _extract_with_pagination(self, params):
        """Extract houses with pagination for given parameters"""
        houses = []
        page = 1
        max_pages = 50  # Safety limit
        
        while page <= max_pages:
            try:
                params['page'] = page
                response = self.session.get(self.search_url, params=params, timeout=30)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    page_houses = self._parse_houses_from_page(soup)
                    
                    if not page_houses:
                        break
                        
                    houses.extend(page_houses)
                    
                    if not self._has_next_page(soup):
                        break
                        
                    page += 1
                    time.sleep(0.5)
                else:
                    break
                    
            except Exception as e:
                break
        
        return houses
    
    def _parse_houses_from_page(self, soup, has_vacancy=None):
        """Parse Oxford House data from a page"""
        houses = []
        
        # Multiple selectors to try
        selectors = [
            'div.house-listing',
            'div.property-item',
            'article.house',
            'div.oxford-house',
            'div.listing-item'
        ]
        
        for selector in selectors:
            listings = soup.select(selector)
            if listings:
                for listing in listings:
                    house_data = self._extract_house_info(listing)
                    if house_data:
                        if has_vacancy is not None:
                            house_data['has_vacancy'] = has_vacancy
                        houses.append(house_data)
                break
        
        return houses
    
    def _extract_house_info(self, element):
        """Extract house information from HTML element"""
        house = {
            'name': '',
            'address': '',
            'city': '',
            'state': '',
            'zip': '',
            'phone': '',
            'gender': '',
            'total_beds': 0,
            'current_vacancies': 0,
            'has_vacancy': False,
            'established_date': '',
            'contact_info': ''
        }
        
        # Extract data (implementation depends on actual HTML structure)
        # This is a template showing what we'd extract
        
        name = self._safe_extract_text(element, '.house-name, h3, .title')
        if name:
            house['name'] = name
            
        address = self._safe_extract_text(element, '.address, .location')
        if address:
            house['address'] = address
            # Parse city, state, zip from address
            parts = self._parse_address(address)
            house.update(parts)
        
        house['phone'] = self._safe_extract_text(element, '.phone, .contact')
        house['gender'] = self._safe_extract_text(element, '.gender, .type')
        
        # Extract capacity info
        beds_text = self._safe_extract_text(element, '.beds, .capacity')
        if beds_text:
            numbers = re.findall(r'\d+', beds_text)
            if numbers:
                house['total_beds'] = int(numbers[0])
        
        # Extract vacancy info
        vacancy_text = self._safe_extract_text(element, '.vacancies, .available')
        if vacancy_text:
            numbers = re.findall(r'\d+', vacancy_text)
            if numbers:
                house['current_vacancies'] = int(numbers[0])
                house['has_vacancy'] = True
        
        return house if house['name'] else None
    
    def _merge_all_sources(self, vacancies_list, directory_list, state_list):
        """Merge all data sources and deduplicate"""
        all_houses = []
        seen_houses = {}
        
        # Process all lists
        all_sources = vacancies_list + directory_list + state_list
        
        for house in all_sources:
            # Create unique key
            key = self._create_house_key(house)
            
            if key not in seen_houses:
                seen_houses[key] = house
            else:
                # Merge data - update with any new information
                existing = seen_houses[key]
                
                # Update vacancy information if available
                if house.get('has_vacancy') and house.get('current_vacancies', 0) > 0:
                    existing['has_vacancy'] = True
                    existing['current_vacancies'] = house['current_vacancies']
                
                # Fill in any missing data
                for field in ['phone', 'gender', 'total_beds', 'established_date']:
                    if not existing.get(field) and house.get(field):
                        existing[field] = house[field]
        
        return list(seen_houses.values())
    
    def _create_house_key(self, house):
        """Create unique identifier for deduplication"""
        name = house.get('name', '').lower().strip()
        city = house.get('city', '').lower().strip()
        state = house.get('state', '').upper()
        return f"{name}|{city}|{state}"
    
    def _process_complete_house_data(self, houses, extraction_timestamp):
        """Process and standardize complete Oxford House data"""
        processed_houses = []
        
        # Statistics
        stats = {
            'total_houses': len(houses),
            'houses_with_vacancies': 0,
            'houses_full': 0,
            'total_beds': 0,
            'total_vacancies': 0,
            'by_state': {},
            'by_gender': {'Men': 0, 'Women': 0, 'Co-ed': 0, 'Unknown': 0}
        }
        
        for idx, house in enumerate(houses):
            # Generate ID
            state = house.get('state', 'XX')
            house_id = f"OXFORD_{state}_{idx+1:04d}"
            
            # Calculate occupancy
            total_beds = house.get('total_beds', 0)
            vacancies = house.get('current_vacancies', 0)
            has_vacancy = house.get('has_vacancy', False) or vacancies > 0
            
            if has_vacancy:
                stats['houses_with_vacancies'] += 1
            else:
                stats['houses_full'] += 1
            
            stats['total_beds'] += total_beds
            stats['total_vacancies'] += vacancies
            
            # Track by state
            if state not in stats['by_state']:
                stats['by_state'][state] = {
                    'total_houses': 0,
                    'with_vacancies': 0,
                    'total_beds': 0,
                    'total_vacancies': 0
                }
            
            stats['by_state'][state]['total_houses'] += 1
            if has_vacancy:
                stats['by_state'][state]['with_vacancies'] += 1
            stats['by_state'][state]['total_beds'] += total_beds
            stats['by_state'][state]['total_vacancies'] += vacancies
            
            # Track by gender
            gender = house.get('gender', 'Unknown')
            if 'men' in gender.lower() and 'women' not in gender.lower():
                stats['by_gender']['Men'] += 1
            elif 'women' in gender.lower():
                stats['by_gender']['Women'] += 1
            elif 'co' in gender.lower():
                stats['by_gender']['Co-ed'] += 1
            else:
                stats['by_gender']['Unknown'] += 1
            
            # Create processed record
            processed_house = {
                'id': house_id,
                'name': f"Oxford House {house.get('name', '')}",
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
                    'street': house.get('address', ''),
                    'city': house.get('city', ''),
                    'state': house.get('state', ''),
                    'zip': house.get('zip', '')
                },
                'contact': {
                    'phone': house.get('phone', ''),
                    'application_process': house.get('contact_info', 'Contact house for interview')
                },
                'capacity': {
                    'total_beds': total_beds,
                    'current_vacancies': vacancies,
                    'has_vacancy': has_vacancy,
                    'occupancy_rate': self._calculate_occupancy(total_beds, vacancies)
                },
                'demographics': {
                    'gender': house.get('gender', 'Not specified'),
                    'age_restrictions': 'Adults (18+)'
                },
                'operational_details': {
                    'established_date': house.get('established_date', ''),
                    'charter_status': 'Active Oxford House Charter',
                    'governance': 'Democratic self-governance'
                },
                'services': [
                    'Democratically run house',
                    'Peer support environment',
                    'Self-supporting through resident fees',
                    'Drug and alcohol free environment',
                    'No time limit on residency',
                    'Weekly house meetings',
                    'Officer elections every 6 months'
                ],
                'data_source': {
                    'source': 'oxfordvacancies.com - Complete Extract',
                    'extraction_date': extraction_timestamp,
                    'data_type': 'Complete network data (all houses)'
                }
            }
            
            processed_houses.append(processed_house)
        
        return {
            'metadata': {
                'source': 'Oxford House Complete Network Extract',
                'extraction_timestamp': extraction_timestamp,
                'extraction_type': 'COMPLETE - All houses with and without vacancies',
                'statistics': stats,
                'pipeline_version': '3.0'
            },
            'houses': processed_houses
        }
    
    def _parse_address(self, address_string):
        """Parse address into components"""
        parts = {'city': '', 'state': '', 'zip': ''}
        
        if not address_string:
            return parts
        
        # Extract ZIP
        zip_match = re.search(r'\b(\d{5})(?:-\d{4})?\b', address_string)
        if zip_match:
            parts['zip'] = zip_match.group(1)
        
        # Extract state
        state_match = re.search(r'\b([A-Z]{2})\b', address_string)
        if state_match:
            parts['state'] = state_match.group(1)
        
        # Extract city (simplified)
        address_parts = address_string.split(',')
        if len(address_parts) >= 2:
            parts['city'] = address_parts[-2].strip()
        
        return parts
    
    def _safe_extract_text(self, element, selector):
        """Safely extract text from element"""
        try:
            found = element.select_one(selector)
            if found:
                return found.get_text(strip=True)
        except:
            pass
        return ''
    
    def _has_next_page(self, soup):
        """Check if there's a next page"""
        next_indicators = [
            'a.next:not(.disabled)',
            'a[rel="next"]',
            '.pagination .next',
            'button.next-page:not(.disabled)'
        ]
        
        for indicator in next_indicators:
            if soup.select_one(indicator):
                return True
        return False
    
    def _calculate_occupancy(self, total_beds, vacancies):
        """Calculate occupancy rate"""
        if total_beds > 0:
            occupied = total_beds - vacancies
            return round((occupied / total_beds) * 100, 1)
        return 0
    
    def _deduplicate_houses(self, houses):
        """Remove duplicate houses"""
        seen = set()
        unique = []
        
        for house in houses:
            key = self._create_house_key(house)
            if key not in seen:
                seen.add(key)
                unique.append(house)
        
        return unique
    
    def save_complete_data(self, data):
        """Save complete Oxford House network data"""
        base_path = Path(__file__).parent.parent.parent
        output_dir = base_path / "03_raw_data" / "oxford_house_data"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"oxford_house_complete_network_{timestamp}.json"
        output_path = output_dir / filename
        
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        # Save as latest
        latest_path = output_dir / "oxford_house_complete_network_latest.json"
        with open(latest_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        # Generate summary report
        stats = data['metadata']['statistics']
        
        print(f"\n{'='*60}")
        print(f"OXFORD HOUSE COMPLETE NETWORK EXTRACTION SUMMARY")
        print(f"{'='*60}")
        print(f"Total Oxford Houses: {stats['total_houses']}")
        print(f"  - With Vacancies: {stats['houses_with_vacancies']}")
        print(f"  - Currently Full: {stats['houses_full']}")
        print(f"Total Bed Capacity: {stats['total_beds']}")
        print(f"Current Vacancies: {stats['total_vacancies']}")
        print(f"Network Occupancy: {round((stats['total_beds'] - stats['total_vacancies']) / stats['total_beds'] * 100, 1)}%")
        print(f"\nGender Distribution:")
        for gender, count in stats['by_gender'].items():
            if count > 0:
                print(f"  - {gender}: {count}")
        print(f"\nGeographic Coverage: {len(stats['by_state'])} states")
        print(f"\nData saved to: {output_path}")
        
        return output_path


def run_complete_oxford_extraction():
    """Run the complete Oxford House network extraction"""
    print("=== OXFORD HOUSE COMPLETE NETWORK EXTRACTION ===")
    print("Extracting ALL Oxford Houses - with and without vacancies")
    print("This captures the entire Oxford House network")
    print()
    
    pipeline = OxfordHouseCompletePipeline()
    
    # Extract complete network data
    complete_data = pipeline.extract_complete_oxford_network()
    
    # Save the data
    output_path = pipeline.save_complete_data(complete_data)
    
    return complete_data, output_path


if __name__ == "__main__":
    data, path = run_complete_oxford_extraction()