#!/usr/bin/env python3
"""
Oxford House Vacancy Data Pipeline v2 - With Pagination Support
Extracts ALL vacancy data from oxfordvacancies.com across all pages
"""

import json
import requests
from datetime import datetime
from pathlib import Path
import time
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse, parse_qs

class OxfordHousePipelineV2:
    """Enhanced pipeline with pagination support for complete data extraction"""
    
    def __init__(self):
        self.base_url = "https://www.oxfordvacancies.com"
        self.search_url = "https://www.oxfordvacancies.com/search"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
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
    
    def extract_all_vacancies(self):
        """Extract ALL Oxford House vacancies handling pagination"""
        all_houses = []
        extraction_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"Starting Oxford House vacancy extraction at {extraction_timestamp}")
        print("This will extract ALL houses across ALL pages...")
        
        # First, try to get all vacancies without state filter to capture everything
        print("\nStep 1: Extracting all vacancies (no state filter)...")
        all_vacancies = self._extract_with_pagination()
        all_houses.extend(all_vacancies)
        
        # Then search by state to ensure we don't miss any
        print(f"\nStep 2: Searching by state to ensure complete coverage...")
        for state_code, state_name in self.states.items():
            print(f"\nSearching {state_name} ({state_code})...")
            state_houses = self._search_state_with_pagination(state_code, state_name)
            
            # Add only new houses not already found
            existing_ids = {self._create_house_id(h) for h in all_houses}
            new_houses = [h for h in state_houses if self._create_house_id(h) not in existing_ids]
            
            if new_houses:
                print(f"  Found {len(new_houses)} additional houses in {state_name}")
                all_houses.extend(new_houses)
            else:
                print(f"  No additional houses found in {state_name}")
            
            # Rate limiting
            time.sleep(1)
        
        # Remove duplicates
        unique_houses = self._deduplicate_houses(all_houses)
        
        # Process and standardize the data
        processed_data = self._process_house_data(unique_houses, extraction_timestamp)
        
        return processed_data
    
    def _extract_with_pagination(self, params=None):
        """Extract houses handling pagination for a given search"""
        all_houses = []
        page = 1
        total_pages = None
        
        while True:
            try:
                # Add page parameter
                search_params = params or {}
                search_params['page'] = page
                
                print(f"  Fetching page {page}...")
                response = self.session.get(self.search_url, params=search_params, timeout=30)
                response.raise_for_status()
                
                # Parse the page
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract houses from this page
                page_houses = self._parse_houses_from_page(soup)
                if page_houses:
                    all_houses.extend(page_houses)
                    print(f"    Found {len(page_houses)} houses on page {page}")
                
                # Check for pagination
                if total_pages is None:
                    total_pages = self._get_total_pages(soup)
                    if total_pages:
                        print(f"    Total pages to process: {total_pages}")
                
                # Check if there's a next page
                if not self._has_next_page(soup, page, total_pages):
                    break
                
                page += 1
                time.sleep(0.5)  # Rate limiting between pages
                
            except Exception as e:
                print(f"    Error on page {page}: {e}")
                break
        
        return all_houses
    
    def _search_state_with_pagination(self, state_code, state_name):
        """Search for Oxford Houses in a specific state with pagination"""
        # Try different search parameters that might work
        search_variations = [
            {'state': state_code},
            {'state': state_name},
            {'location': state_name},
            {'q': state_name}
        ]
        
        all_state_houses = []
        
        for params in search_variations:
            houses = self._extract_with_pagination(params)
            if houses:
                # Filter to ensure they're actually in this state
                state_houses = [h for h in houses if self._is_house_in_state(h, state_code, state_name)]
                all_state_houses.extend(state_houses)
                break  # Found results, no need to try other variations
        
        return all_state_houses
    
    def _parse_houses_from_page(self, soup):
        """Parse Oxford House data from a search results page"""
        houses = []
        
        # Common patterns for house listings
        # This would need to be adapted based on actual HTML structure
        house_selectors = [
            'div.house-listing',
            'div.vacancy-item',
            'div.property-card',
            'article.house',
            'div.result-item'
        ]
        
        for selector in house_selectors:
            listings = soup.select(selector)
            if listings:
                for listing in listings:
                    house_data = self._extract_house_info(listing)
                    if house_data:
                        houses.append(house_data)
                break
        
        return houses
    
    def _extract_house_info(self, listing_element):
        """Extract house information from a listing element"""
        # This is a template - actual implementation depends on HTML structure
        try:
            house = {
                'name': self._safe_extract_text(listing_element, '.house-name, h3, h2'),
                'address': self._safe_extract_text(listing_element, '.address, .location'),
                'phone': self._safe_extract_text(listing_element, '.phone, .contact-phone'),
                'vacancies': self._extract_number(listing_element, '.vacancies, .available-beds'),
                'total_beds': self._extract_number(listing_element, '.total-beds, .capacity'),
                'gender': self._safe_extract_text(listing_element, '.gender, .house-type'),
                'contact_info': self._safe_extract_text(listing_element, '.contact-info, .apply-info'),
                'state': self._extract_state(listing_element)
            }
            
            # Only return if we have minimum required info
            if house['name'] and house['address']:
                return house
                
        except Exception as e:
            print(f"      Error extracting house info: {e}")
        
        return None
    
    def _safe_extract_text(self, element, selector):
        """Safely extract text from element"""
        try:
            found = element.select_one(selector)
            if found:
                return found.get_text(strip=True)
        except:
            pass
        return ''
    
    def _extract_number(self, element, selector):
        """Extract number from element text"""
        text = self._safe_extract_text(element, selector)
        if text:
            numbers = re.findall(r'\d+', text)
            if numbers:
                return int(numbers[0])
        return 0
    
    def _extract_state(self, element):
        """Extract state from address or other fields"""
        address = self._safe_extract_text(element, '.address, .location')
        if address:
            # Look for state abbreviations
            state_match = re.search(r'\b([A-Z]{2})\b', address)
            if state_match:
                return state_match.group(1)
        return ''
    
    def _get_total_pages(self, soup):
        """Extract total number of pages from pagination"""
        try:
            # Common pagination patterns
            pagination_selectors = [
                '.pagination .last-page',
                '.page-numbers li:last-child',
                'a[aria-label*="Last"]',
                '.pagination-info'
            ]
            
            for selector in pagination_selectors:
                element = soup.select_one(selector)
                if element:
                    text = element.get_text()
                    numbers = re.findall(r'\d+', text)
                    if numbers:
                        return int(numbers[-1])
        except:
            pass
        
        return None
    
    def _has_next_page(self, soup, current_page, total_pages):
        """Check if there's a next page"""
        # If we know total pages
        if total_pages and current_page >= total_pages:
            return False
        
        # Check for next button
        next_selectors = [
            'a.next-page:not(.disabled)',
            'a[rel="next"]',
            '.pagination .next:not(.disabled)',
            'a[aria-label="Next"]'
        ]
        
        for selector in next_selectors:
            if soup.select_one(selector):
                return True
        
        # Check if current page has results
        # If no results, we've reached the end
        if not self._parse_houses_from_page(soup):
            return False
        
        return True
    
    def _create_house_id(self, house):
        """Create unique identifier for deduplication"""
        name = house.get('name', '').lower().strip()
        address = house.get('address', '').lower().strip()
        return f"{name}|{address}"
    
    def _deduplicate_houses(self, houses):
        """Remove duplicate houses"""
        seen = set()
        unique = []
        
        for house in houses:
            house_id = self._create_house_id(house)
            if house_id not in seen:
                seen.add(house_id)
                unique.append(house)
        
        return unique
    
    def _is_house_in_state(self, house, state_code, state_name):
        """Check if house is in the specified state"""
        house_state = house.get('state', '')
        address = house.get('address', '').lower()
        
        return (house_state == state_code or 
                state_code.lower() in address or 
                state_name.lower() in address)
    
    def _process_house_data(self, raw_houses, extraction_timestamp):
        """Process and standardize Oxford House data"""
        processed_houses = []
        
        for idx, house in enumerate(raw_houses):
            # Parse address components
            address_parts = self._parse_address(house.get('address', ''))
            
            processed_house = {
                'id': f"OXFORD_{house.get('state', 'XX')}_{idx+1:04d}",
                'name': f"Oxford House {house.get('name', '')}",
                'organization_type': 'recovery_residence',
                'certification_type': 'oxford_house',
                'is_narr_certified': False,
                'address': {
                    'street': address_parts.get('street', house.get('address', '')),
                    'city': address_parts.get('city', ''),
                    'state': house.get('state', address_parts.get('state', '')),
                    'zip': address_parts.get('zip', '')
                },
                'contact': {
                    'phone': house.get('phone', ''),
                    'application_process': house.get('contact_info', '')
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
                'services': [
                    'Democratically run house',
                    'Peer support',
                    'Self-supporting through resident fees',
                    'Drug and alcohol free environment',
                    'No time limit on residency'
                ],
                'data_source': {
                    'source': 'oxfordvacancies.com',
                    'extraction_date': extraction_timestamp,
                    'data_freshness': 'Real-time vacancy data'
                }
            }
            
            processed_houses.append(processed_house)
        
        return {
            'metadata': {
                'source': 'Oxford House Vacancies - Complete Extract',
                'extraction_timestamp': extraction_timestamp,
                'total_houses_with_vacancies': len(processed_houses),
                'states_covered': len(set(h['address']['state'] for h in processed_houses if h['address']['state'])),
                'total_vacancies': sum(h['capacity']['current_vacancies'] for h in processed_houses),
                'pipeline_version': '2.0',
                'extraction_method': 'Full pagination support'
            },
            'houses': processed_houses
        }
    
    def _parse_address(self, address_string):
        """Parse address string into components"""
        parts = {'street': '', 'city': '', 'state': '', 'zip': ''}
        
        if not address_string:
            return parts
        
        # Look for ZIP code
        zip_match = re.search(r'\b(\d{5})(?:-\d{4})?\b', address_string)
        if zip_match:
            parts['zip'] = zip_match.group(1)
        
        # Look for state abbreviation
        state_match = re.search(r'\b([A-Z]{2})\b', address_string)
        if state_match:
            parts['state'] = state_match.group(1)
        
        # Try to parse city and street
        # This is simplified - real implementation would be more sophisticated
        address_parts = address_string.split(',')
        if len(address_parts) >= 2:
            parts['street'] = address_parts[0].strip()
            parts['city'] = address_parts[1].strip()
        elif address_parts:
            parts['street'] = address_parts[0].strip()
        
        return parts
    
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
        filename = f"oxford_house_vacancies_complete_{timestamp}.json"
        output_path = output_dir / filename
        
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        # Also save as latest
        latest_path = output_dir / "oxford_house_vacancies_latest_complete.json"
        with open(latest_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"\nData saved to: {output_path}")
        print(f"Latest symlink: {latest_path}")
        
        return output_path
    
    def generate_summary_report(self, data):
        """Generate comprehensive summary report"""
        houses = data['houses']
        
        # State summary with detailed statistics
        state_summary = {}
        for house in houses:
            state = house['address']['state']
            if not state:
                state = 'Unknown'
                
            if state not in state_summary:
                state_summary[state] = {
                    'total_houses': 0,
                    'total_vacancies': 0,
                    'total_beds': 0,
                    'men_houses': 0,
                    'women_houses': 0,
                    'coed_houses': 0,
                    'unknown_gender': 0,
                    'average_occupancy': 0,
                    'cities': set()
                }
            
            state_summary[state]['total_houses'] += 1
            state_summary[state]['total_vacancies'] += house['capacity']['current_vacancies']
            state_summary[state]['total_beds'] += house['capacity']['total_beds']
            
            # Track cities
            city = house['address'].get('city', '')
            if city:
                state_summary[state]['cities'].add(city)
            
            # Gender breakdown
            gender = house['demographics']['gender'].lower()
            if 'men' in gender and 'women' not in gender:
                state_summary[state]['men_houses'] += 1
            elif 'women' in gender:
                state_summary[state]['women_houses'] += 1
            elif 'co-ed' in gender or 'coed' in gender:
                state_summary[state]['coed_houses'] += 1
            else:
                state_summary[state]['unknown_gender'] += 1
        
        # Calculate average occupancy per state
        for state, stats in state_summary.items():
            if stats['total_beds'] > 0:
                occupied = stats['total_beds'] - stats['total_vacancies']
                stats['average_occupancy'] = round((occupied / stats['total_beds']) * 100, 1)
            
            # Convert cities set to list for JSON serialization
            stats['cities'] = sorted(list(stats['cities']))
            stats['city_count'] = len(stats['cities'])
        
        # National statistics
        total_vacancies = sum(h['capacity']['current_vacancies'] for h in houses)
        total_beds = sum(h['capacity']['total_beds'] for h in houses)
        
        report = {
            'summary': {
                'report_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'extraction_method': 'Complete pagination scan',
                'total_states': len([s for s in state_summary if s != 'Unknown']),
                'total_houses_with_vacancies': len(houses),
                'total_available_beds': total_vacancies,
                'total_bed_capacity': total_beds,
                'national_occupancy_rate': round(((total_beds - total_vacancies) / total_beds * 100), 1) if total_beds > 0 else 0,
                'average_house_size': round(total_beds / len(houses), 1) if houses else 0
            },
            'state_breakdown': state_summary,
            'top_vacancy_states': sorted(
                [(state, stats) for state, stats in state_summary.items() if state != 'Unknown'],
                key=lambda x: x[1]['total_vacancies'],
                reverse=True
            )[:10],
            'largest_house_networks': sorted(
                [(state, stats) for state, stats in state_summary.items() if state != 'Unknown'],
                key=lambda x: x[1]['total_houses'],
                reverse=True
            )[:10]
        }
        
        return report


def run_complete_oxford_pipeline():
    """Run the complete Oxford House data pipeline with pagination support"""
    print("=== OXFORD HOUSE COMPLETE VACANCY DATA PIPELINE V2 ===")
    print("Extracting ALL vacancy data with full pagination support")
    print("This ensures we capture every available house across all pages")
    print()
    
    pipeline = OxfordHousePipelineV2()
    
    # Extract all vacancy data
    print("Step 1: Extracting complete vacancy data...")
    vacancy_data = pipeline.extract_all_vacancies()
    
    # Save the data
    print("\nStep 2: Saving extracted data...")
    output_path = pipeline.save_data(vacancy_data)
    
    # Generate summary report
    print("\nStep 3: Generating comprehensive summary report...")
    summary = pipeline.generate_summary_report(vacancy_data)
    
    # Save summary report
    base_path = Path(__file__).parent.parent.parent
    report_path = base_path / "05_reports" / f"oxford_house_complete_summary_{datetime.now().strftime('%Y%m%d')}.json"
    with open(report_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\nSummary report saved to: {report_path}")
    
    # Print detailed summary
    print("\n=== EXTRACTION SUMMARY ===")
    print(f"Total houses with vacancies: {summary['summary']['total_houses_with_vacancies']}")
    print(f"Total available beds: {summary['summary']['total_available_beds']}")
    print(f"Total bed capacity: {summary['summary']['total_bed_capacity']}")
    print(f"National occupancy rate: {summary['summary']['national_occupancy_rate']}%")
    print(f"Average house size: {summary['summary']['average_house_size']} beds")
    print(f"States covered: {summary['summary']['total_states']}")
    
    if summary['top_vacancy_states']:
        print("\nTop 5 states by vacancy count:")
        for state, stats in summary['top_vacancy_states'][:5]:
            print(f"  {state}: {stats['total_vacancies']} vacancies in {stats['total_houses']} houses ({stats['city_count']} cities)")
    
    return vacancy_data, summary


if __name__ == "__main__":
    # Run the complete pipeline
    data, summary = run_complete_oxford_pipeline()