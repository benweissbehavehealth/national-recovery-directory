#!/usr/bin/env python3
"""
SAMHSA Web Scraper for Outpatient Treatment Centers

Alternative approach using web scraping techniques to extract outpatient
treatment center data from findtreatment.gov when API access is limited.

This script uses Selenium and BeautifulSoup to systematically scrape
treatment facility information from the SAMHSA Treatment Locator.

Author: Claude Code
Date: 2025-07-31
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import os
from datetime import datetime
import random
from urllib.parse import urlencode, urljoin
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('samhsa_web_scraping.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SAMHSAWebScraper:
    """Web scraper for SAMHSA Treatment Locator."""
    
    def __init__(self):
        """Initialize the web scraper."""
        self.base_url = "https://findtreatment.gov"
        self.session = requests.Session()
        
        # Rotate user agents to avoid detection
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        ]
        
        self.session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        self.facilities = []
        self.extracted_count = 0
        
        # State codes and major cities for systematic scraping
        self.state_info = {
            'AL': ['Birmingham', 'Montgomery', 'Mobile', 'Huntsville'],
            'AK': ['Anchorage', 'Fairbanks', 'Juneau'],
            'AZ': ['Phoenix', 'Tucson', 'Mesa', 'Chandler'],
            'AR': ['Little Rock', 'Fort Smith', 'Fayetteville'],
            'CA': ['Los Angeles', 'San Francisco', 'San Diego', 'Sacramento', 'San Jose'],
            'CO': ['Denver', 'Colorado Springs', 'Aurora', 'Fort Collins'],
            'CT': ['Hartford', 'New Haven', 'Stamford', 'Waterbury'],
            'DE': ['Wilmington', 'Dover', 'Newark'],
            'FL': ['Miami', 'Tampa', 'Orlando', 'Jacksonville', 'Fort Lauderdale'],
            'GA': ['Atlanta', 'Augusta', 'Columbus', 'Savannah'],
            'HI': ['Honolulu', 'Hilo', 'Kailua-Kona'],
            'ID': ['Boise', 'Nampa', 'Meridian'],
            'IL': ['Chicago', 'Aurora', 'Springfield', 'Peoria'],
            'IN': ['Indianapolis', 'Fort Wayne', 'Evansville'],
            'IA': ['Des Moines', 'Cedar Rapids', 'Davenport'],
            'KS': ['Wichita', 'Overland Park', 'Kansas City'],
            'KY': ['Louisville', 'Lexington', 'Bowling Green'],
            'LA': ['New Orleans', 'Baton Rouge', 'Shreveport'],
            'ME': ['Portland', 'Lewiston', 'Bangor'],
            'MD': ['Baltimore', 'Frederick', 'Rockville'],
            'MA': ['Boston', 'Worcester', 'Springfield'],
            'MI': ['Detroit', 'Grand Rapids', 'Warren'],
            'MN': ['Minneapolis', 'Saint Paul', 'Rochester'],
            'MS': ['Jackson', 'Gulfport', 'Southaven'],
            'MO': ['Kansas City', 'Saint Louis', 'Springfield'],
            'MT': ['Billings', 'Missoula', 'Great Falls'],
            'NE': ['Omaha', 'Lincoln', 'Bellevue'],
            'NV': ['Las Vegas', 'Henderson', 'Reno'],
            'NH': ['Manchester', 'Nashua', 'Concord'],
            'NJ': ['Newark', 'Jersey City', 'Paterson'],
            'NM': ['Albuquerque', 'Las Cruces', 'Rio Rancho'],
            'NY': ['New York City', 'Buffalo', 'Rochester', 'Syracuse'],
            'NC': ['Charlotte', 'Raleigh', 'Greensboro'],
            'ND': ['Fargo', 'Bismarck', 'Grand Forks'],
            'OH': ['Columbus', 'Cleveland', 'Cincinnati'],
            'OK': ['Oklahoma City', 'Tulsa', 'Norman'],
            'OR': ['Portland', 'Salem', 'Eugene'],
            'PA': ['Philadelphia', 'Pittsburgh', 'Allentown'],
            'RI': ['Providence', 'Warwick', 'Cranston'],
            'SC': ['Columbia', 'Charleston', 'North Charleston'],
            'SD': ['Sioux Falls', 'Rapid City', 'Aberdeen'],
            'TN': ['Nashville', 'Memphis', 'Knoxville'],
            'TX': ['Houston', 'San Antonio', 'Dallas', 'Austin', 'Fort Worth'],
            'UT': ['Salt Lake City', 'West Valley City', 'Provo'],
            'VT': ['Burlington', 'South Burlington', 'Rutland'],
            'VA': ['Virginia Beach', 'Norfolk', 'Chesapeake'],
            'WA': ['Seattle', 'Spokane', 'Tacoma'],
            'WV': ['Charleston', 'Huntington', 'Morgantown'],
            'WI': ['Milwaukee', 'Madison', 'Green Bay'],
            'WY': ['Cheyenne', 'Casper', 'Laramie'],
            'DC': ['Washington']
        }
        
        # Outpatient service identifiers
        self.outpatient_keywords = [
            'outpatient', 'intensive outpatient', 'iop', 'partial hospitalization',
            'php', 'day treatment', 'medication assisted', 'mat', 'opioid treatment',
            'otp', 'methadone', 'suboxone', 'buprenorphine', 'office based',
            'dui', 'dwi', 'counseling', 'therapy', 'group therapy'
        ]
    
    def get_random_user_agent(self) -> str:
        """Get a random user agent string."""
        return random.choice(self.user_agents)
    
    def make_request(self, url: str, params: dict = None, retries: int = 3) -> Optional[requests.Response]:
        """
        Make HTTP request with error handling and retries.
        
        Args:
            url: URL to request
            params: Query parameters
            retries: Number of retry attempts
            
        Returns:
            Response object or None if failed
        """
        for attempt in range(retries):
            try:
                # Rotate user agent
                self.session.headers['User-Agent'] = self.get_random_user_agent()
                
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:
                    # Rate limited - wait longer
                    wait_time = (2 ** attempt) * 5
                    logger.warning(f"Rate limited. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.warning(f"HTTP {response.status_code} for {url}")
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(random.uniform(1, 3))
        
        return None
    
    def search_facilities_by_location(self, state: str, city: str = None) -> List[Dict]:
        """
        Search for treatment facilities by location using web scraping.
        
        Args:
            state: State abbreviation
            city: City name (optional)
            
        Returns:
            List of facility data dictionaries
        """
        facilities = []
        
        try:
            # Build search URL
            search_params = {
                'state': state,
                'distance': '50',  # 50 mile radius
                'service_type': 'outpatient'
            }
            
            if city:
                search_params['city'] = city
            
            # Try different search endpoints
            search_endpoints = [
                f"{self.base_url}/locator",
                f"{self.base_url}/search",
                f"{self.base_url}/treatment-locator"
            ]
            
            for endpoint in search_endpoints:
                search_url = f"{endpoint}?{urlencode(search_params)}"
                
                response = self.make_request(search_url)
                if not response:
                    continue
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Look for facility listings with various selectors
                facility_selectors = [
                    '.facility-card',
                    '.treatment-facility',
                    '.facility-listing',
                    '.facility-item',
                    '[data-facility-id]',
                    '.result-item'
                ]
                
                facility_elements = []
                for selector in facility_selectors:
                    elements = soup.select(selector)
                    if elements:
                        facility_elements = elements
                        break
                
                if facility_elements:
                    logger.info(f"Found {len(facility_elements)} facilities with endpoint {endpoint}")
                    
                    for element in facility_elements:
                        facility_data = self.parse_facility_element(element, state)
                        if facility_data and self.is_outpatient_facility(facility_data):
                            facilities.append(facility_data)
                    
                    break  # Success with this endpoint
                    
                # Rate limiting between endpoint attempts
                time.sleep(random.uniform(1, 3))
            
            # If no structured elements found, try parsing JSON from page
            if not facilities:
                facilities = self.extract_json_from_page(response.content) if response else []
                
        except Exception as e:
            logger.error(f"Error searching facilities in {city}, {state}: {e}")
        
        return facilities
    
    def parse_facility_element(self, element, state: str) -> Optional[Dict]:
        """
        Parse facility information from HTML element.
        
        Args:
            element: BeautifulSoup element containing facility data
            state: State abbreviation
            
        Returns:
            Facility data dictionary or None
        """
        try:
            facility_data = {
                'name': '',
                'address': {},
                'contact': {},
                'services': [],
                'location': {},
                'extraction_date': datetime.now().isoformat(),
                'data_source': 'SAMHSA Web Scraping'
            }
            
            # Extract facility name
            name_selectors = ['.facility-name', '.name', 'h2', 'h3', '.title']
            for selector in name_selectors:
                name_elem = element.select_one(selector)
                if name_elem:
                    facility_data['name'] = name_elem.get_text(strip=True)
                    break
            
            # Extract address
            address_selectors = ['.address', '.facility-address', '.location']
            for selector in address_selectors:
                addr_elem = element.select_one(selector)
                if addr_elem:
                    address_text = addr_elem.get_text(strip=True)
                    facility_data['address'] = self.parse_address_text(address_text, state)
                    break
            
            # Extract phone number
            phone_selectors = ['.phone', '.telephone', '.contact-phone']
            for selector in phone_selectors:
                phone_elem = element.select_one(selector)
                if phone_elem:
                    facility_data['contact']['phone'] = phone_elem.get_text(strip=True)
                    break
            
            # Extract services
            service_selectors = ['.services', '.treatment-services', '.service-list']
            for selector in service_selectors:
                service_elem = element.select_one(selector)
                if service_elem:
                    services_text = service_elem.get_text(strip=True)
                    facility_data['services'] = [s.strip() for s in services_text.split(',')]
                    break
            
            # Extract facility ID if available
            facility_id = element.get('data-facility-id') or element.get('id', '')
            if facility_id:
                facility_data['id'] = facility_id
            
            # Extract additional details from links
            detail_link = element.select_one('a[href*="facility"]')
            if detail_link:
                detail_url = urljoin(self.base_url, detail_link.get('href'))
                detailed_data = self.scrape_facility_details(detail_url)
                if detailed_data:
                    facility_data.update(detailed_data)
            
            return facility_data if facility_data['name'] else None
            
        except Exception as e:
            logger.error(f"Error parsing facility element: {e}")
            return None
    
    def parse_address_text(self, address_text: str, state: str) -> Dict:
        """
        Parse address text into structured components.
        
        Args:
            address_text: Raw address text
            state: State abbreviation
            
        Returns:
            Address dictionary
        """
        address = {
            'street1': '',
            'street2': '',
            'city': '',
            'state': state,
            'zip': '',
            'county': ''
        }
        
        try:
            # Basic address parsing
            lines = [line.strip() for line in address_text.split('\n') if line.strip()]
            
            if len(lines) >= 2:
                address['street1'] = lines[0]
                
                # Last line usually contains city, state, zip
                last_line = lines[-1]
                
                # Extract ZIP code
                zip_match = re.search(r'\b\d{5}(-\d{4})?\b', last_line)
                if zip_match:
                    address['zip'] = zip_match.group()
                    last_line = last_line.replace(zip_match.group(), '').strip()
                
                # Extract state (should be provided state)
                state_pattern = rf'\b{state}\b'
                last_line = re.sub(state_pattern, '', last_line).strip()
                
                # Remaining text is likely the city
                city_text = last_line.rstrip(',').strip()
                if city_text:
                    address['city'] = city_text
                
                # Second line might be street2
                if len(lines) >= 3:
                    address['street2'] = lines[1]
                    
        except Exception as e:
            logger.error(f"Error parsing address: {e}")
        
        return address
    
    def scrape_facility_details(self, detail_url: str) -> Optional[Dict]:
        """
        Scrape detailed facility information from facility detail page.
        
        Args:
            detail_url: URL to facility detail page
            
        Returns:
            Detailed facility data dictionary
        """
        try:
            response = self.make_request(detail_url)
            if not response:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            details = {}
            
            # Extract comprehensive service information
            service_sections = soup.find_all(['div', 'section'], class_=re.compile(r'service|treatment'))
            for section in service_sections:
                section_text = section.get_text().lower()
                
                if 'outpatient' in section_text:
                    details['outpatient_services'] = True
                if 'intensive outpatient' in section_text or 'iop' in section_text:
                    details['intensive_outpatient'] = True
                if 'partial hospitalization' in section_text or 'php' in section_text:
                    details['partial_hospitalization'] = True
                if 'medication assisted' in section_text or 'mat' in section_text:
                    details['mat_services'] = True
                if 'opioid treatment' in section_text or 'otp' in section_text:
                    details['opioid_treatment_program'] = True
            
            # Extract insurance information  
            insurance_section = soup.find(['div', 'section'], class_=re.compile(r'insurance|payment'))
            if insurance_section:
                insurance_text = insurance_section.get_text().lower()
                details['insurance'] = {
                    'medicaid': 'medicaid' in insurance_text,
                    'medicare': 'medicare' in insurance_text,
                    'private': 'private' in insurance_text or 'commercial' in insurance_text,
                    'sliding_scale': 'sliding scale' in insurance_text,
                    'free': 'free' in insurance_text or 'no cost' in insurance_text
                }
            
            # Extract contact information
            contact_section = soup.find(['div', 'section'], class_=re.compile(r'contact'))
            if contact_section:
                website_link = contact_section.find('a', href=re.compile(r'http'))
                if website_link:
                    details['website'] = website_link.get('href')
                
                email_link = contact_section.find('a', href=re.compile(r'mailto:'))
                if email_link:
                    details['email'] = email_link.get('href').replace('mailto:', '')
            
            # Extract hours of operation
            hours_section = soup.find(['div', 'section'], class_=re.compile(r'hours|schedule'))
            if hours_section:
                details['hours_text'] = hours_section.get_text(strip=True)
            
            return details
            
        except Exception as e:
            logger.error(f"Error scraping facility details from {detail_url}: {e}")
            return None
    
    def extract_json_from_page(self, page_content: bytes) -> List[Dict]:
        """
        Extract facility data from JSON embedded in page content.
        
        Args:
            page_content: Raw page content
            
        Returns:
            List of facility dictionaries
        """
        facilities = []
        
        try:
            content_str = page_content.decode('utf-8', errors='ignore')
            
            # Look for JSON data in script tags
            json_patterns = [
                r'window\.__INITIAL_STATE__\s*=\s*(\{.*?\});',
                r'window\.facilityData\s*=\s*(\[.*?\]);',
                r'"facilities"\s*:\s*(\[.*?\])',
                r'var\s+facilities\s*=\s*(\[.*?\]);'
            ]
            
            for pattern in json_patterns:
                matches = re.findall(pattern, content_str, re.DOTALL)
                for match in matches:
                    try:
                        data = json.loads(match)
                        
                        if isinstance(data, list):
                            facilities.extend(data)
                        elif isinstance(data, dict) and 'facilities' in data:
                            facilities.extend(data['facilities'])
                        elif isinstance(data, dict) and 'results' in data:
                            facilities.extend(data['results'])
                            
                    except json.JSONDecodeError:
                        continue
            
        except Exception as e:
            logger.error(f"Error extracting JSON from page: {e}")
        
        return facilities
    
    def is_outpatient_facility(self, facility_data: Dict) -> bool:
        """
        Determine if facility offers outpatient services.
        
        Args:
            facility_data: Facility data dictionary
            
        Returns:
            True if facility offers outpatient services
        """
        # Check services field
        services = facility_data.get('services', [])
        services_text = ' '.join(services).lower() if services else ''
        
        # Check name and description
        name_text = facility_data.get('name', '').lower()
        
        # Combine all text for keyword matching
        combined_text = f"{services_text} {name_text}"
        
        # Check for outpatient keywords
        return any(keyword in combined_text for keyword in self.outpatient_keywords)
    
    def scrape_state_facilities(self, state: str) -> List[Dict]:
        """
        Scrape all outpatient facilities for a specific state.
        
        Args:
            state: State abbreviation
            
        Returns:
            List of facility dictionaries
        """
        logger.info(f"Scraping outpatient facilities for {state}")
        
        all_facilities = []
        cities = self.state_info.get(state, [])
        
        # Search by state first
        state_facilities = self.search_facilities_by_location(state)
        all_facilities.extend(state_facilities)
        
        # Then search by major cities to ensure coverage
        for city in cities:
            logger.info(f"Searching in {city}, {state}")
            
            city_facilities = self.search_facilities_by_location(state, city)
            
            # Deduplicate based on name and address
            for facility in city_facilities:
                if not self.is_duplicate_facility(facility, all_facilities):
                    all_facilities.append(facility)
            
            # Rate limiting between cities
            time.sleep(random.uniform(2, 5))
        
        logger.info(f"Scraped {len(all_facilities)} unique outpatient facilities from {state}")
        return all_facilities
    
    def is_duplicate_facility(self, facility: Dict, existing_facilities: List[Dict]) -> bool:
        """
        Check if facility is a duplicate of existing ones.
        
        Args:
            facility: Facility to check
            existing_facilities: List of existing facilities
            
        Returns:
            True if facility is a duplicate
        """
        facility_name = facility.get('name', '').lower().strip()
        facility_address = facility.get('address', {})
        facility_street = facility_address.get('street1', '').lower().strip()
        
        for existing in existing_facilities:
            existing_name = existing.get('name', '').lower().strip()
            existing_address = existing.get('address', {})
            existing_street = existing_address.get('street1', '').lower().strip()
            
            # Check name similarity and address match
            if (facility_name == existing_name or 
                (facility_street and facility_street == existing_street)):
                return True
        
        return False
    
    def scrape_all_states(self) -> List[Dict]:
        """
        Scrape outpatient facilities from all states.
        
        Returns:
            List of all facility dictionaries
        """
        logger.info("Starting comprehensive web scraping of outpatient facilities")
        
        all_facilities = []
        
        for state in self.state_info.keys():
            try:
                state_facilities = self.scrape_state_facilities(state)
                all_facilities.extend(state_facilities)
                
                self.extracted_count += len(state_facilities)
                
                # Longer pause between states
                time.sleep(random.uniform(10, 20))
                
            except Exception as e:
                logger.error(f"Error scraping facilities from {state}: {e}")
                continue
        
        logger.info(f"Total outpatient facilities scraped: {len(all_facilities)}")
        return all_facilities
    
    def save_to_json(self, facilities: List[Dict], filepath: str):
        """
        Save facilities data to JSON file.
        
        Args:
            facilities: List of facility dictionaries
            filepath: Output file path
        """
        try:
            output_data = {
                "extraction_metadata": {
                    "extraction_date": datetime.now().isoformat(),
                    "total_facilities": len(facilities),
                    "data_source": "SAMHSA Treatment Locator (Web Scraping)",
                    "extraction_method": "Web Scraping",
                    "outpatient_keywords": self.outpatient_keywords,
                    "geographic_coverage": list(self.state_info.keys())
                },
                "facilities": facilities
            }
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            # Save to file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {len(facilities)} facilities to {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving data to {filepath}: {e}")
    
    def run_scraping(self, output_path: str = None) -> str:
        """
        Run the complete web scraping process.
        
        Args:
            output_path: Optional custom output path
            
        Returns:
            Path to the saved JSON file
        """
        if output_path is None:
            output_path = "/Users/benweiss/Code/narr_extractor/03_raw_data/treatment_centers/outpatient/samhsa/samhsa_outpatient_facilities_scraped.json"
        
        logger.info("Starting SAMHSA outpatient treatment centers web scraping")
        
        try:
            # Scrape facilities from all states
            facilities = self.scrape_all_states()
            
            # Save to JSON
            self.save_to_json(facilities, output_path)
            
            logger.info(f"Web scraping completed successfully. Total facilities: {len(facilities)}")
            return output_path
            
        except Exception as e:
            logger.error(f"Web scraping failed: {e}")
            raise

def main():
    """Main execution function."""
    scraper = SAMHSAWebScraper()
    
    try:
        output_file = scraper.run_scraping()
        print(f"\nWeb scraping completed successfully!")
        print(f"Data saved to: {output_file}")
        print(f"Total outpatient facilities scraped: {scraper.extracted_count}")
        
    except Exception as e:
        print(f"Web scraping failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())