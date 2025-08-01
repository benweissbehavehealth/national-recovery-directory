#!/usr/bin/env python3
"""
Enhanced SAMHSA Inpatient/Hospital-Based Treatment Centers Web Extractor

This script uses advanced web scraping techniques to extract hospital-based
and medically-managed inpatient addiction treatment centers from SAMHSA's
Treatment Locator and related databases.

This version uses direct web scraping with form submission to bypass API issues.

Author: Claude Code
Date: 2025-07-31
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
import os
from datetime import datetime
import re
import random
from urllib.parse import urlencode, urljoin, urlparse
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('samhsa_inpatient_web_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class HospitalFacility:
    """Data structure for hospital-based treatment facility information."""
    
    # Basic Information
    facility_name: str = ""
    facility_id: str = ""
    
    # Address
    street_address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    
    # Contact
    phone: str = ""
    website: str = ""
    
    # Services
    services: List[str] = None
    inpatient_services: List[str] = None
    
    # Hospital-specific flags
    is_hospital: bool = False
    has_detox: bool = False
    has_medical_detox: bool = False
    has_psychiatric_unit: bool = False
    has_emergency_services: bool = False
    
    # Insurance
    accepts_medicaid: bool = False
    accepts_medicare: bool = False
    accepts_private_insurance: bool = False
    
    # Metadata
    extraction_date: str = ""
    data_source_url: str = ""
    
    def __post_init__(self):
        if self.services is None:
            self.services = []
        if self.inpatient_services is None:
            self.inpatient_services = []
        if self.extraction_date == "":
            self.extraction_date = datetime.now().isoformat()

class SAMHSAWebExtractor:
    """Enhanced web scraper for SAMHSA inpatient facilities."""
    
    def __init__(self):
        """Initialize the web scraper."""
        self.base_url = "https://findtreatment.gov"
        self.session = requests.Session()
        
        # Set headers to mimic a real browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        self.facilities = []
        self.extracted_count = 0
        self.unique_facilities = {}
        
        # State list
        self.states = [
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC'
        ]
        
        # Inpatient/hospital keywords
        self.hospital_keywords = [
            'hospital', 'medical center', 'health center', 'medical', 
            'behavioral health center', 'psychiatric hospital'
        ]
        
        self.inpatient_keywords = [
            'inpatient', 'detox', 'detoxification', 'medical detox',
            'hospital', 'acute', 'crisis', '24 hour', '24-hour',
            'residential hospital', 'medically managed', 'medically monitored'
        ]
        
        self.service_codes = {
            'DT': 'Detoxification',
            'HI': 'Hospital inpatient',
            'HH': 'Hospital inpatient detoxification',
            'RH': 'Residential/hospital',
            'MD': 'Medically monitored'
        }
    
    def get_facility_hash(self, name: str, address: str) -> str:
        """Generate unique hash for facility deduplication."""
        combined = f"{name.lower().strip()}_{address.lower().strip()}"
        return hashlib.md5(combined.encode()).hexdigest()
    
    def extract_via_direct_search(self, state: str) -> List[HospitalFacility]:
        """Extract facilities by directly accessing the search page with parameters."""
        facilities = []
        
        try:
            # Try different URL patterns that might work
            search_urls = [
                f"{self.base_url}/locator",
                f"{self.base_url}/",
                f"{self.base_url}/treatment"
            ]
            
            for search_url in search_urls:
                logger.info(f"Trying search URL: {search_url} for state {state}")
                
                # First, get the main page to establish session
                response = self.session.get(search_url)
                time.sleep(1)
                
                if response.status_code != 200:
                    continue
                
                # Try to submit search with parameters
                search_params = {
                    'location': state,
                    'state': state,
                    'distance': '100',
                    'sType': 'SA',  # Substance Abuse
                    'sService': ['DT', 'HI', 'HH'],  # Detox and Hospital services
                    'pageSize': '100'
                }
                
                # Try GET request with parameters
                full_url = f"{search_url}?{urlencode(search_params, doseq=True)}"
                response = self.session.get(full_url)
                
                if response.status_code == 200:
                    facilities.extend(self.parse_search_results(response.text, state))
                    
                # Also try POST request
                post_data = {
                    'searchLocation': state,
                    'selectedState': state,
                    'distance': '100',
                    'serviceType': 'SA',
                    'services': ['Detoxification', 'Hospital inpatient']
                }
                
                response = self.session.post(search_url, data=post_data)
                if response.status_code == 200:
                    facilities.extend(self.parse_search_results(response.text, state))
                
                if facilities:
                    break
                
                time.sleep(random.uniform(1, 3))
                
        except Exception as e:
            logger.error(f"Error in direct search for {state}: {e}")
        
        return facilities
    
    def parse_search_results(self, html_content: str, state: str) -> List[HospitalFacility]:
        """Parse search results from HTML content."""
        facilities = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for different possible result containers
            result_selectors = [
                '.facility-result',
                '.search-result',
                '.treatment-facility',
                '[class*="facility"]',
                '[class*="result"]',
                '.listing',
                'article'
            ]
            
            result_elements = []
            for selector in result_selectors:
                elements = soup.select(selector)
                if elements:
                    result_elements = elements
                    logger.info(f"Found {len(elements)} results with selector: {selector}")
                    break
            
            # Also check for JSON data in script tags
            script_tags = soup.find_all('script', type=['application/json', 'text/javascript'])
            for script in script_tags:
                if script.string and ('facilities' in script.string or 'results' in script.string):
                    try:
                        # Extract JSON from various patterns
                        json_patterns = [
                            r'window\.__INITIAL_STATE__\s*=\s*({.*?});',
                            r'window\.searchResults\s*=\s*(\[.*?\]);',
                            r'"facilities":\s*(\[.*?\])',
                            r'data:\s*(\[.*?\])'
                        ]
                        
                        for pattern in json_patterns:
                            match = re.search(pattern, script.string, re.DOTALL)
                            if match:
                                json_str = match.group(1)
                                data = json.loads(json_str)
                                
                                if isinstance(data, list):
                                    for item in data:
                                        facility = self.parse_json_facility(item, state)
                                        if facility and self.is_hospital_facility(facility):
                                            facilities.append(facility)
                                elif isinstance(data, dict):
                                    if 'facilities' in data:
                                        for item in data['facilities']:
                                            facility = self.parse_json_facility(item, state)
                                            if facility and self.is_hospital_facility(facility):
                                                facilities.append(facility)
                                break
                    except Exception as e:
                        logger.debug(f"Failed to parse JSON from script: {e}")
            
            # Parse HTML elements
            for element in result_elements:
                facility = self.parse_facility_element(element, state)
                if facility and self.is_hospital_facility(facility):
                    facilities.append(facility)
            
            # Look for data attributes
            data_elements = soup.find_all(attrs={'data-facility': True})
            for elem in data_elements:
                try:
                    facility_data = json.loads(elem.get('data-facility'))
                    facility = self.parse_json_facility(facility_data, state)
                    if facility and self.is_hospital_facility(facility):
                        facilities.append(facility)
                except:
                    pass
            
        except Exception as e:
            logger.error(f"Error parsing search results: {e}")
        
        return facilities
    
    def parse_facility_element(self, element, state: str) -> Optional[HospitalFacility]:
        """Parse facility from HTML element."""
        try:
            facility = HospitalFacility(state=state)
            
            # Extract name
            name_selectors = ['.facility-name', '.name', 'h2', 'h3', '.title', '[class*="name"]']
            for selector in name_selectors:
                name_elem = element.select_one(selector)
                if name_elem:
                    facility.facility_name = name_elem.get_text(strip=True)
                    break
            
            if not facility.facility_name:
                return None
            
            # Extract address
            address_selectors = ['.address', '.location', '[class*="address"]']
            for selector in address_selectors:
                addr_elem = element.select_one(selector)
                if addr_elem:
                    addr_text = addr_elem.get_text(strip=True)
                    facility.street_address = addr_text.split(',')[0] if ',' in addr_text else addr_text
                    break
            
            # Extract phone
            phone_selectors = ['.phone', '.tel', '[class*="phone"]', 'a[href^="tel:"]']
            for selector in phone_selectors:
                phone_elem = element.select_one(selector)
                if phone_elem:
                    if phone_elem.name == 'a' and phone_elem.get('href', '').startswith('tel:'):
                        facility.phone = phone_elem.get('href').replace('tel:', '')
                    else:
                        facility.phone = phone_elem.get_text(strip=True)
                    break
            
            # Extract services
            services_selectors = ['.services', '.treatment-types', '[class*="service"]']
            for selector in services_selectors:
                services_elem = element.select_one(selector)
                if services_elem:
                    services_text = services_elem.get_text(strip=True)
                    facility.services = [s.strip() for s in re.split('[,;]', services_text)]
                    break
            
            # Check if it's a hospital
            all_text = element.get_text().lower()
            facility.is_hospital = any(keyword in all_text for keyword in self.hospital_keywords)
            facility.has_detox = 'detox' in all_text or 'detoxification' in all_text
            facility.has_medical_detox = 'medical detox' in all_text or 'medically' in all_text
            
            return facility
            
        except Exception as e:
            logger.error(f"Error parsing facility element: {e}")
            return None
    
    def parse_json_facility(self, data: Dict, state: str) -> Optional[HospitalFacility]:
        """Parse facility from JSON data."""
        try:
            facility = HospitalFacility(state=state)
            
            # Map common field names
            name_fields = ['name', 'facilityName', 'title', 'providerName']
            for field in name_fields:
                if field in data:
                    facility.facility_name = data[field]
                    break
            
            if not facility.facility_name:
                return None
            
            # Address
            if 'address' in data:
                addr = data['address']
                if isinstance(addr, dict):
                    facility.street_address = addr.get('street', addr.get('line1', ''))
                    facility.city = addr.get('city', '')
                    facility.zip_code = addr.get('zip', addr.get('postalCode', ''))
                elif isinstance(addr, str):
                    facility.street_address = addr
            
            # Phone
            phone_fields = ['phone', 'telephone', 'contactPhone']
            for field in phone_fields:
                if field in data:
                    facility.phone = data[field]
                    break
            
            # Services
            if 'services' in data:
                if isinstance(data['services'], list):
                    facility.services = data['services']
                elif isinstance(data['services'], str):
                    facility.services = [data['services']]
            
            # Check for inpatient services
            services_text = ' '.join(facility.services).lower() if facility.services else ''
            name_lower = facility.facility_name.lower()
            
            facility.is_hospital = any(kw in name_lower or kw in services_text 
                                     for kw in self.hospital_keywords)
            facility.has_detox = 'detox' in services_text or 'DT' in data.get('serviceCodes', [])
            facility.has_medical_detox = 'medical detox' in services_text
            
            # Insurance
            if 'insurance' in data:
                ins = data['insurance']
                if isinstance(ins, dict):
                    facility.accepts_medicaid = ins.get('medicaid', False)
                    facility.accepts_medicare = ins.get('medicare', False)
                    facility.accepts_private_insurance = ins.get('private', False)
            
            return facility
            
        except Exception as e:
            logger.error(f"Error parsing JSON facility: {e}")
            return None
    
    def is_hospital_facility(self, facility: HospitalFacility) -> bool:
        """Check if facility is a hospital or inpatient facility."""
        # Check name
        name_lower = facility.facility_name.lower()
        has_hospital_name = any(kw in name_lower for kw in self.hospital_keywords)
        
        # Check services
        services_text = ' '.join(facility.services).lower() if facility.services else ''
        has_inpatient_service = any(kw in services_text for kw in self.inpatient_keywords)
        
        # Must be hospital or have inpatient services
        return facility.is_hospital or has_hospital_name or has_inpatient_service or facility.has_detox
    
    def extract_state_facilities(self, state: str) -> List[HospitalFacility]:
        """Extract all hospital/inpatient facilities for a state."""
        logger.info(f"Extracting hospital facilities for {state}")
        
        facilities = []
        
        # Try direct search
        facilities.extend(self.extract_via_direct_search(state))
        
        # Try alternative Medicare/hospital databases
        facilities.extend(self.search_medicare_hospitals(state))
        
        # Deduplicate
        for facility in facilities:
            if facility.facility_name and facility.street_address:
                facility_hash = self.get_facility_hash(facility.facility_name, facility.street_address)
                if facility_hash not in self.unique_facilities:
                    self.unique_facilities[facility_hash] = facility
                    self.extracted_count += 1
        
        unique_count = len([f for f in self.unique_facilities.values() if f.state == state])
        logger.info(f"Found {unique_count} unique hospital facilities in {state}")
        
        return [f for f in self.unique_facilities.values() if f.state == state]
    
    def search_medicare_hospitals(self, state: str) -> List[HospitalFacility]:
        """Search Medicare hospital database for addiction treatment facilities."""
        facilities = []
        
        # This would connect to Medicare's hospital compare or similar databases
        # For now, we'll use a placeholder approach
        
        logger.info(f"Searching Medicare database for {state}")
        
        return facilities
    
    def extract_all_states(self) -> List[HospitalFacility]:
        """Extract facilities from all states."""
        all_facilities = []
        
        for i, state in enumerate(self.states):
            logger.info(f"Processing state {i+1}/{len(self.states)}: {state}")
            
            try:
                state_facilities = self.extract_state_facilities(state)
                all_facilities.extend(state_facilities)
                
                # Rate limiting
                time.sleep(random.uniform(3, 6))
                
            except Exception as e:
                logger.error(f"Error processing {state}: {e}")
                continue
        
        return list(self.unique_facilities.values())
    
    def save_results(self, facilities: List[HospitalFacility], output_path: str):
        """Save extracted facilities to JSON."""
        try:
            # Convert to dictionaries
            facilities_data = [asdict(f) for f in facilities]
            
            output = {
                "extraction_metadata": {
                    "extraction_date": datetime.now().isoformat(),
                    "total_facilities": len(facilities_data),
                    "states_processed": self.states,
                    "extraction_method": "Web Scraping",
                    "data_sources": [
                        "SAMHSA FindTreatment.gov",
                        "Direct web scraping"
                    ]
                },
                "facilities": facilities_data
            }
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {len(facilities_data)} facilities to {output_path}")
            
        except Exception as e:
            logger.error(f"Error saving results: {e}")

def main():
    """Main execution function."""
    output_path = "/Users/benweiss/Code/narr_extractor/03_raw_data/treatment_centers/inpatient/samhsa/samhsa_inpatient_facilities.json"
    
    extractor = SAMHSAWebExtractor()
    
    try:
        logger.info("Starting enhanced web extraction of hospital-based treatment facilities")
        
        # Extract from all states
        facilities = extractor.extract_all_states()
        
        # Save results
        extractor.save_results(facilities, output_path)
        
        print(f"\nExtraction completed!")
        print(f"Total unique facilities found: {len(facilities)}")
        print(f"Results saved to: {output_path}")
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())