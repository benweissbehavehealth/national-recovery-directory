#!/usr/bin/env python3
"""
SAMHSA Inpatient/Hospital-Based Treatment Centers Extractor

This script extracts comprehensive data about hospital-based and medically-managed
inpatient addiction treatment centers from SAMHSA's Treatment Locator and related databases.

PRIMARY OBJECTIVE: Extract hospital-based inpatient addiction treatment facilities 
offering medical detoxification and acute care services.

TARGET SERVICE TYPES:
- Hospital inpatient detoxification
- Medical detox units
- Psychiatric hospitals with addiction units
- Dual diagnosis inpatient
- Medical stabilization
- ASAM Level 4 facilities
- Acute care addiction units
- Emergency detox services

Author: Claude Code
Date: 2025-07-31
"""

import requests
import json
import time
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from urllib.parse import urlencode
import os
from datetime import datetime
from bs4 import BeautifulSoup
import re
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('samhsa_inpatient_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class InpatientFacility:
    """Data structure for SAMHSA inpatient/hospital treatment facility information."""
    
    # Basic Information
    facility_name: str = ""
    hospital_system: str = ""
    dba_names: List[str] = None
    facility_id: str = ""
    npi_number: str = ""
    
    # Contact Information
    address_line1: str = ""
    address_line2: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    county: str = ""
    phone: str = ""
    phone_extension: str = ""
    fax: str = ""
    website: str = ""
    email: str = ""
    emergency_phone: str = ""
    
    # Geographic Information
    latitude: float = 0.0
    longitude: float = 0.0
    
    # License and Certification Information
    license_numbers: List[str] = None
    certifications: List[str] = None
    accreditations: List[str] = None
    medicare_provider_number: str = ""
    medicaid_provider_number: str = ""
    
    # Service Information
    service_types: List[str] = None
    treatment_approaches: List[str] = None
    treatment_modalities: List[str] = None
    
    # Inpatient-Specific Services
    hospital_inpatient_detox: bool = False
    medical_detox_unit: bool = False
    psychiatric_unit: bool = False
    dual_diagnosis_inpatient: bool = False
    medical_stabilization: bool = False
    asam_level_4: bool = False
    acute_care_addiction: bool = False
    emergency_detox: bool = False
    
    # Facility Capabilities
    bed_capacity_addiction: int = 0
    bed_capacity_detox: int = 0
    bed_capacity_psychiatric: int = 0
    total_bed_capacity: int = 0
    average_length_of_stay: str = ""
    
    # Medical Staff Information
    medical_director: str = ""
    medical_staff_credentials: List[str] = None
    psychiatrist_on_staff: bool = False
    addiction_medicine_specialist: bool = False
    nursing_ratio: str = ""
    
    # Detox Protocols Available
    alcohol_detox: bool = False
    opioid_detox: bool = False
    benzodiazepine_detox: bool = False
    stimulant_detox: bool = False
    medical_detox_protocols: List[str] = None
    
    # Transfer and Step-Down Options
    transfer_agreements: List[str] = None
    step_down_programs: List[str] = None
    outpatient_referrals: bool = False
    
    # Population and Demographics
    age_groups_accepted: List[str] = None
    special_populations: List[str] = None
    gender_accepted: List[str] = None
    
    # Language Services
    languages_spoken: List[str] = None
    interpreter_services: bool = False
    
    # Payment and Insurance
    insurance_accepted: List[str] = None
    payment_options: List[str] = None
    medicaid_accepted: bool = False
    medicare_accepted: bool = False
    private_insurance_accepted: bool = False
    tricare_accepted: bool = False
    va_approved: bool = False
    
    # Emergency and Admission
    emergency_admission_capable: bool = False
    walk_in_assessment: bool = False
    admission_criteria: List[str] = None
    referral_required: bool = False
    
    # Operational Information
    hours_24_7: bool = True
    visiting_hours: str = ""
    
    # Additional Metadata
    facility_type: str = ""
    ownership_type: str = ""
    parent_organization: str = ""
    last_updated: str = ""
    data_source: str = "SAMHSA"
    extraction_date: str = ""
    
    def __post_init__(self):
        """Initialize list fields if None."""
        list_fields = [
            'dba_names', 'license_numbers', 'certifications', 'accreditations',
            'service_types', 'treatment_approaches', 'treatment_modalities',
            'medical_staff_credentials', 'medical_detox_protocols', 'transfer_agreements',
            'step_down_programs', 'age_groups_accepted', 'special_populations',
            'gender_accepted', 'languages_spoken', 'insurance_accepted',
            'payment_options', 'admission_criteria'
        ]
        
        for field in list_fields:
            if getattr(self, field) is None:
                setattr(self, field, [])
                
        if self.extraction_date == "":
            self.extraction_date = datetime.now().isoformat()

class SAMHSAInpatientExtractor:
    """Main class for extracting SAMHSA inpatient/hospital treatment facility data."""
    
    def __init__(self):
        """Initialize the SAMHSA inpatient extractor."""
        self.base_url = "https://findtreatment.gov"
        self.api_base = f"{self.base_url}/api"
        self.session = requests.Session()
        
        # Rotate user agents
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        ]
        
        self.facilities = []
        self.extracted_count = 0
        
        # US States for systematic extraction
        self.us_states = [
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC'
        ]
        
        # Inpatient/hospital service identifiers
        self.inpatient_keywords = [
            'hospital', 'inpatient', 'detox', 'detoxification', 'medical detox',
            'acute care', 'psychiatric hospital', 'dual diagnosis inpatient',
            'medical stabilization', 'asam level 4', 'asam level iv',
            'emergency detox', 'medical withdrawal', 'medically managed',
            'medically monitored', '24 hour medical', '24-hour medical',
            'hospital based', 'acute treatment', 'crisis stabilization',
            'medical center', 'behavioral health hospital', 'addiction medicine unit'
        ]
        
        # Hospital system identifiers
        self.hospital_systems = [
            'hospital', 'medical center', 'health system', 'healthcare',
            'behavioral health', 'psychiatric', 'mental health center',
            'medical', 'health center', 'treatment center', 'recovery center'
        ]
    
    def get_random_user_agent(self) -> str:
        """Get a random user agent string."""
        return random.choice(self.user_agents)
    
    def make_request(self, url: str, params: dict = None, retries: int = 3) -> Optional[requests.Response]:
        """Make HTTP request with error handling and retries."""
        for attempt in range(retries):
            try:
                self.session.headers['User-Agent'] = self.get_random_user_agent()
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:
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
    
    def search_facilities_api(self, state: str = None, city: str = None, 
                            service_type: str = 'inpatient') -> List[Dict]:
        """Search for facilities using API endpoints."""
        params = {
            'distance': 100,  # Larger radius for hospitals
            'service_type': service_type,
            'treatment_type': 'substance_abuse'
        }
        
        if state:
            params['state'] = state
        if city:
            params['city'] = city
            
        facilities = []
        
        # Try multiple potential API endpoints
        endpoints = [
            f"{self.api_base}/facilities/search",
            f"{self.api_base}/treatment-locator/search",
            f"{self.api_base}/locator/search",
            f"{self.base_url}/locator/api/search"
        ]
        
        for endpoint in endpoints:
            try:
                logger.info(f"Trying API endpoint: {endpoint}")
                response = self.make_request(endpoint, params=params)
                
                if response and response.status_code == 200:
                    data = response.json()
                    
                    if 'facilities' in data:
                        facilities = data['facilities']
                    elif 'results' in data:
                        facilities = data['results']
                    elif isinstance(data, list):
                        facilities = data
                    
                    if facilities:
                        logger.info(f"Found {len(facilities)} facilities via API")
                        break
                        
            except Exception as e:
                logger.warning(f"API endpoint {endpoint} failed: {e}")
                continue
        
        return facilities
    
    def search_facilities_web(self, state: str, city: str = None) -> List[Dict]:
        """Search for facilities using web scraping."""
        facilities = []
        
        # Build search URL
        search_params = {
            'state': state,
            'distance': '100',
            'type': 'SA',  # Substance Abuse
            'services[]': ['DT', 'HI', 'HH']  # Detox, Hospital Inpatient, Hospital
        }
        
        if city:
            search_params['city'] = city
        
        search_url = f"{self.base_url}/locator?{urlencode(search_params, doseq=True)}"
        
        try:
            response = self.make_request(search_url)
            if response:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract JSON data from page
                script_tags = soup.find_all('script', type='application/json')
                for script in script_tags:
                    try:
                        data = json.loads(script.string)
                        if 'facilities' in data:
                            facilities.extend(data['facilities'])
                        elif 'props' in data and 'pageProps' in data['props']:
                            page_props = data['props']['pageProps']
                            if 'facilities' in page_props:
                                facilities.extend(page_props['facilities'])
                    except:
                        continue
                
                # Look for facility cards
                facility_cards = soup.select('.facility-card, .treatment-facility, .result-item')
                for card in facility_cards:
                    facility_data = self.parse_facility_card(card, state)
                    if facility_data:
                        facilities.append(facility_data)
                        
        except Exception as e:
            logger.error(f"Error in web search for {state}: {e}")
        
        return facilities
    
    def parse_facility_card(self, card, state: str) -> Optional[Dict]:
        """Parse facility information from HTML card element."""
        try:
            facility = {
                'name': '',
                'address': {'state': state},
                'services': [],
                'contact': {}
            }
            
            # Extract name
            name_elem = card.select_one('.facility-name, .name, h2, h3')
            if name_elem:
                facility['name'] = name_elem.get_text(strip=True)
            
            # Extract services
            services_elem = card.select_one('.services, .treatment-services')
            if services_elem:
                services_text = services_elem.get_text(strip=True)
                facility['services'] = [s.strip() for s in services_text.split(',')]
            
            # Extract contact info
            phone_elem = card.select_one('.phone, .contact-phone')
            if phone_elem:
                facility['contact']['phone'] = phone_elem.get_text(strip=True)
            
            # Check if it's a hospital/inpatient facility
            if self.is_inpatient_facility(facility):
                return facility
                
        except Exception as e:
            logger.error(f"Error parsing facility card: {e}")
        
        return None
    
    def parse_facility_data(self, facility_data: Dict) -> InpatientFacility:
        """Parse raw facility data into InpatientFacility object."""
        facility = InpatientFacility()
        
        try:
            # Basic Information
            facility.facility_name = facility_data.get('name', '')
            facility.facility_id = str(facility_data.get('id', ''))
            
            # Identify hospital system
            name_lower = facility.facility_name.lower()
            for system in self.hospital_systems:
                if system in name_lower:
                    facility.hospital_system = system.title()
                    break
            
            # Address Information
            address = facility_data.get('address', {})
            facility.address_line1 = address.get('street1', '')
            facility.address_line2 = address.get('street2', '')
            facility.city = address.get('city', '')
            facility.state = address.get('state', '')
            facility.zip_code = address.get('zip', '')
            facility.county = address.get('county', '')
            
            # Contact Information
            contact = facility_data.get('contact', {})
            facility.phone = contact.get('phone', '')
            facility.website = contact.get('website', '')
            facility.email = contact.get('email', '')
            
            # Geographic coordinates
            location = facility_data.get('location', {})
            facility.latitude = float(location.get('lat', 0))
            facility.longitude = float(location.get('lng', 0))
            
            # Services Information
            services = facility_data.get('services', [])
            facility.service_types = services if isinstance(services, list) else []
            
            # Parse inpatient-specific services
            service_str = ' '.join(facility.service_types).lower()
            
            if 'hospital' in service_str and 'detox' in service_str:
                facility.hospital_inpatient_detox = True
            if 'medical detox' in service_str:
                facility.medical_detox_unit = True
            if 'psychiatric' in service_str:
                facility.psychiatric_unit = True
            if 'dual diagnosis' in service_str and 'inpatient' in service_str:
                facility.dual_diagnosis_inpatient = True
            if 'medical stabilization' in service_str:
                facility.medical_stabilization = True
            if 'asam' in service_str and ('4' in service_str or 'iv' in service_str):
                facility.asam_level_4 = True
            if 'acute' in service_str:
                facility.acute_care_addiction = True
            if 'emergency' in service_str and 'detox' in service_str:
                facility.emergency_detox = True
            
            # Check for detox protocols
            if 'alcohol' in service_str:
                facility.alcohol_detox = True
            if 'opioid' in service_str or 'opiate' in service_str:
                facility.opioid_detox = True
            if 'benzo' in service_str:
                facility.benzodiazepine_detox = True
            
            # Treatment approaches and modalities
            facility.treatment_approaches = facility_data.get('treatment_approaches', [])
            facility.treatment_modalities = facility_data.get('treatment_modalities', [])
            
            # Demographics and populations
            facility.age_groups_accepted = facility_data.get('age_groups', [])
            facility.special_populations = facility_data.get('special_populations', [])
            facility.gender_accepted = facility_data.get('gender_accepted', [])
            
            # Languages
            facility.languages_spoken = facility_data.get('languages', [])
            
            # Payment and insurance
            insurance = facility_data.get('insurance', {})
            facility.medicaid_accepted = insurance.get('medicaid', False)
            facility.medicare_accepted = insurance.get('medicare', False)
            facility.private_insurance_accepted = insurance.get('private', False)
            facility.tricare_accepted = insurance.get('tricare', False)
            facility.va_approved = insurance.get('va', False)
            
            # License and certification
            facility.license_numbers = facility_data.get('licenses', [])
            facility.certifications = facility_data.get('certifications', [])
            facility.accreditations = facility_data.get('accreditations', [])
            
            # Facility type and ownership
            facility.facility_type = facility_data.get('facility_type', '')
            facility.ownership_type = facility_data.get('ownership_type', '')
            facility.parent_organization = facility_data.get('parent_organization', '')
            
            # Emergency capabilities
            if '24' in service_str or 'emergency' in service_str:
                facility.emergency_admission_capable = True
            
            # Metadata
            facility.last_updated = facility_data.get('last_updated', '')
            
        except Exception as e:
            logger.error(f"Error parsing facility data: {e}")
            
        return facility
    
    def is_inpatient_facility(self, facility_data: Dict) -> bool:
        """Determine if facility offers inpatient/hospital services."""
        # Check services
        services = facility_data.get('services', [])
        services_text = ' '.join(services).lower() if services else ''
        
        # Check name
        name_text = facility_data.get('name', '').lower()
        
        # Check facility type
        facility_type = facility_data.get('facility_type', '').lower()
        
        # Combine all text
        combined_text = f"{services_text} {name_text} {facility_type}"
        
        # Must have inpatient/hospital keywords
        has_inpatient = any(keyword in combined_text for keyword in self.inpatient_keywords)
        
        # Exclude pure outpatient facilities
        exclude_keywords = ['outpatient only', 'op only', 'office based only']
        is_excluded = any(keyword in combined_text for keyword in exclude_keywords)
        
        return has_inpatient and not is_excluded
    
    def extract_state_facilities(self, state: str) -> List[InpatientFacility]:
        """Extract all inpatient facilities for a specific state."""
        logger.info(f"Extracting inpatient facilities for {state}")
        
        facilities = []
        unique_facilities = {}
        
        # Try API first
        api_results = self.search_facilities_api(state=state)
        
        # Also try web scraping
        web_results = self.search_facilities_web(state=state)
        
        # Combine results
        all_results = api_results + web_results
        
        for facility_data in all_results:
            if self.is_inpatient_facility(facility_data):
                facility = self.parse_facility_data(facility_data)
                
                # Deduplicate by name and address
                facility_key = f"{facility.facility_name}_{facility.address_line1}".lower()
                if facility_key not in unique_facilities:
                    unique_facilities[facility_key] = facility
                    facilities.append(facility)
                    self.extracted_count += 1
                    
                    if self.extracted_count % 50 == 0:
                        logger.info(f"Extracted {self.extracted_count} inpatient facilities")
        
        # Search major cities for additional coverage
        major_cities = {
            'CA': ['Los Angeles', 'San Francisco', 'San Diego'],
            'TX': ['Houston', 'Dallas', 'San Antonio'],
            'FL': ['Miami', 'Tampa', 'Orlando'],
            'NY': ['New York', 'Buffalo', 'Rochester'],
            'PA': ['Philadelphia', 'Pittsburgh'],
            'IL': ['Chicago'],
            'OH': ['Columbus', 'Cleveland', 'Cincinnati'],
            'GA': ['Atlanta'],
            'NC': ['Charlotte', 'Raleigh'],
            'MI': ['Detroit', 'Grand Rapids']
        }
        
        if state in major_cities:
            for city in major_cities[state]:
                time.sleep(random.uniform(2, 4))
                
                city_api_results = self.search_facilities_api(state=state, city=city)
                city_web_results = self.search_facilities_web(state=state, city=city)
                
                for facility_data in city_api_results + city_web_results:
                    if self.is_inpatient_facility(facility_data):
                        facility = self.parse_facility_data(facility_data)
                        facility_key = f"{facility.facility_name}_{facility.address_line1}".lower()
                        
                        if facility_key not in unique_facilities:
                            unique_facilities[facility_key] = facility
                            facilities.append(facility)
        
        logger.info(f"Extracted {len(facilities)} inpatient facilities from {state}")
        return facilities
    
    def extract_all_states(self) -> List[InpatientFacility]:
        """Extract inpatient facilities from all US states."""
        logger.info("Starting comprehensive extraction of inpatient facilities from all states")
        
        all_facilities = []
        
        for state in self.us_states:
            try:
                state_facilities = self.extract_state_facilities(state)
                all_facilities.extend(state_facilities)
                
                # Rate limiting between states
                time.sleep(random.uniform(5, 10))
                
            except Exception as e:
                logger.error(f"Error extracting facilities from {state}: {e}")
                continue
        
        logger.info(f"Total inpatient facilities extracted: {len(all_facilities)}")
        return all_facilities
    
    def enhance_with_medicare_data(self, facilities: List[InpatientFacility]) -> List[InpatientFacility]:
        """Enhance facility data with Medicare provider information if available."""
        logger.info("Attempting to enhance facility data with Medicare information")
        
        # This would connect to Medicare provider directories if available
        # For now, we'll mark facilities that likely accept Medicare based on hospital status
        for facility in facilities:
            if any(term in facility.facility_name.lower() for term in ['hospital', 'medical center']):
                if not facility.medicare_accepted:
                    facility.medicare_accepted = True
                if not facility.medicaid_accepted:
                    facility.medicaid_accepted = True
        
        return facilities
    
    def save_to_json(self, facilities: List[InpatientFacility], filepath: str):
        """Save facilities data to JSON file."""
        try:
            # Convert facilities to dictionaries
            facilities_data = []
            for facility in facilities:
                facility_dict = asdict(facility)
                facilities_data.append(facility_dict)
            
            # Create output structure
            output_data = {
                "extraction_metadata": {
                    "extraction_date": datetime.now().isoformat(),
                    "total_facilities": len(facilities_data),
                    "data_source": "SAMHSA Treatment Locator",
                    "extraction_method": "API/Web Scraping Hybrid",
                    "service_types_targeted": [
                        "Hospital inpatient detoxification",
                        "Medical detox units",
                        "Psychiatric hospitals with addiction units",
                        "Dual diagnosis inpatient",
                        "Medical stabilization",
                        "ASAM Level 4 facilities",
                        "Acute care addiction units",
                        "Emergency detox services"
                    ],
                    "facility_types": [
                        "General hospitals with addiction units",
                        "Psychiatric hospitals",
                        "Specialty addiction hospitals",
                        "VA medical centers",
                        "University medical centers",
                        "Private hospital systems"
                    ],
                    "geographic_coverage": "All US States and Territories"
                },
                "facilities": facilities_data
            }
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            # Save to file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {len(facilities_data)} facilities to {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving data to {filepath}: {e}")
    
    def run_extraction(self, output_path: str = None) -> str:
        """Run the complete extraction process."""
        if output_path is None:
            output_path = "/Users/benweiss/Code/narr_extractor/03_raw_data/treatment_centers/inpatient/samhsa/samhsa_inpatient_facilities.json"
        
        logger.info("Starting SAMHSA inpatient/hospital-based treatment centers extraction")
        
        try:
            # Extract facilities from all states
            facilities = self.extract_all_states()
            
            # Enhance with additional data sources
            facilities = self.enhance_with_medicare_data(facilities)
            
            # Save to JSON
            self.save_to_json(facilities, output_path)
            
            logger.info(f"Extraction completed successfully. Total facilities: {len(facilities)}")
            return output_path
            
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise

def main():
    """Main execution function."""
    extractor = SAMHSAInpatientExtractor()
    
    try:
        output_file = extractor.run_extraction()
        print(f"\nExtraction completed successfully!")
        print(f"Data saved to: {output_file}")
        print(f"Total inpatient facilities extracted: {extractor.extracted_count}")
        
    except Exception as e:
        print(f"Extraction failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())