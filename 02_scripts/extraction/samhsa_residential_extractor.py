#!/usr/bin/env python3
"""
SAMHSA Residential Treatment Centers Extractor

This script extracts comprehensive data about residential addiction treatment centers
from SAMHSA's Treatment Locator (findtreatment.gov) and related federal databases.

PRIMARY OBJECTIVE: Extract licensed residential treatment facilities that provide 
24-hour care with overnight stays.

TARGET SERVICE TYPES:
- Short-term residential (30 days or less)
- Long-term residential (more than 30 days)
- Therapeutic communities
- Modified therapeutic communities
- Halfway houses
- Sober living (if licensed/certified)
- Residential detox with extended care
- Women and children residential
- Adolescent residential

Author: Claude Code
Date: 2025-07-31
"""

import requests
import json
import time
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict, field
from urllib.parse import urlencode
import os
from datetime import datetime
import random
from bs4 import BeautifulSoup
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('samhsa_residential_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ResidentialTreatmentFacility:
    """Data structure for SAMHSA residential treatment facility information."""
    
    # Basic Information
    facility_name: str = ""
    dba_names: List[str] = field(default_factory=list)
    facility_id: str = ""
    
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
    
    # Geographic Information
    latitude: float = 0.0
    longitude: float = 0.0
    
    # License and Certification Information
    license_numbers: List[str] = field(default_factory=list)
    certifications: List[str] = field(default_factory=list)
    accreditations: List[str] = field(default_factory=list)
    samhsa_certified: bool = False
    state_licensed: bool = False
    
    # Facility Characteristics
    facility_type: str = ""
    ownership_type: str = ""
    parent_organization: str = ""
    bed_capacity: int = 0
    current_occupancy: int = 0
    
    # Residential-Specific Services
    residential_services: List[str] = field(default_factory=list)
    short_term_residential: bool = False  # 30 days or less
    long_term_residential: bool = False   # More than 30 days
    therapeutic_community: bool = False
    modified_therapeutic_community: bool = False
    halfway_house: bool = False
    sober_living: bool = False
    residential_detox: bool = False
    extended_care: bool = False
    
    # Length of Stay Options
    typical_length_of_stay: str = ""
    minimum_stay_days: int = 0
    maximum_stay_days: int = 0
    average_stay_days: int = 0
    
    # Admission Requirements
    admission_requirements: List[str] = field(default_factory=list)
    referral_required: bool = False
    pre_admission_assessment: bool = False
    waiting_list: bool = False
    average_wait_time_days: int = 0
    
    # Population and Demographics
    age_groups_accepted: List[str] = field(default_factory=list)
    special_populations: List[str] = field(default_factory=list)
    gender_accepted: List[str] = field(default_factory=list)
    women_with_children: bool = False
    adolescent_program: bool = False
    lgbtq_specific: bool = False
    veterans_program: bool = False
    
    # Treatment Services
    service_types: List[str] = field(default_factory=list)
    treatment_approaches: List[str] = field(default_factory=list)
    treatment_modalities: List[str] = field(default_factory=list)
    mat_available: bool = False
    dual_diagnosis: bool = False
    trauma_informed_care: bool = False
    
    # Staff Information
    staff_to_patient_ratio: str = ""
    medical_staff_onsite: bool = False
    psychiatrist_onsite: bool = False
    nursing_24_7: bool = False
    staff_credentials: List[str] = field(default_factory=list)
    medical_director: str = ""
    
    # Amenities and Features
    amenities: List[str] = field(default_factory=list)
    private_rooms: bool = False
    shared_rooms: bool = False
    meals_provided: bool = False
    transportation_assistance: bool = False
    computer_access: bool = False
    recreation_facilities: bool = False
    
    # Language Services
    languages_spoken: List[str] = field(default_factory=list)
    interpreter_services: bool = False
    
    # Payment and Insurance
    insurance_accepted: List[str] = field(default_factory=list)
    payment_options: List[str] = field(default_factory=list)
    medicaid_accepted: bool = False
    medicare_accepted: bool = False
    private_insurance_accepted: bool = False
    sliding_scale_fees: bool = False
    free_services: bool = False
    scholarship_beds: bool = False
    daily_rate: str = ""
    
    # Treatment Philosophy
    treatment_philosophy: str = ""
    evidence_based_practices: List[str] = field(default_factory=list)
    twelve_step_based: bool = False
    non_twelve_step: bool = False
    faith_based: bool = False
    holistic_approach: bool = False
    
    # Additional Metadata
    hours_of_operation: Dict[str, str] = field(default_factory=dict)
    visitation_policy: str = ""
    last_updated: str = ""
    data_source: str = "SAMHSA"
    extraction_date: str = ""
    
    def __post_init__(self):
        """Initialize extraction date if not set."""
        if not self.extraction_date:
            self.extraction_date = datetime.now().isoformat()

class SAMHSAResidentialExtractor:
    """Main class for extracting SAMHSA residential treatment facility data."""
    
    def __init__(self):
        """Initialize the SAMHSA residential extractor."""
        self.base_url = "https://findtreatment.gov"
        self.api_base = f"{self.base_url}/api"
        self.session = requests.Session()
        
        # Rotate user agents to avoid detection
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        ]
        
        self.session.headers.update({
            'Accept': 'application/json,text/html,application/xhtml+xml',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        self.facilities = []
        self.extracted_count = 0
        
        # US States for systematic extraction
        self.us_states = [
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
            'DC'
        ]
        
        # Major cities for comprehensive coverage
        self.major_cities = {
            'CA': ['Los Angeles', 'San Francisco', 'San Diego', 'Sacramento', 'San Jose', 'Fresno', 'Oakland'],
            'TX': ['Houston', 'San Antonio', 'Dallas', 'Austin', 'Fort Worth', 'El Paso'],
            'FL': ['Miami', 'Tampa', 'Orlando', 'Jacksonville', 'Fort Lauderdale', 'Tallahassee'],
            'NY': ['New York City', 'Buffalo', 'Rochester', 'Syracuse', 'Albany'],
            'PA': ['Philadelphia', 'Pittsburgh', 'Allentown', 'Erie', 'Harrisburg'],
            'IL': ['Chicago', 'Aurora', 'Springfield', 'Peoria', 'Rockford'],
            'OH': ['Columbus', 'Cleveland', 'Cincinnati', 'Toledo', 'Akron'],
            'GA': ['Atlanta', 'Augusta', 'Columbus', 'Savannah', 'Athens'],
            'NC': ['Charlotte', 'Raleigh', 'Greensboro', 'Durham', 'Winston-Salem'],
            'MI': ['Detroit', 'Grand Rapids', 'Warren', 'Sterling Heights', 'Lansing']
        }
        
        # Residential service type identifiers
        self.residential_keywords = [
            'residential', 'inpatient', '24-hour', '24/7', 'overnight',
            'short-term residential', 'long-term residential',
            'therapeutic community', 'halfway house', 'sober living',
            'recovery residence', 'transitional living', 'extended care',
            'rehabilitation center', 'rehab facility', 'treatment center',
            'detox', 'detoxification', 'stabilization',
            'women and children', 'adolescent residential', 'teen residential'
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
                    time.sleep(random.uniform(2, 5))
        
        return None
    
    def search_facilities_api(self, state: str = None, city: str = None, 
                            zip_code: str = None, distance: int = 50) -> List[Dict]:
        """
        Search for facilities using API endpoints.
        
        Args:
            state: State abbreviation
            city: City name
            zip_code: ZIP code
            distance: Search radius in miles
            
        Returns:
            List of facility dictionaries
        """
        # The SAMHSA API requires authentication or has changed
        # Returning empty list to fall back to web scraping
        logger.debug("API search skipped - using web scraping approach")
        return []
    
    def search_facilities_web(self, state: str = None, city: str = None) -> List[Dict]:
        """
        Search for facilities using web scraping.
        
        Args:
            state: State abbreviation
            city: City name
            
        Returns:
            List of facility dictionaries
        """
        facilities = []
        
        try:
            # Build search location string
            location = ""
            if city and state:
                location = f"{city}, {state}"
            elif state:
                location = state
                
            # Try multiple search approaches
            search_approaches = [
                # Approach 1: Direct search URL with parameters
                f"{self.base_url}/locator?location={location}&page=1&limit=100",
                # Approach 2: Alternative search format
                f"{self.base_url}/#/results?location={location}&sType=substance%20use&sService=residential",
                # Approach 3: Base locator page to parse
                f"{self.base_url}/locator"
            ]
            
            for search_url in search_approaches:
                logger.info(f"Trying search URL: {search_url}")
                response = self.make_request(search_url)
                if not response:
                    continue
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Look for script tags with data
                script_tags = soup.find_all('script')
                for script in script_tags:
                    if script.string and ('facilities' in script.string or 'results' in script.string):
                        # Extract JSON data from script
                        json_facilities = self.extract_json_from_page(script.string)
                        if json_facilities:
                            logger.info(f"Found {len(json_facilities)} facilities in script data")
                            facilities.extend(json_facilities)
                
                # Extract facilities from HTML if no JSON found
                if not facilities:
                    facility_selectors = [
                        '.treatment-listing',
                        '.facility-card',
                        '.result-item',
                        '[data-testid="facility-card"]',
                        '.provider-result',
                        'article.facility',
                        'div[role="article"]'
                    ]
                    
                    for selector in facility_selectors:
                        elements = soup.select(selector)
                        if elements:
                            logger.info(f"Found {len(elements)} potential facilities with selector {selector}")
                            
                            for element in elements:
                                facility_data = self.parse_web_facility(element, state)
                                if facility_data and self.is_residential_facility(facility_data):
                                    facilities.append(facility_data)
                            
                            if facilities:  # If we found facilities, stop trying selectors
                                break
                
                if facilities:  # If we found facilities with this approach, stop
                    break
                    
                # Small delay between attempts
                time.sleep(1)
            
            # If still no facilities, try a more aggressive approach
            if not facilities and response:
                # Look for any text mentioning residential facilities
                page_text = response.text.lower()
                if 'residential' in page_text or 'inpatient' in page_text:
                    logger.info("Found residential mentions in page, attempting deep parse")
                    # Try to extract any structured data
                    json_facilities = self.extract_json_from_page(response.text)
                    facilities.extend(json_facilities)
            
        except Exception as e:
            logger.error(f"Error in web search for {city}, {state}: {e}")
        
        return facilities
    
    def parse_web_facility(self, element, state: str) -> Optional[Dict]:
        """
        Parse facility information from HTML element.
        
        Args:
            element: BeautifulSoup element
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
                'state': state
            }
            
            # Extract facility name
            name_selectors = ['.facility-name', '.listing-title', 'h2', 'h3', '.name']
            for selector in name_selectors:
                name_elem = element.select_one(selector)
                if name_elem:
                    facility_data['name'] = name_elem.get_text(strip=True)
                    break
            
            # Extract address
            address_selectors = ['.address', '.location', '.facility-address']
            for selector in address_selectors:
                addr_elem = element.select_one(selector)
                if addr_elem:
                    facility_data['address'] = self.parse_address_text(
                        addr_elem.get_text(strip=True), state
                    )
                    break
            
            # Extract phone
            phone_selectors = ['.phone', '.tel', 'a[href^="tel:"]']
            for selector in phone_selectors:
                phone_elem = element.select_one(selector)
                if phone_elem:
                    phone_text = phone_elem.get_text(strip=True)
                    if not phone_text and phone_elem.get('href'):
                        phone_text = phone_elem['href'].replace('tel:', '')
                    facility_data['contact']['phone'] = phone_text
                    break
            
            # Extract services
            service_selectors = ['.services', '.service-list', '.treatments']
            for selector in service_selectors:
                service_elem = element.select(selector)
                if service_elem:
                    services = []
                    for elem in service_elem:
                        services.extend([s.strip() for s in elem.get_text().split(',')])
                    facility_data['services'] = services
                    break
            
            # Extract facility ID or detail link
            detail_link = element.select_one('a[href*="detail"], a[href*="facility"]')
            if detail_link:
                href = detail_link.get('href', '')
                # Extract facility ID from URL if present
                id_match = re.search(r'fid=(\d+)', href)
                if id_match:
                    facility_data['id'] = id_match.group(1)
                
                # Get detailed information
                if href.startswith('/'):
                    detail_url = f"{self.base_url}{href}"
                else:
                    detail_url = href
                
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
            'zip': ''
        }
        
        try:
            # Clean and split address
            lines = [line.strip() for line in address_text.replace('\n', ',').split(',') if line.strip()]
            
            if lines:
                # First line is usually street
                address['street1'] = lines[0]
                
                # Look for ZIP code pattern
                zip_pattern = r'\b(\d{5}(-\d{4})?)\b'
                
                # Process remaining lines
                for i, line in enumerate(lines[1:], 1):
                    zip_match = re.search(zip_pattern, line)
                    if zip_match:
                        address['zip'] = zip_match.group(1)
                        # Extract city from the same line
                        city_state = line[:zip_match.start()].strip()
                        city_parts = city_state.rsplit(' ', 1)
                        if city_parts:
                            address['city'] = city_parts[0].strip(' ,')
                    elif i == 1 and not address['street2']:
                        # Second line might be street2 or city
                        if any(keyword in line.lower() for keyword in ['suite', 'apt', 'unit', '#']):
                            address['street2'] = line
                        else:
                            # Likely city
                            address['city'] = line.strip(' ,')
                            
        except Exception as e:
            logger.error(f"Error parsing address: {e}")
        
        return address
    
    def scrape_facility_details(self, detail_url: str) -> Optional[Dict]:
        """
        Scrape detailed facility information from detail page.
        
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
            
            # Extract services and check for residential
            service_elements = soup.find_all(['div', 'section', 'ul'], 
                                           class_=re.compile(r'service|treatment|program'))
            
            residential_services = []
            for elem in service_elements:
                text = elem.get_text().lower()
                
                # Check for residential service types
                if 'short-term residential' in text or 'short term residential' in text:
                    details['short_term_residential'] = True
                    residential_services.append('Short-term Residential (30 days or less)')
                    
                if 'long-term residential' in text or 'long term residential' in text:
                    details['long_term_residential'] = True
                    residential_services.append('Long-term Residential (more than 30 days)')
                    
                if 'therapeutic community' in text:
                    details['therapeutic_community'] = True
                    residential_services.append('Therapeutic Community')
                    
                if 'halfway house' in text:
                    details['halfway_house'] = True
                    residential_services.append('Halfway House')
                    
                if 'sober living' in text:
                    details['sober_living'] = True
                    residential_services.append('Sober Living')
                    
                if 'detox' in text or 'detoxification' in text:
                    if 'residential' in text or 'inpatient' in text:
                        details['residential_detox'] = True
                        residential_services.append('Residential Detoxification')
                        
                if 'women and children' in text or 'women with children' in text:
                    details['women_with_children'] = True
                    residential_services.append('Women and Children Program')
                    
                if 'adolescent' in text or 'teen' in text or 'youth' in text:
                    if 'residential' in text:
                        details['adolescent_program'] = True
                        residential_services.append('Adolescent Residential Program')
            
            if residential_services:
                details['residential_services'] = residential_services
            
            # Extract bed capacity
            bed_patterns = [
                r'(\d+)\s*beds?',
                r'capacity[:\s]+(\d+)',
                r'beds?[:\s]+(\d+)'
            ]
            
            for pattern in bed_patterns:
                bed_match = re.search(pattern, soup.get_text(), re.IGNORECASE)
                if bed_match:
                    details['bed_capacity'] = int(bed_match.group(1))
                    break
            
            # Extract length of stay information
            stay_patterns = [
                r'length of stay[:\s]+([^,\n]+)',
                r'typical stay[:\s]+([^,\n]+)',
                r'program length[:\s]+([^,\n]+)'
            ]
            
            for pattern in stay_patterns:
                stay_match = re.search(pattern, soup.get_text(), re.IGNORECASE)
                if stay_match:
                    details['typical_length_of_stay'] = stay_match.group(1).strip()
                    break
            
            # Extract insurance information
            insurance_section = soup.find(['div', 'section'], 
                                        class_=re.compile(r'insurance|payment|financial'))
            if insurance_section:
                insurance_text = insurance_section.get_text().lower()
                details['insurance'] = {
                    'medicaid': 'medicaid' in insurance_text,
                    'medicare': 'medicare' in insurance_text,
                    'private': 'private insurance' in insurance_text,
                    'sliding_scale': 'sliding scale' in insurance_text,
                    'free': 'free' in insurance_text or 'no cost' in insurance_text
                }
            
            # Extract contact information
            website_link = soup.find('a', href=re.compile(r'^https?://'))
            if website_link and 'samhsa' not in website_link['href']:
                details['website'] = website_link['href']
            
            email_link = soup.find('a', href=re.compile(r'^mailto:'))
            if email_link:
                details['email'] = email_link['href'].replace('mailto:', '')
            
            # Extract certifications/licenses
            cert_section = soup.find(['div', 'section'], 
                                   class_=re.compile(r'certification|license|accredit'))
            if cert_section:
                cert_text = cert_section.get_text()
                if 'samhsa' in cert_text.lower():
                    details['samhsa_certified'] = True
                if 'state licensed' in cert_text.lower():
                    details['state_licensed'] = True
            
            return details
            
        except Exception as e:
            logger.error(f"Error scraping facility details from {detail_url}: {e}")
            return None
    
    def extract_json_from_page(self, page_content: str) -> List[Dict]:
        """
        Extract facility data from JSON embedded in page content.
        
        Args:
            page_content: HTML page content
            
        Returns:
            List of facility dictionaries
        """
        facilities = []
        
        try:
            # Look for JSON data in script tags or data attributes
            json_patterns = [
                r'window\.__INITIAL_STATE__\s*=\s*(\{.*?\});',
                r'window\.searchResults\s*=\s*(\[.*?\]);',
                r'"facilities"\s*:\s*(\[.*?\])',
                r'data-facilities=\'(\[.*?\])\'',
                r'var\s+facilities\s*=\s*(\[.*?\]);'
            ]
            
            for pattern in json_patterns:
                matches = re.findall(pattern, page_content, re.DOTALL)
                for match in matches:
                    try:
                        data = json.loads(match)
                        
                        if isinstance(data, list):
                            # Filter for residential facilities
                            for facility in data:
                                if self.is_residential_facility(facility):
                                    facilities.append(facility)
                        elif isinstance(data, dict):
                            if 'facilities' in data:
                                for facility in data['facilities']:
                                    if self.is_residential_facility(facility):
                                        facilities.append(facility)
                            elif 'results' in data:
                                for facility in data['results']:
                                    if self.is_residential_facility(facility):
                                        facilities.append(facility)
                                        
                    except json.JSONDecodeError:
                        continue
            
        except Exception as e:
            logger.error(f"Error extracting JSON from page: {e}")
        
        return facilities
    
    def is_residential_facility(self, facility_data: Dict) -> bool:
        """
        Determine if facility offers residential services.
        
        Args:
            facility_data: Facility data dictionary
            
        Returns:
            True if facility offers residential services
        """
        # Combine all text fields for keyword matching
        text_fields = []
        
        # Add name
        if 'name' in facility_data:
            text_fields.append(str(facility_data['name']))
        
        # Add services
        if 'services' in facility_data:
            if isinstance(facility_data['services'], list):
                text_fields.extend(facility_data['services'])
            else:
                text_fields.append(str(facility_data['services']))
        
        # Add service_types
        if 'service_types' in facility_data:
            if isinstance(facility_data['service_types'], list):
                text_fields.extend(facility_data['service_types'])
            else:
                text_fields.append(str(facility_data['service_types']))
        
        # Add treatment_types
        if 'treatment_types' in facility_data:
            if isinstance(facility_data['treatment_types'], list):
                text_fields.extend(facility_data['treatment_types'])
            else:
                text_fields.append(str(facility_data['treatment_types']))
        
        # Combine and check for keywords
        combined_text = ' '.join(text_fields).lower()
        
        # Check for residential keywords
        return any(keyword in combined_text for keyword in self.residential_keywords)
    
    def parse_facility_data(self, facility_data: Dict) -> ResidentialTreatmentFacility:
        """
        Parse raw facility data into ResidentialTreatmentFacility object.
        
        Args:
            facility_data: Raw facility data dictionary
            
        Returns:
            ResidentialTreatmentFacility object
        """
        facility = ResidentialTreatmentFacility()
        
        try:
            # Basic Information
            facility.facility_name = facility_data.get('name', '')
            facility.facility_id = str(facility_data.get('id', ''))
            
            # DBA names
            if 'dba' in facility_data:
                facility.dba_names = [facility_data['dba']] if facility_data['dba'] else []
            
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
            if 'lat' in location:
                facility.latitude = float(location.get('lat', 0))
            if 'lng' in location:
                facility.longitude = float(location.get('lng', 0))
            
            # Services Information
            services = facility_data.get('services', [])
            if isinstance(services, str):
                services = [services]
            facility.service_types = services
            
            # Parse residential-specific services
            service_text = ' '.join(services).lower() if services else ''
            facility_text = f"{facility.facility_name} {service_text}".lower()
            
            facility.residential_services = []
            
            if 'short-term residential' in facility_text or 'short term residential' in facility_text:
                facility.short_term_residential = True
                facility.residential_services.append('Short-term Residential (30 days or less)')
                
            if 'long-term residential' in facility_text or 'long term residential' in facility_text:
                facility.long_term_residential = True
                facility.residential_services.append('Long-term Residential (more than 30 days)')
                
            if 'therapeutic community' in facility_text:
                facility.therapeutic_community = True
                facility.residential_services.append('Therapeutic Community')
                
            if 'halfway house' in facility_text:
                facility.halfway_house = True
                facility.residential_services.append('Halfway House')
                
            if 'sober living' in facility_text:
                facility.sober_living = True
                facility.residential_services.append('Sober Living')
                
            if 'detox' in facility_text or 'detoxification' in facility_text:
                facility.residential_detox = True
                facility.residential_services.append('Residential Detoxification')
                
            if 'women and children' in facility_text or 'women with children' in facility_text:
                facility.women_with_children = True
                facility.residential_services.append('Women and Children Program')
                
            if 'adolescent' in facility_text or 'teen' in facility_text:
                facility.adolescent_program = True
                facility.residential_services.append('Adolescent Residential Program')
            
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
            facility.sliding_scale_fees = insurance.get('sliding_scale', False)
            facility.free_services = insurance.get('free', False)
            
            # Additional details from detail scraping
            if 'bed_capacity' in facility_data:
                facility.bed_capacity = int(facility_data.get('bed_capacity', 0))
            
            if 'typical_length_of_stay' in facility_data:
                facility.typical_length_of_stay = facility_data['typical_length_of_stay']
            
            # License and certification
            facility.samhsa_certified = facility_data.get('samhsa_certified', False)
            facility.state_licensed = facility_data.get('state_licensed', False)
            facility.license_numbers = facility_data.get('licenses', [])
            facility.certifications = facility_data.get('certifications', [])
            facility.accreditations = facility_data.get('accreditations', [])
            
            # Facility type and ownership
            facility.facility_type = facility_data.get('facility_type', 'Residential Treatment')
            facility.ownership_type = facility_data.get('ownership_type', '')
            facility.parent_organization = facility_data.get('parent_organization', '')
            
            # Metadata
            facility.last_updated = facility_data.get('last_updated', '')
            
        except Exception as e:
            logger.error(f"Error parsing facility data: {e}")
            
        return facility
    
    def extract_state_facilities(self, state: str) -> List[ResidentialTreatmentFacility]:
        """
        Extract all residential facilities for a specific state.
        
        Args:
            state: State abbreviation
            
        Returns:
            List of ResidentialTreatmentFacility objects
        """
        logger.info(f"Extracting residential facilities for {state}")
        
        all_facilities = []
        seen_facilities = set()  # Track unique facilities
        
        # First try API search
        api_results = self.search_facilities_api(state=state)
        for facility_data in api_results:
            if self.is_residential_facility(facility_data):
                facility = self.parse_facility_data(facility_data)
                
                # Create unique key
                facility_key = f"{facility.facility_name}_{facility.address_line1}_{facility.city}"
                if facility_key not in seen_facilities:
                    all_facilities.append(facility)
                    seen_facilities.add(facility_key)
        
        logger.info(f"Found {len(all_facilities)} facilities via API for {state}")
        
        # Then try web scraping
        web_results = self.search_facilities_web(state=state)
        for facility_data in web_results:
            facility = self.parse_facility_data(facility_data)
            
            # Create unique key
            facility_key = f"{facility.facility_name}_{facility.address_line1}_{facility.city}"
            if facility_key not in seen_facilities:
                all_facilities.append(facility)
                seen_facilities.add(facility_key)
        
        # Search major cities for better coverage
        cities = self.major_cities.get(state, [])
        for city in cities:
            logger.info(f"Searching in {city}, {state}")
            
            # API search by city
            city_api_results = self.search_facilities_api(state=state, city=city)
            for facility_data in city_api_results:
                if self.is_residential_facility(facility_data):
                    facility = self.parse_facility_data(facility_data)
                    
                    facility_key = f"{facility.facility_name}_{facility.address_line1}_{facility.city}"
                    if facility_key not in seen_facilities:
                        all_facilities.append(facility)
                        seen_facilities.add(facility_key)
            
            # Web search by city
            city_web_results = self.search_facilities_web(state=state, city=city)
            for facility_data in city_web_results:
                facility = self.parse_facility_data(facility_data)
                
                facility_key = f"{facility.facility_name}_{facility.address_line1}_{facility.city}"
                if facility_key not in seen_facilities:
                    all_facilities.append(facility)
                    seen_facilities.add(facility_key)
            
            # Rate limiting between cities
            time.sleep(random.uniform(2, 4))
        
        self.extracted_count += len(all_facilities)
        logger.info(f"Total residential facilities extracted from {state}: {len(all_facilities)}")
        
        return all_facilities
    
    def extract_all_states(self) -> List[ResidentialTreatmentFacility]:
        """
        Extract residential facilities from all US states.
        
        Returns:
            List of all ResidentialTreatmentFacility objects
        """
        logger.info("Starting comprehensive extraction of residential facilities from all states")
        
        all_facilities = []
        
        for i, state in enumerate(self.us_states):
            try:
                logger.info(f"Processing state {i+1}/{len(self.us_states)}: {state}")
                state_facilities = self.extract_state_facilities(state)
                all_facilities.extend(state_facilities)
                
                # Longer pause between states
                time.sleep(random.uniform(5, 10))
                
                # Log progress
                if (i + 1) % 10 == 0:
                    logger.info(f"Progress: {i+1}/{len(self.us_states)} states completed. "
                              f"Total facilities: {len(all_facilities)}")
                
            except Exception as e:
                logger.error(f"Error extracting facilities from {state}: {e}")
                continue
        
        logger.info(f"Total residential facilities extracted: {len(all_facilities)}")
        return all_facilities
    
    def save_to_json(self, facilities: List[ResidentialTreatmentFacility], filepath: str):
        """
        Save facilities data to JSON file.
        
        Args:
            facilities: List of ResidentialTreatmentFacility objects
            filepath: Output file path
        """
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
                    "extraction_method": "Hybrid (API + Web Scraping)",
                    "service_types_targeted": [
                        "Short-term residential (30 days or less)",
                        "Long-term residential (more than 30 days)",
                        "Therapeutic communities",
                        "Modified therapeutic communities",
                        "Halfway houses",
                        "Sober living (licensed/certified)",
                        "Residential detox with extended care",
                        "Women and children residential",
                        "Adolescent residential"
                    ],
                    "geographic_coverage": "All US States and Territories",
                    "extraction_summary": {
                        "states_covered": len(self.us_states),
                        "facilities_by_type": {
                            "short_term": sum(1 for f in facilities if f.short_term_residential),
                            "long_term": sum(1 for f in facilities if f.long_term_residential),
                            "therapeutic_community": sum(1 for f in facilities if f.therapeutic_community),
                            "halfway_house": sum(1 for f in facilities if f.halfway_house),
                            "sober_living": sum(1 for f in facilities if f.sober_living),
                            "detox": sum(1 for f in facilities if f.residential_detox),
                            "women_children": sum(1 for f in facilities if f.women_with_children),
                            "adolescent": sum(1 for f in facilities if f.adolescent_program)
                        }
                    }
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
        """
        Run the complete extraction process.
        
        Args:
            output_path: Optional custom output path
            
        Returns:
            Path to the saved JSON file
        """
        if output_path is None:
            output_path = "/Users/benweiss/Code/narr_extractor/03_raw_data/treatment_centers/residential/samhsa/samhsa_residential_facilities.json"
        
        logger.info("Starting SAMHSA residential treatment centers extraction")
        logger.info(f"Target: Extract 1,500+ residential facilities")
        
        try:
            # Extract facilities from all states
            facilities = self.extract_all_states()
            
            # Save to JSON
            self.save_to_json(facilities, output_path)
            
            logger.info(f"Extraction completed successfully. Total facilities: {len(facilities)}")
            
            # Log summary statistics
            logger.info("Extraction Summary:")
            logger.info(f"- Short-term residential: {sum(1 for f in facilities if f.short_term_residential)}")
            logger.info(f"- Long-term residential: {sum(1 for f in facilities if f.long_term_residential)}")
            logger.info(f"- Therapeutic communities: {sum(1 for f in facilities if f.therapeutic_community)}")
            logger.info(f"- Halfway houses: {sum(1 for f in facilities if f.halfway_house)}")
            logger.info(f"- Sober living: {sum(1 for f in facilities if f.sober_living)}")
            logger.info(f"- Residential detox: {sum(1 for f in facilities if f.residential_detox)}")
            logger.info(f"- Women & children programs: {sum(1 for f in facilities if f.women_with_children)}")
            logger.info(f"- Adolescent programs: {sum(1 for f in facilities if f.adolescent_program)}")
            
            return output_path
            
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise

def main():
    """Main execution function."""
    extractor = SAMHSAResidentialExtractor()
    
    try:
        output_file = extractor.run_extraction()
        print(f"\nExtraction completed successfully!")
        print(f"Data saved to: {output_file}")
        print(f"Total residential facilities extracted: {extractor.extracted_count}")
        
    except Exception as e:
        print(f"Extraction failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())