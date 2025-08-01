#!/usr/bin/env python3
"""
SAMHSA Outpatient Treatment Centers Extractor

This script extracts comprehensive data about outpatient addiction treatment centers
from SAMHSA's Treatment Locator (findtreatment.gov) and related federal databases.

PRIMARY OBJECTIVE: Extract licensed outpatient treatment facilities that provide
non-residential addiction treatment services.

TARGET SERVICE TYPES:
- Standard Outpatient (OP)
- Intensive Outpatient (IOP) 
- Partial Hospitalization (PHP/Day Treatment)
- Medication-Assisted Treatment (MAT) clinics
- Opioid Treatment Programs (OTP/Methadone clinics)
- Office-based opioid treatment (OBOT)
- DUI/DWI programs
- Adolescent outpatient programs

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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('samhsa_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class TreatmentFacility:
    """Data structure for SAMHSA treatment facility information."""
    
    # Basic Information
    facility_name: str = ""
    dba_names: List[str] = None
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
    license_numbers: List[str] = None
    certifications: List[str] = None
    accreditations: List[str] = None
    
    # Service Information
    service_types: List[str] = None
    treatment_approaches: List[str] = None
    treatment_modalities: List[str] = None
    
    # Outpatient-Specific Services
    outpatient_services: List[str] = None
    intensive_outpatient: bool = False
    partial_hospitalization: bool = False
    mat_services: bool = False
    opioid_treatment_program: bool = False
    office_based_opioid_treatment: bool = False
    dui_dwi_programs: bool = False
    
    # Population and Demographics
    age_groups_accepted: List[str] = None
    special_populations: List[str] = None
    gender_accepted: List[str] = None
    
    # Language Services
    languages_spoken: List[str] = None
    
    # Payment and Insurance
    insurance_accepted: List[str] = None
    payment_options: List[str] = None
    medicaid_accepted: bool = False
    medicare_accepted: bool = False
    private_insurance_accepted: bool = False
    sliding_scale_fees: bool = False
    free_services: bool = False
    
    # Operational Information
    hours_of_operation: Dict[str, str] = None
    appointment_required: bool = False
    walk_ins_accepted: bool = False
    
    # Staff Information
    staff_credentials: List[str] = None
    medical_director: str = ""
    
    # Additional Metadata
    facility_type: str = ""
    ownership_type: str = ""
    parent_organization: str = ""
    last_updated: str = ""
    data_source: str = "SAMHSA"
    extraction_date: str = ""
    
    def __post_init__(self):
        """Initialize list fields if None."""
        if self.dba_names is None:
            self.dba_names = []
        if self.license_numbers is None:
            self.license_numbers = []
        if self.certifications is None:
            self.certifications = []
        if self.accreditations is None:
            self.accreditations = []
        if self.service_types is None:
            self.service_types = []
        if self.treatment_approaches is None:
            self.treatment_approaches = []
        if self.treatment_modalities is None:
            self.treatment_modalities = []
        if self.outpatient_services is None:
            self.outpatient_services = []
        if self.age_groups_accepted is None:
            self.age_groups_accepted = []
        if self.special_populations is None:
            self.special_populations = []
        if self.gender_accepted is None:
            self.gender_accepted = []
        if self.languages_spoken is None:
            self.languages_spoken = []
        if self.insurance_accepted is None:
            self.insurance_accepted = []
        if self.payment_options is None:
            self.payment_options = []
        if self.hours_of_operation is None:
            self.hours_of_operation = {}
        if self.staff_credentials is None:
            self.staff_credentials = []
        if self.extraction_date == "":
            self.extraction_date = datetime.now().isoformat()

class SAMHSAExtractor:
    """Main class for extracting SAMHSA treatment facility data."""
    
    def __init__(self):
        """Initialize the SAMHSA extractor."""
        self.base_url = "https://findtreatment.gov"
        self.api_base = f"{self.base_url}/api"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SAMHSA-Research-Bot/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
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
        
        # Outpatient service type identifiers
        self.outpatient_service_types = [
            'outpatient',
            'intensive_outpatient',
            'partial_hospitalization',
            'day_treatment',
            'medication_assisted_treatment',
            'opioid_treatment_program',
            'office_based_opioid_treatment',
            'dui_dwi_program',
            'outpatient_detoxification',
            'outpatient_counseling',
            'group_therapy',
            'individual_therapy',
            'family_therapy'
        ]
    
    def get_facilities_by_location(self, 
                                 state: str = None, 
                                 city: str = None, 
                                 zip_code: str = None,
                                 distance: int = 50) -> List[Dict]:
        """
        Extract facilities by geographic location.
        
        Args:
            state: State abbreviation (e.g., 'CA')
            city: City name
            zip_code: ZIP code
            distance: Search radius in miles
            
        Returns:
            List of facility dictionaries
        """
        params = {
            'distance': distance,
            'services': 'outpatient'  # Filter for outpatient services
        }
        
        if state:
            params['state'] = state
        if city:
            params['city'] = city
        if zip_code:
            params['zip'] = zip_code
            
        try:
            # Try multiple potential API endpoints
            endpoints = [
                f"{self.api_base}/facilities/search",
                f"{self.api_base}/treatment-locator/search",
                f"{self.api_base}/locator/search"
            ]
            
            facilities = []
            for endpoint in endpoints:
                try:
                    logger.info(f"Trying endpoint: {endpoint}")
                    response = self.session.get(endpoint, params=params, timeout=30)
                    
                    if response.status_code == 200:
                        data = response.json()
                        logger.info(f"Success with endpoint: {endpoint}")
                        
                        # Handle different response structures
                        if 'facilities' in data:
                            facilities = data['facilities']
                        elif 'results' in data:
                            facilities = data['results']
                        elif isinstance(data, list):
                            facilities = data
                        else:
                            facilities = [data] if data else []
                            
                        break
                        
                except requests.exceptions.RequestException as e:
                    logger.warning(f"Endpoint {endpoint} failed: {e}")
                    continue
                    
            return facilities
            
        except Exception as e:
            logger.error(f"Error searching facilities for {state}: {e}")
            return []
    
    def parse_facility_data(self, facility_data: Dict) -> TreatmentFacility:
        """
        Parse raw facility data into TreatmentFacility object.
        
        Args:
            facility_data: Raw facility data dictionary
            
        Returns:
            TreatmentFacility object
        """
        facility = TreatmentFacility()
        
        try:
            # Basic Information
            facility.facility_name = facility_data.get('name', '')
            facility.facility_id = facility_data.get('id', '')
            
            # Handle DBA names
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
            facility.latitude = float(location.get('lat', 0))
            facility.longitude = float(location.get('lng', 0))
            
            # Services Information
            services = facility_data.get('services', [])
            facility.service_types = services if isinstance(services, list) else []
            
            # Check for outpatient-specific services
            facility.outpatient_services = []
            service_str = ' '.join(facility.service_types).lower()
            
            if 'intensive outpatient' in service_str or 'iop' in service_str:
                facility.intensive_outpatient = True
                facility.outpatient_services.append('Intensive Outpatient Program (IOP)')
                
            if 'partial hospitalization' in service_str or 'php' in service_str:
                facility.partial_hospitalization = True
                facility.outpatient_services.append('Partial Hospitalization Program (PHP)')
                
            if 'medication assisted' in service_str or 'mat' in service_str:
                facility.mat_services = True
                facility.outpatient_services.append('Medication-Assisted Treatment (MAT)')
                
            if 'opioid treatment' in service_str or 'otp' in service_str:
                facility.opioid_treatment_program = True
                facility.outpatient_services.append('Opioid Treatment Program (OTP)')
                
            if 'dui' in service_str or 'dwi' in service_str:
                facility.dui_dwi_programs = True
                facility.outpatient_services.append('DUI/DWI Programs')
            
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
            
            # License and certification
            facility.license_numbers = facility_data.get('licenses', [])
            facility.certifications = facility_data.get('certifications', [])
            facility.accreditations = facility_data.get('accreditations', [])
            
            # Operational information
            facility.hours_of_operation = facility_data.get('hours', {})
            facility.appointment_required = facility_data.get('appointment_required', False)
            facility.walk_ins_accepted = facility_data.get('walk_ins_accepted', False)
            
            # Facility type and ownership
            facility.facility_type = facility_data.get('facility_type', '')
            facility.ownership_type = facility_data.get('ownership_type', '')
            facility.parent_organization = facility_data.get('parent_organization', '')
            
            # Metadata
            facility.last_updated = facility_data.get('last_updated', '')
            
        except Exception as e:
            logger.error(f"Error parsing facility data: {e}")
            
        return facility
    
    def extract_state_facilities(self, state: str) -> List[TreatmentFacility]:
        """
        Extract all outpatient facilities for a specific state.
        
        Args:
            state: State abbreviation
            
        Returns:
            List of TreatmentFacility objects
        """
        logger.info(f"Extracting outpatient facilities for {state}")
        
        facilities = []
        raw_facilities = self.get_facilities_by_location(state=state)
        
        for facility_data in raw_facilities:
            # Filter for outpatient facilities only
            services = facility_data.get('services', [])
            service_str = ' '.join(services).lower() if services else ''
            
            # Check if facility offers outpatient services
            is_outpatient = any(service in service_str for service in self.outpatient_service_types)
            
            if is_outpatient:
                facility = self.parse_facility_data(facility_data)
                facilities.append(facility)
                self.extracted_count += 1
                
                if self.extracted_count % 100 == 0:
                    logger.info(f"Extracted {self.extracted_count} outpatient facilities")
        
        logger.info(f"Extracted {len(facilities)} outpatient facilities from {state}")
        return facilities
    
    def extract_all_states(self) -> List[TreatmentFacility]:
        """
        Extract outpatient facilities from all US states.
        
        Returns:
            List of all TreatmentFacility objects
        """
        logger.info("Starting comprehensive extraction of outpatient facilities from all states")
        
        all_facilities = []
        
        for state in self.us_states:
            try:
                state_facilities = self.extract_state_facilities(state)
                all_facilities.extend(state_facilities)
                
                # Rate limiting - pause between states
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Error extracting facilities from {state}: {e}")
                continue
        
        logger.info(f"Total outpatient facilities extracted: {len(all_facilities)}")
        return all_facilities
    
    def save_to_json(self, facilities: List[TreatmentFacility], filepath: str):
        """
        Save facilities data to JSON file.
        
        Args:
            facilities: List of TreatmentFacility objects
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
                    "extraction_method": "API/Web Scraping",
                    "service_types_targeted": self.outpatient_service_types,
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
        """
        Run the complete extraction process.
        
        Args:
            output_path: Optional custom output path
            
        Returns:
            Path to the saved JSON file
        """
        if output_path is None:
            output_path = "/Users/benweiss/Code/narr_extractor/03_raw_data/treatment_centers/outpatient/samhsa/samhsa_outpatient_facilities.json"
        
        logger.info("Starting SAMHSA outpatient treatment centers extraction")
        
        try:
            # Extract facilities from all states
            facilities = self.extract_all_states()
            
            # Save to JSON
            self.save_to_json(facilities, output_path)
            
            logger.info(f"Extraction completed successfully. Total facilities: {len(facilities)}")
            return output_path
            
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise

def main():
    """Main execution function."""
    extractor = SAMHSAExtractor()
    
    try:
        output_file = extractor.run_extraction()
        print(f"\nExtraction completed successfully!")
        print(f"Data saved to: {output_file}")
        print(f"Total outpatient facilities extracted: {extractor.extracted_count}")
        
    except Exception as e:
        print(f"Extraction failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())