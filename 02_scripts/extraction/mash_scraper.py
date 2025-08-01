#!/usr/bin/env python3
"""
Massachusetts Alliance for Sober Housing (MASH) Certified Residences Scraper

This script extracts all certified recovery residences from the MASH website
by directly calling their API endpoint and saving the data to a JSON file.

Author: Generated for comprehensive directory extraction
Date: 2025-07-30
"""

import requests
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mash_scraper.log'),
        logging.StreamHandler()
    ]
)

class MASHScraper:
    """Scraper for MASH certified recovery residences"""
    
    def __init__(self):
        self.api_url = "https://management.mashsoberhousing.org/api/GetCertifiedHomes"
        self.base_url = "https://mashsoberhousing.org"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://mashsoberhousing.org/certified-residences/',
            'X-Requested-With': 'XMLHttpRequest'
        })
        
    def extract_all_residences(self) -> List[Dict[str, Any]]:
        """
        Extract all certified residences from the MASH API
        
        Returns:
            List of residence dictionaries with complete information
        """
        logging.info("Starting extraction of MASH certified residences...")
        
        try:
            # Make API request
            response = self.session.get(self.api_url, timeout=30)
            response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            logging.info(f"Successfully retrieved {len(data)} residences from API")
            
            # Process and clean the data
            cleaned_residences = []
            for residence in data:
                cleaned_residence = self.clean_residence_data(residence)
                if self.validate_residence_data(cleaned_residence):
                    cleaned_residences.append(cleaned_residence)
                else:
                    logging.warning(f"Invalid residence data: {residence.get('Name', 'Unknown')}")
            
            logging.info(f"Successfully processed {len(cleaned_residences)} valid residences")
            return cleaned_residences
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Network error occurred: {e}")
            raise
        except json.JSONDecodeError as e:
            logging.error(f"JSON parsing error: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            raise
    
    def clean_residence_data(self, residence: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean and standardize residence data
        
        Args:
            residence: Raw residence data from API
            
        Returns:
            Cleaned residence data dictionary
        """
        cleaned = {
            'id': residence.get('Id'),
            'name': self.clean_text(residence.get('Name')),
            'service_type': self.clean_text(residence.get('ServiceType')),
            'languages': self.clean_text(residence.get('Languages')),
            'address': {
                'primary': self.clean_text(residence.get('PrimaryAddress')),
                'secondary': self.clean_text(residence.get('SecondaryAddress')),
                'full_address': self.build_full_address(residence)
            },
            'region': self.clean_text(residence.get('Region')),
            'town': self.clean_text(residence.get('Town')),
            'zip_code': self.clean_text(residence.get('ZipCode')),
            'capacity': self.extract_capacity(residence.get('Capacity')),
            'contact': {
                'phone': self.clean_phone(residence.get('Phone')),
                'email': self.clean_email(residence.get('Email')),
                'website': self.clean_url(residence.get('Website'))
            },
            'accessibility': {
                'handicap_accessible': residence.get('HandicapAccessible', False)
            },
            'additional_info': self.clean_text(residence.get('AdditionalInfo')),
            'certification_status': 'Certified',  # All from this API are certified
            'data_source': 'MASH API',
            'extraction_date': datetime.now().isoformat(),
            'raw_data': residence  # Keep original for reference
        }
        
        return cleaned
    
    def clean_text(self, text: Optional[str]) -> Optional[str]:
        """Clean and normalize text fields"""
        if not text or not isinstance(text, str):
            return None
        return text.strip() if text.strip() else None
    
    def clean_phone(self, phone: Optional[str]) -> Optional[str]:
        """Clean and format phone numbers"""
        if not phone:
            return None
        
        # Remove all non-digit characters
        digits = re.sub(r'\D', '', phone)
        
        # Format if it's a valid US phone number
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        else:
            return phone.strip()  # Return original if can't format
    
    def clean_email(self, email: Optional[str]) -> Optional[str]:
        """Clean and validate email addresses"""
        if not email:
            return None
        
        email = email.strip().lower()
        # Basic email validation
        if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
            return email
        return None
    
    def clean_url(self, url: Optional[str]) -> Optional[str]:
        """Clean and format URLs"""
        if not url:
            return None
        
        url = url.strip()
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return url
    
    def build_full_address(self, residence: Dict[str, Any]) -> str:
        """Build a complete address string"""
        parts = []
        
        if residence.get('PrimaryAddress'):
            parts.append(residence['PrimaryAddress'].strip())
        if residence.get('SecondaryAddress'):
            parts.append(residence['SecondaryAddress'].strip())
        if residence.get('Town'):
            parts.append(residence['Town'].strip())
        if residence.get('ZipCode'):
            parts.append(residence['ZipCode'].strip())
        
        return ', '.join(parts) if parts else ''
    
    def extract_capacity(self, capacity: Any) -> Optional[int]:
        """Extract numeric capacity from various formats"""
        if isinstance(capacity, int):
            return capacity
        if isinstance(capacity, str):
            # Extract first number from string
            match = re.search(r'\d+', capacity)
            if match:
                return int(match.group())
        return None
    
    def validate_residence_data(self, residence: Dict[str, Any]) -> bool:
        """
        Validate that residence has minimum required data
        
        Args:
            residence: Cleaned residence data
            
        Returns:
            True if valid, False otherwise
        """
        # Must have at least a name
        if not residence.get('name'):
            return False
        
        # Should have some contact information or address
        has_contact = any([
            residence.get('contact', {}).get('phone'),
            residence.get('contact', {}).get('email'),
            residence.get('address', {}).get('primary')
        ])
        
        return has_contact
    
    def save_to_json(self, residences: List[Dict[str, Any]], filename: str = None) -> str:
        """
        Save residences data to JSON file
        
        Args:
            residences: List of residence dictionaries
            filename: Optional custom filename
            
        Returns:
            Path to saved file
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"mash_certified_residences_{timestamp}.json"
        
        # Create metadata
        metadata = {
            'extraction_date': datetime.now().isoformat(),
            'total_residences': len(residences),
            'data_source': 'MASH API (https://management.mashsoberhousing.org/api/GetCertifiedHomes)',
            'website': 'https://mashsoberhousing.org/certified-residences/',
            'extractor_version': '1.0.0'
        }
        
        # Prepare final data structure
        final_data = {
            'metadata': metadata,
            'residences': residences
        }
        
        # Save to file
        file_path = Path(filename)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Data saved to {file_path}")
        return str(file_path)
    
    def generate_summary_report(self, residences: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate a summary report of the extracted data"""
        if not residences:
            return {'error': 'No residences data provided'}
        
        # Basic statistics
        total_count = len(residences)
        
        # Service types
        service_types = {}
        regions = {}
        towns = {}
        languages = {}
        
        valid_phones = 0
        valid_emails = 0
        valid_websites = 0
        total_capacity = 0
        
        for residence in residences:
            # Service types
            service_type = residence.get('service_type')
            if service_type:
                service_types[service_type] = service_types.get(service_type, 0) + 1
            
            # Regions
            region = residence.get('region')
            if region:
                regions[region] = regions.get(region, 0) + 1
            
            # Towns
            town = residence.get('town')
            if town:
                towns[town] = towns.get(town, 0) + 1
            
            # Languages
            lang = residence.get('languages')
            if lang:
                languages[lang] = languages.get(lang, 0) + 1
            
            # Contact info
            if residence.get('contact', {}).get('phone'):
                valid_phones += 1
            if residence.get('contact', {}).get('email'):
                valid_emails += 1
            if residence.get('contact', {}).get('website'):
                valid_websites += 1
            
            # Capacity
            capacity = residence.get('capacity')
            if isinstance(capacity, int):
                total_capacity += capacity
        
        return {
            'total_residences': total_count,
            'contact_info_availability': {
                'phone_numbers': valid_phones,
                'email_addresses': valid_emails,
                'websites': valid_websites
            },
            'total_capacity': total_capacity,
            'average_capacity': round(total_capacity / total_count, 1) if total_count > 0 else 0,
            'service_types': dict(sorted(service_types.items(), key=lambda x: x[1], reverse=True)),
            'regions': dict(sorted(regions.items(), key=lambda x: x[1], reverse=True)),
            'top_towns': dict(sorted(towns.items(), key=lambda x: x[1], reverse=True)[:10]),
            'languages': dict(sorted(languages.items(), key=lambda x: x[1], reverse=True))
        }

def main():
    """Main execution function"""
    try:
        # Initialize scraper
        scraper = MASHScraper()
        
        # Extract all residences
        residences = scraper.extract_all_residences()
        
        # Generate summary
        summary = scraper.generate_summary_report(residences)
        print("\n" + "="*50)
        print("EXTRACTION SUMMARY")
        print("="*50)
        print(f"Total residences extracted: {summary['total_residences']}")
        print(f"Phone numbers available: {summary['contact_info_availability']['phone_numbers']}")
        print(f"Email addresses available: {summary['contact_info_availability']['email_addresses']}")
        print(f"Websites available: {summary['contact_info_availability']['websites']}")
        print(f"Total capacity: {summary['total_capacity']} beds")
        print(f"Average capacity: {summary['average_capacity']} beds per residence")
        
        print(f"\nTop service types:")
        for service_type, count in list(summary['service_types'].items())[:5]:
            print(f"  {service_type}: {count}")
        
        print(f"\nTop regions:")
        for region, count in list(summary['regions'].items())[:5]:
            print(f"  {region}: {count}")
        
        # Save to JSON
        json_file = scraper.save_to_json(residences)
        
        # Save summary report
        summary_file = json_file.replace('.json', '_summary.json')
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        print(f"\nFiles created:")
        print(f"  Main data: {json_file}")
        print(f"  Summary: {summary_file}")
        print(f"  Log file: mash_scraper.log")
        
        return json_file
        
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        raise

if __name__ == "__main__":
    main()