#!/usr/bin/env python3
"""
NPI (National Provider Identifier) Database Extractor
Downloads and processes CMS NPI registry data
"""

import requests
import zipfile
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set
import logging

class NPIExtractor:
    """Extract and process NPI registry data from CMS"""
    
    # Relevant taxonomy codes for addiction/behavioral health
    RELEVANT_TAXONOMIES = {
        # Addiction Medicine
        "207RA0401X": "Addiction Medicine (Anesthesiology)",
        "207QA0401X": "Addiction Medicine (Family Medicine)", 
        "207RA0401X": "Addiction Medicine (Internal Medicine)",
        "2084A0401X": "Addiction Medicine (Psychiatry)",
        
        # Counselors
        "101Y00000X": "Counselor",
        "101YA0400X": "Addiction (Substance Use Disorder) Counselor",
        "101YM0800X": "Mental Health Counselor",
        
        # Facilities
        "261Q00000X": "Clinic/Center",
        "261QM0801X": "Mental Health Clinic/Center (Including Community MH Center)",
        "261QM0850X": "Adult Mental Health Clinic/Center",
        "261QM0855X": "Adolescent and Children Mental Health Clinic/Center",
        "261QR0405X": "Substance Abuse Rehabilitation Facility",
        
        # Substance Abuse Facilities
        "324500000X": "Substance Abuse Rehabilitation Facility", 
        "3245S0500X": "Substance Abuse Treatment Children",
        
        # Residential Treatment
        "320800000X": "Community Based Residential Treatment Facility, Mental Illness",
        "320900000X": "Community Based Residential Treatment Facility, Mental Retardation and/or Developmental Disabilities",
        
        # Psychiatric Units/Hospitals
        "273R00000X": "Psychiatric Hospital",
        "283Q00000X": "Psychiatric Unit",
        
        # Halfway Houses
        "311500000X": "Alzheimer Center (Dementia Center)",
        "315D00000X": "Hospice, Inpatient",
        "315P00000X": "Intermediate Care Facility, Mental Illness",
        
        # Other relevant
        "323P00000X": "Psychiatric Residential Treatment Facility",
        "104100000X": "Social Worker",
        "1041C0700X": "Clinical Social Worker",
        "363L00000X": "Nurse Practitioner",
        "363LP0808X": "Psychiatric/Mental Health Nurse Practitioner",
    }
    
    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = base_path or Path(__file__).parent.parent.parent.parent
        self.data_path = self.base_path / "03_raw_data" / "npi"
        self.data_path.mkdir(parents=True, exist_ok=True)
        
        self.logger = self._setup_logger()
        
        # NPI download URL (monthly full replacement file)
        self.npi_url = "https://download.cms.gov/nppes/NPPES_Data_Dissemination_Month_Year.zip"
        
    def _setup_logger(self) -> logging.Logger:
        """Set up logging"""
        logger = logging.getLogger("npi_extractor")
        logger.setLevel(logging.INFO)
        
        # File handler
        log_file = self.data_path / f"npi_extraction_{datetime.now().strftime('%Y%m%d')}.log"
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.INFO)
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        return logger
    
    def get_current_npi_url(self) -> str:
        """Get the current month's NPI file URL"""
        # CMS updates the file monthly with a specific naming convention
        # We need to check their page or use the dissemination files list
        
        # For now, return the main download page
        return "https://download.cms.gov/nppes/NPI_Files.html"
    
    def download_npi_file(self, url: str) -> Path:
        """Download NPI zip file"""
        self.logger.info(f"Downloading NPI file from {url}")
        
        filename = f"npi_data_{datetime.now().strftime('%Y%m%d')}.zip"
        filepath = self.data_path / filename
        
        # Download with progress
        response = requests.get(url, stream=True)
        total_size = int(response.headers.get('content-length', 0))
        
        with open(filepath, 'wb') as f:
            downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    percent = (downloaded / total_size) * 100
                    self.logger.info(f"Download progress: {percent:.1f}%")
        
        self.logger.info(f"Downloaded to {filepath}")
        return filepath
    
    def extract_relevant_providers(self, npi_file: Path) -> List[Dict]:
        """Extract only providers with relevant taxonomy codes"""
        relevant_providers = []
        
        self.logger.info("Extracting NPI zip file...")
        
        with zipfile.ZipFile(npi_file, 'r') as zip_ref:
            # Find the main CSV file
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv') and 'npidata' in f.lower()]
            
            if not csv_files:
                raise ValueError("No NPI data CSV found in zip file")
                
            csv_filename = csv_files[0]
            self.logger.info(f"Processing {csv_filename}")
            
            # Extract and process CSV
            with zip_ref.open(csv_filename) as csvfile:
                # Read as text
                text_data = csvfile.read().decode('utf-8')
                reader = csv.DictReader(text_data.splitlines())
                
                row_count = 0
                relevant_count = 0
                
                for row in reader:
                    row_count += 1
                    if row_count % 100000 == 0:
                        self.logger.info(f"Processed {row_count:,} rows, found {relevant_count:,} relevant")
                    
                    # Check if provider has relevant taxonomy
                    if self._is_relevant_provider(row):
                        provider = self._extract_provider_info(row)
                        relevant_providers.append(provider)
                        relevant_count += 1
                
                self.logger.info(f"Total rows: {row_count:,}, Relevant providers: {relevant_count:,}")
        
        return relevant_providers
    
    def _is_relevant_provider(self, row: Dict) -> bool:
        """Check if provider has relevant taxonomy code"""
        # Check primary taxonomy
        primary_taxonomy = row.get('Healthcare Provider Primary Taxonomy Switch_1', '')
        if primary_taxonomy == 'Y':
            taxonomy_code = row.get('Healthcare Provider Taxonomy Code_1', '')
            if taxonomy_code in self.RELEVANT_TAXONOMIES:
                return True
        
        # Check all 15 possible taxonomy fields
        for i in range(1, 16):
            taxonomy_code = row.get(f'Healthcare Provider Taxonomy Code_{i}', '')
            if taxonomy_code in self.RELEVANT_TAXONOMIES:
                return True
                
        return False
    
    def _extract_provider_info(self, row: Dict) -> Dict:
        """Extract relevant information from NPI record"""
        # Determine if individual or organization
        entity_type = row.get('Entity Type Code', '')
        
        if entity_type == '1':  # Individual
            name = f"{row.get('Provider First Name', '')} {row.get('Provider Last Name (Legal Name)', '')}".strip()
            org_name = row.get('Provider Organization Name (Legal Business Name)', '')
        else:  # Organization
            name = row.get('Provider Organization Name (Legal Business Name)', '')
            org_name = name
        
        # Extract taxonomies
        taxonomies = []
        for i in range(1, 16):
            taxonomy_code = row.get(f'Healthcare Provider Taxonomy Code_{i}', '')
            if taxonomy_code in self.RELEVANT_TAXONOMIES:
                taxonomies.append({
                    'code': taxonomy_code,
                    'description': self.RELEVANT_TAXONOMIES[taxonomy_code],
                    'primary': row.get(f'Healthcare Provider Primary Taxonomy Switch_{i}', '') == 'Y'
                })
        
        # Build provider record
        provider = {
            'npi': row.get('NPI', ''),
            'entity_type': 'individual' if entity_type == '1' else 'organization',
            'name': name,
            'organization_name': org_name,
            
            # Practice location
            'address_street': f"{row.get('Provider First Line Business Practice Location Address', '')} {row.get('Provider Second Line Business Practice Location Address', '')}".strip(),
            'address_city': row.get('Provider Business Practice Location Address City Name', ''),
            'address_state': row.get('Provider Business Practice Location Address State Name', ''),
            'address_zip': row.get('Provider Business Practice Location Address Postal Code', ''),
            
            # Contact
            'phone': row.get('Provider Business Practice Location Address Telephone Number', ''),
            'fax': row.get('Provider Business Practice Location Address Fax Number', ''),
            
            # Taxonomies
            'taxonomies': taxonomies,
            
            # Status
            'enumeration_date': row.get('Provider Enumeration Date', ''),
            'last_update': row.get('Last Update Date', ''),
            'deactivation_date': row.get('NPI Deactivation Date', ''),
            'reactivation_date': row.get('NPI Reactivation Date', ''),
            'is_active': not bool(row.get('NPI Deactivation Date', ''))
        }
        
        return provider
    
    def save_providers(self, providers: List[Dict]) -> Path:
        """Save extracted providers to JSON"""
        output_file = self.data_path / f"npi_behavioral_health_providers_{datetime.now().strftime('%Y%m%d')}.json"
        
        output = {
            'extraction_date': datetime.now().isoformat(),
            'total_providers': len(providers),
            'providers': providers,
            'taxonomy_codes': self.RELEVANT_TAXONOMIES
        }
        
        with open(output_file, 'w') as f:
            json.dump(output, f, indent=2)
            
        self.logger.info(f"Saved {len(providers)} providers to {output_file}")
        return output_file
    
    def create_provider_lookup(self, providers: List[Dict]) -> Dict[str, Dict]:
        """Create NPI lookup dictionary for matching"""
        lookup = {}
        
        for provider in providers:
            npi = provider['npi']
            lookup[npi] = provider
            
        return lookup
    
    def match_facilities_to_npi(self, facilities: List[Dict], npi_lookup: Dict[str, Dict]) -> List[Dict]:
        """Match facilities to NPI records"""
        matched = 0
        
        for facility in facilities:
            # Try to match by NPI if available
            if facility.get('npi'):
                npi_record = npi_lookup.get(facility['npi'])
                if npi_record:
                    facility['npi_data'] = npi_record
                    facility['npi_matched'] = True
                    matched += 1
                    continue
            
            # TODO: Implement fuzzy matching by name/address
            facility['npi_matched'] = False
        
        self.logger.info(f"Matched {matched}/{len(facilities)} facilities to NPI records")
        return facilities
    
    def run(self):
        """Run the NPI extraction process"""
        self.logger.info("=== NPI Extraction Process ===")
        
        # Note: Actual implementation would need to:
        # 1. Check CMS website for current month's file
        # 2. Download the large ZIP file (5GB+)
        # 3. Process the CSV (12GB+ uncompressed)
        
        self.logger.info("NPI extraction requires:")
        self.logger.info("1. Visit https://download.cms.gov/nppes/NPI_Files.html")
        self.logger.info("2. Download the current month's Full Replacement Monthly NPI File")
        self.logger.info("3. Process the large CSV file (12GB+ uncompressed)")
        self.logger.info("4. Extract providers with behavioral health taxonomy codes")
        
        # For demonstration, show what would be extracted
        self.logger.info("\nRelevant taxonomy codes to extract:")
        for code, desc in sorted(self.RELEVANT_TAXONOMIES.items()):
            self.logger.info(f"  {code}: {desc}")
        
        return None


def main():
    """Run NPI extraction"""
    extractor = NPIExtractor()
    extractor.run()


if __name__ == "__main__":
    main()