#!/usr/bin/env python3
"""
California DHCS (Department of Health Care Services) Licensed Facility Extractor
"""

import requests
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any
import time
from pathlib import Path

import sys
sys.path.append(str(Path(__file__).parent.parent))
from base_state_extractor import BaseStateExtractor

class CaliforniaDHCSExtractor(BaseStateExtractor):
    """Extract licensed SUD facilities from California DHCS"""
    
    def __init__(self):
        super().__init__("CA", "California")
        
    def get_agency_info(self) -> Dict[str, Any]:
        """Return California DHCS agency information"""
        return {
            "state_code": "CA",
            "state_name": "California",
            "agency_name": "Department of Health Care Services",
            "agency_abbreviation": "DHCS",
            "website": "https://www.dhcs.ca.gov",
            "data_portal_url": "https://www.dhcs.ca.gov/individuals/Pages/sud-licensed-certified.aspx",
            "licenses_types": [
                "Residential Treatment",
                "Outpatient Treatment", 
                "Narcotic Treatment Program",
                "Detox",
                "Recovery Residence"
            ],
            "data_format": "excel",
            "update_frequency": "monthly",
            "last_updated": datetime.now().isoformat()
        }
    
    def extract_data(self) -> List[Dict[str, Any]]:
        """Extract facility data from DHCS website"""
        facilities = []
        
        # Note: DHCS provides Excel files for download
        # The actual URLs would need to be discovered by visiting the portal
        
        # For now, showing the structure of what we'd extract
        excel_urls = [
            # These would be the actual Excel file URLs from DHCS
            # "https://www.dhcs.ca.gov/path/to/residential_facilities.xlsx",
            # "https://www.dhcs.ca.gov/path/to/outpatient_facilities.xlsx",
        ]
        
        self.logger.info("California DHCS requires manual download of Excel files")
        self.logger.info("Visit: https://www.dhcs.ca.gov/individuals/Pages/sud-licensed-certified.aspx")
        
        # If we had the Excel files, we would process them like this:
        """
        for url in excel_urls:
            try:
                # Download Excel file
                response = requests.get(url)
                
                # Save raw file
                filename = url.split('/')[-1]
                self.save_raw_data(response.content, filename)
                
                # Read Excel
                df = pd.read_excel(response.content)
                
                # Convert to facility records
                for _, row in df.iterrows():
                    facility = self._parse_dhcs_record(row)
                    facilities.append(facility)
                    
            except Exception as e:
                self.logger.error(f"Error processing {url}: {e}")
        """
        
        # For demonstration, return empty list
        return facilities
    
    def _parse_dhcs_record(self, row: pd.Series) -> Dict[str, Any]:
        """Parse a DHCS Excel row into facility record"""
        # This would map DHCS Excel columns to our schema
        return {
            "license_number": row.get("License Number", ""),
            "name": row.get("Facility Name", ""),
            "license_type": row.get("License Type", ""),
            "address": row.get("Street Address", ""),
            "city": row.get("City", ""),
            "zip": row.get("Zip Code", ""),
            "phone": row.get("Phone", ""),
            "capacity": row.get("Capacity", ""),
            "services": self._parse_services(row),
            "license_status": row.get("Status", "active"),
            "license_expiration": row.get("Expiration Date", "")
        }
    
    def _parse_services(self, row: pd.Series) -> List[str]:
        """Parse services from DHCS data"""
        services = []
        
        # DHCS typically lists services in columns or as comma-separated values
        service_columns = [col for col in row.index if "service" in col.lower()]
        for col in service_columns:
            if row[col] and str(row[col]).lower() not in ["no", "false", "0"]:
                services.append(col)
                
        return services
    
    def validate_facility(self, facility: Dict[str, Any]) -> bool:
        """Validate California facility record"""
        # Must have license number and name
        if not facility.get("license_number") or not facility.get("name"):
            return False
            
        # Must have valid address
        if not facility.get("city"):
            return False
            
        return True


class CaliforniaNTPExtractor(BaseStateExtractor):
    """Extract Narcotic Treatment Programs (NTP) from California DHCS"""
    
    def __init__(self):
        super().__init__("CA", "California")
        
    def get_agency_info(self) -> Dict[str, Any]:
        """Return info specific to NTP programs"""
        info = super().get_agency_info()
        info.update({
            "data_portal_url": "https://www.dhcs.ca.gov/services/Pages/NTP.aspx",
            "licenses_types": ["Narcotic Treatment Program", "Opioid Treatment Program"],
            "program_type": "NTP/OTP"
        })
        return info
    
    def extract_data(self) -> List[Dict[str, Any]]:
        """Extract NTP-specific data"""
        # California maintains a separate list for Narcotic Treatment Programs
        # These are also known as Opioid Treatment Programs (OTPs)
        
        facilities = []
        
        # Would extract from NTP-specific data source
        
        return facilities
    
    def validate_facility(self, facility: Dict[str, Any]) -> bool:
        """NTP-specific validation"""
        if not super().validate_facility(facility):
            return False
            
        # NTPs must have DEA registration
        if "narcotic" in facility.get("license_type", "").lower():
            if not facility.get("dea_registration"):
                self.logger.warning(f"NTP without DEA registration: {facility.get('name')}")
                
        return True


def main():
    """Run California DHCS extraction"""
    print("=== California DHCS Facility Extraction ===")
    print()
    
    # Extract general licensed facilities
    dhcs_extractor = CaliforniaDHCSExtractor()
    
    try:
        output_file = dhcs_extractor.run()
        print(f"\nExtraction complete: {output_file}")
    except Exception as e:
        print(f"\nExtraction failed: {e}")
    
    # Extract NTP facilities
    print("\n=== California NTP Extraction ===")
    ntp_extractor = CaliforniaNTPExtractor()
    
    try:
        output_file = ntp_extractor.run()
        print(f"\nNTP extraction complete: {output_file}")
    except Exception as e:
        print(f"\nNTP extraction failed: {e}")


if __name__ == "__main__":
    main()