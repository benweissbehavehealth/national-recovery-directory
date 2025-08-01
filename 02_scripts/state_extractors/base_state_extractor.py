#!/usr/bin/env python3
"""
Base class for state licensing agency data extractors
"""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
import json
import logging
from typing import Dict, List, Optional, Any
import hashlib

class BaseStateExtractor(ABC):
    """Base class for extracting data from state licensing agencies"""
    
    def __init__(self, state_code: str, state_name: str):
        self.state_code = state_code.upper()
        self.state_name = state_name
        self.timestamp = datetime.now()
        
        # Set up paths
        self.base_path = Path(__file__).parent.parent.parent
        self.raw_data_path = self.base_path / "03_raw_data" / "state_agencies" / self.state_code
        self.raw_data_path.mkdir(parents=True, exist_ok=True)
        
        # Set up logging
        self.logger = self._setup_logger()
        
        # Agency information
        self.agency_info = self.get_agency_info()
        
    def _setup_logger(self) -> logging.Logger:
        """Set up logging for the extractor"""
        logger = logging.getLogger(f"{self.state_code}_extractor")
        logger.setLevel(logging.INFO)
        
        # File handler
        log_file = self.raw_data_path / f"{self.state_code}_extraction_{self.timestamp.strftime('%Y%m%d_%H%M%S')}.log"
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.INFO)
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        return logger
    
    @abstractmethod
    def get_agency_info(self) -> Dict[str, Any]:
        """Return information about the state agency"""
        return {
            "state_code": self.state_code,
            "state_name": self.state_name,
            "agency_name": "",
            "agency_abbreviation": "",
            "website": "",
            "data_portal_url": "",
            "licenses_types": [],
            "data_format": "",  # api, csv, excel, web_scrape, pdf
            "update_frequency": "",  # daily, weekly, monthly, quarterly
            "last_updated": None
        }
    
    @abstractmethod
    def extract_data(self) -> List[Dict[str, Any]]:
        """Extract facility data from the state agency"""
        pass
    
    @abstractmethod
    def validate_facility(self, facility: Dict[str, Any]) -> bool:
        """Validate that a facility record has required fields"""
        pass
    
    def normalize_facility(self, facility: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize facility data to common schema"""
        normalized = {
            # Identifiers
            "facility_id": self.generate_facility_id(facility),
            "state_license_number": facility.get("license_number", ""),
            "npi": facility.get("npi", ""),
            
            # Basic Information
            "name": self.clean_name(facility.get("name", "")),
            "organization_type": self.map_organization_type(facility),
            
            # Location
            "address_street": facility.get("address", ""),
            "address_city": facility.get("city", ""),
            "address_state": self.state_code,
            "address_zip": facility.get("zip", ""),
            "latitude": facility.get("latitude"),
            "longitude": facility.get("longitude"),
            
            # Contact
            "phone": self.clean_phone(facility.get("phone", "")),
            "email": facility.get("email", ""),
            "website": facility.get("website", ""),
            
            # Licensing
            "license_type": facility.get("license_type", ""),
            "license_status": facility.get("license_status", "active"),
            "license_expiration": facility.get("license_expiration"),
            
            # Services
            "services": facility.get("services", []),
            "populations_served": facility.get("populations_served", []),
            "capacity": facility.get("capacity"),
            
            # Metadata
            "data_source": f"{self.state_code}_licensing",
            "extraction_date": self.timestamp.isoformat(),
            "agency_name": self.agency_info["agency_name"],
            "raw_data": facility  # Keep original data
        }
        
        return normalized
    
    def generate_facility_id(self, facility: Dict[str, Any]) -> str:
        """Generate unique facility ID"""
        # Use state + license number if available
        if facility.get("license_number"):
            return f"{self.state_code}_{facility['license_number']}"
        
        # Otherwise use hash of name + address
        unique_string = f"{facility.get('name', '')}_{facility.get('address', '')}_{facility.get('city', '')}"
        return f"{self.state_code}_{hashlib.md5(unique_string.encode()).hexdigest()[:8]}"
    
    def clean_name(self, name: str) -> str:
        """Clean and standardize facility name"""
        if not name:
            return ""
        
        # Remove common suffixes
        suffixes = ["LLC", "INC", "CORP", "LTD", "PC", "PLLC"]
        name = name.strip()
        
        for suffix in suffixes:
            if name.upper().endswith(f" {suffix}"):
                name = name[:-len(suffix)-1].strip()
            if name.upper().endswith(f", {suffix}"):
                name = name[:-len(suffix)-2].strip()
                
        return name.strip()
    
    def clean_phone(self, phone: str) -> str:
        """Clean and standardize phone number"""
        if not phone:
            return ""
        
        # Remove non-numeric characters
        cleaned = ''.join(filter(str.isdigit, phone))
        
        # Format as (XXX) XXX-XXXX if 10 digits
        if len(cleaned) == 10:
            return f"({cleaned[:3]}) {cleaned[3:6]}-{cleaned[6:]}"
        
        return phone
    
    def map_organization_type(self, facility: Dict[str, Any]) -> str:
        """Map facility type to our standard organization types"""
        license_type = facility.get("license_type", "").lower()
        facility_type = facility.get("facility_type", "").lower()
        
        # Mapping logic
        if any(term in license_type for term in ["residential", "res treatment", "rtp"]):
            return "treatment_centers"
        elif any(term in license_type for term in ["outpatient", "op", "iop"]):
            return "treatment_centers"
        elif any(term in license_type for term in ["detox", "withdrawal"]):
            return "treatment_centers"
        elif any(term in license_type for term in ["recovery residence", "sober living", "halfway"]):
            return "recovery_residences"
        elif any(term in license_type for term in ["recovery community", "peer support"]):
            return "rccs"
        else:
            return "treatment_centers"  # Default
    
    def save_raw_data(self, data: Any, filename: str):
        """Save raw data to file"""
        filepath = self.raw_data_path / filename
        
        if filename.endswith('.json'):
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2, default=str)
        else:
            # For other formats, just write as-is
            with open(filepath, 'wb' if isinstance(data, bytes) else 'w') as f:
                f.write(data)
                
        self.logger.info(f"Saved raw data to {filepath}")
    
    def save_normalized_data(self, facilities: List[Dict[str, Any]]):
        """Save normalized facility data"""
        filename = f"{self.state_code}_facilities_{self.timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        filepath = self.raw_data_path / filename
        
        output = {
            "state": self.state_code,
            "agency": self.agency_info,
            "extraction_date": self.timestamp.isoformat(),
            "total_facilities": len(facilities),
            "facilities": facilities
        }
        
        with open(filepath, 'w') as f:
            json.dump(output, f, indent=2, default=str)
            
        self.logger.info(f"Saved {len(facilities)} normalized facilities to {filepath}")
        
        return filepath
    
    def run(self) -> Path:
        """Run the extraction process"""
        self.logger.info(f"Starting extraction for {self.state_name} ({self.state_code})")
        self.logger.info(f"Agency: {self.agency_info['agency_name']}")
        self.logger.info(f"Data source: {self.agency_info['data_portal_url']}")
        
        try:
            # Extract raw data
            raw_facilities = self.extract_data()
            self.logger.info(f"Extracted {len(raw_facilities)} raw facilities")
            
            # Validate and normalize
            normalized_facilities = []
            invalid_count = 0
            
            for facility in raw_facilities:
                if self.validate_facility(facility):
                    normalized = self.normalize_facility(facility)
                    normalized_facilities.append(normalized)
                else:
                    invalid_count += 1
            
            self.logger.info(f"Normalized {len(normalized_facilities)} facilities")
            if invalid_count > 0:
                self.logger.warning(f"Skipped {invalid_count} invalid facilities")
            
            # Save results
            output_file = self.save_normalized_data(normalized_facilities)
            
            # Summary statistics
            self._log_summary(normalized_facilities)
            
            return output_file
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _log_summary(self, facilities: List[Dict[str, Any]]):
        """Log summary statistics"""
        self.logger.info("\n=== EXTRACTION SUMMARY ===")
        self.logger.info(f"Total facilities: {len(facilities)}")
        
        # Count by type
        type_counts = {}
        for f in facilities:
            org_type = f.get("organization_type", "unknown")
            type_counts[org_type] = type_counts.get(org_type, 0) + 1
        
        self.logger.info("Facilities by type:")
        for org_type, count in sorted(type_counts.items()):
            self.logger.info(f"  {org_type}: {count}")
        
        # Count by license type
        license_counts = {}
        for f in facilities:
            lic_type = f.get("license_type", "unknown")
            license_counts[lic_type] = license_counts.get(lic_type, 0) + 1
            
        self.logger.info("Facilities by license type:")
        for lic_type, count in sorted(license_counts.items())[:10]:  # Top 10
            self.logger.info(f"  {lic_type}: {count}")