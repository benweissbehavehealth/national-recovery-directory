#!/usr/bin/env python3
"""
Arizona Recovery Housing Alliance (AzRHA) Provider Extractor
Extracts all certified recovery residences using multiple approaches
"""

import requests
import json
import time
from typing import Dict, List, Any
import logging
from urllib.parse import urlencode
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AzRHAExtractor:
    def __init__(self):
        self.api_key = "35d896ff2e06b09f4e5bef6e8e32cf7faa8c97e4"
        self.base_url = "https://api.gethelp.com/v1"
        self.accreditation_id = 27  # AzRHA specific ID
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": "https://myazrha.org/azrha-certified-home-search"
        }
        
    def search_providers(self, offset: int = 0, limit: int = 100) -> Dict[str, Any]:
        """Search for providers using GetHelp API"""
        
        # Construct search parameters
        params = {
            "api_key": self.api_key,
            "accreditation": self.accreditation_id,
            "state": "AZ",
            "distance": 9999,  # Maximum distance to get all results
            "offset": offset,
            "limit": limit,
            "include_beds": True,
            "include_services": True,
            "include_amenities": True
        }
        
        # Try different API endpoints
        endpoints = [
            f"{self.base_url}/search",
            f"{self.base_url}/providers",
            f"{self.base_url}/facilities",
            "https://widget.gethelp.com/api/v1/search"
        ]
        
        for endpoint in endpoints:
            try:
                logging.info(f"Trying endpoint: {endpoint}")
                response = requests.get(endpoint, params=params, headers=self.headers)
                
                if response.status_code == 200:
                    data = response.json()
                    logging.info(f"Success! Found data at {endpoint}")
                    return data
                else:
                    logging.warning(f"Failed at {endpoint}: {response.status_code}")
                    
            except Exception as e:
                logging.error(f"Error at {endpoint}: {str(e)}")
                
        return None
    
    def extract_all_providers(self) -> List[Dict[str, Any]]:
        """Extract all providers with pagination"""
        all_providers = []
        offset = 0
        limit = 100
        
        while True:
            logging.info(f"Fetching providers: offset={offset}, limit={limit}")
            
            result = self.search_providers(offset, limit)
            
            if not result:
                logging.error("Failed to fetch providers")
                break
                
            providers = result.get("providers", result.get("results", []))
            
            if not providers:
                logging.info("No more providers found")
                break
                
            all_providers.extend(providers)
            logging.info(f"Added {len(providers)} providers. Total: {len(all_providers)}")
            
            # Check if we have more results
            if len(providers) < limit:
                break
                
            offset += limit
            time.sleep(1)  # Rate limiting
            
        return all_providers
    
    def parse_provider(self, provider: Dict[str, Any]) -> Dict[str, Any]:
        """Parse provider data into standardized format"""
        return {
            "name": provider.get("name", ""),
            "address": {
                "street": provider.get("address", ""),
                "city": provider.get("city", ""),
                "state": provider.get("state", "AZ"),
                "zip": provider.get("zip", "")
            },
            "contact": {
                "phone": provider.get("phone", ""),
                "email": provider.get("email", ""),
                "website": provider.get("website", "")
            },
            "certification": {
                "status": provider.get("certification_status", ""),
                "level": provider.get("certification_level", ""),
                "accreditation": "AzRHA Certified"
            },
            "capacity": {
                "total_beds": provider.get("total_beds", 0),
                "available_beds": provider.get("available_beds", 0)
            },
            "services": provider.get("services", []),
            "amenities": provider.get("amenities", []),
            "populations_served": provider.get("populations_served", []),
            "gender": provider.get("gender", ""),
            "description": provider.get("description", ""),
            "raw_data": provider
        }
    
    def save_results(self, providers: List[Dict[str, Any]], filename: str):
        """Save extracted data to JSON file"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump({
                "extraction_date": time.strftime("%Y-%m-%d %H:%M:%S"),
                "source": "Arizona Recovery Housing Alliance (AzRHA)",
                "total_providers": len(providers),
                "providers": providers
            }, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Saved {len(providers)} providers to {filename}")

def main():
    """Main extraction function"""
    extractor = AzRHAExtractor()
    
    logging.info("Starting AzRHA provider extraction...")
    
    # Extract all providers
    raw_providers = extractor.extract_all_providers()
    
    if not raw_providers:
        logging.error("No providers extracted. Trying alternative approach...")
        return
    
    # Parse providers
    parsed_providers = [extractor.parse_provider(p) for p in raw_providers]
    
    # Calculate total beds
    total_beds = sum(p["capacity"]["total_beds"] for p in parsed_providers)
    logging.info(f"Total beds across all providers: {total_beds}")
    
    # Save results
    extractor.save_results(parsed_providers, "/Users/benweiss/Code/narr_extractor/azrha_providers.json")
    
    # Summary statistics
    logging.info("\n=== EXTRACTION SUMMARY ===")
    logging.info(f"Total providers extracted: {len(parsed_providers)}")
    logging.info(f"Total beds represented: {total_beds}")
    logging.info(f"Average beds per provider: {total_beds / len(parsed_providers):.1f}")

if __name__ == "__main__":
    main()