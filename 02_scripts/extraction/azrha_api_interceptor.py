#!/usr/bin/env python3
"""
AzRHA API Interceptor - Attempts to directly access GetHelp widget data
"""

import requests
import json
import time
import logging
from typing import Dict, List, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AzRHAAPIInterceptor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://myazrha.org",
            "Referer": "https://myazrha.org/azrha-certified-home-search"
        })
        
    def try_widget_endpoints(self):
        """Try various GetHelp widget endpoints"""
        
        # Known parameters from widget
        api_key = "35d896ff2e06b09f4e5bef6e8e32cf7faa8c97e4"
        accreditation_id = 27
        
        # Potential GetHelp endpoints
        endpoints = [
            # Widget CDN endpoints
            "https://cdn.gethelp.com/api/v1/search",
            "https://cdn.gethelp.com/widget/api/search",
            "https://widget.gethelp.com/v1/search",
            "https://app.gethelp.com/api/search",
            
            # Possible API variations
            "https://api.gethelpnetwork.com/v1/search",
            "https://gethelp.com/api/v1/search",
            "https://www.gethelp.com/api/search",
            
            # Widget-specific endpoints
            "https://widget.gethelp.com/api/providers",
            "https://widget.gethelp.com/api/facilities",
            "https://widget.gethelp.com/api/listings"
        ]
        
        for endpoint in endpoints:
            try:
                logging.info(f"Trying endpoint: {endpoint}")
                
                # Try with different parameter combinations
                params_list = [
                    {
                        "api_key": api_key,
                        "accreditation": accreditation_id,
                        "state": "AZ"
                    },
                    {
                        "apiKey": api_key,
                        "accreditationId": accreditation_id,
                        "state": "AZ"
                    },
                    {
                        "key": api_key,
                        "accreditation_id": accreditation_id,
                        "location": "Arizona"
                    }
                ]
                
                for params in params_list:
                    response = self.session.get(endpoint, params=params, timeout=10)
                    
                    if response.status_code == 200:
                        logging.info(f"SUCCESS! Got 200 response from {endpoint}")
                        return response.json()
                    elif response.status_code == 401:
                        logging.info(f"401 Unauthorized - API key may be invalid")
                    elif response.status_code == 403:
                        logging.info(f"403 Forbidden - Access restricted")
                    else:
                        logging.info(f"Status {response.status_code}")
                        
            except requests.exceptions.ConnectionError:
                logging.info(f"Connection error - domain may not exist")
            except requests.exceptions.Timeout:
                logging.info(f"Timeout - endpoint not responding")
            except Exception as e:
                logging.error(f"Error: {str(e)}")
                
        return None
        
    def try_direct_scraping(self):
        """Try to get the page HTML and look for embedded data"""
        
        url = "https://myazrha.org/azrha-certified-home-search"
        
        try:
            response = self.session.get(url)
            html = response.text
            
            # Look for JSON data embedded in the page
            import re
            
            # Common patterns for embedded JSON
            patterns = [
                r'window\.__INITIAL_STATE__\s*=\s*({.*?});',
                r'<script[^>]*>.*?({.*?"providers".*?}).*?</script>',
                r'data-providers=["\'](.*?)["\']',
                r'gethelp\.init\((.*?)\)',
                r'facilities:\s*(\[.*?\])',
                r'providers:\s*(\[.*?\])'
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, html, re.DOTALL)
                if matches:
                    for match in matches:
                        try:
                            data = json.loads(match)
                            if isinstance(data, (list, dict)):
                                logging.info(f"Found embedded JSON data!")
                                return data
                        except:
                            pass
                            
            # Look for any URLs that might contain provider data
            provider_urls = re.findall(r'https?://[^\s"\'>]+(?:provider|facility|residence|listing)[^\s"\'>]*', html)
            if provider_urls:
                logging.info(f"Found potential provider URLs: {provider_urls}")
                
        except Exception as e:
            logging.error(f"Error scraping page: {str(e)}")
            
        return None
        
    def extract_from_sitemap(self):
        """Check sitemap for individual provider pages"""
        
        sitemap_urls = [
            "https://myazrha.org/sitemap.xml",
            "https://myazrha.org/sitemap_index.xml",
            "https://myazrha.org/robots.txt"
        ]
        
        for url in sitemap_urls:
            try:
                response = self.session.get(url)
                if response.status_code == 200:
                    content = response.text
                    
                    # Look for provider-related URLs
                    if 'provider' in content.lower() or 'facility' in content.lower():
                        logging.info(f"Found potential provider URLs in {url}")
                        
                        # Extract URLs
                        import re
                        urls = re.findall(r'<loc>(.*?)</loc>', content)
                        provider_urls = [u for u in urls if any(keyword in u.lower() for keyword in ['provider', 'facility', 'residence', 'home'])]
                        
                        if provider_urls:
                            return provider_urls
                            
            except Exception as e:
                logging.error(f"Error checking {url}: {str(e)}")
                
        return None

def main():
    interceptor = AzRHAAPIInterceptor()
    
    logging.info("Starting AzRHA API interception attempts...")
    
    # Try API endpoints
    api_data = interceptor.try_widget_endpoints()
    if api_data:
        with open("/Users/benweiss/Code/narr_extractor/azrha_api_data.json", 'w') as f:
            json.dump(api_data, f, indent=2)
        logging.info("Successfully extracted API data!")
        return
        
    # Try direct scraping
    logging.info("\nTrying direct HTML scraping...")
    scraped_data = interceptor.try_direct_scraping()
    if scraped_data:
        with open("/Users/benweiss/Code/narr_extractor/azrha_scraped_data.json", 'w') as f:
            json.dump(scraped_data, f, indent=2)
        logging.info("Successfully extracted embedded data!")
        return
        
    # Try sitemap
    logging.info("\nChecking sitemap for provider pages...")
    provider_urls = interceptor.extract_from_sitemap()
    if provider_urls:
        logging.info(f"Found {len(provider_urls)} potential provider URLs")
        
    # Final summary
    logging.info("\n=== EXTRACTION SUMMARY ===")
    logging.info("Unable to directly access GetHelp widget data")
    logging.info("The widget appears to use client-side rendering with restricted API access")
    logging.info("\nRecommended next steps:")
    logging.info("1. Contact AzRHA directly at membership@myazrha.org")
    logging.info("2. Use browser automation with proper setup")
    logging.info("3. Check Arizona DHS licensing database")
    logging.info("4. Request API access from GetHelp")

if __name__ == "__main__":
    main()