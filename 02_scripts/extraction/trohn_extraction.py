import requests
from bs4 import BeautifulSoup
import json
import time
from urllib.parse import urljoin, quote
import re

class TROHNExtractor:
    def __init__(self):
        self.base_url = "https://trohn.org"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.all_residences = []
        
    def extract_operators_directory(self):
        """Extract data from the operators directory"""
        print("Extracting operators directory...")
        url = f"{self.base_url}/operators/"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all operator entries
            operators = []
            
            # Look for operator listings in various possible containers
            # Common patterns: divs with class names like 'operator', 'member', 'listing'
            for container in soup.find_all(['div', 'article', 'section']):
                # Extract text content to identify operator entries
                text = container.get_text(strip=True)
                if any(keyword in text.lower() for keyword in ['phone:', 'contact:', 'location:', 'address:']):
                    operator_data = self.parse_operator_entry(container)
                    if operator_data and operator_data.get('name'):
                        operators.append(operator_data)
            
            return operators
            
        except Exception as e:
            print(f"Error extracting operators: {e}")
            return []
    
    def parse_operator_entry(self, container):
        """Parse individual operator entry"""
        data = {
            'name': '',
            'phone': '',
            'email': '',
            'contact_person': '',
            'location': '',
            'properties': [],
            'website': ''
        }
        
        # Extract text and look for patterns
        text = container.get_text(separator='\n', strip=True)
        lines = text.split('\n')
        
        # Simple pattern matching for common fields
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Phone numbers
            phone_match = re.search(r'(\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4})', line)
            if phone_match and not data['phone']:
                data['phone'] = phone_match.group(1)
            
            # Email
            email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', line)
            if email_match and not data['email']:
                data['email'] = email_match.group(1)
            
            # Location patterns
            if any(city in line for city in ['TX', 'Texas', 'Austin', 'Houston', 'Dallas', 'San Antonio']):
                if not data['location']:
                    data['location'] = line
        
        # Try to extract name from heading tags
        heading = container.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        if heading:
            data['name'] = heading.get_text(strip=True)
        
        return data if data['name'] else None
    
    def search_housing_directory(self):
        """Attempt to search the housing directory"""
        print("Searching housing directory...")
        search_url = f"{self.base_url}/find/"
        
        try:
            # First, get the search page to understand the form structure
            response = self.session.get(search_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for search forms
            forms = soup.find_all('form')
            print(f"Found {len(forms)} forms on the search page")
            
            # Try to identify search parameters
            search_params = {}
            for form in forms:
                inputs = form.find_all(['input', 'select'])
                for inp in inputs:
                    name = inp.get('name')
                    if name:
                        if inp.name == 'select':
                            options = inp.find_all('option')
                            values = [opt.get('value', '') for opt in options if opt.get('value')]
                            search_params[name] = values
                        else:
                            search_params[name] = inp.get('value', '')
            
            print(f"Identified search parameters: {list(search_params.keys())}")
            
            # Try to submit empty search to get all results
            if forms:
                form_action = forms[0].get('action', search_url)
                form_method = forms[0].get('method', 'get').lower()
                
                if form_method == 'post':
                    response = self.session.post(form_action, data={})
                else:
                    response = self.session.get(form_action, params={})
                
                return self.parse_search_results(response.text)
            
        except Exception as e:
            print(f"Error searching housing directory: {e}")
            return []
    
    def parse_search_results(self, html):
        """Parse search results from the housing directory"""
        soup = BeautifulSoup(html, 'html.parser')
        residences = []
        
        # Common patterns for result listings
        result_selectors = [
            'div.result', 'div.listing', 'article.residence',
            'div.property', 'div.house', 'div.recovery-home'
        ]
        
        for selector in result_selectors:
            results = soup.select(selector)
            if results:
                print(f"Found {len(results)} results using selector: {selector}")
                for result in results:
                    residence_data = self.parse_residence_listing(result)
                    if residence_data:
                        residences.append(residence_data)
                break
        
        return residences
    
    def parse_residence_listing(self, container):
        """Parse individual residence listing"""
        data = {
            'name': '',
            'address': '',
            'phone': '',
            'email': '',
            'website': '',
            'certification_level': '',
            'services': [],
            'capacity': '',
            'gender': '',
            'focus_areas': []
        }
        
        # Extract all text content
        text = container.get_text(separator='\n', strip=True)
        
        # Try to extract structured data
        # Name from heading
        heading = container.find(['h1', 'h2', 'h3', 'h4'])
        if heading:
            data['name'] = heading.get_text(strip=True)
        
        # Contact information
        for line in text.split('\n'):
            line = line.strip()
            
            # Phone
            phone_match = re.search(r'(\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4})', line)
            if phone_match and not data['phone']:
                data['phone'] = phone_match.group(1)
            
            # Email
            email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', line)
            if email_match and not data['email']:
                data['email'] = email_match.group(1)
            
            # Address patterns
            if re.search(r'\d+\s+\w+\s+(St|Street|Ave|Avenue|Rd|Road|Dr|Drive|Blvd|Boulevard)', line):
                if not data['address']:
                    data['address'] = line
        
        # Links
        links = container.find_all('a', href=True)
        for link in links:
            href = link['href']
            if 'http' in href and not data['website']:
                data['website'] = href
        
        return data if data['name'] else None
    
    def inspect_html_structure(self, url):
        """Debug method to inspect HTML structure"""
        try:
            response = self.session.get(url)
            response.raise_for_status()
            
            # Save raw HTML for inspection
            with open('/Users/benweiss/Code/narr_extractor/trohn_page_debug.html', 'w') as f:
                f.write(response.text)
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all divs with classes
            divs_with_classes = soup.find_all('div', class_=True)
            class_counts = {}
            for div in divs_with_classes:
                classes = div.get('class', [])
                for cls in classes:
                    class_counts[cls] = class_counts.get(cls, 0) + 1
            
            print(f"\nFound div classes: {sorted(class_counts.items(), key=lambda x: x[1], reverse=True)[:20]}")
            
            # Find all links
            links = soup.find_all('a', href=True)
            print(f"\nFound {len(links)} links")
            
            # Look for data attributes
            data_attrs = set()
            for elem in soup.find_all(True):
                for attr in elem.attrs:
                    if attr.startswith('data-'):
                        data_attrs.add(attr)
            print(f"\nData attributes found: {data_attrs}")
            
        except Exception as e:
            print(f"Error inspecting HTML: {e}")
    
    def extract_all_data(self):
        """Main extraction method"""
        print("Starting TROHN data extraction...")
        
        # First inspect the structure
        print("\n=== Inspecting operators page structure ===")
        self.inspect_html_structure(f"{self.base_url}/operators/")
        
        print("\n=== Inspecting find housing page structure ===")
        self.inspect_html_structure(f"{self.base_url}/find/")
        
        # 1. Extract operators
        operators = self.extract_operators_directory()
        print(f"Extracted {len(operators)} operators")
        
        # 2. Try to search housing directory
        housing_results = self.search_housing_directory()
        print(f"Found {len(housing_results)} housing results")
        
        # 3. Combine and deduplicate
        all_entries = operators + housing_results
        
        # Save results
        output_data = {
            'source': 'Texas Recovery Organization Housing Network (TROHN)',
            'url': 'https://trohn.org',
            'extraction_date': time.strftime('%Y-%m-%d'),
            'total_found': len(all_entries),
            'operators': operators,
            'housing_listings': housing_results,
            'all_entries': all_entries
        }
        
        with open('/Users/benweiss/Code/narr_extractor/trohn_raw_extraction.json', 'w') as f:
            json.dump(output_data, f, indent=2)
        
        print(f"Saved {len(all_entries)} total entries to trohn_raw_extraction.json")
        return output_data

if __name__ == "__main__":
    extractor = TROHNExtractor()
    results = extractor.extract_all_data()