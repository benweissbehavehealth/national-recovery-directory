import requests
from bs4 import BeautifulSoup
import json
import time
import re
from urllib.parse import urljoin

class TROHNComprehensiveExtractor:
    def __init__(self):
        self.base_url = "https://trohn.org"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.all_residences = []
        
    def extract_everything(self):
        """Comprehensive extraction using all available methods"""
        print("=== TROHN Comprehensive Extraction ===\n")
        
        # 1. Extract from operators page with detailed parsing
        operators = self.extract_operators_detailed()
        
        # 2. Extract from any individual operator pages
        residence_details = self.extract_residence_details_from_operators(operators)
        
        # 3. Compile all unique residences
        self.compile_residences(operators, residence_details)
        
        # 4. Save results
        self.save_comprehensive_results()
        
        return self.all_residences
    
    def extract_operators_detailed(self):
        """Extract detailed operator information with property listings"""
        print("1. Extracting from operators directory...")
        
        response = self.session.get(f"{self.base_url}/operators/")
        soup = BeautifulSoup(response.text, 'html.parser')
        
        operators = []
        
        # Find all operator posts
        posts = soup.find_all(class_='fl-post-feed-post')
        print(f"   Found {len(posts)} operator posts")
        
        for post in posts:
            operator = {}
            
            # Get name
            title = post.find(['h2', 'h3', 'h4'])
            if title:
                link = title.find('a')
                if link:
                    operator['name'] = link.get_text(strip=True)
                    operator['url'] = link.get('href', '')
                else:
                    operator['name'] = title.get_text(strip=True)
            
            # Get content
            content = post.find(class_='fl-post-text')
            if content:
                # Extract all text
                text = content.get_text(separator='\n', strip=True)
                lines = text.split('\n')
                
                operator['properties'] = []
                operator['raw_info'] = {}
                
                current_section = None
                
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Detect sections
                    if line.lower() in ['phone', 'website', 'location', 'contact', 'residences', 'priority population(s)']:
                        current_section = line.lower()
                        continue
                    
                    # Store information by section
                    if current_section == 'phone':
                        phone_match = re.search(r'(\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4})', line)
                        if phone_match:
                            operator['phone'] = phone_match.group(1)
                    
                    elif current_section == 'website':
                        if 'http' in line:
                            operator['website'] = line.strip()
                    
                    elif current_section == 'location':
                        if line and line != '—':
                            operator['location'] = line
                    
                    elif current_section == 'contact':
                        if line and line != '—' and not line.lower().startswith('residences'):
                            operator['contact_person'] = line
                    
                    elif current_section == 'residences':
                        # Parse property addresses
                        if re.search(r'\d+', line) and len(line) > 5:
                            # This looks like an address
                            property_info = {
                                'address': line,
                                'operator': operator.get('name', '')
                            }
                            
                            # Try to parse address components
                            addr_match = re.search(r'(\d+)\s+(.*)', line)
                            if addr_match:
                                property_info['street_number'] = addr_match.group(1)
                                property_info['street_name'] = addr_match.group(2)
                            
                            operator['properties'].append(property_info)
                
                # Store raw info for debugging
                operator['raw_text'] = text[:1000]
            
            if operator.get('name'):
                operators.append(operator)
                print(f"   - {operator['name']}: {len(operator.get('properties', []))} properties")
        
        return operators
    
    def extract_residence_details_from_operators(self, operators):
        """Get additional details from individual operator pages"""
        print("\n2. Extracting details from individual operator pages...")
        
        all_details = []
        
        for operator in operators:
            if not operator.get('url'):
                continue
            
            print(f"   Checking {operator['name']}...")
            
            try:
                response = self.session.get(operator['url'])
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Look for content area
                content = soup.find(class_=['entry-content', 'fl-post-content', 'content'])
                if content:
                    text = content.get_text(separator='\n', strip=True)
                    
                    # Look for additional properties
                    lines = text.split('\n')
                    properties = []
                    
                    for i, line in enumerate(lines):
                        # Look for address patterns
                        if re.search(r'^\d+\s+\w+', line):
                            property_info = {
                                'address': line.strip(),
                                'operator': operator['name'],
                                'operator_phone': operator.get('phone', ''),
                                'operator_website': operator.get('website', ''),
                                'operator_location': operator.get('location', '')
                            }
                            
                            # Check next lines for city/state
                            if i + 1 < len(lines):
                                next_line = lines[i + 1].strip()
                                if re.search(r'[A-Za-z\s]+,?\s*TX', next_line):
                                    property_info['city_state'] = next_line
                            
                            properties.append(property_info)
                    
                    if properties:
                        operator['detailed_properties'] = properties
                        all_details.extend(properties)
                        print(f"     Found {len(properties)} additional property details")
                
                time.sleep(0.5)  # Be respectful
                
            except Exception as e:
                print(f"     Error: {e}")
        
        return all_details
    
    def compile_residences(self, operators, additional_details):
        """Compile all unique residences from all sources"""
        print("\n3. Compiling all residences...")
        
        # Add residences from operator listings
        for operator in operators:
            # From main properties list
            for prop in operator.get('properties', []):
                residence = {
                    'name': f"{operator['name']} - {prop['address']}",
                    'operator_name': operator['name'],
                    'address': prop['address'],
                    'phone': operator.get('phone', ''),
                    'website': operator.get('website', ''),
                    'location': operator.get('location', ''),
                    'contact_person': operator.get('contact_person', ''),
                    'source': 'operators_directory'
                }
                self.all_residences.append(residence)
            
            # From detailed properties
            for prop in operator.get('detailed_properties', []):
                residence = {
                    'name': f"{operator['name']} - {prop['address']}",
                    'operator_name': operator['name'],
                    'address': prop['address'],
                    'city_state': prop.get('city_state', ''),
                    'phone': operator.get('phone', ''),
                    'website': operator.get('website', ''),
                    'location': operator.get('location', ''),
                    'contact_person': operator.get('contact_person', ''),
                    'source': 'operator_detail_page'
                }
                self.all_residences.append(residence)
        
        # Add additional details
        for detail in additional_details:
            if detail not in self.all_residences:
                self.all_residences.append(detail)
        
        # Deduplicate based on address
        unique_residences = []
        seen_addresses = set()
        
        for res in self.all_residences:
            addr = res.get('address', '').lower().strip()
            if addr and addr not in seen_addresses:
                seen_addresses.add(addr)
                unique_residences.append(res)
        
        self.all_residences = unique_residences
        print(f"   Total unique residences: {len(self.all_residences)}")
    
    def save_comprehensive_results(self):
        """Save all results in a comprehensive format"""
        print("\n4. Saving results...")
        
        # Group by operator
        by_operator = {}
        for res in self.all_residences:
            op_name = res.get('operator_name', 'Unknown')
            if op_name not in by_operator:
                by_operator[op_name] = []
            by_operator[op_name].append(res)
        
        output = {
            'source': 'Texas Recovery Organization Housing Network (TROHN)',
            'url': 'https://trohn.org',
            'extraction_date': time.strftime('%Y-%m-%d %H:%M:%S'),
            'extraction_method': 'comprehensive_multi_source',
            'statistics': {
                'total_residences_found': len(self.all_residences),
                'total_operators': len(by_operator),
                'target_total': 78,
                'completion_percentage': round((len(self.all_residences) / 78) * 100, 1)
            },
            'residences_by_operator': by_operator,
            'all_residences': self.all_residences,
            'notes': [
                'Extracted from operators directory and individual operator pages',
                'Some operators may have additional properties not listed online',
                'Contact operators directly for complete current listings',
                f'Found {len(self.all_residences)} of 78 known certified residences'
            ]
        }
        
        # Save main results
        with open('/Users/benweiss/Code/narr_extractor/trohn_comprehensive_results.json', 'w') as f:
            json.dump(output, f, indent=2)
        
        # Save CSV for easy viewing
        import csv
        with open('/Users/benweiss/Code/narr_extractor/trohn_residences.csv', 'w', newline='') as f:
            if self.all_residences:
                writer = csv.DictWriter(f, fieldnames=self.all_residences[0].keys())
                writer.writeheader()
                writer.writerows(self.all_residences)
        
        print(f"   Saved to trohn_comprehensive_results.json and trohn_residences.csv")
        
        # Print summary
        print("\n=== EXTRACTION SUMMARY ===")
        print(f"Total Residences Found: {len(self.all_residences)}")
        print(f"Total Operators: {len(by_operator)}")
        print(f"Target: 78 certified residences")
        print(f"Completion: {output['statistics']['completion_percentage']}%")
        
        if len(self.all_residences) < 78:
            print(f"\n⚠️  Missing approximately {78 - len(self.all_residences)} residences")
            print("These may require:")
            print("- Direct contact with TROHN for complete directory access")
            print("- Login credentials to access full listings")
            print("- Manual review of PDF documents or member-only content")
        
        return output

if __name__ == "__main__":
    extractor = TROHNComprehensiveExtractor()
    results = extractor.extract_everything()