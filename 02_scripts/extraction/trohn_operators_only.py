import requests
from bs4 import BeautifulSoup
import json
import time
import re

class TROHNOperatorsExtractor:
    def __init__(self):
        self.base_url = "https://trohn.org"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
    def extract_operators_detailed(self):
        """Extract detailed operator information"""
        print("\n=== Extracting TROHN Operators ===")
        
        # Get operators page
        response = self.session.get(f"{self.base_url}/operators/")
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Save HTML for inspection
        with open('/Users/benweiss/Code/narr_extractor/trohn_operators_page.html', 'w') as f:
            f.write(response.text)
        
        operators = []
        
        # Method 1: Look for organization posts
        org_posts = soup.find_all('article', class_=re.compile('organization|type-organization'))
        print(f"Found {len(org_posts)} organization posts")
        
        for post in org_posts:
            operator = self.parse_operator_post(post)
            if operator:
                operators.append(operator)
                print(f"  - {operator.get('name', 'Unknown')}")
        
        # Method 2: Look for post feed items
        if not operators:
            print("\nTrying alternative method: post feed items")
            feed_posts = soup.find_all(class_='fl-post-feed-post')
            print(f"Found {len(feed_posts)} feed posts")
            
            for post in feed_posts:
                operator = self.parse_operator_post(post)
                if operator:
                    operators.append(operator)
                    print(f"  - {operator.get('name', 'Unknown')}")
        
        # Method 3: Look for any heading with operator-like content
        if not operators:
            print("\nTrying alternative method: all headings")
            headings = soup.find_all(['h1', 'h2', 'h3', 'h4'])
            
            for heading in headings:
                text = heading.get_text(strip=True)
                # Skip navigation/menu items
                if not any(skip in text.lower() for skip in ['menu', 'navigation', 'search', 'find', 'about']):
                    parent = heading.parent
                    operator = self.parse_content_block(parent, text)
                    if operator:
                        operators.append(operator)
                        print(f"  - {operator.get('name', 'Unknown')}")
        
        # Also look for any structured data
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)
                if '@graph' in data:
                    for item in data['@graph']:
                        if item.get('@type') == 'Organization':
                            operators.append(self.parse_structured_data(item))
            except:
                pass
        
        # Try to get additional details from each operator's page
        for operator in operators:
            if 'url' in operator and operator['url']:
                print(f"\nGetting details for: {operator['name']}")
                details = self.get_operator_details(operator['url'])
                operator.update(details)
        
        return operators
    
    def parse_operator_post(self, post):
        """Parse operator from post"""
        operator = {}
        
        # Get title/name
        title = post.find(['h1', 'h2', 'h3', 'h4'])
        if title:
            # Check if it's a link
            link = title.find('a')
            if link:
                operator['name'] = link.get_text(strip=True)
                operator['url'] = link.get('href', '')
            else:
                operator['name'] = title.get_text(strip=True)
        
        # Get content
        content = post.find(class_=['fl-post-excerpt', 'entry-content', 'post-content', 'fl-post-text'])
        if content:
            operator.update(self.extract_contact_info(content))
        
        return operator if operator.get('name') else None
    
    def parse_content_block(self, block, name):
        """Parse a content block that might contain operator info"""
        if not block:
            return None
            
        operator = {'name': name}
        operator.update(self.extract_contact_info(block))
        
        return operator if any(operator.get(field) for field in ['phone', 'email', 'address']) else None
    
    def extract_contact_info(self, element):
        """Extract contact information from an element"""
        info = {}
        text = element.get_text(separator=' ', strip=True)
        
        # Extract phone
        phone_patterns = [
            r'(\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4})',
            r'(\d{3}[\s.-]\d{3}[\s.-]\d{4})',
            r'Phone:?\s*(\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4})'
        ]
        for pattern in phone_patterns:
            phone_match = re.search(pattern, text, re.IGNORECASE)
            if phone_match:
                info['phone'] = phone_match.group(1) if 'Phone' not in pattern else phone_match.group(1)
                break
        
        # Extract email
        email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', text)
        if email_match:
            info['email'] = email_match.group(1)
        
        # Extract address
        address_patterns = [
            r'(\d+\s+[\w\s]+(?:St|Street|Ave|Avenue|Rd|Road|Dr|Drive|Blvd|Boulevard|Ln|Lane|Way|Pkwy|Parkway)\.?)',
            r'Address:?\s*([^\n]+)',
            r'Location:?\s*([^\n]+)'
        ]
        for pattern in address_patterns:
            address_match = re.search(pattern, text, re.IGNORECASE)
            if address_match:
                info['address'] = address_match.group(1).strip()
                break
        
        # Extract city
        city_patterns = [
            r'([A-Za-z\s]+),?\s+TX\s+(\d{5})',
            r'([A-Za-z\s]+),?\s+Texas'
        ]
        for pattern in city_patterns:
            city_match = re.search(pattern, text)
            if city_match:
                info['city'] = city_match.group(1).strip()
                if len(city_match.groups()) > 1:
                    info['zip'] = city_match.group(2)
                break
        
        # Look for website links
        links = element.find_all('a', href=True)
        for link in links:
            href = link.get('href', '')
            if 'http' in href and 'trohn.org' not in href and '@' not in href:
                info['website'] = href
                break
        
        # Store raw text for debugging
        info['raw_text'] = text[:500] if len(text) > 500 else text
        
        # Look for property/residence count
        properties_match = re.search(r'(\d+)\s*(?:properties|locations|residences|homes|beds)', text, re.IGNORECASE)
        if properties_match:
            info['property_count'] = properties_match.group(1)
        
        # Look for contact person
        contact_patterns = [
            r'Contact:?\s*([A-Za-z\s]+)',
            r'Manager:?\s*([A-Za-z\s]+)',
            r'Director:?\s*([A-Za-z\s]+)'
        ]
        for pattern in contact_patterns:
            contact_match = re.search(pattern, text)
            if contact_match:
                name = contact_match.group(1).strip()
                # Validate it's likely a person name
                if len(name.split()) <= 4 and not any(word in name.lower() for word in ['phone', 'email', 'address']):
                    info['contact_person'] = name
                    break
        
        return info
    
    def get_operator_details(self, url):
        """Get additional details from operator page"""
        details = {}
        try:
            response = self.session.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for main content
            content = soup.find(class_=['entry-content', 'post-content', 'content', 'fl-post-content'])
            if content:
                details.update(self.extract_contact_info(content))
                
                # Look for specific property listings
                properties = []
                
                # Method 1: Look for address-like patterns
                text = content.get_text(separator='\n', strip=True)
                lines = text.split('\n')
                
                for i, line in enumerate(lines):
                    # Look for property addresses
                    if re.search(r'\d+\s+[\w\s]+(?:St|Street|Ave|Avenue|Rd|Road|Dr|Drive)', line):
                        property_info = {'address': line.strip()}
                        
                        # Check next lines for additional info
                        if i + 1 < len(lines):
                            next_line = lines[i + 1].strip()
                            if re.search(r'[A-Za-z\s]+,?\s+TX', next_line):
                                property_info['city_state'] = next_line
                        
                        properties.append(property_info)
                
                if properties:
                    details['properties'] = properties
                    details['property_count'] = len(properties)
                
                # Look for certification info
                if 'certified' in text.lower() or 'trohn' in text.lower():
                    details['certification_mentioned'] = True
                    
                    # Try to extract certification level
                    cert_match = re.search(r'(Level\s*[IVX\d]+|TROHN\s*[IVX\d]+)', text, re.IGNORECASE)
                    if cert_match:
                        details['certification_level'] = cert_match.group(1)
                
                # Look for services
                services = []
                service_keywords = ['recovery', 'sober living', 'transitional', 'residential', 'treatment', 
                                  'men', 'women', 'co-ed', 'mat', 'medication assisted']
                
                for keyword in service_keywords:
                    if keyword in text.lower():
                        services.append(keyword)
                
                if services:
                    details['services'] = services
                    
        except Exception as e:
            print(f"  Error getting details: {e}")
        
        return details
    
    def parse_structured_data(self, data):
        """Parse structured data"""
        return {
            'name': data.get('name', ''),
            'address': data.get('address', {}).get('streetAddress', '') if isinstance(data.get('address'), dict) else '',
            'city': data.get('address', {}).get('addressLocality', '') if isinstance(data.get('address'), dict) else '',
            'state': data.get('address', {}).get('addressRegion', '') if isinstance(data.get('address'), dict) else '',
            'zip': data.get('address', {}).get('postalCode', '') if isinstance(data.get('address'), dict) else '',
            'phone': data.get('telephone', ''),
            'email': data.get('email', ''),
            'website': data.get('url', ''),
            'type': 'structured_data'
        }
    
    def save_operators(self, operators):
        """Save operator results"""
        # Calculate statistics
        total_properties = 0
        operators_with_contact = 0
        operators_with_properties = 0
        
        for op in operators:
            if op.get('phone') or op.get('email'):
                operators_with_contact += 1
            if op.get('properties') or op.get('property_count'):
                operators_with_properties += 1
                if op.get('property_count'):
                    try:
                        total_properties += int(op['property_count'])
                    except:
                        pass
        
        output = {
            'source': 'TROHN Operators Directory',
            'url': 'https://trohn.org/operators/',
            'extraction_date': time.strftime('%Y-%m-%d'),
            'statistics': {
                'total_operators': len(operators),
                'operators_with_contact_info': operators_with_contact,
                'operators_with_properties': operators_with_properties,
                'estimated_total_properties': total_properties
            },
            'operators': operators
        }
        
        with open('/Users/benweiss/Code/narr_extractor/trohn_operators_detailed.json', 'w') as f:
            json.dump(output, f, indent=2)
        
        print(f"\n=== Operators Extraction Complete ===")
        print(f"Total operators found: {len(operators)}")
        print(f"Operators with contact info: {operators_with_contact}")
        print(f"Operators with properties: {operators_with_properties}")
        print(f"Estimated total properties: {total_properties}")
        print(f"\nResults saved to: trohn_operators_detailed.json")
        
        return output

if __name__ == "__main__":
    extractor = TROHNOperatorsExtractor()
    operators = extractor.extract_operators_detailed()
    extractor.save_operators(operators)