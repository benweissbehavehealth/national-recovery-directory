import requests
from bs4 import BeautifulSoup
import json
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import re

class TROHNDynamicExtractor:
    def __init__(self):
        self.base_url = "https://trohn.org"
        self.results = []
        
    def setup_driver(self):
        """Setup Chrome driver with options"""
        from selenium.webdriver.chrome.options import Options
        
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # Run in background
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
        except:
            # Try without specific options if Chrome driver not found
            self.driver = webdriver.Chrome()
            
    def extract_all_residences(self):
        """Main extraction method"""
        print("Starting TROHN dynamic extraction...")
        
        try:
            self.setup_driver()
            
            # Navigate to the find housing page
            self.driver.get(f"{self.base_url}/find/")
            time.sleep(3)  # Wait for page to load
            
            # Try to submit empty search to get all results
            try:
                # Find the search button
                search_button = self.driver.find_element(By.CSS_SELECTOR, 'button[type="submit"], input[type="submit"], button.search-button')
                search_button.click()
                
                # Wait for results to load
                time.sleep(5)
                
                # Extract results
                self.extract_search_results()
                
            except NoSuchElementException:
                print("Could not find search button, trying alternative methods...")
                
                # Try to get all cities and search each one
                self.search_by_cities()
                
        except Exception as e:
            print(f"Error during extraction: {e}")
            
        finally:
            if hasattr(self, 'driver'):
                self.driver.quit()
                
        return self.results
    
    def search_by_cities(self):
        """Search by major Texas cities"""
        major_cities = [
            'Austin', 'Houston', 'Dallas', 'San Antonio', 'Fort Worth',
            'El Paso', 'Arlington', 'Corpus Christi', 'Plano', 'Lubbock',
            'Garland', 'Irving', 'Amarillo', 'Grand Prairie', 'Brownsville',
            'Pasadena', 'Mesquite', 'McKinney', 'McAllen', 'Killeen',
            'Waco', 'Carrollton', 'Beaumont', 'Abilene', 'Denton',
            'Midland', 'Wichita Falls', 'Odessa', 'Round Rock', 'Richardson'
        ]
        
        for city in major_cities:
            print(f"Searching for residences in {city}...")
            try:
                # Navigate to search page
                self.driver.get(f"{self.base_url}/find/")
                time.sleep(2)
                
                # Select city
                location_select = Select(self.driver.find_element(By.NAME, 'rhl-location'))
                location_select.select_by_value(city)
                
                # Submit search
                form = self.driver.find_element(By.ID, 'locator')
                form.submit()
                
                # Wait for results
                time.sleep(3)
                
                # Extract results
                self.extract_search_results()
                
            except Exception as e:
                print(f"Error searching {city}: {e}")
                continue
    
    def extract_search_results(self):
        """Extract results from the current page"""
        try:
            # Wait for results to appear
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '.result, .listing, .property, .recovery-home, .rhl-result, .locator-result'))
            )
            
            # Get page source and parse
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Look for result containers
            result_selectors = [
                '.result', '.listing', '.property', '.recovery-home',
                '.rhl-result', '.locator-result', 'div[class*="result"]',
                'div[class*="listing"]', 'article[class*="recovery"]'
            ]
            
            for selector in result_selectors:
                results = soup.select(selector)
                if results:
                    print(f"Found {len(results)} results with selector: {selector}")
                    for result in results:
                        residence_data = self.parse_residence(result)
                        if residence_data and residence_data not in self.results:
                            self.results.append(residence_data)
                    break
                    
        except TimeoutException:
            print("No results found on current page")
            
            # Try to find any text indicating number of results
            page_text = self.driver.page_source
            if 'No results found' in page_text or 'no properties' in page_text.lower():
                print("No results available for this search")
            else:
                # Save page source for debugging
                with open('/Users/benweiss/Code/narr_extractor/trohn_search_debug.html', 'w') as f:
                    f.write(page_text)
                print("Saved page source for debugging")
    
    def parse_residence(self, container):
        """Parse individual residence data"""
        data = {
            'name': '',
            'address': '',
            'city': '',
            'state': 'TX',
            'zip': '',
            'phone': '',
            'email': '',
            'website': '',
            'certification_level': '',
            'services': [],
            'capacity': '',
            'gender': '',
            'focus_areas': []
        }
        
        # Extract text content
        text = container.get_text(separator=' ', strip=True)
        
        # Try to extract name from heading
        heading = container.find(['h1', 'h2', 'h3', 'h4', 'h5'])
        if heading:
            data['name'] = heading.get_text(strip=True)
        
        # Extract contact info
        # Phone
        phone_match = re.search(r'(\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4})', text)
        if phone_match:
            data['phone'] = phone_match.group(1)
        
        # Email
        email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', text)
        if email_match:
            data['email'] = email_match.group(1)
        
        # Address
        address_match = re.search(r'(\d+\s+[\w\s]+(?:St|Street|Ave|Avenue|Rd|Road|Dr|Drive|Blvd|Boulevard|Ln|Lane|Way|Pkwy|Parkway)\.?)', text)
        if address_match:
            data['address'] = address_match.group(1)
        
        # City and Zip
        city_zip_match = re.search(r'([A-Za-z\s]+),?\s+TX\s+(\d{5})', text)
        if city_zip_match:
            data['city'] = city_zip_match.group(1).strip()
            data['zip'] = city_zip_match.group(2)
        
        # Website
        links = container.find_all('a', href=True)
        for link in links:
            href = link['href']
            if 'http' in href and 'trohn.org' not in href:
                data['website'] = href
                break
        
        return data if data['name'] else None
    
    def save_results(self):
        """Save extraction results"""
        output = {
            'source': 'Texas Recovery Organization Housing Network (TROHN)',
            'url': 'https://trohn.org',
            'extraction_date': time.strftime('%Y-%m-%d'),
            'extraction_method': 'dynamic_search',
            'total_found': len(self.results),
            'residences': self.results
        }
        
        with open('/Users/benweiss/Code/narr_extractor/trohn_dynamic_results.json', 'w') as f:
            json.dump(output, f, indent=2)
        
        print(f"\nExtraction complete!")
        print(f"Total residences found: {len(self.results)}")
        print(f"Results saved to: trohn_dynamic_results.json")
        
        return output

# Alternative approach using the operators page
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
        
        operators = []
        
        # Look for organization posts
        org_posts = soup.find_all('article', class_=re.compile('organization|type-organization'))
        print(f"Found {len(org_posts)} organization posts")
        
        for post in org_posts:
            operator = self.parse_operator_post(post)
            if operator:
                operators.append(operator)
                
                # Try to get more details from individual page
                if 'url' in operator:
                    details = self.get_operator_details(operator['url'])
                    operator.update(details)
        
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
        content = post.find(class_=['fl-post-excerpt', 'entry-content', 'post-content'])
        if content:
            text = content.get_text(separator=' ', strip=True)
            
            # Extract phone
            phone_match = re.search(r'(\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4})', text)
            if phone_match:
                operator['phone'] = phone_match.group(1)
            
            # Extract email
            email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', text)
            if email_match:
                operator['email'] = email_match.group(1)
            
            # Extract address components
            operator['raw_text'] = text
        
        return operator if operator.get('name') else None
    
    def get_operator_details(self, url):
        """Get additional details from operator page"""
        details = {}
        try:
            response = self.session.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for contact information
            content = soup.find(class_=['entry-content', 'post-content', 'content'])
            if content:
                text = content.get_text(separator=' ', strip=True)
                
                # Look for properties/residences
                if 'properties' in text.lower() or 'locations' in text.lower():
                    details['has_multiple_properties'] = True
                
                # Look for certification info
                if 'certified' in text.lower() or 'trohn' in text.lower():
                    details['certification_mentioned'] = True
                    
        except:
            pass
        
        return details
    
    def parse_structured_data(self, data):
        """Parse structured data"""
        return {
            'name': data.get('name', ''),
            'address': data.get('address', {}).get('streetAddress', '') if isinstance(data.get('address'), dict) else '',
            'phone': data.get('telephone', ''),
            'email': data.get('email', ''),
            'website': data.get('url', ''),
            'type': 'structured_data'
        }
    
    def save_operators(self, operators):
        """Save operator results"""
        output = {
            'source': 'TROHN Operators Directory',
            'url': 'https://trohn.org/operators/',
            'extraction_date': time.strftime('%Y-%m-%d'),
            'total_operators': len(operators),
            'operators': operators
        }
        
        with open('/Users/benweiss/Code/narr_extractor/trohn_operators_detailed.json', 'w') as f:
            json.dump(output, f, indent=2)
        
        print(f"\nOperators extraction complete!")
        print(f"Total operators found: {len(operators)}")
        
        return output

if __name__ == "__main__":
    # First try operators extraction
    op_extractor = TROHNOperatorsExtractor()
    operators = op_extractor.extract_operators_detailed()
    op_extractor.save_operators(operators)
    
    # Then try dynamic search
    print("\n\nNote: Dynamic search requires Selenium and Chrome driver.")
    print("If it fails, please install: pip install selenium")
    print("And download ChromeDriver from: https://chromedriver.chromium.org/")
    
    try:
        dyn_extractor = TROHNDynamicExtractor()
        results = dyn_extractor.extract_all_residences()
        dyn_extractor.save_results()
    except Exception as e:
        print(f"Dynamic extraction failed: {e}")
        print("Please check Selenium/ChromeDriver installation")