#!/usr/bin/env python3
"""
Michigan MARR (Michigan Alliance for Recovery Residences) - Comprehensive Operator Extraction

This script systematically extracts ALL certified recovery residence operators from 
Michigan MARR's complete operator database, focusing on operating organizations 
rather than individual facilities.

Target: https://michiganarr.com/full-operator-list
Objective: Extract 60-70+ operators with complete organizational and contact data
"""

import requests
from bs4 import BeautifulSoup
import json
import re
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MichiganMARRExtractor:
    def __init__(self):
        self.base_url = "https://michiganarr.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.operators = []
        self.extraction_metadata = {
            'source': 'Michigan MARR (Michigan Alliance for Recovery Residences)',
            'url': 'https://michiganarr.com/full-operator-list',
            'extraction_date': datetime.now().isoformat(),
            'total_operators_found': 0,
            'data_completeness_score': 0.0,
            'extraction_method': 'Automated web scraping with systematic parsing'
        }

    def fetch_page(self, url, max_retries=3):
        """Fetch a web page with retry logic"""
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching: {url} (attempt {attempt + 1})")
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Failed to fetch {url} after {max_retries} attempts")
                    return None

    def parse_operator_data(self, soup):
        """Parse the full operator list page to extract all certified operators"""
        operators = []
        
        # Look for the main content area containing operator listings
        content_area = soup.find('div', class_='entry-content') or soup.find('main') or soup
        
        if not content_area:
            logger.warning("Could not find main content area")
            return operators

        # Parse operators organized by county
        current_county = None
        operator_blocks = []
        
        # Find all text blocks and paragraphs that contain operator information
        all_elements = content_area.find_all(['h2', 'h3', 'h4', 'p', 'div'], text=True)
        
        for element in all_elements:
            text = element.get_text(strip=True)
            
            # Check if this is a county header
            if re.match(r'^[A-Z][a-z]+ County', text):
                current_county = text
                logger.info(f"Processing county: {current_county}")
                continue
            
            # Look for operator entries (typically start with organization name)
            if self.is_operator_entry(text):
                operator_data = self.extract_operator_details(element, current_county)
                if operator_data:
                    operators.append(operator_data)
                    logger.info(f"Extracted operator: {operator_data.get('name', 'Unknown')}")
        
        # Alternative parsing method - look for structured lists
        if not operators:
            operators = self.parse_structured_list(content_area)
        
        return operators

    def is_operator_entry(self, text):
        """Determine if a text block represents an operator entry"""
        # Look for patterns that indicate operator entries
        patterns = [
            r'^[A-Z][A-Za-z\s&,.-]+(?:Recovery|Residence|House|Home|Center|Services|Inc\.|LLC)',
            r'Level [1-4]',
            r'Phone:?\s*\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.]?[0-9]{4}',
            r'Contact:?\s*[A-Za-z]+\s+[A-Za-z]+',
        ]
        
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        return False

    def extract_operator_details(self, element, county):
        """Extract detailed information for a single operator"""
        operator = {
            'name': '',
            'dba_names': [],
            'address': {
                'street': '',
                'city': '',
                'state': 'Michigan',
                'zip_code': '',
                'county': county or ''
            },
            'contact': {
                'phone': '',
                'email': '',
                'website': '',
                'contact_person': ''
            },
            'certification': {
                'marr_certified': True,
                'certification_levels': [],
                'narr_standards_version': '3.0'
            },
            'services': {
                'housing_type': '',
                'housing_levels': [],
                'capacity': '',
                'target_population': '',
                'mat_acceptance': False,
                'felony_friendly': False,
                'ada_accessible': False
            },
            'program_details': {
                'description': '',
                'rates': '',
                'funding_sources': []
            },
            'metadata': {
                'data_source': 'Michigan MARR Full Operator List',
                'extraction_date': datetime.now().isoformat(),
                'data_completeness': 0.0
            }
        }
        
        # Get the text content and surrounding context
        text_content = element.get_text()
        
        # Look for next siblings to get complete operator information
        current_element = element
        full_text = text_content
        
        # Collect related text from nearby elements
        for _ in range(5):  # Look ahead up to 5 elements
            next_element = current_element.find_next_sibling()
            if next_element and next_element.get_text(strip=True):
                next_text = next_element.get_text(strip=True)
                if len(next_text) > 200 or self.is_operator_entry(next_text):
                    break  # Likely next operator
                full_text += " " + next_text
                current_element = next_element
            else:
                break
        
        # Extract organization name (usually the first meaningful line)
        name_match = re.search(r'^([A-Z][A-Za-z\s&,.-]+(?:Recovery|Residence|House|Home|Center|Services|Inc\.|LLC)[A-Za-z\s&,.-]*)', full_text)
        if name_match:
            operator['name'] = name_match.group(1).strip()
        else:
            # Fallback - use first significant line
            lines = full_text.split('\n')
            for line in lines:
                if len(line.strip()) > 3 and not re.match(r'^(Phone|Email|Level|Contact)', line):
                    operator['name'] = line.strip()
                    break
        
        # Extract contact information
        phone_match = re.search(r'Phone:?\s*(\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.]?[0-9]{4})', full_text)
        if phone_match:
            operator['contact']['phone'] = phone_match.group(1)
        
        email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', full_text)
        if email_match:
            operator['contact']['email'] = email_match.group(1)
        
        # Extract contact person
        contact_match = re.search(r'Contact:?\s*([A-Za-z]+\s+[A-Za-z]+)', full_text)
        if contact_match:
            operator['contact']['contact_person'] = contact_match.group(1)
        
        # Extract certification levels
        level_matches = re.findall(r'Level ([1-4])', full_text)
        if level_matches:
            operator['certification']['certification_levels'] = [f"Level {level}" for level in level_matches]
            operator['services']['housing_levels'] = operator['certification']['certification_levels']
        
        # Extract housing type
        housing_types = ['Single Family', 'Apartment', 'Congregate', 'Transitional']
        for housing_type in housing_types:
            if housing_type.lower() in full_text.lower():
                operator['services']['housing_type'] = housing_type
                break
        
        # Extract special characteristics
        if re.search(r'MAT\s*(friendly|accept)', full_text, re.IGNORECASE):
            operator['services']['mat_acceptance'] = True
        
        if re.search(r'felony\s*friendly', full_text, re.IGNORECASE):
            operator['services']['felony_friendly'] = True
        
        if re.search(r'ADA\s*(accessible|compliant)', full_text, re.IGNORECASE):
            operator['services']['ada_accessible'] = True
        
        # Extract program description and rates
        description_patterns = [
            r'Program:?\s*(.{20,200})',
            r'Description:?\s*(.{20,200})',
            r'Services:?\s*(.{20,200})'
        ]
        
        for pattern in description_patterns:
            desc_match = re.search(pattern, full_text, re.IGNORECASE)
            if desc_match:
                operator['program_details']['description'] = desc_match.group(1).strip()
                break
        
        # Extract rates/fees
        rate_match = re.search(r'Rate:?\s*\$?([0-9,]+)', full_text)
        if rate_match:
            operator['program_details']['rates'] = f"${rate_match.group(1)}"
        
        # Calculate data completeness score
        operator['metadata']['data_completeness'] = self.calculate_completeness(operator)
        
        return operator if operator['name'] else None

    def parse_structured_list(self, content_area):
        """Alternative parsing method for structured operator lists"""
        operators = []
        
        # Look for table structures or organized lists
        tables = content_area.find_all('table')
        lists = content_area.find_all(['ul', 'ol'])
        
        # Parse tables if present
        for table in tables:
            rows = table.find_all('tr')
            for row in rows[1:]:  # Skip header row
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 3:
                    operator = self.parse_table_row(cells)
                    if operator:
                        operators.append(operator)
        
        # Parse lists if present
        for list_element in lists:
            items = list_element.find_all('li')
            for item in items:
                operator = self.parse_list_item(item)
                if operator:
                    operators.append(operator)
        
        return operators

    def parse_table_row(self, cells):
        """Parse operator data from table row"""
        # Implementation for table-based data extraction
        pass

    def parse_list_item(self, item):
        """Parse operator data from list item"""
        # Implementation for list-based data extraction
        pass

    def calculate_completeness(self, operator):
        """Calculate data completeness score for an operator"""
        total_fields = 15
        completed_fields = 0
        
        if operator['name']: completed_fields += 1
        if operator['contact']['phone']: completed_fields += 1
        if operator['contact']['email']: completed_fields += 1
        if operator['contact']['website']: completed_fields += 1
        if operator['contact']['contact_person']: completed_fields += 1
        if operator['address']['city']: completed_fields += 1
        if operator['address']['county']: completed_fields += 1
        if operator['certification']['certification_levels']: completed_fields += 1
        if operator['services']['housing_type']: completed_fields += 1
        if operator['services']['housing_levels']: completed_fields += 1
        if operator['program_details']['description']: completed_fields += 1
        if operator['program_details']['rates']: completed_fields += 1
        if operator['services']['mat_acceptance']: completed_fields += 1
        if operator['services']['felony_friendly']: completed_fields += 1
        if operator['services']['ada_accessible']: completed_fields += 1
        
        return round(completed_fields / total_fields, 2)

    def extract_all_operators(self):
        """Main extraction method - extract all operators from Michigan MARR"""
        logger.info("Starting Michigan MARR operator extraction...")
        
        # Fetch the full operator list page
        response = self.fetch_page(f"{self.base_url}/full-operator-list")
        if not response:
            logger.error("Failed to fetch operator list page")
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract all operator data
        self.operators = self.parse_operator_data(soup)
        
        # Update metadata
        self.extraction_metadata['total_operators_found'] = len(self.operators)
        if self.operators:
            completeness_scores = [op['metadata']['data_completeness'] for op in self.operators]
            self.extraction_metadata['data_completeness_score'] = round(
                sum(completeness_scores) / len(completeness_scores), 2
            )
        
        logger.info(f"Extraction complete. Found {len(self.operators)} operators")
        return self.operators

    def save_results(self, filename="tier1_michigan_marr_data.json"):
        """Save extraction results to JSON file"""
        output_data = {
            'metadata': self.extraction_metadata,
            'operators': self.operators,
            'summary': {
                'total_operators': len(self.operators),
                'counties_covered': len(set(op['address']['county'] for op in self.operators if op['address']['county'])),
                'certification_levels_found': list(set(
                    level for op in self.operators 
                    for level in op['certification']['certification_levels']
                )),
                'average_completeness': self.extraction_metadata['data_completeness_score']
            }
        }
        
        filepath = f"/Users/benweiss/Code/narr_extractor/{filename}"
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Results saved to {filepath}")
        return filepath

def main():
    """Main execution function"""
    extractor = MichiganMARRExtractor()
    
    try:
        # Extract all operators
        operators = extractor.extract_all_operators()
        
        if operators:
            # Save results
            output_file = extractor.save_results()
            
            # Print summary
            print(f"\n{'='*60}")
            print("MICHIGAN MARR OPERATOR EXTRACTION COMPLETE")
            print(f"{'='*60}")
            print(f"Total Operators Found: {len(operators)}")
            print(f"Average Data Completeness: {extractor.extraction_metadata['data_completeness_score']*100:.1f}%")
            print(f"Output File: {output_file}")
            
            # Show sample of extracted operators
            print(f"\nSample of Extracted Operators:")
            for i, operator in enumerate(operators[:5]):
                print(f"{i+1}. {operator['name']}")
                if operator['contact']['phone']:
                    print(f"   Phone: {operator['contact']['phone']}")
                if operator['certification']['certification_levels']:
                    print(f"   Levels: {', '.join(operator['certification']['certification_levels'])}")
                print()
        
        else:
            print("No operators found. Check logs for details.")
            
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise

if __name__ == "__main__":
    main()