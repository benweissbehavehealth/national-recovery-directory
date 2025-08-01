#!/usr/bin/env python3
"""
Arizona Recovery Housing Alliance (AzRHA) Provider Extractor using Selenium
Extracts all certified recovery residences by interacting with GetHelp widget
"""

import json
import time
import logging
from typing import Dict, List, Any
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AzRHASeleniumExtractor:
    def __init__(self, headless: bool = True):
        """Initialize the Selenium driver"""
        self.options = Options()
        if headless:
            self.options.add_argument("--headless")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--window-size=1920,1080")
        
        # Try to use Chrome driver
        try:
            self.driver = webdriver.Chrome(options=self.options)
            logging.info("Chrome driver initialized successfully")
        except:
            logging.error("Chrome driver not found. Please install ChromeDriver")
            raise
            
        self.wait = WebDriverWait(self.driver, 20)
        self.providers = []
        
    def load_search_page(self):
        """Load the AzRHA certified home search page"""
        url = "https://myazrha.org/azrha-certified-home-search"
        logging.info(f"Loading search page: {url}")
        
        self.driver.get(url)
        time.sleep(5)  # Allow page to fully load
        
        # Wait for GetHelp widget to load
        try:
            # Look for GetHelp widget iframe or container
            widget_selectors = [
                "iframe[src*='gethelp']",
                "div[id*='gethelp']",
                "div[class*='gethelp']",
                ".gethelp-widget",
                "#gethelp-widget"
            ]
            
            widget_found = False
            for selector in widget_selectors:
                try:
                    self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                    logging.info(f"GetHelp widget found with selector: {selector}")
                    widget_found = True
                    break
                except TimeoutException:
                    continue
                    
            if not widget_found:
                logging.warning("GetHelp widget not found, checking for alternative elements")
                
        except Exception as e:
            logging.error(f"Error waiting for widget: {str(e)}")
            
    def extract_providers_from_widget(self):
        """Extract provider data from the GetHelp widget"""
        providers = []
        
        try:
            # Check if widget is in iframe
            iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
            
            for iframe in iframes:
                src = iframe.get_attribute("src")
                if src and "gethelp" in src.lower():
                    logging.info(f"Switching to GetHelp iframe: {src}")
                    self.driver.switch_to.frame(iframe)
                    break
                    
            # Wait for provider listings to load
            time.sleep(5)
            
            # Try multiple selectors for provider cards
            provider_selectors = [
                ".provider-card",
                ".facility-card",
                ".listing-item",
                "[class*='provider']",
                "[class*='facility']",
                ".result-item",
                ".search-result"
            ]
            
            provider_elements = []
            for selector in provider_selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    logging.info(f"Found {len(elements)} providers with selector: {selector}")
                    provider_elements = elements
                    break
                    
            if not provider_elements:
                logging.warning("No provider elements found in widget")
                
                # Try to extract any visible text that might contain provider info
                body_text = self.driver.find_element(By.TAG_NAME, "body").text
                logging.info(f"Page body preview: {body_text[:500]}...")
                
            else:
                # Extract data from each provider
                for element in provider_elements:
                    try:
                        provider_data = self.extract_provider_details(element)
                        if provider_data:
                            providers.append(provider_data)
                    except Exception as e:
                        logging.error(f"Error extracting provider: {str(e)}")
                        
        except Exception as e:
            logging.error(f"Error in widget extraction: {str(e)}")
            
        finally:
            # Switch back to main content
            self.driver.switch_to.default_content()
            
        return providers
        
    def extract_provider_details(self, element) -> Dict[str, Any]:
        """Extract details from a single provider element"""
        provider = {}
        
        try:
            # Try to extract name
            name_selectors = [".provider-name", ".facility-name", "h3", "h4", ".title"]
            for selector in name_selectors:
                try:
                    name_elem = element.find_element(By.CSS_SELECTOR, selector)
                    provider["name"] = name_elem.text.strip()
                    break
                except:
                    pass
                    
            # Try to extract address
            address_selectors = [".address", ".location", "[class*='address']"]
            for selector in address_selectors:
                try:
                    addr_elem = element.find_element(By.CSS_SELECTOR, selector)
                    provider["address"] = addr_elem.text.strip()
                    break
                except:
                    pass
                    
            # Try to extract phone
            phone_selectors = [".phone", ".tel", "a[href^='tel:']"]
            for selector in phone_selectors:
                try:
                    phone_elem = element.find_element(By.CSS_SELECTOR, selector)
                    provider["phone"] = phone_elem.text.strip()
                    break
                except:
                    pass
                    
            # Try to extract any other visible text
            provider["raw_text"] = element.text.strip()
            
        except Exception as e:
            logging.error(f"Error extracting provider details: {str(e)}")
            
        return provider if provider else None
        
    def scroll_and_load_more(self):
        """Scroll to load more results if pagination exists"""
        try:
            # Scroll to bottom
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            # Look for "Load More" button
            load_more_selectors = [
                "button[contains(text(),'Load More')]",
                "button[contains(text(),'Show More')]",
                ".load-more",
                ".show-more"
            ]
            
            for selector in load_more_selectors:
                try:
                    button = self.driver.find_element(By.XPATH, f"//{selector}")
                    button.click()
                    time.sleep(3)
                    return True
                except:
                    pass
                    
        except Exception as e:
            logging.error(f"Error scrolling: {str(e)}")
            
        return False
        
    def extract_all_providers(self) -> List[Dict[str, Any]]:
        """Main extraction method"""
        all_providers = []
        
        try:
            self.load_search_page()
            
            # Initial extraction
            providers = self.extract_providers_from_widget()
            all_providers.extend(providers)
            
            # Try to load more results
            while self.scroll_and_load_more():
                new_providers = self.extract_providers_from_widget()
                if new_providers:
                    all_providers.extend(new_providers)
                else:
                    break
                    
        except Exception as e:
            logging.error(f"Extraction failed: {str(e)}")
            
        finally:
            self.driver.quit()
            
        return all_providers
        
    def save_results(self, providers: List[Dict[str, Any]], filename: str):
        """Save extracted data to JSON file"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump({
                "extraction_date": time.strftime("%Y-%m-%d %H:%M:%S"),
                "source": "Arizona Recovery Housing Alliance (AzRHA)",
                "extraction_method": "Selenium WebDriver",
                "total_providers": len(providers),
                "providers": providers
            }, f, indent=2, ensure_ascii=False)
            
        logging.info(f"Saved {len(providers)} providers to {filename}")

def main():
    """Main extraction function"""
    extractor = AzRHASeleniumExtractor(headless=True)
    
    logging.info("Starting AzRHA provider extraction with Selenium...")
    
    # Extract all providers
    providers = extractor.extract_all_providers()
    
    if providers:
        # Save results
        extractor.save_results(providers, "/Users/benweiss/Code/narr_extractor/azrha_selenium_results.json")
        
        logging.info(f"\n=== EXTRACTION SUMMARY ===")
        logging.info(f"Total providers extracted: {len(providers)}")
    else:
        logging.error("No providers extracted. The widget may require manual interaction.")
        
        # Try alternative approach
        logging.info("\nAttempting alternative data collection approach...")
        
        # Save what we learned about the page structure
        page_analysis = {
            "extraction_date": time.strftime("%Y-%m-%d %H:%M:%S"),
            "source": "Arizona Recovery Housing Alliance (AzRHA)",
            "findings": {
                "widget_type": "GetHelp SDK",
                "api_key": "35d896ff2e06b09f4e5bef6e8e32cf7faa8c97e4",
                "accreditation_id": 27,
                "total_beds_claimed": "1,678+",
                "access_method": "JavaScript widget requiring browser interaction",
                "recommendations": [
                    "Contact AzRHA directly for provider list",
                    "Use member portal if available",
                    "Check Arizona Department of Health Services database",
                    "Request API access from GetHelp"
                ]
            },
            "contact_info": {
                "membership_email": "membership@myazrha.org",
                "inspector_email": "inspector@myazrha.org"
            }
        }
        
        with open("/Users/benweiss/Code/narr_extractor/azrha_analysis.json", 'w') as f:
            json.dump(page_analysis, f, indent=2)
            
        logging.info("Saved page analysis to azrha_analysis.json")

if __name__ == "__main__":
    main()