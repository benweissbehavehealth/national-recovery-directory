import requests
import json
import time
from urllib.parse import urlencode

class TROHNAjaxSearch:
    def __init__(self):
        self.base_url = "https://trohn.org"
        self.ajax_url = "https://trohn.org/wp-admin/admin-ajax.php"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://trohn.org/find/'
        })
        
    def search_housing(self, location="", gender="", population=""):
        """Search for housing using AJAX endpoint"""
        print(f"\nSearching TROHN housing directory...")
        
        # Common WordPress AJAX actions for search
        ajax_actions = [
            'rhl_search',
            'search_housing',
            'get_housing',
            'housing_search',
            'recovery_housing_search',
            'wpdg_recovery_housing_search',
            'trohn_search',
            'locator_search'
        ]
        
        results = []
        
        for action in ajax_actions:
            print(f"Trying action: {action}")
            
            data = {
                'action': action,
                'rhl-location': location,
                'rhl-genders': gender,
                'rhl-populations': population,
                'location': location,
                'gender': gender,
                'population': population
            }
            
            try:
                response = self.session.post(self.ajax_url, data=data)
                
                if response.status_code == 200:
                    content = response.text.strip()
                    
                    # Check if it's JSON
                    if content.startswith('{') or content.startswith('['):
                        try:
                            json_data = json.loads(content)
                            if json_data and not isinstance(json_data, dict) or (isinstance(json_data, dict) and 'error' not in json_data):
                                print(f"  Success! Found data with action: {action}")
                                results.append({
                                    'action': action,
                                    'data': json_data
                                })
                        except:
                            pass
                    
                    # Check if it's HTML with results
                    elif '<' in content and len(content) > 100:
                        print(f"  Found HTML response with action: {action}")
                        results.append({
                            'action': action,
                            'html': content
                        })
                        
            except Exception as e:
                print(f"  Error with {action}: {e}")
        
        return results
    
    def search_all_cities(self):
        """Search major Texas cities"""
        major_cities = [
            'Austin', 'Houston', 'Dallas', 'San Antonio', 'Fort Worth',
            'El Paso', 'Arlington', 'Corpus Christi', 'Plano', 'Lubbock'
        ]
        
        all_results = []
        
        for city in major_cities:
            print(f"\n=== Searching {city} ===")
            results = self.search_housing(location=city)
            if results:
                all_results.extend(results)
                # Save intermediate results
                with open(f'/Users/benweiss/Code/narr_extractor/trohn_ajax_{city.lower()}.json', 'w') as f:
                    json.dump(results, f, indent=2)
            time.sleep(1)  # Be respectful
        
        return all_results
    
    def test_form_submission(self):
        """Test direct form submission"""
        print("\nTesting direct form submission...")
        
        # Simulate form submission
        form_data = {
            'rhl-location': '',
            'rhl-genders': '',
            'rhl-populations': ''
        }
        
        # Try GET request (since form uses GET method)
        response = self.session.get(f"{self.base_url}/find/", params=form_data)
        
        with open('/Users/benweiss/Code/narr_extractor/trohn_form_response.html', 'w') as f:
            f.write(response.text)
        
        print("Form response saved to trohn_form_response.html")
        
        # Check if results are in the response
        if 'rhl-search-results' in response.text or 'result' in response.text.lower():
            print("Found search results in response!")
            return response.text
        else:
            print("No search results found in form response")
            return None

if __name__ == "__main__":
    searcher = TROHNAjaxSearch()
    
    # Test AJAX search
    ajax_results = searcher.search_housing()
    
    if ajax_results:
        with open('/Users/benweiss/Code/narr_extractor/trohn_ajax_results.json', 'w') as f:
            json.dump(ajax_results, f, indent=2)
        print(f"\nFound {len(ajax_results)} AJAX responses")
    
    # Test form submission
    form_result = searcher.test_form_submission()
    
    # Try searching specific cities
    # city_results = searcher.search_all_cities()