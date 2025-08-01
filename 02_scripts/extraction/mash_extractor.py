#!/usr/bin/env python3
"""
MASH Certified Recovery Homes Extractor
Extracts all certified recovery homes from Massachusetts Alliance for Sober Housing
"""

import requests
import json
from datetime import datetime
import time

def extract_mash_homes():
    """Extract all certified homes from MASH API"""
    
    # API endpoint discovered from website analysis
    api_url = "https://management.mashsoberhousing.org/api/GetCertifiedHomes"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://mashsoberhousing.org/certified-residences/',
        'Origin': 'https://mashsoberhousing.org'
    }
    
    print(f"Fetching data from MASH API...")
    
    try:
        response = requests.get(api_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Parse the JSON response
        homes_data = response.json()
        
        print(f"Successfully retrieved {len(homes_data)} certified homes")
        
        # Process and structure the data
        processed_homes = []
        
        for home in homes_data:
            # Extract and structure each home's data
            processed_home = {
                "name": home.get("Name", "").strip(),
                "address": {
                    "street": home.get("Address1", "").strip(),
                    "street2": home.get("Address2", "").strip() if home.get("Address2") else "",
                    "city": home.get("CityName", "").strip() if home.get("CityName") else "",
                    "state": "MA",
                    "zip": home.get("Zip", "").strip()
                },
                "region": home.get("Region", "").strip(),
                "service_type": home.get("Home_ServesType", "").strip(),
                "languages": home.get("OtherLanguages", "").strip() if home.get("OtherLanguages") else "",
                "capacity": home.get("Capacity", ""),
                "handicap_accessible": home.get("HandicapAccessible", False),
                "website": home.get("WebsiteAddress", "").strip() if home.get("WebsiteAddress") else "",
                "certification_level": "MASH Certified",
                "weekly_fee": home.get("WeeklyFee", ""),
                "monthly_fee": home.get("MonthlyFee", ""),
                "certification_date": home.get("CertDate", ""),
                "renewal_date": home.get("RenewDate", ""),
                "public_contacts": []
            }
            
            # Process public contacts if available
            if home.get("PublicContacts"):
                contacts = home.get("PublicContacts")
                if isinstance(contacts, list):
                    for contact in contacts:
                        contact_info = {
                            "name": contact.get("Name", "").strip() if isinstance(contact, dict) else "",
                            "phone": contact.get("Phone", "").strip() if isinstance(contact, dict) else "",
                            "email": contact.get("Email", "").strip() if isinstance(contact, dict) else ""
                        }
                        if any(contact_info.values()):
                            processed_home["public_contacts"].append(contact_info)
                elif isinstance(contacts, str):
                    # Handle case where contacts might be a string
                    processed_home["contact_info"] = contacts.strip()
            
            # Add raw data for reference
            processed_home["raw_data"] = home
            
            processed_homes.append(processed_home)
        
        # Sort by name for consistency
        processed_homes.sort(key=lambda x: x["name"])
        
        # Create summary statistics
        summary = {
            "total_homes": len(processed_homes),
            "extraction_date": datetime.now().isoformat(),
            "source": "Massachusetts Alliance for Sober Housing (MASH)",
            "api_endpoint": api_url,
            "by_service_type": {},
            "by_region": {},
            "handicap_accessible_count": sum(1 for h in processed_homes if h["handicap_accessible"])
        }
        
        # Count by service type
        for home in processed_homes:
            service_type = home["service_type"] or "Not Specified"
            summary["by_service_type"][service_type] = summary["by_service_type"].get(service_type, 0) + 1
            
            region = home["region"] or "Not Specified"
            summary["by_region"][region] = summary["by_region"].get(region, 0) + 1
        
        # Create the final output
        output = {
            "summary": summary,
            "certified_homes": processed_homes
        }
        
        return output
        
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None


def save_results(data, filename="mash_certified_homes.json"):
    """Save the extracted data to a JSON file"""
    if data:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"\nData saved to {filename}")
        print(f"Total homes extracted: {data['summary']['total_homes']}")
        print("\nBreakdown by service type:")
        for service_type, count in data['summary']['by_service_type'].items():
            print(f"  {service_type}: {count}")
        print("\nBreakdown by region:")
        for region, count in data['summary']['by_region'].items():
            print(f"  {region}: {count}")
    else:
        print("No data to save")


if __name__ == "__main__":
    print("MASH Certified Recovery Homes Extractor")
    print("="*50)
    
    # Extract the data
    extracted_data = extract_mash_homes()
    
    # Save the results
    if extracted_data:
        save_results(extracted_data)
        
        # Also save a simplified version for easier viewing
        simplified_homes = []
        for home in extracted_data["certified_homes"]:
            # Build complete address
            address_parts = []
            if home['address']['street']:
                address_parts.append(home['address']['street'])
            if home['address']['street2']:
                address_parts.append(home['address']['street2'])
            if home['address']['city']:
                address_parts.append(home['address']['city'])
            address_parts.append(f"MA {home['address']['zip']}")
            
            # Extract primary contact info
            primary_contact = ""
            if home['public_contacts']:
                contact = home['public_contacts'][0]
                contact_parts = []
                if contact.get('name'):
                    contact_parts.append(contact['name'])
                if contact.get('phone'):
                    contact_parts.append(contact['phone'])
                if contact.get('email'):
                    contact_parts.append(contact['email'])
                primary_contact = " | ".join(contact_parts)
            
            simplified_homes.append({
                "name": home["name"],
                "address": ", ".join(address_parts),
                "service_type": home["service_type"],
                "capacity": home["capacity"],
                "region": home["region"],
                "website": home["website"],
                "handicap_accessible": home["handicap_accessible"],
                "primary_contact": primary_contact,
                "weekly_fee": home.get("weekly_fee", ""),
                "monthly_fee": home.get("monthly_fee", "")
            })
        
        with open("mash_certified_homes_simplified.json", 'w', encoding='utf-8') as f:
            json.dump(simplified_homes, f, indent=2, ensure_ascii=False)
        print("\nSimplified data saved to mash_certified_homes_simplified.json")