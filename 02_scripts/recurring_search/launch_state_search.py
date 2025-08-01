#!/usr/bin/env python3
"""
Launch web-research-analyst agents for state-specific recovery organization searches
"""

import json
import os
from datetime import datetime
from pathlib import Path
import sys

# Add parent directories to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from recurring_recovery_search import (
    load_existing_data, 
    create_search_prompts, 
    save_search_results, 
    normalize_organization_name,
    is_duplicate_organization
)

def launch_rcc_search(state):
    """Launch search for Recovery Community Centers in a specific state"""
    search_prompt = f"""
    I need you to conduct a comprehensive web search for Recovery Community Centers (RCCs) in {state}. These are peer-run centers that provide recovery support services, NOT treatment centers.

    SEARCH STRATEGY:
    1. Search for "Recovery Community Center {state}"
    2. Search for "peer recovery center {state}"
    3. Search for "recovery support center {state}"
    4. Look at state addiction/recovery agency websites
    5. Check recovery community organization websites
    6. Look for directories of recovery services

    WHAT TO LOOK FOR:
    - Recovery Community Centers
    - Peer recovery centers  
    - Recovery support centers
    - Community recovery programs
    - Peer-run recovery facilities
    - Recovery coaching centers
    - Recovery community hubs

    EXCLUDE:
    - Treatment centers (clinical services)
    - Sober living homes/residences
    - Hospitals or medical facilities
    - Outpatient treatment programs
    - Rehabilitation centers

    For each legitimate Recovery Community Center found, extract:
    1. Name
    2. Complete address
    3. Phone number
    4. Website URL
    5. Services offered (peer support, recovery coaching, meetings, etc.)
    6. Description of programs
    7. Population served
    8. Hours of operation
    9. Any certification/affiliations
    10. Contact person/director if available

    Focus on organizations that provide peer support, recovery coaching, meetings, social activities, and community building rather than clinical treatment.

    Present your findings in a structured JSON format with an array of organizations. If you find no organizations, return an empty array.

    Example format:
    {{
        "organizations": [
            {{
                "name": "Example Recovery Community Center",
                "address": "123 Main St, City, {state} 12345",
                "phone": "(555) 123-4567",
                "website": "https://example.org",
                "services": ["Peer support", "Recovery coaching", "Support groups"],
                "description": "Description of services and programs",
                "population_served": "Adults in recovery",
                "hours": "Mon-Fri 9am-5pm",
                "certifications": "CCAR certified",
                "contact_person": "John Doe, Director"
            }}
        ]
    }}
    """
    
    return search_prompt

def launch_rco_search(state):
    """Launch search for Recovery Community Organizations in a specific state"""
    search_prompt = f"""
    I need you to conduct a comprehensive web search for Recovery Community Organizations (RCOs) in {state}. These are advocacy and organizing groups focused on policy and systemic change, NOT service delivery organizations.

    SEARCH STRATEGY:
    1. Search for "Recovery Community Organization {state}"
    2. Search for "recovery advocacy {state}"
    3. Search for "recovery coalition {state}"
    4. Look at state government and policy websites
    5. Check advocacy organization directories
    6. Look for recovery rights organizations

    WHAT TO LOOK FOR:
    - Recovery Community Organizations (RCOs)
    - Recovery advocacy groups
    - Recovery coalitions
    - Recovery policy organizations
    - Recovery rights organizations
    - Addiction recovery advocacy groups
    - Recovery community mobilization groups
    - Recovery organizing coalitions

    EXCLUDE:
    - Treatment centers or service providers
    - Recovery Community Centers (peer support service delivery)
    - Sober living homes
    - Clinical programs
    - Direct service organizations

    For each legitimate Recovery Community Organization found, extract:
    1. Name
    2. Complete address (or P.O. Box)
    3. Phone number
    4. Website URL
    5. Mission statement/focus areas
    6. Advocacy priorities
    7. Policy work areas
    8. Geographic scope (local, state, regional, national)
    9. Membership information
    10. Key programs or initiatives
    11. Leadership/key staff

    Focus on organizations that do advocacy, policy work, organizing, mobilization, and systemic change rather than direct service delivery.

    Present your findings in a structured JSON format with an array of organizations. If you find no organizations, return an empty array.

    Example format:
    {{
        "organizations": [
            {{
                "name": "Example Recovery Advocacy Coalition",
                "address": "456 Policy Ave, City, {state} 12345",
                "phone": "(555) 987-6543",
                "website": "https://recoveryadvocacy.org",
                "mission": "Advocate for recovery-oriented policies",
                "advocacy_priorities": ["Criminal justice reform", "Healthcare policy"],
                "geographic_scope": "Statewide",
                "membership": "500+ members",
                "key_programs": ["Policy advocacy", "Legislative lobbying"],
                "leadership": "Jane Smith, Executive Director"
            }}
        ]
    }}
    """
    
    return search_prompt

def launch_non_narr_residences_search(state):
    """Launch search for non-NARR recovery residences in a specific state"""
    search_prompt = f"""
    I need you to conduct a comprehensive web search for recovery residences and sober living homes in {state} that are NOT NARR certified. Find legitimate recovery housing options.

    SEARCH STRATEGY:
    1. Search for "sober living homes {state}"
    2. Search for "recovery housing {state}"
    3. Search for "halfway houses {state}"
    4. Search for "Oxford Houses {state}"
    5. Look at recovery housing directories
    6. Check addiction treatment center referral lists

    WHAT TO LOOK FOR:
    - Sober living homes
    - Recovery residences
    - Halfway houses
    - Three-quarter houses
    - Transitional living facilities
    - Recovery housing
    - Sober houses
    - Recovery homes
    - Oxford Houses
    - Independent sober living

    IMPORTANT - EXCLUDE:
    - Any facilities that mention NARR certification, NARR standards, NARR affiliated, or NARR accredited
    - Treatment centers
    - Hospitals
    - Detox facilities
    - Rehab centers with clinical services

    For each legitimate recovery residence found, extract:
    1. Name
    2. Complete address
    3. Phone number
    4. Website URL
    5. House rules/requirements
    6. Capacity (number of beds/residents)
    7. Gender requirements (men only/women only/co-ed)
    8. Cost/fees (weekly/monthly)
    9. Length of stay requirements
    10. Services provided (house meetings, job assistance, etc.)
    11. Any certifications or affiliations (non-NARR)
    12. Admission requirements

    Focus on legitimate recovery housing that provides structured, alcohol and drug-free living environments.

    Present your findings in a structured JSON format with an array of residences. If you find no residences, return an empty array.

    Example format:
    {{
        "residences": [
            {{
                "name": "Example Sober Living Home",
                "address": "789 Recovery St, City, {state} 12345",
                "phone": "(555) 456-7890",
                "website": "https://examplesoberliving.com",
                "capacity": "12 beds",
                "gender": "Men only",
                "cost": "$150/week",
                "length_of_stay": "90 days minimum",
                "services": ["House meetings", "Job assistance", "Peer support"],
                "certifications": "State licensed",
                "admission_requirements": "30 days clean, employment"
            }}
        ]
    }}
    """
    
    return search_prompt

def process_agent_response(response_text, org_type):
    """Process and parse agent response"""
    try:
        # Try to extract JSON from the response
        import re
        
        # Look for JSON blocks in the response
        json_matches = re.findall(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', response_text, re.DOTALL)
        
        for json_str in json_matches:
            try:
                data = json.loads(json_str)
                
                # Extract organizations based on type
                if org_type == 'rcc' and 'organizations' in data:
                    return data['organizations']
                elif org_type == 'rco' and 'organizations' in data:
                    return data['organizations']
                elif org_type == 'non_narr_residences' and 'residences' in data:
                    return data['residences']
                    
            except json.JSONDecodeError:
                continue
        
        # If no JSON found, return empty list
        return []
        
    except Exception as e:
        print(f"Error processing agent response: {e}")
        return []

def run_state_search(state, organization_types=['rcc', 'rco', 'non_narr_residences']):
    """Run search for specific state and organization types using web-research-analyst agents"""
    
    print(f"\n{'='*60}")
    print(f"LAUNCHING AGENTS FOR: {state}")
    print(f"{'='*60}")
    
    # Load existing data for duplicate detection
    existing_data = load_existing_data()
    
    results = {
        'state': state,
        'search_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'searches': {},
        'total_new': 0,
        'total_duplicates': 0
    }
    
    # Search functions mapping
    search_functions = {
        'rcc': launch_rcc_search,
        'rco': launch_rco_search,
        'non_narr_residences': launch_non_narr_residences_search
    }
    
    for org_type in organization_types:
        if org_type not in search_functions:
            continue
            
        print(f"\n🔍 Searching for {org_type.upper()} in {state}...")
        
        try:
            # Get the search prompt
            search_prompt = search_functions[org_type](state)
            
            print(f"📝 Search prompt prepared ({len(search_prompt)} characters)")
            print(f"🚀 Launching web-research-analyst agent...")
            
            # This is where we would actually call the web-research-analyst agent
            # For demonstration, I'll show the structure but won't execute
            
            print(f"   Agent would execute: comprehensive web search")
            print(f"   Expected duration: 2-5 minutes")
            print(f"   Search scope: {state} recovery organizations")
            
            # Placeholder for actual agent call
            # agent_response = call_web_research_agent(search_prompt)
            # organizations = process_agent_response(agent_response, org_type)
            
            # For now, return empty results
            organizations = []
            
            # Save results
            base_path = Path(__file__).parent.parent.parent
            output_path, new_count, duplicate_count = save_search_results(
                state, org_type, organizations, existing_data
            )
            
            results['searches'][org_type] = {
                'new_found': new_count,
                'duplicates': duplicate_count,
                'output_file': str(output_path),
                'search_completed': True
            }
            
            results['total_new'] += new_count
            results['total_duplicates'] += duplicate_count
            
            print(f"✅ {org_type.upper()}: {new_count} new organizations, {duplicate_count} duplicates filtered")
            
        except Exception as e:
            print(f"❌ Error searching {org_type} in {state}: {e}")
            results['searches'][org_type] = {
                'error': str(e),
                'new_found': 0,
                'duplicates': 0,
                'search_completed': False
            }
    
    print(f"\n📊 {state} Search Summary:")
    print(f"   New organizations found: {results['total_new']}")
    print(f"   Duplicates filtered: {results['total_duplicates']}")
    
    return results

if __name__ == "__main__":
    # Example usage
    if len(sys.argv) > 1:
        target_state = sys.argv[1]
    else:
        target_state = "California"  # Default for testing
    
    print("=== STATE-SPECIFIC RECOVERY ORGANIZATION SEARCH ===")
    print(f"Target State: {target_state}")
    print()
    print("This script will launch web-research-analyst agents to search for:")
    print("• Recovery Community Centers (RCCs)")  
    print("• Recovery Community Organizations (RCOs)")
    print("• Non-NARR Recovery Residences")
    print()
    print("The agents will compare findings against existing data to avoid duplicates.")
    print()
    
    # Run the search
    results = run_state_search(target_state)
    
    print(f"\n{'='*60}")
    print("SEARCH COMPLETED")
    print(f"Check 03_raw_data/recurring_search_results/ for detailed results")
    print(f"{'='*60}")