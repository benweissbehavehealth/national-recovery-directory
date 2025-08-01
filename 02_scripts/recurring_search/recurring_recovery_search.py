#!/usr/bin/env python3
"""
Recurring search agent for discovering additional recovery organizations by state
Searches for: Recovery Community Centers, Recovery Community Organizations, Non-NARR Recovery Residences
"""

import json
import os
from datetime import datetime
from pathlib import Path
import time

def load_existing_data():
    """Load existing data for duplicate detection"""
    base_path = Path(__file__).parent.parent.parent
    
    existing_data = {
        'rccs': [],
        'rcos': [],
        'narr_residences': [],
        'treatment_centers': []
    }
    
    # Load RCCs
    rcc_path = base_path / "04_processed_data" / "master_directories" / "recovery_community_centers_master.json"
    if rcc_path.exists():
        with open(rcc_path, 'r') as f:
            rcc_data = json.load(f)
            existing_data['rccs'] = rcc_data.get('all_centers', [])
    
    # Load RCOs
    rco_path = base_path / "04_processed_data" / "master_directories" / "recovery_organizations_master.json"
    if rco_path.exists():
        with open(rco_path, 'r') as f:
            rco_data = json.load(f)
            existing_data['rcos'] = rco_data.get('all_organizations', [])
    
    # Load NARR residences
    narr_path = base_path / "04_processed_data" / "master_directories" / "master_directory.json"
    if narr_path.exists():
        with open(narr_path, 'r') as f:
            narr_data = json.load(f)
            if 'organizations_by_state' in narr_data:
                for state_orgs in narr_data['organizations_by_state'].values():
                    existing_data['narr_residences'].extend(state_orgs)
    
    # Load treatment centers for context
    treatment_path = base_path / "04_processed_data" / "master_directories" / "treatment_centers_master.json"
    if treatment_path.exists():
        with open(treatment_path, 'r') as f:
            treatment_data = json.load(f)
            existing_data['treatment_centers'] = treatment_data.get('all_facilities', [])[:1000]  # Sample for context
    
    return existing_data

def create_search_prompts():
    """Create specialized search prompts for each organization type"""
    
    rcc_search_prompt = """
    Search for Recovery Community Centers (RCCs) in {state}. These are peer-run centers that provide recovery support services.
    
    SEARCH CRITERIA:
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
    
    For each organization found, extract:
    1. Name
    2. Full address
    3. Phone number
    4. Website
    5. Services offered
    6. Description of programs
    7. Population served
    8. Hours of operation
    9. Any certification/affiliations
    10. Social media links
    
    Focus on organizations that specifically provide peer support, recovery coaching, meetings, social activities, and community building rather than clinical treatment.
    
    Provide results in JSON format with detailed information for each center found.
    """
    
    rco_search_prompt = """
    Search for Recovery Community Organizations (RCOs) in {state}. These are advocacy and organizing groups focused on policy and systemic change, distinct from service delivery.
    
    SEARCH CRITERIA:
    - Recovery Community Organizations
    - Recovery advocacy groups
    - Recovery coalitions
    - Recovery policy organizations
    - Recovery rights organizations
    - Addiction recovery advocacy
    - Recovery community mobilization groups
    - Recovery organizing coalitions
    
    EXCLUDE:
    - Treatment centers or service providers
    - Recovery Community Centers (peer support service delivery)
    - Sober living homes
    - Clinical programs
    
    For each organization found, extract:
    1. Name
    2. Full address (or P.O. Box)
    3. Phone number
    4. Website
    5. Mission/focus areas
    6. Advocacy priorities
    7. Policy work
    8. Geographic scope (local, state, regional, national)
    9. Membership information
    10. Key programs or initiatives
    
    Focus on organizations that do advocacy, policy work, organizing, mobilization, and systemic change rather than direct service delivery.
    
    Provide results in JSON format with detailed information for each organization found.
    """
    
    non_narr_residences_prompt = """
    Search for recovery residences and sober living homes in {state} that are NOT NARR certified. These are residential recovery housing options.
    
    SEARCH CRITERIA:
    - Sober living homes
    - Recovery residences
    - Halfway houses
    - Three-quarter houses
    - Transitional living
    - Recovery housing
    - Sober houses
    - Recovery homes
    - Oxford Houses
    - Independent sober living
    
    IMPORTANT: EXCLUDE any facilities that mention:
    - NARR certification
    - NARR standards
    - NARR affiliated
    - NARR accredited
    
    For each residence found, extract:
    1. Name
    2. Full address
    3. Phone number
    4. Website
    5. House rules/requirements
    6. Capacity (number of beds)
    7. Gender requirements (men/women/co-ed)
    8. Cost/fees
    9. Length of stay requirements
    10. Services provided
    11. Any certifications or affiliations (non-NARR)
    
    Focus on legitimate recovery housing that provides structured, alcohol and drug-free living environments.
    
    Provide results in JSON format with detailed information for each residence found.
    """
    
    return {
        'rcc': rcc_search_prompt,
        'rco': rco_search_prompt,
        'non_narr_residences': non_narr_residences_prompt
    }

def create_state_list():
    """Create list of US states and territories for systematic searching"""
    return [
        'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
        'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho',
        'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana',
        'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota',
        'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada',
        'New Hampshire', 'New Jersey', 'New Mexico', 'New York',
        'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon',
        'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
        'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
        'West Virginia', 'Wisconsin', 'Wyoming', 'Washington DC',
        'Puerto Rico', 'US Virgin Islands', 'Guam', 'American Samoa'
    ]

def normalize_organization_name(name):
    """Normalize organization name for duplicate detection"""
    if not name:
        return ""
    return name.lower().strip().replace(',', '').replace('.', '').replace('-', ' ').replace('  ', ' ')

def is_duplicate_organization(new_org, existing_orgs, org_type):
    """Check if organization is a duplicate of existing ones"""
    new_name = normalize_organization_name(new_org.get('name', ''))
    new_state = new_org.get('state', new_org.get('address', {}).get('state', '')).upper()
    
    for existing_org in existing_orgs:
        existing_name = normalize_organization_name(existing_org.get('name', ''))
        
        # Get existing state based on org type
        existing_state = ''
        if org_type == 'rcc':
            existing_state = existing_org.get('state', '').upper()
        elif org_type == 'rco':
            existing_state = existing_org.get('primary_state', existing_org.get('state', '')).upper()
        else:  # NARR residences
            existing_state = existing_org.get('state', '').upper()
        
        # Check for match
        if existing_state == new_state and existing_name == new_name:
            return True
        
        # Check partial matches for variations
        if existing_state == new_state and len(new_name) > 10 and len(existing_name) > 10:
            if new_name in existing_name or existing_name in new_name:
                return True
    
    return False

def save_search_results(state, org_type, results, existing_data):
    """Save search results with duplicate detection"""
    base_path = Path(__file__).parent.parent.parent
    output_dir = base_path / "03_raw_data" / "recurring_search_results"
    output_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{state.lower().replace(' ', '_')}_{org_type}_{timestamp}.json"
    
    # Filter out duplicates
    new_organizations = []
    duplicates_found = []
    
    if results and isinstance(results, list):
        for org in results:
            if is_duplicate_organization(org, existing_data.get(org_type + 's', []), org_type):
                duplicates_found.append(org)
            else:
                new_organizations.append(org)
    
    # Save results
    output_data = {
        'metadata': {
            'state': state,
            'organization_type': org_type,
            'search_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_found': len(results) if results else 0,
            'new_organizations': len(new_organizations),
            'duplicates_filtered': len(duplicates_found)
        },
        'new_organizations': new_organizations,
        'duplicates_filtered': duplicates_found
    }
    
    output_path = output_dir / filename
    with open(output_path, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    return output_path, len(new_organizations), len(duplicates_found)

def run_recurring_search(target_states=None, organization_types=None):
    """Run the recurring search for specified states and organization types"""
    base_path = Path(__file__).parent.parent.parent
    
    # Load existing data for duplicate detection
    print("Loading existing organization data...")
    existing_data = load_existing_data()
    print(f"Loaded existing data:")
    print(f"  - RCCs: {len(existing_data['rccs'])}")
    print(f"  - RCOs: {len(existing_data['rcos'])}")
    print(f"  - NARR Residences: {len(existing_data['narr_residences'])}")
    
    # Get search prompts
    prompts = create_search_prompts()
    
    # Default to all states and org types if not specified
    if target_states is None:
        target_states = create_state_list()
    if organization_types is None:
        organization_types = ['rcc', 'rco', 'non_narr_residences']
    
    print(f"\nStarting recurring search:")
    print(f"  - States: {len(target_states)} states")
    print(f"  - Organization types: {organization_types}")
    
    total_results = {
        'states_searched': 0,
        'searches_completed': 0,
        'new_organizations_found': 0,
        'duplicates_filtered': 0,
        'by_type': {}
    }
    
    for org_type in organization_types:
        total_results['by_type'][org_type] = {
            'new_found': 0,
            'duplicates': 0
        }
    
    # Process each state
    for state_idx, state in enumerate(target_states):
        print(f"\n{'='*60}")
        print(f"SEARCHING STATE: {state} ({state_idx + 1}/{len(target_states)})")
        print(f"{'='*60}")
        
        state_results = {
            'state': state,
            'searches': {},
            'total_new': 0,
            'total_duplicates': 0
        }
        
        # Search for each organization type in this state
        for org_type in organization_types:
            print(f"\nSearching for {org_type.upper()} in {state}...")
            
            # Format the search prompt for this state
            search_prompt = prompts[org_type].format(state=state)
            
            try:
                # Here we would call the web-research-analyst agent
                # For now, we'll create the framework and show how it would work
                print(f"  Launching web-research-analyst for {org_type} in {state}...")
                print(f"  Search prompt prepared ({len(search_prompt)} characters)")
                
                # Placeholder for agent results
                # In actual implementation, this would call:
                # results = launch_web_research_agent(search_prompt)
                results = []  # Placeholder
                
                # Save results and check for duplicates
                output_path, new_count, duplicate_count = save_search_results(
                    state, org_type, results, existing_data
                )
                
                state_results['searches'][org_type] = {
                    'new_found': new_count,
                    'duplicates': duplicate_count,
                    'output_file': str(output_path)
                }
                
                state_results['total_new'] += new_count
                state_results['total_duplicates'] += duplicate_count
                
                total_results['searches_completed'] += 1
                total_results['new_organizations_found'] += new_count
                total_results['duplicates_filtered'] += duplicate_count
                total_results['by_type'][org_type]['new_found'] += new_count
                total_results['by_type'][org_type]['duplicates'] += duplicate_count
                
                print(f"  ✅ {org_type.upper()}: {new_count} new, {duplicate_count} duplicates")
                
                # Small delay to be respectful of resources
                time.sleep(2)
                
            except Exception as e:
                print(f"  ❌ Error searching {org_type} in {state}: {e}")
                state_results['searches'][org_type] = {
                    'error': str(e),
                    'new_found': 0,
                    'duplicates': 0
                }
        
        total_results['states_searched'] += 1
        
        print(f"\n{state} Summary:")
        print(f"  New organizations: {state_results['total_new']}")
        print(f"  Duplicates filtered: {state_results['total_duplicates']}")
    
    # Save overall summary
    summary_path = base_path / "05_reports" / f"recurring_search_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(summary_path, 'w') as f:
        json.dump(total_results, f, indent=2)
    
    print(f"\n{'='*70}")
    print(f"RECURRING SEARCH COMPLETED")
    print(f"{'='*70}")
    print(f"States Searched: {total_results['states_searched']}")
    print(f"Total Searches: {total_results['searches_completed']}")
    print(f"New Organizations Found: {total_results['new_organizations_found']}")
    print(f"Duplicates Filtered: {total_results['duplicates_filtered']}")
    print(f"\nBreakdown by Type:")
    for org_type, counts in total_results['by_type'].items():
        print(f"  {org_type.upper()}: {counts['new_found']} new, {counts['duplicates']} duplicates")
    print(f"\nSummary saved to: {summary_path}")
    
    return total_results

if __name__ == "__main__":
    # Example: Search first 3 states for all organization types
    test_states = ['California', 'New York', 'Florida']
    
    print("=== RECURRING RECOVERY SEARCH AGENT ===")
    print("This agent will search systematically for additional recovery organizations")
    print("that may not be in our current database.")
    print()
    print("Organization types:")
    print("- Recovery Community Centers (RCCs)")
    print("- Recovery Community Organizations (RCOs)")
    print("- Non-NARR Recovery Residences")
    print()
    
    # Run test search
    results = run_recurring_search(target_states=test_states)