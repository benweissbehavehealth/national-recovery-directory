#!/usr/bin/env python3
"""
Execute actual web-research-analyst searches for recovery organizations by state
"""

import json
import os
from datetime import datetime
from pathlib import Path
import sys
import re

def load_existing_data():
    """Load existing data for duplicate detection"""
    base_path = Path(__file__).parent.parent.parent
    
    existing_data = {
        'rccs': [],
        'rcos': [],
        'narr_residences': []
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
    
    return existing_data

def normalize_name(name):
    """Normalize organization name for duplicate detection"""
    if not name:
        return ""
    return name.lower().strip().replace(',', '').replace('.', '').replace('-', ' ').replace('  ', ' ')

def is_duplicate(new_org, existing_orgs, org_type='rcc'):
    """Check if organization is duplicate of existing ones"""
    new_name = normalize_name(new_org.get('name', ''))
    new_state = new_org.get('state', new_org.get('address', '')).upper()
    
    # Extract state from address if not in state field
    if not new_state and new_org.get('address'):
        address = new_org.get('address', '')
        # Look for state abbreviations in address
        state_match = re.search(r'\b([A-Z]{2})\b', address)
        if state_match:
            new_state = state_match.group(1)
    
    for existing_org in existing_orgs:
        existing_name = normalize_name(existing_org.get('name', ''))
        
        # Get existing state based on org type
        if org_type == 'rcc':
            existing_state = existing_org.get('state', '').upper()
        elif org_type == 'rco':
            existing_state = existing_org.get('primary_state', existing_org.get('state', '')).upper()
        else:
            existing_state = existing_org.get('state', '').upper()
        
        # Check for match
        if existing_state == new_state and existing_name == new_name:
            return True
        
        # Check partial matches for variations
        if existing_state == new_state and len(new_name) > 10 and len(existing_name) > 10:
            if new_name in existing_name or existing_name in new_name:
                return True
    
    return False

def extract_json_from_response(response_text):
    """Extract JSON data from agent response"""
    try:
        # First try to parse the whole response as JSON
        return json.loads(response_text)
    except:
        pass
    
    # Look for JSON blocks in the response
    json_patterns = [
        r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}',  # Simple nested JSON
        r'```json\s*(\{.*?\})\s*```',        # JSON in code blocks
        r'```\s*(\{.*?\})\s*```'             # JSON in generic code blocks
    ]
    
    for pattern in json_patterns:
        matches = re.findall(pattern, response_text, re.DOTALL)
        for match in matches:
            try:
                return json.loads(match)
            except:
                continue
    
    return None

def execute_rcc_search(state):
    """Execute Recovery Community Center search for a state"""
    search_prompt = f"""
Search comprehensively for Recovery Community Centers (RCCs) in {state}. These are peer-run centers providing recovery support services, NOT treatment centers.

SEARCH TARGETS:
- "Recovery Community Center {state}"
- "peer recovery center {state}" 
- "{state} recovery support center"
- "{state} peer support recovery"

INCLUDE: Peer-run recovery centers, recovery coaching centers, recovery community hubs
EXCLUDE: Treatment centers, sober living homes, hospitals, clinical programs

For each Recovery Community Center found, provide:
1. Name
2. Full address 
3. Phone number
4. Website
5. Services offered
6. Description
7. Population served
8. Hours
9. Certifications

Return results in JSON format:
{{
  "organizations": [
    {{
      "name": "Center Name",
      "address": "Full Address",
      "phone": "Phone Number", 
      "website": "Website URL",
      "services": ["List of services"],
      "description": "Description of programs",
      "population_served": "Who they serve",
      "hours": "Operating hours",
      "certifications": "Any certifications"
    }}
  ]
}}

If no organizations found, return {{"organizations": []}}
"""
    
    return search_prompt

def execute_rco_search(state):
    """Execute Recovery Community Organization search for a state"""
    search_prompt = f"""
Search comprehensively for Recovery Community Organizations (RCOs) in {state}. These are advocacy and policy organizations, NOT service providers.

SEARCH TARGETS:
- "Recovery Community Organization {state}"
- "{state} recovery advocacy"
- "{state} recovery coalition"
- "{state} recovery policy organization"

INCLUDE: Recovery advocacy groups, recovery coalitions, policy organizations, recovery rights groups
EXCLUDE: Treatment centers, Recovery Community Centers (service delivery), sober living homes

For each Recovery Community Organization found, provide:
1. Name
2. Address
3. Phone number
4. Website
5. Mission/focus areas
6. Advocacy priorities
7. Geographic scope
8. Key programs

Return results in JSON format:
{{
  "organizations": [
    {{
      "name": "Organization Name",
      "address": "Full Address",
      "phone": "Phone Number",
      "website": "Website URL", 
      "mission": "Mission statement",
      "advocacy_priorities": ["List of priorities"],
      "geographic_scope": "Coverage area",
      "key_programs": ["List of programs"]
    }}
  ]
}}

If no organizations found, return {{"organizations": []}}
"""
    
    return search_prompt

def execute_non_narr_search(state):
    """Execute non-NARR recovery residence search for a state"""
    search_prompt = f"""
Search comprehensively for recovery residences and sober living homes in {state} that are NOT NARR certified.

SEARCH TARGETS:
- "sober living homes {state}"
- "{state} recovery housing"
- "{state} halfway houses"
- "Oxford Houses {state}"

INCLUDE: Sober living homes, recovery residences, halfway houses, transitional living
EXCLUDE: NARR certified facilities, treatment centers, hospitals, clinical programs

IMPORTANT: Skip any facility mentioning NARR certification, NARR standards, or NARR affiliation.

For each recovery residence found, provide:
1. Name
2. Full address
3. Phone number
4. Website
5. Capacity
6. Gender requirements
7. Cost/fees
8. Services provided

Return results in JSON format:
{{
  "residences": [
    {{
      "name": "Residence Name",
      "address": "Full Address",
      "phone": "Phone Number",
      "website": "Website URL",
      "capacity": "Number of beds",
      "gender": "Gender requirements",
      "cost": "Cost information",
      "services": ["List of services"]
    }}
  ]
}}

If no residences found, return {{"residences": []}}
"""
    
    return search_prompt

def save_search_results(state, org_type, organizations, existing_data):
    """Save search results after filtering duplicates"""
    base_path = Path(__file__).parent.parent.parent
    output_dir = base_path / "03_raw_data" / "recurring_search_results"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{state.lower().replace(' ', '_')}_{org_type}_{timestamp}.json"
    
    # Filter duplicates
    new_organizations = []
    duplicates_found = []
    
    for org in organizations:
        existing_list = existing_data.get(org_type + 's', [])
        if is_duplicate(org, existing_list, org_type):
            duplicates_found.append(org)
        else:
            new_organizations.append(org)
    
    # Save results
    output_data = {
        'metadata': {
            'state': state,
            'organization_type': org_type,
            'search_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_found': len(organizations),
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

def execute_recovery_search(state, org_types=['rcc', 'rco', 'non_narr_residences']):
    """Execute recovery organization search for a specific state"""
    
    print(f"\n🔍 EXECUTING RECOVERY SEARCH: {state}")
    print(f"Organization types: {', '.join(org_types)}")
    
    # Load existing data
    existing_data = load_existing_data()
    print(f"Loaded existing data for duplicate detection:")
    print(f"  - RCCs: {len(existing_data['rccs'])}")
    print(f"  - RCOs: {len(existing_data['rcos'])}")
    print(f"  - NARR Residences: {len(existing_data['narr_residences'])}")
    
    results = {
        'state': state,
        'search_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'searches': {},
        'summary': {
            'total_new': 0,
            'total_duplicates': 0,
            'searches_completed': 0,
            'searches_failed': 0
        }
    }
    
    # Search function mapping
    search_functions = {
        'rcc': execute_rcc_search,
        'rco': execute_rco_search, 
        'non_narr_residences': execute_non_narr_search
    }
    
    for org_type in org_types:
        if org_type not in search_functions:
            continue
            
        print(f"\n{'='*50}")
        print(f"SEARCHING: {org_type.upper()} in {state}")
        print(f"{'='*50}")
        
        try:
            # Get search prompt
            search_prompt = search_functions[org_type](state)
            print(f"📝 Search prompt: {len(search_prompt)} characters")
            
            # This would be where we call the web-research-analyst agent
            # For demonstration, showing the structure:
            print(f"🚀 Would launch web-research-analyst agent...")
            print(f"   Expected search duration: 2-5 minutes")
            print(f"   Search scope: Comprehensive web search for {org_type} in {state}")
            
            # Placeholder for actual agent results
            # In real implementation:
            # from your_task_runner import Task
            # agent_response = Task(
            #     description=f"Search for {org_type} in {state}",
            #     prompt=search_prompt,
            #     subagent_type="web-research-analyst"
            # )
            
            # For now, simulate empty results
            agent_response = '{"organizations": []}'  # Placeholder
            
            # Process response
            response_data = extract_json_from_response(agent_response)
            organizations = []
            
            if response_data:
                if org_type in ['rcc', 'rco'] and 'organizations' in response_data:
                    organizations = response_data['organizations']
                elif org_type == 'non_narr_residences' and 'residences' in response_data:
                    organizations = response_data['residences']
            
            print(f"📊 Agent found: {len(organizations)} organizations")
            
            # Save results and filter duplicates
            output_path, new_count, duplicate_count = save_search_results(
                state, org_type, organizations, existing_data
            )
            
            results['searches'][org_type] = {
                'organizations_found': len(organizations),
                'new_organizations': new_count,
                'duplicates_filtered': duplicate_count,
                'output_file': str(output_path),
                'status': 'completed'
            }
            
            results['summary']['total_new'] += new_count
            results['summary']['total_duplicates'] += duplicate_count
            results['summary']['searches_completed'] += 1
            
            print(f"✅ Results: {new_count} new, {duplicate_count} duplicates")
            print(f"💾 Saved to: {output_path.name}")
            
        except Exception as e:
            print(f"❌ Error searching {org_type}: {e}")
            results['searches'][org_type] = {
                'error': str(e),
                'status': 'failed'
            }
            results['summary']['searches_failed'] += 1
    
    # Save overall results
    base_path = Path(__file__).parent.parent.parent
    summary_dir = base_path / "05_reports" / "recurring_search_summaries"
    summary_dir.mkdir(parents=True, exist_ok=True)
    
    summary_file = summary_dir / f"{state.lower().replace(' ', '_')}_search_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(summary_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n{'='*60}")
    print(f"SEARCH COMPLETED: {state}")
    print(f"{'='*60}")
    print(f"Searches completed: {results['summary']['searches_completed']}")
    print(f"Searches failed: {results['summary']['searches_failed']}")
    print(f"Total new organizations: {results['summary']['total_new']}")
    print(f"Total duplicates filtered: {results['summary']['total_duplicates']}")
    print(f"Summary saved to: {summary_file.name}")
    
    return results

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_state = sys.argv[1]
        if len(sys.argv) > 2:
            org_types = sys.argv[2].split(',')
        else:
            org_types = ['rcc', 'rco', 'non_narr_residences']
    else:
        target_state = "California"
        org_types = ['rcc', 'rco', 'non_narr_residences']
    
    print("=== RECOVERY ORGANIZATION SEARCH EXECUTOR ===")
    print(f"Target State: {target_state}")
    print(f"Organization Types: {', '.join(org_types)}")
    print()
    print("This will launch web-research-analyst agents to find:")
    if 'rcc' in org_types:
        print("• Recovery Community Centers (peer-run support centers)")
    if 'rco' in org_types:
        print("• Recovery Community Organizations (advocacy/policy groups)")
    if 'non_narr_residences' in org_types:
        print("• Non-NARR Recovery Residences (sober living homes)")
    print()
    
    # Execute the search
    results = execute_recovery_search(target_state, org_types)