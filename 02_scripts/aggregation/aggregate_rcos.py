#!/usr/bin/env python3
"""
Aggregate all Recovery Community Organization (RCO) extraction results
"""

import json
import os
from datetime import datetime
from pathlib import Path

def load_json(filepath):
    """Load JSON file if it exists"""
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            return json.load(f)
    return None

def extract_organizations(data, source_type):
    """Extract organizations from various JSON structures"""
    orgs = []
    
    # Check for various organization keys
    org_keys = [
        "organizations",
        "recovery_community_organizations", 
        "rcos",
        "national_organizations",
        "state_organizations",
        "regional_organizations",
        "specialized_recovery_organizations",
        "national_coalitions",
        "multi_state_coalitions",
        "metropolitan_area_rcos",
        "border_region_rcos"
    ]
    
    # Extract from any matching keys
    for key in org_keys:
        if key in data:
            if isinstance(data[key], list):
                orgs.extend(data[key])
            elif isinstance(data[key], dict):
                # Handle nested structure
                for sub_key, sub_data in data[key].items():
                    if isinstance(sub_data, list):
                        orgs.extend(sub_data)
    
    # If no organizations found and data is a list
    if not orgs and isinstance(data, list):
        orgs = data
    
    # Add source type to each org
    for org in orgs:
        org["source_category"] = source_type
    
    return orgs

def aggregate_rcos():
    # Base paths
    base_path = Path(__file__).parent.parent.parent
    rco_base = base_path / "03_raw_data" / "recovery_organizations"
    
    # Initialize aggregated data
    aggregated = {
        "extraction_date": datetime.now().strftime("%Y-%m-%d"),
        "data_type": "Recovery Community Organizations (RCOs)",
        "description": "Advocacy and organizing groups distinct from service delivery centers",
        "total_organizations": 0,
        "organizations_by_category": {},
        "organizations_by_state": {},
        "all_organizations": []
    }
    
    # Track unique ID assignment
    next_id = 1
    
    # Process each category
    categories = {
        "national_networks": "National Recovery Advocacy Networks",
        "state_coalitions": "State-Level RCO Coalitions", 
        "specialized": "Specialized Population RCOs",
        "regional": "Regional/Multi-State RCO Networks"
    }
    
    for folder, category_name in categories.items():
        print(f"\nProcessing {category_name}...")
        category_path = rco_base / folder
        
        if category_path.exists():
            aggregated["organizations_by_category"][category_name] = []
            
            for json_file in category_path.glob("*.json"):
                print(f"  Loading {json_file.name}...")
                data = load_json(json_file)
                
                if data:
                    orgs = extract_organizations(data, category_name)
                    
                    for org in orgs:
                        # Assign unique ID
                        org["id"] = f"RCO_{next_id:04d}"
                        org["category"] = category_name
                        
                        # Standardize state field
                        if "states_served" in org and isinstance(org["states_served"], list):
                            org["primary_state"] = org["states_served"][0] if org["states_served"] else "National"
                        elif "state" not in org:
                            org["primary_state"] = org.get("states_served", "National")
                        else:
                            org["primary_state"] = org.get("state", "National")
                        
                        aggregated["all_organizations"].append(org)
                        aggregated["organizations_by_category"][category_name].append(org["id"])
                        next_id += 1
                    
                    print(f"    Added {len(orgs)} organizations from {json_file.name}")
    
    # Organize by state
    for org in aggregated["all_organizations"]:
        states = []
        
        # Handle various state field formats
        if "states_served" in org and isinstance(org["states_served"], list):
            states = org["states_served"]
        elif "states_covered" in org and isinstance(org["states_covered"], list):
            states = org["states_covered"]
        elif "primary_state" in org:
            states = [org["primary_state"]]
        elif "state" in org:
            states = [org["state"]]
        
        # Add to each state served
        for state in states:
            if state not in aggregated["organizations_by_state"]:
                aggregated["organizations_by_state"][state] = []
            aggregated["organizations_by_state"][state].append({
                "id": org["id"],
                "name": org.get("name", org.get("organization_name", "")),
                "category": org["category"]
            })
    
    # Calculate totals
    aggregated["total_organizations"] = len(aggregated["all_organizations"])
    aggregated["states_with_rcos"] = len(aggregated["organizations_by_state"])
    aggregated["categories_count"] = len(aggregated["organizations_by_category"])
    
    # Generate summary
    aggregated["summary"] = {
        "total_rcos": aggregated["total_organizations"],
        "by_category": {
            cat: len(orgs) for cat, orgs in aggregated["organizations_by_category"].items()
        },
        "top_states": sorted(
            [(state, len(orgs)) for state, orgs in aggregated["organizations_by_state"].items()],
            key=lambda x: x[1],
            reverse=True
        )[:15],
        "multi_state_coverage": len([
            org for org in aggregated["all_organizations"] 
            if isinstance(org.get("states_served", []), list) and len(org.get("states_served", [])) > 1
        ])
    }
    
    # Save aggregated data
    output_path = base_path / "04_processed_data" / "master_directories" / "recovery_organizations_master.json"
    with open(output_path, 'w') as f:
        json.dump(aggregated, f, indent=2)
    
    print(f"\n{'='*60}")
    print(f"AGGREGATION COMPLETE!")
    print(f"{'='*60}")
    print(f"Total Recovery Community Organizations: {aggregated['total_organizations']}")
    print(f"States with RCOs: {aggregated['states_with_rcos']}")
    print(f"\nBreakdown by category:")
    for cat, count in aggregated['summary']['by_category'].items():
        print(f"  - {cat}: {count}")
    print(f"\nMulti-state organizations: {aggregated['summary']['multi_state_coverage']}")
    print(f"\nSaved to: {output_path}")
    
    # Generate report
    generate_summary_report(aggregated, base_path)
    
    return aggregated

def generate_summary_report(data, base_path):
    """Generate markdown summary report"""
    report = f"""# Recovery Community Organizations (RCOs) - Aggregation Summary

**Generated**: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Overview
Recovery Community Organizations focus on advocacy, policy, and community organizing rather than direct service delivery.

- **Total RCOs**: {data['total_organizations']}
- **States with RCOs**: {data['states_with_rcos']}
- **Categories**: {data['categories_count']}

## Breakdown by Category
"""
    
    for cat, count in data['summary']['by_category'].items():
        report += f"- **{cat}**: {count} organizations\n"
    
    report += f"\n## Geographic Coverage\n"
    report += f"- **Multi-state organizations**: {data['summary']['multi_state_coverage']}\n"
    report += f"- **National organizations**: {len([org for org in data['all_organizations'] if org.get('primary_state') == 'National'])}\n"
    
    report += "\n## Top 15 States by RCO Count\n"
    for state, count in data['summary']['top_states']:
        report += f"- **{state}**: {count} organizations\n"
    
    report += "\n## Key Distinctions\n"
    report += "- RCOs focus on **advocacy and policy** rather than service delivery\n"
    report += "- Most are **peer-led organizations** with people in recovery in leadership\n"
    report += "- They work to **mobilize recovery communities** for systemic change\n"
    report += "- Many operate at state, regional, or national levels\n"
    
    # Save report
    report_path = base_path / "05_reports" / "analysis" / "rco_aggregation_summary.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, 'w') as f:
        f.write(report)
    
    print(f"Summary report saved to: {report_path}")

if __name__ == "__main__":
    aggregate_rcos()