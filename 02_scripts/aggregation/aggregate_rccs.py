#!/usr/bin/env python3
"""
Aggregate all Recovery Community Center (RCC) extraction results into comprehensive directory
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

def aggregate_rccs():
    # Base path for RCC data
    base_path = Path(__file__).parent.parent.parent
    rcc_base = base_path / "03_raw_data" / "recovery_centers"
    
    # Initialize aggregated data structure
    aggregated = {
        "extraction_date": datetime.now().strftime("%Y-%m-%d"),
        "data_type": "Recovery Community Centers",
        "total_centers": 0,
        "sources": [],
        "centers_by_region": {},
        "centers_by_state": {},
        "all_centers": []
    }
    
    # Track unique ID assignment
    next_id = 1
    
    # Process SAMHSA-funded centers
    print("Processing SAMHSA-funded RCCs...")
    samhsa_data = load_json(rcc_base / "samhsa_funded" / "samhsa_recovery_community_centers.json")
    if samhsa_data and "recovery_community_centers" in samhsa_data:
        aggregated["sources"].append("SAMHSA Recovery Community Services Program")
        for center in samhsa_data["recovery_community_centers"]:
            center["id"] = f"RCC_{next_id:04d}"
            center["funding_source"] = "SAMHSA RCSP"
            center["region"] = determine_region(center.get("state", ""))
            aggregated["all_centers"].append(center)
            next_id += 1
        print(f"  Added {len(samhsa_data['recovery_community_centers'])} SAMHSA-funded centers")
    
    # Process regional extractions
    regional_files = {
        "southwest": "southwest_recovery_community_centers.json",
        "west_coast": "west_coast_recovery_community_centers.json",
        "midwest": "midwest_recovery_community_centers.json",
        "northeast": "northeast_recovery_community_centers.json",
        "southeast": "southeast_recovery_community_centers.json"
    }
    
    for region, filename in regional_files.items():
        filepath = rcc_base / "regional" / filename
        if filepath.exists():
            print(f"Processing {region} RCCs...")
            data = load_json(filepath)
            if data and "recovery_community_centers" in data:
                region_name = region.replace("_", " ").title()
                aggregated["centers_by_region"][region_name] = []
                
                for center in data["recovery_community_centers"]:
                    # Avoid duplicates from SAMHSA list
                    if not is_duplicate(center, aggregated["all_centers"]):
                        center["id"] = f"RCC_{next_id:04d}"
                        center["region"] = region_name
                        aggregated["all_centers"].append(center)
                        aggregated["centers_by_region"][region_name].append(center["id"])
                        next_id += 1
                
                added = len(aggregated["centers_by_region"][region_name])
                print(f"  Added {added} centers from {region_name}")
    
    # Process national networks
    print("Processing national network RCCs...")
    national_data = load_json(rcc_base / "national_networks" / "recovery_community_centers_directory.json")
    if national_data and "recovery_community_centers" in national_data:
        aggregated["sources"].append("National Recovery Advocacy Networks")
        duplicates = 0
        for center in national_data["recovery_community_centers"]:
            if not is_duplicate(center, aggregated["all_centers"]):
                center["id"] = f"RCC_{next_id:04d}"
                center["affiliation"] = "National Network"
                aggregated["all_centers"].append(center)
                next_id += 1
            else:
                duplicates += 1
        added = len(national_data["recovery_community_centers"]) - duplicates
        print(f"  Added {added} centers from national networks ({duplicates} duplicates avoided)")
    
    # Organize by state
    for center in aggregated["all_centers"]:
        state = center.get("state", "Unknown")
        if state not in aggregated["centers_by_state"]:
            aggregated["centers_by_state"][state] = []
        aggregated["centers_by_state"][state].append({
            "id": center["id"],
            "name": center.get("name", ""),
            "city": center.get("city", center.get("address", ""))
        })
    
    # Calculate totals
    aggregated["total_centers"] = len(aggregated["all_centers"])
    aggregated["states_covered"] = len(aggregated["centers_by_state"])
    aggregated["regions_covered"] = len(aggregated["centers_by_region"])
    
    # Generate summary statistics
    aggregated["summary"] = {
        "total_centers": aggregated["total_centers"],
        "states_with_centers": aggregated["states_covered"],
        "centers_by_region_count": {
            region: len(centers) for region, centers in aggregated["centers_by_region"].items()
        },
        "top_states": sorted(
            [(state, len(centers)) for state, centers in aggregated["centers_by_state"].items()],
            key=lambda x: x[1],
            reverse=True
        )[:10]
    }
    
    # Save aggregated data
    output_path = base_path / "04_processed_data" / "master_directories" / "recovery_community_centers_master.json"
    with open(output_path, 'w') as f:
        json.dump(aggregated, f, indent=2)
    
    print(f"\nAggregation complete!")
    print(f"Total Recovery Community Centers: {aggregated['total_centers']}")
    print(f"States covered: {aggregated['states_covered']}")
    print(f"Saved to: {output_path}")
    
    # Generate summary report
    summary_path = base_path / "05_reports" / "analysis" / "rcc_aggregation_summary.md"
    generate_summary_report(aggregated, summary_path)
    
    return aggregated

def determine_region(state):
    """Determine region based on state"""
    regions = {
        "northeast": ["ME", "NH", "VT", "MA", "RI", "CT", "NY", "NJ", "PA"],
        "southeast": ["DE", "MD", "DC", "VA", "WV", "NC", "SC", "GA", "FL", "KY", "TN", "AL", "MS"],
        "midwest": ["OH", "IN", "IL", "MI", "WI", "MN", "IA", "MO", "ND", "SD", "NE", "KS"],
        "southwest": ["TX", "OK", "AR", "LA", "NM", "AZ"],
        "west_coast": ["CA", "OR", "WA", "NV", "ID", "UT", "CO", "MT", "WY", "AK", "HI"]
    }
    
    for region, states in regions.items():
        if state.upper() in states:
            return region.replace("_", " ").title()
    return "Unknown"

def is_duplicate(center, existing_centers):
    """Check if center is likely a duplicate based on name and location"""
    center_name = center.get("name", "").lower().strip()
    center_city = center.get("city", "").lower().strip()
    center_state = center.get("state", "").lower().strip()
    
    for existing in existing_centers:
        existing_name = existing.get("name", "").lower().strip()
        existing_city = existing.get("city", "").lower().strip()
        existing_state = existing.get("state", "").lower().strip()
        
        # Check for exact name match in same city/state
        if (center_name == existing_name and 
            center_state == existing_state and
            (center_city == existing_city or not center_city or not existing_city)):
            return True
            
        # Check for very similar names in same location
        if (center_state == existing_state and center_city == existing_city and
            similarity_ratio(center_name, existing_name) > 0.85):
            return True
    
    return False

def similarity_ratio(s1, s2):
    """Simple string similarity ratio (0-1)"""
    if not s1 or not s2:
        return 0
    
    # Count matching characters
    matches = sum(c1 == c2 for c1, c2 in zip(s1, s2))
    return matches / max(len(s1), len(s2))

def generate_summary_report(data, output_path):
    """Generate markdown summary report"""
    report = f"""# Recovery Community Centers - Aggregation Summary

**Generated**: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Overview
- **Total Centers**: {data['total_centers']}
- **States Covered**: {data['states_covered']}
- **Data Sources**: {len(data['sources'])}

## Regional Distribution
"""
    
    for region, count in data['summary']['centers_by_region_count'].items():
        report += f"- **{region}**: {count} centers\n"
    
    report += "\n## Top 10 States by Center Count\n"
    for state, count in data['summary']['top_states']:
        report += f"- **{state}**: {count} centers\n"
    
    report += "\n## Data Sources\n"
    for source in data['sources']:
        report += f"- {source}\n"
    
    # Save report
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(report)
    
    print(f"Summary report saved to: {output_path}")

if __name__ == "__main__":
    aggregate_rccs()