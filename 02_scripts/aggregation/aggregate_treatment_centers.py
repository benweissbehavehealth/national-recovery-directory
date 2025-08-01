#!/usr/bin/env python3
"""
Aggregate all Treatment Center extraction results (Outpatient, Residential, Inpatient)
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

def extract_facilities(data, source_name):
    """Extract facilities from various JSON structures"""
    facilities = []
    
    # Check for various possible keys
    facility_keys = [
        "facilities",
        "treatment_centers",
        "outpatient_facilities",
        "providers",
        "organizations",
        "centers"
    ]
    
    # Try to extract from standard keys
    for key in facility_keys:
        if key in data:
            if isinstance(data[key], list):
                facilities.extend(data[key])
                break
    
    # If still no facilities, check if data itself is a list
    if not facilities and isinstance(data, list):
        facilities = data
    
    # If data has states structure
    if not facilities and "states" in data:
        for state_data in data["states"]:
            if "facilities" in state_data:
                facilities.extend(state_data["facilities"])
    
    print(f"  Extracted {len(facilities)} facilities from {source_name}")
    return facilities

def aggregate_treatment_centers():
    # Base paths
    base_path = Path(__file__).parent.parent.parent
    treatment_base = base_path / "03_raw_data" / "treatment_centers"
    
    # Initialize aggregated data
    aggregated = {
        "extraction_date": datetime.now().strftime("%Y-%m-%d"),
        "data_type": "Licensed Addiction Treatment Centers",
        "total_facilities": 0,
        "facilities_by_type": {
            "outpatient": [],
            "residential": [],
            "inpatient": []
        },
        "facilities_by_state": {},
        "all_facilities": []
    }
    
    # Track unique ID assignment
    next_id = {
        "outpatient": 1,
        "residential": 1,
        "inpatient": 1
    }
    
    # Process each level of care
    levels_of_care = ["outpatient", "residential", "inpatient"]
    
    for level in levels_of_care:
        print(f"\nProcessing {level} treatment centers...")
        level_path = treatment_base / level
        
        if level_path.exists():
            # Process all subdirectories
            for subdir in level_path.iterdir():
                if subdir.is_dir():
                    print(f"  Checking {subdir.name}/...")
                    
                    # Process all JSON files in subdirectory
                    for json_file in subdir.glob("*.json"):
                        print(f"    Loading {json_file.name}...")
                        data = load_json(json_file)
                        
                        if data:
                            facilities = extract_facilities(data, json_file.name)
                            
                            for facility in facilities:
                                # Generate unique ID
                                id_prefix = {
                                    "outpatient": "TX_OUT",
                                    "residential": "TX_RES",
                                    "inpatient": "TX_INP"
                                }[level]
                                
                                facility["id"] = f"{id_prefix}_{next_id[level]:04d}"
                                facility["level_of_care"] = level
                                facility["data_source"] = f"{subdir.name}/{json_file.name}"
                                
                                # Standardize state field
                                state = facility.get("state", "Unknown")
                                if not state and "address" in facility:
                                    # Try to extract state from address
                                    addr_parts = facility["address"].split(",")
                                    if len(addr_parts) >= 2:
                                        state_zip = addr_parts[-1].strip().split()
                                        if len(state_zip) >= 2:
                                            state = state_zip[0]
                                
                                facility["state"] = state
                                
                                # Add to collections
                                aggregated["all_facilities"].append(facility)
                                aggregated["facilities_by_type"][level].append(facility["id"])
                                
                                # Add to state collection
                                if state not in aggregated["facilities_by_state"]:
                                    aggregated["facilities_by_state"][state] = []
                                aggregated["facilities_by_state"][state].append({
                                    "id": facility["id"],
                                    "name": facility.get("name", facility.get("facility_name", "")),
                                    "level_of_care": level,
                                    "city": facility.get("city", "")
                                })
                                
                                next_id[level] += 1
    
    # Calculate totals
    aggregated["total_facilities"] = len(aggregated["all_facilities"])
    aggregated["states_covered"] = len(aggregated["facilities_by_state"])
    
    # Generate summary statistics
    aggregated["summary"] = {
        "total_facilities": aggregated["total_facilities"],
        "by_level_of_care": {
            level: len(facilities) for level, facilities in aggregated["facilities_by_type"].items()
        },
        "states_with_facilities": aggregated["states_covered"],
        "top_states": sorted(
            [(state, len(facilities)) for state, facilities in aggregated["facilities_by_state"].items()],
            key=lambda x: x[1],
            reverse=True
        )[:10],
        "data_completeness": calculate_completeness(aggregated["all_facilities"])
    }
    
    # Save aggregated data
    output_path = base_path / "04_processed_data" / "master_directories" / "treatment_centers_master.json"
    with open(output_path, 'w') as f:
        json.dump(aggregated, f, indent=2)
    
    print(f"\n{'='*60}")
    print(f"AGGREGATION COMPLETE!")
    print(f"{'='*60}")
    print(f"Total Treatment Centers: {aggregated['total_facilities']}")
    print(f"States covered: {aggregated['states_covered']}")
    print(f"\nBreakdown by level of care:")
    for level, count in aggregated['summary']['by_level_of_care'].items():
        print(f"  - {level.capitalize()}: {count} facilities")
    print(f"\nSaved to: {output_path}")
    
    # Generate summary report
    generate_summary_report(aggregated, base_path)
    
    return aggregated

def calculate_completeness(facilities):
    """Calculate data completeness metrics"""
    total = len(facilities)
    if total == 0:
        return {}
    
    fields = ["name", "address", "phone", "services", "insurance", "website"]
    completeness = {}
    
    for field in fields:
        count = sum(1 for f in facilities if f.get(field))
        completeness[field] = f"{(count/total)*100:.1f}%"
    
    return completeness

def generate_summary_report(data, base_path):
    """Generate markdown summary report"""
    report = f"""# Treatment Centers - Aggregation Summary

**Generated**: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Overview
- **Total Facilities**: {data['total_facilities']:,}
- **States Covered**: {data['states_covered']}
- **Data Sources**: Multiple state and federal databases

## Breakdown by Level of Care
"""
    
    for level, count in data['summary']['by_level_of_care'].items():
        report += f"- **{level.capitalize()}**: {count:,} facilities\n"
    
    report += "\n## Geographic Coverage\n"
    report += "### Top 10 States by Facility Count\n"
    for state, count in data['summary']['top_states']:
        report += f"- **{state}**: {count:,} facilities\n"
    
    report += "\n## Data Completeness\n"
    if data['summary']['data_completeness']:
        for field, percent in data['summary']['data_completeness'].items():
            report += f"- **{field.capitalize()}**: {percent} complete\n"
    
    report += "\n## Service Types\n"
    report += "### Outpatient Services\n"
    report += "- Standard Outpatient (OP)\n"
    report += "- Intensive Outpatient (IOP)\n"
    report += "- Partial Hospitalization (PHP)\n"
    report += "- Medication-Assisted Treatment (MAT)\n"
    report += "- Opioid Treatment Programs (OTP)\n"
    
    report += "\n### Residential Services\n"
    report += "- Short-term residential (30 days or less)\n"
    report += "- Long-term residential (more than 30 days)\n"
    report += "- Therapeutic communities\n"
    
    report += "\n### Inpatient Services\n"
    report += "- Hospital-based detoxification\n"
    report += "- Psychiatric units with addiction focus\n"
    report += "- Dual diagnosis treatment\n"
    
    # Save report
    report_path = base_path / "05_reports" / "analysis" / "treatment_centers_summary.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, 'w') as f:
        f.write(report)
    
    print(f"Summary report saved to: {report_path}")

if __name__ == "__main__":
    aggregate_treatment_centers()