#!/usr/bin/env python3
"""
Run complete clustering analysis on recovery organizations
"""

import json
from pathlib import Path
from datetime import datetime
from network_clustering_framework import RecoveryNetworkAnalyzer, NARRClassificationSystem

def run_complete_analysis():
    """Run comprehensive network analysis on all recovery organizations"""
    
    print("=== RECOVERY NETWORK CLUSTERING ANALYSIS ===")
    print(f"Starting analysis at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Initialize analyzer
    analyzer = RecoveryNetworkAnalyzer()
    
    # Set up paths
    base_path = Path(__file__).parent.parent.parent
    data_path = base_path / "04_processed_data" / "master_directories"
    
    # Load all organization data
    print("Loading organization data...")
    analyzer.load_organizations(data_path)
    print(f"Loaded {len(analyzer.organizations)} organizations")
    
    # Run clustering analysis
    print("\nIdentifying clusters...")
    clusters = analyzer.identify_clusters()
    
    # Display cluster summary
    print("\nCluster Summary:")
    for cluster_type, cluster_data in clusters.items():
        if isinstance(cluster_data, dict):
            total_orgs = sum(len(orgs) for orgs in cluster_data.values())
            print(f"  {cluster_type}: {len(cluster_data)} clusters, {total_orgs} organizations")
        else:
            print(f"  {cluster_type}: {len(cluster_data)} organizations")
    
    # Calculate referral scores
    print("\nCalculating referral partnership scores...")
    referral_scores = analyzer.calculate_referral_scores()
    print(f"Identified {len(referral_scores)} potential referral partnerships")
    
    # Generate and save report
    output_path = base_path / "05_reports" / "network_analysis_report.json"
    report = analyzer.generate_network_report(output_path)
    
    print(f"\nReport saved to: {output_path}")
    
    # Show top referral partnerships
    if referral_scores:
        print("\nTop 10 Potential Referral Partnerships:")
        sorted_referrals = sorted(referral_scores.items(), 
                                key=lambda x: x[1]['score'], 
                                reverse=True)[:10]
        
        for i, (key, data) in enumerate(sorted_referrals, 1):
            print(f"{i}. {data['org1']} <-> {data['org2']}")
            print(f"   Score: {data['score']:.2f}, Factors: {', '.join(data['factors'])}")
    
    # Run NARR classification
    print("\n\nRunning NARR vs Non-NARR Classification...")
    classifier = NARRClassificationSystem()
    
    # Count organization types
    narr_count = 0
    non_narr_count = 0
    unknown_count = 0
    
    for org in analyzer.organizations:
        if org['org_type'] == 'narr_residences':
            classification = classifier.classify_residence(org)
            if classification['certification_type'] == 'narr_certified':
                narr_count += 1
            elif classification['certification_type'] == 'non_narr_certified':
                non_narr_count += 1
            else:
                unknown_count += 1
    
    print(f"\nResidence Classification Results:")
    print(f"  NARR Certified: {narr_count}")
    print(f"  Non-NARR Certified: {non_narr_count}")
    print(f"  Unknown/No Certification: {unknown_count}")
    
    # Save enhanced report with classification
    enhanced_report = {
        'metadata': report['metadata'],
        'clusters': clusters,
        'referral_networks': referral_scores,
        'network_statistics': report.get('network_statistics', {}),
        'classification_summary': {
            'narr_certified': narr_count,
            'non_narr_certified': non_narr_count,
            'unknown': unknown_count
        },
        'recommendations': report.get('recommendations', [])
    }
    
    enhanced_output_path = base_path / "05_reports" / f"network_analysis_enhanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(enhanced_output_path, 'w') as f:
        json.dump(enhanced_report, f, indent=2)
    
    print(f"\nEnhanced report saved to: {enhanced_output_path}")
    
    return enhanced_report

def generate_visualization_data(report):
    """Generate data formatted for visualization tools"""
    
    # Extract nodes and edges for network graph
    nodes = []
    edges = []
    
    # Create nodes from organizations in geographic clusters
    if 'geographic_proximity' in report['clusters']:
        for cluster_name, orgs in report['clusters']['geographic_proximity'].items():
            for org in orgs:
                nodes.append({
                    'id': org['id'],
                    'label': org['name'],
                    'group': cluster_name,
                    'type': org['org_type']
                })
    
    # Create edges from referral networks
    for ref_key, ref_data in report['referral_networks'].items():
        # Handle IDs with underscores by splitting and reconstructing
        parts = ref_key.split('_')
        if len(parts) >= 2:
            # Find the separator between IDs (should be after a numeric ID pattern)
            mid_point = len(parts) // 2
            org1_id = '_'.join(parts[:mid_point])
            org2_id = '_'.join(parts[mid_point:])
            edges.append({
                'from': org1_id,
                'to': org2_id,
                'weight': ref_data['score'],
                'label': f"Score: {ref_data['score']:.2f}"
            })
    
    viz_data = {
        'nodes': nodes,
        'edges': edges,
        'metadata': {
            'total_nodes': len(nodes),
            'total_edges': len(edges),
            'generated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    }
    
    # Save visualization data
    base_path = Path(__file__).parent.parent.parent
    viz_path = base_path / "05_reports" / "network_visualization_data.json"
    with open(viz_path, 'w') as f:
        json.dump(viz_data, f, indent=2)
    
    print(f"\nVisualization data saved to: {viz_path}")
    print(f"  Nodes: {len(nodes)}")
    print(f"  Edges: {len(edges)}")
    
    return viz_data

if __name__ == "__main__":
    # Run complete analysis
    report = run_complete_analysis()
    
    # Generate visualization data
    print("\n\nGenerating visualization data...")
    viz_data = generate_visualization_data(report)
    
    print("\n=== ANALYSIS COMPLETE ===")