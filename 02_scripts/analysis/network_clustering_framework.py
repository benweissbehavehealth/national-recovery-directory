#!/usr/bin/env python3
"""
Network Clustering Analysis Framework
Identifies affiliation networks and referral partnerships among recovery organizations
"""

import json
import os
from datetime import datetime
from pathlib import Path
import numpy as np
from collections import defaultdict
import re

class RecoveryNetworkAnalyzer:
    """Analyzes recovery organizations to identify networks and referral partnerships"""
    
    def __init__(self):
        self.organizations = []
        self.networks = {}
        self.referral_scores = {}
        self.affiliation_patterns = {}
        
    def load_organizations(self, data_path):
        """Load all organization data from master directories"""
        base_path = Path(data_path)
        
        # Load different organization types
        sources = {
            'narr_residences': 'master_directory.json',
            'rccs': 'recovery_community_centers_master.json',
            'rcos': 'recovery_organizations_master.json',
            'treatment_centers': 'treatment_centers_master.json'
        }
        
        for org_type, filename in sources.items():
            filepath = base_path / filename
            if filepath.exists():
                with open(filepath, 'r') as f:
                    data = json.load(f)
                    self._extract_organizations(data, org_type)
    
    def _extract_organizations(self, data, org_type):
        """Extract organizations from various data structures"""
        if isinstance(data, dict):
            # Handle different data structures
            if 'organizations' in data:
                orgs = data['organizations']
            elif 'organizations_by_state' in data:
                # Handle master_directory.json format
                orgs = []
                for state, state_orgs in data['organizations_by_state'].items():
                    orgs.extend(state_orgs)
            elif 'houses' in data:
                orgs = data['houses']
            elif 'centers' in data:
                orgs = data['centers']
            elif 'recovery_centers' in data:
                orgs = data['recovery_centers']
            elif 'recovery_community_centers' in data:
                orgs = data['recovery_community_centers']
            elif 'facilities' in data:
                orgs = data['facilities']
            else:
                # Check if data itself is a list of organizations
                orgs = data.get('data', [])
                
            if isinstance(orgs, list):
                for org in orgs:
                    # Standardize organization data
                    standardized = {
                        'id': org.get('id', ''),
                        'name': org.get('name', ''),
                        'org_type': org_type,
                        'state': self._extract_state(org),
                        'city': self._extract_city(org),
                        'services': self._parse_services(org),
                        'certifications': self._parse_certifications(org),
                        'address': org.get('address', {}),
                        'capacity': org.get('capacity', {}),
                        'demographics': org.get('demographics', {}),
                        'operator': org.get('operator', ''),
                        'funding_sources': org.get('funding_sources', [])
                    }
                    self.organizations.append(standardized)
                    
    def _extract_state(self, org):
        """Extract state from organization data"""
        if 'state' in org:
            return org['state']
        elif 'address' in org and isinstance(org['address'], dict):
            return org['address'].get('state', '')
        elif 'location' in org:
            return org['location'].get('state', '')
        return ''
        
    def _extract_city(self, org):
        """Extract city from organization data"""
        if 'city' in org:
            return org['city']
        elif 'address' in org and isinstance(org['address'], dict):
            return org['address'].get('city', '')
        elif 'location' in org:
            return org['location'].get('city', '')
        elif 'address' in org and isinstance(org['address'], str):
            # Try to extract city from address string
            parts = org['address'].split(',')
            if len(parts) >= 2:
                return parts[-2].strip()
        return ''
        
    def _parse_services(self, org):
        """Parse services from various formats"""
        if 'services' in org:
            if isinstance(org['services'], list):
                return org['services']
            elif isinstance(org['services'], str):
                return [org['services']]
        return []
        
    def _parse_certifications(self, org):
        """Parse certifications from various formats"""
        certs = []
        if 'certifications' in org:
            if isinstance(org['certifications'], list):
                certs.extend(org['certifications'])
            elif isinstance(org['certifications'], str):
                certs.append(org['certifications'])
        if 'certification_level' in org and org['certification_level']:
            certs.append(org['certification_level'])
        if 'certification_type' in org and org['certification_type']:
            certs.append(org['certification_type'])
        return certs
    
    def identify_clusters(self):
        """Identify clusters of organizations based on multiple factors"""
        
        clustering_factors = {
            'geographic_proximity': self._calculate_geographic_clusters,
            'service_overlap': self._calculate_service_overlap,
            'name_similarity': self._calculate_name_similarity,
            'funding_sources': self._identify_funding_connections,
            'leadership_overlap': self._identify_leadership_connections,
            'certification_bodies': self._identify_certification_networks
        }
        
        clusters = {}
        for factor_name, factor_func in clustering_factors.items():
            clusters[factor_name] = factor_func()
        
        return clusters
    
    def _calculate_geographic_clusters(self, radius_miles=10):
        """Cluster organizations by geographic proximity"""
        clusters = defaultdict(list)
        
        # Group by state first
        state_groups = defaultdict(list)
        for org in self.organizations:
            state = org.get('state', 'Unknown')
            state_groups[state].append(org)
        
        # Within each state, cluster by proximity
        for state, orgs in state_groups.items():
            # Simple clustering by city/county
            city_clusters = defaultdict(list)
            for org in orgs:
                city = org.get('city', 'Unknown')
                city_clusters[f"{state}_{city}"].append(org)
            
            clusters.update(city_clusters)
        
        return dict(clusters)
    
    def _calculate_service_overlap(self):
        """Identify organizations with similar services"""
        service_clusters = defaultdict(list)
        
        # Common service categories
        service_categories = {
            'peer_support': ['peer support', 'peer coaching', 'peer specialist'],
            'recovery_coaching': ['recovery coach', 'recovery coaching', 'certified recovery'],
            'support_groups': ['support group', 'group meeting', 'recovery meeting'],
            'housing': ['sober living', 'recovery residence', 'transitional housing'],
            'employment': ['job assistance', 'employment', 'vocational'],
            'mental_health': ['mental health', 'psychiatric', 'counseling'],
            'substance_abuse': ['substance abuse', 'addiction', 'drug alcohol'],
            'harm_reduction': ['harm reduction', 'naloxone', 'syringe exchange']
        }
        
        for org in self.organizations:
            services = ' '.join(org.get('services', [])).lower()
            for category, keywords in service_categories.items():
                if any(keyword in services for keyword in keywords):
                    service_clusters[category].append(org)
        
        return dict(service_clusters)
    
    def _calculate_name_similarity(self):
        """Identify organizations with similar names (potential affiliates)"""
        name_patterns = {}
        
        # Common affiliation patterns
        patterns = {
            'oxford_house': r'oxford\s*house',
            'recovery_cafe': r'recovery\s*caf[eé]',
            'peer_recovery': r'peer\s*(recovery|support)',
            'recovery_community': r'recovery\s*community\s*(center|organization)',
            'salvation_army': r'salvation\s*army',
            'catholic_charities': r'catholic\s*charities'
        }
        
        for pattern_name, pattern in patterns.items():
            matching_orgs = []
            for org in self.organizations:
                name = org.get('name', '').lower()
                if re.search(pattern, name):
                    matching_orgs.append(org)
            
            if matching_orgs:
                name_patterns[pattern_name] = matching_orgs
        
        return name_patterns
    
    def _identify_funding_connections(self):
        """Identify organizations with common funding sources"""
        funding_clusters = defaultdict(list)
        
        common_funders = {
            'samhsa': ['samhsa', 'substance abuse mental health'],
            'state_funded': ['state grant', 'state funded', 'department of health'],
            'federal': ['federal grant', 'cdc', 'hrsa', 'hhs'],
            'private_foundation': ['foundation', 'charitable', 'trust'],
            'united_way': ['united way'],
            'medicaid': ['medicaid', 'medicare']
        }
        
        for org in self.organizations:
            funding = ' '.join(org.get('funding_sources', [])).lower()
            for funder_type, keywords in common_funders.items():
                if any(keyword in funding for keyword in keywords):
                    funding_clusters[funder_type].append(org)
                    
        return dict(funding_clusters)
    
    def _identify_leadership_connections(self):
        """Identify organizations with potential leadership overlap"""
        # This would require more detailed data about board members, executives
        # For now, we'll look for operator/parent organization connections
        operator_clusters = defaultdict(list)
        
        for org in self.organizations:
            operator = org.get('operator', '').strip()
            if operator and operator != 'Unknown':
                operator_clusters[operator].append(org)
                
        # Only return clusters with multiple organizations
        return {op: orgs for op, orgs in operator_clusters.items() if len(orgs) > 1}
    
    def _identify_certification_networks(self):
        """Identify organizations grouped by certification bodies"""
        cert_clusters = defaultdict(list)
        
        certification_types = {
            'narr': ['narr certified', 'narr member', 'narr accredited'],
            'carf': ['carf accredited', 'carf certified'],
            'joint_commission': ['joint commission', 'jcaho'],
            'state_licensed': ['state licensed', 'licensed by'],
            'oxford_house': ['oxford house charter'],
            'recovery_residence': ['recovery residence certified']
        }
        
        for org in self.organizations:
            certs_text = ' '.join(org.get('certifications', [])).lower()
            for cert_type, keywords in certification_types.items():
                if any(keyword in certs_text for keyword in keywords):
                    cert_clusters[cert_type].append(org)
                    
        return dict(cert_clusters)
    
    def calculate_referral_scores(self):
        """Calculate likelihood of referral partnerships between organizations"""
        referral_scores = {}
        
        for i, org1 in enumerate(self.organizations):
            for j, org2 in enumerate(self.organizations[i+1:], i+1):
                score = self._calculate_referral_score(org1, org2)
                if score > 0.3:  # Threshold for likely referral partnership
                    key = f"{org1['id']}_{org2['id']}"
                    referral_scores[key] = {
                        'org1': org1['name'],
                        'org2': org2['name'],
                        'score': score,
                        'factors': self._get_referral_factors(org1, org2)
                    }
        
        return referral_scores
    
    def _calculate_referral_score(self, org1, org2):
        """Calculate referral likelihood score between two organizations"""
        score = 0.0
        
        # Geographic proximity (same state/city)
        if org1.get('state') == org2.get('state'):
            score += 0.2
            if org1.get('city') == org2.get('city'):
                score += 0.3
        
        # Complementary services
        if self._has_complementary_services(org1, org2):
            score += 0.3
        
        # Different organization types (e.g., RCC refers to treatment center)
        if org1.get('org_type') != org2.get('org_type'):
            score += 0.2
        
        # Certification compatibility
        if self._has_compatible_certifications(org1, org2):
            score += 0.1
        
        return min(score, 1.0)
    
    def _has_complementary_services(self, org1, org2):
        """Check if organizations have complementary services"""
        # Example: Detox refers to sober living, RCC refers to treatment
        service_map = {
            'detox': ['sober living', 'recovery residence', 'transitional housing'],
            'outpatient': ['peer support', 'recovery coaching', 'support groups'],
            'peer_support': ['clinical treatment', 'housing', 'mental health'],
            'emergency': ['residential', 'transitional', 'stabilization'],
            'assessment': ['treatment', 'residential', 'outpatient'],
            'crisis': ['stabilization', 'inpatient', 'residential']
        }
        
        # Get services from both organizations
        services1 = ' '.join(org1.get('services', [])).lower()
        services2 = ' '.join(org2.get('services', [])).lower()
        
        # Check if org1's services complement org2's needs
        for service_type, complements in service_map.items():
            if service_type in services1:
                for complement in complements:
                    if complement in services2:
                        return True
                        
        # Also check the reverse
        for service_type, complements in service_map.items():
            if service_type in services2:
                for complement in complements:
                    if complement in services1:
                        return True
                        
        return False
    
    def _has_compatible_certifications(self, org1, org2):
        """Check if organizations have compatible certifications"""
        # NARR certified might refer to other NARR certified
        # Non-NARR might have their own networks
        
        certs1 = org1.get('certifications', [])
        certs2 = org2.get('certifications', [])
        
        # Convert to lowercase for comparison
        certs1_lower = [cert.lower() for cert in certs1]
        certs2_lower = [cert.lower() for cert in certs2]
        
        # Check for NARR certification compatibility
        narr_keywords = ['narr', 'national alliance', 'recovery residence']
        has_narr1 = any(any(keyword in cert for keyword in narr_keywords) for cert in certs1_lower)
        has_narr2 = any(any(keyword in cert for keyword in narr_keywords) for cert in certs2_lower)
        
        if has_narr1 and has_narr2:
            return True
            
        # Check for state-specific certification networks
        state_certs = ['farr', 'garr', 'marr', 'carr', 'parr']  # State recovery residence associations
        for cert in state_certs:
            if any(cert in c for c in certs1_lower) and any(cert in c for c in certs2_lower):
                return True
                
        # Check for treatment center accreditations
        treatment_accreditations = ['carf', 'joint commission', 'coa', 'ncqa']
        for accred in treatment_accreditations:
            if any(accred in c for c in certs1_lower) and any(accred in c for c in certs2_lower):
                return True
                
        return False
    
    def _get_referral_factors(self, org1, org2):
        """Get specific factors contributing to referral likelihood"""
        factors = []
        
        if org1.get('state') == org2.get('state'):
            factors.append('same_state')
        if org1.get('city') == org2.get('city'):
            factors.append('same_city')
        if org1.get('org_type') != org2.get('org_type'):
            factors.append('complementary_types')
        
        return factors
    
    def generate_network_report(self, output_path):
        """Generate comprehensive network analysis report"""
        clusters = self.identify_clusters()
        referral_scores = self.calculate_referral_scores()
        
        report = {
            'metadata': {
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_organizations': len(self.organizations),
                'clustering_methods': list(clusters.keys())
            },
            'clusters': clusters,
            'referral_networks': referral_scores,
            'network_statistics': self._calculate_network_statistics(clusters),
            'recommendations': self._generate_recommendations(clusters, referral_scores)
        }
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        return report
    
    def _calculate_network_statistics(self, clusters):
        """Calculate statistics about the identified networks"""
        stats = {
            'average_cluster_size': {},
            'largest_clusters': {},
            'most_connected_organizations': []
        }
        
        for cluster_type, cluster_data in clusters.items():
            sizes = [len(orgs) for orgs in cluster_data.values()]
            if sizes:
                stats['average_cluster_size'][cluster_type] = np.mean(sizes)
                largest = max(cluster_data.items(), key=lambda x: len(x[1]))
                stats['largest_clusters'][cluster_type] = {
                    'name': largest[0],
                    'size': len(largest[1])
                }
        
        return stats
    
    def _generate_recommendations(self, clusters, referral_scores):
        """Generate recommendations based on network analysis"""
        recommendations = []
        
        # Identify isolated organizations
        recommendations.append({
            'type': 'isolated_organizations',
            'description': 'Organizations with few network connections that may benefit from partnerships',
            'action': 'Facilitate introductions to nearby complementary services'
        })
        
        # Identify strong clusters for resource sharing
        recommendations.append({
            'type': 'resource_sharing_opportunities',
            'description': 'Dense clusters that could benefit from coordinated services',
            'action': 'Explore shared training, resources, or referral protocols'
        })
        
        return recommendations


class NARRClassificationSystem:
    """System to clearly distinguish between NARR and non-NARR recovery residences"""
    
    def __init__(self):
        self.narr_indicators = [
            'narr certified',
            'narr accredited',
            'narr member',
            'narr standards',
            'narr level',
            'narr affiliate'
        ]
        
        self.non_narr_certifications = [
            'oxford house',
            'garr certified',  # Georgia Association of Recovery Residences
            'farr certified',  # Florida Association of Recovery Residences
            'state licensed',
            'independent certification'
        ]
    
    def classify_residence(self, residence_data):
        """Classify a residence as NARR, non-NARR, or unknown"""
        classification = {
            'id': residence_data.get('id'),
            'name': residence_data.get('name'),
            'certification_type': 'unknown',
            'certification_details': [],
            'confidence': 0.0
        }
        
        # Check all text fields for indicators
        text_to_check = ' '.join([
            str(residence_data.get('name', '')),
            str(residence_data.get('description', '')),
            str(residence_data.get('certifications', '')),
            ' '.join(residence_data.get('services', []))
        ]).lower()
        
        # Check for NARR certification
        narr_score = sum(1 for indicator in self.narr_indicators if indicator in text_to_check)
        if narr_score > 0:
            classification['certification_type'] = 'narr_certified'
            classification['confidence'] = min(narr_score / len(self.narr_indicators), 1.0)
            classification['certification_details'] = [ind for ind in self.narr_indicators if ind in text_to_check]
        
        # Check for non-NARR certifications
        else:
            for cert in self.non_narr_certifications:
                if cert in text_to_check:
                    classification['certification_type'] = 'non_narr_certified'
                    classification['certification_details'].append(cert)
                    classification['confidence'] = 0.8
                    break
        
        # If no certification found
        if classification['certification_type'] == 'unknown':
            classification['certification_type'] = 'no_certification_found'
            classification['confidence'] = 0.5
        
        return classification
    
    def update_residence_records(self, residences):
        """Update all residence records with clear certification classification"""
        updated_residences = []
        
        for residence in residences:
            classification = self.classify_residence(residence)
            
            # Add classification to residence data
            residence['narr_classification'] = {
                'is_narr_certified': classification['certification_type'] == 'narr_certified',
                'certification_type': classification['certification_type'],
                'certification_details': classification['certification_details'],
                'classification_confidence': classification['confidence'],
                'last_classified': datetime.now().strftime('%Y-%m-%d')
            }
            
            updated_residences.append(residence)
        
        return updated_residences


if __name__ == "__main__":
    # Example usage
    analyzer = RecoveryNetworkAnalyzer()
    
    # Load data
    base_path = Path(__file__).parent.parent.parent / "04_processed_data" / "master_directories"
    analyzer.load_organizations(base_path)
    
    # Generate network analysis
    output_path = Path(__file__).parent.parent.parent / "05_reports" / "network_analysis_report.json"
    report = analyzer.generate_network_report(output_path)
    
    print("Network Analysis Complete!")
    print(f"Report saved to: {output_path}")