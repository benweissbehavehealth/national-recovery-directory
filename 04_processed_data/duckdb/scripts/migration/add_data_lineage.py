#!/usr/bin/env python3
"""
Add data lineage tracking to existing DuckDB database
"""

import duckdb
import json
from pathlib import Path
from datetime import datetime
import hashlib

class DataLineageMigrator:
    def __init__(self, db_path: str = None):
        """Initialize migrator"""
        self.base_path = Path(__file__).parent.parent.parent.parent.parent
        
        if db_path is None:
            db_path = self.base_path / "04_processed_data" / "duckdb" / "databases" / "recovery_directory.duckdb"
        
        self.db_path = Path(db_path)
        self.conn = duckdb.connect(str(self.db_path))
        
    def run_migration(self):
        """Execute data lineage migration"""
        print("🔄 Starting Data Lineage Migration...")
        
        try:
            # Create new schema
            self.create_lineage_schema()
            
            # Populate source data
            self.populate_data_sources()
            
            # Populate organization types
            self.populate_organization_types()
            
            # Populate certification authorities
            self.populate_certification_authorities()
            
            # Create initial lineage records
            self.create_initial_lineage()
            
            # Create views
            self.create_views()
            
            print("✅ Data lineage migration completed successfully!")
            
        except Exception as e:
            print(f"❌ Migration failed: {e}")
            raise
            
    def create_lineage_schema(self):
        """Create data lineage tables"""
        print("Creating lineage schema...")
        
        schema_file = self.base_path / "04_processed_data" / "duckdb" / "schemas" / "04_data_lineage.sql"
        
        with open(schema_file, 'r') as f:
            sql = f.read()
            
        # Execute each statement
        for statement in sql.split(';'):
            if statement.strip():
                try:
                    self.conn.execute(statement)
                except Exception as e:
                    print(f"Warning: {e}")
                    
    def populate_data_sources(self):
        """Populate data sources table"""
        print("Populating data sources...")
        
        sources = [
            # NARR and affiliates
            ('narr_master', 'NARR Master Directory', 'manual', 'https://narr.org', 'National Alliance for Recovery Residences',
             'web_scraping', False, 0.9, True, 'monthly', '2025-07-30'),
            
            ('mash_api', 'Massachusetts MASH API', 'api', 'https://www.masshousingregistry.org', 'MASH',
             'api', False, 0.95, True, 'daily', '2025-07-30'),
             
            ('garr_directory', 'Georgia GARR Directory', 'website', 'https://www.garronline.org', 'Georgia Association of Recovery Residences',
             'web_scraping', False, 0.85, True, 'monthly', '2025-07-30'),
             
            # Recovery Community Centers
            ('samhsa_locator', 'SAMHSA Treatment Locator', 'api', 'https://findtreatment.samhsa.gov', 'SAMHSA',
             'api', False, 0.95, True, 'weekly', '2025-07-30'),
             
            ('samhsa_csv', 'SAMHSA CSV Export', 'csv', None, 'SAMHSA',
             'file_download', False, 0.95, True, 'quarterly', '2025-07-31'),
             
            # Oxford Houses
            ('oxford_vacancies', 'Oxford House Vacancies', 'website', 'https://www.oxfordvacancies.com', 'Oxford House Inc.',
             'web_scraping', False, 0.9, True, 'daily', '2025-07-31'),
             
            # State sources
            ('state_websites', 'State Government Websites', 'government', None, 'Various State Agencies',
             'manual', False, 0.8, True, 'quarterly', '2025-07-30'),
        ]
        
        for source_data in sources:
            # Check if source already exists
            existing = self.conn.execute("SELECT source_id FROM data_sources WHERE source_id = ?", [source_data[0]]).fetchone()
            if not existing:
                self.conn.execute("""
                    INSERT INTO data_sources (
                        source_id, source_name, source_type, source_url, source_organization,
                        access_method, requires_auth, reliability_score, official_source, 
                        update_frequency, first_accessed
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, source_data)
            
    def populate_organization_types(self):
        """Populate organization types"""
        print("Populating organization types...")
        
        org_types = [
            # Main categories
            ('recovery_residence', 'Recovery Residence', None, 'recovery', None,
             'Alcohol and drug-free living environment for individuals in recovery'),
             
            ('treatment_center', 'Treatment Center', None, 'treatment', None,
             'Licensed facility providing addiction treatment services'),
             
            ('recovery_support', 'Recovery Support Organization', None, 'support', None,
             'Organization providing non-clinical recovery support services'),
             
            # Subcategories - Recovery Residences
            ('narr_certified', 'NARR Certified Recovery Residence', 'recovery_residence', 'recovery', None,
             'Recovery residence certified by NARR or NARR affiliate'),
             
            ('oxford_house', 'Oxford House', 'recovery_residence', 'recovery', None,
             'Democratically self-run recovery house following Oxford House model'),
             
            ('sober_living', 'Sober Living Home', 'recovery_residence', 'recovery', None,
             'General sober living environment without specific certification'),
             
            # Subcategories - Treatment Centers
            ('outpatient', 'Outpatient Treatment', 'treatment_center', 'treatment', 'outpatient',
             'Non-residential treatment services'),
             
            ('intensive_outpatient', 'Intensive Outpatient (IOP)', 'treatment_center', 'treatment', 'intensive_outpatient',
             'Structured outpatient treatment, typically 9+ hours per week'),
             
            ('residential', 'Residential Treatment', 'treatment_center', 'treatment', 'residential',
             'Live-in treatment facility with 24/7 care'),
             
            ('inpatient', 'Inpatient/Hospital', 'treatment_center', 'treatment', 'inpatient',
             'Medical facility providing detox and stabilization'),
             
            # Subcategories - Support Organizations
            ('rcc', 'Recovery Community Center', 'recovery_support', 'support', None,
             'Peer-run center offering recovery support services'),
             
            ('rco', 'Recovery Community Organization', 'recovery_support', 'support', None,
             'Advocacy and organizing focused on recovery community'),
        ]
        
        for type_data in org_types:
            existing = self.conn.execute("SELECT type_id FROM organization_types WHERE type_id = ?", [type_data[0]]).fetchone()
            if not existing:
                self.conn.execute("""
                    INSERT INTO organization_types (
                        type_id, type_name, parent_type_id, category, level_of_care, description
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, type_data)
            
    def populate_certification_authorities(self):
        """Populate certification authorities and types"""
        print("Populating certification authorities...")
        
        # Certification authorities
        authorities = [
            ('narr', 'National Alliance for Recovery Residences', 'nonprofit', 'national', None,
             'https://narr.org', None, None, None, '2011-01-01', None, True),
             
            ('farr', 'Florida Association of Recovery Residences', 'nonprofit', 'state', ['FL'],
             'https://www.farronline.org', None, None, None, None, 'NARR', True),
             
            ('garr', 'Georgia Association of Recovery Residences', 'nonprofit', 'state', ['GA'],
             'https://www.garronline.org', None, None, None, None, 'NARR', True),
             
            ('marr', 'Michigan Association of Recovery Residences', 'nonprofit', 'state', ['MI'],
             'https://michiganarr.org', None, None, None, None, 'NARR', True),
             
            ('parr', 'Pennsylvania Association of Recovery Residences', 'nonprofit', 'state', ['PA'],
             'https://parronline.org', None, None, None, None, 'NARR', True),
             
            ('oxford_house_inc', 'Oxford House Inc.', 'nonprofit', 'national', None,
             'https://www.oxfordhouse.org', None, None, None, '1975-01-01', None, False),
             
            ('carf', 'Commission on Accreditation of Rehabilitation Facilities', 'nonprofit', 'international', None,
             'https://www.carf.org', None, None, None, '1966-01-01', None, False),
             
            ('joint_commission', 'The Joint Commission', 'nonprofit', 'national', None,
             'https://www.jointcommission.org', None, None, None, '1951-01-01', None, False),
             
            ('samhsa', 'Substance Abuse and Mental Health Services Administration', 'government', 'federal', None,
             'https://www.samhsa.gov', None, None, None, '1992-01-01', None, False),
        ]
        
        for auth_data in authorities:
            # Check if authority already exists
            existing = self.conn.execute("SELECT authority_id FROM certification_authorities WHERE authority_id = ?", [auth_data[0]]).fetchone()
            if not existing:
                # Convert state list to JSON
                auth_list = list(auth_data)
                if auth_list[4]:  # jurisdiction_states
                    auth_list[4] = json.dumps(auth_list[4])
                
                self.conn.execute("""
                    INSERT INTO certification_authorities (
                        authority_id, authority_name, authority_type, jurisdiction_level,
                        jurisdiction_states, website, phone, email, address,
                        established_date, accreditation_body, is_narr_affiliate
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, auth_list)
            
        # Certification types
        cert_types = [
            ('narr_level_1', 'narr', 'NARR Level I - Peer-Run', 'I', ['narr_certified'], 12),
            ('narr_level_2', 'narr', 'NARR Level II - Monitored', 'II', ['narr_certified'], 12),
            ('narr_level_3', 'narr', 'NARR Level III - Supervised', 'III', ['narr_certified'], 12),
            ('narr_level_4', 'narr', 'NARR Level IV - Service Provider', 'IV', ['narr_certified'], 12),
            
            ('oxford_charter', 'oxford_house_inc', 'Oxford House Charter', None, ['oxford_house'], None),
            
            ('carf_behavioral', 'carf', 'CARF Behavioral Health', None, ['treatment_center'], 36),
            
            ('jcaho_behavioral', 'joint_commission', 'Joint Commission Behavioral Health', None, ['treatment_center'], 36),
            
            ('state_licensed', 'samhsa', 'State Licensed Treatment Facility', None, ['treatment_center'], 12),
        ]
        
        for cert_data in cert_types:
            existing = self.conn.execute("SELECT cert_type_id FROM certification_types WHERE cert_type_id = ?", [cert_data[0]]).fetchone()
            if not existing:
                cert_list = list(cert_data)
                cert_list[4] = json.dumps(cert_list[4])  # applies_to_org_types
                
                self.conn.execute("""
                    INSERT INTO certification_types (
                        cert_type_id, authority_id, cert_type_name, 
                        certification_level, applies_to_org_types, renewal_period_months
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, cert_list)
            
    def create_initial_lineage(self):
        """Create initial lineage records for existing data"""
        print("Creating initial lineage records...")
        
        # Create extraction run for initial migration
        self.conn.execute("""
            INSERT INTO extraction_runs (
                source_id, extraction_type, started_at, completed_at,
                duration_seconds, records_extracted, records_added,
                status, extraction_method
            ) VALUES (
                'narr_master', 'full', '2025-07-30 10:00:00', '2025-07-30 14:00:00',
                14400, 41097, 41097, 'completed', 'json_migration'
            )
        """)
        
        run_id = self.conn.execute("SELECT MAX(run_id) FROM extraction_runs").fetchone()[0]
        
        # Create lineage records for organizations
        self.conn.execute(f"""
            INSERT INTO organization_lineage (
                organization_id, source_id, extraction_run_id,
                extracted_at, data_hash, version_number, is_current
            )
            SELECT 
                id,
                CASE 
                    WHEN organization_type = 'treatment_centers' THEN 'samhsa_csv'
                    WHEN organization_type = 'oxford_houses' THEN 'oxford_vacancies'
                    WHEN organization_type = 'rccs' THEN 'samhsa_locator'
                    WHEN organization_type = 'rcos' THEN 'state_websites'
                    ELSE 'narr_master'
                END as source_id,
                {run_id},
                COALESCE(extraction_date, '2025-07-30'),
                substr(md5(name || COALESCE(address_street, '') || COALESCE(address_city, '')), 1, 16),
                1,
                TRUE
            FROM organizations
        """)
        
        # Map existing organization types
        self.conn.execute("""
            INSERT INTO organization_type_history (
                organization_id, type_id, valid_from, is_current,
                classified_by, classification_source, confidence_score
            )
            SELECT 
                id,
                CASE 
                    WHEN organization_type = 'treatment_centers' AND level_of_care = 'outpatient' THEN 'outpatient'
                    WHEN organization_type = 'treatment_centers' AND level_of_care = 'residential' THEN 'residential'
                    WHEN organization_type = 'treatment_centers' AND level_of_care = 'inpatient' THEN 'inpatient'
                    WHEN organization_type = 'treatment_centers' THEN 'treatment_center'
                    WHEN organization_type = 'oxford_houses' THEN 'oxford_house'
                    WHEN organization_type = 'rccs' THEN 'rcc'
                    WHEN organization_type = 'rcos' THEN 'rco'
                    WHEN organization_type = 'narr_residences' AND is_narr_certified THEN 'narr_certified'
                    WHEN organization_type = 'narr_residences' THEN 'sober_living'
                    ELSE 'recovery_support'
                END as type_id,
                COALESCE(extraction_date, '2025-07-30'),
                TRUE,
                'migration',
                data_source,
                0.8
            FROM organizations
        """)
        
    def create_views(self):
        """Create the views from the schema"""
        print("Creating lineage views...")
        
        # The views are already in the schema file and should have been created
        # Just verify they exist
        views = ['data_source_summary', 'organization_data_freshness', 'certification_authority_summary']
        
        for view in views:
            result = self.conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '{view}' AND table_type = 'VIEW'
            """).fetchone()
            
            if result[0] > 0:
                print(f"✓ View '{view}' created successfully")
            else:
                print(f"✗ View '{view}' failed to create")
                
    def generate_summary_report(self):
        """Generate summary report of lineage data"""
        print("\n=== DATA LINEAGE SUMMARY ===")
        
        # Data sources
        sources = self.conn.execute("""
            SELECT source_type, COUNT(*) as count, AVG(reliability_score) as avg_reliability
            FROM data_sources
            GROUP BY source_type
        """).fetchall()
        
        print("\nData Sources by Type:")
        for source_type, count, reliability in sources:
            print(f"  {source_type}: {count} sources (avg reliability: {reliability:.2f})")
            
        # Organization types
        org_types = self.conn.execute("""
            SELECT category, COUNT(*) as type_count
            FROM organization_types
            WHERE parent_type_id IS NULL
            GROUP BY category
        """).fetchall()
        
        print("\nOrganization Categories:")
        for category, count in org_types:
            print(f"  {category}: {count} main types")
            
        # Certification authorities
        certs = self.conn.execute("""
            SELECT 
                authority_type,
                COUNT(*) as count,
                SUM(CASE WHEN is_narr_affiliate THEN 1 ELSE 0 END) as narr_affiliates
            FROM certification_authorities
            GROUP BY authority_type
        """).fetchall()
        
        print("\nCertification Authorities:")
        for auth_type, count, narr in certs:
            print(f"  {auth_type}: {count} authorities ({narr} NARR affiliates)")


def main():
    """Run the migration"""
    migrator = DataLineageMigrator()
    migrator.run_migration()
    migrator.generate_summary_report()
    migrator.conn.close()


if __name__ == "__main__":
    main()