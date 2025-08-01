#!/usr/bin/env python3
"""
JSON to DuckDB Migration Script for National Recovery Support Services Directory

This script handles the migration of JSON data files to DuckDB database
with support for parallel processing, error handling, and incremental updates.
"""

import json
import duckdb
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import concurrent.futures
from dataclasses import dataclass
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class MigrationConfig:
    """Configuration for the migration process"""
    db_path: str = "duckdb/database/narr_directory.duckdb"
    batch_size: int = 10000
    max_workers: int = 4
    enable_wal: bool = True
    memory_limit: str = "4GB"
    threads: int = 4


class NARRMigrationPipeline:
    """Main migration pipeline for JSON to DuckDB conversion"""
    
    def __init__(self, config: MigrationConfig):
        self.config = config
        self.conn = None
        self.stats = {
            'organizations_processed': 0,
            'errors': 0,
            'start_time': datetime.now()
        }
    
    def connect(self):
        """Establish DuckDB connection with optimized settings"""
        self.conn = duckdb.connect(self.config.db_path)
        
        # Configure DuckDB for optimal performance
        self.conn.execute(f"SET memory_limit='{self.config.memory_limit}'")
        self.conn.execute(f"SET threads={self.config.threads}")
        self.conn.execute("SET enable_object_cache=true")
        
        if self.config.enable_wal:
            self.conn.execute("SET wal_autocheckpoint='1GB'")
        
        logger.info("Connected to DuckDB with optimized settings")
    
    def create_schema(self):
        """Create the database schema"""
        schema_path = Path("duckdb/schemas/01_core_tables.sql")
        
        if schema_path.exists():
            with open(schema_path, 'r') as f:
                schema_sql = f.read()
                self.conn.execute(schema_sql)
        else:
            # Inline schema creation if file doesn't exist
            self._create_inline_schema()
        
        logger.info("Database schema created successfully")
    
    def _create_inline_schema(self):
        """Create schema inline if SQL files don't exist"""
        # Organizations table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS organizations (
                org_id VARCHAR PRIMARY KEY,
                org_type VARCHAR NOT NULL,
                facility_name VARCHAR NOT NULL,
                dba_names VARCHAR[],
                address_line1 VARCHAR,
                address_line2 VARCHAR,
                city VARCHAR,
                state VARCHAR(2),
                zip_code VARCHAR(10),
                county VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                phone VARCHAR,
                fax VARCHAR,
                website VARCHAR,
                email VARCHAR,
                data_source VARCHAR,
                extraction_date TIMESTAMP,
                last_updated TIMESTAMP,
                geohash VARCHAR GENERATED ALWAYS AS (
                    CASE 
                        WHEN latitude IS NOT NULL AND longitude IS NOT NULL 
                        THEN substr(md5(latitude::varchar || ',' || longitude::varchar), 1, 12)
                        ELSE NULL 
                    END
                ) STORED
            )
        """)
        
        # Treatment Centers table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS treatment_centers (
                org_id VARCHAR PRIMARY KEY REFERENCES organizations(org_id),
                license_numbers VARCHAR[],
                state_license VARCHAR,
                federal_certification VARCHAR,
                accreditations VARCHAR[],
                level_of_care VARCHAR,
                standard_outpatient BOOLEAN DEFAULT false,
                intensive_outpatient BOOLEAN DEFAULT false,
                partial_hospitalization BOOLEAN DEFAULT false,
                medication_assisted_treatment BOOLEAN DEFAULT false,
                opioid_treatment_program BOOLEAN DEFAULT false,
                individual_therapy BOOLEAN DEFAULT false,
                group_therapy BOOLEAN DEFAULT false,
                family_therapy BOOLEAN DEFAULT false,
                cognitive_behavioral_therapy BOOLEAN DEFAULT false,
                serves_adolescents BOOLEAN DEFAULT false,
                serves_adults BOOLEAN DEFAULT false,
                serves_seniors BOOLEAN DEFAULT false,
                minimum_age INTEGER,
                maximum_age INTEGER,
                accepts_medicaid BOOLEAN DEFAULT false,
                accepts_medicare BOOLEAN DEFAULT false,
                accepts_private_insurance BOOLEAN DEFAULT false,
                accepts_cash_self_payment BOOLEAN DEFAULT false,
                sliding_fee_scale BOOLEAN DEFAULT false,
                outpatient_capacity INTEGER,
                residential_capacity INTEGER,
                inpatient_capacity INTEGER,
                current_census INTEGER,
                quality_score DOUBLE,
                last_inspection_date DATE,
                accreditation_status VARCHAR
            )
        """)
        
        # NARR Residences table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS narr_residences (
                org_id VARCHAR PRIMARY KEY REFERENCES organizations(org_id),
                certification_level VARCHAR,
                capacity INTEGER,
                affiliate_organization VARCHAR,
                affiliate_website VARCHAR,
                specializations VARCHAR[],
                gender_specific VARCHAR,
                operating_since DATE,
                certification_date DATE,
                certification_expiry DATE
            )
        """)
        
        # Recovery Centers table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS recovery_centers (
                org_id VARCHAR PRIMARY KEY REFERENCES organizations(org_id),
                trade_names VARCHAR[],
                certification_status VARCHAR,
                services VARCHAR[],
                funding_sources VARCHAR[],
                annual_budget DECIMAL(12,2),
                annual_reach INTEGER,
                target_populations VARCHAR[],
                operating_hours JSON,
                leadership VARCHAR,
                social_media JSON
            )
        """)
        
        # Create indexes
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_org_state ON organizations(state)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_org_type ON organizations(org_type)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_org_city_state ON organizations(city, state)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_tc_level ON treatment_centers(level_of_care)")
    
    def generate_org_id(self, org_data: Dict[str, Any], org_type: str) -> str:
        """Generate a unique organization ID"""
        # Use existing ID if available
        if 'org_id' in org_data:
            return org_data['org_id']
        if 'id' in org_data:
            return org_data['id']
        
        # Generate ID based on organization data
        unique_string = f"{org_type}_{org_data.get('facility_name', '')}_{org_data.get('state', '')}_{org_data.get('city', '')}"
        hash_digest = hashlib.md5(unique_string.encode()).hexdigest()[:8]
        return f"{org_type.upper()}_{hash_digest}"
    
    def migrate_treatment_centers(self):
        """Migrate treatment center data"""
        logger.info("Starting treatment centers migration...")
        
        tc_file = Path("04_processed_data/master_directories/treatment_centers_master.json")
        if not tc_file.exists():
            logger.error(f"Treatment centers file not found: {tc_file}")
            return
        
        with open(tc_file, 'r') as f:
            data = json.load(f)
        
        facilities = data.get('all_facilities', [])
        logger.info(f"Processing {len(facilities)} treatment centers...")
        
        # Process in batches
        for i in range(0, len(facilities), self.config.batch_size):
            batch = facilities[i:i + self.config.batch_size]
            self._process_treatment_center_batch(batch)
            logger.info(f"Processed {min(i + self.config.batch_size, len(facilities))}/{len(facilities)} treatment centers")
    
    def _process_treatment_center_batch(self, batch: List[Dict[str, Any]]):
        """Process a batch of treatment centers"""
        org_records = []
        tc_records = []
        
        for facility in batch:
            try:
                org_id = self.generate_org_id(facility, 'tc')
                
                # Prepare organization record
                org_record = {
                    'org_id': org_id,
                    'org_type': 'treatment_center',
                    'facility_name': facility.get('facility_name', ''),
                    'dba_names': facility.get('dba_names', []),
                    'address_line1': facility.get('address_line1', ''),
                    'address_line2': facility.get('address_line2', ''),
                    'city': facility.get('city', ''),
                    'state': facility.get('state', ''),
                    'zip_code': facility.get('zip_code', ''),
                    'county': facility.get('county', ''),
                    'latitude': facility.get('latitude'),
                    'longitude': facility.get('longitude'),
                    'phone': facility.get('phone', ''),
                    'fax': facility.get('fax', ''),
                    'website': facility.get('website', ''),
                    'email': facility.get('email', ''),
                    'data_source': facility.get('data_source', ''),
                    'extraction_date': facility.get('extraction_date'),
                    'last_updated': facility.get('last_updated')
                }
                
                # Prepare treatment center record
                tc_record = {
                    'org_id': org_id,
                    'license_numbers': facility.get('license_numbers', []),
                    'state_license': facility.get('state_license', ''),
                    'federal_certification': facility.get('federal_certification', ''),
                    'accreditations': facility.get('accreditations', []),
                    'level_of_care': facility.get('level_of_care', ''),
                    'standard_outpatient': facility.get('standard_outpatient', False),
                    'intensive_outpatient': facility.get('intensive_outpatient', False),
                    'partial_hospitalization': facility.get('partial_hospitalization', False),
                    'medication_assisted_treatment': facility.get('medication_assisted_treatment', False),
                    'opioid_treatment_program': facility.get('opioid_treatment_program', False),
                    'individual_therapy': facility.get('individual_therapy', False),
                    'group_therapy': facility.get('group_therapy', False),
                    'family_therapy': facility.get('family_therapy', False),
                    'cognitive_behavioral_therapy': facility.get('cognitive_behavioral_therapy', False),
                    'serves_adolescents': facility.get('serves_adolescents', False),
                    'serves_adults': facility.get('serves_adults', False),
                    'serves_seniors': facility.get('serves_seniors', False),
                    'minimum_age': facility.get('minimum_age'),
                    'maximum_age': facility.get('maximum_age'),
                    'accepts_medicaid': facility.get('accepts_medicaid', False),
                    'accepts_medicare': facility.get('accepts_medicare', False),
                    'accepts_private_insurance': facility.get('accepts_private_insurance', False),
                    'accepts_cash_self_payment': facility.get('accepts_cash_self_payment', False),
                    'sliding_fee_scale': facility.get('sliding_fee_scale', False),
                    'outpatient_capacity': facility.get('outpatient_capacity'),
                    'residential_capacity': facility.get('residential_capacity'),
                    'inpatient_capacity': facility.get('inpatient_capacity'),
                    'current_census': facility.get('current_outpatient_census'),
                    'quality_score': facility.get('quality_score'),
                    'last_inspection_date': facility.get('last_inspection_date'),
                    'accreditation_status': facility.get('accreditation_status', '')
                }
                
                org_records.append(org_record)
                tc_records.append(tc_record)
                self.stats['organizations_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error processing treatment center: {e}")
                self.stats['errors'] += 1
        
        # Bulk insert
        if org_records:
            self.conn.execute("BEGIN TRANSACTION")
            try:
                # Insert organizations
                self.conn.executemany(
                    """INSERT OR REPLACE INTO organizations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [tuple(r.values()) for r in org_records]
                )
                
                # Insert treatment centers
                self.conn.executemany(
                    """INSERT OR REPLACE INTO treatment_centers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [tuple(r.values()) for r in tc_records]
                )
                
                self.conn.execute("COMMIT")
            except Exception as e:
                self.conn.execute("ROLLBACK")
                logger.error(f"Batch insert failed: {e}")
                raise
    
    def migrate_narr_residences(self):
        """Migrate NARR residence data"""
        logger.info("Starting NARR residences migration...")
        
        narr_file = Path("04_processed_data/master_directories/master_directory.json")
        if not narr_file.exists():
            logger.error(f"NARR residences file not found: {narr_file}")
            return
        
        with open(narr_file, 'r') as f:
            data = json.load(f)
        
        organizations = data.get('all_organizations', [])
        logger.info(f"Processing {len(organizations)} NARR residences...")
        
        for i in range(0, len(organizations), self.config.batch_size):
            batch = organizations[i:i + self.config.batch_size]
            self._process_narr_batch(batch)
            logger.info(f"Processed {min(i + self.config.batch_size, len(organizations))}/{len(organizations)} NARR residences")
    
    def _process_narr_batch(self, batch: List[Dict[str, Any]]):
        """Process a batch of NARR residences"""
        org_records = []
        narr_records = []
        
        for org in batch:
            try:
                org_id = self.generate_org_id(org, 'narr')
                
                # Parse address
                address_parts = org.get('address', '').split(',')
                address_line1 = address_parts[0].strip() if address_parts else ''
                city = address_parts[1].strip() if len(address_parts) > 1 else ''
                state_zip = address_parts[2].strip() if len(address_parts) > 2 else ''
                
                # Extract state and zip
                state = ''
                zip_code = ''
                if state_zip:
                    parts = state_zip.split()
                    if len(parts) >= 2:
                        state = parts[0]
                        zip_code = parts[1]
                
                # Prepare organization record
                org_record = {
                    'org_id': org_id,
                    'org_type': 'narr_residence',
                    'facility_name': org.get('name', ''),
                    'dba_names': [],
                    'address_line1': address_line1,
                    'address_line2': '',
                    'city': city,
                    'state': state[:2] if state else org.get('state', '')[:2],
                    'zip_code': zip_code,
                    'county': '',
                    'latitude': None,
                    'longitude': None,
                    'phone': org.get('phone', ''),
                    'fax': '',
                    'website': org.get('website', ''),
                    'email': org.get('email', ''),
                    'data_source': org.get('data_source', ''),
                    'extraction_date': datetime.now().isoformat(),
                    'last_updated': org.get('last_update', datetime.now().isoformat())
                }
                
                # Prepare NARR residence record
                narr_record = {
                    'org_id': org_id,
                    'certification_level': org.get('certification_level', ''),
                    'capacity': int(org.get('capacity')) if org.get('capacity') and org.get('capacity').isdigit() else None,
                    'affiliate_organization': org.get('state', ''),
                    'affiliate_website': org.get('affiliate_website', ''),
                    'specializations': org.get('specializations', []),
                    'gender_specific': org.get('gender_specific', ''),
                    'operating_since': None,
                    'certification_date': None,
                    'certification_expiry': None
                }
                
                org_records.append(org_record)
                narr_records.append(narr_record)
                self.stats['organizations_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error processing NARR residence: {e}")
                self.stats['errors'] += 1
        
        # Bulk insert
        if org_records:
            self.conn.execute("BEGIN TRANSACTION")
            try:
                # Insert organizations
                self.conn.executemany(
                    """INSERT OR REPLACE INTO organizations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [tuple(r.values()) for r in org_records]
                )
                
                # Insert NARR residences
                self.conn.executemany(
                    """INSERT OR REPLACE INTO narr_residences VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [tuple(r.values()) for r in narr_records]
                )
                
                self.conn.execute("COMMIT")
            except Exception as e:
                self.conn.execute("ROLLBACK")
                logger.error(f"Batch insert failed: {e}")
                raise
    
    def migrate_recovery_centers(self):
        """Migrate recovery center data"""
        logger.info("Starting recovery centers migration...")
        
        rc_file = Path("04_processed_data/master_directories/recovery_community_centers_master.json")
        if not rc_file.exists():
            logger.error(f"Recovery centers file not found: {rc_file}")
            return
        
        with open(rc_file, 'r') as f:
            data = json.load(f)
        
        centers = data.get('all_centers', [])
        logger.info(f"Processing {len(centers)} recovery centers...")
        
        for i in range(0, len(centers), self.config.batch_size):
            batch = centers[i:i + self.config.batch_size]
            self._process_recovery_center_batch(batch)
            logger.info(f"Processed {min(i + self.config.batch_size, len(centers))}/{len(centers)} recovery centers")
    
    def _process_recovery_center_batch(self, batch: List[Dict[str, Any]]):
        """Process a batch of recovery centers"""
        org_records = []
        rc_records = []
        
        for center in batch:
            try:
                org_id = self.generate_org_id(center, 'rc')
                
                # Parse address
                address_parts = center.get('address', '').split(',')
                address_line1 = address_parts[0].strip() if address_parts else ''
                city_state_zip = ','.join(address_parts[1:]) if len(address_parts) > 1 else ''
                
                # Prepare organization record
                org_record = {
                    'org_id': org_id,
                    'org_type': 'recovery_center',
                    'facility_name': center.get('organization_name', ''),
                    'dba_names': center.get('trade_names', []),
                    'address_line1': address_line1,
                    'address_line2': '',
                    'city': center.get('city', ''),
                    'state': center.get('state', '')[:2] if center.get('state') else '',
                    'zip_code': '',
                    'county': '',
                    'latitude': None,
                    'longitude': None,
                    'phone': center.get('phone', ''),
                    'fax': '',
                    'website': center.get('website', ''),
                    'email': center.get('email', ''),
                    'data_source': center.get('funding_source', ''),
                    'extraction_date': datetime.now().isoformat(),
                    'last_updated': datetime.now().isoformat()
                }
                
                # Prepare recovery center record
                rc_record = {
                    'org_id': org_id,
                    'trade_names': center.get('trade_names', []),
                    'certification_status': center.get('certification_status', ''),
                    'services': center.get('services', []),
                    'funding_sources': center.get('funding_sources', []),
                    'annual_budget': None,
                    'annual_reach': int(center.get('annual_reach', '0').replace(',', '').replace('+', '')) if center.get('annual_reach') and any(c.isdigit() for c in str(center.get('annual_reach'))) else None,
                    'target_populations': center.get('target_populations', []),
                    'operating_hours': json.dumps(center.get('operating_hours')) if isinstance(center.get('operating_hours'), dict) else center.get('operating_hours', ''),
                    'leadership': center.get('leadership', ''),
                    'social_media': json.dumps(center.get('social_media', {}))
                }
                
                org_records.append(org_record)
                rc_records.append(rc_record)
                self.stats['organizations_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error processing recovery center: {e}")
                self.stats['errors'] += 1
        
        # Bulk insert
        if org_records:
            self.conn.execute("BEGIN TRANSACTION")
            try:
                # Insert organizations
                self.conn.executemany(
                    """INSERT OR REPLACE INTO organizations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [tuple(r.values()) for r in org_records]
                )
                
                # Insert recovery centers
                self.conn.executemany(
                    """INSERT OR REPLACE INTO recovery_centers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [tuple(r.values()) for r in rc_records]
                )
                
                self.conn.execute("COMMIT")
            except Exception as e:
                self.conn.execute("ROLLBACK")
                logger.error(f"Batch insert failed: {e}")
                raise
    
    def create_materialized_views(self):
        """Create materialized views for common queries"""
        logger.info("Creating materialized views...")
        
        # Geographic service availability view
        self.conn.execute("""
            CREATE OR REPLACE VIEW v_services_by_location AS
            SELECT 
                state, 
                city,
                org_type,
                COUNT(*) as facility_count
            FROM organizations
            GROUP BY state, city, org_type
        """)
        
        # Treatment center capacity view
        self.conn.execute("""
            CREATE OR REPLACE VIEW v_treatment_capacity AS
            SELECT 
                o.state,
                tc.level_of_care,
                COUNT(*) as facility_count,
                SUM(tc.outpatient_capacity) as total_outpatient_capacity,
                SUM(tc.residential_capacity) as total_residential_capacity
            FROM organizations o
            JOIN treatment_centers tc ON o.org_id = tc.org_id
            GROUP BY o.state, tc.level_of_care
        """)
        
        logger.info("Materialized views created successfully")
    
    def generate_statistics(self):
        """Generate migration statistics"""
        logger.info("Generating migration statistics...")
        
        stats = {
            'migration_completed': datetime.now().isoformat(),
            'duration_seconds': (datetime.now() - self.stats['start_time']).total_seconds(),
            'organizations_processed': self.stats['organizations_processed'],
            'errors': self.stats['errors'],
            'table_counts': {}
        }
        
        # Get counts for each table
        for table in ['organizations', 'treatment_centers', 'narr_residences', 'recovery_centers']:
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            stats['table_counts'][table] = count
        
        # Save statistics
        with open('duckdb/migration_stats.json', 'w') as f:
            json.dump(stats, f, indent=2)
        
        logger.info(f"Migration completed in {stats['duration_seconds']:.2f} seconds")
        logger.info(f"Total organizations: {stats['organizations_processed']}")
        logger.info(f"Errors encountered: {stats['errors']}")
        
        return stats
    
    def run(self):
        """Execute the complete migration pipeline"""
        try:
            # Setup
            self.connect()
            self.create_schema()
            
            # Migrate data
            self.migrate_narr_residences()
            self.migrate_recovery_centers()
            self.migrate_treatment_centers()
            
            # Post-processing
            self.create_materialized_views()
            stats = self.generate_statistics()
            
            # Optimize database
            logger.info("Optimizing database...")
            self.conn.execute("ANALYZE")
            self.conn.execute("VACUUM")
            
            logger.info("Migration completed successfully!")
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()


def main():
    """Main entry point"""
    config = MigrationConfig()
    
    # Ensure directory structure exists
    Path("duckdb/database").mkdir(parents=True, exist_ok=True)
    
    pipeline = NARRMigrationPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()