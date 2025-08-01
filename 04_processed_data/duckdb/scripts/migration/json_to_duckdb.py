#!/usr/bin/env python3
"""
JSON to DuckDB Migration Script
National Recovery Support Services Directory
Version: 1.0
Date: 2025-07-31
"""

import duckdb
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, date
import logging
from typing import Dict, List, Any
import sys
import hashlib

class DuckDBMigrator:
    """Migrate JSON master directories to DuckDB database"""
    
    def __init__(self, db_path: str = None, log_level: str = 'INFO'):
        """Initialize migrator with database connection"""
        self.base_path = Path(__file__).parent.parent.parent.parent.parent
        
        if db_path is None:
            db_path = self.base_path / "04_processed_data" / "duckdb" / "databases" / "recovery_directory.duckdb"
        
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize connection
        self.conn = duckdb.connect(str(self.db_path))
        
        # Set up logging
        self.setup_logging(log_level)
        
        # Data source paths
        self.master_dir = self.base_path / "04_processed_data" / "master_directories"
        
        self.logger.info(f"Initialized DuckDB migrator with database: {self.db_path}")
        
    def setup_logging(self, log_level: str):
        """Configure logging"""
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('migration.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def run_migration(self):
        """Execute complete migration process"""
        start_time = datetime.now()
        self.logger.info("Starting JSON to DuckDB migration")
        
        try:
            # Create schema
            self.create_schema()
            
            # Load reference data
            self.load_reference_data()
            
            # Migrate each data source
            results = self.migrate_all_sources()
            
            # Create indexes
            self.create_indexes()
            
            # Validate migration
            validation_results = self.validate_migration()
            
            # Log results
            duration = (datetime.now() - start_time).total_seconds()
            self.log_migration_results(results, validation_results, duration)
            
            self.logger.info(f"Migration completed successfully in {duration:.2f} seconds")
            
        except Exception as e:
            self.logger.error(f"Migration failed: {str(e)}")
            raise
            
    def create_schema(self):
        """Create database schema from SQL files"""
        self.logger.info("Creating database schema")
        
        schema_dir = self.base_path / "04_processed_data" / "duckdb" / "schemas"
        schema_files = ["01_core_tables.sql", "02_indexes.sql", "03_views.sql"]
        
        for schema_file in schema_files:
            if schema_file in ["02_indexes.sql", "03_views.sql"]:
                continue  # Create indexes and views after data load
                
            file_path = schema_dir / schema_file
            if file_path.exists():
                self.logger.info(f"Executing {schema_file}")
                with open(file_path, 'r') as f:
                    sql = f.read()
                    # Execute each statement separately
                    for statement in sql.split(';'):
                        if statement.strip():
                            self.conn.execute(statement)
            else:
                self.logger.warning(f"Schema file not found: {file_path}")
                
    def load_reference_data(self):
        """Load reference data for services and certifications"""
        self.logger.info("Loading reference data")
        
        # Core services
        core_services = [
            ('Peer Support', 'support', 'Peer-based recovery support services', True),
            ('Recovery Coaching', 'support', 'Professional recovery coaching', True),
            ('Sober Living', 'housing', 'Substance-free housing', True),
            ('MAT', 'clinical', 'Medication-Assisted Treatment', True),
            ('Detox', 'clinical', 'Medical detoxification services', True),
            ('Outpatient', 'clinical', 'Outpatient treatment services', True),
            ('Residential', 'clinical', 'Residential treatment services', True),
            ('Support Groups', 'support', 'Group recovery meetings', True),
            ('Employment Services', 'social', 'Job assistance and vocational support', True),
            ('Mental Health', 'clinical', 'Mental health counseling and treatment', True)
        ]
        
        # Insert services
        for service_name, category, description, is_core in core_services:
            # Check if service already exists
            existing = self.conn.execute("""
                SELECT id FROM services WHERE service_name = ?
            """, [service_name]).fetchone()
            
            if not existing:
                self.conn.execute("""
                    INSERT INTO services (service_name, service_category, description, is_core_service)
                    VALUES (?, ?, ?, ?)
                """, [service_name, category, description, is_core])
            
        # Certification bodies
        cert_bodies = [
            ('NARR', 'National Alliance for Recovery Residences', 'recovery_residence', True),
            ('Oxford House', 'Oxford House Inc.', 'recovery_residence', False),
            ('CARF', 'Commission on Accreditation of Rehabilitation Facilities', 'treatment', False),
            ('Joint Commission', 'The Joint Commission', 'treatment', False),
            ('FARR', 'Florida Association of Recovery Residences', 'recovery_residence', True),
            ('GARR', 'Georgia Association of Recovery Residences', 'recovery_residence', True),
            ('MARR', 'Michigan Association of Recovery Residences', 'recovery_residence', True),
            ('PARR', 'Pennsylvania Association of Recovery Residences', 'recovery_residence', True)
        ]
        
        for name, full_name, cert_type, is_narr in cert_bodies:
            # Check if certification body already exists
            existing = self.conn.execute("""
                SELECT id FROM certification_bodies WHERE name = ?
            """, [full_name]).fetchone()
            
            if not existing:
                self.conn.execute("""
                    INSERT INTO certification_bodies (name, abbreviation, certification_type, is_narr_affiliate)
                    VALUES (?, ?, ?, ?)
                """, [full_name, name, cert_type, is_narr])
            
    def migrate_all_sources(self) -> Dict[str, int]:
        """Migrate all data sources"""
        results = {}
        
        sources = {
            'narr_residences': 'master_directory.json',
            'rccs': 'recovery_community_centers_master.json',
            'rcos': 'recovery_organizations_master.json',
            'treatment_centers': 'treatment_centers_master.json'
        }
        
        for org_type, filename in sources.items():
            self.logger.info(f"Migrating {org_type} from {filename}")
            count = self.migrate_source(org_type, filename)
            results[org_type] = count
            
        # Also check for Oxford House data
        oxford_path = self.base_path / "03_raw_data" / "oxford_house_data" / "oxford_house_processed_latest.json"
        if oxford_path.exists():
            self.logger.info("Migrating Oxford House data")
            count = self.migrate_oxford_houses(oxford_path)
            results['oxford_houses'] = count
            
        return results
        
    def migrate_source(self, org_type: str, filename: str) -> int:
        """Migrate single data source"""
        file_path = self.master_dir / filename
        
        if not file_path.exists():
            self.logger.warning(f"File not found: {file_path}")
            return 0
            
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        # Extract organizations based on data structure
        organizations = self.extract_organizations(data, org_type)
        
        if not organizations:
            self.logger.warning(f"No organizations found in {filename}")
            return 0
            
        # Convert to DataFrame for batch processing
        df = pd.DataFrame(organizations)
        
        # Process and insert in batches
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            processed_batch = self.process_batch(batch, org_type)
            
            # Insert into database
            self.conn.executemany("""
                INSERT OR REPLACE INTO organizations (
                    id, name, organization_type, address_street, address_city,
                    address_state, address_zip, latitude, longitude, geohash,
                    phone, email, website, services, certifications, demographics,
                    capacity, facility_type, level_of_care, data_source,
                    extraction_date, is_narr_certified, certification_type,
                    certification_level, operator, funding_sources, data_quality_score,
                    has_complete_address, has_contact_info
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, processed_batch)
            
            total_inserted += len(processed_batch)
            self.logger.debug(f"Inserted batch {i//batch_size + 1} ({len(processed_batch)} records)")
            
        self.logger.info(f"Migrated {total_inserted} {org_type} organizations")
        return total_inserted
        
    def extract_organizations(self, data: Dict, org_type: str) -> List[Dict]:
        """Extract organizations from various JSON structures"""
        organizations = []
        
        if 'organizations_by_state' in data:
            # NARR format
            for state, state_orgs in data['organizations_by_state'].items():
                for org in state_orgs:
                    org['extracted_state'] = state
                    organizations.append(org)
                    
        elif 'all_facilities' in data:
            # Treatment centers format  
            organizations = data['all_facilities']
            
        elif 'facilities' in data:
            # Treatment centers format alternate
            organizations = data['facilities']
            
        elif 'all_centers' in data:
            # RCC format
            organizations = data['all_centers']
            
        elif 'recovery_community_centers' in data:
            # RCC format alternate
            organizations = data['recovery_community_centers']
            
        elif 'recovery_community_organizations' in data:
            # RCO format  
            organizations = data['recovery_community_organizations']
            
        elif isinstance(data, list):
            organizations = data
            
        return organizations
        
    def process_batch(self, batch: pd.DataFrame, org_type: str) -> List[tuple]:
        """Process batch of organizations for insertion"""
        processed = []
        
        for _, row in batch.iterrows():
            # Generate ID if missing
            org_id = row.get('id', '')
            if not org_id:
                org_id = self.generate_id(row.get('name', ''), row.get('address', ''))
                
            # Extract address components
            address_data = self.parse_address(row)
            
            # Process services and certifications
            services = self.process_json_field(row.get('services', []))
            certifications = self.process_json_field(row.get('certifications', []))
            demographics = self.process_json_field(row.get('demographics', {}))
            capacity = self.process_json_field(row.get('capacity', {}))
            funding_sources = self.process_json_field(row.get('funding_sources', []))
            
            # Determine certification status
            is_narr, cert_type, cert_level = self.determine_certification(row, certifications)
            
            # Calculate data quality score
            quality_score = self.calculate_quality_score(row, address_data)
            
            # Calculate boolean fields
            has_complete_address = bool(address_data['street'] and address_data['city'] and address_data['state'])
            has_contact_info = bool(row.get('phone') or row.get('email') or row.get('website'))
            
            # Get organization name (handle different field names)
            org_name = row.get('name', '') or row.get('organization_name', '') or row.get('facility_name', '')
            
            # Create tuple for insertion
            processed.append((
                org_id,
                org_name,
                org_type,
                address_data['street'],
                address_data['city'],
                address_data['state'],
                address_data['zip'],
                address_data.get('latitude'),
                address_data.get('longitude'),
                address_data.get('geohash'),
                row.get('phone', ''),
                row.get('email', ''),
                row.get('website', ''),
                services,
                certifications,
                demographics,
                capacity,
                row.get('facility_type', ''),
                row.get('level_of_care', ''),
                row.get('data_source', ''),
                self.parse_date(row.get('extraction_date', '')),
                is_narr,
                cert_type,
                cert_level,
                row.get('operator', ''),
                funding_sources,
                quality_score,
                has_complete_address,
                has_contact_info
            ))
            
        return processed
        
    def parse_address(self, row: pd.Series) -> Dict[str, Any]:
        """Parse address from various formats"""
        address_data = {
            'street': None,
            'city': None,
            'state': None,
            'zip': None,
            'latitude': None,
            'longitude': None,
            'geohash': None
        }
        
        # Handle nested address object
        if 'address' in row and isinstance(row['address'], dict):
            addr = row['address']
            address_data['street'] = addr.get('street', '')
            address_data['city'] = addr.get('city', '')
            address_data['state'] = addr.get('state', '')
            address_data['zip'] = addr.get('zip', '')
            
        # Handle flat address fields
        else:
            address_data['street'] = row.get('address', '') or row.get('address_line1', '')
            address_data['city'] = row.get('city', '')
            address_data['state'] = row.get('state', '') or row.get('extracted_state', '')
            address_data['zip'] = row.get('zip', '') or row.get('zip_code', '')
            
        # Extract coordinates if available
        if 'coordinates' in row and isinstance(row['coordinates'], dict):
            address_data['latitude'] = row['coordinates'].get('latitude')
            address_data['longitude'] = row['coordinates'].get('longitude')
        else:
            address_data['latitude'] = row.get('latitude')
            address_data['longitude'] = row.get('longitude')
            
        # Generate geohash if coordinates available
        if address_data['latitude'] and address_data['longitude']:
            try:
                # Simplified geohash - in production would use proper geohash library
                lat = float(address_data['latitude'])
                lon = float(address_data['longitude'])
                address_data['geohash'] = f"{lat:.4f},{lon:.4f}"
            except (ValueError, TypeError):
                address_data['geohash'] = None
            
        return address_data
        
    def process_json_field(self, value: Any) -> str:
        """Convert Python object to JSON string for storage"""
        if value is None:
            return None
        if isinstance(value, str):
            try:
                # Check if already valid JSON
                json.loads(value)
                return value
            except:
                # Convert string to JSON array
                return json.dumps([value])
        return json.dumps(value)
        
    def determine_certification(self, row: pd.Series, certifications: str) -> tuple:
        """Determine certification status"""
        is_narr = False
        cert_type = None
        cert_level = row.get('certification_level', '')
        
        # Check various certification indicators
        if row.get('is_narr_certified'):
            is_narr = True
            cert_type = 'narr'
        elif row.get('certification_type'):
            cert_type = row['certification_type']
            if isinstance(cert_type, str) and 'narr' in cert_type.lower():
                is_narr = True
        elif certifications and certifications != 'null':
            try:
                cert_list = json.loads(certifications) if isinstance(certifications, str) else certifications
                if isinstance(cert_list, list):
                    for cert in cert_list:
                        if 'narr' in str(cert).lower():
                            is_narr = True
                            cert_type = 'narr'
                            break
            except (json.JSONDecodeError, TypeError):
                pass
                    
        return is_narr, cert_type, cert_level
        
    def calculate_quality_score(self, row: pd.Series, address_data: Dict) -> float:
        """Calculate data quality score (0-1)"""
        score = 0.0
        factors = 0
        
        # Name quality (required)
        if row.get('name'):
            score += 1.0
            factors += 1
            
        # Address completeness
        if all([address_data['street'], address_data['city'], address_data['state']]):
            score += 1.0
            factors += 1
        elif address_data['city'] and address_data['state']:
            score += 0.5
            factors += 1
            
        # Contact information
        contact_score = 0
        if row.get('phone'):
            contact_score += 0.33
        if row.get('email'):
            contact_score += 0.33
        if row.get('website'):
            contact_score += 0.34
        score += contact_score
        factors += 1
        
        # Services information
        if row.get('services'):
            score += 1.0
            factors += 1
            
        # Coordinates
        if address_data['latitude'] and address_data['longitude']:
            score += 1.0
            factors += 1
            
        return score / factors if factors > 0 else 0.0
        
    def generate_id(self, name: str, address: str) -> str:
        """Generate unique ID for organization"""
        # Create hash from name and address
        content = f"{name}|{address}".lower()
        hash_obj = hashlib.md5(content.encode())
        return f"GEN_{hash_obj.hexdigest()[:8].upper()}"
        
    def parse_date(self, date_str: Any) -> date:
        """Parse date from various formats"""
        if not date_str:
            return datetime.now().date()
            
        if isinstance(date_str, date):
            return date_str
            
        if isinstance(date_str, datetime):
            return date_str.date()
            
        if isinstance(date_str, str):
            # Try common date formats
            for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y']:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
                    
        return datetime.now().date()
        
    def migrate_oxford_houses(self, file_path: Path) -> int:
        """Migrate Oxford House data"""
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        houses = data.get('houses', [])
        
        if not houses:
            return 0
            
        # Process Oxford Houses
        processed = []
        for house in houses:
            org_id = house.get('id', self.generate_id(house.get('name', ''), ''))
            
            # Extract address
            addr = house.get('address', {})
            
            # Process capacity
            capacity_data = house.get('capacity', {})
            
            # Calculate boolean fields
            has_complete_address = bool(addr.get('street') and addr.get('city') and addr.get('state'))
            has_contact_info = bool(house.get('contact', {}).get('phone'))
            
            processed.append((
                org_id,
                house.get('name', ''),
                'oxford_houses',
                addr.get('street', ''),
                addr.get('city', ''),
                addr.get('state', ''),
                addr.get('zip', ''),
                None,  # latitude
                None,  # longitude
                None,  # geohash
                house.get('contact', {}).get('phone', ''),
                '',  # email
                '',  # website
                json.dumps(house.get('services', [])),
                json.dumps(['Oxford House Charter']),
                json.dumps(house.get('demographics', {})),
                json.dumps(capacity_data),
                '',  # facility_type
                '',  # level_of_care
                'oxfordvacancies.com',
                self.parse_date(house.get('data_source', {}).get('extraction_date', '')),
                False,  # is_narr_certified
                'oxford_house',
                '',  # cert_level
                'Oxford House Inc.',
                json.dumps([]),
                0.8,  # quality_score
                has_complete_address,
                has_contact_info
            ))
            
        # Insert Oxford Houses
        self.conn.executemany("""
            INSERT OR REPLACE INTO organizations (
                id, name, organization_type, address_street, address_city,
                address_state, address_zip, latitude, longitude, geohash,
                phone, email, website, services, certifications, demographics,
                capacity, facility_type, level_of_care, data_source,
                extraction_date, is_narr_certified, certification_type,
                certification_level, operator, funding_sources, data_quality_score,
                has_complete_address, has_contact_info
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, processed)
        
        return len(processed)
        
    def create_indexes(self):
        """Create database indexes after data load"""
        self.logger.info("Creating database indexes")
        
        schema_file = self.base_path / "04_processed_data" / "duckdb" / "schemas" / "02_indexes.sql"
        
        if schema_file.exists():
            with open(schema_file, 'r') as f:
                sql = f.read()
                # Execute each statement separately
                for statement in sql.split(';'):
                    if statement.strip():
                        try:
                            self.conn.execute(statement)
                        except Exception as e:
                            self.logger.warning(f"Index creation warning: {str(e)}")
                            
    def validate_migration(self) -> Dict[str, Any]:
        """Validate migrated data"""
        self.logger.info("Validating migration")
        
        validation = {}
        
        # Total count validation
        result = self.conn.execute("""
            SELECT organization_type, COUNT(*) as count
            FROM organizations
            GROUP BY organization_type
            ORDER BY organization_type
        """).fetchall()
        
        validation['counts_by_type'] = {row[0]: row[1] for row in result}
        validation['total_count'] = sum(row[1] for row in result)
        
        # Data quality validation
        quality = self.conn.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN has_complete_address THEN 1 ELSE 0 END) as complete_addresses,
                SUM(CASE WHEN has_contact_info THEN 1 ELSE 0 END) as has_contact,
                AVG(data_quality_score) as avg_quality
            FROM organizations
        """).fetchone()
        
        validation['quality_metrics'] = {
            'total': quality[0],
            'complete_addresses': quality[1],
            'has_contact': quality[2],
            'avg_quality_score': round(quality[3], 3) if quality[3] else 0
        }
        
        # State coverage
        states = self.conn.execute("""
            SELECT COUNT(DISTINCT address_state) as state_count
            FROM organizations
            WHERE address_state IS NOT NULL
        """).fetchone()
        
        validation['state_coverage'] = states[0]
        
        return validation
        
    def log_migration_results(self, results: Dict, validation: Dict, duration: float):
        """Log migration results to database"""
        
        # Calculate totals
        total_added = sum(results.values())
        
        # Insert into update history
        self.conn.execute("""
            INSERT INTO update_history (
                update_type, source, records_added, records_updated,
                records_deleted, status, duration_seconds
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            'initial_migration',
            'json_master_directories',
            total_added,
            0,
            0,
            'success',
            duration
        ])
        
        # Log summary
        self.logger.info("=" * 60)
        self.logger.info("MIGRATION SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Total records migrated: {total_added:,}")
        for org_type, count in results.items():
            self.logger.info(f"  {org_type}: {count:,}")
        self.logger.info(f"\nQuality Metrics:")
        self.logger.info(f"  Complete addresses: {validation['quality_metrics']['complete_addresses']:,}")
        self.logger.info(f"  Has contact info: {validation['quality_metrics']['has_contact']:,}")
        self.logger.info(f"  Average quality score: {validation['quality_metrics']['avg_quality_score']}")
        self.logger.info(f"  State coverage: {validation['state_coverage']}")
        self.logger.info(f"\nMigration completed in {duration:.2f} seconds")
        self.logger.info("=" * 60)


def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate JSON data to DuckDB')
    parser.add_argument('--db-path', help='Path to DuckDB database file')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    
    args = parser.parse_args()
    
    # Run migration
    migrator = DuckDBMigrator(db_path=args.db_path, log_level=args.log_level)
    migrator.run_migration()
    
    # Close connection
    migrator.conn.close()


if __name__ == "__main__":
    main()