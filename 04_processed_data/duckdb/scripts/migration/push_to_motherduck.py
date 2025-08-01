#!/usr/bin/env python3
"""
Push Local DuckDB to MotherDuck Cloud Database

This script migrates the local recovery directory database to MotherDuck cloud.
It handles authentication, schema migration, data transfer, and verification.
"""

import duckdb
import json
import os
import sys
import time
from pathlib import Path
from datetime import datetime
import argparse

class MotherDuckMigrator:
    def __init__(self, config_path=None):
        self.local_db_path = Path(__file__).parent.parent.parent / "databases" / "recovery_directory.duckdb"
        self.config_path = config_path or Path(__file__).parent.parent.parent.parent / "duckdb" / "config" / "motherduck_config.json"
        self.local_conn = None
        self.cloud_conn = None
        self.migration_log = []
        
    def log(self, message, level="INFO"):
        """Log migration events"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {level}: {message}"
        print(log_entry)
        self.migration_log.append(log_entry)
        
    def load_config(self):
        """Load MotherDuck configuration"""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            self.config = config['motherduck_configuration']
            self.database_name = self.config['database_name']
            self.log(f"Loaded configuration for database: {self.database_name}")
            return True
        except Exception as e:
            self.log(f"Failed to load configuration: {e}", "ERROR")
            return False
            
    def get_motherduck_token(self):
        """Get MotherDuck authentication token from environment or user input"""
        token = os.getenv('MOTHERDUCK_TOKEN')
        if not token:
            self.log("MOTHERDUCK_TOKEN environment variable not found", "WARNING")
            token = input("Please enter your MotherDuck token: ").strip()
            if not token:
                self.log("No token provided. Exiting.", "ERROR")
                return None
        return token
        
    def connect_local(self):
        """Connect to local database"""
        try:
            if not self.local_db_path.exists():
                self.log(f"Local database not found: {self.local_db_path}", "ERROR")
                return False
                
            self.local_conn = duckdb.connect(str(self.local_db_path))
            self.log(f"Connected to local database: {self.local_db_path}")
            
            # Get database info
            db_size = self.local_db_path.stat().st_size / (1024 * 1024)  # MB
            self.log(f"Local database size: {db_size:.2f} MB")
            
            return True
        except Exception as e:
            self.log(f"Failed to connect to local database: {e}", "ERROR")
            return False
            
    def connect_cloud(self, token):
        """Connect to MotherDuck cloud database"""
        try:
            connection_string = f"md:{self.database_name}?motherduck_token={token}"
            self.cloud_conn = duckdb.connect(connection_string)
            self.log(f"Connected to MotherDuck database: {self.database_name}")
            return True
        except Exception as e:
            self.log(f"Failed to connect to MotherDuck: {e}", "ERROR")
            return False
            
    def get_local_tables(self):
        """Get list of tables in local database"""
        try:
            tables = self.local_conn.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'main' 
                ORDER BY table_name
            """).fetchall()
            return [table[0] for table in tables]
        except Exception as e:
            self.log(f"Failed to get local tables: {e}", "ERROR")
            return []
            
    def get_table_schema(self, table_name):
        """Get CREATE TABLE statement for a table"""
        try:
            schema = self.local_conn.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'").fetchone()
            return schema[0] if schema else None
        except Exception as e:
            self.log(f"Failed to get schema for {table_name}: {e}", "ERROR")
            return None
            
    def migrate_table(self, table_name):
        """Migrate a single table to MotherDuck"""
        try:
            self.log(f"Migrating table: {table_name}")
            
            # Get row count
            local_count = self.local_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            self.log(f"  Local rows: {local_count:,}")
            
            # Create table in cloud
            schema_sql = self.get_table_schema(table_name)
            if schema_sql:
                # Modify schema for MotherDuck compatibility
                schema_sql = schema_sql.replace('"', '')  # Remove quotes
                self.cloud_conn.execute(schema_sql)
                self.log(f"  Created table schema in cloud")
            
            # Copy data
            self.log(f"  Copying data...")
            start_time = time.time()
            
            # Use efficient COPY command
            self.local_conn.execute(f"COPY {table_name} TO 'temp_{table_name}.parquet' (FORMAT PARQUET)")
            self.cloud_conn.execute(f"COPY {table_name} FROM 'temp_{table_name}.parquet' (FORMAT PARQUET)")
            
            # Clean up temp file
            temp_file = Path(f"temp_{table_name}.parquet")
            if temp_file.exists():
                temp_file.unlink()
                
            elapsed = time.time() - start_time
            
            # Verify row count
            cloud_count = self.cloud_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            self.log(f"  Cloud rows: {cloud_count:,}")
            
            if local_count == cloud_count:
                self.log(f"  ✓ Table {table_name} migrated successfully in {elapsed:.2f}s")
                return True
            else:
                self.log(f"  ✗ Row count mismatch for {table_name}: {local_count} vs {cloud_count}", "ERROR")
                return False
                
        except Exception as e:
            self.log(f"  ✗ Failed to migrate {table_name}: {e}", "ERROR")
            return False
            
    def migrate_views(self):
        """Migrate views to MotherDuck"""
        try:
            self.log("Migrating views...")
            
            # Get local views
            views = self.local_conn.execute("""
                SELECT name, sql 
                FROM sqlite_master 
                WHERE type='view' 
                ORDER BY name
            """).fetchall()
            
            for view_name, view_sql in views:
                try:
                    # Create view in cloud
                    self.cloud_conn.execute(view_sql)
                    self.log(f"  ✓ Created view: {view_name}")
                except Exception as e:
                    self.log(f"  ✗ Failed to create view {view_name}: {e}", "WARNING")
                    
        except Exception as e:
            self.log(f"Failed to migrate views: {e}", "ERROR")
            
    def verify_migration(self):
        """Verify the migration was successful"""
        try:
            self.log("Verifying migration...")
            
            local_tables = self.get_local_tables()
            cloud_tables = self.cloud_conn.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'main' 
                ORDER BY table_name
            """).fetchall()
            cloud_tables = [table[0] for table in cloud_tables]
            
            # Check table counts
            for table in local_tables:
                if table in cloud_tables:
                    local_count = self.local_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    cloud_count = self.cloud_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    
                    if local_count == cloud_count:
                        self.log(f"  ✓ {table}: {local_count:,} rows")
                    else:
                        self.log(f"  ✗ {table}: {local_count:,} vs {cloud_count:,} rows", "ERROR")
                else:
                    self.log(f"  ✗ Table {table} missing in cloud", "ERROR")
                    
            # Test a sample query
            sample_query = "SELECT COUNT(*) as total_organizations FROM organizations"
            local_result = self.local_conn.execute(sample_query).fetchone()[0]
            cloud_result = self.cloud_conn.execute(sample_query).fetchone()[0]
            
            if local_result == cloud_result:
                self.log(f"  ✓ Sample query test passed: {local_result:,} organizations")
            else:
                self.log(f"  ✗ Sample query test failed: {local_result:,} vs {cloud_result:,}", "ERROR")
                
        except Exception as e:
            self.log(f"Verification failed: {e}", "ERROR")
            
    def save_migration_report(self):
        """Save migration report to file"""
        try:
            report_path = Path(__file__).parent / f"migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(report_path, 'w') as f:
                f.write("MotherDuck Migration Report\n")
                f.write("=" * 50 + "\n")
                f.write(f"Migration Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Database: {self.database_name}\n")
                f.write(f"Local Database: {self.local_db_path}\n")
                f.write("\nMigration Log:\n")
                f.write("-" * 20 + "\n")
                for log_entry in self.migration_log:
                    f.write(log_entry + "\n")
                    
            self.log(f"Migration report saved to: {report_path}")
            
        except Exception as e:
            self.log(f"Failed to save migration report: {e}", "WARNING")
            
    def run_migration(self, token=None):
        """Run the complete migration process"""
        self.log("Starting MotherDuck migration...")
        self.log("=" * 50)
        
        # Load configuration
        if not self.load_config():
            return False
            
        # Get token
        if not token:
            token = self.get_motherduck_token()
            if not token:
                return False
                
        # Connect to databases
        if not self.connect_local():
            return False
            
        if not self.connect_cloud(token):
            return False
            
        try:
            # Get tables to migrate
            tables = self.get_local_tables()
            self.log(f"Found {len(tables)} tables to migrate: {', '.join(tables)}")
            
            # Migrate tables
            successful_migrations = 0
            for table in tables:
                if self.migrate_table(table):
                    successful_migrations += 1
                    
            self.log(f"Successfully migrated {successful_migrations}/{len(tables)} tables")
            
            # Migrate views
            self.migrate_views()
            
            # Verify migration
            self.verify_migration()
            
            # Save report
            self.save_migration_report()
            
            self.log("Migration completed successfully!")
            return True
            
        except Exception as e:
            self.log(f"Migration failed: {e}", "ERROR")
            return False
        finally:
            # Clean up connections
            if self.local_conn:
                self.local_conn.close()
            if self.cloud_conn:
                self.cloud_conn.close()

def main():
    parser = argparse.ArgumentParser(description="Migrate local DuckDB to MotherDuck")
    parser.add_argument("--token", help="MotherDuck authentication token")
    parser.add_argument("--config", help="Path to configuration file")
    args = parser.parse_args()
    
    migrator = MotherDuckMigrator(args.config)
    success = migrator.run_migration(args.token)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 