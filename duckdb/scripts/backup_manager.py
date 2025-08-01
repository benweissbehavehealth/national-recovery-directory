#!/usr/bin/env python3
"""
Backup Manager for DuckDB National Recovery Directory

Handles automated backups, exports, and disaster recovery procedures.
"""

import duckdb
import json
import logging
import shutil
import gzip
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import hashlib

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BackupManager:
    """Comprehensive backup management for DuckDB database"""
    
    def __init__(self, db_path: str = "duckdb/database/narr_directory.duckdb"):
        self.db_path = db_path
        self.backup_dir = Path("duckdb/backups")
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        self.daily_dir = self.backup_dir / "daily"
        self.weekly_dir = self.backup_dir / "weekly"
        self.exports_dir = self.backup_dir / "exports"
        
        for dir in [self.daily_dir, self.weekly_dir, self.exports_dir]:
            dir.mkdir(exist_ok=True)
    
    def create_full_backup(self, backup_type: str = "daily") -> Dict[str, str]:
        """Create a full database backup"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if backup_type == "daily":
            backup_path = self.daily_dir / f"narr_directory_{timestamp}.duckdb"
        elif backup_type == "weekly":
            backup_path = self.weekly_dir / f"narr_directory_{timestamp}.duckdb"
        else:
            backup_path = self.backup_dir / f"narr_directory_{timestamp}.duckdb"
        
        logger.info(f"Creating {backup_type} backup at {backup_path}")
        
        try:
            # Copy the database file
            shutil.copy2(self.db_path, backup_path)
            
            # Also copy WAL file if it exists
            wal_path = Path(self.db_path).with_suffix('.wal')
            if wal_path.exists():
                backup_wal = backup_path.with_suffix('.wal')
                shutil.copy2(wal_path, backup_wal)
            
            # Compress the backup
            compressed_path = self._compress_backup(backup_path)
            
            # Calculate checksum
            checksum = self._calculate_checksum(compressed_path)
            
            # Create metadata
            metadata = {
                'backup_type': backup_type,
                'timestamp': timestamp,
                'original_path': str(self.db_path),
                'backup_path': str(compressed_path),
                'size_bytes': compressed_path.stat().st_size,
                'checksum': checksum,
                'created_at': datetime.now().isoformat()
            }
            
            # Save metadata
            metadata_path = compressed_path.with_suffix('.json')
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            logger.info(f"Backup completed successfully: {compressed_path}")
            
            return metadata
            
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            raise
    
    def export_to_parquet(self, tables: Optional[List[str]] = None) -> Dict[str, str]:
        """Export tables to Parquet format for archival"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        export_dir = self.exports_dir / f"export_{timestamp}"
        export_dir.mkdir(exist_ok=True)
        
        conn = duckdb.connect(self.db_path, read_only=True)
        
        if tables is None:
            # Export all main tables
            tables = ['organizations', 'treatment_centers', 'narr_residences', 'recovery_centers']
        
        exported_files = {}
        
        try:
            for table in tables:
                parquet_path = export_dir / f"{table}.parquet"
                logger.info(f"Exporting {table} to {parquet_path}")
                
                # Export to Parquet with compression
                conn.execute(f"""
                    COPY {table} TO '{parquet_path}' (
                        FORMAT PARQUET,
                        COMPRESSION 'ZSTD',
                        ROW_GROUP_SIZE 100000
                    )
                """)
                
                # Get row count
                row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                
                exported_files[table] = {
                    'path': str(parquet_path),
                    'rows': row_count,
                    'size_bytes': parquet_path.stat().st_size
                }
            
            # Export views as well
            views = ['v_organization_summary', 'v_mat_providers', 'v_recovery_ecosystem']
            for view in views:
                try:
                    parquet_path = export_dir / f"{view}.parquet"
                    conn.execute(f"COPY (SELECT * FROM {view}) TO '{parquet_path}' (FORMAT PARQUET)")
                    exported_files[view] = {
                        'path': str(parquet_path),
                        'size_bytes': parquet_path.stat().st_size
                    }
                except:
                    logger.warning(f"Could not export view {view}")
            
            # Create export manifest
            manifest = {
                'export_timestamp': timestamp,
                'database_path': self.db_path,
                'export_directory': str(export_dir),
                'tables_exported': exported_files,
                'total_size_bytes': sum(f['size_bytes'] for f in exported_files.values()),
                'export_completed': datetime.now().isoformat()
            }
            
            manifest_path = export_dir / 'export_manifest.json'
            with open(manifest_path, 'w') as f:
                json.dump(manifest, f, indent=2)
            
            logger.info(f"Export completed: {len(exported_files)} tables exported to {export_dir}")
            
            return manifest
            
        except Exception as e:
            logger.error(f"Export failed: {e}")
            raise
        finally:
            conn.close()
    
    def create_incremental_backup(self, since: datetime) -> Dict[str, Any]:
        """Create incremental backup of changes since specified date"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        incremental_dir = self.backup_dir / "incremental" / timestamp
        incremental_dir.mkdir(parents=True, exist_ok=True)
        
        conn = duckdb.connect(self.db_path, read_only=True)
        
        try:
            changes = {}
            
            # Track changes for each table with last_updated field
            for table in ['organizations', 'treatment_centers']:
                change_path = incremental_dir / f"{table}_changes.parquet"
                
                result = conn.execute(f"""
                    SELECT COUNT(*) FROM {table} 
                    WHERE last_updated >= '{since.isoformat()}'
                """).fetchone()
                
                if result and result[0] > 0:
                    conn.execute(f"""
                        COPY (
                            SELECT * FROM {table}
                            WHERE last_updated >= '{since.isoformat()}'
                        ) TO '{change_path}' (FORMAT PARQUET)
                    """)
                    
                    changes[table] = {
                        'rows_changed': result[0],
                        'file': str(change_path)
                    }
            
            # Create incremental backup metadata
            metadata = {
                'backup_type': 'incremental',
                'timestamp': timestamp,
                'since_date': since.isoformat(),
                'changes': changes,
                'created_at': datetime.now().isoformat()
            }
            
            metadata_path = incremental_dir / 'incremental_metadata.json'
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            logger.info(f"Incremental backup completed: {len(changes)} tables with changes")
            
            return metadata
            
        except Exception as e:
            logger.error(f"Incremental backup failed: {e}")
            raise
        finally:
            conn.close()
    
    def restore_from_backup(self, backup_path: str, target_path: Optional[str] = None) -> bool:
        """Restore database from backup"""
        backup_file = Path(backup_path)
        
        if not backup_file.exists():
            logger.error(f"Backup file not found: {backup_path}")
            return False
        
        # If compressed, decompress first
        if backup_file.suffix == '.gz':
            decompressed_path = backup_file.with_suffix('')
            logger.info(f"Decompressing backup: {backup_file}")
            
            with gzip.open(backup_file, 'rb') as f_in:
                with open(decompressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            backup_file = decompressed_path
        
        # Determine target path
        if target_path is None:
            target_path = self.db_path + '.restored'
        
        try:
            # Create backup of current database before restore
            if Path(self.db_path).exists():
                pre_restore_backup = self.db_path + '.pre_restore'
                shutil.copy2(self.db_path, pre_restore_backup)
                logger.info(f"Created pre-restore backup: {pre_restore_backup}")
            
            # Restore the backup
            shutil.copy2(backup_file, target_path)
            logger.info(f"Database restored to: {target_path}")
            
            # Verify the restored database
            conn = duckdb.connect(target_path, read_only=True)
            tables = conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
            conn.close()
            
            logger.info(f"Restore verified: {len(tables)} tables found")
            
            return True
            
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return False
    
    def cleanup_old_backups(self, retention_days: Dict[str, int]) -> Dict[str, int]:
        """Clean up old backups based on retention policy"""
        cleaned = {'daily': 0, 'weekly': 0, 'exports': 0}
        current_time = datetime.now()
        
        # Clean daily backups
        if 'daily' in retention_days:
            cutoff = current_time - timedelta(days=retention_days['daily'])
            for backup in self.daily_dir.glob('*.gz'):
                if backup.stat().st_mtime < cutoff.timestamp():
                    backup.unlink()
                    # Also remove metadata
                    backup.with_suffix('.json').unlink(missing_ok=True)
                    cleaned['daily'] += 1
        
        # Clean weekly backups
        if 'weekly' in retention_days:
            cutoff = current_time - timedelta(days=retention_days['weekly'])
            for backup in self.weekly_dir.glob('*.gz'):
                if backup.stat().st_mtime < cutoff.timestamp():
                    backup.unlink()
                    backup.with_suffix('.json').unlink(missing_ok=True)
                    cleaned['weekly'] += 1
        
        # Clean exports
        if 'exports' in retention_days:
            cutoff = current_time - timedelta(days=retention_days['exports'])
            for export_dir in self.exports_dir.iterdir():
                if export_dir.is_dir() and export_dir.stat().st_mtime < cutoff.timestamp():
                    shutil.rmtree(export_dir)
                    cleaned['exports'] += 1
        
        logger.info(f"Cleanup completed: {cleaned}")
        return cleaned
    
    def _compress_backup(self, backup_path: Path) -> Path:
        """Compress backup file using gzip"""
        compressed_path = backup_path.with_suffix('.duckdb.gz')
        
        with open(backup_path, 'rb') as f_in:
            with gzip.open(compressed_path, 'wb', compresslevel=6) as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Remove uncompressed file
        backup_path.unlink()
        
        return compressed_path
    
    def _calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of file"""
        sha256_hash = hashlib.sha256()
        
        with open(file_path, 'rb') as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        
        return sha256_hash.hexdigest()
    
    def create_backup_schedule(self):
        """Create backup schedule configuration"""
        schedule = {
            'daily_backups': {
                'enabled': True,
                'time': '02:00',
                'retention_days': 7,
                'compress': True
            },
            'weekly_backups': {
                'enabled': True,
                'day': 'Sunday',
                'time': '03:00',
                'retention_days': 30,
                'compress': True
            },
            'monthly_exports': {
                'enabled': True,
                'day': 1,
                'time': '04:00',
                'format': 'parquet',
                'retention_days': 365
            },
            'incremental_backups': {
                'enabled': False,
                'frequency_hours': 6,
                'retention_days': 3
            }
        }
        
        schedule_path = self.backup_dir / 'backup_schedule.json'
        with open(schedule_path, 'w') as f:
            json.dump(schedule, f, indent=2)
        
        logger.info(f"Backup schedule created: {schedule_path}")
        return schedule
    
    def get_backup_status(self) -> Dict[str, Any]:
        """Get current backup status and statistics"""
        status = {
            'last_backups': {},
            'backup_sizes': {},
            'total_backups': 0,
            'total_size_mb': 0
        }
        
        # Check each backup type
        for backup_type, directory in [('daily', self.daily_dir), 
                                       ('weekly', self.weekly_dir)]:
            backups = sorted(directory.glob('*.gz'), key=lambda x: x.stat().st_mtime, reverse=True)
            
            if backups:
                latest = backups[0]
                status['last_backups'][backup_type] = {
                    'file': latest.name,
                    'created': datetime.fromtimestamp(latest.stat().st_mtime).isoformat(),
                    'size_mb': latest.stat().st_size / (1024 * 1024)
                }
            
            total_size = sum(b.stat().st_size for b in backups)
            status['backup_sizes'][backup_type] = {
                'count': len(backups),
                'total_size_mb': total_size / (1024 * 1024)
            }
            
            status['total_backups'] += len(backups)
            status['total_size_mb'] += total_size / (1024 * 1024)
        
        # Check exports
        exports = list(self.exports_dir.iterdir())
        if exports:
            latest_export = max(exports, key=lambda x: x.stat().st_mtime)
            status['last_export'] = {
                'directory': latest_export.name,
                'created': datetime.fromtimestamp(latest_export.stat().st_mtime).isoformat()
            }
        
        return status


def main():
    """Main entry point for backup operations"""
    import argparse
    
    parser = argparse.ArgumentParser(description='DuckDB Backup Manager')
    parser.add_argument('action', choices=['backup', 'export', 'restore', 'cleanup', 'status'],
                       help='Action to perform')
    parser.add_argument('--type', choices=['daily', 'weekly'], default='daily',
                       help='Backup type')
    parser.add_argument('--backup-path', help='Path to backup file for restore')
    parser.add_argument('--retention-days', type=int, default=7,
                       help='Days to retain backups')
    
    args = parser.parse_args()
    
    manager = BackupManager()
    
    if args.action == 'backup':
        result = manager.create_full_backup(args.type)
        print(f"Backup created: {result['backup_path']}")
        
    elif args.action == 'export':
        result = manager.export_to_parquet()
        print(f"Export completed: {result['export_directory']}")
        
    elif args.action == 'restore':
        if not args.backup_path:
            print("Error: --backup-path required for restore")
            return
        success = manager.restore_from_backup(args.backup_path)
        print(f"Restore {'successful' if success else 'failed'}")
        
    elif args.action == 'cleanup':
        retention = {
            'daily': args.retention_days,
            'weekly': args.retention_days * 4,
            'exports': args.retention_days * 12
        }
        result = manager.cleanup_old_backups(retention)
        print(f"Cleanup completed: {result}")
        
    elif args.action == 'status':
        status = manager.get_backup_status()
        print(json.dumps(status, indent=2))


if __name__ == "__main__":
    main()