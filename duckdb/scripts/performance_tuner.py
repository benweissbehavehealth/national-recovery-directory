#!/usr/bin/env python3
"""
Performance Tuning Script for DuckDB National Recovery Directory

This script analyzes query patterns and optimizes database performance
through index recommendations, query optimization, and statistics updates.
"""

import duckdb
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PerformanceTuner:
    """DuckDB performance optimization for recovery services directory"""
    
    def __init__(self, db_path: str = "duckdb/database/narr_directory.duckdb"):
        self.db_path = db_path
        self.conn = None
        self.benchmark_results = {}
        
    def connect(self):
        """Connect to DuckDB with performance monitoring enabled"""
        self.conn = duckdb.connect(self.db_path)
        # Enable profiling
        self.conn.execute("SET enable_profiling = true")
        self.conn.execute("SET profiling_mode = 'detailed'")
        logger.info("Connected to DuckDB with profiling enabled")
    
    def analyze_table_statistics(self):
        """Analyze and update table statistics"""
        logger.info("Analyzing table statistics...")
        
        tables = ['organizations', 'treatment_centers', 'narr_residences', 'recovery_centers']
        stats = {}
        
        for table in tables:
            # Get table size and row count
            result = self.conn.execute(f"""
                SELECT 
                    COUNT(*) as row_count,
                    pg_size_pretty(pg_table_size('{table}')) as table_size
                FROM {table}
            """).fetchone()
            
            if result:
                stats[table] = {
                    'row_count': result[0],
                    'table_size': result[1] if len(result) > 1 else 'N/A'
                }
                
                # Update statistics
                self.conn.execute(f"ANALYZE {table}")
                logger.info(f"Updated statistics for {table}: {result[0]} rows")
        
        return stats
    
    def benchmark_common_queries(self) -> Dict[str, Dict]:
        """Benchmark common query patterns"""
        logger.info("Benchmarking common queries...")
        
        queries = {
            'geographic_search': """
                SELECT COUNT(*) FROM organizations 
                WHERE state = 'CA' AND city = 'Los Angeles'
            """,
            
            'mat_providers_by_state': """
                SELECT o.state, COUNT(*) as provider_count
                FROM organizations o
                JOIN treatment_centers tc ON o.org_id = tc.org_id
                WHERE tc.medication_assisted_treatment = true
                GROUP BY o.state
                ORDER BY provider_count DESC
                LIMIT 10
            """,
            
            'medicaid_facilities': """
                SELECT COUNT(*) FROM treatment_centers
                WHERE accepts_medicaid = true 
                  AND level_of_care = 'outpatient'
            """,
            
            'capacity_analysis': """
                SELECT 
                    o.state,
                    SUM(tc.outpatient_capacity) as total_capacity,
                    AVG(tc.outpatient_capacity) as avg_capacity
                FROM organizations o
                JOIN treatment_centers tc ON o.org_id = tc.org_id
                WHERE tc.outpatient_capacity > 0
                GROUP BY o.state
            """,
            
            'recovery_ecosystem': """
                SELECT 
                    o.city,
                    o.state,
                    COUNT(DISTINCT CASE WHEN o.org_type = 'treatment_center' THEN o.org_id END) as treatment_centers,
                    COUNT(DISTINCT CASE WHEN o.org_type = 'narr_residence' THEN o.org_id END) as recovery_residences,
                    COUNT(DISTINCT CASE WHEN o.org_type = 'recovery_center' THEN o.org_id END) as recovery_centers
                FROM organizations o
                WHERE o.state IN ('CA', 'TX', 'FL', 'NY')
                GROUP BY o.city, o.state
                HAVING COUNT(*) > 5
            """,
            
            'service_availability': """
                SELECT 
                    tc.*,
                    o.facility_name,
                    o.city,
                    o.state
                FROM treatment_centers tc
                JOIN organizations o ON tc.org_id = o.org_id
                WHERE tc.medication_assisted_treatment = true
                  AND tc.accepts_medicaid = true
                  AND tc.telehealth_services = true
                  AND o.state = 'CA'
            """,
            
            'network_analysis': """
                SELECT 
                    o1.facility_name as facility_1,
                    o2.facility_name as facility_2,
                    o1.org_type as type_1,
                    o2.org_type as type_2,
                    SQRT(POWER(o1.latitude - o2.latitude, 2) + 
                         POWER(o1.longitude - o2.longitude, 2)) * 69 as distance_miles
                FROM organizations o1
                CROSS JOIN organizations o2
                WHERE o1.org_id < o2.org_id
                  AND o1.state = o2.state
                  AND o1.latitude IS NOT NULL
                  AND o2.latitude IS NOT NULL
                  AND SQRT(POWER(o1.latitude - o2.latitude, 2) + 
                          POWER(o1.longitude - o2.longitude, 2)) * 69 <= 10
                LIMIT 100
            """
        }
        
        results = {}
        
        for query_name, query in queries.items():
            start_time = time.time()
            
            try:
                # Execute query
                result = self.conn.execute(query).fetchall()
                execution_time = time.time() - start_time
                
                # Get query plan
                plan = self.conn.execute(f"EXPLAIN {query}").fetchall()
                
                results[query_name] = {
                    'execution_time': execution_time,
                    'row_count': len(result),
                    'status': 'success',
                    'plan_summary': plan[0][0] if plan else 'No plan available'
                }
                
                logger.info(f"Query '{query_name}' completed in {execution_time:.3f} seconds")
                
            except Exception as e:
                results[query_name] = {
                    'execution_time': time.time() - start_time,
                    'status': 'error',
                    'error': str(e)
                }
                logger.error(f"Query '{query_name}' failed: {e}")
        
        self.benchmark_results = results
        return results
    
    def recommend_indexes(self) -> List[str]:
        """Recommend indexes based on query patterns"""
        recommendations = []
        
        # Check for missing indexes on foreign keys
        fk_check = """
            SELECT DISTINCT
                'CREATE INDEX idx_' || table_name || '_' || column_name || 
                ' ON ' || table_name || '(' || column_name || ')' as index_sql
            FROM information_schema.columns
            WHERE table_schema = 'main'
              AND column_name LIKE '%_id'
              AND table_name || '_' || column_name NOT IN (
                  SELECT table_name || '_' || column_name
                  FROM duckdb_indexes
              )
        """
        
        try:
            missing_fk_indexes = self.conn.execute(fk_check).fetchall()
            recommendations.extend([idx[0] for idx in missing_fk_indexes])
        except:
            pass
        
        # Recommend composite indexes based on benchmark results
        if self.benchmark_results:
            slow_queries = [
                name for name, result in self.benchmark_results.items()
                if result.get('execution_time', 0) > 0.1  # 100ms threshold
            ]
            
            if 'mat_providers_by_state' in slow_queries:
                recommendations.append(
                    "CREATE INDEX idx_tc_mat_state ON treatment_centers(medication_assisted_treatment, org_id) "
                    "WHERE medication_assisted_treatment = true"
                )
            
            if 'network_analysis' in slow_queries:
                recommendations.append(
                    "CREATE INDEX idx_org_geo_state ON organizations(state, latitude, longitude) "
                    "WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
                )
        
        return recommendations
    
    def optimize_queries(self):
        """Provide query optimization suggestions"""
        logger.info("Analyzing query optimization opportunities...")
        
        optimizations = []
        
        # Check for full table scans
        full_scan_check = """
            SELECT 
                query,
                total_time_ms,
                rows_read,
                rows_returned
            FROM duckdb_queries
            WHERE rows_read > 10000 
              AND rows_returned < rows_read * 0.01
            ORDER BY total_time_ms DESC
            LIMIT 10
        """
        
        try:
            inefficient_queries = self.conn.execute(full_scan_check).fetchall()
            for query in inefficient_queries:
                optimizations.append({
                    'issue': 'Full table scan with low selectivity',
                    'query_snippet': query[0][:100] + '...',
                    'rows_read': query[2],
                    'rows_returned': query[3],
                    'recommendation': 'Consider adding indexes on filter columns'
                })
        except:
            pass
        
        return optimizations
    
    def configure_memory_settings(self):
        """Configure optimal memory settings"""
        logger.info("Configuring memory settings...")
        
        # Get system memory info
        total_memory = self.conn.execute("SELECT memory_limit FROM duckdb_settings()").fetchone()
        
        # Set optimal configurations
        configurations = [
            ("SET memory_limit = '4GB'", "Increased memory limit for large operations"),
            ("SET threads = 4", "Parallel execution threads"),
            ("SET enable_object_cache = true", "Enable object caching"),
            ("SET default_null_order = 'NULLS LAST'", "Consistent NULL handling"),
            ("SET preserve_insertion_order = false", "Allow query optimizer flexibility"),
        ]
        
        applied_configs = []
        for config, description in configurations:
            try:
                self.conn.execute(config)
                applied_configs.append(f"{config} - {description}")
                logger.info(f"Applied: {config}")
            except Exception as e:
                logger.warning(f"Could not apply {config}: {e}")
        
        return applied_configs
    
    def create_performance_report(self) -> Dict[str, Any]:
        """Generate comprehensive performance report"""
        report = {
            'report_date': datetime.now().isoformat(),
            'database_path': self.db_path,
            'table_statistics': self.analyze_table_statistics(),
            'benchmark_results': self.benchmark_results,
            'index_recommendations': self.recommend_indexes(),
            'query_optimizations': self.optimize_queries(),
            'memory_configurations': self.configure_memory_settings(),
            'performance_summary': self._generate_summary()
        }
        
        # Save report
        report_path = f"duckdb/performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Performance report saved to {report_path}")
        return report
    
    def _generate_summary(self) -> Dict[str, Any]:
        """Generate performance summary"""
        if not self.benchmark_results:
            return {'status': 'No benchmarks run'}
        
        total_queries = len(self.benchmark_results)
        successful_queries = sum(1 for r in self.benchmark_results.values() if r.get('status') == 'success')
        avg_execution_time = sum(r.get('execution_time', 0) for r in self.benchmark_results.values()) / total_queries
        
        slow_queries = [
            name for name, result in self.benchmark_results.items()
            if result.get('execution_time', 0) > 0.1
        ]
        
        return {
            'total_queries_tested': total_queries,
            'successful_queries': successful_queries,
            'average_execution_time': f"{avg_execution_time:.3f} seconds",
            'slow_queries': slow_queries,
            'performance_grade': 'A' if avg_execution_time < 0.05 else 'B' if avg_execution_time < 0.1 else 'C'
        }
    
    def run_optimization(self):
        """Run complete optimization process"""
        try:
            self.connect()
            
            logger.info("Starting performance optimization...")
            
            # Run benchmarks
            self.benchmark_common_queries()
            
            # Generate report
            report = self.create_performance_report()
            
            # Apply recommended indexes
            recommendations = self.recommend_indexes()
            if recommendations:
                logger.info(f"Applying {len(recommendations)} index recommendations...")
                for index_sql in recommendations[:5]:  # Limit to 5 for safety
                    try:
                        self.conn.execute(index_sql)
                        logger.info(f"Created index: {index_sql}")
                    except Exception as e:
                        logger.warning(f"Could not create index: {e}")
            
            # Final optimization
            logger.info("Running final optimization...")
            self.conn.execute("VACUUM ANALYZE")
            
            logger.info("Performance optimization completed!")
            
            # Print summary
            print("\n=== Performance Optimization Summary ===")
            print(f"Database: {self.db_path}")
            print(f"Queries tested: {len(self.benchmark_results)}")
            print(f"Average query time: {report['performance_summary']['average_execution_time']}")
            print(f"Performance grade: {report['performance_summary']['performance_grade']}")
            if report['performance_summary']['slow_queries']:
                print(f"Slow queries requiring attention: {', '.join(report['performance_summary']['slow_queries'])}")
            
        except Exception as e:
            logger.error(f"Optimization failed: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()


def main():
    """Main entry point"""
    tuner = PerformanceTuner()
    tuner.run_optimization()


if __name__ == "__main__":
    main()