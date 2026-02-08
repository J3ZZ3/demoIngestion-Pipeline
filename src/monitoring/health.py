"""
Health check endpoints and monitoring utilities.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from src.database.connection import db_manager
from src.ingestion.imap_client import IMAPClient

logger = logging.getLogger(__name__)


class HealthChecker:
    """Provides health checks for system components."""
    
    def __init__(self):
        self.db_manager = db_manager
    
    def check_database_health(self) -> Dict[str, Any]:
        """Check database connectivity and basic operations."""
        start_time = datetime.now()
        
        try:
            # Test basic connectivity
            is_healthy = self.db_manager.health_check()
            
            if is_healthy:
                # Test query performance
                query_start = datetime.now()
                result = self.db_manager.execute_query("SELECT 1 as test")
                query_time = (datetime.now() - query_start).total_seconds()
                
                return {
                    'status': 'healthy',
                    'response_time_ms': int((datetime.now() - start_time).total_seconds() * 1000),
                    'query_time_ms': int(query_time * 1000),
                    'test_query_result': result[0] if result else None,
                    'timestamp': datetime.now().isoformat()
                }
            else:
                return {
                    'status': 'unhealthy',
                    'error': 'Database connection failed',
                    'response_time_ms': int((datetime.now() - start_time).total_seconds() * 1000),
                    'timestamp': datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return {
                'status': 'unhealthy',
                'error': str(e),
                'response_time_ms': int((datetime.now() - start_time).total_seconds() * 1000),
                'timestamp': datetime.now().isoformat()
            }
    
    def check_imap_health(self) -> Dict[str, Any]:
        """Check IMAP server connectivity."""
        start_time = datetime.now()
        
        try:
            with IMAPClient() as imap:
                # Test basic connectivity by checking inbox
                imap.connection.select(imap.inbox)
                
                return {
                    'status': 'healthy',
                    'response_time_ms': int((datetime.now() - start_time).total_seconds() * 1000),
                    'server': imap.imap_server,
                    'inbox_accessible': True,
                    'timestamp': datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"IMAP health check failed: {e}")
            return {
                'status': 'unhealthy',
                'error': str(e),
                'response_time_ms': int((datetime.now() - start_time).total_seconds() * 1000),
                'timestamp': datetime.now().isoformat()
            }
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """Get system metrics and statistics."""
        try:
            # Database metrics
            db_metrics = self._get_database_metrics()
            
            # Recent processing metrics
            processing_metrics = self._get_processing_metrics()
            
            return {
                'timestamp': datetime.now().isoformat(),
                'database': db_metrics,
                'processing': processing_metrics,
                'system': {
                    'uptime_seconds': self._get_uptime_seconds(),
                    'memory_usage_mb': self._get_memory_usage()
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to get system metrics: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def _get_database_metrics(self) -> Dict[str, Any]:
        """Get database-specific metrics."""
        try:
            # File processing statistics
            file_stats = self.db_manager.execute_query("""
                SELECT 
                    status,
                    COUNT(*) as count,
                    AVG(EXTRACT(EPOCH FROM (processing_completed_at - processing_started_at))) as avg_processing_time_seconds
                FROM ingestion_files 
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                GROUP BY status
            """)
            
            # Transaction statistics
            transaction_stats = self.db_manager.execute_query("""
                SELECT 
                    COUNT(*) as total_transactions,
                    COUNT(CASE WHEN success = true THEN 1 END) as successful_transactions,
                    COUNT(CASE WHEN success = false THEN 1 END) as failed_transactions,
                    AVG(fill_time_seconds) as avg_fill_time_seconds
                FROM scale_transactions 
                WHERE created_at >= NOW() - INTERVAL '24 hours'
            """)
            
            # Database size
            db_size = self.db_manager.execute_query("""
                SELECT 
                    pg_size_pretty(pg_database_size(current_database())) as database_size,
                    pg_size_pretty(pg_total_relation_size('ingestion_files')) as ingestion_files_size,
                    pg_size_pretty(pg_total_relation_size('scale_transactions')) as scale_transactions_size
            """)
            
            return {
                'file_processing': file_stats,
                'transactions': transaction_stats[0] if transaction_stats else {},
                'storage': db_size[0] if db_size else {}
            }
            
        except Exception as e:
            logger.error(f"Failed to get database metrics: {e}")
            return {'error': str(e)}
    
    def _get_processing_metrics(self) -> Dict[str, Any]:
        """Get processing-related metrics."""
        try:
            # Recent processing activity
            recent_activity = self.db_manager.execute_query("""
                SELECT 
                    DATE_TRUNC('hour', created_at) as hour,
                    COUNT(*) as files_processed,
                    COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as successful_files,
                    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_files
                FROM ingestion_files 
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                GROUP BY DATE_TRUNC('hour', created_at)
                ORDER BY hour DESC
                LIMIT 24
            """)
            
            # Error rates
            error_rates = self.db_manager.execute_query("""
                SELECT 
                    COUNT(*) as total_files,
                    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_files,
                    ROUND(
                        COUNT(CASE WHEN status = 'FAILED' THEN 1 END) * 100.0 / 
                        NULLIF(COUNT(*), 0), 2
                    ) as failure_rate_percent
                FROM ingestion_files 
                WHERE created_at >= NOW() - INTERVAL '24 hours'
            """)
            
            return {
                'hourly_activity': recent_activity,
                'error_rates': error_rates[0] if error_rates else {}
            }
            
        except Exception as e:
            logger.error(f"Failed to get processing metrics: {e}")
            return {'error': str(e)}
    
    def _get_uptime_seconds(self) -> int:
        """Get system uptime (placeholder implementation)."""
        # In a real implementation, this would track actual process start time
        return int((datetime.now() - datetime(2024, 1, 1)).total_seconds())
    
    def _get_memory_usage(self) -> Optional[float]:
        """Get memory usage in MB (placeholder implementation)."""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024  # Convert to MB
        except ImportError:
            return None
        except Exception:
            return None
    
    def comprehensive_health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check of all components."""
        start_time = datetime.now()
        
        # Check individual components
        db_health = self.check_database_health()
        imap_health = self.check_imap_health()
        
        # Get system metrics
        metrics = self.get_system_metrics()
        
        # Determine overall health
        overall_healthy = (
            db_health['status'] == 'healthy' and 
            imap_health['status'] == 'healthy'
        )
        
        return {
            'overall_status': 'healthy' if overall_healthy else 'unhealthy',
            'response_time_ms': int((datetime.now() - start_time).total_seconds() * 1000),
            'timestamp': datetime.now().isoformat(),
            'components': {
                'database': db_health,
                'imap': imap_health
            },
            'metrics': metrics
        }


def main():
    """CLI for health checks."""
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description='Health check utility')
    parser.add_argument('--component', choices=['database', 'imap', 'all'], default='all',
                       help='Component to check')
    parser.add_argument('--format', choices=['json', 'text'], default='text',
                       help='Output format')
    
    args = parser.parse_args()
    
    health_checker = HealthChecker()
    
    if args.component == 'database':
        result = health_checker.check_database_health()
    elif args.component == 'imap':
        result = health_checker.check_imap_health()
    else:
        result = health_checker.comprehensive_health_check()
    
    if args.format == 'json':
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Health Status: {result.get('overall_status', result.get('status', 'unknown'))}")
        print(f"Timestamp: {result.get('timestamp', 'unknown')}")
        
        if 'response_time_ms' in result:
            print(f"Response Time: {result['response_time_ms']}ms")
        
        if 'error' in result:
            print(f"Error: {result['error']}")
        
        if 'components' in result:
            for component, health in result['components'].items():
                print(f"\n{component.title()}:")
                print(f"  Status: {health.get('status', 'unknown')}")
                if 'error' in health:
                    print(f"  Error: {health['error']}")
        
        if 'metrics' in result:
            print(f"\nSystem Metrics Available")


if __name__ == "__main__":
    main()
