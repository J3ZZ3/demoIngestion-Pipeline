"""
Database operations for the ingestion pipeline with idempotency guarantees.
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from psycopg2 import sql, DatabaseError
from psycopg2.extras import execute_values

from src.database.connection import db_manager
from src.ingestion.csv_processor import ScaleTransaction

logger = logging.getLogger(__name__)


class DatabaseOperations:
    """Handles database operations with idempotency and safety."""
    
    def __init__(self):
        self.db = db_manager
    
    def check_file_duplicate(self, file_sha256: str) -> Optional[Dict[str, Any]]:
        """Check if a file has already been processed."""
        try:
            query = """
                SELECT id, status, filename, received_at, created_at
                FROM ingestion_files 
                WHERE file_sha256 = %s
            """
            
            results = self.db.execute_query(query, (file_sha256,))
            
            if results:
                logger.info(f"Duplicate file found: SHA256 {file_sha256[:16]}... (ID: {results[0]['id']})")
                return results[0]
            
            return None
            
        except Exception as e:
            logger.error(f"Error checking file duplicate: {e}")
            raise
    
    def create_ingestion_file(
        self, 
        file_sha256: str,
        metadata: Dict[str, Any],
        correlation_id: str
    ) -> int:
        """Create a new ingestion file record."""
        try:
            query = """
                INSERT INTO ingestion_files 
                (source, message_id, from_email, subject, received_at, 
                 filename, file_sha256, status, correlation_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'NEW', %s)
                RETURNING id
            """
            
            params = (
                metadata.get('source', 'imap'),
                metadata.get('message_id'),
                metadata.get('from_email'),
                metadata.get('subject'),
                self._parse_email_date(metadata.get('received_at')),
                metadata.get('filename'),
                file_sha256,
                correlation_id
            )
            
            result = self.db.execute_query(query, params)
            
            if result:
                file_id = result[0]['id']
                logger.info(f"Created ingestion file record: ID {file_id}")
                return file_id
            
            raise RuntimeError("Failed to create ingestion file record")
            
        except Exception as e:
            logger.error(f"Error creating ingestion file: {e}")
            raise
    
    def update_file_status(self, file_id: int, status: str, error: str = None) -> None:
        """Update the status of an ingestion file."""
        try:
            if status not in ['NEW', 'PROCESSING', 'COMPLETED', 'FAILED', 'DUPLICATE']:
                raise ValueError(f"Invalid status: {status}")
            
            query = """
                UPDATE ingestion_files 
                SET status = %s, 
                    error = %s,
                    updated_at = NOW()
                WHERE id = %s
            """
            
            self.db.execute_query(query, (status, error, file_id), fetch=False)
            logger.info(f"Updated file {file_id} status to {status}")
            
        except Exception as e:
            logger.error(f"Error updating file status: {e}")
            raise
    
    def set_file_processing_times(self, file_id: int, started: bool = False, completed: bool = False) -> None:
        """Set processing start or completion times."""
        try:
            updates = []
            params = []
            
            if started:
                updates.append("processing_started_at = NOW()")
            if completed:
                updates.append("processing_completed_at = NOW()")
            
            if not updates:
                return
            
            updates.append("updated_at = NOW()")
            params.append(file_id)
            
            query = f"""
                UPDATE ingestion_files 
                SET {', '.join(updates)}
                WHERE id = %s
            """
            
            self.db.execute_query(query, params, fetch=False)
            
        except Exception as e:
            logger.error(f"Error setting processing times: {e}")
            raise
    
    def insert_transactions(self, file_id: int, transactions: List[ScaleTransaction]) -> int:
        """Insert scale transactions with upsert for idempotency."""
        try:
            if not transactions:
                return 0
            
            # Prepare data for batch insert
            transaction_data = []
            for transaction in transactions:
                transaction_data.append((
                    transaction.scale_name,
                    transaction.transact_no,
                    transaction.cyl_size_kg,
                    transaction.tare_weight_kg,
                    transaction.fill_kg,
                    transaction.residual_kg,
                    transaction.success,
                    transaction.started_at,
                    transaction.fill_time_seconds,
                    file_id
                ))
            
            # Use upsert to handle duplicates gracefully
            query = """
                INSERT INTO scale_transactions 
                (scale_name, transact_no, cyl_size_kg, tare_weight_kg, 
                 fill_kg, residual_kg, success, started_at, fill_time_seconds, 
                 ingestion_file_id)
                VALUES %s
                ON CONFLICT (scale_name, transact_no) 
                DO UPDATE SET
                    cyl_size_kg = EXCLUDED.cyl_size_kg,
                    tare_weight_kg = EXCLUDED.tare_weight_kg,
                    fill_kg = EXCLUDED.fill_kg,
                    residual_kg = EXCLUDED.residual_kg,
                    success = EXCLUDED.success,
                    started_at = EXCLUDED.started_at,
                    fill_time_seconds = EXCLUDED.fill_time_seconds,
                    ingestion_file_id = EXCLUDED.ingestion_file_id,
                    updated_at = NOW()
                RETURNING id
            """
            
            # Execute in batches to avoid memory issues
            batch_size = 100
            total_inserted = 0
            
            for i in range(0, len(transaction_data), batch_size):
                batch = transaction_data[i:i + batch_size]
                results = self.db.execute_query(query, (batch,))
                
                if results:
                    total_inserted += len(results)
            
            logger.info(f"Inserted/upserted {total_inserted} transactions for file {file_id}")
            return total_inserted
            
        except Exception as e:
            logger.error(f"Error inserting transactions: {e}")
            raise
    
    def get_file_statistics(self, file_id: int) -> Dict[str, Any]:
        """Get processing statistics for a file."""
        try:
            query = """
                SELECT 
                    f.id,
                    f.filename,
                    f.status,
                    f.file_sha256,
                    f.created_at,
                    f.processing_started_at,
                    f.processing_completed_at,
                    COUNT(t.id) as transaction_count,
                    COUNT(CASE WHEN t.success = true THEN 1 END) as success_count,
                    COUNT(CASE WHEN t.success = false THEN 1 END) as failure_count
                FROM ingestion_files f
                LEFT JOIN scale_transactions t ON f.id = t.ingestion_file_id
                WHERE f.id = %s
                GROUP BY f.id
            """
            
            results = self.db.execute_query(query, (file_id,))
            
            if results:
                return results[0]
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting file statistics: {e}")
            return {}
    
    def get_duplicate_files(self, file_sha256: str) -> List[Dict[str, Any]]:
        """Get all files with the same SHA256 hash."""
        try:
            query = """
                SELECT id, filename, status, created_at, received_at
                FROM ingestion_files 
                WHERE file_sha256 = %s
                ORDER BY created_at DESC
            """
            
            return self.db.execute_query(query, (file_sha256,))
            
        except Exception as e:
            logger.error(f"Error getting duplicate files: {e}")
            return []
    
    def cleanup_old_files(self, days_old: int = 30) -> int:
        """Clean up old processed files (optional maintenance)."""
        try:
            query = """
                DELETE FROM ingestion_files 
                WHERE status IN ('COMPLETED', 'FAILED') 
                AND created_at < NOW() - INTERVAL '%s days'
            """
            
            deleted_count = self.db.execute_query(query, (days_old,), fetch=False)
            logger.info(f"Cleaned up {deleted_count} old files")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up old files: {e}")
            return 0
    
    def _parse_email_date(self, date_string: str) -> Optional[datetime]:
        """Parse email date string to datetime."""
        if not date_string:
            return None
        
        try:
            # Email dates can be in various formats, try common ones
            from email.utils import parsedate_to_datetime
            
            dt = parsedate_to_datetime(date_string)
            if dt:
                return dt
            
        except Exception:
            pass
        
        # Fallback: try to parse as ISO format
        try:
            return datetime.fromisoformat(date_string.replace('Z', '+00:00'))
        except Exception:
            logger.warning(f"Could not parse email date: {date_string}")
            return None
