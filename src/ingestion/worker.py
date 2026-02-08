"""
Main ingestion worker that orchestrates the entire pipeline.
"""

import os
import time
import logging
import uuid
from typing import Optional
from datetime import datetime

from src.ingestion.imap_client import IMAPClient
from src.ingestion.csv_processor import CSVProcessor
from src.ingestion.database_operations import DatabaseOperations
from src.database.connection import db_manager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


class IngestionWorker:
    """Main worker that orchestrates the ingestion pipeline."""
    
    def __init__(self):
        self.imap_client = IMAPClient()
        self.csv_processor = CSVProcessor()
        self.db_ops = DatabaseOperations()
        
        self.polling_interval = int(os.getenv('INGESTION_INTERVAL_SECONDS', '60'))
        self.processed_folder = os.getenv('IMAP_PROCESSED_FOLDER', 'Processed')
        self.failed_folder = os.getenv('IMAP_FAILED_FOLDER', 'Failed')
        self.duplicate_folder = os.getenv('IMAP_DUPLICATE_FOLDER', 'Duplicates')
        
        logger.info("Ingestion worker initialized")
    
    def run_once(self) -> None:
        """Run a single ingestion cycle."""
        logger.info("Starting ingestion cycle")
        
        try:
            with self.imap_client:
                # Find emails with CSV attachments
                csv_emails = self.imap_client.search_unread_emails()
                
                if not csv_emails:
                    logger.info("No emails with CSV attachments found")
                    return
                
                logger.info(f"Processing {len(csv_emails)} emails")
                
                for email_id in csv_emails:
                    try:
                        self._process_email(email_id)
                    except Exception as e:
                        logger.error(f"Failed to process email {email_id}: {e}")
                        # Continue with next email
                        continue
                
        except Exception as e:
            logger.error(f"Ingestion cycle failed: {e}")
            raise
    
    def _process_email(self, email_id: bytes) -> None:
        """Process a single email with CSV attachments."""
        correlation_id = str(uuid.uuid4())
        logger.info(f"Processing email {email_id} - Correlation ID: {correlation_id}")
        
        try:
            # Download CSV attachments
            metadata, attachments = self.imap_client.download_csv_attachments(email_id)
            
            if not attachments:
                logger.warning(f"No CSV attachments found in email {email_id}")
                self.imap_client.move_email(email_id, self.failed_folder)
                return
            
            # Process each attachment
            for filename, content in attachments:
                try:
                    self._process_csv_attachment(
                        filename, content, metadata, correlation_id, email_id
                    )
                except Exception as e:
                    logger.error(f"Failed to process attachment {filename}: {e}")
                    # Continue with other attachments
                    continue
            
            # Move email to processed folder if all attachments succeeded
            self.imap_client.move_email(email_id, self.processed_folder)
            logger.info(f"Email {email_id} moved to processed folder")
            
        except Exception as e:
            logger.error(f"Failed to process email {email_id}: {e}")
            try:
                self.imap_client.move_email(email_id, self.failed_folder)
            except Exception as move_error:
                logger.error(f"Failed to move email to failed folder: {move_error}")
    
    def _process_csv_attachment(
        self, 
        filename: str, 
        content: bytes, 
        email_metadata: dict,
        correlation_id: str,
        email_id: bytes
    ) -> None:
        """Process a single CSV attachment."""
        logger.info(f"Processing CSV attachment: {filename}")
        
        # Calculate file hash for deduplication
        file_sha256 = self.csv_processor.calculate_file_hash(content)
        
        # Check for duplicates
        existing_file = self.db_ops.check_file_duplicate(file_sha256)
        if existing_file:
            logger.info(f"Duplicate file detected: {filename} (SHA256: {file_sha256[:16]}...)")
            # Don't process duplicates, but still move email
            return
        
        # Create ingestion file record
        metadata = {
            'source': 'imap',
            'message_id': email_metadata.get('message_id'),
            'from_email': email_metadata.get('from_email'),
            'subject': email_metadata.get('subject'),
            'received_at': email_metadata.get('received_at'),
            'filename': filename
        }
        
        try:
            file_id = self.db_ops.create_ingestion_file(file_sha256, metadata, correlation_id)
            
            # Update status to processing
            self.db_ops.update_file_status(file_id, 'PROCESSING')
            self.db_ops.set_file_processing_times(file_id, started=True)
            
            # Validate file structure
            is_valid, validation_errors = self.csv_processor.validate_file_structure(content, filename)
            
            if not is_valid:
                error_msg = f"File validation failed: {'; '.join(validation_errors)}"
                logger.error(f"File validation failed for {filename}: {validation_errors}")
                self.db_ops.update_file_status(file_id, 'FAILED', error_msg)
                self.db_ops.set_file_processing_times(file_id, completed=True)
                return
            
            # Parse and validate CSV content
            transactions, parse_errors = self.csv_processor.parse_csv_content(content, filename)
            
            if parse_errors:
                logger.warning(f"CSV {filename} has {len(parse_errors)} validation errors")
            
            if not transactions:
                error_msg = "No valid transactions found in CSV"
                logger.error(f"No valid transactions in {filename}")
                self.db_ops.update_file_status(file_id, 'FAILED', error_msg)
                self.db_ops.set_file_processing_times(file_id, completed=True)
                return
            
            # Insert transactions into database
            inserted_count = self.db_ops.insert_transactions(file_id, transactions)
            
            # Update status to completed
            self.db_ops.update_file_status(file_id, 'COMPLETED')
            self.db_ops.set_file_processing_times(file_id, completed=True)
            
            # Get and log statistics
            stats = self.db_ops.get_file_statistics(file_id)
            logger.info(f"Successfully processed {filename}: "
                       f"{inserted_count} transactions, "
                       f"success rate: {stats.get('success_count', 0)}/{stats.get('transaction_count', 0)}")
            
        except Exception as e:
            logger.error(f"Failed to process CSV {filename}: {e}")
            
            # Try to update file status to failed
            try:
                if 'file_id' in locals():
                    self.db_ops.update_file_status(file_id, 'FAILED', str(e))
                    self.db_ops.set_file_processing_times(file_id, completed=True)
            except Exception as status_error:
                logger.error(f"Failed to update file status: {status_error}")
            
            raise
    
    def run_continuous(self) -> None:
        """Run the worker continuously with polling."""
        logger.info(f"Starting continuous ingestion worker (interval: {self.polling_interval}s)")
        
        try:
            while True:
                try:
                    self.run_once()
                except Exception as e:
                    logger.error(f"Ingestion cycle failed: {e}")
                
                # Wait for next cycle
                logger.info(f"Waiting {self.polling_interval} seconds for next cycle")
                time.sleep(self.polling_interval)
                
        except KeyboardInterrupt:
            logger.info("Ingestion worker stopped by user")
        except Exception as e:
            logger.error(f"Ingestion worker crashed: {e}")
            raise
        finally:
            # Cleanup database connections
            db_manager.close_all_connections()
    
    def health_check(self) -> bool:
        """Perform health check of all components."""
        try:
            # Check database
            db_healthy = db_manager.health_check()
            
            # Check IMAP connection (quick test)
            try:
                with self.imap_client:
                    imap_healthy = True
            except Exception:
                imap_healthy = False
            
            healthy = db_healthy and imap_healthy
            
            logger.info(f"Health check - Database: {db_healthy}, IMAP: {imap_healthy}, Overall: {healthy}")
            
            return healthy
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False


def main():
    """Main entry point for the ingestion worker."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run the ingestion worker')
    parser.add_argument('--once', action='store_true', help='Run once and exit')
    parser.add_argument('--health-check', action='store_true', help='Perform health check')
    
    args = parser.parse_args()
    
    worker = IngestionWorker()
    
    if args.health_check:
        healthy = worker.health_check()
        exit(0 if healthy else 1)
    elif args.once:
        try:
            worker.run_once()
            print("✅ Ingestion cycle completed successfully")
        except Exception as e:
            print(f"❌ Ingestion cycle failed: {e}")
            exit(1)
    else:
        worker.run_continuous()


if __name__ == "__main__":
    main()
