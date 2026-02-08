#!/usr/bin/env python3

"""
Continuous ingestion worker that processes emails automatically.
"""

import time
import logging
import imaplib
import ssl
import email
import uuid
import os
from email.header import decode_header
from dotenv import load_dotenv
from src.ingestion.csv_processor import CSVProcessor
from src.database.sqlite_connection import sqlite_manager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/ingestion_worker.log'),
        logging.StreamHandler()
    ]
)

load_dotenv()
logger = logging.getLogger(__name__)

def decode_subject(subject):
    """Decode email subject."""
    if subject:
        decoded_parts = decode_header(subject)
        subject = ''
        for part, encoding in decoded_parts:
            if isinstance(part, bytes):
                subject += part.decode(encoding or 'utf-8', errors='ignore')
            else:
                subject += part
    return subject

def ensure_folders_exist(mail):
    """Ensure required IMAP folders exist."""
    folders = ['Processed', 'Failed', 'Duplicates']
    for folder in folders:
        try:
            mail.create(folder)
        except:
            pass  # Folder might already exist

def process_email(mail, msg_id):
    """Process a single email."""
    try:
        # Get email content
        result, msg_data = mail.fetch(msg_id, '(RFC822)')
        raw_email = msg_data[0][1]
        email_message = email.message_from_bytes(raw_email)
        
        # Get email details
        subject = decode_subject(email_message.get('Subject', ''))
        from_email = email_message.get('From', '')
        message_id = email_message.get('Message-ID', '')
        
        logger.info(f"Processing email: {subject} from {from_email}")
        
        # Look for CSV attachments
        processor = CSVProcessor()
        csv_processed = False
        
        if email_message.is_multipart():
            for part in email_message.walk():
                if part.get_content_disposition() == 'attachment':
                    filename = part.get_filename()
                    if filename and filename.lower().endswith('.csv'):
                        csv_processed = True
                        logger.info(f"Found CSV attachment: {filename}")
                        
                        # Get attachment content
                        csv_content = part.get_payload(decode=True)
                        
                        # Calculate file hash
                        file_hash = processor.calculate_file_hash(csv_content)
                        logger.info(f"File SHA256: {file_hash[:16]}...")
                        
                        # Check for duplicates
                        existing = sqlite_manager.execute_query(
                            "SELECT id, status FROM ingestion_files WHERE file_sha256 = ?",
                            (file_hash,)
                        )
                        
                        if existing:
                            logger.info(f"Duplicate file found (ID: {existing[0]['id']}, Status: {existing[0]['status']})")
                            # Move to duplicates folder
                            mail.copy(msg_id, 'Duplicates')
                            mail.store(msg_id, '+FLAGS', '\\Seen')
                            logger.info("Email moved to Duplicates folder")
                        else:
                            # Process CSV
                            transactions, errors = processor.parse_csv_content(csv_content, filename)
                            logger.info(f"Processed {len(transactions)} transactions with {len(errors)} errors")
                            
                            if errors:
                                logger.warning(f"Validation errors: {errors[:3]}")
                                # Move to failed folder
                                mail.copy(msg_id, 'Failed')
                                mail.store(msg_id, '+FLAGS', '\\Seen')
                                logger.info("Email moved to Failed folder due to validation errors")
                            else:
                                # Store in database
                                correlation_id = str(uuid.uuid4())
                                
                                # Create file record
                                sqlite_manager.execute_query(
                                    "INSERT INTO ingestion_files (source, message_id, from_email, subject, filename, file_sha256, status, correlation_id) VALUES (?, ?, ?, ?, ?, ?, 'COMPLETED', ?)",
                                    ('imap', message_id, from_email, subject, filename, file_hash, correlation_id),
                                    fetch=False
                                )
                                
                                # Get file ID
                                result = sqlite_manager.execute_query(
                                    "SELECT id FROM ingestion_files WHERE file_sha256 = ?",
                                    (file_hash,)
                                )
                                file_id = result[0]['id']
                                
                                # Insert transactions
                                transaction_data = []
                                for tx in transactions:
                                    try:
                                        transaction_data.append((
                                            tx.scale_name,
                                            tx.transact_no,
                                            tx.fill_kg,
                                            1 if tx.success else 0,
                                            tx.started_at,
                                            file_id,
                                            str(uuid.uuid4())
                                        ))
                                    except Exception as e:
                                        logger.warning(f"Skipping transaction: {e}")
                                
                                if transaction_data:
                                    insert_query = """
                                    INSERT INTO scale_transactions 
                                    (scale_name, transact_no, fill_kg, success, started_at, ingestion_file_id, correlation_id)
                                    VALUES (?, ?, ?, ?, ?, ?, ?)
                                    """
                                    
                                    try:
                                        sqlite_manager.execute_many(insert_query, transaction_data)
                                        logger.info(f"Stored {len(transaction_data)} transactions in database")
                                        
                                        # Move to processed folder
                                        mail.copy(msg_id, 'Processed')
                                        mail.store(msg_id, '+FLAGS', '\\Seen')
                                        logger.info("Email moved to Processed folder")
                                        
                                    except Exception as e:
                                        if "UNIQUE constraint" in str(e):
                                            logger.warning("Some transactions were duplicates (deduplication working)")
                                            mail.copy(msg_id, 'Processed')
                                            mail.store(msg_id, '+FLAGS', '\\Seen')
                                            logger.info("Email moved to Processed folder (with some duplicates)")
                                        else:
                                            logger.error(f"Database error: {e}")
                                            mail.copy(msg_id, 'Failed')
                                            mail.store(msg_id, '+FLAGS', '\\Seen')
                                            logger.info("Email moved to Failed folder due to database error")
                        
                        break
        
        if not csv_processed:
            logger.info("No CSV attachment found in email")
        
        return csv_processed
        
    except Exception as e:
        logger.error(f"Error processing email: {e}")
        return False

def run_continuous_worker():
    """Run the continuous ingestion worker."""
    logger.info("🚀 Starting Continuous Ingestion Worker")
    logger.info("📧 Monitoring jmashoana@gmail.com for CSV attachments")
    logger.info("🔄 Checking for new emails every 30 seconds")
    
    # Ensure log directory exists
    os.makedirs('logs', exist_ok=True)
    
    while True:
        try:
            # Connect to IMAP
            mail = imaplib.IMAP4_SSL(os.getenv('IMAP_SERVER'), int(os.getenv('IMAP_PORT')))
            result, data = mail.login(os.getenv('IMAP_USERNAME'), os.getenv('IMAP_PASSWORD'))
            
            if result != 'OK':
                logger.error(f"IMAP login failed: {result}")
                time.sleep(60)
                continue
            
            # Ensure folders exist
            ensure_folders_exist(mail)
            
            # Select inbox
            mail.select('INBOX')
            
            # Search for unread emails
            result, messages = mail.search(None, '(UNSEEN)')
            
            if messages[0]:
                msg_ids = messages[0].split()
                logger.info(f"Found {len(msg_ids)} unread emails")
                
                processed_count = 0
                for msg_id in msg_ids:
                    if process_email(mail, msg_id):
                        processed_count += 1
                
                if processed_count > 0:
                    logger.info(f"Processed {processed_count} emails with CSV attachments")
            
            mail.logout()
            
        except Exception as e:
            logger.error(f"Worker error: {e}")
        
        # Wait before next check
        time.sleep(30)

if __name__ == "__main__":
    run_continuous_worker()
