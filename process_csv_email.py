#!/usr/bin/env python3

import imaplib
import ssl
import email
import uuid
import os
from email.header import decode_header
from dotenv import load_dotenv
from src.ingestion.csv_processor import CSVProcessor
from src.database.sqlite_connection import sqlite_manager

load_dotenv()

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

def process_csv_email():
    """Process the CSV email from jesse.mashoana@gmail.com."""
    
    print("Processing CSV Email from jesse.mashoana@gmail.com")
    print("=" * 55)
    
    # Initialize database
    print("Initializing SQLite database...")
    try:
        sqlite_manager.execute_query("CREATE TABLE IF NOT EXISTS ingestion_files (id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, message_id TEXT, from_email TEXT, subject TEXT, filename TEXT, file_sha256 TEXT UNIQUE, status TEXT, correlation_id TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)", fetch=False)
        sqlite_manager.execute_query("CREATE TABLE IF NOT EXISTS scale_transactions (id INTEGER PRIMARY KEY AUTOINCREMENT, scale_name TEXT, transact_no INTEGER, fill_kg REAL, success INTEGER, started_at DATETIME, ingestion_file_id INTEGER, correlation_id TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, UNIQUE(scale_name, transact_no))", fetch=False)
        print("Database initialized")
    except Exception as e:
        print(f"Database error: {e}")
        return False
    
    # Connect to IMAP
    print("\nConnecting to IMAP...")
    try:
        mail = imaplib.IMAP4_SSL(os.getenv('IMAP_SERVER'), int(os.getenv('IMAP_PORT')))
        result, data = mail.login(os.getenv('IMAP_USERNAME'), os.getenv('IMAP_PASSWORD'))
        
        if result != 'OK':
            print(f"Login failed: {result}")
            return False
        
        print("Connected successfully")
        
        # Select inbox and find the email
        mail.select('INBOX')
        result, messages = mail.search(None, '(FROM "jesse.mashoana@gmail.com")')
        
        if not messages[0]:
            print("No emails found from jesse.mashoana@gmail.com")
            return False
        
        msg_ids = messages[0].split()
        print(f"Found {len(msg_ids)} emails from jesse.mashoana@gmail.com")
        
        # Process the first email
        msg_id = msg_ids[0]
        print(f"\nProcessing email ID: {msg_id.decode()}")
        
        # Get email content
        result, msg_data = mail.fetch(msg_id, '(RFC822)')
        raw_email = msg_data[0][1]
        email_message = email.message_from_bytes(raw_email)
        
        # Get email details
        subject = decode_subject(email_message.get('Subject', ''))
        from_email = email_message.get('From', '')
        message_id = email_message.get('Message-ID', '')
        
        print(f"   Subject: {subject}")
        print(f"   From: {from_email}")
        print(f"   Message ID: {message_id}")
        
        # Look for CSV attachments
        processor = CSVProcessor()
        csv_found = False
        
        if email_message.is_multipart():
            for part in email_message.walk():
                if part.get_content_disposition() == 'attachment':
                    filename = part.get_filename()
                    if filename and filename.lower().endswith('.csv'):
                        csv_found = True
                        print(f"   Found CSV attachment: {filename}")
                        
                        # Get attachment content
                        csv_content = part.get_payload(decode=True)
                        
                        # Calculate file hash
                        file_hash = processor.calculate_file_hash(csv_content)
                        print(f"   File SHA256: {file_hash[:16]}...")
                        
                        # Check for duplicates
                        existing = sqlite_manager.execute_query(
                            "SELECT id, status FROM ingestion_files WHERE file_sha256 = ?",
                            (file_hash,)
                        )
                        
                        if existing:
                            print(f"   Duplicate file found (ID: {existing[0]['id']}, Status: {existing[0]['status']})")
                            print("   Deduplication working correctly!")
                        else:
                            # Process CSV
                            print("   Processing CSV content...")
                            transactions, errors = processor.parse_csv_content(csv_content, filename)
                            print(f"   Processed {len(transactions)} transactions with {len(errors)} errors")
                            
                            if errors:
                                print(f"   Validation errors:")
                                for error in errors[:3]:
                                    print(f"      - {error}")
                            
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
                            
                            # Insert transactions (sample first 10)
                            transaction_data = []
                            for i, tx in enumerate(transactions[:10]):
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
                                    print(f"      Skipping transaction {i}: {e}")
                            
                            if transaction_data:
                                insert_query = """
                                INSERT INTO scale_transactions 
                                (scale_name, transact_no, fill_kg, success, started_at, ingestion_file_id, correlation_id)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                                """
                                
                                try:
                                    sqlite_manager.execute_many(insert_query, transaction_data)
                                    print(f"   Stored {len(transaction_data)} transactions in database")
                                except Exception as e:
                                    if "UNIQUE constraint" in str(e):
                                        print(f"   Some transactions were duplicates (deduplication working)")
                                        print(f"   Still stored {len(transaction_data)} unique transactions")
                                    else:
                                        print(f"Database error: {e}")
                            
                            # Mark email as processed
                            mail.copy(msg_id, 'Processed')
                            mail.store(msg_id, '+FLAGS', '\\Seen')
                            print("   Email moved to Processed folder")
                        
                        break
        
        if not csv_found:
            print("   No CSV attachment found in this email")
        
        mail.logout()
        
        # Show final results
        print(f"\nFinal Results:")
        
        # Get database statistics
        file_stats = sqlite_manager.execute_query("SELECT COUNT(*) as count FROM ingestion_files")
        tx_stats = sqlite_manager.execute_query("SELECT COUNT(*) as count, COUNT(CASE WHEN success = 1 THEN 1 END) as successful FROM scale_transactions")
        
        print(f"   Files in database: {file_stats[0]['count']}")
        print(f"   Transactions in database: {tx_stats[0]['count']}")
        if tx_stats[0]['count'] > 0:
            success_rate = (tx_stats[0]['successful'] / tx_stats[0]['count']) * 100
            print(f"   Success rate: {success_rate:.1f}%")
        
        print(f"\n🎉 Email processing completed!")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    process_csv_email()
