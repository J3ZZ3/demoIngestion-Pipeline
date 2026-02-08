"""
IMAP client for downloading CSV attachments from email.
"""

import os
import imaplib
import email
import logging
from email.header import decode_header
from typing import List, Optional, Tuple, Dict, Any
from pathlib import Path
import tempfile
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class IMAPClient:
    """IMAP client for downloading CSV attachments."""
    
    def __init__(self):
        self.imap_server = os.getenv('IMAP_SERVER')
        self.imap_port = int(os.getenv('IMAP_PORT', '993'))
        self.imap_username = os.getenv('IMAP_USERNAME')
        self.imap_password = os.getenv('IMAP_PASSWORD')
        self.imap_use_ssl = os.getenv('IMAP_USE_SSL', 'true').lower() == 'true'
        
        self.inbox = os.getenv('IMAP_INBOX', 'INBOX')
        self.processed_folder = os.getenv('IMAP_PROCESSED_FOLDER', 'Processed')
        self.failed_folder = os.getenv('IMAP_FAILED_FOLDER', 'Failed')
        self.duplicate_folder = os.getenv('IMAP_DUPLICATE_FOLDER', 'Duplicates')
        
        if not all([self.imap_server, self.imap_username, self.imap_password]):
            raise ValueError("IMAP_SERVER, IMAP_USERNAME, and IMAP_PASSWORD are required")
        
        self.connection: Optional[imaplib.IMAP4] = None
    
    def connect(self) -> None:
        """Connect to the IMAP server."""
        try:
            if self.imap_use_ssl:
                self.connection = imaplib.IMAP4_SSL(self.imap_server, self.imap_port)
            else:
                self.connection = imaplib.IMAP4(self.imap_server, self.imap_port)
            
            self.connection.login(self.imap_username, self.imap_password)
            logger.info(f"Connected to IMAP server: {self.imap_server}")
            
        except Exception as e:
            logger.error(f"Failed to connect to IMAP server: {e}")
            raise
    
    def disconnect(self) -> None:
        """Disconnect from the IMAP server."""
        if self.connection:
            try:
                self.connection.close()
                self.connection.logout()
                logger.info("Disconnected from IMAP server")
            except Exception as e:
                logger.error(f"Error disconnecting from IMAP server: {e}")
            finally:
                self.connection = None
    
    def ensure_folders_exist(self) -> None:
        """Ensure required folders exist on the server."""
        if not self.connection:
            raise RuntimeError("Not connected to IMAP server")
        
        folders = [self.processed_folder, self.failed_folder, self.duplicate_folder]
        
        for folder in folders:
            try:
                # Try to select the folder
                self.connection.select(f'"{folder}"')
            except Exception:
                # Folder doesn't exist, create it
                try:
                    self.connection.create(f'"{folder}"')
                    logger.info(f"Created folder: {folder}")
                except Exception as e:
                    logger.error(f"Failed to create folder {folder}: {e}")
    
    def search_unread_emails(self) -> List[bytes]:
        """Search for unread emails with CSV attachments."""
        if not self.connection:
            raise RuntimeError("Not connected to IMAP server")
        
        try:
            self.connection.select(self.inbox)
            
            # Search for unread emails
            status, email_ids = self.connection.search(None, 'UNSEEN')
            
            if status != 'OK':
                logger.error("Failed to search for emails")
                return []
            
            email_ids = email_ids[0].split()
            logger.info(f"Found {len(email_ids)} unread emails")
            
            # Filter emails with CSV attachments
            csv_emails = []
            for email_id in email_ids:
                if self._has_csv_attachment(email_id):
                    csv_emails.append(email_id)
            
            logger.info(f"Found {len(csv_emails)} emails with CSV attachments")
            return csv_emails
            
        except Exception as e:
            logger.error(f"Error searching for emails: {e}")
            return []
    
    def _has_csv_attachment(self, email_id: bytes) -> bool:
        """Check if an email has CSV attachments."""
        try:
            status, msg_data = self.connection.fetch(email_id, '(RFC822)')
            
            if status != 'OK':
                return False
            
            email_body = msg_data[0][1]
            email_message = email.message_from_bytes(email_body)
            
            # Check for CSV attachments
            for part in email_message.walk():
                if part.get_content_disposition() == 'attachment':
                    filename = part.get_filename()
                    if filename and filename.lower().endswith('.csv'):
                        return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking email {email_id}: {e}")
            return False
    
    def download_csv_attachments(self, email_id: bytes) -> Tuple[Dict[str, Any], List[Tuple[str, bytes]]]:
        """Download CSV attachments from an email."""
        if not self.connection:
            raise RuntimeError("Not connected to IMAP server")
        
        try:
            status, msg_data = self.connection.fetch(email_id, '(RFC822)')
            
            if status != 'OK':
                raise RuntimeError(f"Failed to fetch email {email_id}")
            
            email_body = msg_data[0][1]
            email_message = email.message_from_bytes(email_body)
            
            # Extract email metadata
            metadata = {
                'message_id': email_message.get('Message-ID', ''),
                'from_email': email_message.get('From', ''),
                'subject': self._decode_header(email_message.get('Subject', '')),
                'received_at': email_message.get('Date', ''),
                'to_email': email_message.get('To', '')
            }
            
            # Download CSV attachments
            attachments = []
            for part in email_message.walk():
                if part.get_content_disposition() == 'attachment':
                    filename = part.get_filename()
                    if filename and filename.lower().endswith('.csv'):
                        decoded_filename = self._decode_header(filename)
                        content = part.get_payload(decode=True)
                        attachments.append((decoded_filename, content))
            
            logger.info(f"Downloaded {len(attachments)} CSV attachments from email {email_id}")
            return metadata, attachments
            
        except Exception as e:
            logger.error(f"Error downloading attachments from email {email_id}: {e}")
            raise
    
    def _decode_header(self, header: str) -> str:
        """Decode email header to handle encoding."""
        if not header:
            return ""
        
        decoded_parts = decode_header(header)
        decoded_string = ""
        
        for part, encoding in decoded_parts:
            if isinstance(part, bytes):
                if encoding:
                    try:
                        decoded_string += part.decode(encoding)
                    except (UnicodeDecodeError, LookupError):
                        decoded_string += part.decode('utf-8', errors='ignore')
                else:
                    decoded_string += part.decode('utf-8', errors='ignore')
            else:
                decoded_string += part
        
        return decoded_string
    
    def move_email(self, email_id: bytes, destination_folder: str) -> None:
        """Move an email to a different folder."""
        if not self.connection:
            raise RuntimeError("Not connected to IMAP server")
        
        try:
            # Copy email to destination folder
            self.connection.copy(email_id, destination_folder)
            
            # Mark original email for deletion
            self.connection.store(email_id, '+FLAGS', '\\Deleted')
            
            # Expunge to actually delete the original
            self.connection.expunge()
            
            logger.info(f"Moved email {email_id} to {destination_folder}")
            
        except Exception as e:
            logger.error(f"Error moving email {email_id} to {destination_folder}: {e}")
            raise
    
    def mark_as_read(self, email_id: bytes) -> None:
        """Mark an email as read without moving it."""
        if not self.connection:
            raise RuntimeError("Not connected to IMAP server")
        
        try:
            self.connection.store(email_id, '+FLAGS', '\\Seen')
            logger.info(f"Marked email {email_id} as read")
            
        except Exception as e:
            logger.error(f"Error marking email {email_id} as read: {e}")
            raise
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        self.ensure_folders_exist()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
