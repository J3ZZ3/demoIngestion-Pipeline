"""
Email sender for simulating CSV attachment delivery.
"""

import os
import smtplib
import argparse
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
from typing import Optional
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class EmailSender:
    """Sends emails with CSV attachments for testing the ingestion pipeline."""
    
    def __init__(self):
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.smtp_username = os.getenv('SMTP_USERNAME')
        self.smtp_password = os.getenv('SMTP_PASSWORD')
        self.smtp_use_tls = os.getenv('SMTP_USE_TLS', 'true').lower() == 'true'
        
        if not all([self.smtp_username, self.smtp_password]):
            raise ValueError("SMTP_USERNAME and SMTP_PASSWORD environment variables are required")
    
    def send_csv_email(
        self, 
        csv_file_path: str, 
        recipient_email: str,
        subject: Optional[str] = None,
        sender_name: Optional[str] = None
    ) -> None:
        """Send an email with CSV attachment."""
        csv_path = Path(csv_file_path)
        
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        if subject is None:
            subject = f"Premier Scale Data - {csv_path.stem}"
        
        if sender_name is None:
            sender_name = "Premier Scale System"
        
        # Create message
        msg = MIMEMultipart()
        msg['From'] = f"{sender_name} <{self.smtp_username}>"
        msg['To'] = recipient_email
        msg['Subject'] = subject
        
        # Email body
        body = f"""
Please process the attached Premier Scale CSV file.

File details:
- Filename: {csv_path.name}
- Generated: {csv_path.stat().st_mtime}

This is an automated message from the Premier Scale data export system.

Best regards,
{sender_name}
        """.strip()
        
        msg.attach(MIMEText(body, 'plain'))
        
        # Attach CSV file
        try:
            with open(csv_path, 'rb') as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
            
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename= {csv_path.name}'
            )
            msg.attach(part)
            
        except Exception as e:
            logger.error(f"Failed to attach CSV file: {e}")
            raise
        
        # Send email
        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                if self.smtp_use_tls:
                    server.starttls()
                
                server.login(self.smtp_username, self.smtp_password)
                server.send_message(msg)
            
            logger.info(f"Email sent successfully to {recipient_email}")
            print(f"✅ Email sent to {recipient_email} with attachment {csv_path.name}")
            
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            raise
    
    def send_test_emails(self, csv_dir: str, recipient_email: str) -> None:
        """Send multiple test emails with different CSV files."""
        csv_path = Path(csv_dir)
        
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV directory not found: {csv_path}")
        
        csv_files = list(csv_path.glob("*.csv"))
        
        if not csv_files:
            print(f"No CSV files found in {csv_dir}")
            return
        
        print(f"Found {len(csv_files)} CSV files to send...")
        
        for csv_file in csv_files:
            try:
                subject = f"Test Data - {csv_file.stem}"
                self.send_csv_email(str(csv_file), recipient_email, subject)
                
            except Exception as e:
                logger.error(f"Failed to send {csv_file.name}: {e}")
                print(f"❌ Failed to send {csv_file.name}: {e}")
        
        print(f"✅ Email sending completed. Check {recipient_email} inbox.")


def main():
    """Main entry point for email sending."""
    parser = argparse.ArgumentParser(description='Send CSV files via email for testing')
    parser.add_argument('--file', '-f', help='Single CSV file to send')
    parser.add_argument('--directory', '-d', help='Directory containing CSV files')
    parser.add_argument('--recipient', '-r', required=True, help='Recipient email address')
    parser.add_argument('--subject', '-s', help='Email subject (for single file)')
    
    args = parser.parse_args()
    
    if not args.file and not args.directory:
        print("Error: Either --file or --directory must be specified")
        exit(1)
    
    try:
        sender = EmailSender()
        
        if args.file:
            sender.send_csv_email(args.file, args.recipient, args.subject)
        else:
            sender.send_test_emails(args.directory, args.recipient)
            
    except Exception as e:
        logger.error(f"Email sending failed: {e}")
        print(f"❌ Email sending failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()
