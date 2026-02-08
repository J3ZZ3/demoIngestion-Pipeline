#!/usr/bin/env python3

"""
Test the demo setup.
"""

import os
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Test database connection
from src.database.sqlite_connection import sqlite_manager
result = sqlite_manager.execute_query('SELECT COUNT(*) as count FROM ingestion_files')
print(f'Database connection: {result[0]["count"]} files in database')

# Test IMAP configuration
if os.getenv('IMAP_USERNAME') and os.getenv('IMAP_PASSWORD'):
    print('IMAP configuration: Configured')
else:
    print('IMAP configuration: Please update .env file with Gmail credentials')

print('Setup test completed successfully')
