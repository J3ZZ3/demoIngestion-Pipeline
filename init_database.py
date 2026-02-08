#!/usr/bin/env python3

"""
Initialize database for the demo.
"""

import os
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Initialize SQLite database
from src.database.sqlite_connection import sqlite_manager

# Create tables
sqlite_manager.execute_query('''
CREATE TABLE IF NOT EXISTS ingestion_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT,
    message_id TEXT,
    from_email TEXT,
    subject TEXT,
    filename TEXT,
    file_sha256 TEXT UNIQUE,
    status TEXT,
    correlation_id TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)''', fetch=False)

sqlite_manager.execute_query('''
CREATE TABLE IF NOT EXISTS scale_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scale_name TEXT,
    transact_no INTEGER,
    fill_kg REAL,
    success INTEGER,
    started_at DATETIME,
    ingestion_file_id INTEGER,
    correlation_id TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(scale_name, transact_no)
)''', fetch=False)

print('Database initialized successfully')
