#!/bin/bash

# Industrial Ingestion Pipeline - One-Click Demo Setup
# This script sets up everything for your friend to run the demo

set -e

echo "Industrial Ingestion Pipeline - Demo Setup"
echo "============================================"
echo "Date: $(date)"
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Please install Docker first."
    echo "Visit: https://docs.docker.com/get-docker/"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "Docker Compose is not installed. Please install Docker Compose first."
    echo "Visit: https://docs.docker.com/compose/install/"
    exit 1
fi

echo "Docker and Docker Compose are installed"
echo ""

# Create necessary directories
echo "Creating directories..."
mkdir -p logs
mkdir -p data
echo "Directories created"
echo ""

# Build and start Docker containers
echo "Building and starting Docker containers..."
docker-compose up -d --build
echo "Docker containers started"
echo ""

# Wait for containers to be ready
echo "Waiting for containers to be ready..."
sleep 10

# Check container health
echo "Checking container health..."
docker-compose ps
echo ""

# Create virtual environment
echo "Creating Python virtual environment..."
python3 -m venv venv || python -m venv venv || echo "Could not create virtual environment, using system Python"
echo "Virtual environment created"
echo ""

# Activate virtual environment and install dependencies
echo "Installing Python dependencies..."
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
fi

pip install --upgrade pip
pip install -r requirements.txt
echo "Dependencies installed"
echo ""

# Create environment file template
echo "Creating environment configuration..."
if [ ! -f ".env" ]; then
    cp .env.example .env
    echo "Created .env file from template"
    echo "IMPORTANT: Edit .env file with your Gmail credentials:"
    echo "   - IMAP_USERNAME: Your Gmail address"
    echo "   - IMAP_PASSWORD: Your Gmail App Password"
    echo "   - Generate App Password: https://myaccount.google.com/apppasswords"
else
    echo ".env file already exists"
fi
echo ""

# Initialize database
echo "Initializing database..."
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
fi

python -c "
import os
from dotenv import load_dotenv
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
"
echo ""

# Test the setup
echo "Testing the setup..."
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
fi

python -c "
import os
from dotenv import load_dotenv
load_dotenv()

# Test database connection
from src.database.sqlite_connection import sqlite_manager
result = sqlite_manager.execute_query('SELECT COUNT(*) as count FROM ingestion_files')
print(f'Database connection: {result[0][\"count\"]} files in database')

# Test IMAP configuration
if os.getenv('IMAP_USERNAME') and os.getenv('IMAP_PASSWORD'):
    print('IMAP configuration: Configured')
else:
    print('IMAP configuration: Please update .env file with Gmail credentials')

print('Setup test completed successfully')
"
echo ""

# Start the demo
echo "Starting the demo..."
echo ""
echo "Demo Instructions:"
echo "1. Send CSV files to jmashoana@gmail.com"
echo "2. Watch the dashboard for real-time processing"
echo "3. Observe file processing and metrics"
echo ""
echo "Test files available in data/ directory:"
echo "   - test_batch_01_high_success.csv (100% success)"
echo "   - test_batch_02_mixed_results.csv (50% success)"
echo "   - test_batch_03_high_failure.csv (0% success)"
echo "   - test_batch_04_edge_cases.csv (edge cases)"
echo ""
echo "Dashboard updates every 10 seconds"
echo "Watch real-time processing and metrics"
echo ""

if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
fi

python start_demo.py
