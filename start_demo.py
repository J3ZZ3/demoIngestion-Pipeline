#!/usr/bin/env python3

"""
One-click demo launcher for the industrial ingestion pipeline.
"""

import subprocess
import time
import os
import sys
from datetime import datetime

def print_banner():
    """Print demo startup banner."""
    print("INDUSTRIAL INGESTION PIPELINE - DEMO LAUNCHER")
    print("=" * 60)
    print(f"Demo Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Status: Initializing Demo Environment...")
    print()

def check_components():
    """Check all demo components are ready."""
    print("Checking Demo Components...")
    
    # Check database
    try:
        from src.database.sqlite_connection import sqlite_manager
        result = sqlite_manager.execute_query("SELECT COUNT(*) as count FROM ingestion_files")
        print(f"   Database: Connected ({result[0]['count']} files in database)")
    except Exception as e:
        print(f"   Database: Error - {e}")
        return False
    
    # Check environment
    try:
        from dotenv import load_dotenv
        load_dotenv()
        required_vars = ['IMAP_SERVER', 'IMAP_USERNAME', 'IMAP_PASSWORD']
        for var in required_vars:
            if not os.getenv(var):
                print(f"   Environment: Missing {var}")
                return False
        print(f"   Environment: Configured")
    except Exception as e:
        print(f"   Environment: Error - {e}")
        return False
    
    # Check test files
    test_files = [
        'data/test_batch_01_high_success.csv',
        'data/test_batch_02_mixed_results.csv',
        'data/test_batch_03_high_failure.csv',
        'data/test_batch_04_edge_cases.csv'
    ]
    
    for file in test_files:
        if not os.path.exists(file):
            print(f"   Test Files: Missing {file}")
            return False
    
    print(f"   Test Files: All 4 files ready")
    return True

def start_demo():
    """Start the demo environment."""
    print("\nStarting Demo Environment...")
    
    # Create logs directory
    os.makedirs('logs', exist_ok=True)
    
    # Start the demo dashboard
    print("Starting Demo Dashboard...")
    try:
        subprocess.run([sys.executable, 'demo_dashboard.py'], check=True)
    except KeyboardInterrupt:
        print("\nDemo completed successfully!")
    except Exception as e:
        print(f"Demo error: {e}")

def show_demo_instructions():
    """Show demo instructions."""
    print("\nDEMO INSTRUCTIONS")
    print("-" * 40)
    print("1. Send CSV files to: jmashoana@gmail.com")
    print("2. Watch the dashboard update in real-time")
    print("3. Observe file processing and metrics")
    print("4. See deduplication and error handling")
    print("5. Monitor scale performance analytics")
    print()
    print("Test Files Available:")
    print("   • test_batch_01_high_success.csv")
    print("   • test_batch_02_mixed_results.csv")
    print("   • test_batch_03_high_failure.csv")
    print("   • test_batch_04_edge_cases.csv")
    print()
    print("Demo Features:")
    print("   Real-time processing")
    print("   Automatic deduplication")
    print("   Error handling & validation")
    print("   Performance metrics")
    print("   Scale analytics")
    print("   Audit trail tracking")
    print()

def main():
    """Main demo launcher function."""
    print_banner()
    
    if not check_components():
        print("\nDemo setup incomplete. Please check the errors above.")
        return
    
    show_demo_instructions()
    
    print("Demo Environment Ready!")
    print("Starting dashboard in 3 seconds...")
    time.sleep(3)
    
    start_demo()

if __name__ == "__main__":
    main()
