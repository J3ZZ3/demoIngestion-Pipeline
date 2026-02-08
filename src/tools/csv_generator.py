"""
CSV generator for simulating Premier Scale data.
"""

import csv
import random
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any
import pytz

# Scale names for realistic simulation
SCALE_NAMES = [
    "Scale-01", "Scale-02", "Scale-03", "Scale-04", "Scale-05",
    "Scale-A", "Scale-B", "Scale-C", "Scale-D", "Scale-E"
]

# Cylinder sizes in kg
CYLINDER_SIZES = [6, 9, 15, 19, 48, 50]

# Johannesburg timezone
JOHANNESBURG_TZ = pytz.timezone('Africa/Johannesburg')


class CSVGenerator:
    """Generates realistic Premier Scale CSV data for testing."""
    
    def __init__(self):
        self.transaction_counter = 1000
    
    def generate_transaction(self, scale_name: str = None) -> Dict[str, Any]:
        """Generate a single scale transaction."""
        if scale_name is None:
            scale_name = random.choice(SCALE_NAMES)
        
        cyl_size = random.choice(CYLINDER_SIZES)
        tare_weight = round(random.uniform(5.0, 20.0), 2)
        fill_kg = round(random.uniform(cyl_size * 0.8, cyl_size * 1.1), 2)
        residual = round(random.uniform(0.0, 2.0), 2)
        success = random.choice([True, True, True, False])  # 75% success rate
        
        # Generate start time in Johannesburg timezone
        days_ago = random.randint(0, 30)
        hours_offset = random.randint(0, 23)
        minutes_offset = random.randint(0, 59)
        
        base_date = datetime.now(JOHANNESBURG_TZ) - timedelta(days=days_ago)
        start_time = base_date.replace(
            hour=hours_offset, 
            minute=minutes_offset, 
            second=0, 
            microsecond=0
        )
        
        fill_time_seconds = random.randint(30, 300) if success else random.randint(10, 60)
        
        transaction = {
            'TransactNo': self.transaction_counter,
            'Scale Name': scale_name,
            'CylSize': cyl_size,
            'TareWeight': f"{tare_weight}kg",
            'Fill kgs': f"{fill_kg}kg", 
            'Residual': f"{residual}kg",
            'Success': 'Y' if success else 'N',
            'Date Time Start': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'Fill Time': fill_time_seconds
        }
        
        self.transaction_counter += 1
        return transaction
    
    def generate_csv(self, output_path: str, num_transactions: int = 100) -> None:
        """Generate a CSV file with simulated scale data."""
        output_file = Path(output_path)
        
        # Ensure output directory exists
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        transactions = []
        for _ in range(num_transactions):
            transactions.append(self.generate_transaction())
        
        # Sort by start time for realistic data flow
        transactions.sort(key=lambda x: x['Date Time Start'])
        
        fieldnames = [
            'TransactNo', 'Scale Name', 'CylSize', 'TareWeight', 
            'Fill kgs', 'Residual', 'Success', 'Date Time Start', 'Fill Time'
        ]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(transactions)
        
        print(f"✅ Generated {num_transactions} transactions in {output_path}")
        
        # Print summary statistics
        success_count = sum(1 for t in transactions if t['Success'] == 'Y')
        print(f"   Success rate: {success_count}/{num_transactions} ({success_count/num_transactions*100:.1f}%)")
        print(f"   Scale names used: {set(t['Scale Name'] for t in transactions)}")
        print(f"   Date range: {transactions[0]['Date Time Start']} to {transactions[-1]['Date Time Start']}")
    
    def generate_test_scenarios(self, output_dir: str) -> None:
        """Generate various test scenarios."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Scenario 1: Normal data
        print("Generating normal data scenario...")
        self.generate_csv(output_path / "normal_data.csv", 50)
        
        # Scenario 2: High failure rate
        print("Generating high failure rate scenario...")
        original_success_bias = random.random
        random.random = lambda: random.random() * 0.3  # 30% success rate
        self.transaction_counter = 2000
        self.generate_csv(output_path / "high_failure_rate.csv", 50)
        random.random = original_success_bias
        
        # Scenario 3: Single scale focus
        print("Generating single scale scenario...")
        self.transaction_counter = 3000
        for scale in ["Scale-01"] * 50:
            transaction = self.generate_transaction(scale)
        # Generate file with single scale
        transactions = [self.generate_transaction("Scale-01") for _ in range(50)]
        
        fieldnames = [
            'TransactNo', 'Scale Name', 'CylSize', 'TareWeight', 
            'Fill kgs', 'Residual', 'Success', 'Date Time Start', 'Fill Time'
        ]
        
        with open(output_path / "single_scale.csv", 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(transactions)
        
        print(f"✅ Generated test scenarios in {output_path}")


def main():
    """Main entry point for CSV generation."""
    parser = argparse.ArgumentParser(description='Generate Premier Scale CSV data for testing')
    parser.add_argument('--output', '-o', required=True, help='Output CSV file path')
    parser.add_argument('--count', '-c', type=int, default=100, help='Number of transactions to generate')
    parser.add_argument('--scenarios', '-s', action='store_true', help='Generate test scenarios')
    
    args = parser.parse_args()
    
    generator = CSVGenerator()
    
    if args.scenarios:
        generator.generate_test_scenarios(Path(args.output).parent)
    else:
        generator.generate_csv(args.output, args.count)


if __name__ == "__main__":
    main()
