"""
CSV processor for parsing and validating Premier Scale data.
"""

import csv
import hashlib
import logging
from io import StringIO
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import pytz
from pydantic import BaseModel, ValidationError, validator

logger = logging.getLogger(__name__)

# Johannesburg timezone
JOHANNESBURG_TZ = pytz.timezone('Africa/Johannesburg')


class ScaleTransaction(BaseModel):
    """Pydantic model for scale transaction validation."""
    
    scale_name: str
    transact_no: int
    cyl_size_kg: Optional[float]
    tare_weight_kg: Optional[float]
    fill_kg: Optional[float]
    residual_kg: Optional[float]
    success: bool
    started_at: datetime
    fill_time_seconds: int
    
    @validator('scale_name')
    def validate_scale_name(cls, v):
        if not v or not v.strip():
            raise ValueError('Scale name is required')
        return v.strip()
    
    @validator('transact_no')
    def validate_transact_no(cls, v):
        if v <= 0:
            raise ValueError('Transaction number must be positive')
        return v
    
    @validator('fill_time_seconds')
    def validate_fill_time(cls, v):
        if v < 0:
            raise ValueError('Fill time cannot be negative')
        return v


class CSVProcessor:
    """Processes and validates Premier Scale CSV data."""
    
    REQUIRED_COLUMNS = [
        'TransactNo', 'Scale Name', 'CylSize', 'TareWeight', 
        'Fill kgs', 'Residual', 'Success', 'Date Time Start', 'Fill Time'
    ]
    
    def __init__(self):
        self.validation_errors = []
    
    def calculate_file_hash(self, csv_content: bytes) -> str:
        """Calculate SHA256 hash of CSV content for deduplication."""
        return hashlib.sha256(csv_content).hexdigest()
    
    def parse_csv_content(self, csv_content: bytes, filename: str = None) -> Tuple[List[ScaleTransaction], List[str]]:
        """Parse CSV content and validate transactions."""
        self.validation_errors = []
        
        try:
            # Decode content and parse CSV
            csv_text = csv_content.decode('utf-8')
            csv_reader = csv.DictReader(StringIO(csv_text))
            
            # Validate required columns
            if not csv_reader.fieldnames:
                raise ValueError("CSV has no headers")
            
            missing_columns = set(self.REQUIRED_COLUMNS) - set(csv_reader.fieldnames)
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # Parse and validate each row
            transactions = []
            row_number = 0
            
            for row in csv_reader:
                row_number += 1
                
                try:
                    transaction = self._parse_row(row, row_number)
                    if transaction:
                        transactions.append(transaction)
                        
                except Exception as e:
                    error_msg = f"Row {row_number}: {str(e)}"
                    self.validation_errors.append(error_msg)
                    logger.warning(f"Validation error in {filename}: {error_msg}")
            
            if self.validation_errors:
                logger.warning(f"CSV {filename} has {len(self.validation_errors)} validation errors")
            
            logger.info(f"Parsed {len(transactions)} valid transactions from {filename}")
            return transactions, self.validation_errors
            
        except Exception as e:
            logger.error(f"Failed to parse CSV {filename}: {e}")
            raise
    
    def _parse_row(self, row: Dict[str, str], row_number: int) -> Optional[ScaleTransaction]:
        """Parse and validate a single CSV row."""
        try:
            # Extract and clean values
            transact_no = self._parse_int(row['TransactNo'], 'TransactNo', row_number)
            scale_name = self._parse_string(row['Scale Name'], 'Scale Name', row_number)
            cyl_size_kg = self._parse_float_optional(row['CylSize'], 'CylSize', row_number)
            tare_weight_kg = self._parse_weight(row['TareWeight'], 'TareWeight', row_number)
            fill_kg = self._parse_weight(row['Fill kgs'], 'Fill kgs', row_number)
            residual_kg = self._parse_weight(row['Residual'], 'Residual', row_number)
            success = self._parse_boolean(row['Success'], 'Success', row_number)
            started_at = self._parse_datetime(row['Date Time Start'], 'Date Time Start', row_number)
            fill_time_seconds = self._parse_int(row['Fill Time'], 'Fill Time', row_number)
            
            # Create validated transaction
            transaction = ScaleTransaction(
                scale_name=scale_name,
                transact_no=transact_no,
                cyl_size_kg=cyl_size_kg,
                tare_weight_kg=tare_weight_kg,
                fill_kg=fill_kg,
                residual_kg=residual_kg,
                success=success,
                started_at=started_at,
                fill_time_seconds=fill_time_seconds
            )
            
            return transaction
            
        except Exception as e:
            error_msg = f"Row {row_number}: {str(e)}"
            self.validation_errors.append(error_msg)
            return None
    
    def _parse_int(self, value: str, field_name: str, row_number: int) -> int:
        """Parse integer value with validation."""
        try:
            if not value or not value.strip():
                raise ValueError(f"{field_name} is required")
            
            return int(float(value.strip()))
            
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid {field_name}: '{value}' - {str(e)}")
    
    def _parse_float_optional(self, value: str, field_name: str, row_number: int) -> Optional[float]:
        """Parse optional float value."""
        try:
            if not value or not value.strip():
                return None
            
            return float(value.strip())
            
        except (ValueError, TypeError):
            raise ValueError(f"Invalid {field_name}: '{value}'")
    
    def _parse_weight(self, value: str, field_name: str, row_number: int) -> Optional[float]:
        """Parse weight value, handling 'kg' suffix."""
        try:
            if not value or not value.strip():
                return None
            
            # Remove 'kg' suffix and whitespace
            clean_value = value.strip().replace('kg', '').strip()
            
            if not clean_value:
                return None
            
            return float(clean_value)
            
        except (ValueError, TypeError):
            raise ValueError(f"Invalid {field_name}: '{value}' - expected numeric value")
    
    def _parse_string(self, value: str, field_name: str, row_number: int) -> str:
        """Parse string value with validation."""
        if not value or not value.strip():
            raise ValueError(f"{field_name} is required")
        
        return value.strip()
    
    def _parse_boolean(self, value: str, field_name: str, row_number: int) -> bool:
        """Parse boolean value from Y/N or True/False."""
        if not value or not value.strip():
            raise ValueError(f"{field_name} is required")
        
        clean_value = value.strip().upper()
        
        if clean_value in ['Y', 'YES', 'TRUE', '1']:
            return True
        elif clean_value in ['N', 'NO', 'FALSE', '0']:
            return False
        else:
            raise ValueError(f"Invalid {field_name}: '{value}' - expected Y/N or True/False")
    
    def _parse_datetime(self, value: str, field_name: str, row_number: int) -> datetime:
        """Parse datetime from Johannesburg timezone and convert to UTC."""
        if not value or not value.strip():
            raise ValueError(f"{field_name} is required")
        
        try:
            # Parse datetime in Johannesburg timezone
            naive_dt = datetime.strptime(value.strip(), '%Y-%m-%d %H:%M:%S')
            local_dt = JOHANNESBURG_TZ.localize(naive_dt)
            
            # Convert to UTC for storage
            utc_dt = local_dt.astimezone(pytz.UTC)
            
            return utc_dt
            
        except ValueError as e:
            raise ValueError(f"Invalid {field_name}: '{value}' - expected format 'YYYY-MM-DD HH:MM:SS'")
    
    def validate_file_structure(self, csv_content: bytes, filename: str) -> Tuple[bool, List[str]]:
        """Validate basic CSV file structure."""
        errors = []
        
        try:
            csv_text = csv_content.decode('utf-8')
            csv_reader = csv.DictReader(StringIO(csv_text))
            
            # Check if we can read the file
            if not csv_reader.fieldnames:
                errors.append("CSV has no headers")
                return False, errors
            
            # Check required columns
            missing_columns = set(self.REQUIRED_COLUMNS) - set(csv_reader.fieldnames)
            if missing_columns:
                errors.append(f"Missing required columns: {missing_columns}")
            
            # Check if file has data rows
            try:
                first_row = next(csv_reader)
                if not first_row:
                    errors.append("CSV has no data rows")
            except StopIteration:
                errors.append("CSV has no data rows")
            
            return len(errors) == 0, errors
            
        except UnicodeDecodeError:
            errors.append("CSV file is not valid UTF-8")
            return False, errors
        except Exception as e:
            errors.append(f"CSV parsing error: {str(e)}")
            return False, errors
