"""
Data Quality Validation module for the Weather ETL Pipeline.
Performs data quality checks on transformed weather data before loading.
"""

from typing import List, Dict, Any, Tuple
import logging
from datetime import datetime

from scripts.logger import setup_logger


class DataQualityValidator:
    """
    Validates data quality of weather records.
    Checks for null values, range validation, and data consistency.
    """
    
    def __init__(self):
        self.logger = setup_logger(__name__)
        self.validation_errors = []
    
    def check_not_null(self, record: Dict, field: str) -> bool:
        """
        Check if a field is not null/None.
        
        Args:
            record: Weather record to validate
            field: Field name to check
            
        Returns:
            True if field is not null, False otherwise
        """
        if record.get(field) is None:
            error_msg = f"Null value found in critical field: {field}"
            self.validation_errors.append(error_msg)
            self.logger.warning(error_msg)
            return False
        return True
    
    def check_temperature_range(self, temperature: float) -> bool:
        """
        Validate temperature is within realistic Earth bounds.
        
        Args:
            temperature: Temperature in Celsius
            
        Returns:
            True if temperature is valid, False otherwise
        """
        if temperature is None:
            return True  # None is handled by not_null check
        
        if temperature < -100 or temperature > 60:
            error_msg = f"Temperature out of range: {temperature}°C"
            self.validation_errors.append(error_msg)
            self.logger.warning(error_msg)
            return False
        return True
    
    def check_humidity_range(self, humidity: int) -> bool:
        """
        Validate humidity is within 0-100% range.
        
        Args:
            humidity: Humidity percentage
            
        Returns:
            True if humidity is valid, False otherwise
        """
        if humidity is None:
            return True  # None is handled by not_null check
        
        if humidity < 0 or humidity > 100:
            error_msg = f"Humidity out of range: {humidity}%"
            self.validation_errors.append(error_msg)
            self.logger.warning(error_msg)
            return False
        return True
    
    def check_pressure_range(self, pressure: int) -> bool:
        """
        Validate atmospheric pressure is within realistic bounds.
        
        Args:
            pressure: Pressure in hPa
            
        Returns:
            True if pressure is valid, False otherwise
        """
        if pressure is None:
            return True  # None is handled by not_null check
        
        if pressure < 800 or pressure > 1100:
            error_msg = f"Pressure out of range: {pressure} hPa"
            self.validation_errors.append(error_msg)
            self.logger.warning(error_msg)
            return False
        return True
    
    def check_wind_speed(self, wind_speed: float) -> bool:
        """
        Validate wind speed is within realistic bounds.
        
        Args:
            wind_speed: Wind speed in meters per second
            
        Returns:
            True if wind speed is valid, False otherwise
        """
        if wind_speed is None:
            return True  # None is handled by not_null check
        
        if wind_speed < 0 or wind_speed > 150:  # 150 m/s is extreme hurricane
            error_msg = f"Wind speed out of range: {wind_speed} m/s"
            self.validation_errors.append(error_msg)
            self.logger.warning(error_msg)
            return False
        return True
    
    def check_timestamp_format(self, timestamp: str) -> bool:
        """
        Validate timestamp is in correct ISO format.
        
        Args:
            timestamp: Timestamp string
            
        Returns:
            True if timestamp is valid, False otherwise
        """
        if timestamp is None:
            return True  # None is handled by not_null check
        
        try:
            datetime.fromisoformat(timestamp)
            return True
        except ValueError:
            error_msg = f"Invalid timestamp format: {timestamp}"
            self.validation_errors.append(error_msg)
            self.logger.warning(error_msg)
            return False
    
    def validate_record(self, record: Dict) -> Tuple[bool, List[str]]:
        """
        Perform all validation checks on a single record.
        
        Args:
            record: Weather record to validate
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        self.validation_errors = []  # Reset for this record
        
        # Critical fields - must not be null
        critical_fields = ['city_name', 'temperature_celsius', 'weather_timestamp']
        for field in critical_fields:
            if not self.check_not_null(record, field):
                errors.append(f"Critical field {field} is null")
        
        # Range validations
        if record.get('temperature_celsius') is not None:
            if not self.check_temperature_range(record['temperature_celsius']):
                errors.append(f"Temperature out of range: {record['temperature_celsius']}")
        
        if record.get('humidity_percent') is not None:
            if not self.check_humidity_range(record['humidity_percent']):
                errors.append(f"Humidity out of range: {record['humidity_percent']}")
        
        if record.get('pressure_hpa') is not None:
            if not self.check_pressure_range(record['pressure_hpa']):
                errors.append(f"Pressure out of range: {record['pressure_hpa']}")
        
        if record.get('wind_speed_mps') is not None:
            if not self.check_wind_speed(record['wind_speed_mps']):
                errors.append(f"Wind speed out of range: {record['wind_speed_mps']}")
        
        # Timestamp validation
        if record.get('weather_timestamp'):
            if not self.check_timestamp_format(record['weather_timestamp']):
                errors.append(f"Invalid timestamp format: {record['weather_timestamp']}")
        
        is_valid = len(errors) == 0
        
        if not is_valid:
            self.logger.warning(f"Validation failed for {record.get('city_name', 'Unknown')}: {errors}")
        
        return is_valid, errors
    
    def validate_batch(self, records: List[Dict]) -> Dict[str, Any]:
        """
        Validate a batch of weather records.
        
        Args:
            records: List of weather records to validate
            
        Returns:
            Dictionary with validation statistics and details
        """
        self.logger.info(f"Starting data quality validation for {len(records)} records")
        
        valid_records = []
        invalid_records = []
        all_errors = []
        
        for record in records:
            is_valid, errors = self.validate_record(record)
            
            if is_valid:
                valid_records.append(record)
            else:
                invalid_records.append({
                    'record': record,
                    'errors': errors
                })
                all_errors.extend(errors)
        
        stats = {
            'total_records': len(records),
            'valid_records': len(valid_records),
            'invalid_records': len(invalid_records),
            'validation_rate': round((len(valid_records) / len(records) * 100), 2) if records else 0,
            'errors': all_errors,
            'invalid_details': invalid_records
        }
        
        self.logger.info(
            f"Validation complete: {stats['valid_records']} valid, "
            f"{stats['invalid_records']} invalid "
            f"({stats['validation_rate']}% pass rate)"
        )
        
        if stats['invalid_records'] > 0:
            self.logger.warning(f"Validation errors: {all_errors}")
        
        return stats
    
    def get_quality_report(self, validation_stats: Dict) -> str:
        """
        Generate a human-readable quality report.
        
        Args:
            validation_stats: Statistics from validate_batch
            
        Returns:
            Formatted quality report string
        """
        report = [
            "=" * 60,
            "DATA QUALITY VALIDATION REPORT",
            "=" * 60,
            f"Total Records: {validation_stats['total_records']}",
            f"Valid Records: {validation_stats['valid_records']}",
            f"Invalid Records: {validation_stats['invalid_records']}",
            f"Validation Rate: {validation_stats['validation_rate']}%",
            ""
        ]
        
        if validation_stats['invalid_records'] > 0:
            report.append("ERRORS FOUND:")
            report.append("-" * 60)
            for error in validation_stats['errors'][:10]:  # Show first 10 errors
                report.append(f"  - {error}")
            
            if len(validation_stats['errors']) > 10:
                report.append(f"  ... and {len(validation_stats['errors']) - 10} more errors")
        
        report.append("=" * 60)
        
        return "\n".join(report)


def main():
    """Main function for standalone execution."""
    validator = DataQualityValidator()
    
    # Example usage
    sample_data = [{
        'city_name': 'London',
        'temperature_celsius': 15.5,
        'humidity_percent': 72,
        'pressure_hpa': 1012,
        'wind_speed_mps': 5.2,
        'weather_timestamp': datetime.now().isoformat()
    }]
    
    stats = validator.validate_batch(sample_data)
    report = validator.get_quality_report(stats)
    print(report)
    return stats


if __name__ == "__main__":
    main()
