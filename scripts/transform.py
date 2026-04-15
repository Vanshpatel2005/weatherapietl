"""
Transformation module for the Weather ETL Pipeline.
Cleans and normalizes JSON weather data into structured tabular format.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

from scripts.logger import setup_logger


class WeatherTransformer:
    """
    Transforms raw weather data from API into clean, structured format.
    Handles missing values, data normalization, and schema consistency.
    """
    
    def __init__(self):
        self.logger = setup_logger(__name__)
    
    def transform_single_record(self, raw_data: Dict) -> Optional[Dict[str, Any]]:
        """
        Transform a single weather record into structured format.
        
        Args:
            raw_data: Raw JSON data from OpenWeatherMap API
            
        Returns:
            Transformed dictionary with standardized schema or None if invalid
        """
        try:
            # Extract nested data with null handling
            transformed = {
                'city_id': raw_data.get('id'),
                'city_name': raw_data.get('name'),
                'country_code': raw_data.get('sys', {}).get('country'),
                'latitude': raw_data.get('coord', {}).get('lat'),
                'longitude': raw_data.get('coord', {}).get('lon'),
                'temperature_celsius': raw_data.get('main', {}).get('temp'),
                'feels_like_celsius': raw_data.get('main', {}).get('feels_like'),
                'humidity_percent': raw_data.get('main', {}).get('humidity'),
                'pressure_hpa': raw_data.get('main', {}).get('pressure'),
                'wind_speed_mps': raw_data.get('wind', {}).get('speed'),
                'wind_direction_degrees': raw_data.get('wind', {}).get('deg'),
                'weather_main': raw_data.get('weather', [{}])[0].get('main') if raw_data.get('weather') else None,
                'weather_description': raw_data.get('weather', [{}])[0].get('description') if raw_data.get('weather') else None,
                'visibility_meters': raw_data.get('visibility'),
                'cloudiness_percent': raw_data.get('clouds', {}).get('all'),
                'weather_timestamp': datetime.fromtimestamp(raw_data.get('dt', 0)).isoformat() if raw_data.get('dt') else None,
                'ingestion_timestamp': datetime.now().isoformat()
            }
            
            # Validate critical fields are present
            if not transformed['city_name'] or transformed['temperature_celsius'] is None:
                self.logger.warning(f"Missing critical fields for record: {raw_data.get('name', 'Unknown')}")
                return None
            
            return transformed
            
        except Exception as e:
            self.logger.error(f"Error transforming record: {e}", exc_info=True)
            return None
    
    def clean_data(self, transformed_data: Dict) -> Dict:
        """
        Clean and validate transformed data.
        Handle edge cases and ensure data quality.
        
        Args:
            transformed_data: Transformed weather record
            
        Returns:
            Cleaned data dictionary
        """
        # Handle null values for numeric fields
        numeric_fields = [
            'temperature_celsius', 'feels_like_celsius', 'humidity_percent',
            'pressure_hpa', 'wind_speed_mps', 'wind_direction_degrees',
            'visibility_meters', 'cloudiness_percent', 'latitude', 'longitude'
        ]
        
        for field in numeric_fields:
            if transformed_data.get(field) is None:
                transformed_data[field] = None
        
        # Ensure string fields are properly formatted
        string_fields = ['city_name', 'country_code', 'weather_main', 'weather_description']
        for field in string_fields:
            if transformed_data.get(field):
                transformed_data[field] = str(transformed_data[field]).strip()
        
        # Validate temperature ranges (reasonable bounds for Earth)
        temp = transformed_data.get('temperature_celsius')
        if temp is not None:
            if temp < -100 or temp > 60:
                self.logger.warning(f"Unusual temperature detected: {temp}°C for {transformed_data['city_name']}")
        
        # Validate humidity (0-100%)
        humidity = transformed_data.get('humidity_percent')
        if humidity is not None and (humidity < 0 or humidity > 100):
            self.logger.warning(f"Invalid humidity: {humidity}% for {transformed_data['city_name']}")
            transformed_data['humidity_percent'] = None
        
        return transformed_data
    
    def transform(self, raw_data_list: List[Dict]) -> List[Dict]:
        """
        Main transformation method.
        Transforms a list of raw weather records.
        
        Args:
            raw_data_list: List of raw weather data from API
            
        Returns:
            List of transformed and cleaned weather records
        """
        self.logger.info(f"Starting transformation for {len(raw_data_list)} records")
        
        transformed_records = []
        failed_transforms = 0
        
        for raw_data in raw_data_list:
            transformed = self.transform_single_record(raw_data)
            
            if transformed:
                cleaned = self.clean_data(transformed)
                transformed_records.append(cleaned)
            else:
                failed_transforms += 1
        
        self.logger.info(
            f"Transformation complete: {len(transformed_records)} successful, "
            f"{failed_transforms} failed"
        )
        
        return transformed_records
    
    def get_schema(self) -> Dict[str, str]:
        """
        Return the expected schema for transformed data.
        Useful for documentation and validation.
        
        Returns:
            Dictionary mapping field names to data types
        """
        return {
            'city_id': 'INTEGER',
            'city_name': 'VARCHAR(100)',
            'country_code': 'VARCHAR(10)',
            'latitude': 'DECIMAL(10, 6)',
            'longitude': 'DECIMAL(10, 6)',
            'temperature_celsius': 'DECIMAL(5, 2)',
            'feels_like_celsius': 'DECIMAL(5, 2)',
            'humidity_percent': 'INTEGER',
            'pressure_hpa': 'INTEGER',
            'wind_speed_mps': 'DECIMAL(5, 2)',
            'wind_direction_degrees': 'INTEGER',
            'weather_main': 'VARCHAR(50)',
            'weather_description': 'VARCHAR(200)',
            'visibility_meters': 'INTEGER',
            'cloudiness_percent': 'INTEGER',
            'weather_timestamp': 'TIMESTAMP',
            'ingestion_timestamp': 'TIMESTAMP'
        }


def main():
    """Main function for standalone execution."""
    transformer = WeatherTransformer()
    # Example usage
    sample_data = [{
        'id': 1,
        'name': 'London',
        'sys': {'country': 'GB'},
        'coord': {'lat': 51.5074, 'lon': -0.1278},
        'main': {
            'temp': 15.5,
            'feels_like': 14.8,
            'humidity': 72,
            'pressure': 1012
        },
        'wind': {'speed': 5.2, 'deg': 230},
        'weather': [{'main': 'Clouds', 'description': 'overcast clouds'}],
        'visibility': 10000,
        'clouds': {'all': 90},
        'dt': 1612345678
    }]
    
    result = transformer.transform(sample_data)
    print(f"Transformed {len(result)} records")
    return result


if __name__ == "__main__":
    main()
