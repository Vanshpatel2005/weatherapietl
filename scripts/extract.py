"""
Extraction module for the Weather ETL Pipeline.
Fetches weather data from OpenWeatherMap API with error handling and retries.
"""

import requests
import time
from typing import List, Dict, Optional
from datetime import datetime
import logging

from config.config import Config
from scripts.logger import setup_logger, log_error


class WeatherExtractor:
    """
    Extracts weather data from OpenWeatherMap API.
    Handles API errors, retries, and rate limiting.
    """
    
    def __init__(self):
        self.logger = setup_logger(__name__)
        self.api_key = Config.OPENWEATHER_API_KEY
        self.base_url = Config.OPENWEATHER_BASE_URL
        self.max_retries = 3
        self.retry_delay = 2  # seconds
        
    def fetch_weather_data(self, city: str) -> Optional[Dict]:
        """
        Fetch weather data for a single city with retry logic.
        
        Args:
            city: Name of the city to fetch weather for
            
        Returns:
            Dictionary containing weather data or None if failed
        """
        params = {
            'q': city,
            'appid': self.api_key,
            'units': 'metric'  # Use metric units (Celsius)
        }
        
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Fetching weather data for {city} (attempt {attempt + 1}/{self.max_retries})")
                
                response = requests.get(self.base_url, params=params, timeout=10)
                response.raise_for_status()
                
                data = response.json()
                self.logger.info(f"Successfully fetched data for {city}")
                return data
                
            except requests.exceptions.HTTPError as e:
                if response.status_code == 404:
                    self.logger.error(f"City '{city}' not found")
                    return None
                elif response.status_code == 401:
                    self.logger.error("Invalid API key")
                    raise ValueError("Invalid OpenWeatherMap API key")
                elif response.status_code == 429:
                    self.logger.warning("Rate limit exceeded, waiting before retry...")
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    self.logger.error(f"HTTP error: {e}")
                    if attempt == self.max_retries - 1:
                        return None
                    time.sleep(self.retry_delay)
                    
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request error for {city}: {e}")
                if attempt == self.max_retries - 1:
                    return None
                time.sleep(self.retry_delay)
                
            except Exception as e:
                self.logger.error(f"Unexpected error fetching data for {city}: {e}")
                if attempt == self.max_retries - 1:
                    return None
                time.sleep(self.retry_delay)
        
        return None
    
    def fetch_all_cities(self, cities: List[str]) -> List[Dict]:
        """
        Fetch weather data for multiple cities.
        
        Args:
            cities: List of city names
            
        Returns:
            List of weather data dictionaries
        """
        self.logger.info(f"Starting extraction for {len(cities)} cities")
        
        weather_data_list = []
        successful_cities = 0
        failed_cities = 0
        
        for city in cities:
            data = self.fetch_weather_data(city)
            
            if data:
                # Add extraction timestamp
                data['extraction_timestamp'] = datetime.now().isoformat()
                weather_data_list.append(data)
                successful_cities += 1
            else:
                failed_cities += 1
        
        self.logger.info(f"Extraction complete: {successful_cities} successful, {failed_cities} failed")
        
        return weather_data_list
    
    def extract(self) -> List[Dict]:
        """
        Main extraction method.
        Fetches weather data for all configured cities.
        
        Returns:
            List of weather data dictionaries
        """
        cities = Config.get_cities_list()
        self.logger.info(f"Starting extraction for cities: {', '.join(cities)}")
        
        weather_data = self.fetch_all_cities(cities)
        
        self.logger.info(f"Extracted {len(weather_data)} weather records")
        
        return weather_data


def main():
    """Main function for standalone execution."""
    extractor = WeatherExtractor()
    data = extractor.extract()
    print(f"Extracted {len(data)} weather records")
    return data


if __name__ == "__main__":
    main()
