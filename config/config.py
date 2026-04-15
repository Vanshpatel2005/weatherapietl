"""
Configuration module for the Weather ETL Pipeline.
Loads environment variables and provides configuration access.
"""

import os
from typing import List
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)


class Config:
    """Configuration class to manage all environment variables."""
    
    # OpenWeatherMap API Configuration
    OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
    OPENWEATHER_BASE_URL = os.getenv('OPENWEATHER_BASE_URL', 'https://api.openweathermap.org/data/2.5/weather')
    CITIES = os.getenv('CITIES', 'London,New York,Tokyo,Paris,Sydney').split(',')
    
    # PostgreSQL Configuration
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5432))
    POSTGRES_DB = os.getenv('POSTGRES_DB', 'weather_warehouse')
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    
    # Airflow Configuration
    AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
    DAG_ID = os.getenv('DAG_ID', 'weather_etl_pipeline')
    SCHEDULE_INTERVAL = os.getenv('SCHEDULE_INTERVAL', '0 2 * * *')
    
    # Logging Configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE_PATH = os.getenv('LOG_FILE_PATH', 'logs/etl_pipeline.log')
    
    @classmethod
    def validate(cls) -> bool:
        """
        Validate that all required configuration values are present.
        Returns True if validation passes, raises ValueError otherwise.
        """
        required_vars = [
            ('OPENWEATHER_API_KEY', cls.OPENWEATHER_API_KEY),
            ('POSTGRES_PASSWORD', cls.POSTGRES_PASSWORD),
        ]
        
        missing_vars = [var_name for var_name, var_value in required_vars if not var_value]
        
        if missing_vars:
            error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info("Configuration validation passed")
        return True
    
    @classmethod
    def get_postgres_connection_string(cls) -> str:
        """
        Construct PostgreSQL connection string.
        Returns connection string for psycopg2 or sqlalchemy.
        """
        return (
            f"postgresql://{cls.POSTGRES_USER}:{cls.POSTGRES_PASSWORD}"
            f"@{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{cls.POSTGRES_DB}"
        )
    
    @classmethod
    def get_cities_list(cls) -> List[str]:
        """
        Get list of cities as clean strings.
        Returns list of city names with whitespace stripped.
        """
        return [city.strip() for city in cls.CITIES if city.strip()]


# Initialize and validate configuration on import
try:
    Config.validate()
except ValueError as e:
    logger.warning(f"Configuration validation failed: {e}")
    logger.warning("Please ensure .env file is properly configured")
