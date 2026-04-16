"""
Clean Data Loading Module for Weather ETL Pipeline.
Transforms and loads data from weather_raw to weather_clean table.
"""

import psycopg2
from typing import List, Dict, Any
from datetime import datetime
import logging

from config.config import Config
from scripts.logger import setup_logger
from scripts.transform import WeatherTransformer
from scripts.validate import DataQualityValidator


class CleanDataLoader:
    """
    Loads structured weather data into weather_clean table.
    Transforms raw data and applies data quality checks.
    """
    
    def __init__(self):
        self.logger = setup_logger(__name__)
        self.connection = None
        self.cursor = None
        self.transformer = WeatherTransformer()
        self.validator = DataQualityValidator()
    
    def connect(self) -> bool:
        """Establish connection to PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(
                host=Config.POSTGRES_HOST,
                port=Config.POSTGRES_PORT,
                database=Config.POSTGRES_DB,
                user=Config.POSTGRES_USER,
                password=Config.POSTGRES_PASSWORD
            )
            self.cursor = self.connection.cursor()
            self.logger.info("Successfully connected to PostgreSQL database")
            return True
        except psycopg2.OperationalError as e:
            self.logger.error(f"Failed to connect to database: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        self.logger.info("Database connection closed")
    
    def create_clean_table_if_not_exists(self):
        """Create weather_clean table with all indexes (idempotent)."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS weather_clean (
            city_name               VARCHAR(100) NOT NULL,
            recorded_at             TIMESTAMP NOT NULL,
            country_code            VARCHAR(10),
            latitude                DECIMAL(10, 6),
            longitude               DECIMAL(10, 6),
            temperature_celsius     DECIMAL(5, 2) NOT NULL,
            feels_like_celsius      DECIMAL(5, 2),
            humidity_percent        INTEGER CHECK (humidity_percent >= 0 AND humidity_percent <= 100),
            pressure_hpa            INTEGER CHECK (pressure_hpa >= 800 AND pressure_hpa <= 1100),
            wind_speed_mps          DECIMAL(5, 2) CHECK (wind_speed_mps >= 0),
            wind_direction_degrees  INTEGER CHECK (wind_direction_degrees >= 0 AND wind_direction_degrees <= 360),
            weather_main            VARCHAR(50),
            weather_description     VARCHAR(200),
            visibility_meters       INTEGER,
            cloudiness_percent      INTEGER CHECK (cloudiness_percent >= 0 AND cloudiness_percent <= 100),
            ingestion_timestamp     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            raw_id                  INTEGER REFERENCES weather_raw(id) ON DELETE SET NULL,
            city_id                 INTEGER REFERENCES cities(id) ON DELETE SET NULL,
            -- Soft delete: NULL = active, timestamp = logically deleted
            deleted_at              TIMESTAMP DEFAULT NULL,
            PRIMARY KEY (city_name, recorded_at)
        );

        -- B-tree indexes for FK joins and equality lookups
        CREATE INDEX IF NOT EXISTS idx_clean_city_name   ON weather_clean(city_name);
        CREATE INDEX IF NOT EXISTS idx_clean_city_id     ON weather_clean(city_id);
        CREATE INDEX IF NOT EXISTS idx_clean_raw_id      ON weather_clean(raw_id);

        -- BRIN indexes for time-series range scans
        CREATE INDEX IF NOT EXISTS idx_clean_recorded_at_brin
            ON weather_clean USING BRIN (recorded_at);
        CREATE INDEX IF NOT EXISTS idx_clean_ingestion_brin
            ON weather_clean USING BRIN (ingestion_timestamp);

        -- Composite index for dashboard city+time filter queries
        CREATE INDEX IF NOT EXISTS idx_clean_city_recorded
            ON weather_clean(city_name, recorded_at DESC);

        -- Partial index for active records only
        CREATE INDEX IF NOT EXISTS idx_clean_active
            ON weather_clean(city_name, recorded_at DESC)
            WHERE deleted_at IS NULL;
        """

        try:
            self.cursor.execute(create_table_query)
            self.connection.commit()
            self.logger.info("Table 'weather_clean' created or already exists")
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error creating clean table: {e}")
            raise
    
    def insert_clean_record(self, record: Dict[str, Any], raw_id: int) -> bool:
        """
        Insert a clean weather record into weather_clean.

        Uses INSERT ... ON CONFLICT DO UPDATE so::
            - Existing rows for the same (city_name, recorded_at) are refreshed
              (e.g. corrected values after a reprocessing run).
            - Historical rows from earlier timestamps are NEVER deleted — they
              remain available for trend queries and audit.

        Soft deletes (deleted_at) are reset to NULL on upsert so a previously
        soft-deleted record becomes active again if re-ingested.

        Args:
            record: Transformed and validated weather record dict.
            raw_id: ID of the corresponding weather_raw row.

        Returns:
            True on success, False otherwise.
        """
        city_name = record.get("city_name")

        insert_query = """
        INSERT INTO weather_clean (
            city_name, recorded_at, country_code, latitude, longitude,
            temperature_celsius, feels_like_celsius, humidity_percent,
            pressure_hpa, wind_speed_mps, wind_direction_degrees,
            weather_main, weather_description, visibility_meters,
            cloudiness_percent, ingestion_timestamp, raw_id, city_id,
            deleted_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            COALESCE((SELECT id FROM cities WHERE LOWER(city_name) = LOWER(%s) LIMIT 1), NULL),
            NULL
        )
        ON CONFLICT (city_name, recorded_at)
        DO UPDATE SET
            temperature_celsius    = EXCLUDED.temperature_celsius,
            feels_like_celsius     = EXCLUDED.feels_like_celsius,
            humidity_percent       = EXCLUDED.humidity_percent,
            pressure_hpa           = EXCLUDED.pressure_hpa,
            wind_speed_mps         = EXCLUDED.wind_speed_mps,
            wind_direction_degrees = EXCLUDED.wind_direction_degrees,
            weather_main           = EXCLUDED.weather_main,
            weather_description    = EXCLUDED.weather_description,
            visibility_meters      = EXCLUDED.visibility_meters,
            cloudiness_percent     = EXCLUDED.cloudiness_percent,
            ingestion_timestamp    = EXCLUDED.ingestion_timestamp,
            raw_id                 = EXCLUDED.raw_id,
            city_id                = EXCLUDED.city_id,
            -- Re-activate if it was previously soft-deleted
            deleted_at             = NULL;
        """

        try:
            self.cursor.execute(insert_query, (
                city_name,
                record.get("weather_timestamp"),
                record.get("country_code"),
                record.get("latitude"),
                record.get("longitude"),
                record.get("temperature_celsius"),
                record.get("feels_like_celsius"),
                record.get("humidity_percent"),
                record.get("pressure_hpa"),
                record.get("wind_speed_mps"),
                record.get("wind_direction_degrees"),
                record.get("weather_main"),
                record.get("weather_description"),
                record.get("visibility_meters"),
                record.get("cloudiness_percent"),
                datetime.now(),
                raw_id,
                city_name,  # for city_id subquery
            ))
            self.logger.debug(f"Upserted clean record for {city_name}")
            return True
        except Exception as e:
            self.logger.error(f"Error inserting clean record for {city_name}: {e}")
            return False
    
    def transform_and_load(self, raw_records: List[Dict]) -> Dict[str, int]:
        """
        Transform raw records and load into clean table.
        
        Args:
            raw_records: List of raw records from weather_raw table
            
        Returns:
            Dictionary with load statistics
        """
        if not raw_records:
            self.logger.warning("No raw records to process")
            return {'total': 0, 'transformed': 0, 'validated': 0, 'loaded': 0, 'failed': 0}
        
        self.logger.info(f"Starting transformation and load for {len(raw_records)} raw records")
        
        if not self.connect():
            raise Exception("Failed to connect to database")
        
        try:
            self.create_clean_table_if_not_exists()
            
            transformed_count = 0
            validated_count = 0
            loaded_count = 0
            failed_count = 0
            processed_raw_ids = []
            
            for raw_record in raw_records:
                raw_id = raw_record['id']
                raw_data = raw_record['raw_data']
                
                # Transform
                transformed = self.transformer.transform_single_record(raw_data)
                if transformed:
                    transformed_count += 1
                    
                    # Validate
                    is_valid, errors = self.validator.validate_record(transformed)
                    if is_valid:
                        validated_count += 1
                        
                        # Load
                        if self.insert_clean_record(transformed, raw_id):
                            loaded_count += 1
                            processed_raw_ids.append(raw_id)
                        else:
                            failed_count += 1
                    else:
                        self.logger.warning(f"Validation failed for {raw_record['city_name']}: {errors}")
                        failed_count += 1
                else:
                    self.logger.warning(f"Transformation failed for {raw_record['city_name']}")
                    failed_count += 1
            
            self.connection.commit()
            
            # Mark processed raw records
            if processed_raw_ids:
                self._mark_raw_as_processed(processed_raw_ids)
            
            stats = {
                'total': len(raw_records),
                'transformed': transformed_count,
                'validated': validated_count,
                'loaded': loaded_count,
                'failed': failed_count
            }
            
            self.logger.info(
                f"Clean data load complete: {transformed_count} transformed, "
                f"{validated_count} validated, {loaded_count} loaded, {failed_count} failed"
            )
            
            return stats
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error during clean data load: {e}")
            raise
        finally:
            self.disconnect()
    
    def _mark_raw_as_processed(self, raw_ids: List[int]) -> int:
        """Mark raw records as processed."""
        if not raw_ids:
            return 0
        
        query = """
        UPDATE weather_raw 
        SET processed = TRUE 
        WHERE id = ANY(%s);
        """
        
        try:
            self.cursor.execute(query, (raw_ids,))
            updated = self.cursor.rowcount
            self.logger.info(f"Marked {updated} raw records as processed")
            return updated
        except Exception as e:
            self.logger.error(f"Error marking records as processed: {e}")
            return 0

    def cleanup_old_records(self) -> int:
        """
        Update existing records with new values instead of deleting them.
        This keeps the same record but updates temperature, humidity, etc.
        """
        if not self.connect():
            return 0
        
        try:
            # Update existing records with the latest data for each city
            # This keeps the same row but updates the values
            query = """
            WITH latest_data AS (
                SELECT 
                    city_name, recorded_at, country_code, latitude, longitude,
                    temperature_celsius, feels_like_celsius, humidity_percent,
                    pressure_hpa, wind_speed_mps, wind_direction_degrees,
                    weather_main, weather_description, visibility_meters,
                    cloudiness_percent, ingestion_timestamp,
                    ROW_NUMBER() OVER (PARTITION BY city_name ORDER BY recorded_at DESC) as rn
                FROM weather_clean
            )
            UPDATE weather_clean wc
            SET 
                recorded_at = ld.recorded_at,
                temperature_celsius = ld.temperature_celsius,
                feels_like_celsius = ld.feels_like_celsius,
                humidity_percent = ld.humidity_percent,
                pressure_hpa = ld.pressure_hpa,
                wind_speed_mps = ld.wind_speed_mps,
                wind_direction_degrees = ld.wind_direction_degrees,
                weather_main = ld.weather_main,
                weather_description = ld.weather_description,
                visibility_meters = ld.visibility_meters,
                cloudiness_percent = ld.cloudiness_percent,
                ingestion_timestamp = ld.ingestion_timestamp
            FROM latest_data ld
            WHERE wc.city_name = ld.city_name 
            AND wc.id != (SELECT id FROM (SELECT id FROM weather_clean wc2 WHERE wc2.city_name = ld.city_name ORDER BY wc2.recorded_at DESC LIMIT 1) sub)
            AND rn = 1;
            """
            
            self.cursor.execute(query)
            updated = self.cursor.rowcount
            self.connection.commit()
            self.logger.info(f"Update: Updated {updated} records with latest data per city")
            return updated
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error during update: {e}")
            return 0
        finally:
            self.disconnect()


def main():
    """Main function for standalone execution."""
    loader = CleanDataLoader()
    
    # Example usage - would normally fetch from load_raw
    sample_raw_records = [{
        'id': 1,
        'city_name': 'London',
        'raw_data': {
            'name': 'London',
            'main': {'temp': 15.5, 'humidity': 72},
            'weather': [{'main': 'Clouds'}],
            'dt': 1612345678
        }
    }]
    
    stats = loader.transform_and_load(sample_raw_records)
    print(f"Clean load stats: {stats}")
    return stats


if __name__ == "__main__":
    main()
