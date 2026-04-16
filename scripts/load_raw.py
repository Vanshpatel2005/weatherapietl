"""
Raw Data Loading Module for Weather ETL Pipeline.
Stores raw API JSON responses into weather_raw table.
"""

import psycopg2
import json
from typing import List, Dict, Any
from datetime import datetime
import logging

from config.config import Config
from scripts.logger import setup_logger


class RawDataLoader:
    """
    Loads raw API JSON responses into weather_raw table.
    Preserves original data for audit trail and reprocessing.
    """
    
    def __init__(self):
        self.logger = setup_logger(__name__)
        self.connection = None
        self.cursor = None
    
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
    
    def create_raw_table_if_not_exists(self):
        """Create weather_raw table if it doesn't exist (idempotent)."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS weather_raw (
            id                   SERIAL PRIMARY KEY,
            city_name            VARCHAR(100) NOT NULL,
            city_id              INTEGER REFERENCES cities(id) ON DELETE SET NULL,
            raw_data             JSONB NOT NULL,
            api_response_code    INTEGER,
            extraction_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            ingestion_timestamp  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            processed            BOOLEAN DEFAULT FALSE,
            -- Soft delete: NULL = active, timestamp = logically deleted
            deleted_at           TIMESTAMP DEFAULT NULL,
            UNIQUE(city_name, extraction_timestamp)
        );

        CREATE INDEX IF NOT EXISTS idx_raw_city_name    ON weather_raw(city_name);
        CREATE INDEX IF NOT EXISTS idx_raw_city_id      ON weather_raw(city_id);
        -- Partial index: only unprocessed active rows (fast for DAG scheduler)
        CREATE INDEX IF NOT EXISTS idx_raw_processed    ON weather_raw(processed)
            WHERE processed = FALSE;
        -- BRIN for time-series range scans on high-volume timestamp column
        CREATE INDEX IF NOT EXISTS idx_raw_extraction_brin
            ON weather_raw USING BRIN (extraction_timestamp);
        -- GIN for JSONB field queries
        CREATE INDEX IF NOT EXISTS idx_raw_data_gin
            ON weather_raw USING GIN (raw_data);
        """

        try:
            self.cursor.execute(create_table_query)
            self.connection.commit()
            self.logger.info("Table 'weather_raw' created or already exists")
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error creating raw table: {e}")
            raise
    
    def insert_raw_record(self, city_name: str, raw_data: Dict, api_response_code: int = 200) -> int:
        """
        Upsert a single raw API response into weather_raw.

        On conflict (same city_name), updates the record so the DAG
        always has the latest payload for reprocessing. Resets processed=FALSE
        so the transform task picks it up again. Does NOT touch deleted_at,
        preserving soft-delete state.

        Args:
            city_name: City name from API response.
            raw_data: Full raw JSON payload.
            api_response_code: HTTP status code from OWM.

        Returns:
            ID of the inserted/updated record, or None on failure.
        """
        insert_query = """
        INSERT INTO weather_raw (
            city_name, city_id, raw_data, api_response_code,
            extraction_timestamp, ingestion_timestamp, processed
        ) VALUES (
            %s,
            COALESCE((SELECT id FROM cities WHERE LOWER(city_name) = LOWER(%s) LIMIT 1), NULL),
            %s, %s, %s, %s, FALSE
        )
        ON CONFLICT (city_name, extraction_timestamp) DO NOTHING
        RETURNING id;
        """

        try:
            extraction_timestamp = datetime.now()
            self.cursor.execute(insert_query, (
                city_name,
                city_name,          # for city_id subquery
                json.dumps(raw_data),
                api_response_code,
                extraction_timestamp,
                extraction_timestamp
            ))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            self.logger.error(f"Error inserting raw record for {city_name}: {e}")
            return None
    
    def load_raw_data(self, raw_data_list: List[Dict]) -> Dict[str, int]:
        """
        Load raw API responses into weather_raw table.
        
        Args:
            raw_data_list: List of raw weather data from API
            
        Returns:
            Dictionary with load statistics
        """
        if not raw_data_list:
            self.logger.warning("No raw data to load")
            return {'total': 0, 'inserted': 0, 'skipped': 0}
        
        self.logger.info(f"Starting raw data load for {len(raw_data_list)} records")
        
        if not self.connect():
            raise Exception("Failed to connect to database")
        
        try:
            self.create_raw_table_if_not_exists()
            
            inserted_count = 0
            skipped_count = 0
            
            for data in raw_data_list:
                # Use requested_city_name to avoid localized spelling mismatches (e.g. Pālanpur) breaking DB joins
                city_name = data.get('requested_city_name') or data.get('name', 'Unknown')
                raw_id = self.insert_raw_record(city_name, data)
                
                if raw_id:
                    inserted_count += 1
                else:
                    skipped_count += 1
            
            self.connection.commit()
            
            stats = {
                'total': len(raw_data_list),
                'inserted': inserted_count,
                'skipped': skipped_count
            }
            
            self.logger.info(
                f"Raw data load complete: {inserted_count} inserted, "
                f"{skipped_count} skipped (duplicates)"
            )
            
            return stats
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error during raw data load: {e}")
            raise
        finally:
            self.disconnect()
    
    def get_unprocessed_raw_records(self) -> List[Dict]:
        """
        Retrieve raw records that haven't been processed yet.

        Filters:
            - processed = FALSE  — not yet transformed
            - deleted_at IS NULL — exclude soft-deleted records

        Returns:
            List of unprocessed raw record dicts.
        """
        if not self.connect():
            return []

        try:
            query = """
            SELECT id, city_name, raw_data, extraction_timestamp
            FROM   weather_raw
            WHERE  processed   = FALSE
              AND  deleted_at  IS NULL
            ORDER  BY extraction_timestamp ASC;
            """

            self.cursor.execute(query)
            rows = self.cursor.fetchall()

            results = [
                {
                    "id":                   row[0],
                    "city_name":            row[1],
                    "raw_data":             row[2],
                    "extraction_timestamp": row[3],
                }
                for row in rows
            ]

            self.logger.info(f"Retrieved {len(results)} unprocessed raw records (active only)")
            return results

        except Exception as e:
            self.logger.error(f"Error fetching unprocessed records: {e}")
            return []
        finally:
            self.disconnect()
    
    def mark_as_processed(self, raw_ids: List[int]) -> int:
        """
        Mark raw records as processed.
        
        Args:
            raw_ids: List of raw record IDs to mark as processed
            
        Returns:
            Number of records updated
        """
        if not raw_ids:
            return 0
        
        if not self.connect():
            raise Exception("Failed to connect to database")
        
        try:
            query = """
            UPDATE weather_raw 
            SET processed = TRUE 
            WHERE id = ANY(%s);
            """
            
            self.cursor.execute(query, (raw_ids,))
            updated = self.cursor.rowcount
            self.connection.commit()
            
            self.logger.info(f"Marked {updated} raw records as processed")
            return updated
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error marking records as processed: {e}")
            raise
        finally:
            self.disconnect()


def main():
    """Main function for standalone execution."""
    loader = RawDataLoader()
    
    # Example usage
    sample_data = [{
        'name': 'London',
        'main': {'temp': 15.5, 'humidity': 72},
        'weather': [{'main': 'Clouds'}],
        'dt': 1612345678
    }]
    
    stats = loader.load_raw_data(sample_data)
    print(f"Raw load stats: {stats}")
    return stats


if __name__ == "__main__":
    main()
