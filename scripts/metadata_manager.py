"""
Pipeline Metadata Manager for Weather ETL Pipeline.
Manages pipeline execution state for incremental loading.
"""

import psycopg2
from typing import Dict, Optional
from datetime import datetime
import logging

from config.config import Config
from scripts.logger import setup_logger


class MetadataManager:
    """
    Manages pipeline metadata for incremental loading.
    Tracks last successful run and pipeline state.
    """
    
    def __init__(self, pipeline_name: str = 'weather_etl_pipeline'):
        self.logger = setup_logger(__name__)
        self.pipeline_name = pipeline_name
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
    
    def create_metadata_table_if_not_exists(self):
        """Create pipeline_metadata table if it doesn't exist."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS pipeline_metadata (
            id SERIAL PRIMARY KEY,
            pipeline_name VARCHAR(100) NOT NULL UNIQUE,
            last_successful_run TIMESTAMP NOT NULL,
            last_successful_run_id VARCHAR(100),
            records_processed INTEGER DEFAULT 0,
            records_inserted INTEGER DEFAULT 0,
            records_failed INTEGER DEFAULT 0,
            pipeline_status VARCHAR(50) DEFAULT 'IDLE',
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_metadata_pipeline_name ON pipeline_metadata(pipeline_name);
        
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        DROP TRIGGER IF EXISTS update_pipeline_metadata_updated_at ON pipeline_metadata;
        CREATE TRIGGER update_pipeline_metadata_updated_at
            BEFORE UPDATE ON pipeline_metadata
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
        """
        
        try:
            self.cursor.execute(create_table_query)
            self.connection.commit()
            self.logger.info("Table 'pipeline_metadata' created or already exists")
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error creating table: {e}")
            raise
    
    def get_last_successful_run(self) -> Optional[datetime]:
        """
        Get the timestamp of the last successful pipeline run.
        
        Returns:
            Datetime of last successful run or None if no previous runs
        """
        if not self.connect():
            return None
        
        try:
            query = """
            SELECT last_successful_run 
            FROM pipeline_metadata 
            WHERE pipeline_name = %s;
            """
            
            self.cursor.execute(query, (self.pipeline_name,))
            result = self.cursor.fetchone()
            
            if result:
                self.logger.info(f"Last successful run: {result[0]}")
                return result[0]
            else:
                self.logger.info("No previous successful runs found")
                return None
                
        except Exception as e:
            self.logger.error(f"Error fetching last successful run: {e}")
            return None
        finally:
            self.disconnect()
    
    def start_pipeline_run(self, run_id: str) -> bool:
        """
        Mark pipeline as started.
        
        Args:
            run_id: Unique identifier for this run
            
        Returns:
            True if successful, False otherwise
        """
        if not self.connect():
            return False
        
        try:
            self.create_metadata_table_if_not_exists()
            
            # Upsert pipeline metadata
            upsert_query = """
            INSERT INTO pipeline_metadata (
                pipeline_name, last_successful_run, last_successful_run_id, 
                pipeline_status, records_processed, records_inserted, records_failed
            ) VALUES (
                %s, %s, %s, %s, 0, 0, 0
            )
            ON CONFLICT (pipeline_name) 
            DO UPDATE SET 
                pipeline_status = %s,
                error_message = NULL;
            """
            
            self.cursor.execute(upsert_query, (
                self.pipeline_name,
                datetime.now(),
                run_id,
                'RUNNING',
                'RUNNING'
            ))
            
            self.connection.commit()
            self.logger.info(f"Pipeline {self.pipeline_name} marked as RUNNING with run_id: {run_id}")
            return True
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error starting pipeline run: {e}")
            return False
        finally:
            self.disconnect()
    
    def end_pipeline_run(
        self, 
        run_id: str, 
        status: str, 
        records_processed: int = 0,
        records_inserted: int = 0,
        records_failed: int = 0,
        error_message: str = None
    ) -> bool:
        """
        Mark pipeline as completed.
        
        Args:
            run_id: Unique identifier for this run
            status: Final status ('SUCCESS' or 'FAILED')
            records_processed: Total records processed
            records_inserted: Records successfully inserted
            records_failed: Records that failed
            error_message: Error message if failed
            
        Returns:
            True if successful, False otherwise
        """
        if not self.connect():
            return False
        
        try:
            self.create_metadata_table_if_not_exists()
            
            update_query = """
            UPDATE pipeline_metadata 
            SET 
                pipeline_status = %s,
                records_processed = %s,
                records_inserted = %s,
                records_failed = %s,
                error_message = %s,
                last_successful_run = CASE 
                    WHEN %s = 'SUCCESS' THEN CURRENT_TIMESTAMP 
                    ELSE last_successful_run 
                END,
                last_successful_run_id = CASE 
                    WHEN %s = 'SUCCESS' THEN %s 
                    ELSE last_successful_run_id 
                END
            WHERE pipeline_name = %s;
            """
            
            self.cursor.execute(update_query, (
                status,
                records_processed,
                records_inserted,
                records_failed,
                error_message,
                status,
                status,
                run_id,
                self.pipeline_name
            ))
            
            self.connection.commit()
            self.logger.info(
                f"Pipeline {self.pipeline_name} marked as {status}: "
                f"{records_processed} processed, {records_inserted} inserted, {records_failed} failed"
            )
            return True
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error ending pipeline run: {e}")
            return False
        finally:
            self.disconnect()
    
    def get_pipeline_status(self) -> Optional[Dict]:
        """
        Get current pipeline status.
        
        Returns:
            Dictionary with pipeline status information
        """
        if not self.connect():
            return None
        
        try:
            query = """
            SELECT 
                pipeline_name, last_successful_run, last_successful_run_id,
                records_processed, records_inserted, records_failed,
                pipeline_status, error_message, updated_at
            FROM pipeline_metadata 
            WHERE pipeline_name = %s;
            """
            
            self.cursor.execute(query, (self.pipeline_name,))
            result = self.cursor.fetchone()
            
            if result:
                return {
                    'pipeline_name': result[0],
                    'last_successful_run': result[1],
                    'last_successful_run_id': result[2],
                    'records_processed': result[3],
                    'records_inserted': result[4],
                    'records_failed': result[5],
                    'pipeline_status': result[6],
                    'error_message': result[7],
                    'updated_at': result[8]
                }
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"Error fetching pipeline status: {e}")
            return None
        finally:
            self.disconnect()


def main():
    """Main function for standalone execution."""
    manager = MetadataManager()
    
    # Test metadata operations
    manager.start_pipeline_run('test_run_001')
    print("Pipeline started")
    
    manager.end_pipeline_run(
        'test_run_001',
        'SUCCESS',
        records_processed=10,
        records_inserted=10,
        records_failed=0
    )
    print("Pipeline completed")
    
    status = manager.get_pipeline_status()
    print(f"Pipeline status: {status}")
    
    return status


if __name__ == "__main__":
    main()
