"""
Production-Ready Airflow DAG for Weather ETL Pipeline.
Orchestrates extract → load_raw → transform_clean → load_clean → quality_check → metadata_update workflow.
Architecture: API → weather_raw → weather_clean → API
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.extract import WeatherExtractor
from scripts.load_raw import RawDataLoader
from scripts.load_clean import CleanDataLoader
from scripts.validate import DataQualityValidator
from scripts.metadata_manager import MetadataManager
from scripts.logger import setup_logger
from config.config import Config

# Default arguments for the DAG
default_args = {
    'owner': 'weather_etl',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'max_active_runs': 1,  # Ensure idempotency - only one run at a time
}

# Create the DAG
dag = DAG(
    'weather_etl_pipeline_v2',
    default_args=default_args,
    description='Production ETL Pipeline: Extract → Raw → Transform → Clean → Load',
    schedule_interval=Config.SCHEDULE_INTERVAL,
    tags=['weather', 'etl', 'production', 'incremental'],
)

# Initialize logger
logger = setup_logger(__name__)
PIPELINE_NAME = 'weather_etl_pipeline_v2'


def generate_run_id(**context):
    """Generate unique run ID for this DAG run."""
    ds = context['ds']
    run_id = f"manual_{ds}_{datetime.now().strftime('%H%M%S')}"
    logger.info(f"Generated run_id: {run_id}")
    context['task_instance'].xcom_push(key='run_id', value=run_id)
    return run_id


def extract_task(**context):
    """
    Extract weather data from OpenWeatherMap API.
    Fetches raw JSON responses for all configured cities from the database.
    """
    logger.info("=" * 60)
    logger.info("TASK: EXTRACT - Fetching weather data from API")
    logger.info("=" * 60)
    
    try:
        # Get cities from database
        import psycopg2
        from config.config import Config
        
        conn = psycopg2.connect(
            host=Config.POSTGRES_HOST,
            port=Config.POSTGRES_PORT,
            database=Config.POSTGRES_DB,
            user=Config.POSTGRES_USER,
            password=Config.POSTGRES_PASSWORD
        )
        cur = conn.cursor()
        
        cur.execute("SELECT city_name FROM cities WHERE active = TRUE ORDER BY city_name;")
        rows = cur.fetchall()
        cities = [row[0] for row in rows]
        
        cur.close()
        conn.close()
        
        logger.info(f"Fetched {len(cities)} cities from database: {cities}")
        
        # Update Config with cities from database
        Config.CITIES = cities
        
        extractor = WeatherExtractor()
        raw_data = extractor.extract()
        
        # Store data in XCom for downstream tasks
        context['task_instance'].xcom_push(key='raw_data', value=raw_data)
        
        logger.info(f"✓ Extraction complete: {len(raw_data)} records fetched")
        
        if len(raw_data) == 0:
            raise ValueError("No data extracted from API")
        
        return len(raw_data)
        
    except Exception as e:
        logger.error(f"✗ Extraction failed: {e}", exc_info=True)
        raise


def load_raw_task(**context):
    """
    Load raw API responses into weather_raw table.
    Preserves original data for audit trail and reprocessing.
    """
    logger.info("=" * 60)
    logger.info("TASK: LOAD_RAW - Storing raw API responses")
    logger.info("=" * 60)
    
    try:
        # Get raw data from XCom
        ti = context['task_instance']
        raw_data = ti.xcom_pull(task_ids='extract', key='raw_data')
        
        if not raw_data:
            raise ValueError("No raw data available to load")
        
        loader = RawDataLoader()
        stats = loader.load_raw_data(raw_data)
        
        logger.info(f"✓ Raw data load complete: {stats['inserted']} inserted, {stats['skipped']} skipped")
        
        return stats['inserted']
        
    except Exception as e:
        logger.error(f"✗ Raw data load failed: {e}", exc_info=True)
        raise


def transform_clean_task(**context):
    """
    Transform raw data from weather_raw and load into weather_clean table.
    Applies data quality checks and transformations.
    """
    logger.info("=" * 60)
    logger.info("TASK: TRANSFORM_CLEAN - Transform and load clean data")
    logger.info("=" * 60)
    
    try:
        loader = CleanDataLoader()
        
        # Get unprocessed raw records
        raw_loader = RawDataLoader()
        raw_records = raw_loader.get_unprocessed_raw_records()
        
        if not raw_records:
            logger.info("No unprocessed raw records found")
            return 0
        
        # Transform and load
        stats = loader.transform_and_load(raw_records)
        
        logger.info(f"✓ Transform and load complete: {stats['loaded']} loaded, {stats['failed']} failed")
        
        # Store stats for quality check
        context['task_instance'].xcom_push(key='transform_stats', value=stats)
        
        return stats['loaded']
        
    except Exception as e:
        logger.error(f"✗ Transform and load failed: {e}", exc_info=True)
        raise


def quality_check_task(**context):
    """
    Perform data quality validation on clean table.
    Ensures data meets quality standards before marking pipeline as successful.
    """
    logger.info("=" * 60)
    logger.info("TASK: QUALITY_CHECK - Validating data quality")
    logger.info("=" * 60)
    
    try:
        import psycopg2
        from config.config import Config
        
        # Sample recent data from clean table for validation
        conn = psycopg2.connect(
            host=Config.POSTGRES_HOST,
            port=Config.POSTGRES_PORT,
            database=Config.POSTGRES_DB,
            user=Config.POSTGRES_USER,
            password=Config.POSTGRES_PASSWORD
        )
        cur = conn.cursor()
        
        query = """
        SELECT 
            city_name, recorded_at, temperature_celsius, humidity_percent,
            pressure_hpa, wind_speed_mps
        FROM weather_clean
        ORDER BY recorded_at DESC
        LIMIT 100;
        """
        
        cur.execute(query)
        rows = cur.fetchall()
        
        # Convert to dict format for validator
        records = []
        for row in rows:
            records.append({
                'city_name': row[0],
                'weather_timestamp': row[1].isoformat(),
                'temperature_celsius': float(row[2]),
                'humidity_percent': row[3],
                'pressure_hpa': row[4],
                'wind_speed_mps': float(row[5])
            })
        
        cur.close()
        conn.close()
        
        # Validate
        validator = DataQualityValidator()
        validation_stats = validator.validate_batch(records)
        
        logger.info(f"✓ Quality check complete: {validation_stats['validation_rate']}% pass rate")
        
        # Fail if validation rate is below threshold
        if validation_stats['validation_rate'] < 90:
            error_msg = f"Data quality check failed: {validation_stats['validation_rate']}% validation rate (threshold: 90%)"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        context['task_instance'].xcom_push(key='validation_stats', value=validation_stats)
        
        return validation_stats['validation_rate']
        
    except Exception as e:
        logger.error(f"✗ Quality check failed: {e}", exc_info=True)
        raise


def update_metadata_success_task(**context):
    """
    Update pipeline metadata on successful completion.
    Records run statistics for incremental loading.
    """
    logger.info("=" * 60)
    logger.info("TASK: UPDATE_METADATA_SUCCESS - Recording successful run")
    logger.info("=" * 60)
    
    try:
        ti = context['task_instance']
        run_id = ti.xcom_pull(task_ids='generate_run_id', key='run_id')
        
        # Get statistics from tasks
        extract_count = ti.xcom_pull(task_ids='extract')
        raw_load_count = ti.xcom_pull(task_ids='load_raw')
        transform_stats = ti.xcom_pull(task_ids='transform_clean', key='transform_stats')
        validation_rate = ti.xcom_pull(task_ids='quality_check')
        
        metadata_manager = MetadataManager(pipeline_name=PIPELINE_NAME)
        
        metadata_manager.end_pipeline_run(
            run_id=run_id,
            status='SUCCESS',
            records_processed=extract_count or 0,
            records_inserted=transform_stats['loaded'] if transform_stats else 0,
            records_failed=transform_stats['failed'] if transform_stats else 0
        )
        
        logger.info(f"✓ Metadata updated: Pipeline marked as SUCCESS")
        logger.info(f"  - Records processed: {extract_count}")
        logger.info(f"  - Records inserted: {transform_stats['loaded'] if transform_stats else 0}")
        logger.info(f"  - Validation rate: {validation_rate}%")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Metadata update failed: {e}", exc_info=True)
        raise


def update_metadata_failure_task(**context):
    """
    Update pipeline metadata on failure.
    Records error information for debugging.
    """
    logger.info("=" * 60)
    logger.info("TASK: UPDATE_METADATA_FAILURE - Recording failure")
    logger.info("=" * 60)
    
    try:
        ti = context['task_instance']
        run_id = ti.xcom_pull(task_ids='generate_run_id', key='run_id')
        
        metadata_manager = MetadataManager(pipeline_name=PIPELINE_NAME)
        
        # Get error from task instance
        error_message = str(context.get('exception', 'Unknown error'))
        
        metadata_manager.end_pipeline_run(
            run_id=run_id,
            status='FAILED',
            error_message=error_message
        )
        
        logger.info(f"✗ Metadata updated: Pipeline marked as FAILED")
        logger.info(f"  - Error: {error_message}")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Failure metadata update failed: {e}", exc_info=True)
        # Don't raise - this is a cleanup task


# Define tasks
generate_run_id = PythonOperator(
    task_id='generate_run_id',
    python_callable=generate_run_id,
    dag=dag,
)

extract = PythonOperator(
    task_id='extract',
    python_callable=extract_task,
    dag=dag,
    retries=3,
    retry_delay=timedelta(minutes=2),
)

load_raw = PythonOperator(
    task_id='load_raw',
    python_callable=load_raw_task,
    dag=dag,
    retries=3,
    retry_delay=timedelta(minutes=2),
)

transform_clean = PythonOperator(
    task_id='transform_clean',
    python_callable=transform_clean_task,
    dag=dag,
)

quality_check = PythonOperator(
    task_id='quality_check',
    python_callable=quality_check_task,
    dag=dag,
    retries=2,
    retry_delay=timedelta(minutes=1),
)

update_metadata_success = PythonOperator(
    task_id='update_metadata_success',
    python_callable=update_metadata_success_task,
    dag=dag,
    trigger_rule='all_success',
)

update_metadata_failure = PythonOperator(
    task_id='update_metadata_failure',
    python_callable=update_metadata_failure_task,
    dag=dag,
    trigger_rule='one_failed',
)

# Set task dependencies
# Linear flow: extract → load_raw → transform_clean → quality_check → update_metadata_success
# On failure: update_metadata_failure
generate_run_id >> extract >> load_raw >> transform_clean >> quality_check >> update_metadata_success
[extract, load_raw, transform_clean, quality_check] >> update_metadata_failure
