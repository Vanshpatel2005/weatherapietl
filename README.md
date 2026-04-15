# Weather ETL Pipeline (Production-Ready)

A production-grade ETL pipeline that extracts weather data from the OpenWeatherMap API, processes it through a raw-to-clean architecture, and serves it via FastAPI. The pipeline is orchestrated using Apache Airflow and implements incremental loading with metadata tracking.

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    OpenWeatherMap API                           │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                    EXTRACT (extract.py)                         │
│  • Fetch weather data for all configured cities                 │
│  • Implement retry logic (3 attempts with exponential backoff)  │
│  • Handle rate limiting and HTTP errors                         │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│              LOAD RAW (load_raw.py)                             │
│  • Store full API JSON response in weather_raw table           │
│  • Preserve original data for audit trail                       │
│  • Mark records as unprocessed                                  │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│           TRANSFORM & LOAD CLEAN (load_clean.py)                 │
│  • Fetch unprocessed records from weather_raw                   │
│  • Transform JSON to structured format                          │
│  • Apply data quality validation                                 │
│  • Load into weather_clean table                                │
│  • Mark raw records as processed                                │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│               QUALITY CHECK (validate.py)                        │
│  • Validate temperature ranges (-100°C to 60°C)                 │
│  • Validate humidity ranges (0-100%)                            │
│  • Ensure no nulls in critical fields                           │
│  • Fail pipeline if validation rate < 90%                        │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│           UPDATE METADATA (metadata_manager.py)                  │
│  • Record pipeline execution status                              │
│  • Track last successful run for incremental loading            │
│  • Store record counts and error messages                       │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PostgreSQL Database                           │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐           │
│  │weather_raw  │  │weather_clean│  │pipeline_     │           │
│  │(JSON audit) │  │(structured) │  │metadata      │           │
│  └─────────────┘  └─────────────┘  └──────────────┘           │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                    FastAPI Service                              │
│  • /api/latest-data - Get recent weather data                  │
│  • /api/historical-trend - Get historical trends                │
│  • /api/pipeline-status - Get pipeline execution status         │
│  • /api/cities - List all monitored cities                      │
└─────────────────────────────────────────────────────────────────┘
```

## 📁 Project Structure

```
weatherapietl/
├── dags/
│   ├── __init__.py
│   ├── weather_etl_dag.py              # Original DAG (legacy)
│   └── weather_etl_dag_v2.py           # Production DAG (new architecture)
├── scripts/
│   ├── __init__.py
│   ├── logger.py                       # Logging configuration
│   ├── extract.py                      # Data extraction from API
│   ├── transform.py                    # Data transformation and cleaning
│   ├── validate.py                     # Data quality validation
│   ├── load.py                         # Original loader (legacy)
│   ├── load_raw.py                     # Raw data loader (NEW)
│   ├── load_clean.py                   # Clean data loader (NEW)
│   ├── metadata_manager.py             # Metadata tracking (NEW)
│   ├── create_watchlist.py             # Watchlist table creation
│   ├── setup_database.py               # Database setup script
│   └── run_pipeline.py                 # Pipeline runner script
├── sql/
│   ├── 01_create_raw_table.sql         # weather_raw table schema (NEW)
│   ├── 02_create_clean_table.sql       # weather_clean table schema (NEW)
│   └── 03_create_metadata_table.sql    # pipeline_metadata table schema (NEW)
├── api/
│   ├── __init__.py
│   └── main.py                         # FastAPI service (NEW)
├── config/
│   ├── __init__.py
│   └── config.py                       # Configuration management
├── templates/
│   └── dashboard.html                  # Flask dashboard UI
├── logs/                               # Log files directory
├── .env                                # Environment variables
├── .env.example                        # Environment variables template
├── requirements.txt                    # Python dependencies
├── docker-compose.yml                  # Docker Compose configuration
├── app.py                              # Flask dashboard application
├── start_dashboard.bat                 # Dashboard launcher (Windows)
└── README.md                           # This file
```

## 🎯 Key Design Decisions

### 1. Separate Raw and Clean Tables

**Why?**
- **Audit Trail**: `weather_raw` preserves original API responses for debugging and compliance
- **Reprocessing**: If transformation logic changes, raw data can be reprocessed
- **Flexibility**: Different consumers can apply different transformations to raw data
- **Data Lineage**: Track source of every record in clean table via `raw_id` foreign key

**Schema Differences:**
- `weather_raw`: JSONB column for flexible schema, minimal structure
- `weather_clean`: Structured columns with proper data types, constraints, and composite PK

### 2. Composite Primary Key in Clean Table

**Design:** `PRIMARY KEY (city_name, recorded_at)`

**Why?**
- **Prevents Duplicates**: Ensures no duplicate records for same city at same timestamp
- **Natural Key**: Business logic naturally defines uniqueness by city + time
- **Query Performance**: Optimizes common queries filtering by city and time
- **Idempotency**: Safe to rerun pipeline without creating duplicates

### 3. Metadata Table for Incremental Loading

**Why?**
- **Incremental Processing**: Only process data since last successful run
- **Pipeline State**: Track current status (IDLE, RUNNING, SUCCESS, FAILED)
- **Observability**: Quick view of pipeline health and statistics
- **Failure Recovery**: Know exactly where pipeline failed

### 4. Airflow DAG with Separate Tasks

**Task Flow:**
1. `generate_run_id` → Generate unique run identifier
2. `extract` → Fetch weather data from API
3. `load_raw` → Store raw JSON responses
4. `transform_clean` → Transform and load into clean table
5. `quality_check` → Validate data quality
6. `update_metadata_success` → Record successful completion
7. `update_metadata_failure` → Record failure (on any task failure)

**Why Separate Tasks?**
- **Granularity**: Each task has a specific responsibility
- **Retry Isolation**: Failed task can be retried independently
- **Monitoring**: Track performance of each pipeline stage
- **Idempotency**: Each task can be safely rerun

### 5. Data Quality Validation

**Validation Rules:**
- Temperature: -100°C to 60°C
- Humidity: 0% to 100%
- Pressure: 800 hPa to 1100 hPa
- Wind Speed: 0 to 150 m/s
- Critical Fields: city_name, temperature_celsius, weather_timestamp must not be null

**Why?**
- **Data Integrity**: Ensure only valid data reaches production
- **Early Failure**: Catch issues before they propagate
- **Trust**: Downstream systems can rely on data quality
- **Debugging**: Validation errors help identify API issues

## 🚀 Features

### Core ETL Features
- **Modular Architecture**: Separate modules for extract, transform, load, and validation
- **Error Handling**: Comprehensive error handling with retry logic for API calls
- **Data Quality**: Built-in validation to ensure data integrity
- **Incremental Loading**: Metadata tracking prevents reprocessing
- **Audit Trail**: Raw data preserved for compliance and debugging
- **Idempotency**: Safe to rerun pipeline without side effects
- **Logging**: Centralized logging system for debugging and monitoring

### API Features (FastAPI)
- **Production-Ready**: FastAPI with automatic documentation
- **Type Safety**: Pydantic models for request/response validation
- **Performance**: Async support for high throughput
- **Data Source**: Query weather_clean table only (no direct API calls)
- **Endpoints**:
  - `GET /api/latest-data` - Latest weather data with optional city filter
  - `GET /api/historical-trend` - Historical trends with aggregation
  - `GET /api/pipeline-status` - Pipeline execution status
  - `GET /api/cities` - List all monitored cities
  - `GET /health` - Health check endpoint

### Orchestration Features (Airflow)
- **Task Dependencies**: Clear dependency flow between pipeline stages
- **Retries**: Configurable retry logic with exponential backoff
- **Monitoring**: Airflow UI for pipeline visibility
- **Scheduling**: Configurable schedule interval
- **Failure Handling**: Automatic metadata updates on failure
- **Idempotency**: `max_active_runs=1` ensures no concurrent executions

## 📋 Prerequisites

- Python 3.12 or higher
- PostgreSQL 15 or higher
- Apache Airflow 2.9.3 or higher
- OpenWeatherMap API key (free tier available at https://openweathermap.org/api)

## 🔧 Setup Instructions

### 1. Clone the Repository

```bash
cd c:/Users/patel/OneDrive/Desktop/weatherapietl
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Set Up Environment Variables

Edit `.env` and add your actual values:

```env
# OpenWeatherMap API Configuration
OPENWEATHER_API_KEY=your_actual_api_key_here
OPENWEATHER_BASE_URL=https://api.openweathermap.org/data/2.5/weather
CITIES=London,New York,Tokyo,Paris,Sydney

# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=weather_warehouse
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_actual_password_here

# Airflow Configuration
AIRFLOW_HOME=/path/to/airflow
DAG_ID=weather_etl_pipeline_v2
SCHEDULE_INTERVAL=0 2 * * *

# Logging Configuration
LOG_LEVEL=INFO
LOG_FILE_PATH=logs/etl_pipeline.log
```

### 4. Set Up PostgreSQL Database

Create a new PostgreSQL database:

```sql
CREATE DATABASE weather_warehouse;
```

Run the schema scripts in order:

```bash
psql -U postgres -d weather_warehouse -f sql/01_create_raw_table.sql
psql -U postgres -d weather_warehouse -f sql/02_create_clean_table.sql
psql -U postgres -d weather_warehouse -f sql/03_create_metadata_table.sql
```

### 5. Configure Airflow

If you don't have Airflow installed, initialize it:

```bash
# Set AIRFLOW_HOME environment variable
export AIRFLOW_HOME=~/airflow

# Initialize Airflow database
airflow db init

# Create an admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Start the webserver (in a separate terminal)
airflow webserver --port 8080

# Start the scheduler (in another separate terminal)
airflow scheduler
```

Copy the DAG file to your Airflow DAGs folder:

```bash
cp dags/weather_etl_dag_v2.py $AIRFLOW_HOME/dags/
```

### 6. Run the Pipeline via Airflow

1. Open the Airflow web UI at http://localhost:8080
2. Navigate to the "DAGs" page
3. Find the `weather_etl_pipeline_v2` DAG
4. Click the "Trigger DAG" button to run manually, or wait for the scheduled run

### 7. Start the FastAPI Service

```bash
cd api
python main.py
```

Or using uvicorn directly:

```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

**Access the API Documentation:**
Open http://localhost:8000/docs for interactive API documentation

### 8. Test the API Endpoints

```bash
# Get latest data
curl http://localhost:8000/api/latest-data

# Get latest data for specific city
curl http://localhost:8000/api/latest-data?city=London

# Get historical trends
curl http://localhost:8000/api/historical-trend?days=7

# Get pipeline status
curl http://localhost:8000/api/pipeline-status

# Get all cities
curl http://localhost:8000/api/cities

# Health check
curl http://localhost:8000/health
```

## 📊 Database Schema

### Table: `weather_raw`

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| city_name | VARCHAR(100) | City name |
| raw_data | JSONB | Full API JSON response |
| api_response_code | INTEGER | HTTP status code from API |
| extraction_timestamp | TIMESTAMP | When data was extracted |
| ingestion_timestamp | TIMESTAMP | When data was loaded |
| processed | BOOLEAN | Whether transformed to clean table |

**Indexes:**
- idx_raw_city_name (city_name)
- idx_raw_extraction_timestamp (extraction_timestamp)
- idx_raw_processed (processed)
- idx_raw_data_gin (raw_data) - GIN index for JSONB queries

### Table: `weather_clean`

| Column | Type | Description |
|--------|------|-------------|
| city_name | VARCHAR(100) | City name (PK part 1) |
| recorded_at | TIMESTAMP | Weather observation timestamp (PK part 2) |
| country_code | VARCHAR(10) | ISO country code |
| latitude | DECIMAL(10,6) | Latitude coordinate |
| longitude | DECIMAL(10,6) | Longitude coordinate |
| temperature_celsius | DECIMAL(5,2) | Temperature in Celsius |
| feels_like_celsius | DECIMAL(5,2) | Feels like temperature |
| humidity_percent | INTEGER | Humidity percentage (0-100, CHECK constraint) |
| pressure_hpa | INTEGER | Atmospheric pressure in hPa (800-1100, CHECK constraint) |
| wind_speed_mps | DECIMAL(5,2) | Wind speed in m/s (>=0, CHECK constraint) |
| wind_direction_degrees | INTEGER | Wind direction in degrees (0-360, CHECK constraint) |
| weather_main | VARCHAR(50) | Main weather condition |
| weather_description | VARCHAR(200) | Detailed weather description |
| visibility_meters | INTEGER | Visibility in meters |
| cloudiness_percent | INTEGER | Cloudiness percentage (0-100, CHECK constraint) |
| ingestion_timestamp | TIMESTAMP | When data was loaded |
| raw_id | INTEGER | Foreign key to weather_raw(id) |

**Constraints:**
- PRIMARY KEY (city_name, recorded_at)
- CHECK constraints on numeric ranges
- Foreign key to weather_raw

**Indexes:**
- idx_clean_city_name (city_name)
- idx_clean_recorded_at (recorded_at)
- idx_clean_ingestion_timestamp (ingestion_timestamp)
- idx_clean_raw_id (raw_id)

### Table: `pipeline_metadata`

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| pipeline_name | VARCHAR(100) | Pipeline name (UNIQUE) |
| last_successful_run | TIMESTAMP | Timestamp of last successful run |
| last_successful_run_id | VARCHAR(100) | Run ID of last successful run |
| records_processed | INTEGER | Total records processed |
| records_inserted | INTEGER | Records successfully inserted |
| records_failed | INTEGER | Records that failed |
| pipeline_status | VARCHAR(50) | Current status (IDLE/RUNNING/SUCCESS/FAILED) |
| error_message | TEXT | Error message if failed |
| created_at | TIMESTAMP | Record creation timestamp |
| updated_at | TIMESTAMP | Record update timestamp (auto-updated) |

**Indexes:**
- idx_metadata_pipeline_name (pipeline_name)

## 📝 Logging

Logs are written to `logs/etl_pipeline.log` and include:
- Timestamp
- Module name
- Log level (INFO, WARNING, ERROR)
- Detailed message

Log level can be configured via `LOG_LEVEL` in `.env` file.

**Pipeline Execution Logs Include:**
- Number of records extracted
- Number of records transformed
- Number of records validated
- Number of records loaded
- Validation rate percentage
- Error messages for failed records

## 🐛 Troubleshooting

### API Key Issues

If you get "Invalid API key" errors:
1. Verify your API key is correct in `.env`
2. Ensure your OpenWeatherMap account is active
3. Check if you've exceeded API rate limits (free tier: 60 calls/minute)

### Database Connection Issues

If you can't connect to PostgreSQL:
1. Verify PostgreSQL is running: `pg_isready`
2. Check credentials in `.env` file
3. Ensure the database `weather_warehouse` exists
4. Check firewall/network settings

### Airflow DAG Not Appearing

If the DAG doesn't show in Airflow UI:
1. Verify DAG file is in `$AIRFLOW_HOME/dags/` folder
2. Check for syntax errors in the DAG file
3. Restart Airflow webserver and scheduler
4. Check Airflow logs for errors

### Data Quality Failures

If quality checks fail:
1. Check logs for specific validation errors
2. Verify API is returning expected data format
3. Adjust validation thresholds in `scripts/validate.py` if needed
4. Check if cities in configuration are valid

### FastAPI Service Won't Start

If FastAPI fails to start:
1. Check port 8000 is not already in use
2. Verify all dependencies are installed: `pip install -r requirements.txt`
3. Check database connection settings in `.env`
4. Review error logs for specific issues

## 🎓 Learning Points for Beginners

This project demonstrates several key data engineering concepts:

1. **Raw-to-Clean Architecture**: Separation of raw and processed data
2. **Incremental Loading**: Metadata tracking for efficient processing
3. **Composite Primary Keys**: Natural keys for business logic
4. **JSONB Storage**: Flexible schema for raw data
5. **Data Quality Validation**: Ensuring data integrity
6. **Orchestration**: Airflow for workflow automation
7. **API Design**: RESTful API with FastAPI
8. **Idempotency**: Safe rerun of pipeline operations
9. **Audit Trails**: Preserving data lineage
10. **Type Safety**: Pydantic models for API validation

## 🔐 Security Best Practices

- Never commit `.env` file to version control
- Use strong passwords for PostgreSQL
- Rotate API keys periodically
- Limit database user permissions to only what's needed
- Use environment variables for all sensitive data
- Consider using secrets management tools in production (e.g., AWS Secrets Manager, HashiCorp Vault)
- Enable SSL/TLS for database connections in production
- Implement authentication for FastAPI endpoints in production

## 📈 Monitoring and Maintenance

### Regular Tasks

1. **Monitor Logs**: Check `logs/etl_pipeline.log` for errors
2. **Database Maintenance**: Run VACUUM and ANALYZE periodically
3. **API Usage**: Monitor OpenWeatherMap API usage to avoid limits
4. **Airflow Health**: Check Airflow UI for failed DAG runs
5. **Storage**: Monitor disk space for logs and database growth
6. **Pipeline Status**: Check `/api/pipeline-status` endpoint

### Alerts (Recommended)

Consider setting up alerts for:
- Pipeline failures (Airflow alerts or custom monitoring)
- Data quality issues (validation rate < 90%)
- Database connection failures
- API rate limit warnings
- FastAPI service downtime

## 🚀 Production Considerations

For production deployment, consider:

1. **Containerization**: Dockerize the application for consistency
2. **Infrastructure as Code**: Use Terraform or CloudFormation for infrastructure
3. **CI/CD Pipeline**: Automate testing and deployment
4. **Monitoring**: Use tools like Prometheus, Grafana, or DataDog
5. **Error Notifications**: Integrate with PagerDuty, Slack, or email
6. **Backup Strategy**: Regular database backups (daily at minimum)
7. **High Availability**: Multi-zone deployment for critical systems
8. **Scalability**: Consider using cloud services (AWS, GCP, Azure)
9. **API Authentication**: Add JWT or OAuth2 to FastAPI
10. **Rate Limiting**: Implement rate limiting on FastAPI endpoints

## 🔄 Migration from Legacy DAG

To migrate from the original DAG to the new production DAG:

1. **Backup existing data**: Export from `weather_data` table
2. **Run new schema scripts**: Create raw, clean, and metadata tables
3. **Migrate data**: Transform and load existing data into new schema
4. **Update Airflow**: Switch DAG from `weather_etl_pipeline` to `weather_etl_pipeline_v2`
5. **Update API consumers**: Switch from Flask dashboard to FastAPI endpoints
6. **Monitor**: Run both in parallel briefly for validation
7. **Decommission**: Remove old DAG and `weather_data` table after validation

## 📄 License

This project is for educational purposes. Feel free to use and modify as needed.

## 👤 Author

Built as a production-grade ETL pipeline project for learning and portfolio purposes.

## 🙏 Acknowledgments

- OpenWeatherMap for providing the weather API
- Apache Airflow community for the orchestration tool
- PostgreSQL team for the database system
- FastAPI team for the modern web framework
