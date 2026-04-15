"""
FastAPI Service for Weather ETL Pipeline.
Provides endpoints to query weather data from PostgreSQL.
Data Source: weather_clean table only (not direct API calls).
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import psycopg2
from pydantic import BaseModel
import logging

# Add project root to Python path
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import Config
from scripts.logger import setup_logger

# Initialize FastAPI app
app = FastAPI(
    title="Weather ETL API",
    description="Production API for weather data from ETL pipeline",
    version="2.0.0"
)

# Add CORS middleware to allow frontend to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize logger
logger = setup_logger(__name__)


# Helper function to convert UTC to IST
def utc_to_ist(utc_dt):
    """Convert UTC datetime to IST (UTC+05:30)."""
    if utc_dt is None:
        return None
    return utc_dt + timedelta(hours=5, minutes=30)


# Pydantic models for request/response
class WeatherData(BaseModel):
    city_name: str
    recorded_at: datetime
    country_code: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    temperature_celsius: float
    feels_like_celsius: Optional[float]
    humidity_percent: Optional[int]
    pressure_hpa: Optional[int]
    wind_speed_mps: Optional[float]
    wind_direction_degrees: Optional[int]
    weather_main: Optional[str]
    weather_description: Optional[str]
    visibility_meters: Optional[int]
    cloudiness_percent: Optional[int]
    ingestion_timestamp: datetime


class PipelineStatus(BaseModel):
    pipeline_name: str
    last_successful_run: Optional[datetime]
    last_successful_run_id: Optional[str]
    records_processed: int
    records_inserted: int
    records_failed: int
    pipeline_status: str
    error_message: Optional[str]
    updated_at: datetime


class HistoricalTrend(BaseModel):
    timestamp: datetime
    avg_temperature: float
    avg_humidity: float
    record_count: int


class APIResponse(BaseModel):
    success: bool
    data: Any
    message: Optional[str] = None
    count: int = 0


def get_db_connection():
    """Create database connection."""
    try:
        conn = psycopg2.connect(
            host=Config.POSTGRES_HOST,
            port=Config.POSTGRES_PORT,
            database=Config.POSTGRES_DB,
            user=Config.POSTGRES_USER,
            password=Config.POSTGRES_PASSWORD
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")


@app.get("/", response_model=dict)
async def root():
    """Root endpoint with API information."""
    return {
        "service": "Weather ETL API",
        "version": "2.0.0",
        "status": "operational",
        "endpoints": {
            "latest-data": "/api/latest-data",
            "historical-trend": "/api/historical-trend",
            "pipeline-status": "/api/pipeline-status"
        }
    }


@app.get("/api/latest-data", response_model=APIResponse)
async def get_latest_data(
    city: Optional[str] = Query(None, description="Filter by city name"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of records")
):
    """
    Get latest weather data from weather_clean table.
    Returns the most recent weather records for all cities or a specific city.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        query = """
        WITH ranked_data AS (
            SELECT 
                wc.city_name,
                wc.recorded_at,
                wc.country_code,
                wc.latitude,
                wc.longitude,
                wc.temperature_celsius,
                wc.feels_like_celsius,
                wc.humidity_percent,
                wc.pressure_hpa,
                wc.wind_speed_mps,
                wc.wind_direction_degrees,
                wc.weather_main,
                wc.weather_description,
                wc.visibility_meters,
                wc.cloudiness_percent,
                wc.ingestion_timestamp,
                ROW_NUMBER() OVER (PARTITION BY wc.city_name ORDER BY wc.recorded_at DESC) as rn
            FROM weather_clean wc
            INNER JOIN cities c ON wc.city_id = c.id
            WHERE c.active = TRUE
        )
        SELECT 
            city_name, recorded_at, country_code, latitude, longitude,
            temperature_celsius, feels_like_celsius, humidity_percent,
            pressure_hpa, wind_speed_mps, wind_direction_degrees,
            weather_main, weather_description, visibility_meters,
            cloudiness_percent, ingestion_timestamp
        FROM ranked_data
        WHERE rn = 1
        """
        
        params = []
        if city:
            query += " AND city_name = %s"
            params.append(city)
        
        query += " ORDER BY city_name"
        
        if limit:
            query += " LIMIT %s"
            params.append(limit)
        
        cur.execute(query, params)
        rows = cur.fetchall()
        
        # Convert to list of dicts with IST timestamps
        data = []
        for row in rows:
            data.append({
                'city_name': row[0],
                'recorded_at': utc_to_ist(row[1]),
                'country_code': row[2],
                'latitude': float(row[3]) if row[3] else None,
                'longitude': float(row[4]) if row[4] else None,
                'temperature_celsius': float(row[5]),
                'feels_like_celsius': float(row[6]) if row[6] else None,
                'humidity_percent': row[7],
                'pressure_hpa': row[8],
                'wind_speed_mps': float(row[9]) if row[9] else None,
                'wind_direction_degrees': row[10],
                'weather_main': row[11],
                'weather_description': row[12],
                'visibility_meters': row[13],
                'cloudiness_percent': row[14],
                'ingestion_timestamp': utc_to_ist(row[15])
            })
        
        cur.close()
        conn.close()
        
        logger.info(f"Retrieved {len(data)} latest weather records")
        
        return APIResponse(
            success=True,
            data=data,
            count=len(data),
            message=f"Retrieved {len(data)} records"
        )
        
    except Exception as e:
        logger.error(f"Error fetching latest data: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")


@app.get("/api/historical-trend", response_model=APIResponse)
async def get_historical_trend(
    city: Optional[str] = Query(None, description="Filter by city name"),
    days: int = Query(7, ge=1, le=30, description="Number of days to look back"),
    metric: str = Query("temperature", regex="^(temperature|humidity)$", description="Metric to analyze")
):
    """
    Get historical weather trends from weather_clean table.
    Returns aggregated statistics over a time period.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        start_date = datetime.now() - timedelta(days=days)
        
        query = """
        SELECT 
            DATE_TRUNC('hour', wc.recorded_at) as hour,
            AVG(wc.temperature_celsius) as avg_temperature,
            AVG(wc.humidity_percent) as avg_humidity,
            COUNT(*) as record_count
        FROM weather_clean wc
        INNER JOIN cities c ON wc.city_id = c.id
        WHERE wc.recorded_at >= %s AND c.active = TRUE
        """
        
        params = [start_date]
        
        if city:
            query += " AND city_name = %s"
            params.append(city)
        
        query += " GROUP BY DATE_TRUNC('hour', recorded_at) ORDER BY hour ASC"
        
        cur.execute(query, params)
        rows = cur.fetchall()
        
        # Convert to list of dicts with IST timestamps
        data = []
        for row in rows:
            data.append({
                'timestamp': utc_to_ist(row[0]),
                'avg_temperature': float(row[1]) if row[1] else None,
                'avg_humidity': float(row[2]) if row[2] else None,
                'record_count': row[3]
            })
        
        cur.close()
        conn.close()
        
        logger.info(f"Retrieved {len(data)} historical trend data points")
        
        return APIResponse(
            success=True,
            data=data,
            count=len(data),
            message=f"Retrieved {len(data)} trend points over {days} days"
        )
        
    except Exception as e:
        logger.error(f"Error fetching historical trends: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching trends: {str(e)}")


@app.get("/api/pipeline-status", response_model=APIResponse)
async def get_pipeline_status():
    """
    Get the status of the ETL pipeline from pipeline_metadata table.
    Returns last successful run information and current status.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        query = """
        SELECT 
            pipeline_name, last_successful_run, last_successful_run_id,
            records_processed, records_inserted, records_failed,
            pipeline_status, error_message, updated_at
        FROM pipeline_metadata
        WHERE pipeline_name = %s
        ORDER BY updated_at DESC
        LIMIT 1;
        """
        
        cur.execute(query, ('weather_etl_pipeline_v2',))
        row = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if row:
            data = {
                'pipeline_name': row[0],
                'last_successful_run': utc_to_ist(row[1]),
                'last_successful_run_id': row[2],
                'records_processed': row[3],
                'records_inserted': row[4],
                'records_failed': row[5],
                'pipeline_status': row[6],
                'error_message': row[7],
                'updated_at': utc_to_ist(row[8])
            }
            logger.info(f"Retrieved pipeline status: {data['pipeline_status']}")
        else:
            data = {
                'pipeline_name': 'weather_etl_pipeline_v2',
                'pipeline_status': 'NOT_INITIALIZED',
                'message': 'No pipeline runs recorded'
            }
            logger.info("No pipeline status found")
        
        return APIResponse(
            success=True,
            data=data,
            count=1 if row else 0,
            message="Pipeline status retrieved"
        )
        
    except Exception as e:
        logger.error(f"Error fetching pipeline status: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching pipeline status: {str(e)}")




@app.get("/api/data-quality", response_model=APIResponse)
async def get_data_quality():
    """
    Get data quality metrics from weather_clean table.
    Returns validation statistics and quality indicators.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get basic statistics
        query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN wc.temperature_celsius < -100 OR wc.temperature_celsius > 60 THEN 1 END) as invalid_temp,
            COUNT(CASE WHEN wc.humidity_percent < 0 OR wc.humidity_percent > 100 THEN 1 END) as invalid_humidity,
            COUNT(CASE WHEN wc.pressure_hpa < 800 OR wc.pressure_hpa > 1100 THEN 1 END) as invalid_pressure,
            COUNT(CASE WHEN wc.wind_speed_mps < 0 OR wc.wind_speed_mps > 150 THEN 1 END) as invalid_wind,
            COUNT(CASE WHEN wc.city_name IS NULL THEN 1 END) as null_city,
            MAX(wc.ingestion_timestamp) as last_ingestion
        FROM weather_clean wc
        INNER JOIN cities c ON wc.city_id = c.id
        WHERE c.active = TRUE;
        """
        
        cur.execute(query)
        row = cur.fetchone()
        
        total_records = row[0]
        invalid_temp = row[1]
        invalid_humidity = row[2]
        invalid_pressure = row[3]
        invalid_wind = row[4]
        null_city = row[5]
        last_ingestion = row[6]
        
        # Calculate quality score
        total_issues = invalid_temp + invalid_humidity + invalid_pressure + invalid_wind + null_city
        quality_score = 0
        if total_records > 0:
            quality_score = round(((total_records - total_issues) / total_records) * 100, 2)
        
        # Determine status
        status = "OK"
        if quality_score < 90:
            status = "WARNING"
        if quality_score < 80:
            status = "CRITICAL"
        
        data = {
            'total_records': total_records,
            'invalid_temperature_count': invalid_temp,
            'invalid_humidity_count': invalid_humidity,
            'invalid_pressure_count': invalid_pressure,
            'invalid_wind_count': invalid_wind,
            'null_city_count': null_city,
            'total_issues': total_issues,
            'quality_score': quality_score,
            'quality_status': status,
            'last_ingestion': utc_to_ist(last_ingestion).isoformat() if last_ingestion else None
        }
        
        cur.close()
        conn.close()
        
        logger.info(f"Data quality: {quality_score}% ({status})")
        
        return APIResponse(
            success=True,
            data=data,
            count=1,
            message=f"Quality score: {quality_score}%"
        )
        
    except Exception as e:
        logger.error(f"Error fetching data quality: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching data quality: {str(e)}")


@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "database": "disconnected",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
        )


@app.get("/api/dag-status", response_model=APIResponse)
async def get_dag_status():
    """
    Get the current status of the weather ETL DAG.
    Returns whether the DAG is currently running.
    """
    try:
        import requests
        airflow_url = "http://airflow-webserver:8080/api/v1/dags/weather_etl_pipeline_v2/dagRuns"
        airflow_auth = ("airflow", "airflow")
        
        response = requests.get(
            airflow_url,
            auth=airflow_auth,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            dag_runs = response.json().get("dag_runs", [])
            # Check if any DAG run is currently running
            is_running = any(run.get("state") == "running" for run in dag_runs)
            
            return APIResponse(
                success=True,
                data={"is_running": is_running},
                message="DAG status retrieved successfully"
            )
        else:
            return APIResponse(
                success=False,
                data={"is_running": False},
                message=f"Failed to retrieve DAG status: {response.status_code}"
            )
    except Exception as e:
        logger.error(f"Error retrieving DAG status: {e}")
        return APIResponse(
            success=False,
            data={"is_running": False},
            message=f"Error retrieving DAG status: {str(e)}"
        )


@app.get("/api/cities", response_model=APIResponse)
async def list_cities():
    """Get list of all active cities."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        query = """
        SELECT id, city_name, country_code, active, created_at
        FROM cities
        WHERE active = TRUE
        ORDER BY city_name;
        """
        
        cur.execute(query)
        rows = cur.fetchall()
        
        cities = []
        for row in rows:
            cities.append({
                'id': row[0],
                'city_name': row[1],
                'country_code': row[2],
                'active': row[3],
                'created_at': row[4]
            })
        
        cur.close()
        conn.close()
        
        logger.info(f"Retrieved {len(cities)} cities")
        
        return APIResponse(
            success=True,
            data=cities,
            count=len(cities),
            message=f"Retrieved {len(cities)} cities"
        )
        
    except Exception as e:
        logger.error(f"Error fetching cities: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching cities: {str(e)}")


@app.post("/api/cities", response_model=APIResponse)
async def add_city(request: dict):
    """Add a new city to the monitoring list."""
    try:
        city_name = request.get('city_name')
        country_code = request.get('country_code', 'IN')
        
        if not city_name:
            raise HTTPException(status_code=400, detail="city_name is required")
        
        # Normalize city name to title case
        city_name = city_name.strip().title()
        
        # Validate city using OpenWeatherMap API
        import requests
        api_key = "71ea710fd7b13fd6e8f5c5a6253f6f46"
        weather_url = f"http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}"
        
        try:
            response = requests.get(weather_url)
            if response.status_code == 404:
                return APIResponse(
                    success=False,
                    data={'city_name': city_name},
                    message=f"no city found named '{city_name}'"
                )
            elif response.status_code != 200:
                logger.warning(f"OpenWeatherMap API returned status {response.status_code}")
        except Exception as e:
            logger.warning(f"Error validating city with OpenWeatherMap API: {e}")
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Check if city already exists and is active (case-insensitive)
        query = """
        SELECT id, city_name, country_code, active, created_at
        FROM cities
        WHERE LOWER(city_name) = LOWER(%s);
        """
        
        cur.execute(query, (city_name,))
        existing_city = cur.fetchone()
        
        is_new_city = False
        
        if existing_city and existing_city[3]:  # City exists and is active
            # City is already active in the list
            row = existing_city
            message = f"City '{city_name}' is already in the list"
        else:
            # City doesn't exist or is inactive, insert/update as active
            if existing_city:
                # City exists but inactive, reactivate it
                query = """
                UPDATE cities
                SET active = TRUE
                WHERE id = %s
                RETURNING id, city_name, country_code, active, created_at;
                """
                
                cur.execute(query, (existing_city[0],))
                row = cur.fetchone()
                message = f"City '{city_name}' reactivated successfully"
            else:
                # City doesn't exist, insert it
                query = """
                INSERT INTO cities (city_name, country_code, active)
                VALUES (%s, %s, TRUE)
                RETURNING id, city_name, country_code, active, created_at;
                """
                
                cur.execute(query, (city_name, country_code))
                row = cur.fetchone()
                message = f"City '{city_name}' added successfully"
                is_new_city = True
        
        # No need to update weather_clean and weather_raw anymore since they use cities.active
        # The foreign key relationship ensures data integrity
        
        conn.commit()
        
        cur.close()
        conn.close()
        
        logger.info(f"Added city: {city_name}")
        
        # Trigger DAG if this is a new city
        if is_new_city:
            try:
                airflow_url = "http://airflow-webserver:8080/api/v1/dags/weather_etl_pipeline_v2/dagRuns"
                airflow_auth = ("airflow", "airflow")
                
                response = requests.post(
                    airflow_url,
                    json={"conf": {}},
                    auth=airflow_auth,
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code == 200:
                    logger.info(f"DAG triggered successfully for new city: {city_name}")
                    message += " - Pipeline triggered to fetch weather data"
                else:
                    logger.warning(f"Failed to trigger DAG: {response.status_code} - {response.text}")
            except Exception as e:
                logger.warning(f"Error triggering DAG: {e}")
        
        return APIResponse(
            success=True,
            data={
                'id': row[0],
                'city_name': row[1],
                'country_code': row[2],
                'active': row[3],
                'created_at': row[4]
            },
            message=message
        )
        
    except Exception as e:
        logger.error(f"Error adding city: {e}")
        raise HTTPException(status_code=500, detail=f"Error adding city: {str(e)}")


@app.delete("/api/cities/{city_name}", response_model=APIResponse)
async def remove_city(city_name: str):
    """Remove a city from the monitoring list (soft delete) and clean up weather data."""
    try:
        # Normalize city name to title case
        city_name = city_name.strip().title()
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Check if city exists (case-insensitive)
        query = """
        SELECT id, city_name, active
        FROM cities
        WHERE LOWER(city_name) = LOWER(%s);
        """
        
        cur.execute(query, (city_name,))
        existing_city = cur.fetchone()
        
        if not existing_city:
            cur.close()
            conn.close()
            return APIResponse(
                success=False,
                data={'city_name': city_name},
                message=f"City '{city_name}' not available in the list"
            )
        
        # Mark city as inactive in cities table (case-insensitive)
        query = """
        UPDATE cities
        SET active = FALSE
        WHERE LOWER(city_name) = LOWER(%s);
        """
        
        cur.execute(query, (city_name,))
        
        conn.commit()
        
        cur.close()
        conn.close()
        
        logger.info(f"Removed city: {city_name} and marked as inactive")
        
        return APIResponse(
            success=True,
            data={'city_name': city_name},
            message=f"City '{city_name}' removed successfully"
        )
        
    except Exception as e:
        logger.error(f"Error removing city: {e}")
        raise HTTPException(status_code=500, detail=f"Error removing city: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    logger.info("Starting FastAPI server on port 8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
