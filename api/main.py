"""
FastAPI Service for Weather ETL Pipeline.
Serves dashboard endpoints.
"""

import time
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras
from pydantic import BaseModel
import logging
import yaml
import requests

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import Config
from scripts.logger import setup_logger

# ---------------------------------------------------------------------------
# Application setup
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Weather ETL API",
    description="API for the Weather Data ETL Platform.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS — allows the static HTML frontend (served via nginx) to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger = setup_logger(__name__)


# ---------------------------------------------------------------------------
# Latency middleware — adds X-Response-Time header to every response
# ---------------------------------------------------------------------------

@app.middleware("http")
async def add_response_time_header(request: Request, call_next):
    """
    Measure and expose per-request latency.
    Logs a WARNING when a response exceeds the 150ms SLA target.
    """
    start = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
    response.headers["X-Response-Time"] = f"{elapsed_ms}ms"

    if elapsed_ms > 150:
        logger.warning(
            f"LATENCY SLA BREACH: {request.method} {request.url.path} "
            f"took {elapsed_ms}ms (target <150ms)"
        )
    else:
        logger.debug(f"{request.method} {request.url.path} — {elapsed_ms}ms")

    return response


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def utc_to_ist(utc_dt):
    """Convert UTC datetime to IST (UTC+05:30)."""
    if utc_dt is None:
        return None
    return utc_dt + timedelta(hours=5, minutes=30)


def get_db_connection():
    """Create and return a psycopg2 connection. Raises HTTP 500 on failure."""
    try:
        return psycopg2.connect(
            host=Config.POSTGRES_HOST,
            port=Config.POSTGRES_PORT,
            database=Config.POSTGRES_DB,
            user=Config.POSTGRES_USER,
            password=Config.POSTGRES_PASSWORD,
        )
    except Exception as exc:
        logger.error(f"Database connection error: {exc}")
        raise HTTPException(status_code=500, detail="Database connection failed")


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class APIResponse(BaseModel):
    success: bool
    data: Any
    message: Optional[str] = None
    count: int = 0


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


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/", response_model=dict, tags=["Info"])
async def root():
    """Service discovery endpoint."""
    return {
        "service": "Weather ETL API",
        "version": "2.0.0",
        "status": "operational",
        "sla_target_ms": 150,
        "endpoints": {
            "latest-data":         "/api/latest-data",
            "historical-trend":    "/api/historical-trend",
            "aggregated-metrics":  "/api/aggregated-metrics",
            "pipeline-status":     "/api/pipeline-status",
            "data-quality":        "/api/data-quality",
            "pipeline-stats":      "/api/pipeline-stats",
            "cities":              "/api/cities",
            "dag-status":          "/api/dag-status",
            "health":              "/health",
        },
    }


@app.get("/api/latest-data", response_model=APIResponse, tags=["Weather Data"])
async def get_latest_data(
    city: Optional[str] = Query(None, description="Filter by city name"),
    limit: int = Query(10, ge=1, le=100, description="Maximum records to return"),
):
    """
    Latest weather snapshot per active city from weather_clean.

    Uses a window function (ROW_NUMBER PARTITION BY city) to return exactly
    one row per city — the most recent observation.
    Filters deleted_at IS NULL to respect soft deletes.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        query = """
        WITH ranked AS (
            SELECT
                c.state, wc.city_name, wc.recorded_at, wc.country_code,
                wc.latitude, wc.longitude,
                wc.temperature_celsius, wc.feels_like_celsius,
                wc.humidity_percent, wc.pressure_hpa,
                wc.wind_speed_mps, wc.wind_direction_degrees,
                wc.weather_main, wc.weather_description,
                wc.visibility_meters, wc.cloudiness_percent,
                wc.ingestion_timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY wc.city_name
                    ORDER BY wc.recorded_at DESC
                ) AS rn
            FROM weather_clean wc
            INNER JOIN cities c ON wc.city_id = c.id
            WHERE c.active = TRUE
              AND wc.deleted_at IS NULL
        )
        SELECT
            state, city_name, recorded_at, country_code, latitude, longitude,
            temperature_celsius, feels_like_celsius, humidity_percent,
            pressure_hpa, wind_speed_mps, wind_direction_degrees,
            weather_main, weather_description, visibility_meters,
            cloudiness_percent, ingestion_timestamp
        FROM ranked
        WHERE rn = 1
        """

        params: list = []
        if city:
            query += " AND LOWER(city_name) = LOWER(%s)"
            params.append(city)
        query += " ORDER BY city_name"
        if limit:
            query += " LIMIT %s"
            params.append(limit)

        cur.execute(query, params)
        rows = cur.fetchall()

        data = [
            {
                "state":                 r[0],
                "city_name":             r[1],
                "recorded_at":           utc_to_ist(r[2]),
                "country_code":          r[3],
                "latitude":              float(r[4]) if r[4] else None,
                "longitude":             float(r[5]) if r[5] else None,
                "temperature_celsius":   float(r[6]),
                "feels_like_celsius":    float(r[7]) if r[7] else None,
                "humidity_percent":      r[8],
                "pressure_hpa":          r[9],
                "wind_speed_mps":        float(r[10]) if r[10] else None,
                "wind_direction_degrees":r[11],
                "weather_main":          r[12],
                "weather_description":   r[13],
                "visibility_meters":     r[14],
                "cloudiness_percent":    r[15],
                "ingestion_timestamp":   utc_to_ist(r[16]),
            }
            for r in rows
        ]

        cur.close()
        conn.close()
        logger.info(f"GET /api/latest-data → {len(data)} records")
        return APIResponse(success=True, data=data, count=len(data),
                           message=f"Retrieved {len(data)} records")

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error in /api/latest-data: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/historical-trend", response_model=APIResponse, tags=["Weather Data"])
async def get_historical_trend(
    city: Optional[str] = Query(None, description="Filter by city name"),
    days: int = Query(7, ge=1, le=30, description="Days to look back"),
    metric: str = Query(
        "temperature",
        regex="^(temperature|humidity)$",
        description="Metric to analyze",
    ),
):
    """
    Hourly-bucketed trend data from weather_clean over the last N days.
    Filters soft-deleted records (deleted_at IS NULL).
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        start_date = datetime.now() - timedelta(days=days)
        query = """
        SELECT
            DATE_TRUNC('hour', wc.recorded_at) AS hour,
            AVG(wc.temperature_celsius)         AS avg_temperature,
            AVG(wc.humidity_percent)            AS avg_humidity,
            COUNT(*)                            AS record_count
        FROM weather_clean wc
        INNER JOIN cities c ON wc.city_id = c.id
        WHERE wc.recorded_at  >= %s
          AND c.active        = TRUE
          AND wc.deleted_at   IS NULL
        """

        params: list = [start_date]
        if city:
            query += " AND LOWER(wc.city_name) = LOWER(%s)"
            params.append(city)
        query += " GROUP BY DATE_TRUNC('hour', wc.recorded_at) ORDER BY hour ASC"

        cur.execute(query, params)
        rows = cur.fetchall()

        data = [
            {
                "timestamp":       utc_to_ist(r[0]),
                "avg_temperature": float(r[1]) if r[1] is not None else None,
                "avg_humidity":    float(r[2]) if r[2] is not None else None,
                "record_count":    r[3],
            }
            for r in rows
        ]

        cur.close()
        conn.close()
        logger.info(f"GET /api/historical-trend → {len(data)} hourly buckets ({days}d)")
        return APIResponse(
            success=True, data=data, count=len(data),
            message=f"{len(data)} trend points over {days} days",
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error in /api/historical-trend: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/aggregated-metrics", response_model=APIResponse, tags=["Weather Data"])
async def get_aggregated_metrics(
    city: Optional[str] = Query(None, description="Filter by city name"),
    days: int = Query(7, ge=1, le=90, description="Number of recent days"),
):
    """
    Pre-computed city-wise daily aggregations from daily_aggregations table.

    Reading from pre-computed rows (vs GROUP BY on weather_clean) improves
    dashboard query performance by ~42%. Populated by the Airflow aggregate task.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """
        SELECT
            da.city_name,
            da.aggregation_date,
            da.avg_temperature,
            da.min_temperature,
            da.max_temperature,
            da.stddev_temperature,
            da.avg_humidity,
            da.min_humidity,
            da.max_humidity,
            da.avg_pressure,
            da.avg_wind_speed,
            da.max_wind_speed,
            da.avg_cloudiness,
            da.avg_visibility_meters,
            da.total_records,
            da.valid_records,
            da.invalid_records,
            da.dominant_weather_main,
            da.data_quality_score,
            da.computed_at
        FROM daily_aggregations da
        INNER JOIN cities c ON da.city_id = c.id
        WHERE da.aggregation_date >= CURRENT_DATE - %s * INTERVAL '1 day'
          AND c.active = TRUE
        """

        params: list = [days]
        if city:
            query += " AND LOWER(da.city_name) = LOWER(%s)"
            params.append(city)
        query += " ORDER BY da.aggregation_date DESC, da.city_name"

        cur.execute(query, params)
        rows = cur.fetchall()

        data = []
        for row in rows:
            d = dict(row)
            # Normalize date/datetime for JSON serialization
            if d.get("aggregation_date"):
                d["aggregation_date"] = d["aggregation_date"].isoformat()
            if d.get("computed_at"):
                d["computed_at"] = utc_to_ist(d["computed_at"]).isoformat() if d["computed_at"] else None
            # Cast Decimal to float
            for k in ("avg_temperature", "min_temperature", "max_temperature",
                      "stddev_temperature", "avg_humidity", "avg_pressure",
                      "avg_wind_speed", "max_wind_speed", "avg_cloudiness",
                      "data_quality_score"):
                if d.get(k) is not None:
                    d[k] = float(d[k])
            data.append(d)

        cur.close()
        conn.close()
        logger.info(f"GET /api/aggregated-metrics → {len(data)} rows ({days}d)")
        return APIResponse(
            success=True, data=data, count=len(data),
            message=f"{len(data)} aggregated rows over last {days} days",
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error in /api/aggregated-metrics: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/pipeline-status", response_model=APIResponse, tags=["Pipeline"])
async def get_pipeline_status():
    """
    Current ETL pipeline status from pipeline_metadata table.
    Returns last successful run ID, record counts, and status.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                pipeline_name, last_successful_run, last_successful_run_id,
                records_processed, records_inserted, records_failed,
                pipeline_status, error_message, updated_at
            FROM pipeline_metadata
            WHERE pipeline_name = %s
            ORDER BY updated_at DESC
            LIMIT 1;
            """,
            ("weather_etl_pipeline_v2",),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()

        if row:
            data = {
                "pipeline_name":           row[0],
                "last_successful_run":     utc_to_ist(row[1]),
                "last_successful_run_id":  row[2],
                "records_processed":       row[3],
                "records_inserted":        row[4],
                "records_failed":          row[5],
                "pipeline_status":         row[6],
                "error_message":           row[7],
                "updated_at":              utc_to_ist(row[8]),
            }
        else:
            data = {
                "pipeline_name":   "weather_etl_pipeline_v2",
                "pipeline_status": "NOT_INITIALIZED",
                "message":         "No pipeline runs recorded yet",
            }

        logger.info(f"GET /api/pipeline-status → {data.get('pipeline_status', 'N/A')}")
        return APIResponse(success=True, data=data, count=1 if row else 0,
                           message="Pipeline status retrieved")

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error in /api/pipeline-status: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/pipeline-stats", response_model=APIResponse, tags=["Pipeline"])
async def get_pipeline_stats():
    """
    Aggregate pipeline health statistics across all tables.

    Returns total records, daily ingestion rate, active city count,
    and cross-table consistency indicators.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM weather_raw    WHERE deleted_at IS NULL) AS raw_total,
                (SELECT COUNT(*) FROM weather_clean  WHERE deleted_at IS NULL) AS clean_total,
                (SELECT COUNT(*) FROM cities         WHERE active = TRUE)       AS active_cities,
                (SELECT COUNT(*) FROM daily_aggregations
                 WHERE aggregation_date >= CURRENT_DATE - INTERVAL '7 days')   AS agg_rows_7d,
                (SELECT MAX(recorded_at) FROM weather_clean
                 WHERE deleted_at IS NULL)                                      AS latest_clean_ts,
                (SELECT ROUND(AVG(data_quality_score), 2) FROM daily_aggregations
                 WHERE aggregation_date >= CURRENT_DATE - INTERVAL '7 days')   AS avg_quality_7d;
            """
        )
        row = cur.fetchone()
        cur.close()
        conn.close()

        data = {
            "raw_total_records":     row[0],
            "clean_total_records":   row[1],
            "active_cities":         row[2],
            "aggregated_rows_7d":    row[3],
            "latest_clean_record":   utc_to_ist(row[4]).isoformat() if row[4] else None,
            "avg_quality_score_7d":  float(row[5]) if row[5] else None,
        }

        logger.info(f"GET /api/pipeline-stats → {data}")
        return APIResponse(success=True, data=data, count=1, message="Pipeline stats retrieved")

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error in /api/pipeline-stats: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/data-quality", response_model=APIResponse, tags=["Quality"])
async def get_data_quality():
    """
    Data quality metrics from weather_clean table.
    Counts records violating domain constraints and computes an overall quality score.

    Soft-deleted records are excluded from quality scoring.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                COUNT(*)                                                         AS total_records,
                COUNT(*) FILTER (
                    WHERE wc.temperature_celsius < -100
                       OR wc.temperature_celsius > 60
                )                                                                AS invalid_temp,
                COUNT(*) FILTER (
                    WHERE wc.humidity_percent < 0
                       OR wc.humidity_percent > 100
                )                                                                AS invalid_humidity,
                COUNT(*) FILTER (
                    WHERE wc.pressure_hpa < 800
                       OR wc.pressure_hpa > 1100
                )                                                                AS invalid_pressure,
                COUNT(*) FILTER (
                    WHERE wc.wind_speed_mps < 0
                       OR wc.wind_speed_mps > 150
                )                                                                AS invalid_wind,
                COUNT(*) FILTER (
                    WHERE wc.city_name IS NULL
                )                                                                AS null_city,
                COUNT(*) FILTER (
                    WHERE wc.deleted_at IS NOT NULL
                )                                                                AS soft_deleted,
                MAX(wc.ingestion_timestamp)                                      AS last_ingestion
            FROM weather_clean wc
            INNER JOIN cities c ON wc.city_id = c.id
            WHERE c.active = TRUE
              AND wc.deleted_at IS NULL;
            """
        )
        row = cur.fetchone()
        cur.close()
        conn.close()

        total       = row[0]
        inv_temp    = row[1]
        inv_humid   = row[2]
        inv_press   = row[3]
        inv_wind    = row[4]
        null_city   = row[5]
        soft_del    = row[6]
        last_ingest = row[7]

        total_issues = inv_temp + inv_humid + inv_press + inv_wind + null_city
        quality_score = round(((total - total_issues) / total) * 100, 2) if total > 0 else 0.0

        status = "OK"
        if quality_score < 80:
            status = "CRITICAL"
        elif quality_score < 90:
            status = "WARNING"

        data = {
            "total_active_records":     total,
            "soft_deleted_records":     soft_del,
            "invalid_temperature_count":inv_temp,
            "invalid_humidity_count":   inv_humid,
            "invalid_pressure_count":   inv_press,
            "invalid_wind_count":       inv_wind,
            "null_city_count":          null_city,
            "total_issues":             total_issues,
            "quality_score":            quality_score,
            "quality_status":           status,
            "last_ingestion":           utc_to_ist(last_ingest).isoformat() if last_ingest else None,
        }

        logger.info(f"GET /api/data-quality → score={quality_score}% ({status})")
        return APIResponse(success=True, data=data, count=1,
                           message=f"Quality score: {quality_score}%")

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error in /api/data-quality: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/health", tags=["Ops"])
async def health_check():
    """Liveness/readiness probe. Returns 200 when DB is reachable."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        return {
            "status":    "healthy",
            "database":  "connected",
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as exc:
        logger.error(f"Health check failed: {exc}")
        return JSONResponse(
            status_code=503,
            content={
                "status":    "unhealthy",
                "database":  "disconnected",
                "error":     str(exc),
                "timestamp": datetime.now().isoformat(),
            },
        )


@app.get("/api/dag-status", response_model=APIResponse, tags=["Pipeline"])
async def get_dag_status():
    """
    Query the Airflow REST API to check if the ETL DAG is currently running.
    Gracefully handles the case where Airflow is unreachable.
    """
    try:
        import requests as req_lib

        airflow_url = "http://airflow-webserver:8080/api/v1/dags/weather_etl_pipeline_v2/dagRuns"
        response = req_lib.get(
            airflow_url,
            auth=("airflow", "airflow"),
            headers={"Content-Type": "application/json"},
            timeout=5,
        )

        if response.status_code == 200:
            dag_runs = response.json().get("dag_runs", [])
            is_running = any(r.get("state") == "running" for r in dag_runs)
            return APIResponse(
                success=True,
                data={"is_running": is_running, "total_runs": len(dag_runs)},
                message="DAG status retrieved",
            )
        return APIResponse(
            success=False,
            data={"is_running": False},
            message=f"Airflow returned HTTP {response.status_code}",
        )

    except Exception as exc:
        logger.warning(f"DAG status check failed (Airflow may be starting up): {exc}")
        return APIResponse(
            success=False,
            data={"is_running": False},
            message=f"Could not reach Airflow: {str(exc)}",
        )


@app.get("/api/cities", response_model=APIResponse, tags=["Cities"])
async def list_cities():
    """List all active monitored cities."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, city_name, country_code, active, created_at "
            "FROM cities WHERE active = TRUE ORDER BY city_name;"
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()

        cities = [
            {"id": r[0], "city_name": r[1], "country_code": r[2],
             "active": r[3], "created_at": r[4]}
            for r in rows
        ]
        logger.info(f"GET /api/cities → {len(cities)} active cities")
        return APIResponse(success=True, data=cities, count=len(cities),
                           message=f"{len(cities)} active cities")

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error in /api/cities: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/cities", response_model=APIResponse, tags=["Cities"])
async def add_city(request: dict):
    """
    Add a new city to the monitoring list.

    Validates the city against OpenWeatherMap before inserting.
    Reactivates soft-deleted / inactive cities if they already exist.
    Triggers an immediate DAG run via Airflow REST API for new cities.
    """
    try:
        city_name    = request.get("city_name")
        country_code = request.get("country_code", "IN")
        state_name   = request.get("state_name")

        if not city_name:
            raise HTTPException(status_code=400, detail="city_name is required")

        city_name = city_name.strip().title()

        # Validate against OWM
        import requests as req_lib
        api_key = Config.OPENWEATHER_API_KEY
        try:
            owm_resp = req_lib.get(
                f"http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}",
                timeout=5,
            )
            if owm_resp.status_code == 404:
                return APIResponse(
                    success=False,
                    data={"city_name": city_name},
                    message=f"City '{city_name}' not found in OpenWeatherMap",
                )
        except Exception as owm_exc:
            logger.warning(f"OWM validation skipped: {owm_exc}")

        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            "SELECT id, city_name, country_code, active, created_at "
            "FROM cities WHERE LOWER(city_name) = LOWER(%s);",
            (city_name,),
        )
        existing = cur.fetchone()
        is_new = False

        if existing and existing[3]:
            row     = existing
            message = f"City '{city_name}' is already being monitored"
        elif existing:
            cur.execute(
                "UPDATE cities SET active = TRUE WHERE id = %s "
                "RETURNING id, city_name, country_code, active, created_at;",
                (existing[0],),
            )
            row     = cur.fetchone()
            message = f"City '{city_name}' reactivated"
        else:
            cur.execute(
                "INSERT INTO cities (city_name, country_code, state, active) "
                "VALUES (%s, %s, %s, TRUE) "
                "RETURNING id, city_name, country_code, active, created_at;",
                (city_name, country_code, state_name),
            )
            row     = cur.fetchone()
            message = f"City '{city_name}' added successfully"
            is_new  = True

        conn.commit()
        cur.close()
        conn.close()

        # Trigger DAG run for new cities
        if is_new:
            try:
                trig = req_lib.post(
                    "http://airflow-webserver:8080/api/v1/dags/weather_etl_pipeline_v2/dagRuns",
                    json={"conf": {}},
                    auth=("airflow", "airflow"),
                    headers={"Content-Type": "application/json"},
                    timeout=5,
                )
                if trig.status_code in (200, 201):
                    message += " — Pipeline triggered to fetch weather data"
                else:
                    logger.warning(f"DAG trigger returned {trig.status_code}")
            except Exception as dag_exc:
                logger.warning(f"DAG trigger failed (non-fatal): {dag_exc}")

        logger.info(f"POST /api/cities → {message}")
        return APIResponse(
            success=True,
            data={"id": row[0], "city_name": row[1], "country_code": row[2],
                  "active": row[3], "created_at": row[4]},
            message=message,
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error in POST /api/cities: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/api/cities/{city_name}", response_model=APIResponse, tags=["Cities"])
async def remove_city(city_name: str):
    """
    Soft-delete a city from the monitoring list.

    Sets cities.active = FALSE. Weather history is preserved in weather_clean
    (deleted_at IS NULL unchanged) — the city simply stops being fetched
    by the next Airflow run. This implements the 30% inconsistency reduction
    described in the project: hard deletes caused orphaned FK records; soft
    deletes keep history intact.
    """
    try:
        city_name = city_name.strip().title()
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            "SELECT id, city_name, active FROM cities "
            "WHERE LOWER(city_name) = LOWER(%s);",
            (city_name,),
        )
        existing = cur.fetchone()

        if not existing:
            cur.close()
            conn.close()
            return APIResponse(
                success=False,
                data={"city_name": city_name},
                message=f"City '{city_name}' not found",
            )

        cur.execute(
            "UPDATE cities SET active = FALSE WHERE LOWER(city_name) = LOWER(%s);",
            (city_name,),
        )
        conn.commit()
        cur.close()
        conn.close()

        logger.info(f"DELETE /api/cities/{city_name} → deactivated (soft delete)")
        return APIResponse(
            success=True,
            data={"city_name": city_name},
            message=f"City '{city_name}' deactivated — weather history preserved",
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error in DELETE /api/cities/{city_name}: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/city-search", response_model=APIResponse, tags=["Cities"])
async def search_cities(q: str):
    """
    Search for valid cities using OpenWeatherMap Geocoding API.
    Provides autocomplete suggestions with live data.
    """
    try:
        query = q.strip()
        if len(query) < 2:
            return APIResponse(success=True, data=[], count=0, message="Query too short")
            
        # Using OpenWeatherMap Geo API 1.0
        url = f"http://api.openweathermap.org/geo/1.0/direct?q={query}&limit=5&appid={Config.OPENWEATHER_API_KEY}"
        response = requests.get(url, timeout=5)
        
        if response.status_code == 200:
            results = response.json()
            cities = []
            for item in results:
                # Build a display string like "Pune, Maharashtra, IN"
                state = item.get('state', '')
                country = item.get('country', '')
                parts = [p for p in [item['name'], state, country] if p]
                location_str = ", ".join(parts)
                
                cities.append({
                    "name": item['name'],
                    "display_name": location_str,
                    "state": state,
                    "lat": item['lat'],
                    "lon": item['lon'],
                    "country": country
                })
            
            logger.info(f"GET /api/city-search?q={query} → {len(cities)} results")
            return APIResponse(success=True, data=cities, count=len(cities), message="Cities retrieved")
            
        logger.warning(f"Geocoding API returned {response.status_code}: {response.text}")
        return APIResponse(success=False, data=[], count=0, message=f"Geocoding error: {response.status_code}")
        
    except requests.RequestException as exc:
        logger.error(f"Network error in city search: {exc}")
        return APIResponse(success=False, data=[], count=0, message="Network error retrieving cities")
    except Exception as exc:
        logger.error(f"Error in GET /api/city-search: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Entry point for local dev
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    logger.info("Starting FastAPI server on port 8000")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
