"""
Computes and stores daily weather metrics for cities.
"""

import psycopg2
import psycopg2.extras
from typing import Dict, Any, Optional
from datetime import datetime, date, timedelta

from config.config import Config
from scripts.logger import setup_logger


class WeatherAggregator:
    """
    Computes city-wise daily weather aggregations and stores them in
    the daily_aggregations table.

    Produces metrics used by the FastAPI dashboard endpoints:
        - Temperature (avg / min / max / stddev)
        - Humidity, Pressure, Wind speed
        - Dominant weather condition
        - Data quality score (valid_records / total_records * 100)
    """

    def __init__(self):
        self.logger = setup_logger(__name__)
        self.connection: Optional[psycopg2.extensions.connection] = None
        self.cursor: Optional[psycopg2.extensions.cursor] = None

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def connect(self) -> bool:
        """Establish a connection to PostgreSQL."""
        try:
            self.connection = psycopg2.connect(
                host=Config.POSTGRES_HOST,
                port=Config.POSTGRES_PORT,
                database=Config.POSTGRES_DB,
                user=Config.POSTGRES_USER,
                password=Config.POSTGRES_PASSWORD,
            )
            self.cursor = self.connection.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            self.logger.info("WeatherAggregator: connected to PostgreSQL")
            return True
        except psycopg2.OperationalError as exc:
            self.logger.error(f"WeatherAggregator: DB connection failed — {exc}")
            return False

    def disconnect(self) -> None:
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        self.logger.info("WeatherAggregator: DB connection closed")

    # ------------------------------------------------------------------
    # Core aggregation logic
    # ------------------------------------------------------------------

    def aggregate_for_date(self, target_date: date) -> Dict[str, Any]:
        """
        Compute and persist city-wise daily aggregations for a specific date.

        Args:
            target_date: The calendar date to aggregate (default: yesterday for
                         complete-day data).

        Returns:
            Dictionary with aggregation run statistics.
        """
        self.logger.info(f"Starting aggregation for date: {target_date.isoformat()}")

        upsert_query = """
        INSERT INTO daily_aggregations (
            city_id, city_name, aggregation_date,
            avg_temperature, min_temperature, max_temperature, stddev_temperature,
            avg_humidity, min_humidity, max_humidity,
            avg_pressure, min_pressure, max_pressure,
            avg_wind_speed, max_wind_speed,
            avg_cloudiness, avg_visibility_meters,
            total_records, valid_records, invalid_records,
            dominant_weather_main, data_quality_score,
            computed_at
        )
        SELECT
            c.id                                                   AS city_id,
            wc.city_name,
            DATE(wc.recorded_at)                                   AS aggregation_date,

            -- Temperature
            ROUND(AVG(wc.temperature_celsius)::NUMERIC, 2)        AS avg_temperature,
            MIN(wc.temperature_celsius)                            AS min_temperature,
            MAX(wc.temperature_celsius)                            AS max_temperature,
            ROUND(STDDEV(wc.temperature_celsius)::NUMERIC, 3)     AS stddev_temperature,

            -- Humidity
            ROUND(AVG(wc.humidity_percent)::NUMERIC, 2)           AS avg_humidity,
            MIN(wc.humidity_percent)                               AS min_humidity,
            MAX(wc.humidity_percent)                               AS max_humidity,

            -- Pressure
            ROUND(AVG(wc.pressure_hpa)::NUMERIC, 2)               AS avg_pressure,
            MIN(wc.pressure_hpa)                                   AS min_pressure,
            MAX(wc.pressure_hpa)                                   AS max_pressure,

            -- Wind
            ROUND(AVG(wc.wind_speed_mps)::NUMERIC, 2)             AS avg_wind_speed,
            MAX(wc.wind_speed_mps)                                 AS max_wind_speed,

            -- Cloudiness & Visibility
            ROUND(AVG(wc.cloudiness_percent)::NUMERIC, 2)         AS avg_cloudiness,
            ROUND(AVG(wc.visibility_meters))::INTEGER              AS avg_visibility_meters,

            -- Record counts
            COUNT(*)                                               AS total_records,
            COUNT(*) FILTER (
                WHERE wc.temperature_celsius BETWEEN -100 AND 60
                  AND wc.humidity_percent    BETWEEN 0 AND 100
                AND wc.pressure_hpa        BETWEEN 800 AND 1100
            )                                                      AS valid_records,
            COUNT(*) FILTER (
                WHERE wc.temperature_celsius NOT BETWEEN -100 AND 60
                   OR wc.humidity_percent    NOT BETWEEN 0 AND 100
                   OR wc.pressure_hpa        NOT BETWEEN 800 AND 1100
            )                                                      AS invalid_records,

            -- Dominant weather condition (mode)
            (
                SELECT sub.weather_main
                FROM   weather_clean sub
                WHERE  sub.city_name   = wc.city_name
                  AND  DATE(sub.recorded_at) = %s
                  AND  sub.deleted_at IS NULL
                GROUP  BY sub.weather_main
                ORDER  BY COUNT(*) DESC
                LIMIT  1
            )                                                      AS dominant_weather_main,

            -- Data quality score (0-100)
            ROUND(
                (COUNT(*) FILTER (
                    WHERE wc.temperature_celsius BETWEEN -100 AND 60
                      AND wc.humidity_percent    BETWEEN 0 AND 100
                      AND wc.pressure_hpa        BETWEEN 800 AND 1100
                )::DECIMAL / NULLIF(COUNT(*), 0)) * 100,
                2
            )                                                      AS data_quality_score,

            CURRENT_TIMESTAMP                                      AS computed_at

        FROM weather_clean wc
        INNER JOIN cities c
            ON  LOWER(c.city_name) = LOWER(wc.city_name)
            AND c.active = TRUE
        WHERE DATE(wc.recorded_at) = %s
          AND wc.deleted_at IS NULL
        GROUP BY c.id, wc.city_name, DATE(wc.recorded_at)

        ON CONFLICT (city_id, aggregation_date)
        DO UPDATE SET
            avg_temperature       = EXCLUDED.avg_temperature,
            min_temperature       = EXCLUDED.min_temperature,
            max_temperature       = EXCLUDED.max_temperature,
            stddev_temperature    = EXCLUDED.stddev_temperature,
            avg_humidity          = EXCLUDED.avg_humidity,
            min_humidity          = EXCLUDED.min_humidity,
            max_humidity          = EXCLUDED.max_humidity,
            avg_pressure          = EXCLUDED.avg_pressure,
            min_pressure          = EXCLUDED.min_pressure,
            max_pressure          = EXCLUDED.max_pressure,
            avg_wind_speed        = EXCLUDED.avg_wind_speed,
            max_wind_speed        = EXCLUDED.max_wind_speed,
            avg_cloudiness        = EXCLUDED.avg_cloudiness,
            avg_visibility_meters = EXCLUDED.avg_visibility_meters,
            total_records         = EXCLUDED.total_records,
            valid_records         = EXCLUDED.valid_records,
            invalid_records       = EXCLUDED.invalid_records,
            dominant_weather_main = EXCLUDED.dominant_weather_main,
            data_quality_score    = EXCLUDED.data_quality_score,
            computed_at           = EXCLUDED.computed_at;
        """

        try:
            self.cursor.execute(upsert_query, (target_date, target_date))
            rows_affected = self.cursor.rowcount
            self.connection.commit()

            self.logger.info(
                f"✓ Aggregation complete for {target_date.isoformat()}: "
                f"{rows_affected} city-day rows upserted"
            )
            return {
                "aggregation_date": target_date.isoformat(),
                "rows_upserted": rows_affected,
                "status": "SUCCESS",
            }

        except Exception as exc:
            self.connection.rollback()
            self.logger.error(f"✗ Aggregation failed for {target_date}: {exc}", exc_info=True)
            raise

    def run_daily_aggregation(self, days_back: int = 1) -> Dict[str, Any]:
        """
        Entry point called by the Airflow DAG task.
        Aggregates the last N calendar days (default: yesterday for full-day coverage).

        Args:
            days_back: How many days back to aggregate.

        Returns:
            Summary statistics dict.
        """
        if not self.connect():
            raise RuntimeError("WeatherAggregator: Cannot connect to database")

        try:
            total_upserted = 0
            results = []

            for offset in range(days_back, 0, -1):
                target_date = (datetime.now() - timedelta(days=offset)).date()
                result = self.aggregate_for_date(target_date)
                total_upserted += result["rows_upserted"]
                results.append(result)

            summary = {
                "days_aggregated": days_back,
                "total_city_day_rows_upserted": total_upserted,
                "details": results,
                "status": "SUCCESS",
            }

            self.logger.info(
                f"Daily aggregation run complete: {days_back} day(s), "
                f"{total_upserted} city-day rows upserted"
            )
            return summary

        finally:
            self.disconnect()

    def get_city_daily_stats(self, city_name: str, days: int = 7) -> list:
        """
        Retrieve pre-aggregated stats for a specific city over the last N days.
        Used by FastAPI /api/aggregated-metrics endpoint.

        Args:
            city_name: City name to query.
            days: Number of recent days to return.

        Returns:
            List of daily stat dicts.
        """
        if not self.connect():
            return []

        try:
            query = """
            SELECT
                aggregation_date, city_name,
                avg_temperature, min_temperature, max_temperature,
                avg_humidity, avg_pressure,
                avg_wind_speed, max_wind_speed,
                avg_cloudiness, avg_visibility_meters,
                total_records, valid_records, invalid_records,
                dominant_weather_main, data_quality_score,
                computed_at
            FROM daily_aggregations
            WHERE LOWER(city_name) = LOWER(%s)
              AND aggregation_date >= CURRENT_DATE - %s * INTERVAL '1 day'
            ORDER BY aggregation_date DESC;
            """
            self.cursor.execute(query, (city_name, days))
            rows = self.cursor.fetchall()

            # RealDictCursor returns dict-like rows; convert dates to strings
            results = []
            for row in rows:
                row_dict = dict(row)
                if isinstance(row_dict.get("aggregation_date"), date):
                    row_dict["aggregation_date"] = row_dict["aggregation_date"].isoformat()
                if isinstance(row_dict.get("computed_at"), datetime):
                    row_dict["computed_at"] = row_dict["computed_at"].isoformat()
                results.append(row_dict)

            self.logger.info(
                f"Retrieved {len(results)} aggregated rows for {city_name} (last {days} days)"
            )
            return results

        except Exception as exc:
            self.logger.error(f"Error fetching city daily stats: {exc}")
            return []
        finally:
            self.disconnect()


def main():
    """Standalone execution entry point for manual testing."""
    aggregator = WeatherAggregator()
    summary = aggregator.run_daily_aggregation(days_back=7)
    print(f"Aggregation summary: {summary}")
    return summary


if __name__ == "__main__":
    main()
