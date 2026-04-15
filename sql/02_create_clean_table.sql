-- ============================================
-- Weather Clean Table Schema
-- ============================================
-- Purpose: Store structured, validated weather data for analytics and API consumption
-- Why: Separates concerns - raw for audit, clean for production use
-- Design: Composite primary key (city_name, recorded_at) ensures no duplicates
-- ============================================

CREATE TABLE IF NOT EXISTS weather_clean (
    city_name VARCHAR(100) NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    country_code VARCHAR(10),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    temperature_celsius DECIMAL(5, 2) NOT NULL,
    feels_like_celsius DECIMAL(5, 2),
    humidity_percent INTEGER CHECK (humidity_percent >= 0 AND humidity_percent <= 100),
    pressure_hpa INTEGER CHECK (pressure_hpa >= 800 AND pressure_hpa <= 1100),
    wind_speed_mps DECIMAL(5, 2) CHECK (wind_speed_mps >= 0),
    wind_direction_degrees INTEGER CHECK (wind_direction_degrees >= 0 AND wind_direction_degrees <= 360),
    weather_main VARCHAR(50),
    weather_description VARCHAR(200),
    visibility_meters INTEGER,
    cloudiness_percent INTEGER CHECK (cloudiness_percent >= 0 AND cloudiness_percent <= 100),
    ingestion_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    raw_id INTEGER REFERENCES weather_raw(id) ON DELETE SET NULL,
    PRIMARY KEY (city_name, recorded_at)
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_clean_city_name ON weather_clean(city_name);
CREATE INDEX IF NOT EXISTS idx_clean_recorded_at ON weather_clean(recorded_at);
CREATE INDEX IF NOT EXISTS idx_clean_ingestion_timestamp ON weather_clean(ingestion_timestamp);
CREATE INDEX IF NOT EXISTS idx_clean_raw_id ON weather_clean(raw_id);

-- Comment for documentation
COMMENT ON TABLE weather_clean IS 'Structured and validated weather data for analytics and API consumption';
COMMENT ON COLUMN weather_clean.recorded_at IS 'Weather observation timestamp from API';
COMMENT ON COLUMN weather_clean.raw_id IS 'Reference to raw table for audit trail';
COMMENT ON CONSTRAINT weather_clean_pkey ON weather_clean IS 'Composite PK prevents duplicate records for same city at same timestamp';
