-- ============================================
-- Weather Raw Table Schema
-- ============================================
-- Purpose: Store raw JSON responses from OpenWeatherMap API
-- Why: Preserve original data for audit trail, debugging, and reprocessing
-- Design: JSONB type for flexible schema storage
-- ============================================

CREATE TABLE IF NOT EXISTS weather_raw (
    id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    raw_data JSONB NOT NULL,
    api_response_code INTEGER,
    extraction_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ingestion_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE,
    UNIQUE(city_name, extraction_timestamp)
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_raw_city_name ON weather_raw(city_name);
CREATE INDEX IF NOT EXISTS idx_raw_extraction_timestamp ON weather_raw(extraction_timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_processed ON weather_raw(processed);
CREATE INDEX IF NOT EXISTS idx_raw_data_gin ON weather_raw USING GIN (raw_data);

-- Comment for documentation
COMMENT ON TABLE weather_raw IS 'Stores raw API JSON responses from OpenWeatherMap for audit trail and reprocessing capability';
COMMENT ON COLUMN weather_raw.raw_data IS 'Full JSON response from API (JSONB type for efficient querying)';
COMMENT ON COLUMN weather_raw.processed IS 'Flag indicating if this raw record has been transformed into clean table';
