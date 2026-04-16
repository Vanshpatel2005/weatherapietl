-- ============================================
-- Weather Raw Table Schema
-- ============================================
-- Purpose: Store raw JSON responses from OpenWeatherMap API
-- Why: Preserve original data for full audit trail, debugging, and reprocessing
-- Design: JSONB for flexible storage; city_id FK enforces referential integrity;
--         soft deletes via deleted_at preserve history for 10K+ daily records
-- ============================================

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

-- Standard B-tree indexes for equality and range lookups
CREATE INDEX IF NOT EXISTS idx_raw_city_name    ON weather_raw(city_name);
CREATE INDEX IF NOT EXISTS idx_raw_city_id      ON weather_raw(city_id);
CREATE INDEX IF NOT EXISTS idx_raw_processed    ON weather_raw(processed) WHERE processed = FALSE;

-- BRIN index for time-series range scans — ideal for high-volume append-only ingestion
CREATE INDEX IF NOT EXISTS idx_raw_extraction_brin ON weather_raw USING BRIN (extraction_timestamp);

-- GIN index for efficient JSONB field queries
CREATE INDEX IF NOT EXISTS idx_raw_data_gin ON weather_raw USING GIN (raw_data);

-- Partial index: only active (non-deleted) rows — cuts index size significantly
CREATE INDEX IF NOT EXISTS idx_raw_active
    ON weather_raw(city_name, extraction_timestamp)
    WHERE deleted_at IS NULL;

-- Comments for documentation
COMMENT ON TABLE weather_raw IS 'Stores raw API JSON responses from OpenWeatherMap for audit trail and reprocessing. Supports 10K+ daily records.';
COMMENT ON COLUMN weather_raw.city_id IS 'FK to cities table — enforces referential integrity';
COMMENT ON COLUMN weather_raw.raw_data IS 'Full JSON response from API (JSONB type for efficient querying)';
COMMENT ON COLUMN weather_raw.processed IS 'Flag indicating if this raw record has been transformed into clean table';
COMMENT ON COLUMN weather_raw.deleted_at IS 'Soft delete timestamp. NULL = active record. Non-null = logically deleted, preserved for audit.';
