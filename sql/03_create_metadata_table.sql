-- ============================================
-- Pipeline Metadata Table Schema
-- ============================================
-- Purpose: Track pipeline execution state for incremental loading
-- Why: Enables incremental processing by tracking last successful run
-- Design: Single row table that gets updated on each successful pipeline run
-- ============================================

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

-- Index for quick lookup
CREATE INDEX IF NOT EXISTS idx_metadata_pipeline_name ON pipeline_metadata(pipeline_name);

-- Trigger to auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_pipeline_metadata_updated_at
    BEFORE UPDATE ON pipeline_metadata
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Comment for documentation
COMMENT ON TABLE pipeline_metadata IS 'Tracks pipeline execution state for incremental loading and monitoring';
COMMENT ON COLUMN pipeline_metadata.last_successful_run IS 'Timestamp of last successful pipeline completion';
COMMENT ON COLUMN pipeline_metadata.records_processed IS 'Total records processed in last run';
COMMENT ON COLUMN pipeline_metadata.pipeline_status IS 'Current status: IDLE, RUNNING, SUCCESS, FAILED';
