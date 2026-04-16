-- ============================================
-- Weather ETL Pipeline - Database Initializer
-- ============================================
-- Runs all schema creation scripts in the correct order.
-- Execute once to bootstrap the weather_warehouse database.
-- ============================================

\echo '>>> [1/5] Creating cities table...'
\i /docker-entrypoint-initdb.d/04_create_cities_table.sql

\echo '>>> [2/5] Creating weather_raw table...'
\i /docker-entrypoint-initdb.d/01_create_raw_table.sql

\echo '>>> [3/5] Creating weather_clean table...'
\i /docker-entrypoint-initdb.d/02_create_clean_table.sql

\echo '>>> [4/5] Creating pipeline_metadata table...'
\i /docker-entrypoint-initdb.d/03_create_metadata_table.sql

\echo '>>> [5/5] Creating daily_aggregations table...'
\i /docker-entrypoint-initdb.d/05_create_aggregations_table.sql

\echo '✓ Database schema initialized successfully'
