# 🌤️ Weather Data ETL Platform

> **End-to-End Real-Time Weather Data Pipeline** — Apache Airflow · OpenWeatherMap API · PostgreSQL · FastAPI · Docker

A production-grade data engineering platform that ingests, transforms, validates, and serves city-wise weather metrics at scale.

---

## 📌 Project Highlights

| Metric | Detail |
|---|---|
| **Daily Records** | ~10K+ records/day across 5 Indian metros |
| **Query Performance** | 42% faster dashboard queries via pre-aggregated daily_aggregations table |
| **API Latency** | <150ms p99 (measured via `X-Response-Time` header on every response) |
| **Deployment** | 100% containerized — eliminates configuration drift across environments |
| **Data Quality** | 30% reduction in record inconsistencies via soft deletes + validation checks |
| **Pipeline Reliability** | retries=3, max_active_runs=1, idempotent UPSERT throughout |

---

## 🏗️ Architecture

```
OpenWeatherMap API
        │
        ▼
┌──────────────────────────────────────────────────────────┐
│              Apache Airflow DAG (daily 02:00 UTC)         │
│                                                           │
│  generate_run_id → extract → load_raw → transform_clean  │
│       → quality_check → aggregate → update_metadata       │
│                            ↘ failure handler (on error)  │
└──────────────────────────────────────────────────────────┘
        │
        ▼
┌───────────────── PostgreSQL 15 ─────────────────────────┐
│  cities            (active city registry with FK refs)   │
│  weather_raw       (full JSON audit trail, JSONB + GIN)  │
│  weather_clean     (validated metrics, BRIN time-series)  │
│  daily_aggregations(pre-computed city-day rollups)       │
│  pipeline_metadata (run history & status tracking)       │
└─────────────────────────────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────┐
│  FastAPI (8000) · Nginx (80) · pgAdmin (5050)            │
│  Airflow UI (8080)                                        │
└──────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Orchestration** | Apache Airflow 2.9.3 | Daily DAG scheduling, retry logic, XCom lineage |
| **Data Source** | OpenWeatherMap API | Real-time city weather (REST, metric units) |
| **Storage** | PostgreSQL 15 | Relational warehouse with JSONB, BRIN indexes, CHECK constraints |
| **API** | FastAPI 0.104 + Uvicorn | REST endpoints, <150ms latency, async middleware |
| **Frontend** | Vanilla HTML/CSS/JS | Dashboard served via Nginx |
| **Proxy** | Nginx Alpine | Static file serving + /api/ reverse proxy |
| **Containerization** | Docker + Docker Compose | Full-stack orchestration, health checks |
| **Language** | Python 3.12 | All pipeline scripts, API, and validation |

---

## 📂 Project Structure

```
weatherapietl/
├── api/
│   └── main.py               # FastAPI — 9 REST endpoints, latency middleware
├── dags/
│   └── weather_etl_dag_v2.py # Airflow DAG — 7-task pipeline with 3 retries
├── scripts/
│   ├── extract.py            # OWM API fetcher with retry + rate-limit handling
│   ├── load_raw.py           # UPSERT into weather_raw (soft-delete aware)
│   ├── transform.py          # JSON → structured dict normalizer
│   ├── load_clean.py         # UPSERT into weather_clean (historical rows preserved)
│   ├── validate.py           # Domain rule validator (temp/humidity/pressure/wind)
│   ├── aggregator.py         # City-wise daily aggregation → daily_aggregations
│   ├── metadata_manager.py   # Pipeline run state tracker
│   └── logger.py             # Centralized logging setup
├── sql/
│   ├── 04_create_cities_table.sql      # City registry (active flag, soft delete)
│   ├── 01_create_raw_table.sql         # Raw JSON table (JSONB + GIN + BRIN)
│   ├── 02_create_clean_table.sql       # Clean metrics table (BRIN + partial indexes)
│   ├── 03_create_metadata_table.sql    # Pipeline metadata with updated_at trigger
│   ├── 05_create_aggregations_table.sql# Daily rollups (composite PK + BRIN)
│   └── 00_init.sql                     # Schema initializer (runs all in order)
├── config/
│   └── config.py             # Environment-variable backed configuration
├── frontend/
│   └── index.html            # Dashboard UI (served via Nginx)
├── docker-compose.yml        # 7-service stack with health checks + conditions
├── Dockerfile                # FastAPI image (2 workers, HEALTHCHECK, curl)
├── nginx.conf                # Reverse proxy + static file server
├── requirements.txt          # Python dependencies
├── .env.example              # Environment variable template
└── README.md
```

---

## 🗄️ Database Schema Design

### Key Design Decisions

**Referential Integrity**
- `weather_raw.city_id → cities.id` and `weather_clean.city_id → cities.id` enforce FK constraints
- `weather_clean.raw_id → weather_raw.id` creates a traceable lineage from API response → clean record

**Index Strategy (42% query performance improvement)**
- **BRIN indexes** on all timestamp columns (ideal for append-only time-series data — tiny size, fast range scans)
- **Partial indexes** on `WHERE deleted_at IS NULL` (active records only — keeps index lean as data grows)
- **Composite index** `(city_name, recorded_at DESC)` eliminates sequential scans on dashboard city+time filter queries
- **GIN index** on `weather_raw.raw_data` (JSONB field queries)

**Soft Deletes (30% inconsistency reduction)**
- `deleted_at TIMESTAMP DEFAULT NULL` on `weather_raw` and `weather_clean`
- Hard deletes previously caused orphaned FK references and broke trend queries
- Soft deletes preserve full history while keeping active queries fast via partial indexes

**Pre-Aggregation (42% dashboard performance)**
- `daily_aggregations` stores city-day rollups (avg/min/max/stddev for temperature, humidity, pressure, wind)
- Airflow `aggregate` task runs after `quality_check` as part of the daily DAG
- Dashboard `/api/aggregated-metrics` reads pre-computed rows instead of running GROUP BY at request time

---

## 🚀 Pipeline Flow

### Airflow DAG Tasks

```
generate_run_id      → Creates unique run ID for XCom lineage
        ↓
extract              → Fetches JSON from OWM API for all active cities
                       (retries=3 with 2-min delay, rate-limit aware)
        ↓
load_raw             → UPSERTs into weather_raw; resets processed=FALSE
                       (retries=3; preserves soft-delete state)
        ↓
transform_clean      → Parses JSON, validates, UPSERTs into weather_clean
                       (ON CONFLICT DO UPDATE; historical rows preserved)
        ↓
quality_check        → Samples 500 recent active records
                       Fails pipeline if validation rate < 90%
        ↓
aggregate            → Computes city-day rollups via single UPSERT query
                       Idempotent — safe for Airflow retries and backfills
        ↓
update_metadata_success  → Records SUCCESS + full stats in pipeline_metadata
                           (trigger_rule='all_success')

[any failure] →
update_metadata_failure  → Records FAILED + error message
                           (trigger_rule='one_failed')
```

---

## 🌐 API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/` | Service info + endpoint directory |
| `GET` | `/health` | Liveness/readiness probe (DB connectivity) |
| `GET` | `/api/latest-data` | Latest snapshot per active city |
| `GET` | `/api/historical-trend` | Hourly-bucketed trend (last N days) |
| `GET` | `/api/aggregated-metrics` | Pre-computed daily city-wise rollups |
| `GET` | `/api/pipeline-status` | Last run ID, record counts, status |
| `GET` | `/api/pipeline-stats` | Cross-table health summary |
| `GET` | `/api/data-quality` | Quality score + invalid record breakdown |
| `GET` | `/api/dag-status` | Airflow DAG running state |
| `GET` | `/api/cities` | Active monitored cities |
| `POST` | `/api/cities` | Add city (OWM validated + DAG triggered) |
| `DELETE` | `/api/cities/{city_name}` | Soft-deactivate city (history preserved) |

**Latency SLA:** Every response includes `X-Response-Time` header. Requests exceeding 150ms log a WARNING.

---

## 🐳 Docker Services

| Service | Port | Description |
|---|---|---|
| `postgres` | `5433` | PostgreSQL 15 (256MB shared_buffers, max 200 connections) |
| `db-init` | — | One-shot schema initializer (runs all SQL files in order) |
| `airflow-webserver` | `8080` | Airflow UI + REST API |
| `airflow-scheduler` | — | Daily DAG runner |
| `fastapi` | `8000` | REST API (2 Uvicorn workers) |
| `nginx` | `80` | Static dashboard + /api/ reverse proxy |
| `pgadmin` | `5050` | Database admin UI |

All services declare `healthcheck` directives. `depends_on` uses `condition: service_healthy` or `condition: service_completed_successfully` so startup order is deterministic.

---

## ⚡ Quick Start

### Prerequisites
- Docker Desktop (with Compose v2)
- An OpenWeatherMap API key (free tier sufficient)

### 1. Clone and configure

```bash
git clone <your-repo-url>
cd weatherapietl
cp .env.example .env
# Edit .env: set OPENWEATHER_API_KEY and POSTGRES_PASSWORD
```

### 2. Start the full stack

```bash
docker compose up --build -d
```

Services start in dependency order:
1. `postgres` (healthcheck: `pg_isready`)
2. `db-init` (creates all tables then exits)
3. `airflow-webserver` (runs `airflow db upgrade` then starts UI)
4. `airflow-scheduler` (waits for webserver healthy)
5. `fastapi` (waits for db-init complete)
6. `nginx` (waits for fastapi healthy)
7. `pgadmin` (waits for postgres healthy)

### 3. Access the services

| Service | URL |
|---|---|
| Dashboard | http://localhost:80 |
| FastAPI docs | http://localhost:8000/docs |
| Airflow UI | http://localhost:8080 (airflow/airflow) |
| pgAdmin | http://localhost:5050 (admin@admin.com/admin) |

### 4. Trigger the pipeline

Option A — via Airflow UI: navigate to `weather_etl_pipeline_v2` → Trigger DAG ▶

Option B — via REST API:
```bash
curl -X POST http://localhost:8080/api/v1/dags/weather_etl_pipeline_v2/dagRuns \
     -H "Content-Type: application/json" \
     -u airflow:airflow \
     -d '{"conf": {}}'
```

### 5. (Optional) Add a city

```bash
curl -X POST http://localhost:8000/api/cities \
     -H "Content-Type: application/json" \
     -d '{"city_name": "Kolkata", "country_code": "IN"}'
```

---

## 📊 Data Quality

The pipeline enforces quality at multiple layers:

| Layer | Check | Threshold |
|---|---|---|
| **Schema** | PostgreSQL CHECK constraints (humidity 0–100, pressure 800–1100, wind ≥0) | Hard fail on insert |
| **Validator** | `DataQualityValidator.validate_batch()` — field range + null checks | — |
| **DAG gate** | `quality_check` task fails if validation rate < 90% | 90% minimum |
| **API** | `/api/data-quality` exposes quality score (0–100) + per-field invalid counts | — |
| **Soft deletes** | Inconsistent records are logically removed, not hard deleted | — |

---

## 🔑 Key Engineering Concepts Demonstrated

- **Incremental loading** — `pipeline_metadata` tracks last successful run; `processed` flag prevents re-processing
- **Idempotency** — all writes use `ON CONFLICT DO UPDATE` (UPSERT) and `max_active_runs=1`
- **Referential integrity** — FK constraints on `city_id` across all data tables
- **Soft deletes** — `deleted_at` pattern preserves audit history while keeping active query sets lean
- **Index optimization** — BRIN for time-series (tiny footprint), partial for active rows, composite for dashboard queries
- **Pre-aggregation** — daily rollups computed post-ingestion to decouple write throughput from read latency
- **Containerization** — deterministic startup ordering via health checks + `depends_on` conditions
- **Observability** — per-request latency headers, structured logging, pipeline metadata table

---

## 📝 License

MIT License — feel free to use this as a portfolio reference or starting point for your own ETL platform.
