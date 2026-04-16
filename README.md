# Weather ETL Dashboard

A simple ETL pipeline for fetching weather data from OpenWeatherMap, storing it in PostgreSQL, and displaying it on a local web dashboard.

## Overview

This project automatically pulls weather data for specific cities on a daily basis. The data is processed through Apache Airflow, validated, aggregated, and made available via a FastAPI backend. A static HTML frontend serves the data visually.

## Tech Stack

- **Orchestration**: Apache Airflow
- **Database**: PostgreSQL
- **Backend API**: FastAPI (Python)
- **Frontend**: HTML / CSS / JS / Nginx
- **Deployment**: Docker Compose

## Repository Structure

- `api/` - FastAPI backend to serve data to the dashboard
- `dags/` - Airflow DAG definitions
- `scripts/` - Python scripts for the extraction, transformation, and validation steps
- `sql/` - SQL schema definitions for the database
- `frontend/` - Static dashboard files served by Nginx

## How to Run

1. Make sure you have Docker and Docker Compose installed.
2. Clone this repository.
3. Copy `.env.example` to `.env` and configure your API keys (you need an OpenWeatherMap API key).
   ```bash
   cp .env.example .env
   ```
4. Start the stack:
   ```bash
   docker compose up -d --build
   ```

## Services

Once the containers are running, you can access:

- **Dashboard**: `http://localhost`
- **FastAPI Docs**: `http://localhost:8000/docs`
- **Airflow UI**: `http://localhost:8080` (login: `airflow` / `airflow`)
- **pgAdmin**: `http://localhost:5050` (login: `admin@admin.com` / `admin`)

## Pipeline Features

- **Daily extraction:** Fetches weather updates every day for active cities.
- **Historical Data:** Soft-deletes are used so historical data remains intact.
- **Pre-aggregations:** A scheduled task aggregates daily metrics so the dashboard remains fast.
