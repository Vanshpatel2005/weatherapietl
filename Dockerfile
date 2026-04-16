FROM python:3.12-slim

LABEL maintainer="Weather ETL Platform"
LABEL description="FastAPI service delivering weather dashboard data with <150ms latency"

WORKDIR /app

# Install system dependencies
# curl is needed for Docker healthcheck; gcc for psycopg2-binary compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies (cached layer — only invalidated when requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY . .

# Create logs directory
RUN mkdir -p logs

# Expose FastAPI port
EXPOSE 8000

# Health check — validates DB connectivity via /health endpoint
HEALTHCHECK --interval=20s --timeout=5s --start-period=15s --retries=5 \
    CMD curl --fail http://localhost:8000/health || exit 1

# Run with 2 workers and keep-alive for low-latency dashboard responses
CMD ["uvicorn", "api.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "2", \
     "--timeout-keep-alive", "30", \
     "--log-level", "info"]
