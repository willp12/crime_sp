# Task image for the crime_sp Airflow DAG (dags/crime_sp.py).
# Contains ONLY the pipeline dependencies — the Streamlit app (app/, src/)
# is deployed separately on Streamlit Cloud and is not part of this image.
# No ENTRYPOINT/CMD: the DockerOperator passes the full command for each task.
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Dependency layer first — editing pipeline code does not bust this cache.
# Exact pins live in requirements-pipeline.txt (pipeline + health-check deps
# only; the Streamlit app installs from requirements.txt separately).
COPY requirements-pipeline.txt .
RUN pip install -r requirements-pipeline.txt

# Bake the duckdb httpfs extension in at build time so the DAG's health_check
# only needs `LOAD httpfs` — no dependency on extensions.duckdb.org at run time.
RUN python -c "import duckdb; duckdb.connect().execute('INSTALL httpfs')"

COPY pipeline/ ./pipeline/
