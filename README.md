# crime_sp
Interactive map &amp; dashboard of cellphone-related crimes in São Paulo, Brazil. Automated pipeline (GitHub Actions → S3 → DuckDB) from SSP-SP open data. | Mapa interativo e dashboard de crimes contra celulares em SP.

## Orchestration

The pipeline now also runs on a self-hosted [Airflow platform](https://github.com/willp12/airflow): DAG `crime_sp_biweekly` (`dags/crime_sp.py`), with each task executed as a container built from this repo's `Dockerfile` (image `pipelines/crime_sp` in Artifact Registry). `.github/workflows/deploy.yml` publishes the image and syncs `dags/` to the platform's DAGs bucket on every push to `main`.

During the parallel-run period the GitHub Actions cron in `.github/workflows/pipeline.yml` stays active — both schedulers run the same `0 8 5,20 * *` schedule until the Airflow runs are verified, after which the GH `schedule:` will be removed (`workflow_dispatch` remains as a manual fallback).

Secrets: Airflow reads AWS credentials from GCP Secret Manager (`airflow-variables-aws_*`) — nothing is read from this repo. `.streamlit/secrets.toml` is gitignored and has never been tracked; see `.streamlit/secrets.toml.example` for the expected structure.
