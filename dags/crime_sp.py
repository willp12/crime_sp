"""crime_sp biweekly pipeline — SSP-SP cellphone-theft data to S3.

Source repo: https://github.com/willp12/crime_sp — this file is rsynced to the
platform DAGs bucket (gs://<project>-airflow-dags/dags/crime_sp/) and the task
image is built from the same repo's Dockerfile by .github/workflows/deploy.yml.

PARALLEL-RUN STATUS: the original GitHub Actions cron
(.github/workflows/pipeline.yml, same "0 8 5,20 * *" schedule) is still active.
Both schedulers run side by side until cutover; only after scheduled Airflow
runs are verified green is the GH Actions `schedule:` removed
(workflow_dispatch stays as a manual fallback).

The five task containers hand files to each other through the docker named
volume "crime-sp-work" mounted at /work (replacing the GH runner's shared
/tmp). Only upload and health_check receive AWS credentials.

Year selection mirrors pipeline.yml's `date +%Y`: params.ano overrides when
set; otherwise the year at the moment the run executes (dag_run.run_after) is
used. run_after — not logical_date — because in Airflow 3 logical_date is None
on manual triggers (AIP-83) and, on scheduled runs, points at the START of the
data interval (the Jan 5 run would render the previous year).
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG
from docker.types import Mount

PROJECT_ID = os.environ["GCP_PROJECT_ID"]  # injected into the scheduler by the stack
IMAGE = f"us-central1-docker.pkg.dev/{PROJECT_ID}/pipelines/crime_sp:latest"

S3_BUCKET = "crime-data-sp"

# params.ano ("" by default) wins; empty string is falsy -> year of run_after.
# run_after is always populated (scheduled AND manual runs), unlike
# logical_date, which is None on Airflow 3 manual triggers and lags one
# interval behind on scheduled ones.
ANO = "{{ params.ano or dag_run.run_after.strftime('%Y') }}"

# Shared scratch space between task containers.
WORK = Mount(source="crime-sp-work", target="/work", type="volume")

# Resolved at runtime from GCP Secret Manager (airflow-variables-<name>) via the
# Airflow secrets backend — only upload/health_check ever see these.
AWS_ENV = {
    "AWS_ACCESS_KEY_ID": "{{ var.value.aws_access_key_id }}",
    "AWS_SECRET_ACCESS_KEY": "{{ var.value.aws_secret_access_key }}",
    "AWS_REGION": "us-east-1",
    "AWS_S3_BUCKET": S3_BUCKET,
}

COMMON = dict(
    image=IMAGE,
    force_pull=True,       # deploying = pushing a new :latest image
    auto_remove="force",   # never leak stopped containers on a 30 GB disk
    mount_tmp_dir=False,   # no host tmp coupling
    mem_limit="1g",        # the VM has 4 GB total — always cap
)

# Reimplements the inline duckdb S3 count check from pipeline.yml.
HEALTH_CHECK_SRC = """\
import os

import duckdb

conn = duckdb.connect()
# httpfs is baked into the image at build time (see Dockerfile), so the check
# does not depend on extensions.duckdb.org being reachable at run time.
conn.execute("LOAD httpfs;")
conn.execute(f"SET s3_region='{os.environ.get('AWS_REGION', 'us-east-1')}'")
conn.execute(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}'")
conn.execute(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}'")
bucket = os.environ["AWS_S3_BUCKET"]
r = conn.execute(
    f"SELECT COUNT(*) AS n FROM read_parquet("
    f"'s3://{bucket}/transformed/parquet/**/*.parquet', hive_partitioning=true)"
).fetchone()
print(f"Health check: {r[0]} records in S3")
assert r[0] > 0, "ERROR: no records found in S3!"
"""

with DAG(
    dag_id="crime_sp_biweekly",
    description="Download SSP-SP Excel, transform to Parquet, upload to S3, health-check.",
    schedule="0 8 5,20 * *",  # 5th and 20th, 08:00 UTC — mirrors pipeline.yml
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # runs share the crime-sp-work volume; never overlap them
    tags=["pipeline", "crime_sp"],
    params={"ano": ""},  # override year, e.g. {"ano": "2024"}; "" = year at execution
    default_args={
        "retries": 2,  # absorbs Spot preemption mid-task
        "retry_delay": timedelta(minutes=10),
    },
) as dag:
    clean_workspace = DockerOperator(
        task_id="clean_workspace",
        command='bash -c "find /work -mindepth 1 -delete"',
        mounts=[WORK],
        **COMMON,
    )

    download = DockerOperator(
        task_id="download",
        command=f"python pipeline/download.py --ano {ANO} --output /work/raw",
        mounts=[WORK],
        **COMMON,
    )

    transform = DockerOperator(
        task_id="transform",
        command="python pipeline/transform.py --input /work/raw --output /work/parquet --layer all",
        mounts=[WORK],
        **COMMON,
    )

    upload = DockerOperator(
        task_id="upload",
        command=f"python pipeline/upload.py --source /work/parquet --bucket {S3_BUCKET}",
        # PIPELINE_SOURCE feeds the manifest.json "source" audit field.
        environment={**AWS_ENV, "PIPELINE_SOURCE": "airflow"},
        mounts=[WORK],
        **COMMON,
    )

    health_check = DockerOperator(
        task_id="health_check",
        command=["python", "-c", HEALTH_CHECK_SRC],
        environment=AWS_ENV,
        **COMMON,
    )

    clean_workspace >> download >> transform >> upload >> health_check
