"""Scheduled dbt rebuild for the findata pipeline.

Airflow's scope here is deliberately narrow. The Spark streaming job is a
long-running service that schedules its own micro-batches every 10 seconds -
there is no "when should this run" question for an orchestrator to answer, so
Airflow does not touch it. The genuinely recurring work is the dbt rebuild,
which was previously "remember to type `docker compose run --rm dbt build`".

Two tasks, in order:
  1. dbt source freshness - fails if the streaming job has stopped landing rows
  2. dbt build            - rebuild staging + marts, then run the data tests

Freshness gates the build on purpose. If ingestion is dead, rebuilding the
marts would republish the same stale numbers under a fresh build timestamp,
which reads as "fine" to anyone looking at the BI layer. Failing loudly at the
freshness step is the more useful outcome.

Note that `dbt source freshness` only exits non-zero at the *error* threshold
(15 min stale, per sources.yml); a warn (5 min) still passes, so a brief blip
in the streaming job won't block a rebuild.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# dbt is installed in an isolated venv (see airflow/Dockerfile) to avoid
# dependency conflicts with Airflow itself, so it must be called by path.
DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

with DAG(
    dag_id="findata_dbt_build",
    description="Hourly rebuild of the findata staging + marts models",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    # The marts are a full refresh of current state, not a per-interval
    # partition, so replaying every missed hour since start_date would just
    # rebuild the same tables N times.
    catchup=False,
    # Two concurrent full-refresh builds would race on the same target tables.
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["findata", "dbt"],
) as dag:

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} source freshness --profiles-dir .",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} build --profiles-dir .",
    )

    dbt_source_freshness >> dbt_build
