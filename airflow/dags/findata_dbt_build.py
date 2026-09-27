"""Scheduled dbt rebuild for the findata pipeline.

Airflow's scope here is deliberately narrow. The Spark streaming job is a
long-running service that schedules its own micro-batches every 10 seconds -
there is no "when should this run" question for an orchestrator to answer, so
Airflow does not touch it. The genuinely recurring work is the dbt rebuild,
which was previously "remember to type `docker compose run --rm dbt build`".

Three tasks, in order:
  1. dbt source freshness - fails if the streaming job has stopped landing rows
  2. dbt build            - rebuild staging + marts, then run the data tests
  3. pipeline_doctor_check - does the rebuilt mart *look* like normal data?

Each answers a different question, and none of them substitutes for another:
freshness asks whether data is still arriving, dbt's tests ask whether each row
is valid, and Pipeline Doctor asks whether the shape and distribution of the
data has changed. A mart can be fresh and pass every test while half its rows
have vanished or a column has quietly changed type.

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

# Pipeline Doctor lives in a third venv for the same dependency-isolation
# reason dbt has its own (see airflow/Dockerfile), so it is also called by path.
PD_BIN = "/home/airflow/pd_venv/bin/pipeline-doctor"
# Explicit and stable: the baseline is filed under this name, so changing it
# silently starts comparing against nothing.
PD_NAME = "findata_fct_transactions"
# A bounded slice, not the whole mart: profiles carry a sample of each numeric
# column, and the interesting question is "does recent data look normal", not
# "has the cumulative total grown" (it always has).
PD_ROW_LIMIT = 5000
# ORDER BY is not cosmetic here. `limit N` with no ordering lets Postgres return
# whichever rows it likes, so each run profiles a *different* arbitrary slice and
# drift detection compares two unrelated samples - observed live as a phantom
# account_id distribution shift that vanished once the query was ordered.
# Newest-first also makes this "does recent data look normal", which is the
# question worth asking of a mart that only ever grows.
PD_QUERY = (
    "select transaction_id, account_id, event_time, event_date, amount, "
    "merchant, category, location, card_present, is_large_transaction "
    "from analytics_marts.fct_transactions "
    f"order by event_time desc limit {PD_ROW_LIMIT}"
)

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

    # Exit codes: 0 clean, 2 drift is awaiting human review, 1 error. Anything
    # non-zero fails the task, which is the point - a pending review should be
    # visible as a red task rather than passing silently. Clear it with
    #   docker compose exec airflow-scheduler \
    #     /home/airflow/pd_venv/bin/pipeline-doctor resume <thread_id> <approve|reject>
    # using the thread_id printed in this task's log.
    pipeline_doctor_check = BashOperator(
        task_id="pipeline_doctor_check",
        bash_command=f'{PD_BIN} check --source postgres --name {PD_NAME} --query "{PD_QUERY}"',
    )

    dbt_source_freshness >> dbt_build >> pipeline_doctor_check
