FROM apache/airflow:2.10.5-python3.11

# dbt lives in its own virtualenv rather than Airflow's site-packages.
# dbt-core and Airflow pin conflicting versions of shared dependencies
# (jinja2, click most notably), so installing dbt alongside Airflow is a
# known dependency-resolution fight. Isolating it means neither constrains
# the other, and the DAG just calls this venv's dbt binary by absolute path.
#
# The alternative - mounting the Docker socket and driving the existing dbt
# container via DockerOperator - was rejected deliberately: it would give the
# scheduler root-equivalent control of the host Docker daemon to run a job
# that only needs a Postgres connection.
# dbt-core is pinned explicitly, not left to the resolver. Installing
# dbt-postgres==1.8.2 alone pulls dbt-core 2.0.0rc1 (a release candidate),
# which would both put an unstable RC in a reproducible build and mean
# Airflow ran a different dbt version than `docker compose run --rm dbt`
# (which is 1.8.3 via ghcr.io/dbt-labs/dbt-postgres:1.8.latest). Keep these
# two in step whenever the dbt service image is bumped.
RUN python -m venv /home/airflow/dbt_venv \
    && /home/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip \
    && /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
        "dbt-core==1.8.3" \
        "dbt-postgres==1.8.2"
