# Build context is the REPO ROOT (see docker-compose.yml: context: ., dockerfile:
# airflow/Dockerfile) so this image can copy pipeline_doctor/ in. A context of
# ./airflow would put it out of reach, and mounting it instead wouldn't work
# either: pip needs the source present at build time.
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

# Pipeline Doctor gets a THIRD venv, for the same reason dbt got the second:
# it brings langgraph + langchain + pydantic v2, which pin against Airflow's
# own dependencies. Three isolated environments in one image is not elegant,
# but it is far less fragile than trying to satisfy all three resolvers at once.
# --chown matters: this image runs as the `airflow` user, and building the
# wheel writes build/ inside the copied source tree. Left root-owned, the build
# fails with "could not create 'build/lib/pipeline_doctor': Permission denied".
COPY --chown=airflow:root pipeline_doctor/pyproject.toml /tmp/pipeline_doctor/pyproject.toml
COPY --chown=airflow:root pipeline_doctor/src /tmp/pipeline_doctor/src
RUN python -m venv /home/airflow/pd_venv \
    && /home/airflow/pd_venv/bin/pip install --no-cache-dir --upgrade pip \
    && /home/airflow/pd_venv/bin/pip install --no-cache-dir \
        /tmp/pipeline_doctor \
        "psycopg[binary]>=3.1"

# Pre-create Pipeline Doctor's state directories, owned by the runtime user.
# Docker initializes an *empty* named volume from the image's directory at that
# path, ownership included - so creating them here is what makes the mounted
# volumes writable. Without it the volumes arrive root-owned, the container
# runs as uid 50000, and the first run dies with
# "sqlite3.OperationalError: unable to open database file".
USER root
RUN mkdir -p /opt/pipeline_doctor/data/baselines \
             /opt/pipeline_doctor/data/runs \
             /opt/pipeline_doctor/data/configs \
             /opt/pipeline_doctor/checkpoints \
    && chown -R airflow:root /opt/pipeline_doctor
USER airflow
