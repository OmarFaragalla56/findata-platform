# findata pipeline: the complete explanation

A detailed walkthrough of every part of this project: what it does, why each
piece exists, how data moves from one piece to the next, and what the code
does line by line.

Written 2026-09-14, against the code as it is today. If a code snippet here
disagrees with the real file, **the file wins**; update this document.

---

## Contents

1. [What this project is](#1-what-this-project-is)
2. [The big picture](#2-the-big-picture)
3. [The lifecycle](#3-the-lifecycle)
4. [Docker concepts you need](#4-docker-concepts-you-need)
5. [docker-compose.yml, service by service](#5-docker-composeyml-service-by-service)
6. [The producer](#6-the-producer)
7. [The Spark streaming job](#7-the-spark-streaming-job)
8. [Postgres init scripts](#8-postgres-init-scripts)
9. [dbt](#9-dbt)
10. [Airflow](#10-airflow)
11. [Batch mode](#11-batch-mode)
12. [Failure scenarios: what happens when...](#12-failure-scenarios-what-happens-when)
13. [Pipeline Doctor integration](#13-pipeline-doctor-integration)
14. [Known issues and open items](#14-known-issues-and-open-items)
15. [Command cheat sheet](#15-command-cheat-sheet)
16. [Glossary](#16-glossary)
17. [Self-test questions](#17-self-test-questions)

---

## 1. What this project is

### The problem

A bank has card transactions arriving all the time. It wants two different
things from them:

1. **Right now:** "Is account 7 spending unusually fast *this minute*?"
   Fraud-style signals need data within seconds.
2. **Later:** "What did each account do yesterday?" Reports and dashboards
   need data that's clean, tested, and shaped for analysis.

These pull in opposite directions, and **the project is built around that
split**:

| Need | Handled by | Delay |
|---|---|---|
| Right now | **Spark** Structured Streaming | seconds to ~3 minutes |
| Later | **dbt**, scheduled by **Airflow** | up to ~1 hour |
| Storage for both | **Postgres** | n/a |

There's no real bank, so a Python **producer** invents realistic transactions,
and **Kafka** holds them between the producer and Spark.

---|---|
| "real-time data ingestion pipeline using Kafka to stream simulated financial transactions" | `pipeline/producer/produce_transactions.py` → Kafka topic `transactions` |
| "Apache Spark for stateful windowed aggregations, calculating rolling metrics like velocity and average transaction size" | `pipeline/spark_jobs/streaming_job.py`, query 2: `groupBy(window(...), account_id)` → `txn_count`, `avg_amount`, `velocity_per_min` |
| "warehouse transformation models using dbt to clean, test, and structure raw data for BI consumption" | `pipeline/dbt_project/`: staging views → mart tables, 19 tests |
| "Containerized the entire infrastructure using Docker Compose" | `docker-compose.yml`: 11 services, 1 network, 5 volumes |

Airflow was added later and isn't in the CV text.

---

## 2. The big picture

### Architecture

```
┌──────────┐    JSON     ┌─────────┐   bytes    ┌──────────────────────────────┐
│ producer │ ──────────► │  Kafka  │ ─────────► │ Spark Structured Streaming   │
│ (Python) │  5 txn/sec  │ topic:  │  every 10s │                              │
└──────────┘             │ "trans- │            │  query 1: raw rows ──────────┼──► raw.transactions
                         │ actions"│            │  query 2: 1-min windows ─────┼──► raw.account_activity_windows
                         └─────────┘            └──────────────────────────────┘
                                                                  │
                                                            (Postgres)
                                                                  │
            ┌─────────────────────────────────────────────────────┘
            ▼
   ┌──────────────────┐   hourly   ┌────────────────────────────────────────────┐
   │ Airflow          │ ─────────► │ dbt                                        │
   │ scheduler        │            │  1. source freshness (is data arriving?)   │
   │ + webserver :8080│            │  2. build: staging views → mart tables     │
   └──────────────────┘            │            + 19 tests                      │
                                   └────────────────────────────────────────────┘
                                                     │
                                                     ▼
                                   analytics_marts.fct_transactions
                                   analytics_marts.dim_accounts
                                   analytics_marts.agg_account_activity_daily
```

### Every file, and what it's for

| File | What it is |
|---|---|
| `docker-compose.yml` | Defines all 11 services, the network, and the volumes. The single entry point. |
| `.env` | Settings and passwords shared by every service. Gitignored. |
| `.gitignore` | Keeps `.env`, `notes.md`, dbt build output, and Python caches out of git. |
| `README.md` | User-facing overview and run instructions. |
| `notes.md` | Deferred decisions and a log of fixes. Gitignored. |
| `pipeline/producer/produce_transactions.py` | Invents transactions and sends them to Kafka. Stream or batch mode. |
| `pipeline/producer/Dockerfile` | Builds the producer's container image. |
| `pipeline/producer/requirements.txt` | Pinned Python dependencies: `kafka-python`, `faker`. |
| `pipeline/spark_jobs/streaming_job.py` | **The core.** Reads Kafka, writes raw rows plus per-minute window aggregates to Postgres. |
| `pipeline/postgres_init/01_init.sql` | Creates the `raw` schema and both raw tables, on first boot only. |
| `pipeline/postgres_init/02_airflow_db.sql` | Creates the `airflow` database, on first boot only. |
| `pipeline/dbt_project/dbt_project.yml` | dbt configuration: staging = views, marts = tables. |
| `pipeline/dbt_project/profiles.yml` | How dbt connects to Postgres (reads `.env` values). |
| `pipeline/dbt_project/models/staging/sources.yml` | Declares the raw tables to dbt, plus the freshness thresholds. |
| `pipeline/dbt_project/models/staging/stg_transactions.sql` | View: type-cleaned `raw.transactions`. |
| `pipeline/dbt_project/models/staging/stg_account_activity_windows.sql` | View: type-cleaned `raw.account_activity_windows`. |
| `pipeline/dbt_project/models/staging/schema.yml` | Tests on the staging models. |
| `pipeline/dbt_project/models/marts/fct_transactions.sql` | Table: one row per transaction, report-ready. |
| `pipeline/dbt_project/models/marts/dim_accounts.sql` | Table: one row per account, lifetime stats. |
| `pipeline/dbt_project/models/marts/agg_account_activity_daily.sql` | Table: one row per account per day. |
| `pipeline/dbt_project/models/marts/schema.yml` | Tests on the marts. |
| `pipeline/dbt_project/tests/assert_positive_amounts.sql` | Custom test: no amount ≤ 0. |
| `airflow/Dockerfile` | Airflow image with dbt installed in an isolated virtualenv. |
| `airflow/dags/findata_dbt_build.py` | The hourly DAG: freshness → build. |

### Versions

| Component | Version | Pinned where |
|---|---|---|
| Kafka | 3.7.0 (KRaft mode, no ZooKeeper) | `docker-compose.yml` |
| Spark | 3.5.1 | `docker-compose.yml` |
| Spark Kafka connector | `spark-sql-kafka-0-10_2.12:3.5.1` | `--packages` in compose |
| Postgres JDBC driver | 42.7.3 | `--packages` in compose |
| Postgres | 16 | `docker-compose.yml` |
| Airflow | 2.10.5, Python 3.11 | `airflow/Dockerfile` |
| dbt (Airflow) | dbt-core 1.8.3, dbt-postgres 1.8.2 | `airflow/Dockerfile` |
| dbt (ad-hoc service) | `dbt-postgres:1.8.latest` (resolves to 1.8.3) | `docker-compose.yml` |
| kafka-python | 2.0.2 | `pipeline/producer/requirements.txt` |
| Faker | 26.0.0 | `pipeline/producer/requirements.txt` |

---

## 3. The lifecycle

### 3.1 What happens on `docker compose up -d`

Containers wait for each other through `depends_on`. The actual order:

```
STEP 1   postgres ──────────────────┐        kafka ──────────────┐
         (runs pipeline/postgres_init/*.sql  │        (healthcheck lists  │
          ONLY if volume is empty)  │         topics)            │
                                    │                            │
              waits for "healthy"   │                            │ waits for "healthy"
                                    ▼                            ▼
STEP 2   airflow-init          streaming-job ◄── needs BOTH      producer
         (db migrate,          (spark-submit, downloads          (connects, starts
          create admin user,    jars, starts 2 queries)           sending 5 txn/sec)
          then EXITS)
              │
              │ waits for "completed successfully" (exit code 0)
              ▼
STEP 3   airflow-scheduler           airflow-webserver
         (parses DAGs, runs          (UI at localhost:8080)
          them on schedule)

ALSO     adminer (waits only for postgres to *start*, not be healthy)

NOT STARTED by `up`:  dbt, producer-batch, streaming-job-batch   (profile "tools")
```

After startup, `docker ps` shows **7** containers, even though 11 services are
defined. The 3 "tools" services only run when you invoke them, and
`airflow-init` exits after it finishes.

### 3.2 The life of one transaction

We'll follow one transaction: `acct_0007`, $73.28, groceries, at 14:23:41 UTC.

**Hop 1: the producer creates it** (`pipeline/producer/produce_transactions.py`)

```json
{ "transaction_id": "9f3c2a1e-...", "account_id": "acct_0007",
  "event_time": "2026-09-13T14:23:41.123456+00:00", "amount": 73.28,
  "merchant": "Lopez LLC", "category": "groceries",
  "location": "Denver", "card_present": true }
```

Sent to topic `transactions` with **key = `acct_0007`**.

**Hop 2: Kafka stores it**

It's now bytes in an ordered log, at a numbered position (the **offset**):

```
partition 0, offset 48211:  key=b'acct_0007'  value=b'{"transaction_id": ...}'
```

Kafka keeps it after it's read, for 7 days by default.

**Hop 3: Spark reads and parses it** (`pipeline/spark_jobs/streaming_job.py`)

Every 10 seconds Spark reads the new offsets, casts the bytes to text, parses the
JSON against `TRANSACTION_SCHEMA`, and drops rows missing required fields. It's
now a typed row:

```
transaction_id | account_id | event_time          | amount | merchant  | category  | location | card_present
9f3c2a1e-...   | acct_0007  | 2026-09-13 14:23:41 | 73.28  | Lopez LLC | groceries | Denver   | true
```

Here the path splits in two.

**Hop 4a: query 1 stores the raw row**

```
row ─► raw._staging_continuous_transactions   (replaced every batch)
          │  INSERT ... SELECT ... ON CONFLICT (transaction_id) DO NOTHING
          ▼
       raw.transactions   (+ ingested_at = now(), filled in by Postgres)
```

It's in Postgres within about 10 seconds.

**Hop 4b: query 2 adds it to a one-minute window**

The row joins the bucket *(acct_0007, 14:23:00–14:24:00)*. Spark keeps a running
total for that bucket across batches:

```
(acct_0007, 14:23–14:24):  txn_count=3  total_amount=210.45  avg_amount=70.15
```

Once Spark has seen events up to 14:26:00, the **watermark** (latest event
time minus 2 minutes) passes 14:24:00. The window is then final, and **one row**
is written:

```
account_id | window_start | window_end | txn_count | total_amount | avg_amount | velocity_per_min
acct_0007  | 14:23:00     | 14:24:00   | 3         | 210.45       | 70.15      | 3.0
```

It goes through the same staging-and-merge step into
`raw.account_activity_windows`, about 2–3 minutes after the transaction happened.

**Hop 5: dbt reshapes it** (scheduled hourly by Airflow)

```
raw.transactions ─► stg_transactions (VIEW) ─┬─► fct_transactions (TABLE)
                                             └─► dim_accounts (TABLE)

raw.account_activity_windows ─► stg_account_activity_windows (VIEW)
                                     └─► agg_account_activity_daily (TABLE)
```

Our transaction ends up in `fct_transactions` with `event_date = 2026-09-13`
and `is_large_transaction = false`. It also counts toward acct_0007's lifetime
totals in `dim_accounts`, and its minute is part of acct_0007's 2026-09-13 row in
`agg_account_activity_daily`.

### 3.3 The hourly cycle

```
airflow-scheduler: "it's 15:00, run findata_dbt_build"
     │
     ▼
TASK 1: dbt source freshness
     "is max(ingested_at) in raw less than 15 minutes old?"
     │
     ├── NO  ─► task fails (after 2 retries, 2 min apart)
     │          ─► TASK 2 marked upstream_failed, never runs
     │          ─► DAG run is red in the UI
     │
     └── YES ─► TASK 2: dbt build
                  staging views ─► mart tables ─► 19 tests
                  │
                  └─► TASK 3: pipeline_doctor_check
                        profile the rebuilt mart, compare to the baseline
                        │
                        ├── looks normal ──► task green
                        └── drift needing review ──► exit 2 ──► task red
                  │
                  └─► results in the `airflow` database, visible at localhost:8080
```

### 3.4 How long each step takes

| Step | Delay added | Why |
|---|---|---|
| Producer → Kafka | ~0 | Queued in memory, sent within milliseconds |
| Kafka → `raw.transactions` | **≤ 10 s** | Spark's micro-batch trigger interval |
| Kafka → `raw.account_activity_windows` | **~2–3 min** | 1-minute window + 2-minute watermark |
| raw → staging views | 0 | Views read raw data at query time |
| raw → mart tables | **≤ 60 min** | Tables are rebuilt only when the hourly DAG runs |

**Worst case from transaction to mart: about 63 minutes.**

---

## 4. Docker concepts you need

### Image vs container

- An **image** is a packaged, read-only template: an operating system,
  installed software, and your code. `postgres:16` is an image.
- A **container** is a running instance of an image, like a program started
  from an installer. You can delete a container and create another from the
  same image.

### Dockerfile and layers

A `Dockerfile` is a recipe for building an image. **Each instruction creates
a layer**, and Docker **reuses unchanged layers** when you rebuild. Once a layer
changes, every layer after it is rebuilt.

```dockerfile
COPY requirements.txt .                  # layer A - rarely changes
RUN pip install -r requirements.txt      # layer B - slow; reused if A unchanged
COPY produce_transactions.py .           # layer C - changes often
```

Editing the `.py` file rebuilds only layer C, which takes about 1 second. If
the `.py` were copied **before** `pip install`, every code edit would reinstall
all the packages.

### Compose

`docker-compose.yml` describes several containers and how they connect.
`docker compose up -d` creates all of them. The top-level `name:
findata-pipeline` sets the **project name**, which prefixes everything Compose
creates: containers, networks, and volumes (e.g. `findata-pipeline_postgres-data`).

### Networks and service names

Every service is on the `findata-net` network. On that network, **a service's
name is its hostname**: Spark connects to `kafka:9092` and `postgres:5432`
as if those were real machines. These names **only resolve inside the
network**. Your laptop doesn't know what `kafka` is.

### Ports: `"HOST:CONTAINER"`

```yaml
ports:
  - "8081:8080"     # localhost:8081 on your laptop → port 8080 inside adminer
```

Every container has its own network space, so **two containers can both use
port 8080 internally** (adminer and airflow-webserver both do). Only the
**host** side of the mapping has to be unique.

### Volumes: named volumes vs bind mounts

```yaml
volumes:
  - postgres-data:/var/lib/postgresql/data     # NAMED volume: Docker manages where it's stored
  - ./pipeline/postgres_init:/docker-entrypoint-initdb.d # BIND mount: a folder from your project
```

- **Named volume:** survives `docker compose down`, and is **deleted by
  `docker compose down -v`**. Used for data that has to persist: Postgres
  data, Kafka's log, Spark checkpoints.
- **Bind mount:** the container sees **your actual folder**, and edits on
  your laptop show up immediately. Used for code and config: Spark jobs,
  dbt project, DAGs, init SQL.

### `env_file` vs `environment`

```yaml
env_file: .env            # load every KEY=value from .env
environment:
  PRODUCER_MODE: batch    # set / override individual variables
```

If both set the same variable, **`environment:` wins**. That's how
`producer-batch` and `streaming-job-batch` reuse `.env` but switch modes.

### `${VAR}` vs `$${VAR}`

- `${POSTGRES_USER}` is replaced **by Compose, when it reads the file**, using
  your `.env`.
- `$${POSTGRES_USER}` is an escaped `$`. Compose leaves `${POSTGRES_USER}` in
  place, and **the shell inside the container** fills it in when the command runs.

### `depends_on` conditions

| Form | Waits until the dependency has... |
|---|---|
| `- postgres` (short form) | **started** (it may not be ready yet) |
| `condition: service_healthy` | passed its **healthcheck** |
| `condition: service_completed_successfully` | **exited with code 0** |

### Restart policies

`restart: unless-stopped` means that if the container stops for any reason
**other than you stopping it**, Docker starts it again. Together with Spark's
checkpoints, this is the pipeline's crash recovery.

### Profiles

```yaml
profiles: ["tools"]
```

Services with a profile are **skipped by `docker compose up`**. You run them
explicitly: `docker compose run --rm dbt build`.

### YAML anchors: `&`, `*`, `<<`

```yaml
x-spark-common: &spark-common        # & defines an anchor named spark-common
  image: apache/spark:3.5.1
  ...

services:
  streaming-job:
    <<: *spark-common                # * refers to it; <<: merges its keys in here
    container_name: findata-streaming-job
```

Top-level keys starting with `x-` are ignored by Compose, so they're a place to
define shared blocks. This lets two services share one definition, so they
can't drift apart.

### `entrypoint` vs `command`

The container runs `entrypoint + command`. For the `dbt` service:

```yaml
entrypoint: ["dbt"]
command: ["build", "--profiles-dir", "."]    # default → runs: dbt build --profiles-dir .
```

`docker compose run --rm dbt test` **replaces the whole `command`**, so it runs
`dbt test` and `--profiles-dir .` is not included. Add it back explicitly when
you override the command.

### The commands, compared

| Command | What it does |
|---|---|
| `docker compose up -d` | Create and start all non-profile services, in the background |
| `docker compose run --rm dbt build` | Start **one** service, run a command, **delete the container** afterwards |
| `docker compose down` | Stop and delete containers + network. **Volumes kept.** |
| `docker compose down -v` | Same, **and delete volumes**, which wipes all data |
| `docker ps` | **Running** containers only |
| `docker ps -a` | All containers, including exited ones (e.g. `airflow-init`) |
| `docker compose logs -f streaming-job` | Follow one service's output |

---
## 5. docker-compose.yml, service by service

### Top level

```yaml
name: findata-pipeline

networks:
  findata-net:
    driver: bridge

volumes:
  postgres-data:
  kafka-data:
  spark-ivy-cache:
  spark-checkpoints:
  airflow-logs:
```

- `name:` sets the project name. **Don't change it.** Volume names include it
  (`findata-pipeline_postgres-data`), so renaming the project makes Compose
  look for new, empty volumes. The old data would still exist but no service
  would use it.
- `bridge` is Docker's standard private network on a single machine.
- The five named volumes:

| Volume | Mounted at | Holds | Lose it and... |
|---|---|---|---|
| `postgres-data` | `/var/lib/postgresql/data` | All raw data, marts, the Airflow DB | Everything is gone; init SQL runs again |
| `kafka-data` | `/tmp/kafka-logs` | Every message in the topic | Nothing can be replayed |
| `spark-checkpoints` | `/tmp/checkpoints` | Offsets + open window state | Spark restarts from `latest` and skips data |
| `spark-ivy-cache` | `/root/.ivy2` | Downloaded jars | Only slower startup (jars download again) |
| `airflow-logs` | `/opt/airflow/logs` | Task logs | Log history |

### `x-spark-common`, shared by both Spark services

```yaml
x-spark-common: &spark-common
  image: apache/spark:3.5.1
  user: root
  env_file: .env
  working_dir: /opt/spark_jobs
  volumes:
    - ./pipeline/spark_jobs:/opt/spark_jobs
    - spark-ivy-cache:/root/.ivy2
    - spark-checkpoints:/tmp/checkpoints
  networks:
    - findata-net
  depends_on:
    kafka:
      condition: service_healthy
    postgres:
      condition: service_healthy
  command: >
    /opt/spark/bin/spark-submit
    --master local[*]
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3
    /opt/spark_jobs/streaming_job.py
```

| Line | Meaning |
|---|---|
| `user: root` | Run as root so Spark can write to the mounted volumes. Side effect: the home directory is `/root`, so Ivy's cache is at `/root/.ivy2`. The volume used to be mounted at `/opt/spark/.ivy2`, which Spark never read, so jars downloaded again on every restart. |
| `./spark_jobs:/opt/spark_jobs` | Bind mount: edit `streaming_job.py` on your laptop, then restart the container to pick it up. No rebuild needed. |
| `command: >` | `>` is YAML's "folded" string: the lines are joined with spaces into one command. |
| `spark-submit` | Spark's launcher. It starts a Java process (JVM), and your Python drives it. |
| `--master local[*]` | Run everything in this one container using all CPU cores, not on a cluster. |
| `--packages` | Download two Java libraries at startup. Format `group:artifact:version`. `_2.12` is the Scala version Spark 3.5 uses. The Kafka connector makes `.format("kafka")` work; the Postgres driver makes JDBC work. |

### `x-airflow-common`, shared by the three Airflow services

```yaml
x-airflow-common: &airflow-common
  build: ./airflow
  env_file: .env
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/airflow
    AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY}
    AIRFLOW__WEBSERVER__SECRET_KEY: ${AIRFLOW_SECRET_KEY}
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG: "false"
  volumes:
    - ./airflow/dags:/opt/airflow/dags
    - ./pipeline/dbt_project:/opt/airflow/dbt_project
    - airflow-logs:/opt/airflow/logs
  networks:
    - findata-net
```

Airflow reads settings from environment variables named
`AIRFLOW__<SECTION>__<KEY>` (double underscores):

| Variable | Meaning |
|---|---|
| `CORE__EXECUTOR: LocalExecutor` | Tasks run as **subprocesses of the scheduler**, in the scheduler container. So `dbt` actually runs inside `findata-airflow-scheduler`. |
| `CORE__LOAD_EXAMPLES: "false"` | Don't load Airflow's ~50 example DAGs into the UI. |
| `DATABASE__SQL_ALCHEMY_CONN` | Where Airflow stores its own data: the `airflow` database on our Postgres. `${...}` values are filled in by Compose from `.env`. |
| `CORE__FERNET_KEY` | Encryption key for passwords Airflow stores (e.g. saved connections). |
| `WEBSERVER__SECRET_KEY` | Signs the web UI's login session cookies. |
| `WEBSERVER__EXPOSE_CONFIG: "false"` | Hide the config page in the UI, since it would show these secrets. |

The volumes: `dags/` is how the scheduler finds DAG files. `pipeline/dbt_project/` is
the **same folder** the `dbt` service mounts, so both see the same models (and
share one `target/` folder; see Known issues).

### `kafka`

```yaml
kafka:
  build: ./kafka          # thin image over apache/kafka:3.7.0, for volume ownership
  container_name: findata-kafka
  restart: unless-stopped
  ports:
    - "9092:9092"
    - "29092:29092"
  environment:
    KAFKA_NODE_ID: "1"
    KAFKA_PROCESS_ROLES: broker,controller
    KAFKA_LISTENERS: PLAINTEXT://:9092,EXTERNAL://:29092,CONTROLLER://:9093
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,EXTERNAL://localhost:29092
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
    KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
    KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
  volumes:
    - kafka-data:/tmp/kafka-logs
  healthcheck:
    test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"]
    interval: 10s
    timeout: 10s
    retries: 10
    start_period: 30s
```

**KRaft mode.** Kafka used to need a separate ZooKeeper service to coordinate.
In KRaft mode Kafka coordinates itself. `PROCESS_ROLES: broker,controller`
means this single node both stores messages (broker) and manages the cluster
(controller). `CONTROLLER_QUORUM_VOTERS: 1@kafka:9093` lists the controllers:
just node 1.

**Listeners.** A listener is a port Kafka accepts connections on. There are three:

| Listener | Port | Used by |
|---|---|---|
| `PLAINTEXT` | 9092 | Containers on `findata-net` (producer, Spark) |
| `EXTERNAL` | 29092 | Programs on **your laptop** |
| `CONTROLLER` | 9093 | Kafka's internal coordination |

**Why two client listeners?** Connecting to Kafka is a two-step process:

```
1. client connects to the address you gave it
2. Kafka replies: "for this topic, talk to <ADVERTISED ADDRESS>"
3. client reconnects to that advertised address
```

With only `PLAINTEXT://kafka:9092` advertised, a laptop program could connect to
`localhost:9092` (the port mapping works), but in step 2 Kafka would reply
"talk to `kafka:9092`", and your laptop can't resolve the name `kafka`. So the
**EXTERNAL** listener advertises `localhost:29092`, an address the laptop
*can* reach.

- Inside Docker → `kafka:9092`
- From your laptop → `localhost:29092`

**`OFFSETS_TOPIC_REPLICATION_FACTOR: "1"`.** Kafka's internal
`__consumer_offsets` topic defaults to 3 copies, which needs 3 brokers. With
a single broker it has to be 1, or anything that uses consumer groups (like
the console consumer) fails.

**Topic creation.** Nothing creates the `transactions` topic explicitly. Kafka's
default `auto.create.topics.enable=true` creates it the first time the producer
sends, with **one partition**.

**The healthcheck** lists topics. If that command succeeds, Kafka is accepting
clients. `start_period: 30s` means failures in the first 30 seconds don't count
against the 10 retries.

### `postgres`

```yaml
postgres:
  image: postgres:16
  container_name: findata-postgres
  restart: unless-stopped
  env_file: .env
  ports:
    - "5432:5432"
  volumes:
    - postgres-data:/var/lib/postgresql/data
    - ./pipeline/postgres_init:/docker-entrypoint-initdb.d
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}"]
```

- The image reads **`POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB`** from
  `.env` on first boot to create the user and database. `POSTGRES_HOST` and
  `POSTGRES_PORT` are **not** used by Postgres itself; they're for clients.
- `/docker-entrypoint-initdb.d` is where the image looks for init scripts
  (Section 8).
- `pg_isready` is a small tool that exits 0 when Postgres accepts
  connections. `$$` defers the variables to the container's shell.
- Port 5432 is published so you can connect from your laptop (e.g. with DBeaver).

### `adminer`

```yaml
adminer:
  image: adminer
  ports:
    - "8081:8080"
  depends_on:
    - postgres
```

A web page for browsing Postgres at `http://localhost:8081`. Log in with
server `postgres` and the credentials from `.env`. Short-form `depends_on`
waits only for postgres to *start*.

### `producer` and `producer-batch`

```yaml
producer:
  build: ./pipeline/producer
  restart: unless-stopped
  env_file: .env
  depends_on:
    kafka:
      condition: service_healthy

producer-batch:
  build: ./pipeline/producer
  profiles: ["tools"]
  env_file: .env
  environment:
    PRODUCER_MODE: batch
  depends_on:
    kafka:
      condition: service_healthy
```

The same code, built from `./producer/Dockerfile`. `producer-batch` has no
`restart` because it's meant to finish and exit.

### `streaming-job` and `streaming-job-batch`

```yaml
streaming-job:
  <<: *spark-common
  container_name: findata-streaming-job
  restart: unless-stopped

streaming-job-batch:
  <<: *spark-common
  container_name: findata-streaming-job-batch
  profiles: ["tools"]
  environment:
    SPARK_TRIGGER_MODE: batch
```

Everything else comes from the anchor. `.env` has
`SPARK_TRIGGER_MODE=continuous`, and the batch service overrides it.

### `dbt`

```yaml
dbt:
  image: ghcr.io/dbt-labs/dbt-postgres:1.8.latest
  profiles: ["tools"]
  env_file: .env
  volumes:
    - ./pipeline/dbt_project:/usr/app/dbt_project
  working_dir: /usr/app/dbt_project
  entrypoint: ["dbt"]
  command: ["build", "--profiles-dir", "."]
```

For running dbt by hand. `env_file` supplies the `POSTGRES_*` values that
`profiles.yml` reads. `--profiles-dir .` tells dbt to use `profiles.yml` from
the current folder, which is the project folder.

### `airflow-init`

```yaml
airflow-init:
  <<: *airflow-common
  entrypoint: /bin/bash
  command:
    - -c
    - >
      airflow db migrate &&
      { airflow users create
      --username "$${AIRFLOW_ADMIN_USER}"
      --password "$${AIRFLOW_ADMIN_PASSWORD}"
      --firstname Admin --lastname User --role Admin
      --email admin@example.com || true; }
  depends_on:
    postgres:
      condition: service_healthy
```

This runs `bash -c "<script>"`:

1. `airflow db migrate` creates or upgrades Airflow's ~50 tables in the `airflow` database.
2. `&&` runs the next part only if the migration succeeded.
3. `airflow users create ...` creates the admin login for the UI.
4. `|| true`: on a second run the user already exists and the create fails.
   `|| true` makes that failure harmless.

**Why the `{ ... }` braces matter:** without them,
`migrate && create || true` is read as `(migrate && create) || true`, so a
**failed migration** would also be hidden by `|| true`. The container would
exit 0 and the scheduler would start against a broken database. With braces,
only the user-create failure is ignored.

### `airflow-scheduler` and `airflow-webserver`

```yaml
airflow-scheduler:
  <<: *airflow-common
  restart: unless-stopped
  command: scheduler
  depends_on:
    postgres: { condition: service_healthy }
    airflow-init: { condition: service_completed_successfully }

airflow-webserver:
  <<: *airflow-common
  restart: unless-stopped
  command: webserver
  ports:
    - "8080:8080"
  depends_on: (same)
```

The same image, with different commands. The **scheduler** decides when DAGs
run, and with LocalExecutor it also runs the tasks. The **webserver** is only
the UI. Both wait until `airflow-init` has exited successfully.

---
## 6. The producer

**Files:** `pipeline/producer/produce_transactions.py`, `pipeline/producer/Dockerfile`,
`pipeline/producer/requirements.txt`

**Why it exists:** there's no real bank feeding us data, so this script
generates realistic transactions and sends them to Kafka.

### 6.1 Dependencies

```
kafka-python==2.0.2
faker==26.0.0
```

`==` pins exact versions, so rebuilding the image months later gives the same
libraries.

### 6.2 Dockerfile

```dockerfile
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY produce_transactions.py .
CMD ["python", "produce_transactions.py"]
```

| Line | Why |
|---|---|
| `python:3.11-slim` | Official Python image, minimal Debian, small download |
| `PYTHONUNBUFFERED=1` | Without it Python holds `print()` output back and writes it in chunks, so `docker compose logs` shows nothing for a while |
| `WORKDIR /app` | Folder for all following commands (created if missing) |
| `COPY requirements.txt` **before** the `.py` | Layer caching: code edits don't trigger a reinstall (Section 4) |
| `--no-cache-dir` | Don't keep pip's download cache in the image, since it's never used again |
| `CMD` | What runs when the container starts |

### 6.3 Configuration

```python
fake = Faker()

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "transactions")
RATE_PER_SEC = float(os.environ.get("PRODUCER_RATE_PER_SEC", "5"))
NUM_ACCOUNTS = int(os.environ.get("PRODUCER_NUM_ACCOUNTS", "25"))
MODE = os.environ.get("PRODUCER_MODE", "stream").lower()  # "stream" | "batch"
BATCH_SIZE = int(os.environ.get("PRODUCER_BATCH_SIZE", "1000"))
```

- `os.environ.get(NAME, DEFAULT)` reads an environment variable, or uses the
  default if it isn't set.
- The values travel `.env` → Compose `env_file` → container environment →
  `os.environ`. Inside Docker, `BOOTSTRAP_SERVERS` is `kafka:9092`.
- **Environment variables are always text**, so `float()` and `int()` convert them.
- `.lower()` makes the mode case-insensitive.
- Note: the `localhost:9092` default is out of date. From a laptop the correct
  address is `localhost:29092`. It doesn't matter inside Docker.

```python
CATEGORIES = [
    "groceries", "dining", "travel", "electronics", "utilities",
    "entertainment", "healthcare", "fuel", "subscription", "cash_withdrawal",
]

ACCOUNTS = [f"acct_{i:04d}" for i in range(1, NUM_ACCOUNTS + 1)]
```

- `CATEGORIES` has 10 values. The dbt `accepted_values` test lists these same 10.
- `ACCOUNTS` is a **list comprehension**. `range(1, 26)` gives 1–25 (the end
  is excluded), and `{i:04d}` pads to 4 digits, producing
  `["acct_0001", ..., "acct_0025"]`. Zero-padding keeps text sorting correct.
- **Why reuse 25 accounts** instead of a new account per transaction: with
  unique accounts, every per-account window would contain exactly one
  transaction, and "velocity" would mean nothing.

### 6.4 `build_transaction()`

```python
def build_transaction() -> dict:
    category = random.choice(CATEGORIES)
    # cash withdrawals / travel skew higher; everyday categories skew lower
    base = {
        "cash_withdrawal": (50, 500),
        "travel": (80, 1200),
        "electronics": (40, 900),
    }.get(category, (5, 150))

    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": random.choice(ACCOUNTS),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "amount": round(random.uniform(*base), 2),
        "merchant": fake.company(),
        "category": category,
        "location": fake.city(),
        "card_present": random.random() > 0.35,
    }
```

| Field | How it's made | Notes |
|---|---|---|
| `category` | Random, 1 in 10 each | |
| `base` | Dict lookup with `.get(key, default)` | Travel $80–1200, electronics $40–900, cash $50–500, others $5–150 |
| `transaction_id` | `uuid4()`, random 128-bit ID, as text | Becomes the **primary key** |
| `account_id` | Random from 25 | |
| `event_time` | Current time **in UTC**, ISO text | `"2026-09-13T14:23:41.123456+00:00"` |
| `amount` | `uniform(*base)`: `*` unpacks the pair into two arguments, then rounded to cents | Only travel/electronics/cash can reach ≥ $500, so only they can be `is_large_transaction` |
| `merchant`, `location` | Faker | A new random value each time, so near-unique |
| `card_present` | `random() > 0.35` | True ~65% of the time |

⚠️ **`event_time` is always "now", so there are never late events.** Spark's
2-minute watermark exists to handle late data, and this producer never sends
any, so that code path is never exercised.

### 6.5 `on_send_error()`

```python
def on_send_error(excp):
    """errback for producer.send() — without this, failed sends vanish silently
    and the 'sent' counter keeps incrementing as if delivery succeeded."""
    print(f"[producer] send failed: {excp!r}")
```

**How `send()` works:** it **doesn't send anything right away**. It puts the message
in an in-memory queue and returns a **future** immediately. A background thread
sends queued messages to Kafka in batches.

```
your loop                           background thread
send(msg1) → queued, returns
send(msg2) → queued, returns
send(msg3) → queued, returns        ──► sends msg1..3 to Kafka
                                          ├── success: nothing happens
                                          └── failure: calls on_send_error(error)
```

A failure happens **later, on another thread**, after your loop has moved on,
so there's no line in your loop for a `try/except` to catch it.
`.add_errback(on_send_error)` is the only way to find out. `{excp!r}` uses
`repr()`, which includes the error type.

### 6.6 `request_shutdown()`

```python
def request_shutdown(signum, frame):
    """docker compose down sends SIGTERM, not SIGINT, so without this handler
    the KeyboardInterrupt catch below never fires and buffered messages are
    dropped instead of flushed."""
    raise KeyboardInterrupt
```

- **Ctrl+C** sends **SIGINT**, which Python turns into `KeyboardInterrupt` → caught →
  flush → clean exit.
- **`docker compose down`** sends **SIGTERM**, which by default makes Python **exit
  immediately**: no `except`, no `finally`, and queued messages are lost.
- This handler turns SIGTERM into `KeyboardInterrupt`, so both go through the
  same clean shutdown. `signum` and `frame` are required parameters for a
  signal handler, even though they aren't used.

### 6.7 `connect_with_retry()`

```python
def connect_with_retry(max_attempts: int = 15, delay_sec: int = 5) -> KafkaProducer:
    for attempt in range(1, max_attempts + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )
        except NoBrokersAvailable:
            print(f"[producer] Kafka not ready (attempt {attempt}/{max_attempts}), retrying in {delay_sec}s...")
            time.sleep(delay_sec)
    raise RuntimeError("Could not connect to Kafka after multiple retries")
```

- It tries up to 15 times, 5 seconds apart (about 75 seconds), then raises.
- `return` inside the `try` exits the function as soon as a connection succeeds.
- **Serializers:** Kafka stores only **bytes**. The two `lambda` functions
  (small unnamed one-line functions) convert automatically on every `send()`:
  - value: `dict` → `json.dumps` → text → `.encode` → bytes
  - key: `"acct_0007"` → `.encode` → `b"acct_0007"`
- **Why retry when Compose already waits for `service_healthy`:** the
  healthcheck can pass slightly before Kafka accepts clients, and
  `restart: unless-stopped` can restart the producer while Kafka itself is
  restarting, when Compose's startup ordering doesn't apply.

### 6.8 `run_stream()`

```python
def run_stream(producer: KafkaProducer) -> None:
    print(f"[producer] connected to {BOOTSTRAP_SERVERS}, streaming to topic '{TOPIC}' at ~{RATE_PER_SEC}/sec")

    interval = 1.0 / RATE_PER_SEC
    sent = 0
    try:
        while True:
            txn = build_transaction()
            producer.send(TOPIC, key=txn["account_id"], value=txn).add_errback(on_send_error)
            sent += 1
            if sent % 50 == 0:
                producer.flush()
                print(f"[producer] sent {sent} transactions")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("[producer] shutting down, flushing buffered messages...")
    finally:
        producer.flush()
        producer.close()
```

| Line | Meaning |
|---|---|
| `interval = 1.0 / RATE_PER_SEC` | 0.2 seconds between messages |
| `while True` | Loop forever; only an exception ends it |
| `key=txn["account_id"]` | Kafka sends messages with the same key to the same partition, **keeping each account's events in order**. With one partition this changes nothing yet, but it's correct for scaling out. |
| `sent % 50 == 0` | Every 50th message (`%` = remainder) |
| `producer.flush()` | **Wait** until the queue has reached Kafka, so "sent 500" means delivered, not just queued |
| `time.sleep(interval)` | Rate limiting. Building and sending also take a little time, so the real rate is slightly **below** 5/sec (hence the `~` in the log) |
| `except KeyboardInterrupt` | Ctrl+C, or SIGTERM via `request_shutdown` |
| `finally` | **Always runs**: flush what's left, close connections |

### 6.9 `run_batch()`

```python
def run_batch(producer: KafkaProducer) -> None:
    print(f"[producer] connected to {BOOTSTRAP_SERVERS}, batch mode: sending {BATCH_SIZE} transactions to '{TOPIC}'")

    try:
        for sent in range(1, BATCH_SIZE + 1):
            txn = build_transaction()
            producer.send(TOPIC, key=txn["account_id"], value=txn).add_errback(on_send_error)
            if sent % 500 == 0:
                producer.flush()
                print(f"[producer] sent {sent}/{BATCH_SIZE}")
    except KeyboardInterrupt:
        print("[producer] batch interrupted, flushing buffered messages...")
    finally:
        producer.flush()
        producer.close()
    print("[producer] batch complete")
```

Differences from stream mode:
- A `for` loop over 1000, not `while True`, so it ends.
- **No `sleep`**, so it sends as fast as possible.
- It flushes every 500 instead of every 50, which gives more efficient batching.
- Side effect: all 1000 `event_time`s fall within about a second, so **they all land in
  the same 1-minute Spark window**. That's about 40 transactions per account in one
  window, which is useful for testing the aggregation.

🐛 **Bug (logged in notes.md):** if interrupted, the `except` handles the
exception, then `finally` runs, then the last `print` **also runs**. You get
"batch interrupted" followed by "batch complete". The fix is to move that print
inside the `try`, after the `for` loop.

### 6.10 `main()` and the entry point

```python
def main():
    signal.signal(signal.SIGTERM, request_shutdown)

    producer = connect_with_retry()

    if MODE == "batch":
        run_batch(producer)
    else:
        run_stream(producer)


if __name__ == "__main__":
    main()
```

- The signal handler is registered **first**, so a stop during the ~75-second
  connection retry is also handled cleanly.
- **Any mode other than `"batch"`, including typos, runs stream mode.**
- `__name__ == "__main__"` is true only when the file is run directly. If
  another file does `import produce_transactions`, `main()` doesn't run, so
  `build_transaction()` can be imported (e.g. in a test) without starting an
  endless producer.

### 6.11 The producer in one picture

```
.env ─► compose ─► container env ─► os.environ.get() ─► constants

build_transaction() ─► dict ─► serializer ─► bytes ─► send() queue ─► background thread ─► Kafka
                                                          │
                                                failure ──► on_send_error()

docker compose down ─► SIGTERM ─► request_shutdown() ─► KeyboardInterrupt ─► finally: flush(), close()
```

---
## 7. The Spark streaming job

**File:** `pipeline/spark_jobs/streaming_job.py`
**Why it exists:** this is the "right now" side. It handles the continuous flow
and does the per-minute math.

### 7.1 The key idea: Spark is lazy

Almost every line in `main()` **computes nothing**. `.select()`, `.where()`,
and `.groupBy()` add steps to a **plan**; they don't process data.

```python
parsed = raw_stream.select(...).where(...)   # nothing happens
windowed = parsed.groupBy(...).agg(...)      # still nothing
windowed.writeStream....start()              # ← NOW Spark starts running the plan
```

A variable like `parsed` isn't data. It's **a description of how to get data**.
Keep this in mind, because it explains:
- why two queries built from the same `parsed` still **read Kafka twice** (each
  query runs its own copy of the plan)
- why `persist()` is needed (without it, using a DataFrame twice
  **computes it twice**)

### 7.2 Micro-batches, and why every 10 seconds

Spark Structured Streaming doesn't process each message the moment it arrives.
It wakes up, takes **everything that arrived since last time**, processes that
group as a batch, and waits again. That group is a **micro-batch**.

```python
{"processingTime": "10 seconds"}
```

- In data engineering, "real-time" usually means **seconds**. Ten seconds is
  real-time for a bank dashboard.
- **Why not 1 second?** Every batch has a fixed cost: drop and recreate the staging
  table, run the merge, open and close a database connection. At 5 msg/sec, a 1-second
  trigger pays that cost for about 5 rows; 10 seconds pays it for about 50.
- **The window output can't be faster anyway:** a 1-minute window plus a 2-minute
  watermark means window rows arrive about 3 minutes after the fact.
- Spark does have true **Continuous Processing** (`trigger(continuous=...)`,
  ~1 ms latency), but it **doesn't support aggregations or `foreachBatch`**, so
  neither of our queries could use it.

⚠️ The env value `SPARK_TRIGGER_MODE=continuous` is a **misleading name**: it
means "micro-batches every 10s, forever", not Spark's Continuous Processing.
Renaming it is logged in notes.md, and it needs care because the name is part of
the checkpoint path.

### 7.3 Docstring and imports

```python
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, sum as _sum, avg
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, TimestampType
)
```

| Import | Used for |
|---|---|
| `SparkSession` | The entry point to Spark |
| `col("x")` | Refer to column `x` |
| `from_json` | Parse JSON text into structured columns |
| `window` | Assign timestamps to time buckets |
| `count`, `avg` | Aggregations |
| `sum as _sum` | Spark's `sum`, **renamed** so it doesn't replace Python's built-in `sum()` for the whole file |
| `StructType`, `StructField`, `*Type` | Describe the columns and types of the JSON |

### 7.4 Configuration

```python
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "transactions")
```
The **same** `.env` variables the producer reads. That's how both sides agree
on the topic name.

```python
WINDOW_MINUTES = 1  # single source of truth: used both to build the tumbling
                     # window and to convert txn_count into a per-minute rate
```
Used in **two places that must agree**: the window width in `groupBy` and the
divisor in `velocity_per_min`. A per-minute rate is by definition "events ÷
minutes in the window". If the two disagreed, velocity would be wrong and no
error would appear. (Before a fix, the divisor was a hardcoded `/ 1.0`.)

```python
TRIGGER_MODE = os.environ.get("SPARK_TRIGGER_MODE", "continuous").lower()  # "continuous" | "batch"
IS_BATCH = TRIGGER_MODE == "batch"
CHECKPOINT_ROOT = f"/tmp/checkpoints/{TRIGGER_MODE}"
```
- `IS_BATCH` is `True`/`False`; read it as `IS_BATCH = (TRIGGER_MODE == "batch")`.
- Any value other than `"batch"`, including typos, runs continuous mode.
- The checkpoint path **includes the mode**, so a batch run can never use the
  live job's offsets.

```python
PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_DB", "findata")
PG_USER = os.environ.get("POSTGRES_USER", "findata")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "findata_pw")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}
```
- `"postgres"` is the Compose service name, which works as a hostname on the network.
- **JDBC** is Java's standard database interface. Spark runs on Java, so it
  talks to Postgres through JDBC. The URL is `jdbc:postgresql://postgres:5432/findata`.
- `driver` names the Java class inside the Postgres jar loaded by `--packages`.

```python
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("account_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("amount", DoubleType()),
    StructField("merchant", StringType()),
    StructField("category", StringType()),
    StructField("location", StringType()),
    StructField("card_present", BooleanType()),
])
```
**The contract with the producer.** If the two disagree, **nothing crashes**:
- a JSON field not listed here is **silently dropped**
- a field whose value doesn't fit the type becomes **NULL**

This is why adding a field (e.g. `currency`) requires a change here.

⚠️ `amount` is `DoubleType`, a **floating-point** number, which can't store most
decimals exactly (sums can come out as `210.45000000000002`). It's stored
correctly only because Postgres rounds on insert into `NUMERIC`. A real financial
system would use `DecimalType(12, 2)`.

```python
RAW_TXN_STAGING_TABLE = f"raw._staging_{TRIGGER_MODE}_transactions"
WINDOW_STAGING_TABLE = f"raw._staging_{TRIGGER_MODE}_account_activity_windows"
```
Four staging tables in total (2 modes × 2 queries). The leading `_` means "internal".

### 7.5 `_merge_staging_into()`

```python
def _merge_staging_into(spark, staging_table, target_table, conflict_columns, select_columns):
    columns_sql = ", ".join(select_columns)
    merge_sql = (
        f"INSERT INTO {target_table} ({columns_sql}) "
        f"SELECT {columns_sql} FROM {staging_table} "
        f"ON CONFLICT ({', '.join(conflict_columns)}) DO NOTHING"
    )
    jvm = spark.sparkContext._jvm
    conn = jvm.java.sql.DriverManager.getConnection(JDBC_URL, PG_USER, PG_PASSWORD)
    try:
        stmt = conn.createStatement()
        try:
            return stmt.executeUpdate(merge_sql)
        finally:
            stmt.close()
    finally:
        conn.close()
```

**Why this function exists:** Spark's JDBC writer can only `append` or
`overwrite`. **It can't do `ON CONFLICT`.** So we write the SQL ourselves.

**Building the SQL:**
- `", ".join([...])` joins a list into `"transaction_id, account_id, ..."`.
- Adjacent f-strings inside parentheses are **concatenated automatically**.
- Result, for the raw sink:

```sql
INSERT INTO raw.transactions (transaction_id, account_id, event_time, amount, merchant, category, location, card_present)
SELECT transaction_id, account_id, event_time, amount, merchant, category, location, card_present
FROM raw._staging_continuous_transactions
ON CONFLICT (transaction_id) DO NOTHING
```

In plain terms: copy every staging row into the real table, and skip any whose
`transaction_id` already exists.

> Building SQL with f-strings is normally a red flag (SQL injection). It's safe
> here because every value is a constant from this file, never outside input.

**`spark.sparkContext._jvm`:** Spark runs as a Java process, and your Python
drives it through a bridge library called **py4j**. `_jvm` gives Python
**direct access to Java classes**:

```
Python process                          Java process (JVM)
jvm.java.sql.DriverManager   ──py4j──►  java.sql.DriverManager
  .getConnection(...)       (socket)      .getConnection(...) ──► Postgres
```

Why not install `psycopg2` (a Python Postgres library)? The Java Postgres
driver is **already loaded** via `--packages`, so this adds no dependency.

**Running it:**
- `createStatement()` returns an object that runs SQL.
- `executeUpdate()` runs it and **returns the number of rows inserted**. Skipped
  rows aren't counted.
- **Nested `finally`:** the statement and the connection are closed **even if the SQL
  fails**. This runs every 10 seconds, so a leaked connection per failure would exhaust
  Postgres's ~100 connection slots.
- **No `commit()`:** JDBC connections default to **autocommit**, so the insert is
  saved as soon as it succeeds.

### 7.6 `write_raw_batch()`, the sink for query 1

```python
def write_raw_batch(batch_df, batch_id):
    batch_df.persist()
    try:
        attempted = batch_df.count()
        if attempted == 0:
            return
        (
            batch_df.write
            .mode("overwrite")
            .jdbc(JDBC_URL, RAW_TXN_STAGING_TABLE, properties=JDBC_PROPS)
        )
        inserted = _merge_staging_into(
            batch_df.sparkSession,
            RAW_TXN_STAGING_TABLE,
            "raw.transactions",
            conflict_columns=["transaction_id"],
            select_columns=batch_df.columns,
        )
        skipped_note = f" ({attempted - inserted} already present, skipped)" if inserted < attempted else ""
        print(f"[streaming-job] raw batch {batch_id}: wrote {inserted} rows{skipped_note}")
    finally:
        batch_df.unpersist()
```

**Who calls it:** Spark, once per micro-batch, via `.foreachBatch(write_raw_batch)`.
- `batch_df`: this batch's rows, as a normal (non-streaming) DataFrame
- `batch_id`: the batch number, which **keeps counting across restarts** (stored in the checkpoint)

| Step | Line | Why |
|---|---|---|
| 1 | `persist()` | The batch is used **twice** (`count` and `write`). Without caching, Spark would read Kafka and parse the JSON twice. |
| 2 | `count()` | The first real computation: runs the plan, fills the cache, counts rows |
| 3 | `if attempted == 0: return` | Nothing new in the last 10s. `return` still goes through `finally`. |
| 4 | `.mode("overwrite").jdbc(... staging ...)` | **Drop and recreate** the staging table from `batch_df`'s columns, then insert. Staging holds only this batch. Reads from the cache. |
| 5 | `_merge_staging_into(...)` | Copy staging → real table, skipping existing IDs. `batch_df.columns` is taken from the DataFrame, so it always matches staging. `ingested_at` isn't in that list, so Postgres fills it with `now()`. |
| 6 | `skipped_note` | Conditional expression, `A if cond else B` |
| 7 | `unpersist()` in `finally` | **Always** free the cache. Otherwise a new batch is cached every 10 seconds and memory runs out. |

### 7.7 Why a retry is safe: checkpoints and idempotency

For each batch, Spark writes two records to the checkpoint:

```
/tmp/checkpoints/continuous/raw_transactions/
  offsets/1071    ← BEFORE running: "batch 1071 = Kafka offsets 48000–48050"
  commits/1071    ← AFTER success
```

Suppose the container crashes **after the merge, before `commits/1071`**:

```
restart ─► Spark finds offsets/1071 but no commits/1071
        ─► "1071 never finished" ─► runs it AGAIN with the SAME offsets
        ─► same 50 rows ─► staging ─► merge
        ─► ON CONFLICT: all 50 exist ─► inserted = 0
        ─► log: "wrote 0 rows (50 already present, skipped)"
```

Without the merge, the replay would be a plain insert → duplicate primary key →
crash → restart → replay → crash, forever. The staging-and-merge step makes the
replay harmless. **Idempotent** means running it twice has the same effect as
running it once.

### 7.8 `write_window_batch()`, the sink for query 2

```python
def write_window_batch(batch_df, batch_id):
    out = (
        batch_df
        .select(
            col("account_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("txn_count"),
            col("total_amount"),
            col("avg_amount"),
            (col("txn_count") / WINDOW_MINUTES).alias("velocity_per_min"),
        )
    )
    out.persist()
    try:
        attempted = out.count()
        if attempted == 0:
            return
        (
            out.write
            .mode("overwrite")
            .jdbc(JDBC_URL, WINDOW_STAGING_TABLE, properties=JDBC_PROPS)
        )
        inserted = _merge_staging_into(
            out.sparkSession,
            WINDOW_STAGING_TABLE,
            "raw.account_activity_windows",
            conflict_columns=["account_id", "window_start"],
            select_columns=out.columns,
        )
        skipped_note = f" ({attempted - inserted} already present, skipped)" if inserted < attempted else ""
        print(f"[streaming-job] window batch {batch_id}: wrote {inserted} rows{skipped_note}")
    finally:
        out.unpersist()
```

**What `batch_df` looks like here:** `groupBy(window(...))` produces a column
called `window` that is a **struct** (a column with sub-fields):

```
window                            | account_id | txn_count | total_amount | avg_amount
{start: 14:23:00, end: 14:24:00}  | acct_0007  | 3         | 210.45       | 70.15
```

- Postgres has no struct type, so `col("window.start").alias("window_start")` pulls
  out the sub-field and names it.
- `velocity_per_min = txn_count / WINDOW_MINUTES`.
- The output column names **must match** `raw.account_activity_windows`, because the merge
  uses them as its column list.
- `out` is cached, not `batch_df`. `out` is what gets counted and written.
- **Composite conflict key** `["account_id", "window_start"]`: one row per
  account per window. It must match the table's `PRIMARY KEY (account_id, window_start)`.

### 7.9 `main()`: session and Kafka source

```python
def main():
    spark = (
        SparkSession.builder
        .appName("financial-transaction-streaming")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    starting_offsets = "earliest" if IS_BATCH else "latest"

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", starting_offsets)
        .load()
    )
```

| Line | Meaning |
|---|---|
| `getOrCreate()` | Reuse the existing session or create one; picks up `spark-submit` settings like `--master` |
| `setLogLevel("WARN")` | Spark logs a lot at INFO; WARN keeps your `print` lines readable |
| `startingOffsets` | Where to start reading **the first time**: `earliest` = start of topic (batch), `latest` = only new messages (live). **Ignored after the first run**, because the checkpoint knows the exact offset. |
| `readStream` | A **streaming** source that keeps producing new rows |
| `format("kafka")` | Needs the connector jar from `--packages` |
| `load()` | Still reads nothing; lazy |

**What the Kafka source gives you**, whatever is in your messages:

```
key (binary) | value (binary) | topic        | partition | offset | timestamp | timestampType
b'acct_0007' | b'{"trans...'  | transactions | 0         | 48211  | ...       | 0
```

Your transaction is in `value`, as raw bytes.

### 7.10 `main()`: parse, filter, watermark

```python
    parsed = (
        raw_stream
        .select(from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"))
        .select("data.*")
        .where(
            col("transaction_id").isNotNull()
            & col("account_id").isNotNull()
            & col("event_time").isNotNull()
            & col("amount").isNotNull()
        )
        .withWatermark("event_time", "2 minutes")
    )
```

**Parsing, from the inside out:**
1. `col("value")` → bytes
2. `.cast("string")` → text `'{"transaction_id": ...}'`
3. `from_json(text, TRANSACTION_SCHEMA)` → a struct
4. `.alias("data")` → name it `data`
5. `.select("data.*")` → **unpack** the struct into 8 real columns

All other Kafka columns (`key`, `offset`, ...) are dropped by the first `select`.

**The null filter:** `from_json` **doesn't raise** on invalid JSON. It returns a
row of **all NULLs**. Without the filter, that row reaches the insert, breaks
`NOT NULL` / `PRIMARY KEY`, and **crashes the query**. The four columns checked
are exactly the four `NOT NULL` columns in `raw.transactions`.

> In Spark, use `&` (not Python's `and`) to combine column conditions.

**The watermark.** After each batch, Spark computes:

```
watermark = (latest event_time seen so far) − 2 minutes
```

A window whose **end** is before the watermark is final: it's written out and its
state is freed.

```
latest event seen: 14:26:05   ─►  watermark: 14:24:05
window 14:23–14:24 ends 14:24:00 < 14:24:05  ─►  FINAL, write it
window 14:24–14:25 ends 14:25:00 > 14:24:05  ─►  still open
```

Without a watermark, Spark could never know a window is finished, and its state would grow forever.

⚠️ **The watermark only advances when newer events arrive.** If the producer
stops, the last ~2–3 minutes of windows stay unwritten. A batch-mode run exits
with them still in its checkpoint, and they're written by the next run that brings
newer data.

(The watermark is on the shared `parsed`, so it also applies to query 1, where it
does nothing because query 1 has no windows.)

### 7.11 `main()`: the two queries

```python
    trigger_kwargs = {"availableNow": True} if IS_BATCH else {"processingTime": "10 seconds"}

    raw_query = (
        parsed.writeStream
        .foreachBatch(write_raw_batch)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/raw_transactions")
        .trigger(**trigger_kwargs)
        .start()
    )

    windowed = (
        parsed
        .groupBy(window(col("event_time"), f"{WINDOW_MINUTES} minute"), col("account_id"))
        .agg(
            count("transaction_id").alias("txn_count"),
            _sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
        )
    )

    window_query = (
        windowed.writeStream
        .foreachBatch(write_window_batch)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/account_activity_windows")
        .trigger(**trigger_kwargs)
        .start()
    )
```

- `**trigger_kwargs` **unpacks a dict into named arguments**:
  `.trigger(**{"processingTime": "10 seconds"})` becomes `.trigger(processingTime="10 seconds")`.
  One dict is shared, so the two queries can't end up with different triggers.
- `availableNow=True`: process everything available right now, then stop.
- `foreachBatch(fn)` calls your function per micro-batch, which allows custom logic like the merge.
- `checkpointLocation` is a separate folder per query.
- **`.start()` is where Spark actually begins**; it runs in the background and returns immediately.

**The stateful part:**
- `window(col("event_time"), "1 minute")` assigns 14:23:41 to `{14:23:00, 14:24:00}`
- `groupBy(window, account_id)` makes one group per (minute, account)
- `agg(...)` computes `txn_count`, `total_amount`, `avg_amount`
- Groups **stay open across batches**: one transaction in batch 100 and two more in
  batch 103 accumulate into the same running totals, stored in the checkpoint.

**Why the two queries read Kafka twice:** each `.start()` creates an independent
query with its own Kafka consumer and its own checkpoint. Sharing `parsed`
shares the *plan*, not the execution.

### 7.12 Output modes, and why `append` has to match the merge

| Mode | When a window row is emitted | Here |
|---|---|---|
| `complete` | **Every** window, every batch | Rewrites all history every 10s |
| `update` | Each time a window **changes** | The 14:23 window emitted several times as it grows |
| **`append`** | **Once**, when final (past the watermark) | ✅ Each window exactly once |

`append` with an aggregation **requires a watermark**; Spark refuses to start
without one.

**Why `update` would break with our merge:**

```
batch 100: window has 1 txn  → emit txn_count=1  → INSERT ✅
batch 103: window has 3 txns → emit txn_count=3  → CONFLICT → DO NOTHING ❌
```

The table would keep `txn_count=1` forever, with no error. `ON CONFLICT DO NOTHING`
keeps the **first** version, so it's only correct when the first version is
final, which is exactly what `append` guarantees. `update` would need
`ON CONFLICT DO UPDATE`.

`avg_amount` is computed and stored, but **no mart uses it** (see Known issues).

### 7.13 `main()`: waiting, and crash recovery

```python
    if IS_BATCH:
        for query in (raw_query, window_query):
            query.awaitTermination()
        print("[streaming-job] batch mode: all available data processed, exiting")
    else:
        spark.streams.awaitAnyTermination()
```

Both `.start()` calls return immediately. Without waiting, `main()` would end
and the process would exit, stopping both queries.

- **Batch:** `availableNow` queries stop on their own. Wait for **each one**.
  `awaitAnyTermination()` would return when the *first* finished and cut off the other.
- **Continuous:** queries never stop on their own. Wait until **either one dies**.

**The recovery chain in continuous mode:**

```
a query crashes (e.g. Postgres unreachable)
 ─► awaitAnyTermination() raises
 ─► the Python process exits ─► the container stops
 ─► restart: unless-stopped ─► Docker starts it again
 ─► spark-submit ─► both queries resume from their checkpoints
```

There's no retry loop in the code. **Docker's restart policy and Spark's
checkpoints together** are the retry mechanism.

### 7.14 The whole job in one picture

```
spark-submit ─► main()
  ├─ raw_stream: Kafka source (key, value bytes, offset, ...)
  ├─ parsed: cast ─► from_json ─► data.* ─► where not null ─► watermark
  │
  ├─ QUERY 1 (every 10s)                 QUERY 2
  │   write_raw_batch():                  groupBy(1-min window, account) + agg
  │     persist, count                    (state kept across batches)
  │     overwrite staging                 every 10s, windows past watermark:
  │     merge ON CONFLICT                 write_window_batch():
  │     unpersist                           flatten + velocity
  │       │                                 persist, count, staging, merge
  │       ▼                                   │
  │   raw.transactions                        ▼
  │                                       raw.account_activity_windows
  │
  └─ wait (each query in batch mode / any query in continuous mode)
```

---
## 8. Postgres init scripts

**Files:** `pipeline/postgres_init/01_init.sql`, `pipeline/postgres_init/02_airflow_db.sql`

### 8.1 When they run

The Postgres image's startup script does this:

```
container starts
   │
   ▼
is /var/lib/postgresql/data empty?
   ├── NO  ─► start normally. Init files IGNORED.
   └── YES ─► 1. create a new database cluster
              2. create user $POSTGRES_USER / $POSTGRES_PASSWORD
              3. create database $POSTGRES_DB
              4. run every file in /docker-entrypoint-initdb.d ALPHABETICALLY,
                 connected to $POSTGRES_DB as $POSTGRES_USER
              5. start normally
```

What follows from this:
- **`01_` / `02_` prefixes** set the order.
- **They run once**, on the first boot of an empty volume. Adding a new file to a
  running stack does nothing until `docker compose down -v`, which wipes all data.
  To change a live database, run the SQL by hand (e.g. `ALTER TABLE`).
- They run connected to `findata`, so `CREATE SCHEMA raw` creates `raw` inside `findata`.

### 8.2 Where everything lives

```
Postgres server (container findata-postgres)
├── database: findata
│   ├── schema: public               default, unused
│   ├── schema: raw                  ← 01_init.sql (Spark writes here)
│   │     ├── transactions
│   │     ├── account_activity_windows
│   │     └── _staging_{mode}_*      created by Spark at runtime
│   ├── schema: analytics_staging    ← created by dbt (views)
│   └── schema: analytics_marts      ← created by dbt (tables)
└── database: airflow                ← 02_airflow_db.sql (Airflow's own tables)
```

A **schema** is a folder of tables inside a database. A **database** is a fully
separate space. A query in one database can't read tables in another.

### 8.3 `01_init.sql`

```sql
-- Raw landing zone written to by the Spark Structured Streaming job.
-- dbt staging models read from this schema and never mutate it.
CREATE SCHEMA IF NOT EXISTS raw;
```

- **Ownership:** Spark writes to `raw`, dbt only reads it. If dbt modified raw data,
  you could no longer rebuild the marts from the original data.
- `IF NOT EXISTS` lets the file be rerun safely by hand, even though init only runs
  on an empty database.

```sql
CREATE TABLE IF NOT EXISTS raw.transactions (
    transaction_id   VARCHAR(64) PRIMARY KEY,
    account_id       VARCHAR(32) NOT NULL,
    event_time       TIMESTAMP   NOT NULL,
    amount            NUMERIC(12, 2) NOT NULL,
    merchant          VARCHAR(128),
    category          VARCHAR(64),
    location          VARCHAR(64),
    card_present      BOOLEAN,
    ingested_at       TIMESTAMP DEFAULT now()
);
```

| Column | Type | Notes |
|---|---|---|
| `transaction_id` | `VARCHAR(64) PRIMARY KEY` | Max 64 characters (a UUID is 36). **PRIMARY KEY = unique + not null**, enforced on every insert. Required by `ON CONFLICT (transaction_id)`. |
| `account_id` | `VARCHAR(32) NOT NULL` | Required |
| `event_time` | `TIMESTAMP NOT NULL` | Date and time **without timezone**. Works because everything is UTC. `TIMESTAMPTZ` would be more robust. |
| `amount` | `NUMERIC(12, 2) NOT NULL` | **Exact** decimal: 12 digits, 2 after the point, max `9,999,999,999.99`. This is where Spark's float is rounded to cents. |
| `merchant`, `category`, `location`, `card_present` | nullable | Optional fields |
| `ingested_at` | `TIMESTAMP DEFAULT now()` | **Nobody sends this.** Postgres fills it in when the row arrives. It's the **freshness** field. |

**The four `NOT NULL` columns match Spark's `.where(isNotNull)` filter exactly.**
They're two halves of one contract. Add `NOT NULL` to a column here without
adding it to the filter, and a message missing that field crashes the job.

**The two timestamps:**

| Column | Meaning | Set by | Used for |
|---|---|---|---|
| `event_time` | When the transaction **happened** | Producer | Windows, reports |
| `ingested_at` | When the row **arrived in Postgres** | Postgres | Freshness check |

Freshness has to use `ingested_at`. The question it answers is "is data still
arriving?", and `event_time` describes when the transaction happened, not when it arrived.

```sql
CREATE INDEX IF NOT EXISTS idx_transactions_account_time
    ON raw.transactions (account_id, event_time);
```

An **index** is a sorted lookup structure, like a book's index: it lets Postgres
find "account 7, yesterday" without reading every row. The cost is that every insert
also updates the index. ⚠️ **Nothing in the pipeline uses it**, since dbt reads whole
tables. It only helps ad-hoc queries.

```sql
CREATE TABLE IF NOT EXISTS raw.account_activity_windows (
    account_id        VARCHAR(32) NOT NULL,
    window_start       TIMESTAMP NOT NULL,
    window_end         TIMESTAMP NOT NULL,
    txn_count           INTEGER NOT NULL,
    total_amount        NUMERIC(14, 2) NOT NULL,
    avg_amount           NUMERIC(12, 2) NOT NULL,
    velocity_per_min     NUMERIC(10, 4) NOT NULL,
    computed_at          TIMESTAMP DEFAULT now(),
    PRIMARY KEY (account_id, window_start)
);
```

| Column | Notes |
|---|---|
| column names | Must **exactly match** the `.select()` in `write_window_batch` |
| `total_amount NUMERIC(14, 2)` | 14 digits (vs 12), because a sum can exceed any single amount |
| `velocity_per_min NUMERIC(10, 4)` | 4 decimals, so rates like `0.6` (3 txns in a 5-minute window) fit |
| `window_end` | Redundant (always `window_start + 1 min`), but saves querying code from needing to know the window width |
| `computed_at` | This table's freshness field |
| `PRIMARY KEY (account_id, window_start)` | **Composite:** the *combination* is unique. Matches the Spark merge's `conflict_columns`. |

### 8.4 `02_airflow_db.sql`

```sql
-- NOTE: files in docker-entrypoint-initdb.d only execute on the FIRST boot of
-- an empty postgres volume. If you add this to a stack whose volume already
-- exists, create the database by hand instead:
--   docker exec findata-postgres psql -U findata -d postgres -c "CREATE DATABASE airflow;"
CREATE DATABASE airflow;
```

- A separate **database** for Airflow's own records (DAG runs, task states, users).
  It reuses this Postgres server, because a second server would be unnecessary at this scale.
- **Why no `IF NOT EXISTS`:** Postgres **doesn't support** `CREATE DATABASE IF NOT EXISTS`.
  That form exists only for schemas, tables, and indexes. So this file can't be rerun.
- This file was added **after** the stack already had data, so the existing volume never ran
  it and the database had to be created by hand, as the comment describes.

### 8.5 What depends on `01_init.sql`

```
NOT NULL columns ─────────────────► Spark .where(isNotNull) filter        (must match)
window table column names ────────► write_window_batch .select()          (must match)
PRIMARY KEY (transaction_id) ─────► merge ON CONFLICT (transaction_id)    (required)
PRIMARY KEY (account_id, window_start) ─► merge ON CONFLICT (...)         (required)
NUMERIC types ────────────────────► round Spark's floats to exact cents
ingested_at / computed_at ────────► dbt sources.yml loaded_at_field (freshness)
schema "raw" ─────────────────────► dbt sources.yml  schema: raw
```

---

## 9. dbt

**Folder:** `pipeline/dbt_project/`
**Why it exists:** raw tables are exactly what Spark wrote, which isn't what a dashboard
needs. dbt turns them into clean, **tested**, report-ready tables using only
SQL `SELECT` files.

### 9.1 What dbt actually does

You write a `SELECT`. dbt:
1. **Compiles** it, replacing Jinja such as `{{ ref('stg_transactions') }}` with real
   table names like `"findata"."analytics_staging"."stg_transactions"`.
2. **Wraps** it in DDL depending on the materialization:
   - view → `CREATE VIEW ... AS <your select>`
   - table → roughly: build into a temporary name (`..._dbt_tmp`), then swap it in,
     so readers never see a half-built table.
3. **Runs** models in dependency order, worked out from `ref()` and `source()`.
4. **Tests**: runs each test query and fails if it returns rows.

Compiled SQL is written to `pipeline/dbt_project/target/` (gitignored), which is useful
for seeing what actually ran.

### 9.2 `dbt_project.yml`

```yaml
name: 'findata_pipeline'
version: '1.0.0'
config-version: 2

profile: 'findata_pipeline'

model-paths: ["models"]
test-paths: ["tests"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  findata_pipeline:
    staging:
      +materialized: view
      +schema: staging
    marts:
      +materialized: table
      +schema: marts
```

| Key | Meaning |
|---|---|
| `profile` | Which connection in `profiles.yml` to use |
| `model-paths`, `test-paths` | Where the SQL files are |
| `target-path` | Where compiled output goes |
| `clean-targets` | What `dbt clean` deletes |
| `staging: +materialized: view` | Every model in `models/staging/` becomes a **view** |
| `marts: +materialized: table` | Every model in `models/marts/` becomes a **table** |
| `+schema: staging` / `marts` | Schema suffix (see 9.3) |

The `+` means "apply to everything in this folder".

### 9.3 `profiles.yml`: the connection

```yaml
findata_pipeline:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('POSTGRES_HOST') }}"
      user: "{{ env_var('POSTGRES_USER') }}"
      password: "{{ env_var('POSTGRES_PASSWORD') }}"
      port: "{{ env_var('POSTGRES_PORT') | as_number }}"
      dbname: "{{ env_var('POSTGRES_DB') }}"
      schema: analytics
      threads: 4
```

- `env_var('X')` reads environment variable X, so **no password in the file**.
  Both the `dbt` service and the Airflow containers get these values from `.env`.
- `| as_number`: env vars are text, and `port` must be a number.
- `target: dev` selects the `dev` output. You could add a `prod` output later.
- `threads: 4` runs up to 4 models or tests in parallel when they don't depend on each other.
- **`schema: analytics` + `+schema: staging` = `analytics_staging`.** dbt's default
  rule **joins** the base schema and the custom one with `_`. That's why the schemas
  are `analytics_staging` and `analytics_marts`, not `staging` and `marts`.

### 9.4 `sources.yml`: declaring the raw tables

```yaml
version: 2

sources:
  - name: raw
    schema: raw
    description: Landing tables written directly by the Spark Structured Streaming job.
    freshness:
      warn_after: {count: 5, period: minute}
      error_after: {count: 15, period: minute}
    tables:
      - name: transactions
        loaded_at_field: ingested_at
        description: One row per raw financial transaction event consumed from Kafka.
      - name: account_activity_windows
        loaded_at_field: computed_at
        description: Per-account, 1-minute tumbling window aggregates (velocity, avg transaction size) computed by Spark.
```

- A **source** is a table **dbt didn't create**. `source('raw', 'transactions')` → `raw.transactions`.
- **`freshness` + `loaded_at_field`** define the check that `dbt source freshness` runs.
  Roughly:

  ```sql
  select max(ingested_at) as max_loaded_at, now() as snapshotted_at
  from raw.transactions
  ```

  | Age of newest row | Result | Exit code |
  |---|---|---|
  | < 5 min | pass | 0 |
  | 5–15 min | **warn** | 0, so the DAG continues |
  | > 15 min | **error** | non-zero, so the DAG stops |

- `loaded_at_field` alone does nothing without a `freshness:` block. That was a
  bug that has been fixed.

### 9.5 `ref()` vs `source()`, and the model graph

| Function | Points at | Used in |
|---|---|---|
| `source('raw', 'transactions')` | A table dbt **didn't** create | **staging** only |
| `ref('stg_transactions')` | Another **dbt model** | **marts** |

`ref()` is how dbt works out build order:

```
source: raw.transactions ─────────► stg_transactions ─────┬─► fct_transactions
                                                         └─► dim_accounts

source: raw.account_activity_windows ─► stg_account_activity_windows ─► agg_account_activity_daily
```

**Why staging exists:** `source()` appears **only in staging**. If a raw column is
renamed, you fix **one line** in one staging model. Every mart reads the staging
model, and staging keeps the old name via an alias:

```sql
txn_amount::numeric(12, 2) as amount   -- only this line changes
```

Without the staging layer, every mart would repeat the same casting, and a rename
would mean editing several files.

### 9.6 View vs table: the practical difference

| | View (staging) | Table (marts) |
|---|---|---|
| Stores data? | No, it's a saved query | Yes, a copy |
| When new raw rows arrive | **Visible immediately** | **Not visible** until next `dbt build` |
| Query cost | Re-runs the query every read | Fast read |

**This is where the up-to-one-hour delay comes from.** It's also why the Pipeline Doctor
export returned zero rows when it asked for "the last 10 minutes from now()":
the newest mart row was older than that.

### 9.7 `stg_transactions.sql`

```sql
-- Type-cleaned pass-through over the raw landing table.
--
-- There is deliberately no dedupe step here. transaction_id is the PRIMARY KEY
-- of raw.transactions, and the Spark sink merges each micro-batch with
-- INSERT ... ON CONFLICT (transaction_id) DO NOTHING, so a duplicate cannot
-- physically reach this model. ...
select
    transaction_id,
    account_id,
    event_time,
    amount::numeric(12, 2) as amount,
    merchant,
    category,
    location,
    card_present,
    ingested_at

from {{ source('raw', 'transactions') }}
```

- `amount::numeric(12, 2)` is Postgres's cast syntax (`::`). The column is already
  `NUMERIC(12,2)`, so this mostly documents the type.
- **No dedupe:** a `row_number()` dedupe used to be here. It could never remove
  anything, because the primary key already prevents duplicates. It was removed. If duplicates ever appeared,
  the `unique` test should fail loudly rather than the model hiding them.

### 9.8 `stg_account_activity_windows.sql`

```sql
select
    account_id,
    window_start,
    window_end,
    txn_count,
    total_amount::numeric(14, 2) as total_amount,
    avg_amount::numeric(12, 2) as avg_amount,
    velocity_per_min::numeric(10, 4) as velocity_per_min,
    computed_at

from {{ source('raw', 'account_activity_windows') }}
```

A pass-through with explicit types.

### 9.9 `fct_transactions.sql`: the fact table

```sql
select
    transaction_id,
    account_id,
    event_time,
    -- cast to a real date: date_trunc alone returns a timestamp pinned to
    -- midnight, so a column named *_date was actually carrying a timestamp.
    date_trunc('day', event_time)::date as event_date,
    amount,
    merchant,
    category,
    location,
    card_present,
    amount >= 500 as is_large_transaction

from {{ ref('stg_transactions') }}
```

- A **fact table** has one row per event, for reporting.
- `date_trunc('day', ts)` gives `2026-09-13 00:00:00`, still a timestamp. `::date`
  makes it `2026-09-13`, a real date. (It used to lack `::date`.)
- `amount >= 500 as is_large_transaction`: a comparison produces a boolean. Only
  travel, electronics, and cash withdrawals of exactly $500 can be true.

### 9.10 `dim_accounts.sql`: the dimension table

```sql
select
    account_id,
    min(event_time) as first_seen_at,
    max(event_time) as last_seen_at,
    count(*) as lifetime_txn_count,
    sum(amount) as lifetime_total_amount,
    round(avg(amount), 2) as lifetime_avg_amount

from {{ ref('stg_transactions') }}
group by account_id
```

- A **dimension table** has one row per *thing* (here, per account), with its attributes.
- `group by account_id` with aggregates gives 25 rows.
- `avg(amount)` here is correct: it averages **individual transactions**, not averages
  of averages.

### 9.11 `agg_account_activity_daily.sql`: the daily rollup

```sql
select
    account_id,
    date_trunc('day', window_start)::date as activity_date,
    sum(txn_count) as total_txn_count,
    sum(total_amount) as total_amount,
    round(sum(total_amount) / nullif(sum(txn_count), 0), 2) as avg_transaction_size,
    max(velocity_per_min) as peak_velocity_per_min,
    round(avg(velocity_per_min), 2) as avg_velocity_per_active_min

from {{ ref('stg_account_activity_windows') }}
group by 1, 2
```

One row per **account per day**, built from per-minute window rows.

| Column | How | Why |
|---|---|---|
| `activity_date` | `date_trunc(...)::date` | Day grain, as a real date |
| `total_txn_count` | `sum(txn_count)` | Transactions in the day |
| `total_amount` | `sum(total_amount)` | Spend in the day |
| `avg_transaction_size` | `sum(total_amount) / sum(txn_count)` | **Weighted** average (see below) |
| `nullif(x, 0)` | Returns NULL if x is 0 | Avoids a divide-by-zero error; the result is NULL instead |
| `peak_velocity_per_min` | `max(velocity_per_min)` | Busiest minute |
| `avg_velocity_per_active_min` | `avg(velocity_per_min)` | Average over minutes **with activity** |
| `group by 1, 2` | Positions in the select list | Same as `group by account_id, activity_date` |

**The average-of-averages bug (fixed):** it used to be `avg(avg_amount)`. Example:

```
minute A: 1 transaction  of $100           → avg_amount = 100
minute B: 9 transactions of $10 each       → avg_amount = 10

avg(avg_amount)                = (100 + 10) / 2        = 55     ❌
sum(total_amount)/sum(txn_count) = (100 + 90) / (1 + 9) = 19     ✅
```

The minute with 1 transaction counted as much as the minute with 9. The weighted version is correct.

**Why `avg(velocity_per_min)` is fine:** every window is the same width (1 minute),
so an unweighted average is correct here.

**Why "per active minute":** Spark only emits a row for minutes in which an account
**transacted**. An account idle for 23 hours with one busy minute reports that
minute's rate, not a rate averaged over 24 hours. The column was renamed from
`avg_velocity_per_min` so it isn't misread.

### 9.12 Tests

A **dbt test is a SELECT that looks for bad rows. Zero rows = pass.**

**Generic tests** (declared in YAML, SQL generated by dbt). For example, `unique` on
`transaction_id` compiles to roughly:

```sql
select transaction_id, count(*)
from analytics_staging.stg_transactions
where transaction_id is not null
group by transaction_id
having count(*) > 1
```

**Singular tests** (a SQL file you write). `tests/assert_positive_amounts.sql`:

```sql
-- Fails if any fact row has a non-positive amount, which would indicate a
-- bad event slipped past the simulator or a bug in upstream cleaning logic.
select *
from {{ ref('fct_transactions') }}
where amount <= 0
```

**`models/staging/schema.yml`:**

```yaml
models:
  - name: stg_transactions
    description: Type-cleaned financial transaction events, one row per transaction_id.
    columns:
      - name: transaction_id
        description: Primary key, UUID assigned by the producer.
        data_tests: [unique, not_null]
      - name: account_id
        data_tests: [not_null]
      - name: event_time
        data_tests: [not_null]
      - name: amount
        data_tests: [not_null]
      - name: category
        data_tests:
          - not_null
          - accepted_values:
              values: [groceries, dining, travel, electronics, utilities,
                       entertainment, healthcare, fuel, subscription, cash_withdrawal]

  - name: stg_account_activity_windows
    columns:
      - name: account_id
        data_tests: [not_null]
      - name: window_start
        data_tests: [not_null]
      - name: txn_count
        data_tests: [not_null]
```

(Shown in compact `[a, b]` form; the file uses one item per line. Same meaning.)

**`models/marts/schema.yml`:**

```yaml
models:
  - name: dim_accounts
    columns:
      - name: account_id
        data_tests: [unique, not_null]

  - name: fct_transactions
    columns:
      - name: transaction_id
        data_tests: [unique, not_null]
      - name: account_id
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_accounts')
              field: account_id

  - name: agg_account_activity_daily
    columns:
      - name: account_id
        data_tests: [not_null]
      - name: activity_date
        data_tests: [not_null]
```

`relationships` means every `fct_transactions.account_id` must exist in
`dim_accounts.account_id` (a foreign-key check).

**Complete inventory: 19 tests**

| # | Model | Column | Test | Can it realistically fail? |
|---|---|---|---|---|
| 1 | stg_transactions | transaction_id | unique | No: primary key upstream |
| 2 | stg_transactions | transaction_id | not_null | No: primary key upstream |
| 3 | stg_transactions | account_id | not_null | No: `NOT NULL` + Spark filter |
| 4 | stg_transactions | event_time | not_null | No: `NOT NULL` + Spark filter |
| 5 | stg_transactions | amount | not_null | No: `NOT NULL` + Spark filter |
| 6 | stg_transactions | category | not_null | Only if a message lacks category |
| 7 | stg_transactions | category | accepted_values | **Yes**, if the producer's list changes |
| 8 | stg_account_activity_windows | account_id | not_null | No: `NOT NULL` |
| 9 | stg_account_activity_windows | window_start | not_null | No: `NOT NULL` |
| 10 | stg_account_activity_windows | txn_count | not_null | No: `NOT NULL` |
| 11 | dim_accounts | account_id | unique | No: it's a `group by account_id` |
| 12 | dim_accounts | account_id | not_null | No |
| 13 | fct_transactions | transaction_id | unique | No: primary key upstream |
| 14 | fct_transactions | transaction_id | not_null | No |
| 15 | fct_transactions | account_id | not_null | No |
| 16 | fct_transactions | account_id | relationships | No: dim is built from the same data |
| 17 | agg_account_activity_daily | account_id | not_null | No |
| 18 | agg_account_activity_daily | activity_date | not_null | No |
| 19 | fct_transactions | (singular) | assert_positive_amounts | Only if the producer's ranges change |

**Most of these restate rules the database already enforces.** They document the
data contract more than they test it. **Freshness catches the failure that looks
healthy** (a dead streaming job with green tests), but it isn't part of `dbt build`
(9.13).

**Tests check what the data looks like, not whether it's recent.** A mart with
yesterday's numbers still passes all 19.

### 9.13 `dbt build` vs `dbt source freshness`

| Command | Does | Checks |
|---|---|---|
| `dbt run` | Builds models only | n/a |
| `dbt test` | Runs tests only | Data shape |
| **`dbt build`** | Models **and** tests, in dependency order; a failing test stops models downstream of it | Data shape |
| **`dbt source freshness`** | Only the freshness queries | **Time**: is data still arriving? |

`dbt build` runs **5 models + 19 tests = 24 nodes**, which is why its summary
line reads `PASS=24`.

### 9.14 Other files in `pipeline/dbt_project/`

| Path | What | In git? |
|---|---|---|
| `target/` | Compiled SQL, run results | No (gitignored) |
| `logs/` | dbt log files | No (gitignored) |
| `.user.yml` | dbt's anonymous usage-tracking ID | ⚠️ **Not gitignored yet** (see Known issues) |

---
## 10. Airflow

**Files:** `airflow/Dockerfile`, `airflow/dags/findata_dbt_build.py`
**Why it exists:** Spark runs on its own forever, but dbt has to be **started**
each time. Before Airflow, that meant someone remembering to type the command. Airflow
runs it on a schedule, retries failures, and keeps a visible history.

### 10.1 Airflow's parts

| Part | Here | Role |
|---|---|---|
| **DAG** | `findata_dbt_build.py` | A workflow: tasks + their order + a schedule |
| **Task** | `dbt_source_freshness`, `dbt_build` | One step |
| **Operator** | `BashOperator` | The kind of task (here: run a shell command) |
| **Scheduler** | `airflow-scheduler` container | Reads DAG files, decides when runs happen, and (with LocalExecutor) runs the tasks |
| **Executor** | `LocalExecutor` | Runs tasks as subprocesses on the scheduler's machine |
| **Webserver** | `airflow-webserver`, `localhost:8080` | The UI only; runs nothing |
| **Metadata DB** | the `airflow` database | Every DAG run, task state, retry, log pointer, and user |

DAG files are found through the bind mount `./airflow/dags` → `/opt/airflow/dags`.
The scheduler re-reads that folder periodically, so a new or edited DAG shows up
without a restart. **New DAGs start paused by default**; enable them in the UI.

### 10.2 `airflow/Dockerfile`

```dockerfile
FROM apache/airflow:2.10.5-python3.11

RUN python -m venv /home/airflow/dbt_venv \
    && /home/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip \
    && /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
        "dbt-core==1.8.3" \
        "dbt-postgres==1.8.2"
```

**Why a separate virtualenv:** Airflow and dbt-core require **conflicting versions**
of shared libraries (notably `jinja2` and `click`). Installed together, one of them
breaks. A **venv** is an isolated Python environment with its own packages, so
dbt gets its own copy of everything, and the DAG calls it by full path.

```
/home/airflow/.local/...           ← Airflow's packages
/home/airflow/dbt_venv/bin/dbt     ← dbt, with its own jinja2/click
```

**Why `dbt-core` is pinned explicitly:** installing only `dbt-postgres==1.8.2`
made pip pick **`dbt-core 2.0.0rc1`**, an unstable release candidate that also
differs from the `dbt` service's 1.8.3. Pinning both keeps scheduled runs and
manual runs on the same version.

**`&& \`:** `\` continues the command on the next line, and `&&` runs the next
command only if the previous one succeeded. Everything is one `RUN`, so one layer.

**Rejected alternative:** running the existing `dbt` container through DockerOperator
would need the Docker socket mounted into Airflow, which gives it root-level
control of the host just to run a job that only needs a Postgres connection.

### 10.3 `findata_dbt_build.py`

```python
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
    catchup=False,
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

    dbt_source_freshness >> dbt_build >> pipeline_doctor_check
```

| Line | Meaning |
|---|---|
| `DBT_BIN` | Full path into the venv, because a plain `dbt` isn't on Airflow's PATH |
| `DBT_PROJECT_DIR` | Where Compose mounts `./dbt_project` in the Airflow containers |
| `with DAG(...) as dag:` | Tasks created inside the `with` block belong to this DAG |
| `dag_id` | The name shown in the UI |
| `start_date=datetime(2026, 1, 1)` | The earliest date the schedule applies from |
| `schedule="@hourly"` | Run at the start of every hour |
| `catchup=False` | **Don't** go back and run every missed hour since `start_date`. Marts are a full rebuild of current state, so replaying thousands of hours would rebuild the same tables thousands of times. |
| `max_active_runs=1` | Never two runs at once, since two full rebuilds would conflict on the same tables |
| `retries: 2`, `retry_delay: 2 min` | A failing task is retried twice, 2 minutes apart, before it counts as failed. Short blips don't turn the DAG red. |
| `tags` | Labels for filtering in the UI |
| `BashOperator(bash_command=...)` | Run a shell command. **The exit code decides the outcome**: 0 = success, anything else = failure. |
| `cd ... && dbt ... --profiles-dir .` | Go to the project, then run dbt with the `profiles.yml` there |
| `dbt_source_freshness >> dbt_build >> pipeline_doctor_check` | **The dependency chain:** freshness gates the build, and the drift check runs on what the build produced |

**Why freshness gates the build:** if ingestion is dead, rebuilding would publish
the same old numbers with a new build timestamp, and they'd look healthy.
Stopping at step one makes the problem visible.

**If task 1 fails:** after its retries, it's marked `failed`, task 2 is marked
**`upstream_failed`** and never runs, and the DAG run shows red.

**Where the tasks run:** with LocalExecutor, `dbt` runs **inside
`findata-airflow-scheduler`**, connects to Postgres over `findata-net`, and gets
credentials from `.env` via `env_file`.

---

## 11. Batch mode

Both the producer and the Spark job have a one-shot mode.

```bash
docker compose run --rm producer-batch        # send 1000 transactions as fast as possible, exit
docker compose run --rm streaming-job-batch   # process everything in Kafka, exit
```

| | Continuous / stream | Batch |
|---|---|---|
| Producer | 5/sec forever | `PRODUCER_BATCH_SIZE` (1000) messages, no sleep, exit |
| Spark trigger | `processingTime="10 seconds"` | `availableNow=True` |
| Spark first-run start | `latest` | `earliest` |
| Checkpoints | `/tmp/checkpoints/continuous/...` | `/tmp/checkpoints/batch/...` |
| Staging tables | `raw._staging_continuous_*` | `raw._staging_batch_*` |
| Waiting | `awaitAnyTermination()` | `awaitTermination()` on each query |
| Restart policy | `unless-stopped` | none |

**Uses:**
- **Seeding/testing:** put 1000 rows in quickly instead of waiting minutes.
- **Catch-up/backfill:** process a backlog, e.g. after the live job was down.

**Why batch mode doesn't create duplicates when run alongside the live job:** both jobs
may process the same Kafka messages, but the merge's `ON CONFLICT DO NOTHING`
skips rows that already exist.

**Why it has its own checkpoints:** with a shared checkpoint, both jobs would read and
update the same offset record, so each would skip messages the other had already processed.

**On re-runs:** the batch checkpoint persists on the volume, so a second batch run
starts where the last one stopped, not from the beginning of the topic.

⚠️ **The last windows aren't written at the end of a batch run.** The watermark trails
the newest event by 2 minutes, so the final 2–3 minutes of windows are still open when the
run exits. They're kept in the checkpoint and written by the next run that brings newer data.

---

## 12. Failure scenarios: what happens when...

### The streaming job crashes and stays down for 2 hours

1. The producer keeps sending; **Kafka keeps every message** (retention is 7 days).
2. `raw.transactions` stops growing, so `max(ingested_at)` gets older.
3. After 5 minutes, freshness **warns** (the DAG still passes). After 15 minutes it **errors**,
   so the next hourly DAG run fails at task 1, `dbt_build` is `upstream_failed`,
   and **the marts keep their last good data**. The DAG shows red.
4. Spark comes back (Docker restart or manual) → reads its checkpoint → continues from
   its last committed offset → its first batch is large (5/sec × 2h ≈ **36,000 messages**).
5. The next DAG run passes freshness and rebuilds the marts. Nothing was lost.

### Spark crashes after a merge but before the batch is marked done

It reruns the same batch with the same offsets → `ON CONFLICT DO NOTHING` skips everything →
"wrote 0 rows (N already present, skipped)". Nothing breaks (7.7).

### A malformed (non-JSON) message reaches the topic

`from_json` returns an all-NULL row → the `.where(isNotNull)` filter drops it →
the pipeline continues. That row is **silently discarded**, with no log line.

### A message that parses but exceeds a column limit

E.g. `amount = 99999999999.00` (too large for `NUMERIC(12,2)`) → passes the null filter →
the staging insert works (no limits there) → the **merge fails** → the query dies → the
container restarts → the same batch is replayed → it fails again → **permanent crash loop**.
It can't happen with today's producer, but it's a real gap (see Known issues).

### Kafka restarts

The producer's sends fail briefly (logged by `on_send_error`), and Spark's query may
fail and restart. Kafka's messages are on `kafka-data`, so nothing is lost. Everyone
reconnects.

### Postgres restarts

Spark's merge fails → the query dies → the container restarts → the batch is replayed once
Postgres is back. The DAG may fail that hour and retry 2 minutes later.

### The mart shows old numbers, but every test is green

Possible causes:
- **Scheduler down or DAG paused**, so dbt hasn't run
- **Ingestion is dead** and freshness is correctly blocking the build (the DAG is red, but
  someone looking only at the dashboard wouldn't see it)
- **Normal delay:** marts are rebuilt hourly

The 19 tests check what the data looks like, not how recent it is.

### `docker compose down` then `up -d`

Containers are recreated, **volumes are kept**. All data, Kafka messages,
checkpoints, and Airflow history survive. Init SQL does **not** run again.

### `docker compose down -v` then `up -d`

**Everything is wiped:** Postgres data (raw, marts, Airflow history), Kafka
messages, checkpoints. Postgres runs the init SQL again. Spark starts from `latest`.
Jars download again. It's like a brand-new install.

### A raw column is renamed

In dbt, change **one line** in the staging model and keep the old name with an alias. You
also need to change the Spark schema, the Postgres table (`ALTER TABLE`, since init SQL
won't run again), and the producer.

### A new field (`currency`) is added end to end

1. `produce_transactions.py`: add to the dict
2. `streaming_job.py`: add to `TRANSACTION_SCHEMA` (**otherwise silently dropped**)
3. Postgres: `ALTER TABLE raw.transactions ADD COLUMN currency VARCHAR(3);`
   (editing `01_init.sql` does nothing to an existing database)
4. `stg_transactions.sql`: select it
5. `fct_transactions.sql`: select it
6. Optionally, a test in `schema.yml`

### Going from 25 accounts to 25,000

**Load doesn't change**: the rate is still 5/sec. Each account gets a transaction
about every 83 minutes, so nearly every window has `txn_count = 1`, and velocity loses
its meaning. **If you also raise the rate**, the limits are the single Kafka partition and
Spark running in one container in `local[*]` mode.

### `WINDOW_MINUTES` changed from 1 to 5

The math stays consistent (width and divisor change together). But the **existing
checkpoint holds 1-minute window state**. Changing a stateful query's grouping
while reusing its checkpoint isn't safe, so the checkpoint has to be reset,
which then means deciding where to restart reading from.

---

## 13. Pipeline Doctor integration

**Pipeline Doctor** (`pipeline_doctor/`) is the second half of this repo: a
LangGraph agent that profiles data, compares it to a saved baseline, and pauses
for a human when something looks wrong. It is a separate installable package,
so it stays usable against pipelines other than this one.

### What each layer actually checks

```
dbt source freshness  ──►  dbt build  ──►  pipeline_doctor_check
 "is data arriving?"       "is each row     "does this data look like
                             valid?"          normal data?"
```

None of them substitutes for another. A mart can be fresh, pass all 19 tests,
and still have lost half its rows or had a column change type.

### How it reads the mart

Directly, over Postgres:

```bash
pipeline-doctor check --source postgres \
  --table analytics_marts.fct_transactions \
  --name findata_fct_transactions --limit 5000
```

That is the whole integration. There is no export script and no CSV: an earlier
version shelled out to `docker exec findata-postgres psql --csv`, which also
meant it could only run somewhere with a Docker socket — ruling out the Airflow
container it now runs in.

Reading Postgres directly also makes the types **declared** rather than
guessed: `varchar`, `timestamp`, `numeric`. Over CSV, pandas infers types, so a
column that happens to be all-null in one run reads as `float64` and looks like
a type change when nothing changed.

### What it checks for

| Flag kind | Catches |
|---|---|
| `schema_column_removed` | A column that was in the baseline is gone |
| `schema_column_added` | A new column appeared |
| `schema_dtype_changed` | Same column, different type |
| `row_count` | Total rows moved more than the threshold |
| `null_rate` | A column's null rate moved |
| `categorical` | A category distribution shifted (chi-square) |
| `numeric` | A value distribution shifted (two-sample KS) |

A removed or retyped column goes to a human **regardless of severity**: value
distributions can legitimately shift, a column that stopped existing cannot.

### Per-column config

`pipeline_doctor/data/configs/findata_fct_transactions.json` overrides the
role-inference heuristic for this mart:

```json
{
  "categorical": ["account_id", "category", "card_present", "is_large_transaction"],
  "numeric": ["amount"],
  "ignore": ["merchant", "location", "transaction_id"]
}
```

Why it's needed: role inference uses a hardcoded "≤ 20 distinct values" cut for
categorical columns. `account_id` has 25 and is a string, so it was neither
categorical nor numeric — **the dimension this whole pipeline is about was
excluded from drift testing**. `merchant` and `location` go the other way: the
producer generates a fresh Faker value per row, so they are near-unique by
construction and testing their frequencies is pure noise.

### Where its memory lives

| Path in the Airflow container | Volume | Holds |
|---|---|---|
| `/opt/pipeline_doctor/data/baselines` | `pd-baselines` | What "normal" looks like |
| `/opt/pipeline_doctor/data/runs` | `pd-runs` | One JSONL report per run |
| `/opt/pipeline_doctor/checkpoints` | `pd-checkpoints` | Paused runs awaiting review |
| `/opt/pipeline_doctor/data/configs` | bind mount, read-only | The config above |

The volumes are not optional. Without them, every container recreate would
re-bootstrap each baseline from the current run, so there would never be a
prior state to compare against — it would report "no drift" forever and look
healthy while checking nothing.

### Exit codes, and the pending-review problem

| Code | Meaning | Airflow task |
|---|---|---|
| 0 | Clean | green |
| 2 | Drift awaiting human review | **red** |
| 1 | Error, abort, or a human rejected the drift | red |

A paused run used to exit 0, which as a task reads as success: the DAG would go
green while a review sat unanswered forever. Clear a pending review with:

```bash
docker compose exec airflow-scheduler \
  /home/airflow/pd_venv/bin/pipeline-doctor resume <thread_id> <approve|reject>
```

`approve` makes the observed data the new baseline; `reject` leaves the
baseline alone. The `thread_id` is printed in the task log.

### Why a third virtualenv

`airflow/Dockerfile` now builds three Python environments: Airflow's own, one
for dbt, and one for Pipeline Doctor. dbt conflicts with Airflow on
jinja2/click; Pipeline Doctor brings langgraph, langchain and pydantic v2,
which conflict in their own way. Three isolated environments is inelegant and
much less fragile than making one resolver satisfy all three.

## 14. Known issues and open items

Also tracked in `notes.md`.

| Issue | Where | Severity | Notes |
|---|---|---|---|
| **Poison message crash loop** | `streaming_job.py` | High (latent) | A value that parses but breaks a column limit crashes the job forever. Fix: validation + dead-letter table. |
| `SPARK_TRIGGER_MODE="continuous"` misnamed | `streaming_job.py`, `.env` | Low | Should be `stream`. Renaming changes checkpoint paths, so move the checkpoint folder as part of the change. |
| `run_batch` prints "batch complete" after an interrupt | `produce_transactions.py` | Low | Move the print inside the `try` |
| Final windows unwritten until newer data arrives | Spark watermark | Info | Expected; document it in the README |
| Money as `DoubleType` | `TRANSACTION_SCHEMA` | Low | Use `DecimalType(12, 2)` |
| `avg_amount` has no consumer | Spark → raw → staging | Low | Keep or drop throughout |
| Index `idx_transactions_account_time` unused | `01_init.sql` | Low | Only helps ad-hoc queries |
| No late events ever produced | `produce_transactions.py` | Realism | The watermark is never exercised |
| Producer default `localhost:9092` out of date | `produce_transactions.py` | Low | From the host it's `localhost:29092`; doesn't matter inside Docker |
| ~~`.user.yml` not gitignored~~ | `.gitignore` | **fixed** | covered by the merged root `.gitignore` |
| Two dbt runners share `target/` | Compose mounts | Low | Overlapping runs could overwrite each other's artifacts |
| Two dbt version pins must stay aligned | `airflow/Dockerfile` vs `dbt` service | Maintenance | |
| ~~No `.env.example`~~ | repo root | **fixed** | created in Phase 0, secrets placeholdered |
| ~~Not a git repo~~ | repo root | **fixed** | `git init` + first commit |
| Single Kafka partition, Spark `local[*]` | By design | Demo-scale | See the README's known limitations |
| ~~`kafka-data` mounted where Kafka never writes~~ | `docker-compose.yml` | **fixed** | was `/tmp/kraft-combined-logs`, actually `/tmp/kafka-logs`; cost the topic's history to discover |
| ~~Pipeline Doctor state volumes root-owned~~ | `airflow/Dockerfile` | **fixed** | dirs created + chowned in the image, so empty volumes inherit the ownership |

---

## 15. Command cheat sheet

```bash
# ── Stack ───────────────────────────────────────────────────────────────
docker compose up -d                          # start everything (not the "tools")
docker compose ps                             # status of this project's services
docker ps -a                                  # all containers, including exited ones
docker compose logs -f streaming-job          # follow Spark's output
docker compose restart streaming-job          # pick up edits to streaming_job.py
docker compose down                           # stop; KEEP data
docker compose down -v                        # stop; DELETE ALL DATA

# ── Batch mode ──────────────────────────────────────────────────────────
docker compose run --rm producer-batch
docker compose run --rm streaming-job-batch

# ── dbt by hand ─────────────────────────────────────────────────────────
docker compose run --rm dbt build --profiles-dir .
docker compose run --rm dbt test --profiles-dir .
docker compose run --rm dbt source freshness --profiles-dir .
docker compose run --rm dbt run --select fct_transactions --profiles-dir .

# ── Postgres ────────────────────────────────────────────────────────────
docker exec -it findata-postgres psql -U findata -d findata
docker exec findata-postgres psql -U findata -d findata -c "select count(*) from raw.transactions;"
docker exec findata-postgres psql -U findata -d findata \
  -c "select * from analytics_marts.agg_account_activity_daily order by activity_date desc limit 10;"

# ── Kafka ───────────────────────────────────────────────────────────────
docker exec findata-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic transactions --from-beginning --max-messages 5
docker exec findata-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic transactions

# ── UIs ─────────────────────────────────────────────────────────────────
# Airflow:  http://localhost:8080   (AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD from .env)
# Adminer:  http://localhost:8081   (server: postgres, credentials from .env)
```

---

## 16. Glossary

| Term | Meaning |
|---|---|
| **Aggregation** | Combining many rows into one (count, sum, avg) |
| **Anchor (YAML)** | `&name` defines a reusable block; `<<: *name` merges it in |
| **Append mode** | Spark output mode: each result row is emitted once, when final |
| **Autocommit** | Each SQL statement is saved immediately, with no explicit `commit()` |
| **Bind mount** | A folder from your machine made visible inside a container |
| **Broker** | A Kafka server that stores messages |
| **Checkpoint** | Spark's saved progress: offsets, commits, window state |
| **Composite key** | A primary key made of more than one column |
| **Consumer** | A program that reads from Kafka |
| **Container** | A running instance of an image |
| **DAG** | Directed Acyclic Graph; in Airflow, a workflow of ordered tasks |
| **Dimension table** | One row per thing (account), with its attributes |
| **Drift** | Data changing from its usual pattern |
| **Executor** | Airflow component that runs tasks (LocalExecutor = subprocesses) |
| **Fact table** | One row per event (transaction) |
| **foreachBatch** | Spark hook that calls your function once per micro-batch |
| **Freshness** | How recent the newest data is |
| **Future** | A placeholder for a result that isn't ready yet |
| **Idempotent** | Running it twice has the same effect as running it once |
| **Image** | A packaged template that containers are started from |
| **Index** | A lookup structure that speeds up filtered queries |
| **JDBC** | Java's standard database connection interface |
| **Jinja** | Template language dbt uses: `{{ ref(...) }}` |
| **JVM** | Java Virtual Machine; where Spark actually runs |
| **KRaft** | Kafka mode that coordinates itself, without ZooKeeper |
| **Lazy evaluation** | Build a plan now, compute only when an action requires it |
| **Layer (Docker)** | A cached step of an image build |
| **Listener (Kafka)** | A port Kafka accepts connections on |
| **Mart** | A report-ready dbt table |
| **Materialization** | How dbt stores a model: view, table, ... |
| **Micro-batch** | A group of messages Spark processes together, per trigger |
| **Named volume** | Docker-managed persistent storage |
| **Offset** | A message's position number in a Kafka partition |
| **Partition** | An ordered sub-log of a Kafka topic |
| **persist()** | Cache a DataFrame's computed result in memory |
| **Primary key** | Column(s) that uniquely identify a row; unique + not null |
| **Producer** | A program that writes to Kafka |
| **Profile (Compose)** | A label that excludes a service from `up` |
| **py4j** | Library connecting PySpark's Python to Spark's JVM |
| **Schema (Postgres)** | A folder of tables within a database |
| **Schema (Spark)** | The column names and types expected in data |
| **Serializer** | Converts objects to bytes (and a deserializer converts back) |
| **SIGINT / SIGTERM** | Stop signals: Ctrl+C / `docker stop` |
| **Source (dbt)** | A table dbt didn't create |
| **Staging (dbt)** | Light-cleanup views, 1:1 with sources |
| **Staging table (Spark)** | Temporary table that holds a batch before the merge |
| **State (Spark)** | Running aggregates kept across micro-batches |
| **Struct** | A column containing named sub-fields |
| **Topic** | A named stream of messages in Kafka |
| **Trigger** | When Spark runs a micro-batch |
| **Tumbling window** | Fixed, non-overlapping time buckets (14:23–14:24, 14:24–14:25, ...) |
| **UUID** | A random, practically unique 128-bit ID |
| **venv** | An isolated Python environment with its own packages |
| **View** | A saved query; always shows current data |
| **Watermark** | "Latest event time − 2 min"; windows ending before it are final |

---

## 17. Self-test questions

Try to answer each one out loud before opening the answer.

<details>
<summary>1. Why is Kafka between the producer and Spark?</summary>

**Decoupling:** the producer doesn't depend on Spark being up. **Buffering:** Kafka keeps
messages while Spark is down. **Replay:** consumers track offsets and can re-read,
which is how batch mode works. **Fan-out:** several independent readers of one topic.
</details>

<details>
<summary>2. Why are there two Spark queries?</summary>

One saves every raw transaction; the other computes per-account, per-minute windows.
They have different outputs, different write patterns, and separate checkpoints. They
also read Kafka independently.
</details>

<details>
<summary>3. What does <code>persist()</code> prevent?</summary>

Recomputing the batch. A DataFrame is a plan, so `count()` and `write` would each
read Kafka and parse the JSON again.
</details>

<details>
<summary>4. Spark crashes after the merge but before the commit. What happens?</summary>

On restart it finds `offsets/N` without `commits/N` and reruns batch N with the same
offsets. The merge's `ON CONFLICT DO NOTHING` skips every row, so no crash and no duplicates.
</details>

<details>
<summary>5. Why must the window query use <code>append</code> mode with this merge?</summary>

`append` emits each window once, when final. `DO NOTHING` keeps the first version it
sees, so that has to be the final version. With `update`, the first incomplete version
would be stored and later corrections silently skipped.
</details>

<details>
<summary>6. What does the watermark control, and what's its side effect?</summary>

How late an event may arrive and still be counted. Windows ending before `latest event − 2 min`
are finalized and their state freed. Side effect: the last few minutes of windows aren't
written until newer data advances the watermark.
</details>

<details>
<summary>7. When does <code>01_init.sql</code> run?</summary>

Only when Postgres starts with an **empty** data volume: first boot, or after `down -v`.
</details>

<details>
<summary>8. Why <code>ingested_at</code> and not <code>event_time</code> for freshness?</summary>

Freshness asks whether data is still arriving, and `ingested_at` records when each row
arrived. `event_time` records when the transaction happened, which can differ (late events,
replays, backfills).
</details>

<details>
<summary>9. What is a dbt test, physically?</summary>

A SELECT that returns bad rows. Zero rows = pass.
</details>

<details>
<summary>10. <code>source()</code> vs <code>ref()</code>?</summary>

`source()` points at tables dbt didn't create (raw), and is used only in staging.
`ref()` points at other dbt models, and is how dbt works out build order.
</details>

<details>
<summary>11. A new raw row lands. Is it visible in <code>stg_transactions</code>? In <code>fct_transactions</code>?</summary>

Staging: yes, immediately (view). Fact: no, not until the next `dbt build` (table).
</details>

<details>
<summary>12. Why does the DAG run freshness before build?</summary>

If ingestion is dead, building would republish stale numbers that look healthy. Stopping
at freshness makes the failure visible (`dbt_build` becomes `upstream_failed`).
</details>

<details>
<summary>13. Why does dbt live in its own venv inside the Airflow image?</summary>

Airflow and dbt-core need conflicting versions of jinja2/click. The venv isolates them,
and the DAG calls dbt by full path.
</details>

<details>
<summary>14. Why does <code>docker ps</code> show 7 containers when 11 services are defined?</summary>

`dbt`, `producer-batch`, and `streaming-job-batch` are in the `tools` profile, so `up`
skips them. `airflow-init` runs once and exits (visible with `docker ps -a`).
</details>

<details>
<summary>15. <code>down</code> vs <code>down -v</code>?</summary>

`down` removes containers and keeps volumes, so data survives. `down -v` also deletes
volumes: Postgres data, Kafka messages, checkpoints, all gone.
</details>

<details>
<summary>16. Why can't a laptop script use <code>kafka:9092</code>?</summary>

`kafka` only resolves inside the Docker network. Kafka also tells clients which
address to reconnect to. Laptop clients use the EXTERNAL listener, `localhost:29092`.
</details>

<details>
<summary>17. Why is <code>requirements.txt</code> copied before the <code>.py</code> in the Dockerfile?</summary>

Layer caching. Code changes often and dependencies rarely, so code edits reuse the slow
`pip install` layer.
</details>

<details>
<summary>18. Why does the producer need a SIGTERM handler?</summary>

`docker compose down` sends SIGTERM, and Python's default is to exit immediately,
skipping `finally`, so queued messages are lost. The handler converts it to
`KeyboardInterrupt`, which triggers the clean flush.
</details>

<details>
<summary>19. Why is <code>avg(avg_amount)</code> wrong for a daily average?</summary>

It weights a minute with 1 transaction the same as a minute with 40. The correct value
is `sum(total_amount) / sum(txn_count)`.
</details>

<details>
<summary>20. How does the pipeline recover from a crash with no retry code?</summary>

The query dies → `awaitAnyTermination` raises → the process exits → `restart: unless-stopped`
restarts the container → Spark resumes from its checkpoints → any replayed batch is
made harmless by the merge.
</details>
