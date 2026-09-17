# Real-Time Financial Data Streaming Pipeline

A real-time pipeline that simulates card transactions, streams them through
Kafka, computes stateful windowed aggregations in Spark, lands everything in
Postgres, and transforms it into BI-ready tables with dbt — all orchestrated
by Docker Compose.

```
producer (Python/Faker) --> Kafka --> Spark Structured Streaming --> Postgres (raw)
                                                                          |
                                                                          v
                                                                  dbt (staging + marts)
```

## Repository layout

```
findata-platform/
├── docker-compose.yml     the whole stack (11 services)
├── .env / .env.example    shared settings for every service
├── pipeline/              the streaming pipeline
│   ├── producer/          Kafka producer (simulated transactions)
│   ├── spark_jobs/        Spark Structured Streaming job
│   ├── postgres_init/     schema created on first boot
│   └── dbt_project/       staging views + marts + tests
├── airflow/               image (dbt in its own venv) + DAGs
├── pipeline_doctor/       data-drift monitoring (own installable package)
├── plan.md                roadmap for integrating the two
└── exp.md                 detailed explanation of every part
```

## Components

| Layer                 | Tech                              | What it does                                                             |
|------------------------|------------------------------------|---------------------------------------------------------------------------|
| `pipeline/producer/`            | Python, Faker, kafka-python       | Simulates card transactions across 25 accounts, publishes JSON to Kafka   |
| Kafka                  | Apache Kafka 3.7 (KRaft, no ZK)   | `transactions` topic, keyed by `account_id`                              |
| `pipeline/spark_jobs/`          | PySpark Structured Streaming      | Raw sink to Postgres + 1-minute tumbling window aggregation (velocity, avg transaction size) per account, with a 2-minute watermark |
| Postgres               | PostgreSQL 16                     | `raw.transactions`, `raw.account_activity_windows`                       |
| `pipeline/dbt_project/`         | dbt-postgres 1.8                  | `staging` views -> `marts` tables (BI-ready facts/dims) with schema + custom tests |
| `airflow/`             | Airflow 2.10 (LocalExecutor)      | Hourly scheduled `dbt source freshness` -> `dbt build`; UI at `localhost:8080` |
| Adminer                | -                                  | Web UI to browse Postgres at `localhost:8081`                            |

## Running it

```bash
docker compose up -d          # kafka, postgres, adminer, producer, streaming-job, airflow
docker compose run --rm dbt build   # ad-hoc dbt run (Airflow also does this hourly)
```

**Airflow UI**: http://localhost:8080 — credentials from `AIRFLOW_ADMIN_USER` /
`AIRFLOW_ADMIN_PASSWORD` in `.env` (`admin` / `admin` by default). The
`findata_dbt_build` DAG runs hourly and is what turns "remember to run dbt"
into a scheduled, retried, observable job.

Airflow deliberately does **not** orchestrate the Spark streaming job: that's a
long-running service scheduling its own 10-second micro-batches, so there is no
"when should this run" for an orchestrator to decide. It schedules the dbt
rebuild, which is the only genuinely recurring task here.

Everything is reproducible from a clean state — `docker compose down -v && docker compose up -d`
recreates the full stack, including Kafka topics and the Postgres schema
(auto-applied from `pipeline/postgres_init/01_init.sql` on first boot).

Give the streaming job a few minutes on first run: it downloads the Kafka/Postgres
JDBC connector jars via `--packages`, and the first windowed aggregation batch
only appears once a 1-minute window closes past its 2-minute watermark
(~3-4 minutes after startup).

### Batch mode

The producer and streaming job can each run in a one-shot batch mode instead
of running forever — useful for seeding/testing quickly without waiting on
the live stream:

```bash
docker compose run --rm producer-batch        # sends PRODUCER_BATCH_SIZE (default 1000) messages, then exits
docker compose run --rm streaming-job-batch   # processes everything currently in Kafka, then exits
```

`streaming-job-batch` reads from Kafka's earliest offset and uses its own
checkpoint directory, so it never interferes with the continuous job's offset
tracking — run it any time to catch up on a backlog (e.g. after
`producer-batch`, or after the continuous job was down for a while). Its
checkpoint persists across runs, so re-running it only picks up backlog since
the last time it ran, not everything since the beginning of the topic.

## Poking around

- **Adminer**: http://localhost:8081 — server `postgres`, user/pass/db from `.env`
- **Raw Kafka messages** (from inside the `kafka` container):
  ```bash
  docker exec findata-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic transactions --from-beginning --max-messages 5
  ```
- **Raw Kafka messages from your host machine** (e.g. a local Python script, not `docker exec`):
  use `localhost:29092` — the `EXTERNAL` listener. `localhost:9092` won't work from the
  host: Kafka advertises `kafka:9092` for that listener, which only resolves inside the
  `findata-net` Docker network.
- **Query Postgres directly**:
  ```bash
  docker exec findata-postgres psql -U findata -d findata \
    -c "select * from analytics_marts.agg_account_activity_daily order by activity_date desc limit 10;"
  ```
- **dbt docs**: `docker compose run --rm dbt docs generate --profiles-dir .`

## Data model

**Raw (written by Spark):**
- `raw.transactions` — one row per transaction event
- `raw.account_activity_windows` — one row per account per 1-minute window: `txn_count`, `total_amount`, `avg_amount`, `velocity_per_min`

**dbt staging** (`analytics_staging` schema): `stg_transactions`, `stg_account_activity_windows` — type-cleaned views over the raw tables.

**dbt marts** (`analytics_marts` schema):
- `dim_accounts` — one row per account, lifetime stats
- `fct_transactions` — BI-ready transaction fact table, with an `is_large_transaction` flag
- `agg_account_activity_daily` — daily rollup of Spark's per-minute velocity/avg-size windows

19 dbt tests cover uniqueness/not-null on keys, `accepted_values` on transaction category,
a `relationships` test tying facts to `dim_accounts`, and a custom singular test asserting
no transaction amount is non-positive.

Note that most of those 19 are backed by database constraints upstream (the raw tables have
primary keys), so they document the data contract more than they stress it. The test that
can genuinely fail is **`dbt source freshness`** — it errors when the streaming job stops
landing rows for 15 minutes, which is the failure mode that otherwise looks like a healthy
stack quietly ingesting nothing. It runs as the gating first task of the Airflow DAG.

## Known limitations (by design, for a demo pipeline)

- Spark runs in `local[*]` mode inside a single container rather than a multi-node cluster — sufficient to demonstrate stateful windowed streaming without the operational overhead of a standalone cluster.
- The `transactions` Kafka topic auto-creates with a single partition, so Spark's `local[*]` parallelism has nothing to actually parallelize across at this data volume — acceptable for a demo, would need an explicitly multi-partitioned topic to matter at real scale.

Both raw sinks (`write_raw_batch`/`write_window_batch` in `pipeline/spark_jobs/streaming_job.py`) stage each micro-batch via Spark's normal JDBC writer, then merge it into the target table with `INSERT ... ON CONFLICT DO NOTHING` over a raw JDBC connection — so a Spark task retry that replays an already-committed micro-batch is a safe no-op instead of a duplicate-key crash.
