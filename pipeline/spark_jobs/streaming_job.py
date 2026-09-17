"""Structured Streaming job: reads the simulated transaction feed from Kafka,
persists the raw events to Postgres, and maintains a stateful windowed
aggregation (per-account velocity + rolling average transaction size) that is
continuously flushed to a second Postgres table.

Two independent streaming queries read the same Kafka topic. They share the
`parsed` DataFrame definition but not an execution: each .start() below
creates its own StreamingQuery with its own Kafka consumer and its own
checkpoint, so the topic is genuinely read twice and the two queries track
their offsets separately.

The two sinks:
  1. raw sink       -> raw.transactions            (append, one row per event)
  2. windowed sink   -> raw.account_activity_windows (append, one row per
                          closed 1-minute window per account)

Windowing uses event-time watermarking so late-arriving events are handled
correctly and state is dropped once a window can no longer be updated.

Both sinks write via a stage-then-merge upsert rather than a plain append:
each micro-batch lands in a per-query staging table via Spark's normal JDBC
writer (so all the type mapping stays exactly as tested), then a single
`INSERT ... ON CONFLICT DO NOTHING` moves it into the target table. This
makes a Spark task retry that replays an already-committed micro-batch a
safe no-op instead of a duplicate-key crash.

Two trigger modes, controlled by SPARK_TRIGGER_MODE:
  - "continuous" (default): runs forever, micro-batches every 10 seconds,
    starting from the latest Kafka offset. This is the live pipeline.
  - "batch": processes whatever is currently available in Kafka (starting
    from the earliest offset) and then exits — a one-shot catch-up/backfill
    run, using Structured Streaming's trigger(availableNow=True). Uses its
    own checkpoint directories so it never shares offset state with the
    continuous job.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, sum as _sum, avg
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, TimestampType
)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "transactions")

WINDOW_MINUTES = 1  # single source of truth: used both to build the tumbling
                     # window and to convert txn_count into a per-minute rate

TRIGGER_MODE = os.environ.get("SPARK_TRIGGER_MODE", "continuous").lower()  # "continuous" | "batch"
IS_BATCH = TRIGGER_MODE == "batch"
# batch runs need their own checkpoint dirs so a one-off catch-up run never
# shares (and can't corrupt) the continuous job's offset tracking.
CHECKPOINT_ROOT = f"/tmp/checkpoints/{TRIGGER_MODE}"

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

# Per-trigger-mode staging table names, so a continuous run and a batch
# catch-up run (which could in principle overlap) never share - and can't
# corrupt - each other's in-flight staging data.
RAW_TXN_STAGING_TABLE = f"raw._staging_{TRIGGER_MODE}_transactions"
WINDOW_STAGING_TABLE = f"raw._staging_{TRIGGER_MODE}_account_activity_windows"


def _merge_staging_into(spark, staging_table, target_table, conflict_columns, select_columns):
    """Move rows from a staging table into target_table, skipping any row
    whose conflict key already exists. Runs over a plain JDBC connection
    (via the JVM gateway - no new Python dependency needed, since the
    Postgres driver jar is already loaded via --packages) because Spark's
    own DataFrameWriter has no ON CONFLICT support.

    Returns the number of rows actually inserted, which can be less than
    what's in staging if some were already present from a prior attempt.
    """
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


def write_raw_batch(batch_df, batch_id):
    # persist first, then let count() double as the empty check - an
    # .rdd.isEmpty() guard ahead of persist() would compute the batch an
    # extra time (and force a DataFrame->RDD conversion) to learn something
    # count() is about to tell us anyway.
    batch_df.persist()
    try:
        attempted = batch_df.count()
        if attempted == 0:
            return
        # stage via Spark's normal JDBC writer (mode="overwrite" drops and
        # recreates the staging table from batch_df's own schema each time,
        # so type mapping is exactly what was already tested) ...
        (
            batch_df.write
            .mode("overwrite")
            .jdbc(JDBC_URL, RAW_TXN_STAGING_TABLE, properties=JDBC_PROPS)
        )
        # ... then merge staging -> target, skipping rows already committed
        # from a prior attempt at this same micro-batch (task retry).
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


def write_window_batch(batch_df, batch_id):
    # only `out` is ever acted on below, so persisting batch_df as well would
    # cache a dataset that's read exactly once - `out`'s cache is the only
    # one that earns its keep.
    out = (
        batch_df
        .select(
            col("account_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("txn_count"),
            col("total_amount"),
            col("avg_amount"),
            # per-minute rate derived from the window width rather than a
            # hardcoded divisor, so it stays correct if WINDOW_MINUTES changes.
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


def main():
    spark = (
        SparkSession.builder
        .appName("financial-transaction-streaming")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # batch mode backfills everything sitting in Kafka; continuous mode only
    # ever wants new events (and after the first run, the checkpoint takes
    # over startingOffsets anyway).
    starting_offsets = "earliest" if IS_BATCH else "latest"

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", starting_offsets)
        .load()
    )

    parsed = (
        raw_stream
        .select(from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"))
        .select("data.*")
        # from_json silently turns an unparseable payload into an all-NULL
        # row; without this filter that row reaches the JDBC write, violates
        # NOT NULL/PRIMARY KEY in raw.transactions, and crashes the query.
        .where(
            col("transaction_id").isNotNull()
            & col("account_id").isNotNull()
            & col("event_time").isNotNull()
            & col("amount").isNotNull()
        )
        .withWatermark("event_time", "2 minutes")
    )

    # batch mode: process everything currently available, then stop.
    # continuous mode: keep polling Kafka every 10 seconds, forever.
    trigger_kwargs = {"availableNow": True} if IS_BATCH else {"processingTime": "10 seconds"}

    # --- Query 1: raw event sink ---
    raw_query = (
        parsed.writeStream
        .foreachBatch(write_raw_batch)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/raw_transactions")
        .trigger(**trigger_kwargs)
        .start()
    )

    # --- Query 2: stateful windowed aggregation (velocity + avg txn size) ---
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

    if IS_BATCH:
        # availableNow queries terminate on their own once caught up; wait
        # for both explicitly instead of awaitAnyTermination(), which would
        # return as soon as the first of the two finishes.
        for query in (raw_query, window_query):
            query.awaitTermination()
        print("[streaming-job] batch mode: all available data processed, exiting")
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
