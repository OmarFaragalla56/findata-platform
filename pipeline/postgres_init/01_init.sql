-- Raw landing zone written to by the Spark Structured Streaming job.
-- dbt staging models read from this schema and never mutate it.
CREATE SCHEMA IF NOT EXISTS raw;

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

CREATE INDEX IF NOT EXISTS idx_transactions_account_time
    ON raw.transactions (account_id, event_time);

-- Rolling, windowed aggregates computed by Spark's stateful streaming job.
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
