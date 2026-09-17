-- Daily rollup of Spark's per-minute velocity/avg-size windows, giving BI
-- tools a stable grain (one row per account per day) instead of the raw
-- streaming grain (one row per account per minute).
select
    account_id,
    -- ::date because date_trunc returns a midnight timestamp, and this column
    -- is the day grain, not an instant.
    date_trunc('day', window_start)::date as activity_date,
    sum(txn_count) as total_txn_count,
    sum(total_amount) as total_amount,
    -- avg(avg_amount) would be an unweighted average of per-minute averages,
    -- so a minute with 1 transaction counts as much as a minute with 40.
    -- Weight by txn_count instead: total spend / total transactions.
    round(sum(total_amount) / nullif(sum(txn_count), 0), 2) as avg_transaction_size,
    -- NOTE on both velocity columns: Spark only emits a window row for
    -- minutes in which an account actually transacted, so these are per
    -- ACTIVE minute, not per wall-clock minute. An account idle for 23 hours
    -- with one busy minute reports that busy minute's rate, not a rate
    -- averaged across the day. Unweighted avg() is correct here (unlike
    -- avg_transaction_size above) because every window is the same width.
    max(velocity_per_min) as peak_velocity_per_min,
    round(avg(velocity_per_min), 2) as avg_velocity_per_active_min

from {{ ref('stg_account_activity_windows') }}
group by 1, 2
