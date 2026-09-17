select
    account_id,
    min(event_time) as first_seen_at,
    max(event_time) as last_seen_at,
    count(*) as lifetime_txn_count,
    sum(amount) as lifetime_total_amount,
    round(avg(amount), 2) as lifetime_avg_amount

from {{ ref('stg_transactions') }}
group by account_id
