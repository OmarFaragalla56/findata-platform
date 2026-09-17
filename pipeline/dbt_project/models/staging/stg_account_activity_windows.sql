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
