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
