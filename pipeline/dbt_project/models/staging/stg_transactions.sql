-- Type-cleaned pass-through over the raw landing table.
--
-- There is deliberately no dedupe step here. transaction_id is the PRIMARY KEY
-- of raw.transactions, and the Spark sink merges each micro-batch with
-- INSERT ... ON CONFLICT (transaction_id) DO NOTHING, so a duplicate cannot
-- physically reach this model. A row_number() dedupe used to sit here citing
-- "foreachBatch retries can duplicate rows" - that rationale was never true
-- given the PK, and is doubly untrue now. If duplicates ever do appear, the
-- unique test on transaction_id is what should catch it, loudly, rather than a
-- window function quietly hiding it on every build.
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
