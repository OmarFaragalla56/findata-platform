-- Fails if any fact row has a non-positive amount, which would indicate a
-- bad event slipped past the simulator or a bug in upstream cleaning logic.
select *
from {{ ref('fct_transactions') }}
where amount <= 0
