-- "Last known decision per (apartment, target_date, day)" (spec 05 §7) —
-- if Stage B re-decides the same apartment/night more than once on the same
-- calendar day, only the latest one as of that day survives. This is what a
-- price-evolution chart actually plots, without the noise of every
-- recalculation fct_price_decision (the transaction-grain fact) keeps.
with ranked as (
    select
        *,
        cast(decided_at as date) as snapshot_day,
        row_number() over (
            partition by apartment_id, target_date, cast(decided_at as date)
            order by decided_at desc
        ) as rn
    from {{ ref('stg_price_decision') }}
)
select * exclude (rn)
from ranked
where rn = 1
