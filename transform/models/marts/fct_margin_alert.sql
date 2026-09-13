-- Factless/accumulating fact, the README's own "margin alerts" model (spec
-- 05 §8). 1 row per decision where the cost floor pushed the price above
-- the raw market average (ADR-0007's rule, renamed 2026-09-12 from
-- "cost_protected" to "minimum_profitable_price" for clarity). Iceberg is
-- an append-only audit trail, never rewritten in place, so decisions made
-- before the rename still carry the old literal. Matching on both values
-- is what keeps every historical margin alert visible, not just
-- post-rename ones (found live: 2179 pre-rename rows would otherwise
-- silently vanish from this mart).
select *
from {{ ref('fct_price_decision') }}
where rule_applied in ('minimum_profitable_price', 'cost_protected')
