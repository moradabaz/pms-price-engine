-- Factless/accumulating fact — the README's own "margin alerts" model (spec
-- 05 §8): 1 row per decision where the cost floor pushed the price above the
-- raw market average (ADR-0007's rule, renamed 2026-09-12 from
-- "cost_protected" to "minimum_profitable_price" for clarity).
select *
from {{ ref('fct_price_decision') }}
where rule_applied = 'minimum_profitable_price'
