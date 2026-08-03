# ADR-0009 — Profitability floor reform

**Date:** 2026-08-03
**Status:** Accepted (2026-08-03) — `commission_pct` default `0.15` and `reduced_margin_factor` `0.75` confirmed. Stay-length (D5) and channel (D6) deferred post-PoC, see `docs/post-poc-roadmap.md`.

## Context — 4 gaps vs. the stakeholder model

1. **Our formula has the exact "common error" the stakeholders' own infographic warns about.** `pricing.py:25` does `daily_cost * (1 + margin)` (multiply). Correct is `daily_cost / (1 - margin)` (divide) — multiplying understates real margin.
2. **Commissions (`Cp`) don't exist anywhere.** Must go in the denominator, next to margin — it scales with final price.
3. **We already receive `cost_type` (`fixed`/`variable`/`one_time`) per cost line** (`payment_line.py:34`) but `cost_aggregation.py` sums everything into one blended `daily_cost_eur`, discarding it.
4. **Only one floor exists.** The model wants two: structural (far-out bookings) vs. contribution (imminent, sunk-cost-aware).

## Decision

**D1 — Formula, division-based:**
```
structural_floor   = ((n×Cf) + (n×Cv) + Cr) / (1 - M_effective - Cp)
contribution_floor = ((n×Cv) + Cr) / (1 - Cp)          # no Cf: it's sunk regardless
```

**D2 — `Cp` (commission):** new per-apartment field, same place as `target_margin`. **Default `0.15`.**

**D3 — Cost split, from existing `cost_type`:**
- `Cf` (fixed/day) = sum of `fixed` lines ÷ available_days
- `Cv` (variable/night) = sum of `variable` lines ÷ available_days
- `Cr` (one-time/reservation) = **average** (not sum) of `one_time` lines — each is already "cost of one turnover," summing a month of them would overstate it.

**D4 — Antelación tiers:**

| Días hasta la fecha | Suelo | Margen |
|---|---|---|
| > 30 | Estructural | margen completo |
| 15–30 | Estructural | margen × `reduced_margin_factor`. **Default `0.75`.** |
| 7–14 | Contribución | — |
| 0–3 | Contribución | — (misma fórmula que 7–14; la fuente no da otra) |

`rule_applied` (ADR-0007) no cambia de forma — solo cambia qué suelo compara. `calculation` gana `floor_type` y `days_to_arrival`.

**D5 — Duración de estancia (`n`): fija en `1`.** No hay ningún concepto de "reserva con noches" en Fases 1-3 — modelarlo de verdad es un proyecto aparte. `n=1` es la opción conservadora (nunca infra-cubre `Cr`).

**D6 — Canal: fuera de alcance.** Un único `Cp` por apartamento, no por canal (Airbnb/Booking/directo).

## Por qué

- D1 es el bug real que el propio material de los stakeholders señala como error común.
- D3: `Cr` ya es un coste "por evento" — sumarlo por periodo lo confundiría con el total de todos los turnovers del mes.
- D4 reutiliza tal cual la tabla de tramos del documento de los stakeholders.
- D5/D6 rechazados como "hacerlo bien ahora" porque no existe el dato base (reservas con noches, mercado por canal) en ninguna fase anterior — quedan como trabajo futuro, no como hueco silencioso.

## Consecuencias

- `pricing.py`, `cost_aggregation.py`, `models.py` (`CostAggregate`, `SegmentAssignment`): cambian de forma para llevar `Cf`/`Cv`/`Cr`/`Cp`.
- `apartment_market_segments` (Fase 1): nueva columna `commission_pct`.
- `price_decision.v1.json`: `daily_cost_eur` → `Cf`/`Cv`/`Cr`; `calculation` gana `commission_pct`, `floor_type`, `days_to_arrival`. Sin bump de `schema_version` (Fase 4 aún sin consumidor real, igual que ADR-0007).
- `spec.md` §8 y los fixtures de `price_decision` se reescriben una vez aceptado esto.
- Fuera de alcance, explícito: estancia real multi-noche y precio por canal — necesitan datos que no existen hoy en ninguna fase.
