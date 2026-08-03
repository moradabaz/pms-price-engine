# Post-PoC roadmap — deferred from ADR-0009

**Date:** 2026-08-03

Two items from the stakeholders' profitability model are **explicitly deferred until after the PoC ships**, to keep Phase 4's reform (ADR-0009) scoped to what's cheap: fixing the floor formula, adding commissions, splitting cost by category, and the antelación-tiered dual floor. Not silently dropped — tracked here so they get picked up as real follow-up work.

## 1. Real stay-length pricing (`n` beyond the fixed `1`)

Price a whole candidate stay (several consecutive nights), not one isolated night — so a one-time cost (`Cr`, e.g. cleaning) amortizes correctly across the stay.

**Why deferred:** no phase of this project models an actual reservation with a length. Doing this for real means:
- `price_decision` keyed by `(apartment_id, start_date, stay_length)` instead of `(apartment_id, target_date)` — a primary-key change in DynamoDB (spec §10) and in what Phase 6's dashboard reads.
- Multiple candidate stay lengths (1, 2, 3, 7, 14 nights) evaluated per apartment/night → multiplies Stage B's emission volume.
- No real occupancy/availability calendar exists anywhere (already a known limitation, spec §14) — "possible stay" can't be validated against real availability.

## 2. Per-channel pricing (direct / Airbnb / Booking / Vrbo)

Separate floor and commission per sales channel, instead of one blended `Cp`.

**Why deferred:**
- `market-ingestor` (Phase 3) produces one blended average rate, no channel dimension — needs a Phase 3 change to ingest/derive per-channel rates.
- Apartment config would need per-channel commission, not a single `commission_pct` — a Phase 1 schema change.
- Multiplies Stage B's fan-out again, this time by number of channels.

## When to pick these up

After the PoC's current reform (ADR-0009) ships and is verified live. Whoever starts this should read ADR-0009's "Alternatives considered and rejected" section first — the reasoning for deferring is there, not just the fact of it.
