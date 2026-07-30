# Found in review: cost-side fan-out doesn't filter expired nights

**Phase:** 4 | **Component:** `stage_price_decision.py`, `process_element1`

## What

`process_element2` (market side) evicts `nights` entries whose `target_date < today` on every market event (spec §6). `process_element1` (cost side) does not — it fans out over every entry currently in `self.nights`, including ones that aged into the past since their last market update. If a cost update lands for a segment that hasn't received a market tick since one of its nights expired, that night gets re-priced and emitted with a `target_date` in the past.

## Why it hasn't bitten in practice

`market-ingestor`'s cyclic tick (D.1) hits all 18 segments continuously, so a segment's nights are swept almost as soon as they expire. The gap is real but currently invisible under this demo's load profile.

## Not fixed here

Fixing it means deciding whether cost-side fan-out should also sweep expired nights (extra work per cost event) or just skip yielding for past dates (cheap, but leaves stale entries in state). Both are one-line changes but change `process_element1`'s emission count, which `AC-03` explicitly pins ("exactly N events, one per known night") — left for the next spec revision rather than changed silently.
