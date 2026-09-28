# Anticipated risks: Flink processing (Phase 4) — prospective analysis, not yet observed

**Phase:** 4 (Flink processing)
**Component:** `streaming/flink-jobs/` (not implemented yet — see `docs/phase-4-streaming-design-decisions.md`)
**Date written:** 2026-07-28
**Status:** **Prospective.** Nothing in this document has happened live. Every entry below is a risk identified *while designing* the job (see `docs/phase-4-streaming-design-decisions.md`), written down now so it can be checked against reality once the job actually runs — not a substitute for a real incident write-up. When (if) one of these actually occurs, or a different one does, it gets its own file in this folder following the format of the four Phase 2 incidents (`debezium-*.md`), and this document should link to it rather than being edited to pretend it was predicted with that level of detail.

**Why this file exists instead of waiting:** this project's own practice (Phase 3 §7) is "document real incidents, not hypothetical ones." This file does not violate that — it is explicitly labeled prospective, kept separate from the real incident files, and exists because the design conversation for Phase 4 already surfaced several *specific, non-obvious* failure mechanisms before any code was written. Writing them down now, while the reasoning is fresh, is cheaper than reconstructing it later if one of them actually fires.

---

## Risk 1 — Event-time watermark stall (design decision: avoided, not mitigated)

**What could happen:** if Phase 4 had used event-time semantics with watermarks (Decision A), a Flink join's combined watermark only advances as fast as its *slowest* input. `market-price-events` (Kinesis) can legitimately go quiet for a segment for a while (traffic is bursty by segment, unlike Kafka's steadier per-partition flow from CDC). If that happens, the combined watermark stalls — and event-time-based logic on the *Kafka side*, which is still producing normally, cannot fire either, even though nothing is actually wrong with it.

**Why it doesn't apply here:** Decision A (confirmed 2026-07-27) chose **processing-time**, specifically because Phase 4 has no fixed-time windows to close — only reactive `MapState` upserts and `onTimer`-based dead-man's-switch timers (E.1). There is no watermark to stall because there is no watermark. This entry exists so a future reader who considers switching to event-time (e.g., if a future phase adds windowed aggregation) knows this exact failure mode was already weighed and is the reason processing-time was chosen, not an oversight.

**Situations where you'd hit this if the decision were reversed:** any Flink job with a multi-source join or `CoGroup` where one source's traffic is bursty or segment-scoped (uneven arrival, not uniform per partition) — watch for it the moment someone proposes `WatermarkStrategy.forBoundedOutOfOrderness` on this job.

---

## Risk 2 — Stale overwrite during manual reprocessing (design decision: mitigated by a one-line comparison, not yet implemented)

**What could happen:** `MapState.put()` performs no comparison — it simply overwrites. In normal operation, Kafka/Kinesis ordering per key guarantees "arrival order" equals "real order," so this is safe. The failure case is a **manual reprocess** — restarting the job from an old offset after a failure *without* a valid checkpoint, or a deliberate backfill — where an old event can arrive *after* a newer one already in state and silently overwrite it with stale data. Nothing in the event itself would flag this; it would look like a normal, valid update.

**Mitigation already decided, not yet built:** compare the incoming event's own `updated_at` (`payment_line`) or `collected_at` (`market_price`) against what's already stored before overwriting, discarding if older. This must land in the `KeyedProcessFunction`/`KeyedBroadcastProcessFunction` implementations themselves — it is easy to write the happy-path `put()` and forget this check, since nothing in local/manual testing (which naturally replays events in order) will ever exercise it.

**How you'd detect it if it slipped through unmitigated:** a `price_decision` whose `cost_inputs`/`market_inputs` visibly regress (lower cost than a previously-emitted decision for the same apartment, or an older `collected_at` than one already seen) without a corresponding real-world event explaining it — that's the signature to grep audit records for, if this is ever suspected in practice.

**Situations where you can hit this:** any stateful Flink job recovering without checkpointing enabled (Risk 2 is *why* Decision F chose to enable checkpointing — with valid checkpoints, "reprocess from offset 0" stops being the normal recovery path and this becomes a residual, backfill-only case, not a routine one) — and any deliberate backfill/replay tooling built later that doesn't go through the same upsert-with-comparison code path.

---

## Risk 3 — Unbounded keyed state / orphaned timers (design decision: must be a hard limit in the formal spec, not written yet)

**What could happen:** Decision D's two `MapState`s per segment (`apartamentos`, `noches`) have no stated eviction rule yet — nothing bounds how many apartments or how many future dates accumulate per segment key over the job's lifetime. Separately, Decision E.1's dead-man's-switch timer is registered per key (apartment or date); if an apartment is deleted from the source system, its timer has nothing left to cancel it and nothing left to fire meaningfully against — an orphaned timer, structurally the same category of leak as an unbounded `MapState`, just less visible because Flink doesn't surface "timer count" the way it surfaces state size.

**Why this is flagged now instead of after implementation:** the design document (`docs/phase-4-streaming-design-decisions.md`, "Qué debe asegurar el spec de Flink") already names this as a **non-negotiable** requirement for the formal spec — explicit size limits and eviction rules for both `MapState`s, and an explicit answer for "what happens to a key's timer when the key disappears." Writing the spec without this is exactly how this kind of leak reaches production without anyone deciding it should be allowed to.

**Situations where you can hit this:** any long-running keyed-state Flink job where the key space grows monotonically (new apartments, new segments, new dates) without a compensating cleanup path — this is the general shape of "state leak," not specific to this project. In this job specifically: an apartment permanently removed from `mock-pm-app` without a corresponding tombstone/removal event flowing through the same broadcast channel that added it in the first place.

---

## Risk 4 — DynamoDB sink throttling under fan-out load (design decision: needs the same Kleppmann rigor as Phase 3's `put_records`, not written yet)

**What could happen:** Decision D's fan-out means a single market-price update for one date can emit up to ~6 `price_decision` writes (one per apartment in that segment), and a single cost update can emit up to ~60 (one per known future date). Under `EXACTLY_ONCE` checkpointing, a sink failure (DynamoDB throttling, `ProvisionedThroughputExceededException`-equivalent) during that burst needs a defined retry/backoff story — copy-pasting Phase 3's Kinesis retry logic without adapting it would be a mistake, since `decision_id`'s idempotency guarantee (Decision G) means retries here are *safe by construction* in a way Phase 3's `put_records` retries were not (Phase 3 had to reason carefully about at-least-once vs at-most-once; this sink can lean fully into at-least-once with no caveat).

**Situations where you can hit this:** any downstream sink whose write volume is a multiplier of the triggering event count (fan-out sinks in general) — under-provisioned DynamoDB write capacity, or a burst coinciding with many segments updating near-simultaneously (e.g. `market-ingestor`'s cyclic tick, D.1, updating all 18 segments once per minute).

---

## Risk 5 — Fan-out volume undersized/oversized relative to what D.1's cyclic coverage actually produces

**What could happen:** Decision D.1 changes `market-ingestor` from random-date to deterministic cyclic coverage specifically so Hoja 2 (`noches`) fills up to a full 60-entry calendar per segment. Once that's true, **every** market-price tick for a segment triggers a fan-out across all apartments in that segment (small, ~5–6), and **every** cost update triggers a fan-out across all 60 known dates. This is fully bounded and expected — not a leak — but it's worth explicitly load-testing once D.1 is implemented, since the emitted-`price_decision`-per-second rate changes materially from "occasional, sparse" (today's random-date Phase 3 behavior) to "every segment, every tick, predictably" — exactly the numbers Risk 4's sink needs to be provisioned for.

**Situations where you can hit this:** any design where a follow-up change to an upstream producer (D.1) silently changes the load profile of a downstream consumer (Flink's fan-out, then the DynamoDB sink) that was designed and reasoned about under the old profile — the fix is to re-derive the volume math after D.1 ships, not to assume Decision D's original fan-out estimate still holds.

---

## What to learn from this exercise, independent of whether any of the five risks above ever fire

- All five were catchable **from the design document alone**, before a line of `streaming/flink-jobs/` code exists — reinforcing this project's own precedent (Phase 1–3) of writing the spec's Configuration/Acceptance-Criteria sections *before* implementation, not after.
- Three of the five (Risk 1, 2, 4) are direct instances of patterns already lived through in Phases 2–3 (Debezium's heartbeat-topic stall = a stuck-pipeline symptom; ADR-0003's upsert-by-key = the same reprocessing-safety concern as Risk 2; Kleppmann's delivery-semantics framing for Kinesis `put_records` = the same framing Risk 4 needs) — the lesson generalizes: **a new component in the same pipeline family tends to fail in the same handful of ways**, just at a different layer.
- Risk 3 is the one item on this list that is a **hard blocker for writing the formal spec**, not just a thing to watch — `specs/phases/04-flink-processing/spec.md` cannot be considered complete without an explicit answer to it.
