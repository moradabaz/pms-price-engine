# Incident: a DynamoDB Streams ARN survived a LocalStack restart in `describe_table`, but the stream itself was gone — and the documented recovery (disable/re-enable) didn't fully fix it either

**Phase:** 5 | **Date:** 2026-08-04 | **Component:** `services/lakehouse-consumer`, LocalStack DynamoDB Streams

## What happened

`lakehouse-consumer` crash-looped on startup against a live LocalStack stack:

```
RuntimeError: Stream 'arn:aws:dynamodb:...price_decision/stream/2026-08-04T20:55:09.670' from
describe_table is not a registered stream for 'price_decision' (registered: none)
```

This is `resolve_stream_arn`'s own guard firing correctly (spec 05 §5) — but it fired on a table that, from the outside, looked fine: `describe-table` reported a `LatestStreamArn` for `price_decision`, and the table's items were intact (LocalStack's `PERSISTENCE=1`). `dynamodbstreams list-streams` and `describe-stream` for that exact ARN both came back empty/`ResourceNotFoundException`.

## Root cause

Between creating `price_decision` fresh and the consumer's next startup, the LocalStack container was restarted (`docker compose restart localstack`, done in this same session to verify `init-aws.sh`'s idempotency fix). `PERSISTENCE=1` faithfully restored the table's own metadata — including the `LatestStreamArn` string — but LocalStack's DynamoDB Streams shard/stream registry did not come back in a consistent state after that restart. This is exactly the failure mode the Phase 5 pre-spec's Decision F already flagged from earlier testing ("el ARN del stream no sobrevive un reinicio del contenedor de forma fiable") — confirmed again here, this time inside the actual crash-loop it causes if a consumer doesn't guard against it.

**New finding beyond what the pre-spec already knew:** the pre-spec's own documented recovery — `update-table --stream-specification StreamEnabled=false` then `StreamEnabled=true` to force a fresh, registered stream — did not reliably work in this session. The disable succeeded, but the subsequent enable call returned `ValidationException: Table already has an enabled stream` (a stale internal flag) while *also* silently rotating `LatestStreamArn` to a new value that was, itself, unregistered in `list-streams`/`describe-stream` — the same broken state, just with a different ARN string.

## How it was solved

Deleted and recreated the `price_decision` table entirely (synthetic PoC data, same "safe to lose, recreate from scratch" reasoning this project already applies to Kafka offsets in Phase 2 and LocalStack's Kinesis mock state per `AUDIT_DIARY.md`). A table created fresh — not toggled — reliably produced a stream that immediately showed up in both `list-streams` and `describe-stream`, confirmed by comparison against a throwaway `streams_probe` table created in the same session for exactly this check. `docker delete-table` needed a poll-until-gone wait before recreating (`describe-table` kept reporting the old table as still `ACTIVE` immediately after the delete call) — it isn't synchronous even on a mocked backend.

## What to learn from this

A documented recovery procedure from an earlier finding is a lead worth trying, not a guarantee — the underlying bug it was written against may not repro identically every time, or may have a partial/flaky fix depending on exactly what internal state the mock is in. When the documented fix doesn't fully resolve the symptom (here: it changed the ARN but not the actual brokenness), escalate to a coarser, more reliable fix rather than iterating on variations of the same command — recreating the whole resource is blunter than toggling a flag, but it's also much less likely to hit a partially-fixed intermediate state, and for disposable local/PoC data the cost of doing so is close to zero.

## Situations where you can hit this

Any time a LocalStack-backed integration test or local dev workflow survives a container restart via a persistent volume for the *base* resource (a table, a queue) but layers a *derived* live resource on top of it (a stream, a subscription, a trigger) — don't assume the derived resource survived just because the base resource's own describe/get call still mentions it. If a documented single-step recovery (a toggle, a flag flip) doesn't fully resolve it on the first try, don't keep retrying variations — delete and recreate the resource outright once, and confirm the fresh version works before spending more time on the narrower fix.
