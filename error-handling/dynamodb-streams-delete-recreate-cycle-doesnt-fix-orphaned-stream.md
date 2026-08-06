# Incident: deleting and recreating `price_decision` (the documented fix for an orphaned DynamoDB stream) reproduced the same broken state instead of fixing it — needed a full LocalStack data reset

**Phase:** 6 (Dashboard) | **Date:** 2026-08-06 | **Component:** LocalStack DynamoDB Streams, `services/lakehouse-consumer`

## What happened

Bringing up the full stack for Phase 6 (after weeks of the environment sitting stopped), `lakehouse-consumer` and `lakehouse-maintenance` crash-looped on startup. Two separate problems stacked on top of each other:

1. The PyIceberg catalog (SQLite, `lakehouse_catalog` volume) pointed at a metadata version (`00009-...json`) that no longer existed in the `pms-lakehouse` S3 bucket — fixed by dropping that volume, letting the catalog recreate itself fresh.
2. With that fixed, `lakehouse-consumer` still crash-looped, this time on the exact symptom already documented in [`dynamodb-streams-orphaned-after-localstack-restart-needs-full-table-recreation.md`](dynamodb-streams-orphaned-after-localstack-restart-needs-full-table-recreation.md): `describe_table` reported a `LatestStreamArn` that `list_streams`/`describe_stream` didn't recognize.

That earlier write-up's documented fix is: delete the `price_decision` table and recreate it. Applied here, it did not work:

```
$ aws dynamodb delete-table --table-name price_decision ...
$ aws dynamodb describe-table --table-name price_decision ...   # ResourceNotFoundException after ~1s
$ aws dynamodb create-table --table-name price_decision ...     # ResourceInUseException
$ # ...repeated with 2s backoff for 60+ seconds, still ResourceInUseException every time
```

`describe-table` confirmed the table gone almost immediately, but `create-table` kept refusing with "already exists" for over a minute straight. Odder still: a `describe-table` run during that window showed a *different* `LatestStreamArn` timestamp each time it was checked — something was recreating table/stream state on its own, and `list-streams` stayed empty against every one of those ARNs. The delete/recreate cycle wasn't stuck — it was actively reproducing the exact same orphaned-stream symptom on every cycle.

## Root cause

Not fully diagnosed — and that's the point of this write-up. LocalStack Community's DynamoDB Streams mock has an internal registry (which stream ARNs are actually "live") that is separate from the table's own persisted metadata (which believes `StreamEnabled: true` and has *a* `LatestStreamArn` string). The previous incident already found this split-brain state surviving a container *restart*; this session found the same split-brain state re-emerging from delete-table/create-table API calls within a single, uninterrupted container lifetime. Whatever LocalStack's Streams registry is keyed on, it did not get reliably reset by a table-level delete — the leading theory is that `PERSISTENCE=1`'s background flush-to-disk cycle raced with the API calls, but this was not confirmed by inspecting LocalStack's own internals — deliberately not asserted here as fact.

## How it was solved

Escalated past the table-level fix entirely: stopped the whole stack (`docker compose down`, no `-v`) and removed the `localstack_data` and `lakehouse_catalog` named volumes outright, then brought everything back up. `init-aws.sh`'s idempotent provisioning (fixed in an earlier Phase 5 incident) recreated every table/stream/bucket from a genuinely empty LocalStack state, and `lakehouse-consumer` started clean on the first attempt — no crash-loop, a real `stream_arn` visible in its own startup log, immediately consistent with `list-streams`.

## What to learn from this

A previously-documented single-resource fix (delete/recreate one table) is scoped to the failure mode that was actually diagnosed at the time — it is not a general guarantee against every future instance of "the symptom looks the same." When a documented fix is applied and reproduces the *same* symptom rather than resolving it, that's a signal the state corruption lives one level higher than where the fix operates (here: LocalStack's own Streams subsystem, not the one table), not a signal to retry the same fix harder. This project's own established playbook for LocalStack — full local/PoC data is disposable, so the correct response to a wedged local emulator is to reset it, not to reverse-engineer its internals — applied cleanly once escalated to the volume level, after roughly two minutes lost to table-level retries that were never going to work.

## Situations where you can hit this

Any LocalStack-backed local/PoC environment left stopped for an extended period (here: since the previous Phase 5 session, days earlier) is a higher-risk candidate for waking up with inconsistent internal state, especially around derived resources with their own subsystem (Streams, Triggers) layered over a base resource (a table, a queue). If a documented single-resource recovery doesn't fix the symptom on the first clean attempt, don't spend more than one or two retries on it — go straight to resetting the emulator's whole persisted-state volume for that service. This is cheap for synthetic/PoC data specifically because it's disposable and every generator/consumer in this project (`mock-pm-app`, `market-ingestor`, Flink, `lakehouse-consumer`, `dbt-runner`) re-populates its own state automatically once the stack is back up — there is nothing to manually re-seed.
