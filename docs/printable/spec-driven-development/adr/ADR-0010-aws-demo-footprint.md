# ADR-0010 — AWS demo footprint: storage/state real, compute local

**Date:** 2026-08-30
**Status:** Accepted

## Context

Every prior phase built and verified against LocalStack — a deliberate cost guardrail (README) to keep development free until the very end. Phase 7 needs a real AWS deployment to prove the pipeline works against actual service behavior LocalStack can't fully emulate (real IAM enforcement, real Glue tier limits, real S3/DynamoDB/Kinesis semantics) — not just a local sign-off. But this project has no production budget: it's a PoC with AWS Budget alerts at $5 and $10 as its only real constraint, and no ongoing uptime requirement to justify anything staying up.

## Decision

Only the **storage/state layer** goes to real AWS: **S3** (two buckets — `pms-iceberg` for Flink checkpoints, disposable; `pms-lakehouse` for the Iceberg warehouse, versioned), **Kinesis** (`market-price-events`, one shard), **DynamoDB** (`price_decision` with Streams, plus `stream_checkpoints`), and **Glue Data Catalog** (`pms_lakehouse` database). **Compute stays local**: Kafka, Kafka Connect/Debezium, Flink, and Postgres keep running in Docker Compose, reconfigured by config alone (env vars, real credentials) to talk to the real AWS endpoints instead of LocalStack.

The deployment is one-shot, never left running: `terraform apply` → run the demo → `terraform destroy` immediately after, confirmed by an empty `terraform state list` and a spot-check on every resource. AWS Budget alerts at $5/$10 must already exist on the target account before `terraform apply` runs — this is a hard precondition, not a nice-to-have.

**Alternatives considered and rejected:**
- **Managed Kafka (MSK) or managed Flink (KDA/EMR), i.e. compute on real AWS too.** Rejected — these bill per broker/node-hour even at the smallest tier and would exhaust a $5–10 budget in minutes, not the duration of a demo. Nothing about proving the pipeline against real AWS requires paying for managed compute; the storage/state layer is where LocalStack's fidelity gaps actually matter.
- **A persistent/always-on AWS deployment.** Rejected — this is a one-shot demo to prove the pipeline once, not a hosted environment. There is no uptime requirement to justify continuous cost, and leaving anything running past the demo is exactly the discipline this project already applies to disposable local state (Kafka offsets, LocalStack volumes), now with real money attached.
- **Stay on LocalStack entirely, skip real AWS.** Rejected — the whole point of this phase is exposure to real-AWS-specific behavior LocalStack doesn't reproduce (e.g. Glue's tier restrictions, IAM's actual enforcement of least-privilege policies, DynamoDB Streams' real ARN-scoping behavior) — deferring that indefinitely would leave the project's stated learning goal (a scalable AWS-based lakehouse) untested against the one environment that actually matters.

## Consequences

- `infra/terraform/` needs a full rewrite — the Phase 0 stub doesn't match what `infra/localstack/init-aws.sh` actually provisions today (wrong DynamoDB schema, one S3 bucket instead of two, no Glue, no IAM).
- `libs/lakehouse-shared/src/lakehouse_shared/catalog.py`'s `build_catalog()` needs a `GlueCatalog` branch — the one part of this footprint that's a real code change, not just an endpoint/credential swap (already anticipated in the Phase 5 spec, §10).
- Every other local service (`market-ingestor`, `dbt-runner`, `dashboard`, Flink) switches to real AWS by configuration alone — no code change required for those.
- IAM roles must be written as real Terraform resources with zero wildcard resources, converting the least-privilege design tables already written in the Phase 5/6 specs into policies that are actually enforced for the first time (LocalStack Community doesn't enforce IAM at all).
- **As of this ADR, the AWS deployment itself is not yet executed** — no real account has been touched, no resources exist. This ADR records the footprint decision only; the live demo (`terraform apply` → demo → `terraform destroy`) remains a separate, deliberate, one-time action to be run when Phase 7's documentation track is otherwise complete.
- If this project ever became a real production service, compute-stays-local is exactly the first assumption to revisit — with a real budget and a real uptime requirement, neither of which this PoC has.
