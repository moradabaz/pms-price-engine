# Incident: AWS Glue Data Catalog doesn't exist at all in LocalStack Community

**Phase:** 5 | **Date:** 2026-08-04 | **Component:** `services/lakehouse-consumer`, `services/lakehouse-maintenance`, `infra/localstack/init-aws.sh`

## What happened

The Phase 5 pre-spec (`docs/phase-5-persistence-design-decisions.md`, Decision C) stated: "LocalStack Community soporta las operaciones de catálogo de Glue (`CreateDatabase`/`CreateTable`/`GetTable`) — lo que no soporta en Community son los *Glue Jobs* (cómputo Spark), que es Pro." Building `lakehouse-consumer`/`lakehouse-maintenance` against a `GlueCatalog` and running `init-aws.sh`'s `glue create-database` step against a live LocalStack container failed immediately:

```
An error occurred (InternalFailure) when calling the CreateDatabase operation: API for service 'glue' not yet implemented or pro feature - please check https://docs.localstack.cloud/references/coverage/ for further information
```

`glue get-database`, `glue get-databases`, and every other Glue API called during this session returned the same `501`/`InternalFailure`, not just the *Jobs* (Spark compute) subset the pre-spec assumed was the only gated part.

## Root cause

Checked LocalStack's own coverage documentation directly (`docs.localstack.cloud/references/coverage/coverage_glue/`) rather than continuing to work around symptoms: the page's own banner states Glue support is **"Included in Plans: Ultimate"** — the entire service, catalog operations included, is a paid-tier feature. The pre-spec's Decision C claim was never actually checked against LocalStack; it was reasoned from a plausible-sounding split ("Data Catalog is metadata, Jobs are compute, metadata APIs are usually the free tier") that happened to be wrong for this specific product's tiering.

## How it was solved

Confirmed with the user this was worth a design change rather than a paid LocalStack tier (keeping local development free, consistent with this project's PoC ethos). Swapped `lakehouse-consumer`/`lakehouse-maintenance`'s Iceberg catalog from `GlueCatalog` to PyIceberg's `SqlCatalog` (SQLite, on a small dedicated Docker volume, `lakehouse_catalog`) for local/PoC use — storage (S3, via LocalStack) is completely unaffected, only the *catalog* backend (the piece that tracks which table points at which metadata.json) changes. Removed the `glue create-database` step from `init-aws.sh` entirely (PyIceberg's own `create_namespace_if_not_exists` creates the namespace itself, no separate provisioning needed) and dropped `glue` from `docker-compose.yml`'s LocalStack `SERVICES` list. `build_catalog()` in both services now carries an explicit docstring: Phase 7 (real AWS) swaps this one function for a real `GlueCatalog` — the promotion is a real code change for this specific piece, not just an endpoint/credential swap the way the rest of Phase 5's LocalStack→AWS promotion is.

## What to learn from this

A design decision phrased as "X supports A but not B" deserves the same live-verification discipline as any other technical claim in this project — checking a product's own coverage/pricing page takes less time than building against the wrong assumption and finding out at integration time. The fact that a split *sounds* architecturally reasonable (free metadata APIs, paid compute) is not evidence it's how a specific vendor actually tiers their product. This is the same category of miss as the PyIceberg `rewrite_data_files`/`expire_snapshots` version claim from earlier the same day (see `error-handling/pyiceberg-native-compaction-unavailable-under-shared-lockfile-pyarrow-pin.md`) — two pre-spec claims about third-party tooling, both wrong, both caught only once real infrastructure was actually stood up and exercised.

## Situations where you can hit this

Any time a local emulator/simulator product (LocalStack, a cloud emulator, a "free tier" SDK mode) is chosen specifically because a design doc asserts "the operations we need are in the free/community tier" — verify that claim against the vendor's own current coverage matrix before writing code against it, especially for services that sound like they should have an obvious free/paid split (catalog metadata vs. compute, read vs. write, control plane vs. data plane) — the actual tiering is a business decision, not a technical one, and doesn't have to follow the split that seems obvious from the outside.
