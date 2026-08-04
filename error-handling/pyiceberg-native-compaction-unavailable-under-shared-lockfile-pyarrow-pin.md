# Incident: PyIceberg's native compaction API doesn't exist in the only version this workspace can install

**Phase:** 5 | **Date:** 2026-08-04 | **Component:** `services/lakehouse-consumer`, `services/lakehouse-maintenance`, root `pyproject.toml` (uv workspace)

## What happened

The Phase 5 pre-spec (`docs/phase-5-persistence-design-decisions.md`, Decision D) stated: "recent versions of PyIceberg (0.7+) bring `rewrite_data_files` and `expire_snapshots` without needing Spark." Adding `pyiceberg[glue,pyarrow]` to the workspace and running `uv sync` failed immediately with an unresolvable dependency conflict, not a compaction bug:

```
Because flink-jobs depends on apache-flink>=2.3.0 and apache-flink==2.3.0 depends on
apache-beam>=2.54.0,<=2.61.0 ... apache-beam ... depends on pyarrow>=3.0.0,<17.0.0 ...
And because pyiceberg[pyarrow]>=0.9.0 depends on pyarrow>=17.0.0 ...
flink-jobs and lakehouse-consumer are incompatible.
```

Pinning `pyiceberg[glue,pyarrow]>=0.8.0,<0.9.0` resolved cleanly (0.8.1's `pyarrow` extra allows `>=14.0.0,<19.0.0`, overlapping `apache-beam`'s `<17.0.0` ceiling). But inspecting the *installed* `pyiceberg==0.8.1` source directly — `grep -rn "rewrite\|expire" .venv/lib/python3.11/site-packages/pyiceberg` — found no `rewrite_data_files`, no `expire_snapshots`, not as a `Table` method, not as a standalone function, not as a CLI action. The pre-spec's claim was true of a version this workspace cannot actually install.

## Root cause

Two unrelated facts collided:

1. **Phase 0's single-lockfile decision** (`AUDIT_DIARY.md`) means every workspace member's dependencies must resolve together. `flink-jobs`' `apache-flink>=2.3.0` transitively pins `pyarrow<17` via `apache-beam`.
2. **PyIceberg's native compaction actions require `pyarrow>=17`**, because they only exist starting in the `0.9.x` line — the pre-spec's "0.7+" claim was never verified against the actual installed package, just recalled/assumed from general PyIceberg familiarity.

Those two constraints don't overlap. There is no `pyiceberg` version that both installs in this workspace and ships `rewrite_data_files`/`expire_snapshots`.

## How it was solved

Verified the two APIs that *do* exist in `0.8.1` — `Table.scan().to_arrow()` and `Table.overwrite(df, overwrite_filter=ALWAYS_TRUE)` — and confirmed `overwrite()` internally does exactly "delete everything matching the filter, then re-append the given data" in a single commit (read directly from `pyiceberg/table/__init__.py`'s source, not the docstring alone). Implemented compaction in `lakehouse-maintenance` as a manual full-table rewrite:

```python
full_data = table.scan().to_arrow()
table.overwrite(full_data)  # ALWAYS_TRUE is the default filter
```

This produces the same observable result AC-06 asks for (identical row count/content, fewer data files) without Spark, without a second lockfile, and without upgrading `pyiceberg`. `expire_snapshots` has no equivalent in 0.8.1 and was deferred as an explicit, documented known limitation (spec 05 §13) rather than worked around — it isn't correctness-critical, it just means old snapshots/manifests accumulate in S3 over time.

Both the formal spec (`specs/phases/05-persistence/spec.md` §6/§13) and the pre-spec's own Decision D section were corrected in place with the live finding, dated, rather than left silently wrong for the next person to trip over.

## What to learn from this

A pre-spec's technical claim about a library's API ("version X+ supports Y") is a hypothesis until it's checked against the exact version that will actually run — not the latest version, not the version in the library's current docs, but whatever a real `uv sync`/`pip install` resolves to under this project's actual constraints. Two libraries can each individually look compatible with "the project" while being mutually exclusive once a single shared lockfile forces one resolution across all of them. The fix for a missing library feature doesn't have to be "get a newer version" — checking what the *pinned* version's actual public API can already do (here, `overwrite()` as a coarser-grained stand-in for `rewrite_data_files()`) can satisfy the real acceptance criterion without touching the dependency constraint at all.

## Situations where you can hit this

Any time a new dependency is added to a workspace/monorepo that already pins a wide-reaching package (a distributed compute framework, a large SDK) via one of its own transitive dependencies — Spark, Beam, TensorFlow, and similar tend to carry narrow pins on numeric/columnar libraries (`pyarrow`, `numpy`, `protobuf`) that quietly cap what else can be installed alongside them. Before trusting a design doc's "library version X supports feature Y," resolve the dependency for real and inspect the *installed* package's source or `dir()` output for the feature — a version number in a pre-spec is a hypothesis, not a citation.
