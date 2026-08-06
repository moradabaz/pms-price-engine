# Incident (pre-empted, not hit live): would a Streamlit dashboard reading a DuckDB file crash if dbt-runner is writing to it at the same time?

**Phase:** 6 (Dashboard) | **Date:** 2026-08-06 | **Component:** `dashboard/src/dashboard/marts.py`, `services/dbt-runner`

## What happened

Phase 6's design (`docs/phase-6-dashboard-design-decisions.md`, Decision C) flagged this as an open question rather than an assumption: the dashboard reads the same DuckDB file (`pms_lakehouse.duckdb`) that `dbt-runner` writes to on a 15-minute tick, from a separate container/process. Before writing the spec, it was explicitly left unresolved whether DuckDB's `read_only=True` connections are safe against a concurrent writer, or whether they'd raise.

A unit test (`dashboard/tests/test_marts.py::test_read_retries_then_raises_on_persistent_lock`) answered this directly: a second **process** (via `multiprocessing`, not just a second connection in the same process) holding a plain writable `duckdb.connect(path)` open causes a same-process `duckdb.connect(path, read_only=True)` to raise `duckdb.OperationalError` for as long as the writer holds the file open. A same-process, different-`read_only`-flag attempt raises a different, narrower exception (`_duckdb.ConnectionException: ... different configuration than existing connections`) — that variant is an artifact of DuckDB's in-process connection cache and not representative of the real cross-container scenario, which is why the test uses a real second OS process.

Live, against the actual deployed containers, this was re-checked: triggering a real `dbt run --full-refresh` (`services/dbt-runner`) and firing 8 concurrent read attempts from inside the real `dashboard` container while it ran. None of the 8 crashed or raised — `dbt-duckdb`'s writes here commit in ~0.25s (each mart is a fast `CREATE TABLE AS`), so the actual collision window is narrow.

## Root cause

DuckDB enforces single-writer/multi-reader access at the file level, but only readers that open with `read_only=True` benefit from that — and even then, a writer's open transaction can still block a reader for the duration of that transaction. This isn't a bug in either service; it's DuckDB's documented concurrency model applied to two independent processes sharing one file via a Docker volume, a topology DuckDB is not primarily designed around (it's an embedded, single-process-oriented engine).

## How it was solved

Not "solved" so much as designed against: `dashboard/src/dashboard/marts.py`'s `_read_with_retry` opens a short-lived `read_only=True` connection per query (never held across a Streamlit rerun) and retries a bounded number of times with a short backoff on `duckdb.OperationalError` — the same base class that covers both the real cross-process lock error and any related variant, rather than pattern-matching one specific exception subtype. This was written into the design *before* knowing whether it would ever actually fire live — cheap insurance, not a reaction to an observed failure — and live testing confirmed the collision window is narrow enough that it may rarely trigger in practice, without making the retry unnecessary (a slower or larger `dbt run` in the future would widen that window).

## What to learn from this

Not every open concurrency question needs to be fully resolved before writing the spec — it's fine to commit to "verify live before/while building, and have a documented fallback either way" (pre-spec Decision C did exactly this) rather than blocking on an answer that a two-line test settles quickly. Separately: when testing file-level concurrency behavior, a real second OS process is not optional — DuckDB (and likely other embedded single-writer engines) can have an *internal, same-process* connection cache that produces a different, less representative error than what two genuinely independent processes sharing a file will hit. A concurrency test that never spawns a second process risks validating the wrong failure mode.

## Situations where you can hit this

Any architecture sharing an embedded/single-writer database file (DuckDB, SQLite) between independent services via a shared volume — one always-writing tick process and one on-demand reader is exactly the shape most likely to hit this, and it gets more likely as the writer's own commit duration grows (a bigger `dbt run`, more models, larger data). If you're testing this kind of concurrency locally, force the two sides into separate OS processes (or containers, as in the live check here) rather than trusting a same-process simulation.
