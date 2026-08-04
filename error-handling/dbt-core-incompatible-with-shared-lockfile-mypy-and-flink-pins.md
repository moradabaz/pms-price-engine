# Incident: no dbt-core version is compatible with this workspace's mypy pin and Flink's protobuf pin at the same time

**Phase:** 5 (PR2, dbt) | **Date:** 2026-08-04 | **Component:** `services/dbt-runner`, root `pyproject.toml` (uv workspace)

## What happened

Adding `dbt-core`/`dbt-duckdb` to a new `services/dbt-runner` package and running `uv sync` failed with an unresolvable dependency conflict — not a version pin that just needed adjusting, but a genuine three-way collision:

```
dbt-core<1.12.0 requires pathspec<0.13  →  conflicts with mypy>=2.1.0 (requires pathspec>=1.0.0)
dbt-core>=1.12.0 requires protobuf>=6.0  →  conflicts with apache-beam (requires protobuf<6.0, needed by flink-jobs)
```

Every dbt-core release either falls in the first bucket or the second. There is no version that satisfies both this workspace's dev-dependency `mypy>=2.1.0` and `flink-jobs`' transitive `apache-beam` pin simultaneously.

## Root cause

Same underlying shape as the `pyiceberg`/`pyarrow` conflict found earlier the same day (`error-handling/pyiceberg-native-compaction-unavailable-under-shared-lockfile-pyarrow-pin.md`): this workspace's single-lockfile policy (Phase 0) requires every member's dependencies — including a shared dev-dependency like `mypy` — to resolve together. `dbt-core` sits at the intersection of two things this project already pins narrowly for unrelated reasons (a strict, recent `mypy`; `apache-flink`'s `apache-beam` dependency), and dbt-core's own dependency graph doesn't have a version that threads both needles.

## How it was solved

Confirmed with the user this was worth resolving without touching mypy's version or reopening a second workspace lockfile (already recently decided against once today). Treated `dbt` as an external CLI tool instead of a Python library dependency: `dbt-core`/`dbt-duckdb` are installed into a separate venv baked into `services/dbt-runner`'s Docker image (`RUN python -m venv /opt/dbt-venv && /opt/dbt-venv/bin/pip install dbt-core dbt-duckdb`, added to `PATH`), completely outside `uv sync`'s resolution. `dbt_runner`'s own package (`pyproject.toml`) declares none of dbt's dependencies — only `pydantic-settings`/`structlog`/`common` — and its `main.py` invokes `dbt seed`/`dbt run`/`dbt test` via `subprocess.run(["dbt", ...])`, the same shape this project already uses for other external tools (`awslocal`, `docker`, `psql` via the README's manual-verification commands).

## What to learn from this

Not every third-party tool needs to be a first-class dependency in the same dependency graph as the rest of the codebase, especially inside a single-lockfile monorepo. When a tool (here, dbt) is invoked as a CLI rather than imported as a library (no code in this project calls into `dbt`'s Python internals), decoupling its installation from the shared resolver sidesteps version collisions entirely — at the cost of losing static type-checking visibility into that tool's own Python API, which this project doesn't use anyway. This is a different flavor of the same lesson as the `pyiceberg`/`pyarrow` incident: a single shared lockfile is a real, recurring constraint surface once a workspace accumulates enough narrowly-pinned dependencies (a strict dev-tool version, a specific big-framework version), and the fix doesn't always have to be "relax one of the pins" — sometimes it's "this dependency doesn't need to be in this graph at all."

## Situations where you can hit this

Any time a workspace enforces one shared lockfile/dependency resolution across many packages (a monorepo, a single `uv`/`poetry`/`pip-tools` environment for multiple services) and a new tool needs to be added that's used purely as a CLI (a linter, a database migration tool, an orchestrator like dbt, Terraform-adjacent Python wrappers) — check whether it actually needs to be a resolved Python dependency at all, or whether installing it into an isolated environment and shelling out to it avoids constraining the whole workspace's resolution unnecessarily. This is especially worth considering before reaching for "downgrade a shared dev-dependency" or "give this one service its own lockfile," both of which have their own workspace-wide costs.
