# ADR-0004 — pandas pinned to 2.2.x workspace-wide

**Date:** 2026-07-01
**Status:** Accepted

## Context

The project uses a single `uv` workspace with one unified lockfile across all Python services (stated stack decision in the README). `dashboard/pyproject.toml` originally declared `pandas>=3.0.0`. `apache-flink==2.3.0` (the current latest release, confirmed against PyPI — no newer version exists) depends on `pandas>=1.3.0,<2.3`. Because the workspace resolves one dependency graph for every member, these two constraints made the workspace **entirely unresolvable**: `uv sync` failed for every package, not just `dashboard` or `flink-jobs`, since a single lockfile must satisfy both simultaneously.

Separately, once the pandas conflict was resolved, `uv sync` hit a second, unrelated build failure: `apache-beam` (a transitive dependency of `apache-flink`) has a legacy `setup.py` that imports `pkg_resources` during its build step without declaring it as a build dependency. Current `setuptools` (82.x) has removed `pkg_resources` entirely, so any isolated PEP 517 build using the latest setuptools fails with `ModuleNotFoundError: No module named 'pkg_resources'`.

## Decision

1. Relax `dashboard`'s pin to `pandas>=2.2,<2.3` — the newest 2.x line still inside `apache-flink`'s allowed range.
2. Add `[tool.uv.extra-build-dependencies] apache-beam = ["setuptools<81"]` to the root `pyproject.toml`, forcing `apache-beam`'s isolated build environment to use a `setuptools` version old enough to still bundle `pkg_resources`.

## Rationale

- `dashboard` has zero implementation code as of this decision (Phase 5 is unbuilt). Relaxing an aspirational, never-validated version pin costs nothing today — there is no code depending on pandas 3.x-specific behavior to preserve.
- The alternative — splitting the workspace into conflicting resolution groups (uv supports this, but it adds real complexity: per-package lock forking, `--package`-scoped installs) — was rejected because it contradicts the project's explicit "single lockfile" simplicity goal for a stack whose primary purpose is learning, not managing dependency isolation machinery.
- Pinning an older `setuptools` **only for apache-beam's build environment** (not the whole workspace) is the narrowest possible fix: it doesn't affect what `setuptools` version is actually installed/used at runtime by any project code, only what's available while `apache-beam`'s own `setup.py` executes once during dependency resolution.

## Consequences

- `dashboard` cannot use pandas 3.x-only APIs. If a future dashboard feature genuinely needs pandas 3.x, this ADR must be revisited — the fix at that point would likely mean upgrading `apache-flink` (if a version supporting pandas 3.x is released) or reintroducing workspace conflict groups.
- This pin is coupled to `apache-flink`'s own pandas constraint. If `streaming/flink-jobs/pyproject.toml`'s `apache-flink` version ever changes, re-check whether pandas's allowed range changed too, and revisit `dashboard`'s pin accordingly.
- The `setuptools<81` build-time pin for `apache-beam` is a workaround for an upstream packaging gap (an old `setup.py` with an undeclared build dependency), not a project design choice — it should be removed if a future `apache-beam`/`apache-flink` release fixes this upstream.
