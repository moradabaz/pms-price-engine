# Incident: five small bugs building a working PyFlink Docker image

**Phase:** 4 | **Date:** 2026-07-29 | **Component:** `streaming/flink-jobs/Dockerfile`, `infra/docker-compose.yml`

Building the first real deploy image for `streaming/flink-jobs/` hit five distinct, real bugs in sequence — each one only surfaced by actually building and running the image, not by review. Grouped here instead of five separate files since they're all "first time deploying this image" issues.

## 1. Flink cluster image didn't match the `apache-flink` pip version

`docker-compose.yml` used `apache/flink:2.0-java21`, but `streaming/flink-jobs/pyproject.toml` pins `apache-flink==2.3.0`. The comment next to it even said "must match" — nobody had checked. **Fix:** confirmed `apache/flink:2.3.0-java21` exists (Docker Hub tag search) and switched to it.

## 2. `apt-get`/`curl` failed: base image runs as non-root

Official Flink images run as `flink` (uid 9999), not root. `apt-get update` failed with `Permission denied`. **Fix:** `USER root` before install steps, `USER flink` again at the end.

## 3. Python broke at runtime with `ModuleNotFoundError: No module named 'encodings'`

Ubuntu 24.04 (the base image's OS) only ships Python 3.12, but the lockfile was resolved against 3.11 — so `uv sync` downloaded its own managed Python 3.11 build. That download went to `uv`'s default location, `/root/.local/share/uv/python/...`, owned by root. Once the image drops to the non-root `flink` user, that Python build becomes unreadable — Python couldn't even find its own standard library. **Fix:** `ENV UV_PYTHON_INSTALL_DIR=/opt/uv-python` before `uv sync`, then `chmod -R a+rX /opt/uv-python`.

## 4. Job failed at submission: `ModuleNotFoundError: No module named 'boto3'`

`streaming/flink-jobs/pyproject.toml` never declared `boto3` or `pydantic-settings` as dependencies, even though `dynamodb_sink.py` and `settings.py` import them directly. This never failed locally because the shared workspace venv had them installed transitively via `market-ingestor`'s own dependencies — a real "worked on my machine" gap, only caught by Docker's isolated `--package flink-jobs` sync. **Fix:** added both to `pyproject.toml`, re-ran `uv lock`.

## 5. Checkpointing needed the S3 plugin manually activated

`EmbeddedRocksDBStateBackend` + S3 checkpoint storage needs Flink's S3 filesystem plugin. It ships in every Flink image under `/opt/flink/opt/`, but Flink only loads plugins from `/opt/flink/plugins/<name>/` — copying it there is required, not automatic. **Fix:** `mkdir -p /opt/flink/plugins/s3-fs-hadoop && cp .../flink-s3-fs-hadoop-2.3.0.jar` into it, in the Dockerfile.

## Lesson

None of these five would have been caught by code review — every one only showed up by actually building the image and running a container from it. This is the same argument this project already makes for live verification over trusting a healthy-looking status (Phase 2's Debezium incidents, Phase 3's live shard check) — it applies just as much to "does this Docker image actually start and run the job," not just "does `docker build` exit 0."
