# Build context is the repo root (see infra/docker-compose.yml) — same
# reasoning as the other services: this is a uv workspace member needing
# sibling packages (libs/common, libs/shared-schemas) and the root lockfile.
#
# Base image must match apache-flink's pinned version in this package's
# pyproject.toml (currently 2.3.0) — a mismatched Flink cluster version is
# exactly the kind of silent gap this project has been burned by before
# (Debezium default topic naming, Debezium default key). Official Flink
# images ship Java-only; Python is installed here explicitly for PyFlink.
FROM apache/flink:2.3.0-java21

# The base image runs as a non-root `flink` user (uid 9999) by default —
# apt-get/curl below need root.
USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-venv build-essential && \
    rm -rf /var/lib/apt/lists/*

# No Kafka connector release targets Flink 2.3 yet (checked Maven Central
# 2026-07-29) — closest available build (2.2) used as-is, works in practice.
# No Kinesis connector runs on Flink 2.x at all (error-handling/
# flink-2x-removes-legacy-sourcefunction-...md) — market-price-events reaches
# this job via kinesis-kafka-bridge + this same Kafka connector instead.
RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar

# S3 filesystem plugin (checkpoint storage, Decision F) ships in /opt/flink/opt/
# but Flink only loads plugins from /opt/flink/plugins/<name>/ — must be copied
# there explicitly to activate.
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-2.3.0.jar /opt/flink/plugins/s3-fs-hadoop/

COPY --from=ghcr.io/astral-sh/uv:0.11.28 /uv /uvx /usr/local/bin/

WORKDIR /app
COPY . .

# Ubuntu 24.04 (this base image) only ships python3.12 — the lockfile was
# resolved against 3.11, so uv must download its own managed 3.11 build
# rather than use the system one. Redirect that download to a location the
# non-root `flink` user (uid 9999) can actually read — uv's default,
# ~/.local/share/uv under /root, is root-only and breaks Python's own
# stdlib lookup once we drop to `flink` below.
ENV UV_PYTHON_INSTALL_DIR=/opt/uv-python

RUN uv sync --frozen --package flink-jobs && \
    chmod -R a+rX /opt/uv-python

ENV PATH="/app/.venv/bin:${PATH}"

USER flink
