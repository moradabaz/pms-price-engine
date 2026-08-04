# Build context is the repo root (see infra/docker-compose.yml) — required
# because this service is a member of the uv workspace and depends on
# sibling packages (libs/common) via a single lockfile, and needs
# transform/ (the dbt project itself) copied alongside it.
FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:0.11.28 /uv /uvx /usr/local/bin/

WORKDIR /app
COPY . .

# dbt-core/dbt-duckdb live in their own venv, isolated from the uv
# workspace's shared lockfile — no dbt-core version is compatible with both
# this workspace's mypy pin and apache-beam's protobuf pin (see
# error-handling/dbt-core-incompatible-with-shared-lockfile-mypy-and-flink-pins.md).
# dbt_runner.main invokes `dbt` as an external CLI (subprocess), found via
# PATH below — never imported as a Python dependency of dbt-runner itself.
RUN python -m venv /opt/dbt-venv && \
    /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-core>=1.9.0" "dbt-duckdb>=1.9.0"
ENV PATH="/opt/dbt-venv/bin:${PATH}"

RUN uv sync --frozen --package dbt-runner

CMD ["uv", "run", "--no-sync", "--package", "dbt-runner", "python", "-m", "dbt_runner.main"]
