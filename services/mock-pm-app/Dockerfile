# Build context is the repo root (see infra/docker-compose.yml) — required
# because this service is a member of the uv workspace and depends on
# sibling packages (libs/common, libs/shared-schemas) via a single lockfile.
FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:0.11.28 /uv /uvx /usr/local/bin/

WORKDIR /app
COPY . .

RUN uv sync --frozen --package mock-pm-app

CMD ["uv", "run", "--no-sync", "--package", "mock-pm-app", "python", "-m", "mock_pm_app.main"]
