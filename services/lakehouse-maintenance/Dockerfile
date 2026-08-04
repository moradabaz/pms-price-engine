# Build context is the repo root (see infra/docker-compose.yml) — required
# because this service is a member of the uv workspace and depends on
# sibling packages (libs/common) via a single lockfile.
FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:0.11.28 /uv /uvx /usr/local/bin/

WORKDIR /app
COPY . .

RUN uv sync --frozen --package lakehouse-maintenance

CMD ["uv", "run", "--no-sync", "--package", "lakehouse-maintenance", "python", "-m", "lakehouse_maintenance.main"]
