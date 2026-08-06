# Build context is the repo root — dashboard is a uv workspace member and
# depends on sibling packages (libs/common, libs/shared-schemas) via the
# single shared lockfile.
FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:0.11.28 /uv /uvx /usr/local/bin/

WORKDIR /app
COPY . .

RUN uv sync --frozen --package dashboard

EXPOSE 8501

CMD ["uv", "run", "--no-sync", "--package", "dashboard", "streamlit", "run", \
     "dashboard/src/dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501"]
