# Same shape as streaming/flink-jobs/Dockerfile — a second, independent
# Flink job on the same cluster (spec §2), not a variant of that job.
FROM apache/flink:2.3.0-java21

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-venv build-essential && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar

RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-2.3.0.jar /opt/flink/plugins/s3-fs-hadoop/

COPY --from=ghcr.io/astral-sh/uv:0.11.28 /uv /uvx /usr/local/bin/

WORKDIR /app
COPY . .

ENV UV_PYTHON_INSTALL_DIR=/opt/uv-python

RUN uv sync --frozen --package market-pulse-job && \
    chmod -R a+rX /opt/uv-python

ENV PATH="/app/.venv/bin:${PATH}"

USER flink
