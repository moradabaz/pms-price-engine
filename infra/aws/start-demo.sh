#!/usr/bin/env bash
# Starts the portfolio demo end-to-end: boots the EC2 instance (if stopped),
# waits for SSH, syncs build artifacts that are deliberately gitignored
# (uv.lock, the Debezium custom-converter jar), brings up docker-compose,
# and — idempotently — creates the Kafka topics, registers the Debezium
# connector, and submits both Flink jobs if they aren't already running.
# Finally points Caddy at the demo domain.
#
# Required environment variables (no defaults, on purpose — see
# infra/terraform/variables.tf for why the AWS side never falls back
# silently):
#   AWS_PROFILE       - the personal-account CLI profile (NOT "default")
#   DEMO_INSTANCE_ID  - from `terraform output -raw instance_id`
#   DEMO_SSH_KEY_PATH - path to the private key matching ssh_key_name
#   DEMO_DOMAIN       - e.g. demo.example.com, must already point at the
#                       instance's Elastic IP
set -euo pipefail

: "${AWS_PROFILE:?Set AWS_PROFILE to your personal-account profile, not default}"
: "${DEMO_INSTANCE_ID:?Set DEMO_INSTANCE_ID (terraform output -raw instance_id)}"
: "${DEMO_SSH_KEY_PATH:?Set DEMO_SSH_KEY_PATH}"
: "${DEMO_DOMAIN:?Set DEMO_DOMAIN}"

readonly SSH_USER="ec2-user"
readonly REPO_PATH="/opt/pms-price-engine"
readonly POLL_INTERVAL_SECONDS=5
readonly MAX_POLL_ATTEMPTS=60

readonly LOCAL_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly LOCAL_UV_LOCK="${LOCAL_REPO_ROOT}/uv.lock"
readonly LOCAL_CONVERTER_JAR="${LOCAL_REPO_ROOT}/infra/debezium/custom-converters/target/custom-converters-1.0.0.jar"
readonly REMOTE_CONVERTER_JAR_DIR="infra/debezium/custom-converters/target"

readonly DEBEZIUM_CONNECTOR_NAME="pms-payment-lines-connector"
readonly DEBEZIUM_CONNECTOR_CONFIG="infra/debezium/postgres-connector.json"
readonly DEBEZIUM_CONNECT_URL="http://localhost:8083"

readonly FLINK_REST_URL="http://localhost:8081"
readonly FLINK_PYTHON_BIN="/app/.venv/bin/python3"
readonly PRICING_JOB_NAME="pms-price-engine"
readonly PRICING_JOB_SCRIPT="/app/streaming/flink-jobs/src/flink_jobs/main.py"
readonly MARKET_PULSE_JOB_NAME="market-pulse-job"
readonly MARKET_PULSE_JOB_SCRIPT="/app/streaming/market-pulse-job/src/market_pulse_job/main.py"

# name:partitions — kept in sync with docs/manual/MANUAL.md's bring-up list
# plus pricing-strategies.v1 (added by a later phase, missing from that doc).
readonly KAFKA_TOPICS=(
  "payment-events.v1:6"
  "apartment-market-segments.v1:1"
  "market-price-bridge.v1:4"
  "owner-contracts.v1:1"
  "manual-overrides.v1:1"
  "booking-events.v1:1"
  "cost-definitions.v1:1"
  "cost-allocation-rules.v1:1"
  "company-cost-occurrences.v1:1"
  "pricing-strategies.v1:1"
)

if [[ ! -f "${LOCAL_UV_LOCK}" ]]; then
  echo "Missing ${LOCAL_UV_LOCK} — run 'uv lock' locally first." >&2
  exit 1
fi
if [[ ! -f "${LOCAL_CONVERTER_JAR}" ]]; then
  echo "Missing ${LOCAL_CONVERTER_JAR} — build infra/debezium/custom-converters first (mvn package)." >&2
  exit 1
fi

echo "Starting instance ${DEMO_INSTANCE_ID} (profile: ${AWS_PROFILE})..."
aws ec2 start-instances --profile "${AWS_PROFILE}" --instance-ids "${DEMO_INSTANCE_ID}"
aws ec2 wait instance-running --profile "${AWS_PROFILE}" --instance-ids "${DEMO_INSTANCE_ID}"

public_ip=$(aws ec2 describe-instances \
  --profile "${AWS_PROFILE}" \
  --instance-ids "${DEMO_INSTANCE_ID}" \
  --query "Reservations[0].Instances[0].PublicIpAddress" \
  --output text)
echo "Instance running at ${public_ip}, waiting for SSH..."

for ((attempt = 1; attempt <= MAX_POLL_ATTEMPTS; attempt++)); do
  if ssh -i "${DEMO_SSH_KEY_PATH}" -o StrictHostKeyChecking=accept-new \
      -o ConnectTimeout=5 "${SSH_USER}@${public_ip}" true 2>/dev/null; then
    break
  fi
  sleep "${POLL_INTERVAL_SECONDS}"
done

echo "Syncing gitignored build artifacts (uv.lock, Debezium converter jar)..."
ssh -i "${DEMO_SSH_KEY_PATH}" "${SSH_USER}@${public_ip}" \
  "mkdir -p ${REPO_PATH}/${REMOTE_CONVERTER_JAR_DIR}"
scp -i "${DEMO_SSH_KEY_PATH}" -q "${LOCAL_UV_LOCK}" \
  "${SSH_USER}@${public_ip}:${REPO_PATH}/uv.lock"
scp -i "${DEMO_SSH_KEY_PATH}" -q "${LOCAL_CONVERTER_JAR}" \
  "${SSH_USER}@${public_ip}:${REPO_PATH}/${REMOTE_CONVERTER_JAR_DIR}/custom-converters-1.0.0.jar"

ssh -i "${DEMO_SSH_KEY_PATH}" "${SSH_USER}@${public_ip}" bash -s <<REMOTE_SCRIPT
set -euo pipefail
cd "${REPO_PATH}"
git pull --ff-only
docker compose -f infra/docker-compose.yml up -d --build

echo "Waiting for Kafka Connect to be ready..."
until curl -sf ${DEBEZIUM_CONNECT_URL}/connectors >/dev/null 2>&1; do sleep "${POLL_INTERVAL_SECONDS}"; done

echo "Creating Kafka topics (idempotent)..."
for entry in ${KAFKA_TOPICS[@]}; do
  topic="\${entry%%:*}"
  partitions="\${entry##*:}"
  docker exec pms_kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "\${topic}" --partitions "\${partitions}" --replication-factor 1
done

echo "Registering Debezium connector (idempotent)..."
connector_status=\$(curl -s -o /dev/null -w "%{http_code}" "${DEBEZIUM_CONNECT_URL}/connectors/${DEBEZIUM_CONNECTOR_NAME}/status")
if [[ "\${connector_status}" != "200" ]]; then
  curl -s -X POST -H "Content-Type: application/json" \
    --data @"${DEBEZIUM_CONNECTOR_CONFIG}" "${DEBEZIUM_CONNECT_URL}/connectors"
fi

echo "Waiting for the Flink cluster to be ready..."
until curl -sf ${FLINK_REST_URL}/overview >/dev/null 2>&1; do sleep "${POLL_INTERVAL_SECONDS}"; done

echo "Submitting Flink jobs (idempotent, skips jobs already RUNNING)..."
running_jobs=\$(docker exec pms_flink_jobmanager flink list 2>/dev/null | grep RUNNING || true)
if ! grep -q "${PRICING_JOB_NAME}" <<<"\${running_jobs}"; then
  docker exec -d pms_flink_jobmanager flink run \
    -pyclientexec ${FLINK_PYTHON_BIN} -pyexec ${FLINK_PYTHON_BIN} -py ${PRICING_JOB_SCRIPT}
fi
if ! grep -q "${MARKET_PULSE_JOB_NAME}" <<<"\${running_jobs}"; then
  docker exec -d pms_flink_jobmanager flink run \
    -pyclientexec ${FLINK_PYTHON_BIN} -pyexec ${FLINK_PYTHON_BIN} -py ${MARKET_PULSE_JOB_SCRIPT}
fi

sed "s/DNS_DOMAIN/${DEMO_DOMAIN}/" infra/aws/Caddyfile | sudo tee /etc/caddy/Caddyfile >/dev/null
sudo systemctl restart caddy || sudo caddy start --config /etc/caddy/Caddyfile
REMOTE_SCRIPT

echo "Demo live at https://${DEMO_DOMAIN}"
