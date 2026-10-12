#!/usr/bin/env bash
# Stops the portfolio demo: tears down docker-compose and Caddy on the
# instance, then stops the EC2 instance itself (not terminates — the EBS
# volume and Elastic IP survive, so start-demo.sh can bring it back
# without rebuilding anything).
set -euo pipefail

: "${AWS_PROFILE:?Set AWS_PROFILE to your personal-account profile, not default}"
: "${DEMO_INSTANCE_ID:?Set DEMO_INSTANCE_ID (terraform output -raw instance_id)}"
: "${DEMO_SSH_KEY_PATH:?Set DEMO_SSH_KEY_PATH}"

readonly SSH_USER="ec2-user"
readonly REPO_PATH="/opt/pms-price-engine"

public_ip=$(aws ec2 describe-instances \
  --profile "${AWS_PROFILE}" \
  --instance-ids "${DEMO_INSTANCE_ID}" \
  --query "Reservations[0].Instances[0].PublicIpAddress" \
  --output text)

if [[ "${public_ip}" != "None" && -n "${public_ip}" ]]; then
  ssh -i "${DEMO_SSH_KEY_PATH}" -o ConnectTimeout=5 "${SSH_USER}@${public_ip}" bash -s <<REMOTE_SCRIPT || true
set -euo pipefail
cd "${REPO_PATH}"
docker compose -f infra/docker-compose.yml down
sudo systemctl stop caddy || sudo caddy stop || true
REMOTE_SCRIPT
else
  echo "Instance has no public IP (already stopped?); skipping remote teardown."
fi

echo "Stopping instance ${DEMO_INSTANCE_ID} (profile: ${AWS_PROFILE})..."
aws ec2 stop-instances --profile "${AWS_PROFILE}" --instance-ids "${DEMO_INSTANCE_ID}"
aws ec2 wait instance-stopped --profile "${AWS_PROFILE}" --instance-ids "${DEMO_INSTANCE_ID}"
echo "Instance stopped. Only EBS + Elastic IP charges accrue while off."
