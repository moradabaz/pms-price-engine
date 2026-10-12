#!/usr/bin/env bash
# Starts the portfolio demo: boots the EC2 instance (if stopped), waits for
# SSH, then remotely brings up docker-compose and Caddy.
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

ssh -i "${DEMO_SSH_KEY_PATH}" "${SSH_USER}@${public_ip}" bash -s <<REMOTE_SCRIPT
set -euo pipefail
cd "${REPO_PATH}"
git pull --ff-only
docker compose -f infra/docker-compose.yml up -d
sed "s/DNS_DOMAIN/${DEMO_DOMAIN}/" infra/aws/Caddyfile | sudo tee /etc/caddy/Caddyfile >/dev/null
sudo systemctl restart caddy || sudo caddy start --config /etc/caddy/Caddyfile
REMOTE_SCRIPT

echo "Demo live at https://${DEMO_DOMAIN}"
