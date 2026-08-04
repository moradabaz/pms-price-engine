#!/bin/bash
# Bootstraps LocalStack AWS resources on container startup.
# Runs automatically via /etc/localstack/init/ready.d/
#
# Idempotent by design (existence-check-then-create for every resource) —
# PERSISTENCE=1 (docker-compose.yml) means the localstack_data volume
# survives a container restart, so every resource below already exists on
# any restart after the first. Live-verified 2026-08-04: without these
# guards, `set -e` aborted the whole script on the very first command
# (Kinesis stream already exists) and every later resource — including
# stream_checkpoints/pms-lakehouse/pms_lakehouse, added for Phase 5 — was
# silently never created on a restart. See
# error-handling/localstack-init-script-not-idempotent-against-persisted-state.md.

set -euo pipefail

REGION="eu-west-1"
ENDPOINT="http://localhost:4566"

echo ">> [LocalStack] Creating Kinesis stream: market-price-events"
awslocal kinesis describe-stream --stream-name market-price-events --region "$REGION" >/dev/null 2>&1 || \
awslocal kinesis create-stream \
  --stream-name market-price-events \
  --shard-count 4 \
  --region "$REGION"

echo ">> [LocalStack] Creating S3 bucket: pms-iceberg (Flink checkpoints)"
awslocal s3api head-bucket --bucket pms-iceberg --region "$REGION" >/dev/null 2>&1 || \
awslocal s3 mb s3://pms-iceberg --region "$REGION"

echo ">> [LocalStack] Creating S3 bucket: pms-lakehouse (spec 05 §10, separate lifecycle from Flink checkpoints)"
awslocal s3api head-bucket --bucket pms-lakehouse --region "$REGION" >/dev/null 2>&1 || \
awslocal s3 mb s3://pms-lakehouse --region "$REGION"

echo ">> [LocalStack] Creating DynamoDB table: price_decision (spec 04 §10, ADR-0006)"
awslocal dynamodb describe-table --table-name price_decision --region "$REGION" >/dev/null 2>&1 || \
awslocal dynamodb create-table \
  --table-name price_decision \
  --attribute-definitions \
      AttributeName=apartment_id,AttributeType=S \
      AttributeName=target_date,AttributeType=S \
  --key-schema \
      AttributeName=apartment_id,KeyType=HASH \
      AttributeName=target_date,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES \
  --region "$REGION"

echo ">> [LocalStack] Creating DynamoDB table: stream_checkpoints (spec 05 §5/§10 — lakehouse-consumer's per-shard offsets)"
awslocal dynamodb describe-table --table-name stream_checkpoints --region "$REGION" >/dev/null 2>&1 || \
awslocal dynamodb create-table \
  --table-name stream_checkpoints \
  --attribute-definitions \
      AttributeName=shard_id,AttributeType=S \
  --key-schema \
      AttributeName=shard_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region "$REGION"

echo ">> [LocalStack] Bootstrap complete."
# No Glue database step here — confirmed 2026-08-04, AWS Glue is Ultimate-tier
# only in LocalStack (https://docs.localstack.cloud/references/coverage/coverage_glue/),
# not available in Community at all. pms_lakehouse is created by PyIceberg's
# SqlCatalog itself (create_namespace_if_not_exists, in iceberg_writer.py's
# ensure_table()) — no separate provisioning step needed. See
# error-handling/localstack-glue-is-ultimate-tier-only-not-community.md.
