#!/bin/bash
# Bootstraps LocalStack AWS resources on container startup.
# Runs automatically via /etc/localstack/init/ready.d/

set -euo pipefail

REGION="eu-west-1"
ENDPOINT="http://localhost:4566"

echo ">> [LocalStack] Creating Kinesis stream: market-price-events"
awslocal kinesis create-stream \
  --stream-name market-price-events \
  --shard-count 4 \
  --region "$REGION"

echo ">> [LocalStack] Creating S3 bucket: pms-iceberg"
awslocal s3 mb s3://pms-iceberg --region "$REGION"

echo ">> [LocalStack] Creating DynamoDB table: price_decision (spec 04 §10, ADR-0006)"
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

echo ">> [LocalStack] Bootstrap complete."
