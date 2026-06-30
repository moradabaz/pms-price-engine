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

echo ">> [LocalStack] Creating DynamoDB table: apartment_prices"
awslocal dynamodb create-table \
  --table-name apartment_prices \
  --attribute-definitions \
      AttributeName=apartment_id,AttributeType=S \
  --key-schema \
      AttributeName=apartment_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region "$REGION"

echo ">> [LocalStack] Bootstrap complete."
