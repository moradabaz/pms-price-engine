terraform {
  required_version = ">= 1.8.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

locals {
  common_tags = {
    Project     = "pms-price-engine"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# ---------------------------------------------------------------------------
# Kinesis — market-price-events (published by market-ingestor)
# ---------------------------------------------------------------------------
resource "aws_kinesis_stream" "market_price_events" {
  name             = "market-price-events"
  shard_count      = var.kinesis_shard_count
  retention_period = 24

  tags = local.common_tags
}

# ---------------------------------------------------------------------------
# S3 — Iceberg cold path (full pricing decision history + audit trail)
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "iceberg" {
  bucket = var.iceberg_bucket_name
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "iceberg" {
  bucket = aws_s3_bucket.iceberg.id
  versioning_configuration {
    status = "Enabled"
  }
}

# ---------------------------------------------------------------------------
# DynamoDB — hot path (current recommended price per apartment, low-latency)
# ---------------------------------------------------------------------------
resource "aws_dynamodb_table" "apartment_prices" {
  name         = "apartment_prices"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "apartment_id"

  attribute {
    name = "apartment_id"
    type = "S"
  }

  tags = local.common_tags
}
