variable "aws_region" {
  description = "AWS region for the demo deployment."
  type        = string
  default     = "eu-west-1"
}

variable "environment" {
  description = "Deployment environment label (used in resource tags)."
  type        = string
  default     = "demo"
}

variable "kinesis_shard_count" {
  description = "Number of shards for the market-price-events stream. 1 shard = 1 MB/s in, 2 MB/s out. Minimum 1 for demo."
  type        = number
  default     = 1
}

variable "iceberg_bucket_name" {
  description = "S3 bucket name for Iceberg data. Must be globally unique."
  type        = string
}
