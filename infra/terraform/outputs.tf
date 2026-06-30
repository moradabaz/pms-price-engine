output "kinesis_stream_arn" {
  description = "ARN of the market-price-events Kinesis stream."
  value       = aws_kinesis_stream.market_price_events.arn
}

output "iceberg_bucket_name" {
  description = "S3 bucket name for Iceberg."
  value       = aws_s3_bucket.iceberg.bucket
}

output "dynamodb_table_name" {
  description = "DynamoDB table for apartment hot-path prices."
  value       = aws_dynamodb_table.apartment_prices.name
}
