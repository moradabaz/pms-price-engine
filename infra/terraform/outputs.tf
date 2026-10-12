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

output "instance_id" {
  description = "EC2 instance id for the portfolio demo (feed into start-demo.sh / stop-demo.sh as DEMO_INSTANCE_ID)."
  value       = aws_instance.demo.id
}

output "demo_elastic_ip" {
  description = "Elastic IP to point your DNS A record at."
  value       = aws_eip.demo.public_ip
}
