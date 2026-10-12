variable "aws_profile" {
  description = <<-EOT
    Named AWS CLI profile to use. Required, with no default, on purpose:
    this project's AWS resources must only ever be created in the
    maintainer's personal AWS account, never in whatever account happens
    to be the CLI's [default] profile (e.g. a work account). Configure a
    dedicated profile first (`aws configure --profile <name>` or an SSO
    profile), then pass it explicitly, e.g.:
      terraform plan -var="aws_profile=personal" -var="iceberg_bucket_name=..."
  EOT
  type        = string
}

variable "expected_aws_account_id" {
  description = <<-EOT
    Account id Terraform must be targeting (your personal AWS account).
    No default, on purpose: if `aws_profile` ever resolves to the wrong
    account (e.g. a stale/misconfigured profile), the provider refuses to
    proceed instead of silently creating resources there. Set it once in
    terraform.tfvars (gitignored) to your personal account's 12-digit id.
  EOT
  type        = string
}

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

variable "ssh_allowed_cidr" {
  description = <<-EOT
    CIDR allowed to SSH into the demo instance, e.g. "203.0.113.4/32" for a
    single IP. No default, on purpose: this must never fall back to
    0.0.0.0/0 by accident.
  EOT
  type        = string
}

variable "ssh_key_name" {
  description = "Name of an existing EC2 key pair (in the target account/region) to install on the demo instance."
  type        = string
}
