variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "aws_access_key_id" {
  description = "AWS access key ID"
  type        = string
  sensitive   = true
}

variable "aws_secret_access_key" {
  description = "AWS secret access key"
  type        = string
  sensitive   = true
}

variable "bucket_name" {
  description = "S3 bucket name"
  type        = string
  default     = "ecommerce-data-platform-krutarth-2025"
}

variable "iam_user_name" {
  description = "IAM user for the ecommerce platform service account"
  type        = string
  default     = "ecommerce-platform-svc"
}

variable "iam_role_name" {
  description = "IAM role for Snowflake integration"
  type        = string
  default     = "ecommerce-snowflake-role"
}

variable "iam_policy_name" {
  description = "IAM policy name for S3 access"
  type        = string
  default     = "ecommerce-s3-policy"
}

variable "environment" {
  description = "Environment tag"
  type        = string
  default     = "production"
}

variable "project" {
  description = "Project tag"
  type        = string
  default     = "ecommerce-data-platform"
}
