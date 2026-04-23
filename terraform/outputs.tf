output "bucket_arn" {
  description = "ARN of the ecommerce S3 bucket"
  value       = module.s3.bucket_arn
}

output "bucket_name" {
  description = "Name of the ecommerce S3 bucket"
  value       = module.s3.bucket_name
}

output "iam_user_arn" {
  description = "ARN of the ecommerce platform IAM user"
  value       = module.iam.iam_user_arn
}

output "iam_role_arn" {
  description = "ARN of the Snowflake integration IAM role"
  value       = module.iam.iam_role_arn
}

output "iam_policy_arn" {
  description = "ARN of the S3 access IAM policy"
  value       = module.iam.iam_policy_arn
}
