output "iam_user_arn" {
  description = "ARN of the IAM user"
  value       = aws_iam_user.ecommerce_svc.arn
}

output "iam_role_arn" {
  description = "ARN of the Snowflake IAM role"
  value       = aws_iam_role.snowflake.arn
}

output "iam_policy_arn" {
  description = "ARN of the S3 IAM policy"
  value       = aws_iam_policy.ecommerce_s3.arn
}
