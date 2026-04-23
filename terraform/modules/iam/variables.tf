variable "bucket_name" {
  description = "S3 bucket name to grant access to"
  type        = string
}

variable "iam_user_name" {
  description = "IAM user name for the service account"
  type        = string
}

variable "iam_role_name" {
  description = "IAM role name for Snowflake integration"
  type        = string
}

variable "iam_policy_name" {
  description = "IAM policy name"
  type        = string
}

variable "environment" {
  description = "Environment tag"
  type        = string
}

variable "project" {
  description = "Project tag"
  type        = string
}
