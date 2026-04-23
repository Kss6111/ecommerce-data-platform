data "aws_caller_identity" "current" {}

# IAM User for the ecommerce platform service account
resource "aws_iam_user" "ecommerce_svc" {
  name = var.iam_user_name

  tags = {
    Environment = var.environment
    Project     = var.project
  }
}

# IAM Policy granting S3 access to the ecommerce bucket
# Description matches existing AWS resource to avoid forced replacement
resource "aws_iam_policy" "ecommerce_s3" {
  name        = var.iam_policy_name
  description = "S3 access for ecommerce data platform"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListBucket"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = ["arn:aws:s3:::${var.bucket_name}"]
      },
      {
        Sid    = "ReadWriteObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:GetObjectVersion",
          "s3:DeleteObjectVersion",
          "s3:ListMultipartUploadParts",
          "s3:AbortMultipartUpload"
        ]
        Resource = ["arn:aws:s3:::${var.bucket_name}/*"]
      }
    ]
  })

  tags = {
    Environment = var.environment
    Project     = var.project
  }
}

# Attach policy to the IAM user
resource "aws_iam_user_policy_attachment" "ecommerce_svc_s3" {
  user       = aws_iam_user.ecommerce_svc.name
  policy_arn = aws_iam_policy.ecommerce_s3.arn
}

# IAM Role for Snowflake integration
# Principal and ExternalId match existing AWS resource exactly to avoid breaking Snowflake stage
resource "aws_iam_role" "snowflake" {
  name        = var.iam_role_name
  description = "Role assumed by Snowflake for S3 external stage access"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::867418408889:user/sb0n1000-s"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = "LGC19742_SFCRole=4_enwN51It2keuWm4Q4nU6JOnvpjQ="
          }
        }
      }
    ]
  })

  tags = {
    Environment = var.environment
    Project     = var.project
  }
}

# Inline policy on the Snowflake role granting S3 read access
resource "aws_iam_role_policy" "snowflake_s3" {
  name = "snowflake-s3-access"
  role = aws_iam_role.snowflake.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "SnowflakeReadBucket"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = "arn:aws:s3:::${var.bucket_name}"
      },
      {
        Sid    = "SnowflakeReadObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion"
        ]
        Resource = "arn:aws:s3:::${var.bucket_name}/*"
      }
    ]
  })
}