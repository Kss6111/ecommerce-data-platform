import pathlib

files = {}

files["terraform/provider.tf"] = """\
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.7.0"
}

provider "aws" {
  region     = var.aws_region
  access_key = var.aws_access_key_id
  secret_key = var.aws_secret_access_key
}
"""

files["terraform/variables.tf"] = """\
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
"""

files["terraform/outputs.tf"] = """\
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
"""

files["terraform/main.tf"] = """\
module "s3" {
  source      = "./modules/s3"
  bucket_name = var.bucket_name
  environment = var.environment
  project     = var.project
}

module "iam" {
  source          = "./modules/iam"
  bucket_name     = var.bucket_name
  iam_user_name   = var.iam_user_name
  iam_role_name   = var.iam_role_name
  iam_policy_name = var.iam_policy_name
  environment     = var.environment
  project         = var.project
}
"""

files["terraform/terraform.tfvars"] = """\
aws_region   = "us-east-1"
bucket_name  = "ecommerce-data-platform-krutarth-2025"
iam_user_name   = "ecommerce-platform-svc"
iam_role_name   = "ecommerce-snowflake-role"
iam_policy_name = "ecommerce-s3-policy"
environment  = "production"
project      = "ecommerce-data-platform"
"""

files["terraform/modules/s3/main.tf"] = """\
resource "aws_s3_bucket" "ecommerce" {
  bucket = var.bucket_name

  tags = {
    Name        = var.bucket_name
    Environment = var.environment
    Project     = var.project
  }
}

resource "aws_s3_bucket_versioning" "ecommerce" {
  bucket = aws_s3_bucket.ecommerce.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "ecommerce" {
  bucket = aws_s3_bucket.ecommerce.id

  rule {
    id     = "expire-old-versions"
    status = "Enabled"

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }

  rule {
    id     = "transition-to-ia"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "ecommerce" {
  bucket = aws_s3_bucket.ecommerce.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
"""

files["terraform/modules/s3/variables.tf"] = """\
variable "bucket_name" {
  description = "S3 bucket name"
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
"""

files["terraform/modules/s3/outputs.tf"] = """\
output "bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.ecommerce.arn
}

output "bucket_name" {
  description = "Name of the S3 bucket"
  value       = aws_s3_bucket.ecommerce.id
}
"""

files["terraform/modules/iam/main.tf"] = """\
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
resource "aws_iam_policy" "ecommerce_s3" {
  name        = var.iam_policy_name
  description = "Grants ecommerce platform service account access to S3 bucket"

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
        Resource = "arn:aws:s3:::${var.bucket_name}"
      },
      {
        Sid    = "ReadWriteObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:GetObjectVersion"
        ]
        Resource = "arn:aws:s3:::${var.bucket_name}/*"
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
resource "aws_iam_role" "snowflake" {
  name        = var.iam_role_name
  description = "Role assumed by Snowflake for S3 external stage access"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "SnowflakeAssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = "ecommerce_snowflake_external_id"
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
"""

files["terraform/modules/iam/variables.tf"] = """\
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
"""

files["terraform/modules/iam/outputs.tf"] = """\
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
"""

for path, content in files.items():
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8", newline="\n") as f:
        f.write(content)
    print(f"Written: {path}")

print("\nAll Terraform files written.")