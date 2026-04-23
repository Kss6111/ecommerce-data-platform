// Workflow trigger note: documentation/comment-only changes under terraform/
// are enough to exercise the Terraform GitHub Actions plan/apply pipeline.

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

# DynamoDB table for Terraform remote state locking
resource "aws_dynamodb_table" "terraform_locks" {
  name         = "ecommerce-terraform-locks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  tags = {
    Environment = var.environment
    Project     = var.project
  }
}
