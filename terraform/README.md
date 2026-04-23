# Terraform Infrastructure as Code

## Overview

This directory manages the AWS infrastructure for the ecommerce data platform using Terraform. All resources are defined as code, versioned in Git, and state is stored remotely in S3 with DynamoDB locking.

## Structure

- `terraform/`
  - `main.tf` — Root module, calls child modules and defines DynamoDB lock table
  - `provider.tf` — AWS provider configuration and S3 remote backend
  - `variables.tf` — Input variable declarations
  - `outputs.tf` — Output value declarations
  - `terraform.tfvars` — Variable values (non-sensitive only)
  - `modules/`
    - `s3/`
      - `main.tf` — S3 bucket, versioning, lifecycle rules, encryption
      - `variables.tf` — Module input variables
      - `outputs.tf` — Bucket ARN and name outputs
    - `iam/`
      - `main.tf` — IAM user, policy, role, and attachments
      - `variables.tf` — Module input variables
      - `outputs.tf` — IAM ARN outputs

## Resources Managed

### S3 Module (`modules/s3`)
| Resource | Name | Description |
|---|---|---|
| `aws_s3_bucket` | `ecommerce-data-platform-krutarth-2025` | Main data lake bucket |
| `aws_s3_bucket_versioning` | — | Versioning enabled |
| `aws_s3_bucket_lifecycle_configuration` | — | 30-day IA transition, 90-day old version expiry |
| `aws_s3_bucket_server_side_encryption_configuration` | — | AES256 encryption at rest |

### IAM Module (`modules/iam`)
| Resource | Name | Description |
|---|---|---|
| `aws_iam_user` | `ecommerce-platform-svc` | Service account for pipeline access |
| `aws_iam_policy` | `ecommerce-s3-policy` | S3 read/write policy for the service account |
| `aws_iam_user_policy_attachment` | — | Attaches policy to service account user |
| `aws_iam_role` | `ecommerce-snowflake-role` | Role assumed by Snowflake for S3 external stage |
| `aws_iam_role_policy` | `snowflake-s3-access` | Inline S3 read policy on Snowflake role |

### Root Module
| Resource | Name | Description |
|---|---|---|
| `aws_dynamodb_table` | `ecommerce-terraform-locks` | State locking table |

## Remote State

State is stored in S3 with DynamoDB locking:

- **State file:** `s3://ecommerce-data-platform-krutarth-2025/terraform/state/terraform.tfstate`
- **Lock table:** `ecommerce-terraform-locks` (DynamoDB, `us-east-1`)
- **Encryption:** enabled at rest

## Usage

### Prerequisites
- Terraform >= 1.7.0
- AWS credentials with IAM and S3 permissions

### Initialize
```bash
terraform init
```

### Plan
```bash
terraform plan \
  -var="aws_access_key_id=$AWS_ACCESS_KEY_ID" \
  -var="aws_secret_access_key=$AWS_SECRET_ACCESS_KEY"
```

### Apply
```bash
terraform apply \
  -var="aws_access_key_id=$AWS_ACCESS_KEY_ID" \
  -var="aws_secret_access_key=$AWS_SECRET_ACCESS_KEY"
```

### Import existing resources (first-time only)
All existing AWS resources were imported rather than recreated:
```bash
terraform import module.s3.aws_s3_bucket.ecommerce ecommerce-data-platform-krutarth-2025
terraform import module.iam.aws_iam_user.ecommerce_svc ecommerce-platform-svc
terraform import module.iam.aws_iam_role.snowflake ecommerce-snowflake-role
terraform import module.iam.aws_iam_policy.ecommerce_s3 arn:aws:iam::304928288032:policy/ecommerce-s3-policy
terraform import module.iam.aws_iam_user_policy_attachment.ecommerce_svc_s3 "ecommerce-platform-svc/arn:aws:iam::304928288032:policy/ecommerce-s3-policy"
```

## Important Notes

- **Never run `terraform apply` without `terraform plan` first** — always review the plan for unexpected destroys
- **The Snowflake IAM role** (`ecommerce-snowflake-role`) has a specific Principal and ExternalId tied to the live Snowflake account — do not modify the `assume_role_policy` without updating the Snowflake external stage configuration first
- **Sensitive variables** (`aws_access_key_id`, `aws_secret_access_key`) are never stored in `terraform.tfvars` — always passed via `-var` flags or environment variables
- **`terraform.tfstate` is gitignored** — state lives in S3 only