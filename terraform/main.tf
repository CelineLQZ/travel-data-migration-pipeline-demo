terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# -----------------------------------------------------------------------
# S3 Bucket
# -----------------------------------------------------------------------

resource "aws_s3_bucket" "demo" {
  bucket        = var.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "demo" {
  bucket                  = aws_s3_bucket.demo.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Create folder structure with empty placeholder objects
resource "aws_s3_object" "folders" {
  for_each = toset([
    "raw/hotels/",
    "raw/customers/",
    "raw/bookings/",
    "staging/hotels/",
    "staging/customers/",
    "staging/bookings/",
    "staging/errors/hotels/",
    "staging/errors/customers/",
    "staging/errors/bookings/",
    "scripts/",
  ])

  bucket  = aws_s3_bucket.demo.id
  key     = each.value
  content = ""
}

# -----------------------------------------------------------------------
# IAM User — for local scripts and Airflow
# -----------------------------------------------------------------------

resource "aws_iam_user" "demo_user" {
  name = var.iam_user_name
}

resource "aws_iam_user_policy_attachment" "s3_full_access" {
  user       = aws_iam_user.demo_user.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_user_policy_attachment" "iam_full_access" {
  user       = aws_iam_user.demo_user.name
  policy_arn = "arn:aws:iam::aws:policy/IAMFullAccess"
}

resource "aws_iam_user_policy" "emr_serverless_access" {
  name = "emr-serverless-full-access"
  user = aws_iam_user.demo_user.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "emr-serverless:*"
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = aws_iam_role.emr_execution_role.arn
      }
    ]
  })
}

resource "aws_iam_access_key" "demo_user_key" {
  user = aws_iam_user.demo_user.name
}

# -----------------------------------------------------------------------
# IAM Role — EMR Serverless execution
# -----------------------------------------------------------------------

resource "aws_iam_role" "emr_execution_role" {
  name = var.emr_role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "emr-serverless.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "emr_s3_access" {
  role       = aws_iam_role.emr_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "emr_service_policy" {
  role       = aws_iam_role.emr_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2"
}

resource "aws_iam_role_policy_attachment" "emr_full_access" {
  role       = aws_iam_role.emr_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEMRFullAccessPolicy_v2"
}

# -----------------------------------------------------------------------
# IAM Role — Snowflake S3 integration
# Note: trust policy is a placeholder. After running
#   DESC INTEGRATION s3_integration in Snowflake,
#   fill in snowflake_iam_user_arn and snowflake_external_id variables
#   and re-run `terraform apply` to update the trust policy.
# -----------------------------------------------------------------------

resource "aws_iam_role" "snowflake_s3_role" {
  name = var.snowflake_role_name

  assume_role_policy = var.snowflake_iam_user_arn != "" ? jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = var.snowflake_iam_user_arn }
      Action    = "sts:AssumeRole"
      Condition = {
        StringEquals = { "sts:ExternalId" = var.snowflake_external_id }
      }
    }]
  }) : jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "snowflake_s3_read" {
  name = "snowflake-s3-read-policy"
  role = aws_iam_role.snowflake_s3_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:ListBucket"]
      Resource = [
        aws_s3_bucket.demo.arn,
        "${aws_s3_bucket.demo.arn}/*"
      ]
    }]
  })
}

# -----------------------------------------------------------------------
# EMR Serverless Application
# -----------------------------------------------------------------------

resource "aws_emrserverless_application" "spark" {
  name          = var.emr_app_name
  release_label = "emr-7.11.0"
  type          = "SPARK"

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }
}

# -----------------------------------------------------------------------
# Data sources
# -----------------------------------------------------------------------

data "aws_caller_identity" "current" {}
