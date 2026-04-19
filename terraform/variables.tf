variable "aws_region" {
  description = "AWS region for all resources"
  default     = "eu-north-1"
}

variable "bucket_name" {
  description = "S3 bucket name (must be globally unique)"
  default     = "travel-industry-migration-demo-celineli"
}

variable "iam_user_name" {
  description = "IAM user for local scripts and Airflow"
  default     = "travel-industry-demo-user"
}

variable "emr_role_name" {
  description = "IAM role for EMR Serverless execution"
  default     = "travel_industry_demo_role"
}

variable "snowflake_role_name" {
  description = "IAM role for Snowflake S3 integration"
  default     = "snowflake-s3-role"
}

variable "emr_app_name" {
  description = "EMR Serverless application name"
  default     = "travel-industry-migration-demo-2"
}

# Filled in after running: DESC INTEGRATION s3_integration in Snowflake
variable "snowflake_iam_user_arn" {
  description = "Snowflake's IAM user ARN (from DESC INTEGRATION s3_integration)"
  default     = "arn:aws:iam::871308866360:user/8u0m1000-s"
}

variable "snowflake_external_id" {
  description = "Snowflake's external ID (from DESC INTEGRATION s3_integration)"
  default     = "BQ45936_SFCRole=6_05bkbS07jkGdyRLasJfB973Flzg="
}
