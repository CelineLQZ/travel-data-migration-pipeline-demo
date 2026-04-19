output "s3_bucket_name" {
  description = "S3 bucket name — use as S3_BUCKET in .env"
  value       = aws_s3_bucket.demo.id
}

output "aws_access_key_id" {
  description = "IAM user access key ID — use as AWS_ACCESS_KEY_ID in .env"
  value       = aws_iam_access_key.demo_user_key.id
}

output "aws_secret_access_key" {
  description = "IAM user secret key — use as AWS_SECRET_ACCESS_KEY in .env"
  value       = aws_iam_access_key.demo_user_key.secret
  sensitive   = true
}

output "emr_execution_role_arn" {
  description = "EMR execution role ARN — use as EMR_EXECUTION_ROLE_ARN in .env"
  value       = aws_iam_role.emr_execution_role.arn
}

output "emr_app_id" {
  description = "EMR Serverless application ID — use as EMR_APP_ID in .env"
  value       = aws_emrserverless_application.spark.id
}

output "snowflake_role_arn" {
  description = "Snowflake S3 role ARN — use as STORAGE_AWS_ROLE_ARN in Snowflake integration"
  value       = aws_iam_role.snowflake_s3_role.arn
}
