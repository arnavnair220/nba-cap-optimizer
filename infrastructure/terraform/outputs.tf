output "rds_endpoint" {
  description = "RDS database endpoint"
  value       = aws_db_instance.main.endpoint
  sensitive   = true
}

output "rds_database_name" {
  description = "RDS database name"
  value       = aws_db_instance.main.db_name
}

output "data_bucket" {
  description = "S3 bucket for data storage"
  value       = aws_s3_bucket.data.bucket
}

output "step_function_arn" {
  description = "Step Functions state machine ARN"
  value       = aws_sfn_state_machine.etl_pipeline.arn
}

output "lambda_function_names" {
  description = "Map of Lambda function names"
  value = {
    fetch_data     = aws_lambda_function.fetch_data.function_name
    validate_data  = aws_lambda_function.validate_data.function_name
    transform_data = aws_lambda_function.transform_data.function_name
    load_to_rds    = aws_lambda_function.load_to_rds.function_name
  }
}

output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "db_secret_arn" {
  description = "Secrets Manager ARN for database credentials"
  value       = aws_secretsmanager_secret.db_credentials.arn
}
