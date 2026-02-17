output "api_endpoint" {
  description = "API Gateway endpoint URL"
  value       = aws_api_gateway_deployment.main.invoke_url
}

output "frontend_bucket" {
  description = "S3 bucket name for frontend"
  value       = aws_s3_bucket.frontend.bucket
}

output "frontend_cloudfront_url" {
  description = "CloudFront distribution URL"
  value       = aws_cloudfront_distribution.frontend.domain_name
}

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

output "models_bucket" {
  description = "S3 bucket for ML models"
  value       = aws_s3_bucket.models.bucket
}

output "sagemaker_endpoint_name" {
  description = "SageMaker endpoint name"
  value       = aws_sagemaker_endpoint.main.name
}

output "step_function_arn" {
  description = "Step Functions state machine ARN"
  value       = aws_sfn_state_machine.etl_pipeline.arn
}

output "lambda_function_names" {
  description = "Map of Lambda function names"
  value = {
    fetch_data      = aws_lambda_function.fetch_data.function_name
    validate_data   = aws_lambda_function.validate_data.function_name
    transform_data  = aws_lambda_function.transform_data.function_name
    load_to_rds     = aws_lambda_function.load_to_rds.function_name
    api_handler     = aws_lambda_function.api_handler.function_name
  }
}
