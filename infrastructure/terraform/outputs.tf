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

output "bastion_instance_id" {
  description = "Instance ID of bastion host"
  value       = aws_instance.bastion.id
}

output "bastion_public_ip" {
  description = "Public IP address of bastion host"
  value       = aws_instance.bastion.public_ip
}

output "ssm_port_forward_command" {
  description = "AWS SSM command to port forward to RDS (recommended - no SSH key needed)"
  value       = "aws ssm start-session --target ${aws_instance.bastion.id} --document-name AWS-StartPortForwardingSessionToRemoteHost --parameters '{\"host\":[\"${aws_db_instance.main.address}\"],\"portNumber\":[\"5432\"],\"localPortNumber\":[\"5432\"]}' --profile personal-account"
}

output "ssh_connection_command" {
  description = "SSH command to connect to bastion with port forwarding to RDS (requires SSH key)"
  value       = var.bastion_key_name != "" ? "ssh -i ${var.bastion_key_name}.pem -L 5432:${aws_db_instance.main.address}:5432 ec2-user@${aws_instance.bastion.public_ip}" : "SSH not configured - use SSM instead"
}

output "api_gateway_url" {
  description = "Base URL for the predictions API"
  value       = "${aws_api_gateway_stage.v1.invoke_url}"
}

output "api_endpoints" {
  description = "Available API endpoints"
  value = {
    predictions            = "${aws_api_gateway_stage.v1.invoke_url}/predictions"
    predictions_undervalued = "${aws_api_gateway_stage.v1.invoke_url}/predictions/undervalued"
    predictions_overvalued  = "${aws_api_gateway_stage.v1.invoke_url}/predictions/overvalued"
    predictions_player     = "${aws_api_gateway_stage.v1.invoke_url}/predictions/{player_name}"
    teams                  = "${aws_api_gateway_stage.v1.invoke_url}/teams"
    teams_detail           = "${aws_api_gateway_stage.v1.invoke_url}/teams/{team_abbreviation}"
  }
}

output "api_lambda_function" {
  description = "API Lambda function name"
  value       = aws_lambda_function.api_handler.function_name
}
