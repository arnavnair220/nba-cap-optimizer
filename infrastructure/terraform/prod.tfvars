environment = "production"
aws_region  = "us-east-1"

# Database - production sizing
db_instance_class    = "db.t3.small"
db_allocated_storage = 50

# Lambda - more memory for prod performance
lambda_memory_size = 1024
lambda_timeout     = 300

# SageMaker
sagemaker_instance_type          = "ml.m5.xlarge"
sagemaker_endpoint_instance_type = "ml.t3.medium"

# API Gateway - higher rate limits for production
api_throttle_burst_limit = 500
api_throttle_rate_limit  = 100

tags = {
  CostCenter = "production"
  Backup = "true"
}
