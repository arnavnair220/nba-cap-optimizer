environment = "development"
aws_region  = "us-east-1"

# Database - smaller for dev
db_instance_class    = "db.t3.micro"
db_allocated_storage = 20

# Lambda - smaller memory for dev
lambda_memory_size = 512
lambda_timeout     = 300

# SageMaker - use spot instances for cost savings
sagemaker_instance_type          = "ml.m5.xlarge"
sagemaker_endpoint_instance_type = "ml.t3.medium"

# API Gateway - lower rate limits for dev
api_throttle_burst_limit = 100
api_throttle_rate_limit  = 50

tags = {
  CostCenter = "development"
  AutoShutdown = "true"
}
