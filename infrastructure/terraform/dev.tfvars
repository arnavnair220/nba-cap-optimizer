environment = "development"
aws_region  = "us-east-1"

# Database - smaller for dev
db_instance_class    = "db.t3.micro"
db_allocated_storage = 20

# Lambda - smaller memory for dev
lambda_memory_size = 512
lambda_timeout     = 300

tags = {
  CostCenter   = "development"
  AutoShutdown = "true"
}
