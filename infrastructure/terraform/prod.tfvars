environment = "production"
aws_region  = "us-east-1"

# Database - production sizing
db_instance_class    = "db.t3.small"
db_allocated_storage = 50

# Lambda - more memory for prod performance
lambda_memory_size = 1024
lambda_timeout     = 300

tags = {
  CostCenter = "production"
  Backup     = "true"
}
