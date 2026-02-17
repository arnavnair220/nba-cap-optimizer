variable "environment" {
  description = "Environment name (development, production)"
  type        = string
}

variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "nba-cap"
}

# Database variables
variable "db_password" {
  description = "RDS database master password"
  type        = string
  sensitive   = true
}

variable "db_username" {
  description = "RDS database master username"
  type        = string
  default     = "admin"
}

variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.micro"
}

variable "db_allocated_storage" {
  description = "RDS allocated storage in GB"
  type        = number
  default     = 20
}

# Lambda variables
variable "lambda_runtime" {
  description = "Lambda runtime version"
  type        = string
  default     = "python3.11"
}

variable "lambda_timeout" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 300
}

variable "lambda_memory_size" {
  description = "Lambda memory size in MB"
  type        = number
  default     = 512
}

# SageMaker variables
variable "sagemaker_instance_type" {
  description = "SageMaker instance type for training/processing"
  type        = string
  default     = "ml.m5.xlarge"
}

variable "sagemaker_endpoint_instance_type" {
  description = "SageMaker endpoint instance type"
  type        = string
  default     = "ml.t3.medium"
}

# API Gateway variables
variable "api_stage_name" {
  description = "API Gateway stage name"
  type        = string
  default     = "v1"
}

variable "api_throttle_burst_limit" {
  description = "API Gateway throttle burst limit"
  type        = number
  default     = 500
}

variable "api_throttle_rate_limit" {
  description = "API Gateway throttle rate limit"
  type        = number
  default     = 100
}

# Frontend variables
variable "frontend_domain_name" {
  description = "Custom domain name for frontend (optional)"
  type        = string
  default     = ""
}

# Tags
variable "tags" {
  description = "Additional tags for resources"
  type        = map(string)
  default     = {}
}
