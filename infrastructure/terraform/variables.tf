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
  default     = "nbacapdb"
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

# Bastion variables
variable "bastion_instance_type" {
  description = "EC2 instance type for bastion host"
  type        = string
  default     = "t3.micro"
}

variable "bastion_key_name" {
  description = "EC2 key pair name for bastion host SSH access (optional - leave empty to use SSM only)"
  type        = string
  default     = ""
}

variable "bastion_allowed_cidr" {
  description = "CIDR block allowed to SSH into bastion (e.g., your IP as x.x.x.x/32). Leave empty to disable SSH and use SSM only (recommended)"
  type        = string
  default     = ""
}

# Tags
variable "tags" {
  description = "Additional tags for resources"
  type        = map(string)
  default     = {}
}

# AWS CLI profile
variable "aws_profile" {
  description = "AWS CLI profile to use for deployments"
  type        = string
  default     = "personal-account"
}

# Current NBA season
variable "current_season" {
  description = "Current NBA season (YYYY-YY format)"
  type        = string
  default     = "2025-26"
}
