locals {
  name_prefix = "${var.project_name}-${var.environment}"

  common_tags = merge(
    var.tags,
    {
      Environment = var.environment
      Project     = var.project_name
    }
  )
}

# ============================================================================
# S3 BUCKET (Data Storage Only)
# ============================================================================

# Data storage bucket (raw and processed data)
resource "aws_s3_bucket" "data" {
  bucket = "${local.name_prefix}-data"
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ============================================================================
# VPC & NETWORKING (for RDS)
# ============================================================================

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = merge(
    local.common_tags,
    {
      Name = "${local.name_prefix}-vpc"
    }
  )
}

resource "aws_subnet" "private_a" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = "${var.aws_region}a"

  tags = merge(
    local.common_tags,
    {
      Name = "${local.name_prefix}-private-a"
    }
  )
}

resource "aws_subnet" "private_b" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.2.0/24"
  availability_zone = "${var.aws_region}b"

  tags = merge(
    local.common_tags,
    {
      Name = "${local.name_prefix}-private-b"
    }
  )
}

# Public subnet for bastion host
resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.10.0/24"
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = true

  tags = merge(
    local.common_tags,
    {
      Name = "${local.name_prefix}-public"
    }
  )
}

# Internet Gateway
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = merge(
    local.common_tags,
    {
      Name = "${local.name_prefix}-igw"
    }
  )
}

# Public route table
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = merge(
    local.common_tags,
    {
      Name = "${local.name_prefix}-public-rt"
    }
  )
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_db_subnet_group" "main" {
  name       = "${local.name_prefix}-db-subnet"
  subnet_ids = [aws_subnet.private_a.id, aws_subnet.private_b.id]

  tags = merge(
    local.common_tags,
    {
      Name = "${local.name_prefix}-db-subnet-group"
    }
  )
}

resource "aws_security_group" "rds" {
  name        = "${local.name_prefix}-rds-sg"
  description = "Security group for RDS PostgreSQL"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "PostgreSQL from VPC"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound"
  }

  tags = local.common_tags
}

# Allow RDS access from bastion
resource "aws_security_group_rule" "rds_from_bastion" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.bastion.id
  security_group_id        = aws_security_group.rds.id
  description              = "PostgreSQL from bastion host"
}

resource "aws_security_group" "lambda" {
  name        = "${local.name_prefix}-lambda-sg"
  description = "Security group for Lambda functions"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound"
  }

  tags = local.common_tags
}

# Bastion host security group
resource "aws_security_group" "bastion" {
  name        = "${local.name_prefix}-bastion-sg"
  description = "Security group for bastion host"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound"
  }

  tags = local.common_tags
}

# Optional SSH ingress rule (only if bastion_allowed_cidr is provided)
resource "aws_security_group_rule" "bastion_ssh" {
  count = var.bastion_allowed_cidr != "" ? 1 : 0

  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = [var.bastion_allowed_cidr]
  description       = "SSH from allowed IP"
  security_group_id = aws_security_group.bastion.id
}

# Route tables for private subnets (needed for VPC endpoints)
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  tags = merge(
    local.common_tags,
    {
      Name = "${local.name_prefix}-private-rt"
    }
  )
}

resource "aws_route_table_association" "private_a" {
  subnet_id      = aws_subnet.private_a.id
  route_table_id = aws_route_table.private.id
}

resource "aws_route_table_association" "private_b" {
  subnet_id      = aws_subnet.private_b.id
  route_table_id = aws_route_table.private.id
}

# Security group for VPC endpoints
resource "aws_security_group" "vpc_endpoints" {
  name        = "${local.name_prefix}-vpc-endpoints-sg"
  description = "Security group for VPC endpoints"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "HTTPS from VPC"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound"
  }

  tags = local.common_tags
}

# S3 VPC Gateway Endpoint (FREE - allows Lambda to access S3 without internet)
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]

  tags = merge(
    local.common_tags,
    {
      Name = "${local.name_prefix}-s3-endpoint"
    }
  )
}

# Secrets Manager VPC Interface Endpoint (allows Lambda to access Secrets Manager without internet)
resource "aws_vpc_endpoint" "secretsmanager" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.secretsmanager"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = merge(
    local.common_tags,
    {
      Name = "${local.name_prefix}-secretsmanager-endpoint"
    }
  )
}

# ============================================================================
# RDS POSTGRESQL
# ============================================================================

resource "aws_db_instance" "main" {
  identifier     = "${local.name_prefix}-db"
  engine         = "postgres"
  engine_version = "15"

  instance_class    = var.db_instance_class
  allocated_storage = var.db_allocated_storage
  storage_encrypted = false  # Disabled for AWS free tier compatibility

  db_name  = "nba_cap_optimizer"
  username = var.db_username
  password = var.db_password

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]

  backup_retention_period = 1  # Limited to 1 day for AWS free tier
  backup_window          = "03:00-04:00"
  maintenance_window     = "sun:04:00-sun:05:00"

  skip_final_snapshot       = var.environment != "production"
  final_snapshot_identifier = var.environment == "production" ? "${local.name_prefix}-final-snapshot-${formatdate("YYYY-MM-DD-hhmm", timestamp())}" : null

  # CloudWatch logs exports disabled for AWS free tier
  # enabled_cloudwatch_logs_exports = ["postgresql", "upgrade"]

  tags = local.common_tags
}

# ============================================================================
# BASTION HOST
# ============================================================================

# Get latest Amazon Linux 2023 AMI
data "aws_ami" "amazon_linux_2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# IAM role for bastion host (for SSM access)
resource "aws_iam_role" "bastion" {
  name = "${local.name_prefix}-bastion-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

# Attach SSM managed policy to bastion role
resource "aws_iam_role_policy_attachment" "bastion_ssm" {
  role       = aws_iam_role.bastion.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# IAM instance profile for bastion
resource "aws_iam_instance_profile" "bastion" {
  name = "${local.name_prefix}-bastion-profile"
  role = aws_iam_role.bastion.name

  tags = local.common_tags
}

# Bastion host EC2 instance
resource "aws_instance" "bastion" {
  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = var.bastion_instance_type
  subnet_id              = aws_subnet.public.id
  iam_instance_profile   = aws_iam_instance_profile.bastion.name

  vpc_security_group_ids = [aws_security_group.bastion.id]
  key_name               = var.bastion_key_name != "" ? var.bastion_key_name : null

  user_data = <<-EOF
              #!/bin/bash
              yum install -y postgresql15 amazon-ssm-agent
              systemctl enable amazon-ssm-agent
              systemctl start amazon-ssm-agent
              nohup yum update -y &
              EOF

  tags = merge(
    local.common_tags,
    {
      Name = "${local.name_prefix}-bastion"
    }
  )
}

# ============================================================================
# IAM ROLES
# ============================================================================

# Lambda execution role
resource "aws_iam_role" "lambda_execution" {
  name = "${local.name_prefix}-lambda-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_vpc" {
  role       = aws_iam_role.lambda_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "lambda_s3_rds" {
  name = "${local.name_prefix}-lambda-s3-rds"
  role = aws_iam_role.lambda_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = aws_secretsmanager_secret.db_credentials.arn
      }
    ]
  })
}

# Step Functions execution role
resource "aws_iam_role" "step_functions" {
  name = "${local.name_prefix}-step-functions"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "step_functions_lambda" {
  name = "${local.name_prefix}-step-functions-lambda"
  role = aws_iam_role.step_functions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = "arn:aws:lambda:${var.aws_region}:*:function:${local.name_prefix}-*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}

# ============================================================================
# SECRETS MANAGER (for DB credentials)
# ============================================================================

resource "aws_secretsmanager_secret" "db_credentials" {
  name = "${local.name_prefix}-db-credentials"
  tags = local.common_tags
}

resource "aws_secretsmanager_secret_version" "db_credentials" {
  secret_id = aws_secretsmanager_secret.db_credentials.id

  secret_string = jsonencode({
    username = var.db_username
    password = var.db_password
    host     = aws_db_instance.main.address
    port     = aws_db_instance.main.port
    dbname   = aws_db_instance.main.db_name
  })
}

# ============================================================================
# LAMBDA FUNCTIONS (ETL Pipeline Only)
# ============================================================================
#
# Note: Lambda layers are managed by CI/CD (not Terraform) for faster deployments.
# The deploy-backend job publishes layer versions and attaches them to functions.

# Fetch data Lambda
resource "aws_lambda_function" "fetch_data" {
  function_name = "${local.name_prefix}-fetch-data"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "src.etl.fetch_data.handler"
  runtime       = var.lambda_runtime
  timeout       = var.lambda_timeout
  memory_size   = var.lambda_memory_size

  filename         = "placeholder.zip"
  source_code_hash = filebase64sha256("placeholder.zip")

  environment {
    variables = {
      ENVIRONMENT = var.environment
      DATA_BUCKET = aws_s3_bucket.data.bucket
    }
  }

  tags = local.common_tags

  lifecycle {
    ignore_changes = [
      filename,
      source_code_hash,
      layers  # Managed by CI/CD
    ]
  }
}

# Validate data Lambda
resource "aws_lambda_function" "validate_data" {
  function_name = "${local.name_prefix}-validate-data"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "src.etl.validate_data.handler"
  runtime       = var.lambda_runtime
  timeout       = var.lambda_timeout
  memory_size   = var.lambda_memory_size

  filename         = "placeholder.zip"
  source_code_hash = filebase64sha256("placeholder.zip")

  environment {
    variables = {
      ENVIRONMENT = var.environment
      DATA_BUCKET = aws_s3_bucket.data.bucket
    }
  }

  tags = local.common_tags

  lifecycle {
    ignore_changes = [
      filename,
      source_code_hash,
      layers  # Managed by CI/CD
    ]
  }
}

# Transform data Lambda
resource "aws_lambda_function" "transform_data" {
  function_name = "${local.name_prefix}-transform-data"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "src.etl.transform_data.handler"
  runtime       = var.lambda_runtime
  timeout       = var.lambda_timeout
  memory_size   = var.lambda_memory_size

  filename         = "placeholder.zip"
  source_code_hash = filebase64sha256("placeholder.zip")

  environment {
    variables = {
      ENVIRONMENT = var.environment
      DATA_BUCKET = aws_s3_bucket.data.bucket
    }
  }

  tags = local.common_tags

  lifecycle {
    ignore_changes = [
      filename,
      source_code_hash,
      layers  # Managed by CI/CD
    ]
  }
}

# Load to RDS Lambda
resource "aws_lambda_function" "load_to_rds" {
  function_name = "${local.name_prefix}-load-to-rds"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "src.etl.load_to_rds.handler"
  runtime       = var.lambda_runtime
  timeout       = var.lambda_timeout
  memory_size   = var.lambda_memory_size

  filename         = "placeholder.zip"
  source_code_hash = filebase64sha256("placeholder.zip")

  vpc_config {
    subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      ENVIRONMENT   = var.environment
      DATA_BUCKET   = aws_s3_bucket.data.bucket
      DB_SECRET_ARN = aws_secretsmanager_secret.db_credentials.arn
    }
  }

  tags = local.common_tags

  lifecycle {
    ignore_changes = [
      filename,
      source_code_hash,
      layers  # Managed by CI/CD
    ]
  }
}

# ============================================================================
# STEP FUNCTIONS (ETL Pipeline)
# ============================================================================

resource "aws_sfn_state_machine" "etl_pipeline" {
  name     = "${local.name_prefix}-etl-pipeline"
  role_arn = aws_iam_role.step_functions.arn

  definition = jsonencode({
    Comment = "Daily ETL pipeline for NBA stats"
    StartAt = "FetchData"
    States = {
      FetchData = {
        Type     = "Task"
        Resource = aws_lambda_function.fetch_data.arn
        Next     = "ValidateData"
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "HandleError"
          }
        ]
      }
      ValidateData = {
        Type     = "Task"
        Resource = aws_lambda_function.validate_data.arn
        Next     = "TransformData"
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "HandleError"
          }
        ]
      }
      TransformData = {
        Type     = "Task"
        Resource = aws_lambda_function.transform_data.arn
        Next     = "LoadToRDS"
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "HandleError"
          }
        ]
      }
      LoadToRDS = {
        Type     = "Task"
        Resource = aws_lambda_function.load_to_rds.arn
        End      = true
      }
      HandleError = {
        Type  = "Fail"
        Cause = "ETL Pipeline failed"
      }
    }
  })

  tags = local.common_tags
}

# ============================================================================
# EVENTBRIDGE (Schedule for ETL)
# ============================================================================

# Daily schedule for player stats only
resource "aws_cloudwatch_event_rule" "daily_etl" {
  name                = "${local.name_prefix}-daily-etl"
  description         = "Trigger daily ETL pipeline for stats at 6 AM UTC"
  schedule_expression = "cron(0 6 * * ? *)"
  tags                = local.common_tags
}

resource "aws_cloudwatch_event_target" "daily_etl_pipeline" {
  rule      = aws_cloudwatch_event_rule.daily_etl.name
  target_id = "DailyETLStateMachine"
  arn       = aws_sfn_state_machine.etl_pipeline.arn
  role_arn  = aws_iam_role.eventbridge_step_functions.arn

  input = jsonencode({
    fetch_type = "stats_only"
    season     = "2025-26"
  })
}

# Monthly schedule for players and salaries (1st of month at 6 AM UTC)
resource "aws_cloudwatch_event_rule" "monthly_etl" {
  name                = "${local.name_prefix}-monthly-etl"
  description         = "Trigger monthly ETL pipeline for players/salaries on 1st of month at 6 AM UTC"
  schedule_expression = "cron(0 6 1 * ? *)"
  tags                = local.common_tags
}

resource "aws_cloudwatch_event_target" "monthly_etl_pipeline" {
  rule      = aws_cloudwatch_event_rule.monthly_etl.name
  target_id = "MonthlyETLStateMachine"
  arn       = aws_sfn_state_machine.etl_pipeline.arn
  role_arn  = aws_iam_role.eventbridge_step_functions.arn

  input = jsonencode({
    fetch_type = "monthly"
    season     = "2025-26"
  })
}

# EventBridge role to trigger Step Functions
resource "aws_iam_role" "eventbridge_step_functions" {
  name = "${local.name_prefix}-eventbridge-sfn"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "eventbridge_step_functions" {
  name = "${local.name_prefix}-eventbridge-sfn-policy"
  role = aws_iam_role.eventbridge_step_functions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "states:StartExecution"
        Resource = aws_sfn_state_machine.etl_pipeline.arn
      }
    ]
  })
}

# ============================================================================
# CLOUDWATCH ALARMS
# ============================================================================

# RDS CPU alarm
resource "aws_cloudwatch_metric_alarm" "rds_cpu" {
  alarm_name          = "${local.name_prefix}-rds-high-cpu"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/RDS"
  period              = 300
  statistic           = "Average"
  threshold           = 80
  alarm_description   = "RDS CPU utilization is too high"

  dimensions = {
    DBInstanceIdentifier = aws_db_instance.main.id
  }

  tags = local.common_tags
}

# Step Functions execution failures
resource "aws_cloudwatch_metric_alarm" "etl_failures" {
  alarm_name          = "${local.name_prefix}-etl-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "ETL pipeline execution failed"

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.etl_pipeline.arn
  }

  tags = local.common_tags
}
