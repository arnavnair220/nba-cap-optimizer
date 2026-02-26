# ============================================================================
# API GATEWAY (Public REST API)
# ============================================================================

# API Gateway REST API
resource "aws_api_gateway_rest_api" "predictions_api" {
  name        = "${local.name_prefix}-predictions-api"
  description = "Public REST API for NBA player value predictions and team efficiency metrics"

  endpoint_configuration {
    types = ["REGIONAL"]
  }

  tags = local.common_tags
}

# ============================================================================
# API GATEWAY RESOURCES (URL Paths)
# ============================================================================

# /predictions
resource "aws_api_gateway_resource" "predictions" {
  rest_api_id = aws_api_gateway_rest_api.predictions_api.id
  parent_id   = aws_api_gateway_rest_api.predictions_api.root_resource_id
  path_part   = "predictions"
}

# /predictions/undervalued
resource "aws_api_gateway_resource" "predictions_undervalued" {
  rest_api_id = aws_api_gateway_rest_api.predictions_api.id
  parent_id   = aws_api_gateway_resource.predictions.id
  path_part   = "undervalued"
}

# /predictions/overvalued
resource "aws_api_gateway_resource" "predictions_overvalued" {
  rest_api_id = aws_api_gateway_rest_api.predictions_api.id
  parent_id   = aws_api_gateway_resource.predictions.id
  path_part   = "overvalued"
}

# /predictions/{player_name}
resource "aws_api_gateway_resource" "predictions_player" {
  rest_api_id = aws_api_gateway_rest_api.predictions_api.id
  parent_id   = aws_api_gateway_resource.predictions.id
  path_part   = "{player_name}"
}

# /teams
resource "aws_api_gateway_resource" "teams" {
  rest_api_id = aws_api_gateway_rest_api.predictions_api.id
  parent_id   = aws_api_gateway_rest_api.predictions_api.root_resource_id
  path_part   = "teams"
}

# /teams/{team_abbreviation}
resource "aws_api_gateway_resource" "teams_detail" {
  rest_api_id = aws_api_gateway_rest_api.predictions_api.id
  parent_id   = aws_api_gateway_resource.teams.id
  path_part   = "{team_abbreviation}"
}

# ============================================================================
# API GATEWAY METHODS
# ============================================================================

# GET /predictions
resource "aws_api_gateway_method" "predictions_get" {
  rest_api_id   = aws_api_gateway_rest_api.predictions_api.id
  resource_id   = aws_api_gateway_resource.predictions.id
  http_method   = "GET"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "predictions_get" {
  rest_api_id             = aws_api_gateway_rest_api.predictions_api.id
  resource_id             = aws_api_gateway_resource.predictions.id
  http_method             = aws_api_gateway_method.predictions_get.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.api_handler.invoke_arn
}

# GET /predictions/undervalued
resource "aws_api_gateway_method" "predictions_undervalued_get" {
  rest_api_id   = aws_api_gateway_rest_api.predictions_api.id
  resource_id   = aws_api_gateway_resource.predictions_undervalued.id
  http_method   = "GET"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "predictions_undervalued_get" {
  rest_api_id             = aws_api_gateway_rest_api.predictions_api.id
  resource_id             = aws_api_gateway_resource.predictions_undervalued.id
  http_method             = aws_api_gateway_method.predictions_undervalued_get.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.api_handler.invoke_arn
}

# GET /predictions/overvalued
resource "aws_api_gateway_method" "predictions_overvalued_get" {
  rest_api_id   = aws_api_gateway_rest_api.predictions_api.id
  resource_id   = aws_api_gateway_resource.predictions_overvalued.id
  http_method   = "GET"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "predictions_overvalued_get" {
  rest_api_id             = aws_api_gateway_rest_api.predictions_api.id
  resource_id             = aws_api_gateway_resource.predictions_overvalued.id
  http_method             = aws_api_gateway_method.predictions_overvalued_get.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.api_handler.invoke_arn
}

# GET /predictions/{player_name}
resource "aws_api_gateway_method" "predictions_player_get" {
  rest_api_id   = aws_api_gateway_rest_api.predictions_api.id
  resource_id   = aws_api_gateway_resource.predictions_player.id
  http_method   = "GET"
  authorization = "NONE"

  request_parameters = {
    "method.request.path.player_name" = true
  }
}

resource "aws_api_gateway_integration" "predictions_player_get" {
  rest_api_id             = aws_api_gateway_rest_api.predictions_api.id
  resource_id             = aws_api_gateway_resource.predictions_player.id
  http_method             = aws_api_gateway_method.predictions_player_get.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.api_handler.invoke_arn
}

# GET /teams
resource "aws_api_gateway_method" "teams_get" {
  rest_api_id   = aws_api_gateway_rest_api.predictions_api.id
  resource_id   = aws_api_gateway_resource.teams.id
  http_method   = "GET"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "teams_get" {
  rest_api_id             = aws_api_gateway_rest_api.predictions_api.id
  resource_id             = aws_api_gateway_resource.teams.id
  http_method             = aws_api_gateway_method.teams_get.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.api_handler.invoke_arn
}

# GET /teams/{team_abbreviation}
resource "aws_api_gateway_method" "teams_detail_get" {
  rest_api_id   = aws_api_gateway_rest_api.predictions_api.id
  resource_id   = aws_api_gateway_resource.teams_detail.id
  http_method   = "GET"
  authorization = "NONE"

  request_parameters = {
    "method.request.path.team_abbreviation" = true
  }
}

resource "aws_api_gateway_integration" "teams_detail_get" {
  rest_api_id             = aws_api_gateway_rest_api.predictions_api.id
  resource_id             = aws_api_gateway_resource.teams_detail.id
  http_method             = aws_api_gateway_method.teams_detail_get.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.api_handler.invoke_arn
}

# ============================================================================
# CORS CONFIGURATION (Enable OPTIONS for preflight requests)
# ============================================================================

module "cors_predictions" {
  source = "./modules/cors"

  api_id          = aws_api_gateway_rest_api.predictions_api.id
  api_resource_id = aws_api_gateway_resource.predictions.id
}

module "cors_predictions_undervalued" {
  source = "./modules/cors"

  api_id          = aws_api_gateway_rest_api.predictions_api.id
  api_resource_id = aws_api_gateway_resource.predictions_undervalued.id
}

module "cors_predictions_overvalued" {
  source = "./modules/cors"

  api_id          = aws_api_gateway_rest_api.predictions_api.id
  api_resource_id = aws_api_gateway_resource.predictions_overvalued.id
}

module "cors_predictions_player" {
  source = "./modules/cors"

  api_id          = aws_api_gateway_rest_api.predictions_api.id
  api_resource_id = aws_api_gateway_resource.predictions_player.id
}

module "cors_teams" {
  source = "./modules/cors"

  api_id          = aws_api_gateway_rest_api.predictions_api.id
  api_resource_id = aws_api_gateway_resource.teams.id
}

module "cors_teams_detail" {
  source = "./modules/cors"

  api_id          = aws_api_gateway_rest_api.predictions_api.id
  api_resource_id = aws_api_gateway_resource.teams_detail.id
}

# ============================================================================
# API GATEWAY DEPLOYMENT
# ============================================================================

resource "aws_api_gateway_deployment" "predictions_api" {
  rest_api_id = aws_api_gateway_rest_api.predictions_api.id

  triggers = {
    redeployment = sha1(jsonencode([
      aws_api_gateway_resource.predictions.id,
      aws_api_gateway_resource.predictions_undervalued.id,
      aws_api_gateway_resource.predictions_overvalued.id,
      aws_api_gateway_resource.predictions_player.id,
      aws_api_gateway_resource.teams.id,
      aws_api_gateway_resource.teams_detail.id,
      aws_api_gateway_method.predictions_get.id,
      aws_api_gateway_method.predictions_undervalued_get.id,
      aws_api_gateway_method.predictions_overvalued_get.id,
      aws_api_gateway_method.predictions_player_get.id,
      aws_api_gateway_method.teams_get.id,
      aws_api_gateway_method.teams_detail_get.id,
      aws_api_gateway_integration.predictions_get.id,
      aws_api_gateway_integration.predictions_undervalued_get.id,
      aws_api_gateway_integration.predictions_overvalued_get.id,
      aws_api_gateway_integration.predictions_player_get.id,
      aws_api_gateway_integration.teams_get.id,
      aws_api_gateway_integration.teams_detail_get.id,
    ]))
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_stage" "v1" {
  deployment_id = aws_api_gateway_deployment.predictions_api.id
  rest_api_id   = aws_api_gateway_rest_api.predictions_api.id
  stage_name    = "v1"

  tags = merge(
    local.common_tags,
    {
      Name = "${local.name_prefix}-api-v1"
    }
  )
}

# ============================================================================
# API LAMBDA FUNCTION
# ============================================================================

resource "aws_lambda_function" "api_handler" {
  function_name = "${local.name_prefix}-api-handler"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "src.api.handler.handler"
  runtime       = var.lambda_runtime
  timeout       = 30
  memory_size   = 512

  filename         = "placeholder.zip"
  source_code_hash = filebase64sha256("placeholder.zip")

  vpc_config {
    subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      ENVIRONMENT    = var.environment
      DB_SECRET_ARN  = aws_secretsmanager_secret.db_credentials.arn
      CURRENT_SEASON = var.current_season
    }
  }

  tags = local.common_tags

  lifecycle {
    ignore_changes = [
      filename,
      source_code_hash,
      layers
    ]
  }

  depends_on = [null_resource.trigger_schema_migration]
}

# Lambda permission for API Gateway to invoke
resource "aws_lambda_permission" "api_gateway" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api_handler.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.predictions_api.execution_arn}/*/*"
}

# ============================================================================
# API GATEWAY LOGGING (Optional but recommended)
# ============================================================================

resource "aws_cloudwatch_log_group" "api_gateway" {
  name              = "/aws/apigateway/${local.name_prefix}-predictions-api"
  retention_in_days = 7

  tags = local.common_tags
}
