# ============================================================================
# ROUTE53 HOSTED ZONE (Production Only)
# ============================================================================

resource "aws_route53_zone" "main" {
  count = var.environment == "production" ? 1 : 0
  name  = "dunkonomics.net"

  tags = merge(
    local.common_tags,
    {
      Name = "dunkonomics.net"
    }
  )
}

# ============================================================================
# ACM CERTIFICATE FOR CLOUDFRONT (must be in us-east-1, Production Only)
# ============================================================================

resource "aws_acm_certificate" "frontend" {
  count             = var.environment == "production" ? 1 : 0
  provider          = aws.us_east_1
  domain_name       = "www.dunkonomics.net"
  validation_method = "DNS"

  subject_alternative_names = [
    "dunkonomics.net"
  ]

  lifecycle {
    create_before_destroy = true
  }

  tags = merge(
    local.common_tags,
    {
      Name = "dunkonomics.net Frontend Certificate"
    }
  )
}

# DNS validation records for frontend certificate
resource "aws_route53_record" "frontend_cert_validation" {
  for_each = var.environment == "production" ? {
    for dvo in aws_acm_certificate.frontend[0].domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      record = dvo.resource_record_value
      type   = dvo.resource_record_type
    }
  } : {}

  allow_overwrite = true
  name            = each.value.name
  records         = [each.value.record]
  ttl             = 60
  type            = each.value.type
  zone_id         = aws_route53_zone.main[0].zone_id
}

# Certificate validation wait
resource "aws_acm_certificate_validation" "frontend" {
  count                   = var.environment == "production" ? 1 : 0
  provider                = aws.us_east_1
  certificate_arn         = aws_acm_certificate.frontend[0].arn
  validation_record_fqdns = [for record in aws_route53_record.frontend_cert_validation : record.fqdn]
}

# ============================================================================
# ACM CERTIFICATE FOR API GATEWAY (Production Only)
# ============================================================================

resource "aws_acm_certificate" "api" {
  count             = var.environment == "production" ? 1 : 0
  domain_name       = "api.dunkonomics.net"
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }

  tags = merge(
    local.common_tags,
    {
      Name = "dunkonomics.net API Certificate"
    }
  )
}

# DNS validation records for API certificate
resource "aws_route53_record" "api_cert_validation" {
  for_each = var.environment == "production" ? {
    for dvo in aws_acm_certificate.api[0].domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      record = dvo.resource_record_value
      type   = dvo.resource_record_type
    }
  } : {}

  allow_overwrite = true
  name            = each.value.name
  records         = [each.value.record]
  ttl             = 60
  type            = each.value.type
  zone_id         = aws_route53_zone.main[0].zone_id
}

# Certificate validation wait
resource "aws_acm_certificate_validation" "api" {
  count                   = var.environment == "production" ? 1 : 0
  certificate_arn         = aws_acm_certificate.api[0].arn
  validation_record_fqdns = [for record in aws_route53_record.api_cert_validation : record.fqdn]
}

# ============================================================================
# API GATEWAY CUSTOM DOMAIN (Production Only)
# ============================================================================

resource "aws_api_gateway_domain_name" "api" {
  count           = var.environment == "production" ? 1 : 0
  domain_name     = "api.dunkonomics.net"
  certificate_arn = aws_acm_certificate_validation.api[0].certificate_arn

  tags = merge(
    local.common_tags,
    {
      Name = "dunkonomics.net API Domain"
    }
  )
}

# Map custom domain to API Gateway stage
resource "aws_api_gateway_base_path_mapping" "api" {
  count       = var.environment == "production" ? 1 : 0
  api_id      = aws_api_gateway_rest_api.predictions_api.id
  stage_name  = aws_api_gateway_stage.v1.stage_name
  domain_name = aws_api_gateway_domain_name.api[0].domain_name
}

# ============================================================================
# DNS RECORDS (Production Only)
# ============================================================================

# Frontend: www.dunkonomics.net -> CloudFront
resource "aws_route53_record" "frontend_www" {
  count   = var.environment == "production" ? 1 : 0
  zone_id = aws_route53_zone.main[0].zone_id
  name    = "www.dunkonomics.net"
  type    = "A"

  alias {
    name                   = aws_cloudfront_distribution.frontend.domain_name
    zone_id                = aws_cloudfront_distribution.frontend.hosted_zone_id
    evaluate_target_health = false
  }
}

# Root domain: dunkonomics.net -> CloudFront (same as www)
resource "aws_route53_record" "frontend_root" {
  count   = var.environment == "production" ? 1 : 0
  zone_id = aws_route53_zone.main[0].zone_id
  name    = "dunkonomics.net"
  type    = "A"

  alias {
    name                   = aws_cloudfront_distribution.frontend.domain_name
    zone_id                = aws_cloudfront_distribution.frontend.hosted_zone_id
    evaluate_target_health = false
  }
}

# API: api.dunkonomics.net -> API Gateway
resource "aws_route53_record" "api" {
  count   = var.environment == "production" ? 1 : 0
  zone_id = aws_route53_zone.main[0].zone_id
  name    = "api.dunkonomics.net"
  type    = "A"

  alias {
    name                   = aws_api_gateway_domain_name.api[0].cloudfront_domain_name
    zone_id                = aws_api_gateway_domain_name.api[0].cloudfront_zone_id
    evaluate_target_health = false
  }
}
