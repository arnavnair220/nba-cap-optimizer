"""
Tests for load predictions module (MVP level).

Basic tests for handler error cases. S3/RDS integration tested at system level.
"""

import json
import os

# Set AWS region before importing modules that use boto3
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from src.ml.load_predictions import handler  # noqa: E402


class TestHandler:
    """Test Lambda handler integration."""

    def test_handler_missing_env_variables(self):
        """Test Lambda handler with missing environment variables."""
        event = {"season": "2024-25"}

        response = handler(event, None)

        # Returns 500 due to missing environment variables
        assert response["statusCode"] == 500
        body = json.loads(response["body"])
        assert "error" in body
