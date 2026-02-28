"""
Tests for copy trained model Lambda function.

Basic tests for Lambda handler error cases. S3 integration tested at system level.
"""

import os
from unittest.mock import MagicMock, patch

import pytest

# Set AWS region before importing modules that use boto3
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ["DATA_BUCKET"] = "test-bucket"

from src.lambdas.ml.copy_trained_model import lambda_handler  # noqa: E402


class TestLambdaHandler:
    """Test Lambda handler."""

    def test_handler_missing_season(self):
        """Test Lambda handler with missing season parameter."""
        event = {
            "model_artifacts": {
                "S3ModelArtifacts": "s3://bucket/ml/models/2025-26/train-abc/output/model.tar.gz"
            }
        }

        with pytest.raises(ValueError, match="Missing required parameters"):
            lambda_handler(event, None)

    def test_handler_missing_model_artifacts(self):
        """Test Lambda handler with missing model_artifacts parameter."""
        event = {"season": "2025-26"}

        with pytest.raises(ValueError, match="Missing required parameters"):
            lambda_handler(event, None)

    @patch("src.lambdas.ml.copy_trained_model.boto3")
    def test_handler_success(self, mock_boto3):
        """Test successful model copy."""
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_boto3.client.return_value = mock_s3_client

        event = {
            "season": "2025-26",
            "training_job_name": "train-abc123",
            "model_artifacts": {
                "S3ModelArtifacts": "s3://source-bucket/ml/models/2025-26/train-abc123/output/model.tar.gz"
            },
        }

        result = lambda_handler(event, None)

        # Verify S3 copy was called
        mock_s3_client.copy_object.assert_called_once()
        call_args = mock_s3_client.copy_object.call_args[1]
        assert call_args["CopySource"]["Bucket"] == "source-bucket"
        assert (
            call_args["CopySource"]["Key"] == "ml/models/2025-26/train-abc123/output/model.tar.gz"
        )
        assert call_args["Bucket"] == "test-bucket"
        assert call_args["Key"] == "ml/models/2025-26/model.tar.gz"

        # Verify response
        assert result["statusCode"] == 200
        assert result["season"] == "2025-26"
        assert "s3://test-bucket/ml/models/2025-26/model.tar.gz" in result["destination"]

    @patch("src.lambdas.ml.copy_trained_model.boto3")
    def test_handler_s3_error(self, mock_boto3):
        """Test handler when S3 copy fails."""
        # Mock S3 client to raise an error
        mock_s3_client = MagicMock()
        mock_s3_client.copy_object.side_effect = Exception("S3 Error")
        mock_boto3.client.return_value = mock_s3_client

        event = {
            "season": "2025-26",
            "training_job_name": "train-abc123",
            "model_artifacts": {
                "S3ModelArtifacts": "s3://bucket/ml/models/2025-26/train-abc123/output/model.tar.gz"
            },
        }

        with pytest.raises(Exception, match="S3 Error"):
            lambda_handler(event, None)
