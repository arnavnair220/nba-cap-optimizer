"""
Tests for load predictions module.

Tests value categorization, inefficiency score calculation,
and database operations for loading predictions.
"""

import json
import os
from unittest.mock import MagicMock, call, patch

import pytest

# Set AWS region before importing modules that use boto3
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from src.ml.load_predictions import (
    enrich_predictions_with_actuals,
    handler,
    load_predictions_from_s3,
)


class TestLoadPredictionsFromS3:
    """Test loading predictions from S3."""

    @patch("src.ml.load_predictions.s3_client")
    def test_load_predictions_from_s3_single_file(self, mock_s3):
        """Test loading predictions from single S3 file."""
        mock_s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "predictions/prediction1.json.out"}]
        }

        mock_s3.get_object.return_value = {
            "Body": MagicMock(
                read=lambda: json.dumps(
                    {"player_name": "Player A", "predicted_salary_cap_pct": 15.5}
                ).encode()
            )
        }

        predictions = load_predictions_from_s3("test-bucket", "predictions/")

        assert len(predictions) == 1
        assert predictions[0]["player_name"] == "Player A"
        assert predictions[0]["predicted_salary_cap_pct"] == 15.5

    @patch("src.ml.load_predictions.s3_client")
    def test_load_predictions_from_s3_multiple_files(self, mock_s3):
        """Test loading predictions from multiple S3 files."""
        mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "predictions/prediction1.json.out"},
                {"Key": "predictions/prediction2.json.out"},
            ]
        }

        def mock_get_object(Bucket, Key):
            if "prediction1" in Key:
                return {
                    "Body": MagicMock(read=lambda: json.dumps({"player_name": "Player A"}).encode())
                }
            else:
                return {
                    "Body": MagicMock(read=lambda: json.dumps({"player_name": "Player B"}).encode())
                }

        mock_s3.get_object.side_effect = mock_get_object

        predictions = load_predictions_from_s3("test-bucket", "predictions/")

        assert len(predictions) == 2

    @patch("src.ml.load_predictions.s3_client")
    def test_load_predictions_from_s3_empty_bucket(self, mock_s3):
        """Test loading from empty S3 prefix."""
        mock_s3.list_objects_v2.return_value = {}

        predictions = load_predictions_from_s3("test-bucket", "predictions/")

        assert len(predictions) == 0


class TestHandler:
    """Test Lambda handler integration."""

    @patch("src.ml.load_predictions.get_db_connection")
    @patch("src.ml.load_predictions.s3_client")
    def test_handler_success(self, mock_s3, mock_get_conn):
        """Test Lambda handler with successful execution."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        # Mock S3 predictions
        mock_s3.list_objects_v2.return_value = {"Contents": [{"Key": "predictions/pred1.json.out"}]}
        mock_s3.get_object.return_value = {
            "Body": MagicMock(
                read=lambda: json.dumps(
                    {"player_name": "Player A", "predicted_salary_cap_pct": 15.0}
                ).encode()
            )
        }

        # Mock RDS actuals query
        mock_cursor.fetchall.return_value = [("Player A", 25000000, 140000000, 17.86)]
        mock_cursor.description = [
            ("player_name",),
            ("annual_salary",),
            ("salary_cap",),
            ("salary_cap_pct",),
        ]

        event = {
            "predictions_s3_path": "s3://bucket/predictions/",
            "season": "2024-25",
            "model_version": "v1.0.0",
        }

        response = handler(event, None)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["status"] == "success"
        assert body["predictions_loaded"] == 1

    def test_handler_missing_parameters(self):
        """Test Lambda handler with missing required parameters."""
        event = {"season": "2024-25"}

        response = handler(event, None)

        assert response["statusCode"] == 400
        body = json.loads(response["body"])
        assert "error" in body

    @patch("src.ml.load_predictions.get_db_connection")
    def test_handler_database_error(self, mock_get_conn):
        """Test Lambda handler with database connection error."""
        mock_get_conn.side_effect = Exception("Database connection failed")

        event = {
            "predictions_s3_path": "s3://bucket/predictions/",
            "season": "2024-25",
            "model_version": "v1.0.0",
        }

        response = handler(event, None)

        assert response["statusCode"] == 500
        body = json.loads(response["body"])
        assert "error" in body
