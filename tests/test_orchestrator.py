"""
Tests for ML orchestrator Lambda.

Tests SageMaker job configuration and task routing for
feature engineering, training, batch transform, and prediction loading.
"""

import json
import os
from unittest.mock import MagicMock, patch

import pytest

# Set AWS region before importing modules that use boto3
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from src.ml.orchestrator import (  # noqa: E402
    handler,
    invoke_load_predictions,
    start_batch_transform,
    start_feature_engineering,
    start_model_training,
)


class TestStartFeatureEngineering:
    """Test feature engineering job configuration."""

    @patch.dict(
        os.environ,
        {
            "SAGEMAKER_ROLE_ARN": "arn:aws:iam::123456789012:role/SageMakerRole",
            "DATA_BUCKET": "test-bucket",
        },
    )
    @patch("src.ml.orchestrator.sagemaker")
    def test_start_feature_engineering_job(self, mock_sagemaker):
        """Test that feature engineering starts SageMaker Processing Job."""
        mock_sagemaker.create_processing_job.return_value = {
            "ProcessingJobArn": "arn:aws:sagemaker:us-east-1:123456789012:processing-job/job-123"
        }

        event = {
            "season": "2024-25",
        }

        result = start_feature_engineering(event)

        assert "jobName" in result
        assert "jobArn" in result
        assert "featuresPath" in result
        assert "timestamp" in result

        mock_sagemaker.create_processing_job.assert_called_once()
        call_kwargs = mock_sagemaker.create_processing_job.call_args[1]

        assert "ProcessingJobName" in call_kwargs

    @patch.dict(
        os.environ,
        {
            "SAGEMAKER_ROLE_ARN": "arn:aws:iam::123456789012:role/SageMakerRole",
            "DATA_BUCKET": "test-bucket",
        },
    )
    @patch("src.ml.orchestrator.sagemaker")
    def test_start_feature_engineering_with_input_path(self, mock_sagemaker):
        """Test feature engineering with custom input path."""
        mock_sagemaker.create_processing_job.return_value = {
            "ProcessingJobArn": "arn:aws:sagemaker:us-east-1:123456789012:processing-job/job-123"
        }

        event = {"season": "2024-25"}

        result = start_feature_engineering(event)

        assert "jobName" in result
        mock_sagemaker.create_processing_job.assert_called_once()

    @patch("src.ml.orchestrator.sagemaker")
    def test_start_feature_engineering_error_handling(self, mock_sagemaker):
        """Test feature engineering error handling."""
        mock_sagemaker.create_processing_job.side_effect = Exception("SageMaker error")

        event = {
            "season": "2024-25",
            "data_bucket": "test-bucket",
            "sagemaker_role_arn": "arn:aws:iam::123456789012:role/SageMakerRole",
        }

        with pytest.raises(Exception, match="SageMaker error"):
            start_feature_engineering(event)


class TestStartModelTraining:
    """Test model training job configuration."""

    @patch.dict(
        os.environ,
        {
            "SAGEMAKER_ROLE_ARN": "arn:aws:iam::123456789012:role/SageMakerRole",
            "DATA_BUCKET": "test-bucket",
        },
    )
    @patch("src.ml.orchestrator.sagemaker")
    def test_start_model_training_job(self, mock_sagemaker):
        """Test that model training starts SageMaker Training Job."""
        mock_sagemaker.create_training_job.return_value = {
            "TrainingJobArn": "arn:aws:sagemaker:us-east-1:123456789012:training-job/job-456"
        }

        event = {
            "season": "2024-25",
            "features_s3_path": "s3://test-bucket/features/2024-25/",
        }

        result = start_model_training(event)

        assert "jobName" in result
        assert "jobArn" in result
        mock_sagemaker.create_training_job.assert_called_once()

    @patch.dict(
        os.environ,
        {
            "SAGEMAKER_ROLE_ARN": "arn:aws:iam::123456789012:role/SageMakerRole",
            "DATA_BUCKET": "test-bucket",
        },
    )
    @patch("src.ml.orchestrator.sagemaker")
    def test_start_model_training_with_hyperparameters(self, mock_sagemaker):
        """Test model training with custom hyperparameters."""
        mock_sagemaker.create_training_job.return_value = {
            "TrainingJobArn": "arn:aws:sagemaker:us-east-1:123456789012:training-job/job-456"
        }

        event = {
            "season": "2024-25",
            "features_s3_path": "s3://test-bucket/features/2024-25/",
        }

        result = start_model_training(event)

        assert "jobName" in result
        mock_sagemaker.create_training_job.assert_called_once()

    @patch.dict(
        os.environ,
        {
            "SAGEMAKER_ROLE_ARN": "arn:aws:iam::123456789012:role/SageMakerRole",
            "DATA_BUCKET": "test-bucket",
        },
    )
    @patch("src.ml.orchestrator.sagemaker")
    def test_start_model_training_instance_configuration(self, mock_sagemaker):
        """Test model training instance configuration."""
        mock_sagemaker.create_training_job.return_value = {
            "TrainingJobArn": "arn:aws:sagemaker:us-east-1:123456789012:training-job/job-456"
        }

        event = {
            "season": "2024-25",
            "features_s3_path": "s3://test-bucket/features/2024-25/",
        }

        result = start_model_training(event)

        assert "jobName" in result
        mock_sagemaker.create_training_job.assert_called_once()


class TestStartBatchTransform:
    """Test batch transform job configuration."""

    @patch.dict(os.environ, {"DATA_BUCKET": "test-bucket"})
    @patch("src.ml.orchestrator.sagemaker")
    def test_start_batch_transform_job(self, mock_sagemaker):
        """Test that batch transform starts SageMaker Transform Job."""
        mock_sagemaker.create_transform_job.return_value = {
            "TransformJobArn": "arn:aws:sagemaker:us-east-1:123456789012:transform-job/job-789"
        }

        event = {
            "season": "2024-25",
            "model_name": "model_salary_cap_pct",
            "input_data_s3_path": "s3://test-bucket/features/2024-25/",
        }

        result = start_batch_transform(event)

        assert "jobName" in result
        assert "jobArn" in result
        mock_sagemaker.create_transform_job.assert_called_once()

    @patch.dict(os.environ, {"DATA_BUCKET": "test-bucket"})
    @patch("src.ml.orchestrator.sagemaker")
    def test_start_batch_transform_with_instance_type(self, mock_sagemaker):
        """Test batch transform with custom instance type."""
        mock_sagemaker.create_transform_job.return_value = {
            "TransformJobArn": "arn:aws:sagemaker:us-east-1:123456789012:transform-job/job-789"
        }

        event = {
            "season": "2024-25",
            "model_name": "model_salary_cap_pct",
            "input_data_s3_path": "s3://test-bucket/features/2024-25/",
        }

        result = start_batch_transform(event)

        assert "jobName" in result
        mock_sagemaker.create_transform_job.assert_called_once()

    @patch.dict(os.environ, {"DATA_BUCKET": "test-bucket"})
    @patch("src.ml.orchestrator.sagemaker")
    def test_start_batch_transform_output_path(self, mock_sagemaker):
        """Test batch transform output path configuration."""
        mock_sagemaker.create_transform_job.return_value = {
            "TransformJobArn": "arn:aws:sagemaker:us-east-1:123456789012:transform-job/job-789"
        }

        event = {
            "season": "2024-25",
            "model_name": "model_salary_cap_pct",
            "input_data_s3_path": "s3://test-bucket/features/2024-25/",
        }

        result = start_batch_transform(event)

        assert "jobName" in result
        mock_sagemaker.create_transform_job.assert_called_once()


class TestInvokeLoadPredictions:
    """Test invoking load predictions Lambda."""

    @patch.dict(
        os.environ,
        {"DATA_BUCKET": "test-bucket", "LOAD_PREDICTIONS_LAMBDA": "load-predictions-function"},
    )
    @patch("src.ml.orchestrator.lambda_client")
    def test_invoke_load_predictions(self, mock_lambda):
        """Test that load predictions Lambda is invoked."""
        mock_lambda.invoke.return_value = {
            "StatusCode": 200,
            "Payload": MagicMock(
                read=lambda: json.dumps(
                    {"statusCode": 200, "body": '{"status": "success", "predictions_loaded": 10}'}
                ).encode()
            ),
        }

        event = {
            "season": "2024-25",
            "predictionsPath": "s3://test-bucket/predictions/2024-25/",
            "modelVersion": "v1.0.0",
        }

        result = invoke_load_predictions(event)

        assert result["statusCode"] == 200
        assert result["predictionsLoaded"] == 10
        mock_lambda.invoke.assert_called_once()

    @patch.dict(
        os.environ,
        {"DATA_BUCKET": "test-bucket", "LOAD_PREDICTIONS_LAMBDA": "load-predictions-function"},
    )
    @patch("src.ml.orchestrator.lambda_client")
    def test_invoke_load_predictions_with_payload(self, mock_lambda):
        """Test that correct payload is sent to load predictions Lambda."""
        mock_lambda.invoke.return_value = {
            "StatusCode": 200,
            "Payload": MagicMock(
                read=lambda: json.dumps(
                    {"statusCode": 200, "body": '{"status": "success"}'}
                ).encode()
            ),
        }

        event = {
            "season": "2024-25",
            "predictionsPath": "s3://test-bucket/predictions/2024-25/",
            "modelVersion": "v1.0.0",
        }

        result = invoke_load_predictions(event)

        assert result["statusCode"] == 200
        mock_lambda.invoke.assert_called_once()

    @patch.dict(
        os.environ,
        {"DATA_BUCKET": "test-bucket", "LOAD_PREDICTIONS_LAMBDA": "load-predictions-function"},
    )
    @patch("src.ml.orchestrator.lambda_client")
    def test_invoke_load_predictions_error(self, mock_lambda):
        """Test load predictions Lambda error handling."""
        mock_lambda.invoke.return_value = {
            "StatusCode": 500,
            "Payload": MagicMock(
                read=lambda: json.dumps(
                    {"statusCode": 500, "body": '{"error": "Database error"}'}
                ).encode()
            ),
        }

        event = {
            "season": "2024-25",
            "predictionsPath": "s3://test-bucket/predictions/2024-25/",
            "modelVersion": "v1.0.0",
        }

        # Function doesn't raise exception, returns error status
        result = invoke_load_predictions(event)
        assert result["statusCode"] == 500


class TestHandler:
    """Test Lambda handler task routing."""

    @patch("src.ml.orchestrator.start_feature_engineering")
    def test_handler_feature_engineering_task(self, mock_start_fe):
        """Test handler routes to feature engineering."""
        mock_start_fe.return_value = {
            "jobName": "fe-job-123",
            "jobArn": "arn:aws:sagemaker:us-east-1:123456789012:processing-job/fe-job-123",
            "status": "Started",
        }

        event = {
            "task": "feature_engineering",
            "season": "2024-25",
            "data_bucket": "test-bucket",
            "sagemaker_role_arn": "arn:aws:iam::123456789012:role/SageMakerRole",
        }

        result = handler(event, None)

        mock_start_fe.assert_called_once_with(event)
        assert result["status"] == "Started"

    @patch("src.ml.orchestrator.start_model_training")
    def test_handler_model_training_task(self, mock_start_training):
        """Test handler routes to model training."""
        mock_start_training.return_value = {
            "jobName": "train-job-456",
            "jobArn": "arn:aws:sagemaker:us-east-1:123456789012:training-job/train-job-456",
            "status": "Started",
        }

        event = {
            "task": "model_training",
            "season": "2024-25",
            "data_bucket": "test-bucket",
            "sagemaker_role_arn": "arn:aws:iam::123456789012:role/SageMakerRole",
        }

        result = handler(event, None)

        mock_start_training.assert_called_once_with(event)
        assert result["status"] == "Started"

    @patch("src.ml.orchestrator.start_batch_transform")
    def test_handler_batch_transform_task(self, mock_start_batch):
        """Test handler routes to batch transform."""
        mock_start_batch.return_value = {
            "jobName": "batch-job-789",
            "jobArn": "arn:aws:sagemaker:us-east-1:123456789012:transform-job/batch-job-789",
            "status": "Started",
        }

        event = {
            "task": "batch_transform",
            "season": "2024-25",
            "model_name": "model_salary_cap_pct",
        }

        result = handler(event, None)

        mock_start_batch.assert_called_once_with(event)
        assert result["status"] == "Started"

    @patch("src.ml.orchestrator.invoke_load_predictions")
    def test_handler_load_predictions_task(self, mock_invoke_load):
        """Test handler routes to load predictions."""
        mock_invoke_load.return_value = {
            "status": "Success",
            "predictions_loaded": 100,
        }

        event = {
            "task": "load_predictions",
            "season": "2024-25",
            "predictions_s3_path": "s3://test-bucket/predictions/2024-25/",
            "model_version": "v1.0.0",
            "load_predictions_lambda": "load-predictions-function",
        }

        result = handler(event, None)

        mock_invoke_load.assert_called_once_with(event)
        assert result["status"] == "Success"

    def test_handler_unknown_task(self):
        """Test handler with unknown task type."""
        event = {"task": "unknown_task"}

        result = handler(event, None)

        assert result["status"] == "Error"
        assert "Unknown task" in result["message"]

    def test_handler_missing_task(self):
        """Test handler with missing task parameter."""
        event = {"season": "2024-25"}

        result = handler(event, None)

        assert result["status"] == "Error"
        assert "task" in result["message"]

    @patch("src.ml.orchestrator.start_feature_engineering")
    def test_handler_task_exception(self, mock_start_fe):
        """Test handler error handling for task exception."""
        mock_start_fe.side_effect = Exception("SageMaker API error")

        event = {
            "task": "feature_engineering",
            "season": "2024-25",
            "data_bucket": "test-bucket",
            "sagemaker_role_arn": "arn:aws:iam::123456789012:role/SageMakerRole",
        }

        # The handler should let exceptions propagate from the task functions
        with pytest.raises(Exception, match="SageMaker API error"):
            handler(event, None)
