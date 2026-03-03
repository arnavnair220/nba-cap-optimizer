"""
Lambda function to copy trained model from SageMaker training output to standard path.

After SageMaker training completes, the model is saved to:
  s3://bucket/ml/models/{season}/{training_job_name}/output/model.tar.gz

This function copies it to the standard location expected by predictions:
  s3://bucket/ml/models/{season}/model.tar.gz
"""

import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Copy trained model to standard path.

    Args:
        event: Lambda event with training job details
        context: Lambda context

    Expected event format:
        {
            "season": "2025-26",
            "training_job_name": "train-abc123",
            "model_artifacts": {
                "S3ModelArtifacts": "s3://bucket/ml/models/2025-26/train-abc123/output/model.tar.gz"
            }
        }

    Returns:
        Dict with source and destination paths
    """
    try:
        season = event.get("season")
        model_artifacts_s3_uri = event.get("model_artifacts", {}).get("S3ModelArtifacts")
        data_bucket = os.environ["DATA_BUCKET"]

        if not season or not model_artifacts_s3_uri:
            raise ValueError("Missing required parameters: season or model_artifacts")

        logger.info(f"Copying model for season {season}")
        logger.info(f"Source: {model_artifacts_s3_uri}")

        # Parse source S3 path
        # Format: s3://bucket/ml/models/2025-26/train-abc123/output/model.tar.gz
        source_bucket = model_artifacts_s3_uri.split("/")[2]
        source_key = "/".join(model_artifacts_s3_uri.split("/")[3:])

        # Construct destination path
        destination_key = f"ml/models/{season}/model.tar.gz"
        destination_s3_uri = f"s3://{data_bucket}/{destination_key}"

        logger.info(f"Destination: {destination_s3_uri}")

        # Copy model
        s3_client = boto3.client("s3")
        copy_source = {"Bucket": source_bucket, "Key": source_key}

        s3_client.copy_object(CopySource=copy_source, Bucket=data_bucket, Key=destination_key)

        logger.info("Model copied successfully")

        return {
            "statusCode": 200,
            "source": model_artifacts_s3_uri,
            "destination": destination_s3_uri,
            "season": season,
        }

    except Exception as e:
        logger.error(f"Error copying model: {str(e)}", exc_info=True)
        raise
