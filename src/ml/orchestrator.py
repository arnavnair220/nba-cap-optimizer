"""
Lambda orchestrator for ML pipeline Step Functions.

This Lambda triggers SageMaker jobs and returns job ARNs for Step Functions to monitor.
"""

import json
import logging
import os
from datetime import datetime

import boto3

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize clients
sagemaker = boto3.client("sagemaker")
lambda_client = boto3.client("lambda")

# Environment variables
SAGEMAKER_ROLE = os.environ.get("SAGEMAKER_ROLE_ARN")
DATA_BUCKET = os.environ.get("DATA_BUCKET")
REGION = os.environ.get("AWS_REGION", "us-east-1")


def start_feature_engineering(event):
    """
    Start SageMaker Processing job for feature engineering.

    Args:
        event: Step Functions input with season, etc.

    Returns:
        Processing job ARN and details
    """
    season = event.get("season", "2025-26")
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    job_name = f"nba-feature-engineering-{timestamp}"

    logger.info(f"Starting feature engineering job: {job_name}")

    # Get Python container image URI
    python_image = (
        f"683313688378.dkr.ecr.{REGION}.amazonaws.com/sagemaker-scikit-learn:1.0-1-cpu-py3"
    )

    response = sagemaker.create_processing_job(
        ProcessingJobName=job_name,
        RoleArn=SAGEMAKER_ROLE,
        AppSpecification={
            "ImageUri": python_image,
            "ContainerEntrypoint": ["python3", "feature_engineering.py"],
            "ContainerArguments": [
                "--seasons-before",
                season,
                "--output-path",
                f"s3://{DATA_BUCKET}/ml/features/{timestamp}/features.csv",
            ],
        },
        ProcessingInputs=[
            {
                "InputName": "code",
                "S3Input": {
                    "S3Uri": f"s3://{DATA_BUCKET}/code/models/",
                    "LocalPath": "/opt/ml/processing/input/code",
                    "S3DataType": "S3Prefix",
                    "S3InputMode": "File",
                },
            }
        ],
        ProcessingOutputConfig={
            "Outputs": [
                {
                    "OutputName": "features",
                    "S3Output": {
                        "S3Uri": f"s3://{DATA_BUCKET}/ml/features/{timestamp}/",
                        "LocalPath": "/opt/ml/processing/output",
                        "S3UploadMode": "EndOfJob",
                    },
                }
            ]
        },
        ProcessingResources={
            "ClusterConfig": {
                "InstanceCount": 1,
                "InstanceType": "ml.m5.xlarge",
                "VolumeSizeInGB": 30,
            }
        },
    )

    return {
        "jobName": job_name,
        "jobArn": response["ProcessingJobArn"],
        "featuresPath": f"s3://{DATA_BUCKET}/ml/features/{timestamp}/features.csv",
        "timestamp": timestamp,
    }


def start_model_training(event):
    """
    Start SageMaker Training job.

    Args:
        event: Contains features_path from previous step

    Returns:
        Training job ARN and details
    """
    features_path = event.get("featuresPath")
    timestamp = event.get("timestamp", datetime.utcnow().strftime("%Y%m%d-%H%M%S"))
    job_name = f"nba-model-training-{timestamp}"

    logger.info(f"Starting training job: {job_name}")

    # XGBoost container
    xgboost_image = f"683313688378.dkr.ecr.{REGION}.amazonaws.com/sagemaker-xgboost:1.7-1"

    response = sagemaker.create_training_job(
        TrainingJobName=job_name,
        RoleArn=SAGEMAKER_ROLE,
        AlgorithmSpecification={"TrainingImage": xgboost_image, "TrainingInputMode": "File"},
        InputDataConfig=[
            {
                "ChannelName": "train",
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": features_path,
                        "S3DataDistributionType": "FullyReplicated",
                    }
                },
                "ContentType": "text/csv",
            }
        ],
        OutputDataConfig={"S3OutputPath": f"s3://{DATA_BUCKET}/ml/models/{timestamp}/"},
        ResourceConfig={"InstanceType": "ml.m5.xlarge", "InstanceCount": 1, "VolumeSizeInGB": 30},
        StoppingCondition={"MaxRuntimeInSeconds": 3600},
        HyperParameters={
            "max_depth": "6",
            "eta": "0.1",
            "subsample": "0.8",
            "colsample_bytree": "0.8",
            "num_round": "1000",
            "objective": "reg:squarederror",
        },
    )

    return {
        "jobName": job_name,
        "jobArn": response["TrainingJobArn"],
        "modelPath": f"s3://{DATA_BUCKET}/ml/models/{timestamp}/",
        "timestamp": timestamp,
    }


def start_batch_transform(event):
    """
    Start SageMaker Batch Transform job.

    Args:
        event: Contains model_path from previous step

    Returns:
        Transform job ARN and details
    """
    model_path = event.get("modelPath")
    season = event.get("season", "2025-26")
    timestamp = event.get("timestamp", datetime.utcnow().strftime("%Y%m%d-%H%M%S"))
    job_name = f"nba-batch-predictions-{timestamp}"

    logger.info(f"Starting batch transform job: {job_name}")

    # First, create model from training artifacts
    model_name = f"nba-salary-model-{timestamp}"

    xgboost_image = f"683313688378.dkr.ecr.{REGION}.amazonaws.com/sagemaker-xgboost:1.7-1"

    sagemaker.create_model(
        ModelName=model_name,
        PrimaryContainer={
            "Image": xgboost_image,
            "ModelDataUrl": f"{model_path}output/model.tar.gz",
        },
        ExecutionRoleArn=SAGEMAKER_ROLE,
    )

    # Now create transform job
    input_path = f"s3://{DATA_BUCKET}/ml/features/{season}/current_season.csv"

    response = sagemaker.create_transform_job(
        TransformJobName=job_name,
        ModelName=model_name,
        TransformInput={
            "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": input_path}},
            "ContentType": "text/csv",
            "SplitType": "Line",
        },
        TransformOutput={
            "S3OutputPath": f"s3://{DATA_BUCKET}/ml/predictions/{timestamp}/",
            "AssembleWith": "Line",
        },
        TransformResources={"InstanceType": "ml.m5.xlarge", "InstanceCount": 1},
    )

    return {
        "jobName": job_name,
        "jobArn": response["TransformJobArn"],
        "predictionsPath": f"s3://{DATA_BUCKET}/ml/predictions/{timestamp}/predictions.csv.out",
        "timestamp": timestamp,
        "season": season,
    }


def invoke_load_predictions(event):
    """
    Invoke Lambda to load predictions to RDS.

    Args:
        event: Contains predictions_path from previous step

    Returns:
        Lambda invocation result
    """
    predictions_key = event.get("predictionsPath").replace(f"s3://{DATA_BUCKET}/", "")
    season = event.get("season", "2025-26")
    model_version = event.get("modelVersion", "v1.0.0")

    logger.info("Invoking load_predictions Lambda")

    function_name = os.environ.get("LOAD_PREDICTIONS_FUNCTION")

    payload = {
        "predictions_s3_key": predictions_key,
        "season": season,
        "model_version": model_version,
    }

    response = lambda_client.invoke(
        FunctionName=function_name, InvocationType="RequestResponse", Payload=json.dumps(payload)
    )

    result = json.loads(response["Payload"].read().decode())

    return {
        "statusCode": result.get("statusCode"),
        "body": result.get("body"),
        "predictionsLoaded": json.loads(result.get("body", "{}")).get("predictions_loaded", 0),
    }


def handler(event, context):
    """
    Lambda handler for Step Functions tasks.

    Routes to appropriate function based on task type.
    """
    logger.info(f"Orchestrator invoked with event: {json.dumps(event)}")

    task_type = event.get("task")

    if not task_type:
        return {"status": "Error", "message": "Missing required parameter: task"}

    if task_type == "feature_engineering":
        return start_feature_engineering(event)
    elif task_type == "model_training":
        return start_model_training(event)
    elif task_type == "batch_transform":
        return start_batch_transform(event)
    elif task_type == "load_predictions":
        return invoke_load_predictions(event)
    else:
        return {"status": "Error", "message": f"Unknown task type: {task_type}"}


# For local testing
if __name__ == "__main__":
    test_event = {"task": "feature_engineering", "season": "2025-26"}

    class Context:
        function_name = "ml_orchestrator"
        aws_request_id = "test-123"

    result = handler(test_event, Context())
    print(json.dumps(result, indent=2))
