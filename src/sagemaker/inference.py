"""
SageMaker inference script for batch transform predictions.

This script is loaded by SageMaker Batch Transform jobs to make predictions
on new player data using a trained Random Forest model.
"""

import json
import logging
import pickle
from io import StringIO

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def model_fn(model_dir):
    """
    Load model from model directory.

    SageMaker calls this function to load the model artifacts.

    Args:
        model_dir: Path to the directory containing model artifacts

    Returns:
        Dictionary containing both models and metadata
    """
    logger.info(f"Loading models from {model_dir}")

    # Load both models
    with open(f"{model_dir}/model_salary_cap_pct.pkl", "rb") as f:
        model_cap = pickle.load(f)

    with open(f"{model_dir}/model_salary_pct_of_max.pkl", "rb") as f:
        model_fmv = pickle.load(f)

    # Load feature metadata
    with open(f"{model_dir}/model_salary_cap_pct_features.json", "r") as f:
        features_metadata = json.load(f)
        feature_cols = features_metadata["features"]

    logger.info(f"Loaded models with {len(feature_cols)} features")

    return {"model_cap": model_cap, "model_fmv": model_fmv, "features": feature_cols}


def input_fn(request_body, content_type="text/csv"):
    """
    Parse input data for prediction.

    SageMaker calls this to deserialize the prediction request.

    Args:
        request_body: The request payload
        content_type: Content type of the request

    Returns:
        Pandas DataFrame ready for prediction
    """
    logger.info(f"Parsing input with content_type: {content_type}")

    if content_type == "text/csv":
        # Read CSV from string
        df = pd.read_csv(StringIO(request_body))
        logger.info(f"Loaded {len(df)} records for prediction")
        return df
    else:
        raise ValueError(f"Unsupported content type: {content_type}")


def predict_fn(input_data, model):
    """
    Make predictions on input data.

    Args:
        input_data: DataFrame from input_fn
        model: Model dictionary from model_fn

    Returns:
        DataFrame with predictions
    """
    logger.info(f"Making predictions on {len(input_data)} records")

    # Extract models and features
    model_cap = model["model_cap"]
    model_fmv = model["model_fmv"]
    feature_cols = model["features"]

    # Keep player info
    player_info = input_data[["player_name", "season"]].copy()

    # Extract features for prediction
    X = input_data[feature_cols]

    # Make predictions with both models
    pred_cap = model_cap.predict(X)
    pred_fmv = model_fmv.predict(X)

    # Add predictions to output
    player_info["predicted_log_salary_cap_pct"] = pred_cap
    player_info["predicted_log_salary_pct_of_max"] = pred_fmv

    # Convert from log space to percentages
    # Note: exp() already returns percentages since we took log of percentages during training
    player_info["predicted_salary_cap_pct"] = np.exp(pred_cap)
    player_info["predicted_salary_pct_of_max"] = np.exp(pred_fmv)

    logger.info("Predictions complete")

    return player_info


def output_fn(prediction, accept="text/csv"):
    """
    Serialize predictions for response.

    Args:
        prediction: DataFrame from predict_fn
        accept: Desired content type

    Returns:
        Tuple of (serialized_data, content_type)
    """
    logger.info(f"Formatting output with accept: {accept}")

    if accept == "text/csv":
        output = StringIO()
        prediction.to_csv(output, index=False)
        return output.getvalue(), accept
    else:
        raise ValueError(f"Unsupported accept type: {accept}")
