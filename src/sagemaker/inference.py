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

    # Load smearing factors (for retransformation bias correction)
    smearing_cap = None
    smearing_fmv = None
    try:
        with open(f"{model_dir}/model_salary_cap_pct_smearing.json", "r") as f:
            data = json.load(f)
            smearing_cap = data["smearing_factor"]
            logger.info(f"Loaded smearing factor for salary_cap_pct: {smearing_cap:.4f}")
    except FileNotFoundError:
        logger.warning(
            "No smearing factor found for model_salary_cap_pct, predictions may be biased"
        )

    try:
        with open(f"{model_dir}/model_salary_pct_of_max_smearing.json", "r") as f:
            data = json.load(f)
            smearing_fmv = data["smearing_factor"]
            logger.info(f"Loaded smearing factor for salary_pct_of_max: {smearing_fmv:.4f}")
    except FileNotFoundError:
        logger.warning(
            "No smearing factor found for model_salary_pct_of_max, predictions may be biased"
        )

    logger.info(f"Loaded models with {len(feature_cols)} features")

    return {
        "model_cap": model_cap,
        "model_fmv": model_fmv,
        "features": feature_cols,
        "smearing_cap": smearing_cap,
        "smearing_fmv": smearing_fmv,
    }


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


def apply_smearing(log_predictions, smearing_factor):
    """
    Apply smearing correction to log-space predictions.

    Corrects retransformation bias when converting from log space back to original space.
    Uses the Duan (1983) smearing estimator.

    Args:
        log_predictions: Predictions in log space
        smearing_factor: Single smearing factor (float) or None

    Returns:
        Corrected predictions in original space
    """
    if smearing_factor is None:
        logger.warning("No smearing factor provided, using naive exp() transformation")
        return np.exp(log_predictions) - 1e-6

    # Convert to original space and apply smearing correction
    naive_predictions = np.exp(log_predictions) - 1e-6
    corrected_predictions = naive_predictions * smearing_factor

    return corrected_predictions


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
    smearing_cap = model.get("smearing_cap")
    smearing_fmv = model.get("smearing_fmv")

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

    # Convert from log space to percentages with smearing correction
    player_info["predicted_salary_cap_pct"] = apply_smearing(pred_cap, smearing_cap)
    player_info["predicted_salary_pct_of_max"] = apply_smearing(pred_fmv, smearing_fmv)

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
