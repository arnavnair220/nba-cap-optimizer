"""
SageMaker training script for NBA salary prediction.

Trains two Random Forest regression models:
1. Salary as % of cap (actual contracts with CBA constraints)
2. Salary as % of personal max (Fair Market Value)

This script is executed by SageMaker Training Jobs and follows
the SageMaker container contract for model training.
"""

import argparse
import json
import logging
import os
import pickle
from typing import Dict, Tuple

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_data(data_path: str) -> Tuple[pd.DataFrame, list]:
    """
    Load feature-engineered data from CSV.

    Args:
        data_path: Path to features CSV file

    Returns:
        Tuple of (dataframe, feature_columns)
    """
    logger.info(f"Loading data from {data_path}")

    df = pd.read_csv(data_path)
    logger.info(f"Loaded {len(df)} records with {len(df.columns)} columns")

    # Feature columns (all except metadata and targets)
    exclude_cols = [
        "player_name",
        "season",
        "annual_salary",
        "salary_cap",
        "log_salary_cap_pct",
        "log_salary_pct_of_max",
    ]
    feature_cols = [col for col in df.columns if col not in exclude_cols]

    logger.info(f"Using {len(feature_cols)} features")

    return df, feature_cols


def split_data(
    df: pd.DataFrame,
    feature_cols: list,
    target_col: str,
    test_size: float = 0.2,
    random_state: int = 42,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    """
    Split data into train and test sets.

    Args:
        df: Feature dataframe
        feature_cols: List of feature column names
        target_col: Target column name
        test_size: Fraction for test set
        random_state: Random seed

    Returns:
        Tuple of (X_train, X_test, y_train, y_test)
    """
    logger.info(f"Splitting data: {100*(1-test_size):.0f}% train, {100*test_size:.0f}% test")

    X = df[feature_cols]
    y = df[target_col]

    # Remove any rows with missing targets
    valid_mask = ~y.isna()
    X = X[valid_mask]
    y = y[valid_mask]

    logger.info(f"After removing missing targets: {len(X)} records")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )

    logger.info(f"Train set: {len(X_train)} records")
    logger.info(f"Test set: {len(X_test)} records")

    return X_train, X_test, y_train, y_test


def train_model(
    X_train: pd.DataFrame, y_train: pd.Series, X_test: pd.DataFrame, y_test: pd.Series, params: Dict
) -> RandomForestRegressor:
    """
    Train Random Forest model.

    Args:
        X_train: Training features
        y_train: Training targets
        X_test: Test features
        y_test: Test targets
        params: Random Forest hyperparameters

    Returns:
        Trained Random Forest model
    """
    logger.info("Training Random Forest model...")
    logger.info(f"Hyperparameters: {params}")

    # Create and train Random Forest model
    model = RandomForestRegressor(
        n_estimators=params.get("n_estimators", 200),
        max_depth=params.get("max_depth", 20),
        min_samples_split=params.get("min_samples_split", 5),
        min_samples_leaf=params.get("min_samples_leaf", 2),
        max_features=params.get("max_features", "sqrt"),
        random_state=params.get("random_state", 42),
        n_jobs=-1,
        verbose=1,
    )

    model.fit(X_train, y_train)

    logger.info("Training complete")

    return model


def evaluate_model(
    model: RandomForestRegressor, X_test: pd.DataFrame, y_test: pd.Series, target_name: str
) -> Dict:
    """
    Evaluate model performance on test set.

    Args:
        model: Trained Random Forest model
        X_test: Test features
        y_test: Test targets
        target_name: Name of target variable

    Returns:
        Dictionary of evaluation metrics
    """
    logger.info(f"Evaluating model for {target_name}...")

    y_pred = model.predict(X_test)

    # Calculate metrics
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    # Calculate percentage of predictions within ±20% (in log space)
    # Convert back from log space for comparison
    y_test_actual = np.exp(y_test) - 1e-6
    y_pred_actual = np.exp(y_pred) - 1e-6

    percent_diff = np.abs((y_pred_actual - y_test_actual) / (y_test_actual + 1e-6)) * 100
    within_20pct = (percent_diff <= 20).mean() * 100

    metrics = {
        "target": target_name,
        "rmse": float(rmse),
        "mae": float(mae),
        "r2": float(r2),
        "within_20pct": float(within_20pct),
        "test_samples": len(y_test),
    }

    logger.info(f"Metrics for {target_name}:")
    logger.info(f"  RMSE: {rmse:.4f}")
    logger.info(f"  MAE: {mae:.4f}")
    logger.info(f"  R²: {r2:.4f}")
    logger.info(f"  Within ±20%: {within_20pct:.2f}%")

    return metrics


def save_model(
    model: RandomForestRegressor,
    feature_cols: list,
    metrics: Dict,
    output_dir: str,
    model_name: str,
):
    """
    Save trained model and metadata.

    Args:
        model: Trained Random Forest model
        feature_cols: List of feature columns
        metrics: Evaluation metrics
        output_dir: Directory to save model
        model_name: Name for model file
    """
    logger.info(f"Saving model to {output_dir}/{model_name}")

    os.makedirs(output_dir, exist_ok=True)

    # Save Random Forest model using pickle
    model_path = os.path.join(output_dir, f"{model_name}.pkl")
    with open(model_path, "wb") as f:
        pickle.dump(model, f)

    # Save feature columns
    features_path = os.path.join(output_dir, f"{model_name}_features.json")
    with open(features_path, "w") as f:
        json.dump({"features": feature_cols}, f, indent=2)

    # Save metrics
    metrics_path = os.path.join(output_dir, f"{model_name}_metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)

    # Save model metadata
    metadata = {
        "model_name": model_name,
        "algorithm": "RandomForest",
        "num_features": len(feature_cols),
        "n_estimators": model.n_estimators,
        "metrics": metrics,
    }
    metadata_path = os.path.join(output_dir, f"{model_name}_metadata.json")
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    logger.info("Model saved successfully")


def main():
    """Main training function."""
    parser = argparse.ArgumentParser()

    # Data paths (set by SageMaker)
    parser.add_argument("--train", type=str, default=os.environ.get("SM_CHANNEL_TRAIN"))
    parser.add_argument("--model-dir", type=str, default=os.environ.get("SM_MODEL_DIR"))

    # Hyperparameters
    parser.add_argument("--n-estimators", type=int, default=200)
    parser.add_argument("--max-depth", type=int, default=20)
    parser.add_argument("--min-samples-split", type=int, default=5)
    parser.add_argument("--min-samples-leaf", type=int, default=2)
    parser.add_argument("--max-features", type=str, default="sqrt")
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--random-state", type=int, default=42)

    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("NBA Salary Prediction Training")
    logger.info("=" * 80)

    # Find CSV file in train directory
    train_files = [f for f in os.listdir(args.train) if f.endswith(".csv")]
    if not train_files:
        raise ValueError(f"No CSV files found in {args.train}")

    data_path = os.path.join(args.train, train_files[0])

    # Load data
    df, feature_cols = load_data(data_path)

    # Random Forest parameters
    rf_params = {
        "n_estimators": args.n_estimators,
        "max_depth": args.max_depth,
        "min_samples_split": args.min_samples_split,
        "min_samples_leaf": args.min_samples_leaf,
        "max_features": args.max_features,
        "random_state": args.random_state,
    }

    # Train Model 1: Salary as % of cap
    logger.info("\n" + "=" * 80)
    logger.info("Training Model 1: Salary as % of Cap (CBA-constrained)")
    logger.info("=" * 80)

    X_train_cap, X_test_cap, y_train_cap, y_test_cap = split_data(
        df,
        feature_cols,
        "log_salary_cap_pct",
        test_size=args.test_size,
        random_state=args.random_state,
    )

    model_cap = train_model(X_train_cap, y_train_cap, X_test_cap, y_test_cap, rf_params)

    metrics_cap = evaluate_model(model_cap, X_test_cap, y_test_cap, "log_salary_cap_pct")

    save_model(model_cap, feature_cols, metrics_cap, args.model_dir, "model_salary_cap_pct")

    # Train Model 2: Salary as % of personal max (FMV)
    logger.info("\n" + "=" * 80)
    logger.info("Training Model 2: Salary as % of Personal Max (Fair Market Value)")
    logger.info("=" * 80)

    X_train_fmv, X_test_fmv, y_train_fmv, y_test_fmv = split_data(
        df,
        feature_cols,
        "log_salary_pct_of_max",
        test_size=args.test_size,
        random_state=args.random_state,
    )

    model_fmv = train_model(X_train_fmv, y_train_fmv, X_test_fmv, y_test_fmv, rf_params)

    metrics_fmv = evaluate_model(model_fmv, X_test_fmv, y_test_fmv, "log_salary_pct_of_max")

    save_model(model_fmv, feature_cols, metrics_fmv, args.model_dir, "model_salary_pct_of_max")

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("Training Complete!")
    logger.info("=" * 80)
    logger.info(
        f"Model 1 (salary_cap_pct): RMSE={metrics_cap['rmse']:.4f}, "
        f"R²={metrics_cap['r2']:.4f}, Within ±20%={metrics_cap['within_20pct']:.2f}%"
    )
    logger.info(
        f"Model 2 (salary_pct_of_max): RMSE={metrics_fmv['rmse']:.4f}, "
        f"R²={metrics_fmv['r2']:.4f}, Within ±20%={metrics_fmv['within_20pct']:.2f}%"
    )


if __name__ == "__main__":
    main()
