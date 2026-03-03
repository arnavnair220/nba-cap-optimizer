"""
Lambda function to load batch prediction results from S3 to RDS.

This function is triggered after SageMaker Batch Transform completes.
It reads prediction CSV from S3, joins with player data, and loads to predictions table.
"""

import csv
import json
import logging
import os
from datetime import datetime
from io import StringIO
from typing import Any, Dict, List

import boto3
import psycopg2
from psycopg2.extras import execute_batch

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize clients
s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")

# Environment variables
DB_SECRET_ARN = os.environ.get("DB_SECRET_ARN")
DATA_BUCKET = os.environ.get("DATA_BUCKET")


def get_db_connection():
    """Get database connection from Secrets Manager."""
    logger.info(f"Loading DB credentials from {DB_SECRET_ARN}")
    secret_value = secrets_client.get_secret_value(SecretId=DB_SECRET_ARN)
    credentials = json.loads(secret_value["SecretString"])

    conn = psycopg2.connect(
        host=credentials["host"],
        port=credentials["port"],
        database=credentials["dbname"],
        user=credentials["username"],
        password=credentials["password"],
    )
    return conn


def load_predictions_from_s3(bucket: str, key: str) -> List[Dict[str, Any]]:
    """
    Load prediction results from S3 CSV.

    Expected CSV format:
    player_name,season,predicted_salary_cap_pct,predicted_salary_pct_of_max

    Args:
        bucket: S3 bucket name
        key: S3 object key

    Returns:
        List of prediction dictionaries
    """
    logger.info(f"Loading predictions from s3://{bucket}/{key}")

    response = s3_client.get_object(Bucket=bucket, Key=key)
    csv_content = response["Body"].read().decode("utf-8")

    predictions = []
    reader = csv.DictReader(StringIO(csv_content))

    for row in reader:
        predictions.append(
            {
                "player_name": row["player_name"],
                "season": row["season"],
                "predicted_salary_cap_pct": float(row["predicted_salary_cap_pct"]),
                "predicted_salary_pct_of_max": float(row["predicted_salary_pct_of_max"]),
            }
        )

    logger.info(f"Loaded {len(predictions)} predictions")
    return predictions


def enrich_predictions_with_actuals(conn, predictions: List[Dict], season: str) -> List[Dict]:
    """
    Enrich predictions with actual salaries and player stats from database.

    Args:
        conn: Database connection
        predictions: List of prediction dictionaries
        season: Season to query

    Returns:
        List of enriched prediction dictionaries
    """
    logger.info(f"Enriching predictions with actual data for season {season}")

    cur = conn.cursor()

    # Query to get actual salaries and VORP
    query = """
    SELECT
        ps.player_name,
        s.annual_salary,
        sch.salary_cap,
        ps.vorp
    FROM player_stats ps
    LEFT JOIN salaries s
        ON ps.player_name = s.player_name
        AND ps.season = s.season
    LEFT JOIN salary_cap_history sch
        ON ps.season = sch.season
    WHERE ps.season = %s
    """

    cur.execute(query, (season,))
    actuals = {row[0]: row for row in cur.fetchall()}  # Index by player_name

    enriched = []

    for pred in predictions:
        player_name = pred["player_name"]

        if player_name in actuals:
            actual_salary, salary_cap, vorp = actuals[player_name][1:]

            # Convert predictions from log space to actual values
            # predicted_salary_cap_pct is already in percentage
            # predicted_salary_pct_of_max is also in percentage

            # Calculate predicted FMV in dollars (using % of cap prediction)
            if salary_cap and pred["predicted_salary_cap_pct"]:
                predicted_fmv = int((pred["predicted_salary_cap_pct"] / 100) * salary_cap)
            else:
                predicted_fmv = None

            # Calculate actual salary as % of cap
            actual_salary_cap_pct = (
                (actual_salary / salary_cap * 100) if salary_cap and actual_salary else None
            )

            # Calculate inefficiency score (how over/under valued)
            # Positive = overpaid, Negative = underpaid
            if predicted_fmv and actual_salary:
                inefficiency_score = (actual_salary - predicted_fmv) / predicted_fmv
            else:
                inefficiency_score = None

            # Categorize value
            value_category = None
            if inefficiency_score is not None:
                if inefficiency_score < -0.20:  # Paid < 80% of predicted FMV
                    value_category = "Bargain"
                elif inefficiency_score > 0.20:  # Paid > 120% of predicted FMV
                    value_category = "Overpaid"
                else:
                    value_category = "Fair"

            enriched_pred = {
                **pred,
                "predicted_fmv": predicted_fmv,
                "actual_salary": actual_salary,
                "actual_salary_cap_pct": actual_salary_cap_pct,
                "value_over_replacement": vorp,
                "inefficiency_score": inefficiency_score,
                "value_category": value_category,
            }

            enriched.append(enriched_pred)
        else:
            logger.warning(f"No actual data found for {player_name} in season {season}")

    logger.info(f"Enriched {len(enriched)} predictions with actual data")
    return enriched


def load_predictions_to_db(
    conn, predictions: List[Dict], model_version: str, run_id: str, etl_run_id: str
):
    """
    Load enriched predictions to database.

    Uses UPSERT (INSERT ... ON CONFLICT UPDATE) to handle re-runs.

    Args:
        conn: Database connection
        predictions: List of enriched prediction dictionaries
        model_version: Model version identifier
        run_id: Unique identifier for this prediction run
        etl_run_id: ETL run ID that sourced the player stats
    """
    logger.info(f"Loading {len(predictions)} predictions to database")

    cur = conn.cursor()

    # UPSERT query
    query = """
    INSERT INTO predictions (
        player_name, season,
        predicted_salary_cap_pct, predicted_salary_pct_of_max, predicted_fmv,
        actual_salary, actual_salary_cap_pct,
        value_over_replacement, inefficiency_score, value_category,
        model_version, run_id, etl_run_id, prediction_date
    ) VALUES (
        %s, %s,
        %s, %s, %s,
        %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s
    )
    ON CONFLICT (player_name, season, model_version, run_id)
    DO UPDATE SET
        predicted_salary_cap_pct = EXCLUDED.predicted_salary_cap_pct,
        predicted_salary_pct_of_max = EXCLUDED.predicted_salary_pct_of_max,
        predicted_fmv = EXCLUDED.predicted_fmv,
        actual_salary = EXCLUDED.actual_salary,
        actual_salary_cap_pct = EXCLUDED.actual_salary_cap_pct,
        value_over_replacement = EXCLUDED.value_over_replacement,
        inefficiency_score = EXCLUDED.inefficiency_score,
        value_category = EXCLUDED.value_category,
        etl_run_id = EXCLUDED.etl_run_id,
        prediction_date = EXCLUDED.prediction_date
    """

    prediction_date = datetime.utcnow()

    # Prepare batch data
    batch_data = [
        (
            pred["player_name"],
            pred["season"],
            pred.get("predicted_salary_cap_pct"),
            pred.get("predicted_salary_pct_of_max"),
            pred.get("predicted_fmv"),
            pred.get("actual_salary"),
            pred.get("actual_salary_cap_pct"),
            pred.get("value_over_replacement"),
            pred.get("inefficiency_score"),
            pred.get("value_category"),
            model_version,
            run_id,
            etl_run_id,
            prediction_date,
        )
        for pred in predictions
    ]

    # Execute batch insert
    execute_batch(cur, query, batch_data, page_size=100)
    conn.commit()

    logger.info(f"Successfully loaded {len(predictions)} predictions to database")

    # Log summary stats
    cur.execute(
        """
        SELECT
            value_category,
            COUNT(*) as count,
            AVG(inefficiency_score) as avg_inefficiency
        FROM predictions
        WHERE model_version = %s
        GROUP BY value_category
    """,
        (model_version,),
    )

    summary = cur.fetchall()
    logger.info("Prediction summary:")
    for category, count, avg_inefficiency in summary:
        logger.info(
            f"  {category}: {count} players, " f"avg inefficiency: {avg_inefficiency:.2%}"
            if avg_inefficiency
            else "N/A"
        )


def handler(event, context):
    """
    Lambda handler for loading batch predictions.

    Expected event structure:
    {
        "predictions_s3_key": "predictions/2025-26/predictions.csv",
        "season": "2025-26",
        "model_version": "v1.0.0",
        "run_id": "predictions-run-2026-02-28-123456",
        "etl_run_id": "etl-run-2026-02-28-123456"
    }
    """
    logger.info("Starting prediction load Lambda")
    logger.info(f"Event: {json.dumps(event)}")

    # Validate environment variables
    if not DB_SECRET_ARN:
        logger.error("DB_SECRET_ARN environment variable is not set")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "DB_SECRET_ARN is required"}),
        }

    if not DATA_BUCKET:
        logger.error("DATA_BUCKET environment variable is not set")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "DATA_BUCKET is required"}),
        }

    # Get parameters from event
    predictions_s3_key = event.get("predictions_s3_key") or event.get("predictionsPath")
    season = event.get("season", "2025-26")
    model_version = event.get("model_version") or event.get("modelVersion", "v1.0.0")
    run_id = event.get("run_id") or event.get("runId")
    etl_run_id = event.get("etl_run_id") or event.get("etlRunId")

    if not predictions_s3_key:
        logger.error("Missing predictions_s3_key in event")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "predictions_s3_key is required"}),
        }

    if not run_id:
        logger.error("Missing run_id in event")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "run_id is required"}),
        }

    if not etl_run_id:
        logger.warning("Missing etl_run_id in event, using 'unknown'")
        etl_run_id = "unknown"

    try:
        # Load predictions from S3
        predictions = load_predictions_from_s3(DATA_BUCKET, predictions_s3_key)

        if not predictions:
            logger.warning("No predictions found in S3 file")
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "message": "No predictions to load",
                        "predictions_loaded": 0,
                    }
                ),
            }

        # Connect to database
        conn = get_db_connection()

        # Enrich predictions with actual data
        enriched_predictions = enrich_predictions_with_actuals(conn, predictions, season)

        # Load to database
        load_predictions_to_db(conn, enriched_predictions, model_version, run_id, etl_run_id)

        conn.close()

        # Return success
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Predictions loaded successfully",
                    "predictions_loaded": len(enriched_predictions),
                    "season": season,
                    "model_version": model_version,
                    "run_id": run_id,
                }
            ),
        }

    except Exception as e:
        logger.error(f"Error loading predictions: {e}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "error": str(e),
                    "message": "Failed to load predictions",
                }
            ),
        }


# For local testing
if __name__ == "__main__":
    test_event = {
        "predictions_s3_key": "predictions/2025-26/predictions.csv",
        "season": "2025-26",
        "model_version": "v1.0.0",
        "run_id": "test-run-123",
    }

    class Context:
        function_name = "load_predictions"
        aws_request_id = "test-123"

    result = handler(test_event, Context())
    print(json.dumps(result, indent=2))
