"""
Lambda function to extract training data from RDS to S3.

Queries RDS for player stats and salaries before a specified season,
and saves the result as CSV to S3 for SageMaker processing.
"""

import json
import logging
import os
from datetime import datetime

import boto3
import pandas as pd
import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_db_connection():
    """
    Get database connection using credentials from Secrets Manager.

    Returns:
        psycopg2 connection object
    """
    db_secret_arn = os.environ["DB_SECRET_ARN"]

    secrets_client = boto3.client("secretsmanager")
    secret_value = secrets_client.get_secret_value(SecretId=db_secret_arn)
    credentials = json.loads(secret_value["SecretString"])

    conn = psycopg2.connect(
        host=credentials["host"],
        port=credentials["port"],
        database=credentials["dbname"],
        user=credentials["username"],
        password=credentials["password"],
    )

    return conn


def lambda_handler(event, context):
    """
    Extract training data from RDS and save to S3.

    Args:
        event: Lambda event with optional 'seasons_before' parameter
        context: Lambda context

    Returns:
        Dict with S3 path to extracted data
    """
    try:
        # Get parameters
        seasons_before = event.get("seasons_before", "2025-26")
        data_bucket = os.environ["DATA_BUCKET"]

        logger.info(f"Extracting training data for seasons before {seasons_before}")

        # Connect to database
        conn = get_db_connection()

        # Query to join player_stats with salaries
        query = """
        SELECT
            ps.*,
            s.annual_salary,
            s.season as salary_season
        FROM player_stats ps
        INNER JOIN salaries s
            ON ps.player_name = s.player_name
            AND ps.season = s.season
        WHERE ps.season < %s
            AND s.annual_salary > 0
            AND ps.minutes > 0
            AND ps.games_played > 0
        ORDER BY ps.season, ps.player_name
        """

        # Load data
        df = pd.read_sql(query, conn, params=(seasons_before,))
        conn.close()

        logger.info(f"Extracted {len(df)} player-season records")
        logger.info(f"Seasons: {sorted(df['season'].unique())}")

        # Generate S3 path with timestamp
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        s3_key = f"ml-data/training/raw_data_{timestamp}.csv"
        s3_path = f"s3://{data_bucket}/{s3_key}"

        # Save to S3
        s3_client = boto3.client("s3")
        csv_buffer = df.to_csv(index=False)
        s3_client.put_object(Bucket=data_bucket, Key=s3_key, Body=csv_buffer)

        logger.info(f"Saved training data to {s3_path}")

        return {
            "statusCode": 200,
            "s3_path": s3_path,
            "s3_bucket": data_bucket,
            "s3_key": s3_key,
            "record_count": len(df),
            "seasons": sorted(df["season"].unique().tolist()),
        }

    except Exception as e:
        logger.error(f"Error extracting training data: {str(e)}", exc_info=True)
        raise
