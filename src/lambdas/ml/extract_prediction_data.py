"""
Lambda function to extract prediction data from RDS to S3.

Queries RDS for latest season player stats (last 7 days),
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
    Extract prediction data from RDS and save to S3.

    Args:
        event: Lambda event (no parameters needed)
        context: Lambda context

    Returns:
        Dict with S3 path to extracted data
    """
    try:
        data_bucket = os.environ["DATA_BUCKET"]

        logger.info("Extracting prediction data for latest season (last 7 days)")

        # Connect to database
        conn = get_db_connection()

        # Query for latest season and last 7 days
        query = """
        SELECT ps.*
        FROM player_stats ps
        WHERE ps.season = (SELECT MAX(season) FROM player_stats)
            AND ps.created_at >= NOW() - INTERVAL '7 days'
            AND ps.minutes > 0
            AND ps.games_played > 0
        ORDER BY ps.player_name
        """

        # Load data
        df = pd.read_sql(query, conn)
        conn.close()

        logger.info(f"Extracted {len(df)} player records for predictions")
        if len(df) > 0:
            logger.info(f"Season: {df['season'].iloc[0]}")
            logger.info(f"Date range: {df['created_at'].min()} to {df['created_at'].max()}")
        else:
            logger.warning("No data found for predictions")

        # Generate S3 path with timestamp
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        s3_key = f"ml-data/predictions/raw_data_{timestamp}.csv"
        s3_path = f"s3://{data_bucket}/{s3_key}"

        # Save to S3
        s3_client = boto3.client("s3")
        csv_buffer = df.to_csv(index=False)
        s3_client.put_object(Bucket=data_bucket, Key=s3_key, Body=csv_buffer)

        logger.info(f"Saved prediction data to {s3_path}")

        return {
            "statusCode": 200,
            "s3_path": s3_path,
            "s3_bucket": data_bucket,
            "s3_key": s3_key,
            "record_count": len(df),
        }

    except Exception as e:
        logger.error(f"Error extracting prediction data: {str(e)}", exc_info=True)
        raise
