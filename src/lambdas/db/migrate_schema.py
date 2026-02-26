"""
Lambda function to apply database schema on deployment.

This function is invoked by Terraform as a custom resource to ensure
database schema is created/updated after RDS is provisioned.

Uses CREATE TABLE IF NOT EXISTS so it's safe to run multiple times.
"""

import json
import logging
import os

import boto3
import psycopg2

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize clients
s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")

# Environment variables
DB_SECRET_ARN = os.environ.get("DB_SECRET_ARN")
SCHEMA_S3_BUCKET = os.environ.get("SCHEMA_S3_BUCKET")
SCHEMA_S3_KEY = os.environ.get("SCHEMA_S3_KEY", "db/schema.sql")


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


def load_schema_from_s3():
    """Load schema SQL from S3."""
    logger.info(f"Loading schema from s3://{SCHEMA_S3_BUCKET}/{SCHEMA_S3_KEY}")

    response = s3_client.get_object(Bucket=SCHEMA_S3_BUCKET, Key=SCHEMA_S3_KEY)
    schema_sql = response["Body"].read().decode("utf-8")

    logger.info(f"Loaded schema SQL ({len(schema_sql)} bytes)")
    return schema_sql


def apply_schema(conn, schema_sql):
    """
    Apply schema SQL to database.

    Uses CREATE TABLE IF NOT EXISTS, so safe to run multiple times.
    """
    logger.info("Applying schema to database...")

    cur = conn.cursor()

    try:
        # Execute schema SQL
        cur.execute(schema_sql)
        conn.commit()

        logger.info("Schema applied successfully")

        # Get list of tables created
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = [row[0] for row in cur.fetchall()]

        logger.info(f"Tables in database: {', '.join(tables)}")

        return {
            "status": "success",
            "tables": tables,
            "message": f"Schema applied. {len(tables)} tables exist.",
        }

    except Exception as e:
        conn.rollback()
        logger.error(f"Error applying schema: {e}")
        raise

    finally:
        cur.close()


def send_cfn_response(event, context, status, reason, physical_resource_id, data=None):
    """
    Send response to CloudFormation/Terraform custom resource.

    Required for Terraform to know if resource creation succeeded/failed.
    """
    import urllib3

    response_body = {
        "Status": status,
        "Reason": reason,
        "PhysicalResourceId": physical_resource_id,
        "StackId": event.get("StackId", ""),
        "RequestId": event.get("RequestId", ""),
        "LogicalResourceId": event.get("LogicalResourceId", ""),
        "Data": data or {},
    }

    response_url = event.get("ResponseURL")
    if not response_url:
        logger.warning("No ResponseURL in event, skipping CFN response")
        return

    logger.info(f"Sending response to CloudFormation: {status}")

    http = urllib3.PoolManager()
    response = http.request(
        "PUT",
        response_url,
        body=json.dumps(response_body).encode("utf-8"),
        headers={"Content-Type": ""},
    )

    logger.info(f"CloudFormation response status: {response.status}")


def handler(event, context):
    """
    Lambda handler for schema migration.

    Supports both:
    1. CloudFormation/Terraform custom resource events (Create/Update/Delete)
    2. Direct invocation for manual schema updates
    """
    logger.info("Schema migration invoked")
    logger.info(f"Event: {json.dumps(event)}")

    # Check if this is a CloudFormation custom resource event
    request_type = event.get("RequestType")
    is_cfn_event = request_type in ["Create", "Update", "Delete"]

    physical_resource_id = event.get("PhysicalResourceId", "schema-migration")

    try:
        # For Delete events, just acknowledge (don't drop tables!)
        if request_type == "Delete":
            logger.info("Delete event received - acknowledging without dropping tables")
            if is_cfn_event:
                send_cfn_response(
                    event,
                    context,
                    "SUCCESS",
                    "Schema migration delete acknowledged",
                    physical_resource_id,
                )
            return {"statusCode": 200, "body": json.dumps({"message": "Delete acknowledged"})}

        # For Create/Update or direct invocation, apply schema
        # Load schema SQL from S3
        schema_sql = load_schema_from_s3()

        # Connect to database
        conn = get_db_connection()

        # Apply schema
        result = apply_schema(conn, schema_sql)

        conn.close()

        # Send success response to CloudFormation if needed
        if is_cfn_event:
            send_cfn_response(
                event,
                context,
                "SUCCESS",
                result["message"],
                physical_resource_id,
                data={"tables": result["tables"]},
            )

        return {"statusCode": 200, "body": json.dumps(result)}

    except Exception as e:
        logger.error(f"Schema migration failed: {e}", exc_info=True)

        # Send failure response to CloudFormation if needed
        if is_cfn_event:
            send_cfn_response(
                event, context, "FAILED", f"Schema migration failed: {str(e)}", physical_resource_id
            )

        return {"statusCode": 500, "body": json.dumps({"status": "error", "message": str(e)})}


# For local testing
if __name__ == "__main__":
    test_event = {"RequestType": "Create"}

    class Context:
        function_name = "migrate_schema"
        aws_request_id = "test-123"

    result = handler(test_event, Context())
    print(json.dumps(result, indent=2))
