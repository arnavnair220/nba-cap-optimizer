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


def apply_migrations(conn, s3_bucket):
    """
    Apply database migrations from S3.

    Creates a tracking table to record which migrations have been applied,
    then applies any pending migrations in alphabetical order.

    Migrations use idempotent SQL (IF NOT EXISTS), so safe to run on both
    fresh databases and existing databases.
    """
    logger.info("Checking for pending migrations...")

    cur = conn.cursor()

    try:
        # Create migrations tracking table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version VARCHAR(255) PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        logger.info("Schema migrations tracking table ready")

        # Get list of applied migrations
        cur.execute("SELECT version FROM schema_migrations ORDER BY version")
        applied_migrations = {row[0] for row in cur.fetchall()}
        logger.info(f"Already applied {len(applied_migrations)} migrations")

        # List all migration files from S3
        try:
            response = s3_client.list_objects_v2(
                Bucket=s3_bucket, Prefix="db/migrations/"
            )

            if "Contents" not in response:
                logger.info("No migration files found in S3")
                return {"migrations_applied": 0, "migrations": []}

            # Sort migrations by filename to ensure correct order
            migration_keys = sorted(
                [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".sql")]
            )

            logger.info(f"Found {len(migration_keys)} migration files in S3")

        except Exception as e:
            logger.warning(f"Could not list migrations from S3: {e}")
            return {"migrations_applied": 0, "migrations": []}

        # Apply unapplied migrations
        applied_count = 0
        applied_list = []

        for migration_key in migration_keys:
            # Extract version from key (e.g., "db/migrations/001_add_column.sql" -> "001_add_column.sql")
            version = migration_key.split("/")[-1]

            if version in applied_migrations:
                logger.info(f"Skipping already applied migration: {version}")
                continue

            logger.info(f"Applying migration: {version}")

            # Fetch migration SQL from S3
            migration_response = s3_client.get_object(Bucket=s3_bucket, Key=migration_key)
            migration_sql = migration_response["Body"].read().decode("utf-8")

            # Execute migration
            cur.execute(migration_sql)

            # Mark as applied
            cur.execute(
                "INSERT INTO schema_migrations (version) VALUES (%s)", (version,)
            )

            conn.commit()
            applied_count += 1
            applied_list.append(version)
            logger.info(f"Successfully applied migration: {version}")

        if applied_count > 0:
            logger.info(f"Applied {applied_count} new migrations")
        else:
            logger.info("No new migrations to apply")

        return {"migrations_applied": applied_count, "migrations": applied_list}

    except Exception as e:
        conn.rollback()
        logger.error(f"Error applying migrations: {e}")
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

        # Apply base schema (creates tables if they don't exist)
        schema_result = apply_schema(conn, schema_sql)

        # Apply migrations (adds columns/indexes to existing tables)
        migration_result = apply_migrations(conn, SCHEMA_S3_BUCKET)

        conn.close()

        # Combine results
        result = {
            "status": "success",
            "tables": schema_result["tables"],
            "migrations_applied": migration_result["migrations_applied"],
            "migrations": migration_result["migrations"],
            "message": (
                f"Schema applied. {len(schema_result['tables'])} tables exist. "
                f"{migration_result['migrations_applied']} migrations applied."
            ),
        }

        # Send success response to CloudFormation if needed
        if is_cfn_event:
            send_cfn_response(
                event,
                context,
                "SUCCESS",
                result["message"],
                physical_resource_id,
                data={
                    "tables": result["tables"],
                    "migrations_applied": result["migrations_applied"],
                },
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
