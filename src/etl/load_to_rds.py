"""
Load transformed data to RDS PostgreSQL
TODO: Implement database loading
"""


def handler(event, context):
    """Lambda handler for loading data to RDS"""
    print("Load to RDS Lambda triggered")
    print(f"Event: {event}")

    # TODO: Implement database connection
    # TODO: Upsert data to PostgreSQL tables

    return {"statusCode": 200, "body": "Database loading placeholder - not implemented yet"}
