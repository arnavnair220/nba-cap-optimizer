"""
Fetch NBA data from APIs
TODO: Implement actual data fetching
"""


def handler(event, context):
    """Lambda handler for fetching NBA data"""
    print("Fetch data Lambda triggered")
    print(f"Event: {event}")

    # TODO: Implement NBA API calls
    # TODO: Save raw data to S3

    return {"statusCode": 200, "body": "Data fetching placeholder - not implemented yet"}
