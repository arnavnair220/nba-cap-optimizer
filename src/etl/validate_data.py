"""
Validate NBA data fetched from APIs.
This Lambda function validates the structure and quality of fetched data
using JSON schemas and data quality checks.
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, cast

import boto3
from botocore.exceptions import ClientError
from jsonschema import ValidationError, validate

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client
s3_client = boto3.client("s3")

# Environment variables (validated in handler to allow module imports for testing)
S3_BUCKET = os.environ.get("DATA_BUCKET")
ENVIRONMENT = os.environ.get("ENVIRONMENT")

# JSON Schemas for different data types
PLAYER_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["id", "full_name"],
    "properties": {
        "id": {"type": "integer"},
        "full_name": {"type": "string", "minLength": 1},
        "first_name": {"type": "string"},
        "last_name": {"type": "string"},
        "is_active": {"type": "boolean"},
    },
}

PLAYER_STATS_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["season", "fetch_timestamp", "players"],
    "properties": {
        "season": {"type": "string", "pattern": "^\\d{4}-\\d{2}$"},
        "fetch_timestamp": {"type": "string", "format": "date-time"},
        "players": {
            "type": "object",
            "required": ["resultSets"],
            "properties": {"resultSets": {"type": "array", "minItems": 1}},
        },
    },
}

TEAM_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["id", "full_name", "abbreviation"],
    "properties": {
        "id": {"type": "integer"},
        "full_name": {"type": "string", "minLength": 1},
        "abbreviation": {"type": "string", "minLength": 2, "maxLength": 4},
        "nickname": {"type": "string"},
        "city": {"type": "string"},
        "state": {"type": "string"},
        "year_founded": {"type": "integer", "minimum": 1946},
    },
}

SALARY_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["fetch_timestamp", "source", "salaries"],
    "properties": {
        "fetch_timestamp": {"type": "string", "format": "date-time"},
        "source": {"type": "string"},
        "error": {"type": "string"},  # Present only when fetch fails
        "salaries": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["player_name", "annual_salary", "season", "source"],
                "properties": {
                    "player_name": {"type": "string"},
                    "annual_salary": {"type": "number", "minimum": 0},
                    "season": {"type": "string"},
                    "source": {"type": "string"},
                    # Fields added during transform (not in raw data):
                    # - player_id: Added by joining with active_players
                    # - contract_years: From future data sources
                    # - cap_hit: From future data sources
                },
            },
        },
    },
}


def load_from_s3(s3_key: str) -> Optional[Dict[str, Any]]:
    """
    Load JSON data from S3.

    Args:
        s3_key: S3 object key

    Returns:
        Parsed JSON data or None if error
    """
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        data = json.loads(response["Body"].read().decode("utf-8"))
        logger.info(f"Successfully loaded data from s3://{S3_BUCKET}/{s3_key}")
        return cast(Dict[str, Any], data)
    except ClientError as e:
        logger.error(f"Failed to load from S3: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in S3 object: {e}")
        return None


def save_validation_report(report: Dict[str, Any], s3_key: str) -> bool:
    """
    Save validation report to S3.

    Args:
        report: Validation report
        s3_key: S3 object key

    Returns:
        Success status
    """
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(report, default=str, indent=2),
            ContentType="application/json",
        )
        logger.info(f"Saved validation report to s3://{S3_BUCKET}/{s3_key}")
        return True
    except ClientError as e:
        logger.error(f"Failed to save validation report: {e}")
        return False


def validate_json_schema(data: Any, schema: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate data against a JSON schema.

    Args:
        data: Data to validate
        schema: JSON schema

    Returns:
        Tuple of (is_valid, error_messages)
    """
    try:
        validate(instance=data, schema=schema)
        return True, []
    except ValidationError as e:
        return False, [str(e)]


def validate_players_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate players data.

    Args:
        data: Players data

    Returns:
        Validation results
    """
    errors: List[str] = []
    warnings: List[str] = []
    statistics: Dict[str, Any] = {}
    valid = True

    if "players" not in data:
        errors.append("Missing 'players' key in data")
        return {
            "data_type": "players",
            "valid": False,
            "errors": errors,
            "warnings": warnings,
            "statistics": statistics,
        }

    players = data["players"]
    statistics["total_players"] = len(players)

    # Validate each player
    invalid_players = []
    for i, player in enumerate(players):
        is_valid_player, player_errors = validate_json_schema(player, PLAYER_SCHEMA)
        if not is_valid_player:
            invalid_players.append(f"Player {i}: {player_errors}")

    if invalid_players:
        valid = False
        errors.extend(invalid_players[:10])  # Limit to first 10 errors
        statistics["invalid_players"] = len(invalid_players)

    # Data quality checks
    # Note: fetch_active_players() already returns only active players, so total_players == active
    total_count = len(players)
    if total_count < 400:
        warnings.append(f"Low player count: {total_count} (expected 450+)")
    elif total_count > 600:
        warnings.append(f"High player count: {total_count} (expected ~450-550)")

    # Check for duplicate player IDs
    player_ids = [p.get("id") for p in players if p.get("id") is not None]
    unique_ids = set(player_ids)
    if len(player_ids) != len(unique_ids):
        duplicates = len(player_ids) - len(unique_ids)
        valid = False
        errors.append(f"Found {duplicates} duplicate player IDs")
        statistics["duplicate_players"] = duplicates

    return {
        "data_type": "players",
        "valid": valid,
        "errors": errors,
        "warnings": warnings,
        "statistics": statistics,
    }


def _validate_season_and_timestamp(data: Dict[str, Any], warnings: List[str]) -> None:
    """
    Validate season format/year and fetch timestamp.

    Args:
        data: Stats data
        warnings: List to append warnings to
    """
    # Validate season format and year
    season = data.get("season", "")
    if season:
        try:
            # Parse season format like "2024-25"
            start_year = int(season.split("-")[0])
            current_year = datetime.utcnow().year
            if start_year < 1946:  # NBA founded in 1946
                warnings.append(f"Season year {start_year} is before NBA founding (1946)")
            elif start_year > current_year + 1:
                warnings.append(
                    f"Season year {start_year} is in the future (current: {current_year})"
                )
        except (ValueError, IndexError):
            pass  # Already validated by schema

    # Validate timestamp is not in the future
    fetch_timestamp = data.get("fetch_timestamp", "")
    if fetch_timestamp:
        try:
            fetch_dt = datetime.fromisoformat(fetch_timestamp.replace("Z", "+00:00"))
            if fetch_dt > datetime.utcnow():
                warnings.append("Fetch timestamp is in the future")
        except ValueError:
            pass  # Already validated by schema


def _check_missing_data(
    rows: List[Any], warnings: List[str], errors: List[str], statistics: Dict[str, Any]
) -> bool:
    """
    Check for missing values in key columns.

    Args:
        rows: Data rows
        warnings: List to append warnings to
        errors: List to append errors to
        statistics: Statistics dict to update

    Returns:
        True if validation passes, False if critical error
    """
    if not rows:
        return True

    missing_stats = 0
    for row in rows:
        if any(val is None for val in row[:10]):  # Check first 10 columns
            missing_stats += 1

    statistics["players_with_missing_data"] = missing_stats

    if missing_stats > 0:
        warnings.append(
            f"Found {missing_stats}/{len(rows)} players with missing data in key columns"
        )

    if missing_stats > len(rows) * 0.05:  # More than 5% with missing data
        errors.append(
            f"CRITICAL: High missing data rate: {missing_stats}/{len(rows)} players "
            f"({missing_stats/len(rows)*100:.1f}%) exceeds 5% threshold"
        )
        return False

    return True


def _validate_stat_ranges(
    headers: List[Any], rows: List[Any], warnings: List[str], statistics: Dict[str, Any]
) -> None:
    """
    Validate statistical values are within realistic ranges.

    Args:
        headers: Column headers
        rows: Data rows
        warnings: List to append warnings to
        statistics: Statistics dict to update
    """
    # Try to find common stat columns (case-insensitive)
    headers_lower = [h.lower() if isinstance(h, str) else "" for h in headers]
    stat_checks = {
        "ppg": (80, "points per game"),  # 0-80
        "pts": (80, "points per game"),
        "rpg": (30, "rebounds per game"),  # 0-30
        "reb": (30, "rebounds per game"),
        "apg": (25, "assists per game"),  # 0-25
        "ast": (25, "assists per game"),
        "mpg": (60, "minutes per game"),  # 0-60 (accounts for OT)
        "min": (60, "minutes per game"),
    }

    unrealistic_values = []
    for stat_key, (max_val, stat_name) in stat_checks.items():
        try:
            stat_idx = headers_lower.index(stat_key)
            for row_idx, row in enumerate(rows):
                if len(row) > stat_idx and row[stat_idx] is not None:
                    value = float(row[stat_idx])
                    if value < 0 or value > max_val:
                        unrealistic_values.append(
                            f"Row {row_idx}: {stat_name} = {value} (expected 0-{max_val})"
                        )
        except (ValueError, IndexError):
            # Stat column not found or conversion error - skip this check
            pass

    if unrealistic_values:
        warnings.append(
            f"Found {len(unrealistic_values)} unrealistic stat values "
            f"(first few: {', '.join(unrealistic_values[:3])})"
        )
        statistics["unrealistic_stat_values"] = len(unrealistic_values)


def validate_stats_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate player statistics data.

    Args:
        data: Stats data

    Returns:
        Validation results
    """
    results = {"data_type": "stats", "valid": True, "errors": [], "warnings": [], "statistics": {}}

    # Schema validation
    is_valid, errors = validate_json_schema(data, PLAYER_STATS_SCHEMA)
    if not is_valid:
        results["valid"] = False
        results["errors"].extend(errors)
        return results

    # Validate season and timestamp
    _validate_season_and_timestamp(data, cast(List[str], results["warnings"]))

    # Extract stats
    try:
        result_sets = data["players"]["resultSets"][0]
        headers = result_sets.get("headers", [])
        rows = result_sets.get("rowSet", [])

        results["statistics"]["total_players"] = len(rows)
        results["statistics"]["stat_columns"] = len(headers)

        # Data quality checks
        if len(rows) < 300:
            results["warnings"].append(f"Low player count in stats: {len(rows)}")

        # Check for missing data
        if not _check_missing_data(
            rows,
            cast(List[str], results["warnings"]),
            cast(List[str], results["errors"]),
            cast(Dict[str, Any], results["statistics"]),
        ):
            results["valid"] = False

        # Validate statistical ranges
        _validate_stat_ranges(
            headers,
            rows,
            cast(List[str], results["warnings"]),
            cast(Dict[str, Any], results["statistics"]),
        )

    except (KeyError, IndexError) as e:
        results["valid"] = False
        results["errors"].append(f"Invalid stats structure: {e}")

    return results


def validate_teams_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate teams data.

    Args:
        data: Teams data

    Returns:
        Validation results
    """
    results = {"data_type": "teams", "valid": True, "errors": [], "warnings": [], "statistics": {}}

    if "teams" not in data:
        results["valid"] = False
        results["errors"].append("Missing 'teams' key in data")
        return results

    teams = data["teams"]
    results["statistics"]["total_teams"] = len(teams)

    # NBA should have exactly 30 teams
    if len(teams) != 30:
        results["warnings"].append(f"Unexpected team count: {len(teams)} (expected 30)")

    # Validate each team
    invalid_teams = []
    for team in teams:
        is_valid, errors = validate_json_schema(team, TEAM_SCHEMA)
        if not is_valid:
            invalid_teams.append(f"Team {team.get('full_name', 'Unknown')}: {errors}")

    if invalid_teams:
        results["valid"] = False
        results["errors"].extend(invalid_teams)

    # Check for duplicate team IDs
    team_ids = [t.get("id") for t in teams if t.get("id") is not None]
    unique_ids = set(team_ids)
    if len(team_ids) != len(unique_ids):
        duplicates = len(team_ids) - len(unique_ids)
        results["valid"] = False
        results["errors"].append(f"Found {duplicates} duplicate team IDs")

    # Check for duplicate team abbreviations
    team_abbrevs = [t.get("abbreviation") for t in teams if t.get("abbreviation") is not None]
    unique_abbrevs = set(team_abbrevs)
    if len(team_abbrevs) != len(unique_abbrevs):
        duplicates = len(team_abbrevs) - len(unique_abbrevs)
        results["valid"] = False
        results["errors"].append(f"Found {duplicates} duplicate team abbreviations")

    return results


def validate_salary_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate salary data.

    Args:
        data: Salary data

    Returns:
        Validation results
    """
    results = {
        "data_type": "salaries",
        "valid": True,
        "errors": [],
        "warnings": [],
        "statistics": {},
    }

    # Schema validation
    is_valid, errors = validate_json_schema(data, SALARY_SCHEMA)
    if not is_valid:
        results["valid"] = False
        results["errors"].extend(errors)
        return results

    # Validate timestamp is not in the future
    fetch_timestamp = data.get("fetch_timestamp", "")
    if fetch_timestamp:
        try:
            fetch_dt = datetime.fromisoformat(fetch_timestamp.replace("Z", "+00:00"))
            if fetch_dt > datetime.utcnow():
                results["warnings"].append("Fetch timestamp is in the future")
        except ValueError:
            pass  # Already validated by schema

    salaries = data.get("salaries", [])
    results["statistics"]["total_salaries"] = len(salaries)

    if len(salaries) == 0 and data.get("source") != "placeholder":
        results["warnings"].append("No salary data found")

    # Validate salary ranges
    if salaries:
        salary_values = [s["annual_salary"] for s in salaries if "annual_salary" in s]
        if salary_values:
            results["statistics"]["min_salary"] = min(salary_values)
            results["statistics"]["max_salary"] = max(salary_values)
            results["statistics"]["avg_salary"] = sum(salary_values) / len(salary_values)

            # Check for unrealistic salaries
            if results["statistics"]["max_salary"] > 80_000_000:  # $80M
                results["warnings"].append(
                    f"Unusually high salary found: ${results['statistics']['max_salary']:,.0f}"
                )

            if results["statistics"]["min_salary"] < 500_000:  # $500K
                results["warnings"].append(
                    f"Below minimum salary found: ${results['statistics']['min_salary']:,.0f}"
                )

            # League-wide salary sanity checks
            # Note: Per-team cap validation requires team assignment data
            # Cap: $154M, Luxury: $188M, First apron: $195M, Second apron: $207M, Min: $139M
            total_salaries = sum(salary_values)
            results["statistics"]["total_salaries"] = total_salaries

            # With 30 teams, expect total between $3.5B (below floor with leniency) and
            # $7B (above second apron with leniency)
            if total_salaries < 3_500_000_000:  # $3.5B
                results["warnings"].append(
                    f"Total league salaries unusually low: ${total_salaries:,.0f} "
                    f"(expected >$3.5B for 30 teams)"
                )
            elif total_salaries > 7_000_000_000:  # $7B
                results["warnings"].append(
                    f"Total league salaries unusually high: ${total_salaries:,.0f} "
                    f"(expected <$7B for 30 teams)"
                )

    # Check for duplicate salary entries (same player name + season)
    if salaries:
        salary_keys = [
            (s.get("player_name"), s.get("season"))
            for s in salaries
            if s.get("player_name") is not None and s.get("season") is not None
        ]
        unique_keys = set(salary_keys)
        if len(salary_keys) != len(unique_keys):
            duplicates = len(salary_keys) - len(unique_keys)
            results["valid"] = False
            results["errors"].append(
                f"Found {duplicates} duplicate salary entries for same player/season"
            )

        # Validate contract years (placeholder for future data sources)
        # Note: contract_years field added during transform, not present in raw ESPN data
        invalid_contract_years = [
            s.get("player_name", "Unknown")
            for s in salaries
            if s.get("contract_years") is not None
            and (s.get("contract_years") < 1 or s.get("contract_years") > 5)
        ]
        if invalid_contract_years:
            results["warnings"].append(
                f"Found {len(invalid_contract_years)} players with unusual contract years "
                f"(expected 1-5 years)"
            )

        # Validate season years
        current_year = datetime.utcnow().year
        invalid_seasons = []
        for s in salaries:
            season = s.get("season", "")
            if season:
                try:
                    # Try to parse year from season string (e.g., "2024-25" or "2024")
                    if "-" in season:
                        start_year = int(season.split("-")[0])
                    else:
                        start_year = int(season)

                    if start_year < 1946 or start_year > current_year + 1:
                        invalid_seasons.append(s.get("player_name", "Unknown"))
                except ValueError:
                    # Non-numeric season format
                    invalid_seasons.append(s.get("player_name", "Unknown"))

        if invalid_seasons:
            results["warnings"].append(
                f"Found {len(invalid_seasons)} players with invalid season years "
                f"(expected 1946-{current_year + 1})"
            )

    return results


def handler(event, context):
    """
    Lambda handler for validating NBA data.

    This function validates data fetched by the fetch_data Lambda:
    1. Loads data from S3
    2. Validates against JSON schemas
    3. Performs data quality checks
    4. Generates validation report
    """
    logger.info("Starting data validation Lambda")
    logger.info(f"Event: {json.dumps(event)}")

    # Validate required environment variables
    if not S3_BUCKET:
        logger.error("DATA_BUCKET environment variable is not set")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "DATA_BUCKET environment variable is required"}),
        }
    if not ENVIRONMENT:
        logger.error("ENVIRONMENT environment variable is not set")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "ENVIRONMENT environment variable is required"}),
        }

    # Get data location from previous step
    if "data_location" not in event:
        return {"statusCode": 400, "body": json.dumps({"error": "Missing data_location in event"})}

    partition = event["data_location"]["partition"]
    fetch_type = event.get("fetch_type", "stats_only")

    validation_results = {
        "timestamp": datetime.utcnow().isoformat(),
        "environment": ENVIRONMENT,
        "partition": partition,
        "fetch_type": fetch_type,
        "validations": [],
        "overall_valid": True,
        "error_count": 0,
        "warning_count": 0,
    }

    # Define files to validate based on fetch type
    # stats_only (daily): Only player_stats
    # monthly: active_players, player_stats, teams, salaries
    # full: All of the above (game_logs validation TBD)
    all_files = {
        "stats": (f"raw/stats/{partition}/league_player_stats.json", validate_stats_data, True),
        "players": (
            f"raw/players/{partition}/active_players.json",
            validate_players_data,
            fetch_type in ["monthly", "full"],
        ),
        "teams": (
            f"raw/teams/{partition}/nba_teams.json",
            validate_teams_data,
            fetch_type in ["monthly", "full"],
        ),
        "salaries": (
            f"raw/salaries/{partition}/player_salaries.json",
            validate_salary_data,
            fetch_type in ["monthly", "full"],
        ),
    }

    files_to_validate = [
        (s3_key, validator_func, is_required)
        for s3_key, validator_func, is_required in all_files.values()
    ]

    for s3_key, validator_func, is_required in files_to_validate:
        logger.info(f"Validating {s3_key} (required={is_required})")

        # Load data from S3
        data = load_from_s3(s3_key)
        if data is None:
            data_type = s3_key.split("/")[1]  # Extract data type from path

            if is_required:
                # File is missing or invalid - this is a critical error
                logger.error(f"CRITICAL: Required file {s3_key} not found or invalid")
                validation_results["overall_valid"] = False
                validation_results["error_count"] += 1
                validation_results["validations"].append(
                    {
                        "data_type": data_type,
                        "valid": False,
                        "errors": [f"Required file not found or could not be loaded: {s3_key}"],
                        "warnings": [],
                        "statistics": {},
                        "s3_key": s3_key,
                    }
                )
            else:
                # Optional file - just log and skip
                logger.info(
                    f"Optional file {s3_key} not found - skipping (fetch_type={fetch_type})"
                )
            continue

        # Validate data
        result = validator_func(data)
        result["s3_key"] = s3_key
        validation_results["validations"].append(result)

        # Update overall status
        if not result["valid"]:
            validation_results["overall_valid"] = False
            validation_results["error_count"] += len(result["errors"])

        validation_results["warning_count"] += len(result["warnings"])

        # Log results
        if result["valid"]:
            logger.info(f"✓ {result['data_type']}: Valid with {len(result['warnings'])} warnings")
        else:
            logger.error(f"✗ {result['data_type']}: Invalid with {len(result['errors'])} errors")

    # Save validation report
    report_key = f"validation/{partition}/validation_report.json"
    save_validation_report(validation_results, report_key)

    # Determine status code based on validation results
    status_code = 200 if validation_results["overall_valid"] else 422

    # Return results for Step Functions
    return {
        "statusCode": status_code,
        "body": json.dumps(
            {
                "message": (
                    "Validation complete"
                    if validation_results["overall_valid"]
                    else "Validation failed"
                ),
                "valid": validation_results["overall_valid"],
                "error_count": validation_results["error_count"],
                "warning_count": validation_results["warning_count"],
            }
        ),
        "validation_report": {"bucket": S3_BUCKET, "key": report_key},
        "data_location": event["data_location"],  # Pass through for next step
        "validation_passed": validation_results["overall_valid"],
    }


# For local testing
if __name__ == "__main__":
    # Test event
    test_event = {
        "data_location": {
            "bucket": "dev-nba-cap-optimizer-data",
            "partition": "year=2024/month=02/day=17",
        }
    }

    # Mock context
    class Context:
        function_name = "validate_data"
        aws_request_id = "test-456"

    result = handler(test_event, Context())
    print(json.dumps(result, indent=2))
