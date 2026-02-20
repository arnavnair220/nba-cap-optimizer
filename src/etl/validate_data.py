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
    "required": [
        "season",
        "fetch_timestamp",
        "source",
        "per_game_stats",
        "advanced_stats",
        "per_game_columns",
        "advanced_columns",
    ],
    "properties": {
        "season": {"type": "string", "pattern": "^\\d{4}-\\d{2}$"},
        "fetch_timestamp": {"type": "string", "format": "date-time"},
        "source": {"type": "string", "enum": ["basketball_reference"]},
        "per_game_stats": {"type": "array"},
        "advanced_stats": {"type": "array"},
        "per_game_columns": {"type": "array"},
        "advanced_columns": {"type": "array"},
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
    stats: List[Dict[str, Any]],
    warnings: List[str],
    errors: List[str],
    statistics: Dict[str, Any],
) -> bool:
    """
    Check for missing values in key columns (Basketball Reference format).

    Args:
        stats: List of player stat dictionaries
        warnings: List to append warnings to
        errors: List to append errors to
        statistics: Statistics dict to update

    Returns:
        True if validation passes, False if critical error
    """
    if not stats:
        return True

    # Key columns to check for missing data (common across per_game and advanced stats)
    key_columns = ["Player", "Pos", "Age", "Team", "G", "MP"]

    missing_stats = 0
    for player_stat in stats:
        # Check if any key column is missing or empty
        if any(
            player_stat.get(col) is None or player_stat.get(col) == ""
            for col in key_columns
            if col in player_stat
        ):
            missing_stats += 1

    statistics["players_with_missing_data"] = missing_stats

    if missing_stats > 0:
        warnings.append(
            f"Found {missing_stats}/{len(stats)} players with missing data in key columns"
        )

    if missing_stats > len(stats) * 0.05:  # More than 5% with missing data
        errors.append(
            f"CRITICAL: High missing data rate: {missing_stats}/{len(stats)} players "
            f"({missing_stats/len(stats)*100:.1f}%) exceeds 5% threshold"
        )
        return False

    return True


def _is_value_zero_or_null(value: Any) -> bool:
    """
    Check if a value is null, empty, or zero.

    Args:
        value: Value to check

    Returns:
        True if value is null, empty, or zero
    """
    if value is None or value == "":
        return True
    try:
        return float(value) == 0
    except (ValueError, TypeError):
        return False


def _validate_percentage_null_logic(
    stats: List[Dict[str, Any]], errors: List[str], statistics: Dict[str, Any]
) -> None:
    """
    Validate that percentage columns are null only when corresponding attempt columns are zero.

    For example, 3P% can only be null if 3PA (3-point attempts) is 0 or null.

    Args:
        stats: List of player stat dictionaries
        errors: List to append errors to
        statistics: Statistics dict to update
    """
    if not stats:
        return

    # Define percentage -> attempt column mappings
    # Percentage columns can ONLY be null if their attempt/dependency column(s) are 0, null, or empty
    # Format: {percentage_column: (dependency_columns, logic)}
    # logic: "any" means any dependency can be non-zero to require percentage
    #        "all" means all dependencies must be zero to allow null
    percentage_dependencies = {
        # Per-game stats
        "FG%": (["FGA"], "any"),  # Field goal % requires field goal attempts
        "3P%": (["3PA"], "any"),  # 3-point % requires 3-point attempts
        "2P%": (["2PA"], "any"),  # 2-point % requires 2-point attempts
        "FT%": (["FTA"], "any"),  # Free throw % requires free throw attempts
        "eFG%": (["FGA"], "any"),  # Effective FG% requires field goal attempts
        # Advanced stats
        "TS%": (
            ["FGA", "FTA"],
            "all",
        ),  # True shooting % requires any shooting attempts (FGA or FTA)
        "3PAr": (["FGA"], "any"),  # 3-point attempt rate requires FGA
        "FTr": (["FGA"], "any"),  # Free throw rate requires FGA
    }

    invalid_nulls = []
    for player_idx, player_stat in enumerate(stats):
        player_name = player_stat.get("Player", f"Player {player_idx}")

        for pct_col, (dep_cols, logic) in percentage_dependencies.items():
            # Only check if percentage column exists in the data
            if pct_col not in player_stat:
                continue

            pct_value = player_stat.get(pct_col)
            pct_is_null = pct_value is None or pct_value == ""

            # If percentage is not null, no need to check dependencies
            if not pct_is_null:
                continue

            # Check dependency columns
            dep_values = []
            all_deps_exist = True
            for dep_col in dep_cols:
                if dep_col not in player_stat:
                    all_deps_exist = False
                    break
                dep_values.append(player_stat.get(dep_col))

            # Skip if not all dependency columns exist
            if not all_deps_exist:
                continue

            # Check if dependencies allow null based on logic
            dep_is_zero_or_null = [_is_value_zero_or_null(v) for v in dep_values]

            if logic == "any":
                # If ANY dependency is non-zero, percentage cannot be null
                if not all(dep_is_zero_or_null):
                    non_zero_deps = [
                        f"{dep_cols[i]}={dep_values[i]}"
                        for i in range(len(dep_cols))
                        if not dep_is_zero_or_null[i]
                    ]
                    invalid_nulls.append(
                        f"{player_name}: {pct_col} is null but {', '.join(non_zero_deps)}"
                    )
            elif logic == "all":
                # If ALL dependencies are zero/null, percentage can be null
                # If ANY dependency is non-zero, percentage cannot be null
                if not all(dep_is_zero_or_null):
                    non_zero_deps = [
                        f"{dep_cols[i]}={dep_values[i]}"
                        for i in range(len(dep_cols))
                        if not dep_is_zero_or_null[i]
                    ]
                    invalid_nulls.append(
                        f"{player_name}: {pct_col} is null but {', '.join(non_zero_deps)}"
                    )

    if invalid_nulls:
        errors.append(
            f"Found {len(invalid_nulls)} players with invalid null percentages "
            f"(first few: {', '.join(invalid_nulls[:5])})"
        )
        statistics["invalid_null_percentages"] = len(invalid_nulls)


def _validate_stat_ranges(
    stats: List[Dict[str, Any]], warnings: List[str], statistics: Dict[str, Any]
) -> None:
    """
    Validate statistical values are within realistic ranges (Basketball Reference format).

    Args:
        stats: List of player stat dictionaries
        warnings: List to append warnings to
        statistics: Statistics dict to update
    """
    if not stats:
        return

    # Basketball Reference per-game stat column checks
    # Note: MP (minutes per game) can exceed 48 with overtime
    stat_checks = {
        "PTS": (80, "points per game"),  # 0-80
        "TRB": (30, "total rebounds per game"),  # 0-30
        "AST": (25, "assists per game"),  # 0-25
        "MP": (60, "minutes per game"),  # 0-60 (accounts for OT)
        "STL": (10, "steals per game"),  # 0-10
        "BLK": (10, "blocks per game"),  # 0-10
        "TOV": (15, "turnovers per game"),  # 0-15
        "FG%": (1.0, "field goal percentage"),  # 0-1.0
        "3P%": (1.0, "three-point percentage"),  # 0-1.0
        "FT%": (1.0, "free throw percentage"),  # 0-1.0
    }

    unrealistic_values = []
    for player_idx, player_stat in enumerate(stats):
        player_name = player_stat.get("Player", f"Player {player_idx}")

        for stat_key, (max_val, stat_name) in stat_checks.items():
            if stat_key in player_stat:
                value = player_stat.get(stat_key)
                if value is not None and value != "":
                    try:
                        value_float = float(value)
                        if value_float < 0 or value_float > max_val:
                            unrealistic_values.append(
                                f"{player_name}: {stat_name} = {value_float} "
                                f"(expected 0-{max_val})"
                            )
                    except (ValueError, TypeError):
                        # Can't convert to float - skip this check
                        pass

    if unrealistic_values:
        warnings.append(
            f"Found {len(unrealistic_values)} unrealistic stat values "
            f"(first few: {', '.join(unrealistic_values[:3])})"
        )
        statistics["unrealistic_stat_values"] = len(unrealistic_values)


def validate_stats_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate player statistics data from Basketball Reference.

    Args:
        data: Stats data (Basketball Reference format with per_game_stats and advanced_stats)

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
        per_game_stats = data.get("per_game_stats", [])
        advanced_stats = data.get("advanced_stats", [])
        per_game_columns = data.get("per_game_columns", [])
        advanced_columns = data.get("advanced_columns", [])

        per_game_count = len(per_game_stats)
        advanced_count = len(advanced_stats)

        results["statistics"]["total_players_per_game"] = per_game_count
        results["statistics"]["total_players_advanced"] = advanced_count
        results["statistics"]["per_game_columns"] = len(per_game_columns)
        results["statistics"]["advanced_columns"] = len(advanced_columns)

        # Player count validation - both stat arrays should have adequate data
        min_expected_players = 300  # NBA typically has 450+ active players

        # Check per-game stats player count
        if per_game_count < min_expected_players:
            results["warnings"].append(
                f"Low player count in per-game stats: {per_game_count} "
                f"(expected {min_expected_players}+)"
            )

        # Check advanced stats player count
        if advanced_count < min_expected_players:
            results["warnings"].append(
                f"Low player count in advanced stats: {advanced_count} "
                f"(expected {min_expected_players}+)"
            )

        # Check that both stat lists have similar counts (within 2.5% of each other)
        # Both arrays should be scraped at the same time and contain the same players
        if per_game_stats and advanced_stats:
            count_diff = abs(per_game_count - advanced_count)
            max_count = max(per_game_count, advanced_count)
            count_diff_pct = count_diff / max_count if max_count > 0 else 0

            if count_diff_pct > 0.025:  # More than 2.5% difference
                results["warnings"].append(
                    f"Player count mismatch: {per_game_count} per-game vs "
                    f"{advanced_count} advanced ({count_diff_pct*100:.1f}% difference, "
                    f"expected within 2.5%)"
                )
                results["statistics"]["player_count_diff_pct"] = round(count_diff_pct * 100, 2)

        # Check for missing data in per-game stats
        if per_game_stats:
            if not _check_missing_data(
                per_game_stats,
                cast(List[str], results["warnings"]),
                cast(List[str], results["errors"]),
                cast(Dict[str, Any], results["statistics"]),
            ):
                results["valid"] = False

        # Validate percentage-attempt null logic (per-game stats)
        if per_game_stats:
            errors_before = len(cast(List[str], results["errors"]))
            _validate_percentage_null_logic(
                per_game_stats,
                cast(List[str], results["errors"]),
                cast(Dict[str, Any], results["statistics"]),
            )
            # Mark as invalid if percentage null errors were found
            if len(cast(List[str], results["errors"])) > errors_before:
                results["valid"] = False

        # Validate statistical ranges (per-game stats)
        if per_game_stats:
            _validate_stat_ranges(
                per_game_stats,
                cast(List[str], results["warnings"]),
                cast(Dict[str, Any], results["statistics"]),
            )

        # Validate percentage-attempt null logic (advanced stats)
        if advanced_stats:
            errors_before = len(cast(List[str], results["errors"]))
            _validate_percentage_null_logic(
                advanced_stats,
                cast(List[str], results["errors"]),
                cast(Dict[str, Any], results["statistics"]),
            )
            # Mark as invalid if percentage null errors were found
            if len(cast(List[str], results["errors"])) > errors_before:
                results["valid"] = False

        # Validate that we have the expected columns (only if we have data)
        if per_game_stats and per_game_columns:
            expected_per_game_cols = [
                "Player",
                "Pos",
                "Age",
                "Team",
                "G",
                "MP",
                "PTS",
                "TRB",
                "AST",
            ]
            missing_cols = [col for col in expected_per_game_cols if col not in per_game_columns]
            if missing_cols:
                results["errors"].append(
                    f"Missing expected per-game columns: {', '.join(missing_cols)}"
                )
                results["valid"] = False

        if advanced_stats and advanced_columns:
            expected_advanced_cols = ["Player", "Pos", "Age", "Team", "G", "MP", "PER"]
            missing_advanced_cols = [
                col for col in expected_advanced_cols if col not in advanced_columns
            ]
            if missing_advanced_cols:
                results["errors"].append(
                    f"Missing expected advanced columns: {', '.join(missing_advanced_cols)}"
                )
                results["valid"] = False

    except (KeyError, TypeError) as e:
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
