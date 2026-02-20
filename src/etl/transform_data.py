"""
Transform and normalize NBA data.
This Lambda function enriches raw data with calculated fields, joins datasets,
and prepares data for analysis.
"""

import json
import logging
import math
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, cast

import boto3
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client
s3_client = boto3.client("s3")

# Environment variables
S3_BUCKET = os.environ.get("DATA_BUCKET")
ENVIRONMENT = os.environ.get("ENVIRONMENT")


def normalize_team_abbreviation(team_abbrev: str) -> str:
    """
    Normalize Basketball Reference team abbreviations to match NBA API.

    Args:
        team_abbrev: Team abbreviation from Basketball Reference

    Returns:
        Normalized team abbreviation matching NBA API format
    """
    mapping = {
        "BRK": "BKN",  # Brooklyn Nets
        "CHO": "CHA",  # Charlotte Hornets
        "PHO": "PHX",  # Phoenix Suns
    }
    return mapping.get(team_abbrev, team_abbrev)


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
        logger.error(f"Failed to load from S3 {s3_key}: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in S3 object {s3_key}: {e}")
        return None


def save_to_s3(data: Dict[str, Any], s3_key: str) -> bool:
    """
    Save transformed data to S3 as JSON.

    Args:
        data: Data to save
        s3_key: S3 object key

    Returns:
        Success status
    """
    try:
        json_str = json.dumps(data, default=str, indent=2)
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json_str.encode("utf-8"),
            ContentType="application/json; charset=utf-8",
        )
        logger.info(f"Successfully saved data to s3://{S3_BUCKET}/{s3_key}")
        return True
    except ClientError as e:
        logger.error(f"Failed to save to S3: {e}")
        return False


def match_salaries_with_players(
    salaries: List[Dict[str, Any]], active_players: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Match salary data with player IDs using normalized name matching.

    Args:
        salaries: List of salary dictionaries (from ESPN - has player_name)
        active_players: List of player dictionaries (from NBA API - has id and full_name)

    Returns:
        Salaries enriched with player_id field
    """
    logger.info("Matching salaries with player IDs...")

    # Create a lookup dictionary: normalized_name -> player_id
    player_lookup = {}
    for player in active_players:
        full_name = player.get("full_name", "")
        player_id = player.get("id")

        if full_name and player_id:
            # Normalize: lowercase, remove extra whitespace
            normalized_name = " ".join(full_name.lower().split())
            player_lookup[normalized_name] = player_id

    # Match salaries
    matched = 0
    for salary in salaries:
        player_name = salary.get("player_name", "")
        normalized = " ".join(player_name.lower().split())

        if normalized in player_lookup:
            salary["player_id"] = player_lookup[normalized]
            matched += 1
        else:
            salary["player_id"] = None

    match_rate = (matched / len(salaries) * 100) if salaries else 0
    logger.info(f"Matched: {matched}/{len(salaries)} ({match_rate:.1f}%)")

    return salaries


def enrich_player_stats(stats_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Enrich player statistics by merging per-game and advanced stats from Basketball Reference.

    Args:
        stats_data: Basketball Reference data with per_game_stats and advanced_stats

    Returns:
        List of enriched player stat dictionaries
    """
    logger.info("Enriching player statistics from Basketball Reference...")

    try:
        per_game_stats = stats_data.get("per_game_stats", [])
        advanced_stats = stats_data.get("advanced_stats", [])

        if not per_game_stats:
            logger.warning("No per-game stats found")
            return []

        # Create advanced stats lookup by player name
        advanced_lookup = {}
        for adv_stat in advanced_stats:
            if not isinstance(adv_stat, dict):
                continue
            player_name = adv_stat.get("Player", "")
            if player_name:
                normalized_name = " ".join(player_name.lower().split())
                advanced_lookup[normalized_name] = adv_stat

        enriched_stats = []

        for pg_stat in per_game_stats:
            if not isinstance(pg_stat, dict):
                continue

            player_name = pg_stat.get("Player", "")
            if not player_name:
                continue

            # Skip summary rows (League Average, etc.)
            if player_name in ["League Average"]:
                logger.debug(f"Skipping summary row: {player_name}")
                continue

            # Create enriched player record
            team_abbrev = pg_stat.get("Team")
            player_stat = {
                # Basic info
                "player_name": player_name,
                "age": pg_stat.get("Age"),
                "team_abbreviation": (
                    normalize_team_abbreviation(team_abbrev) if team_abbrev else None
                ),
                "position": pg_stat.get("Pos"),
                "games_played": pg_stat.get("G"),
                "games_started": pg_stat.get("GS"),
                "minutes": pg_stat.get("MP"),
                # Scoring
                "points": pg_stat.get("PTS"),
                "fgm": pg_stat.get("FG"),
                "fga": pg_stat.get("FGA"),
                "fg_pct": pg_stat.get("FG%"),
                "fg3m": pg_stat.get("3P"),
                "fg3a": pg_stat.get("3PA"),
                "fg3_pct": pg_stat.get("3P%"),
                "fg2m": pg_stat.get("2P"),
                "fg2a": pg_stat.get("2PA"),
                "fg2_pct": pg_stat.get("2P%"),
                "ftm": pg_stat.get("FT"),
                "fta": pg_stat.get("FTA"),
                "ft_pct": pg_stat.get("FT%"),
                # Rebounds
                "oreb": pg_stat.get("ORB"),
                "dreb": pg_stat.get("DRB"),
                "rebounds": pg_stat.get("TRB"),
                # Other
                "assists": pg_stat.get("AST"),
                "steals": pg_stat.get("STL"),
                "blocks": pg_stat.get("BLK"),
                "turnovers": pg_stat.get("TOV"),
                "fouls": pg_stat.get("PF"),
            }

            # Match with advanced stats
            normalized_name = " ".join(player_name.lower().split())
            if normalized_name in advanced_lookup:
                adv_stat = advanced_lookup[normalized_name]

                # Add all advanced stats from Basketball Reference
                player_stat["per"] = adv_stat.get("PER")
                player_stat["ts_pct"] = adv_stat.get("TS%")
                player_stat["efg_pct"] = adv_stat.get("eFG%")  # B-R has this already
                player_stat["usg_pct"] = adv_stat.get("USG%")
                player_stat["ws"] = adv_stat.get("WS")
                player_stat["ws_per_48"] = adv_stat.get("WS/48")
                player_stat["bpm"] = adv_stat.get("BPM")
                player_stat["obpm"] = adv_stat.get("OBPM")
                player_stat["dbpm"] = adv_stat.get("DBPM")
                player_stat["vorp"] = adv_stat.get("VORP")
                player_stat["orb_pct"] = adv_stat.get("ORB%")
                player_stat["drb_pct"] = adv_stat.get("DRB%")
                player_stat["trb_pct"] = adv_stat.get("TRB%")
                player_stat["ast_pct"] = adv_stat.get("AST%")
                player_stat["stl_pct"] = adv_stat.get("STL%")
                player_stat["blk_pct"] = adv_stat.get("BLK%")
                player_stat["tov_pct"] = adv_stat.get("TOV%")
                player_stat["ows"] = adv_stat.get("OWS")
                player_stat["dws"] = adv_stat.get("DWS")
            else:
                # Set to None if no advanced stats match
                player_stat["per"] = None
                player_stat["ts_pct"] = None
                player_stat["efg_pct"] = None
                player_stat["usg_pct"] = None
                player_stat["ws"] = None
                player_stat["ws_per_48"] = None
                player_stat["bpm"] = None
                player_stat["obpm"] = None
                player_stat["dbpm"] = None
                player_stat["vorp"] = None
                player_stat["orb_pct"] = None
                player_stat["drb_pct"] = None
                player_stat["trb_pct"] = None
                player_stat["ast_pct"] = None
                player_stat["stl_pct"] = None
                player_stat["blk_pct"] = None
                player_stat["tov_pct"] = None
                player_stat["ows"] = None
                player_stat["dws"] = None

            enriched_stats.append(player_stat)

        logger.info(f"Enriched {len(enriched_stats)} player stat records")
        return enriched_stats

    except (KeyError, IndexError) as e:
        logger.error(f"Error enriching player stats: {e}")
        return []


def enrich_team_data(
    teams: List[Dict[str, Any]],
    enriched_salaries: List[Dict[str, Any]],
    enriched_stats: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Enrich team data with aggregated salary and performance metrics.

    Args:
        teams: List of team dictionaries
        enriched_salaries: List of salary dictionaries with player_id
        enriched_stats: List of player stat dictionaries

    Returns:
        List of enriched team dictionaries
    """
    logger.info("Enriching team data with aggregations...")

    # Create salary lookup by player_name (since B-R uses names not IDs)
    salary_by_player = {}
    for sal in enriched_salaries:
        player_name = sal.get("player_name")
        if player_name:
            normalized_name = " ".join(player_name.lower().split())
            salary_by_player[normalized_name] = sal["annual_salary"]

    enriched_teams = []

    for team in teams:
        team_abbrev = team.get("abbreviation")

        # Find all players on this team
        team_players = [
            stat for stat in enriched_stats if stat.get("team_abbreviation") == team_abbrev
        ]

        # Calculate salary metrics
        team_salaries = []
        for player in team_players:
            player_name = player.get("player_name", "")
            normalized_name = " ".join(player_name.lower().split())
            if normalized_name in salary_by_player:
                team_salaries.append(salary_by_player[normalized_name])

        # Find team stats (players with stats on this team)
        team_player_stats = [
            stat for stat in enriched_stats if stat.get("team_abbreviation") == team_abbrev
        ]

        # Calculate aggregations
        enriched_team = {
            **team,  # Include all original team fields
            # Salary metrics
            "total_payroll": sum(team_salaries) if team_salaries else 0,
            "roster_count": len(team_players),
            "roster_with_salary": len(team_salaries),
            "avg_salary": (
                round(sum(team_salaries) / len(team_salaries), 2) if team_salaries else 0
            ),
            "min_salary": min(team_salaries) if team_salaries else 0,
            "max_salary": max(team_salaries) if team_salaries else 0,
        }

        # Find top paid player
        if team_salaries:
            max_salary = max(team_salaries)
            # Find player name with max salary
            for player in team_players:
                player_name = player.get("player_name", "")
                normalized_name = " ".join(player_name.lower().split())
                if salary_by_player.get(normalized_name) == max_salary:
                    enriched_team["top_paid_player"] = player_name
                    enriched_team["top_paid_salary"] = max_salary
                    break

        # Calculate team performance metrics (from player stats)
        if team_player_stats:
            enriched_team["total_players_with_stats"] = len(team_player_stats)

            # Sum up team totals
            total_points = sum(float(s.get("points", 0) or 0) for s in team_player_stats)
            total_rebounds = sum(float(s.get("rebounds", 0) or 0) for s in team_player_stats)
            total_assists = sum(float(s.get("assists", 0) or 0) for s in team_player_stats)

            enriched_team["team_total_points"] = round(total_points, 1)
            enriched_team["team_total_rebounds"] = round(total_rebounds, 1)
            enriched_team["team_total_assists"] = round(total_assists, 1)

            # Calculate averages
            enriched_team["avg_player_points"] = round(total_points / len(team_player_stats), 2)
            enriched_team["avg_player_rebounds"] = round(total_rebounds / len(team_player_stats), 2)
            enriched_team["avg_player_assists"] = round(total_assists / len(team_player_stats), 2)

        enriched_teams.append(enriched_team)

    logger.info(f"Enriched {len(enriched_teams)} teams with aggregated data")
    return enriched_teams


def handler(event, context):
    """
    Lambda handler for transforming NBA data.

    This function receives validated data location from validate_data,
    loads raw data from S3, enriches it with calculated fields,
    and saves transformed data back to S3.

    Expected event structure from validate_data:
    {
        "validation_passed": true/false,
        "data_location": {"bucket": "...", "partition": "year=2024/month=02/day=17"},
        ...
    }
    """
    logger.info("Starting data transformation Lambda")
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

    # Check if validation passed
    validation_passed = event.get("validation_passed", False)
    if not validation_passed:
        logger.error("Data validation failed in previous step. Skipping transformation.")
        return {
            "statusCode": 400,
            "body": json.dumps(
                {
                    "error": "Data validation failed",
                    "message": "Transformation skipped due to validation failure",
                }
            ),
        }

    # Get data location from previous step
    if "data_location" not in event:
        logger.error("Missing data_location in event")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing data_location in event"}),
        }

    partition = event["data_location"]["partition"]
    logger.info(f"Processing data from partition: {partition}")

    results = {
        "statusCode": 200,
        "transformed": [],
        "errors": [],
        "statistics": {},
    }

    try:
        # 1. Load raw data from S3
        logger.info("Loading raw data from S3...")

        players_data = load_from_s3(f"raw/players/{partition}/active_players.json")
        stats_data = load_from_s3(f"raw/stats/{partition}/league_player_stats.json")
        salary_data = load_from_s3(f"raw/salaries/{partition}/player_salaries.json")
        teams_data = load_from_s3(f"raw/teams/{partition}/nba_teams.json")

        # Check if critical data was loaded
        if not stats_data:
            results["errors"].append("Failed to load player stats data")
            results["statusCode"] = 500
            return {
                "statusCode": 500,
                "body": json.dumps(results),
            }

        # 2. Enrich salary data with player IDs (if both datasets available)
        enriched_salaries = []
        if salary_data and players_data and salary_data.get("salaries"):
            logger.info("Enriching salary data with player IDs...")
            enriched_salaries = match_salaries_with_players(
                salary_data["salaries"], players_data.get("players", [])
            )

            # Calculate salary statistics
            matched_count = sum(1 for s in enriched_salaries if s.get("player_id") is not None)
            total_count = len(enriched_salaries)
            match_rate = (matched_count / total_count * 100) if total_count > 0 else 0

            results["statistics"]["salary_match_rate"] = round(match_rate, 2)
            results["statistics"]["total_salaries"] = total_count
            results["statistics"]["matched_salaries"] = matched_count
            results["statistics"]["unmatched_salaries"] = total_count - matched_count

            # Calculate salary aggregations
            salary_values = [s["annual_salary"] for s in enriched_salaries]
            if salary_values:
                results["statistics"]["avg_salary"] = round(
                    sum(salary_values) / len(salary_values), 2
                )
                results["statistics"]["min_salary"] = min(salary_values)
                results["statistics"]["max_salary"] = max(salary_values)
                results["statistics"]["total_salary_cap"] = sum(salary_values)

            # Save enriched salaries
            transformed_salary_data = {
                "transform_timestamp": datetime.utcnow().isoformat(),
                "source": salary_data.get("source", "unknown"),
                "season": (
                    salary_data.get("salaries", [{}])[0].get("season", "unknown")
                    if enriched_salaries
                    else "unknown"
                ),
                "statistics": {
                    "total_salaries": total_count,
                    "matched_salaries": matched_count,
                    "match_rate": round(match_rate, 2),
                },
                "salaries": enriched_salaries,
            }

            s3_key = f"transformed/salaries/{partition}/enriched_salaries.json"
            if save_to_s3(transformed_salary_data, s3_key):
                results["transformed"].append("enriched_salaries")
                logger.info(f"Saved enriched salaries: {matched_count}/{total_count} matched")
            else:
                results["errors"].append("Failed to save enriched salaries")
        else:
            logger.warning("Skipping salary enrichment - missing salary or player data")

        # 3. Enrich player stats from Basketball Reference
        logger.info("Enriching player statistics...")
        enriched_stats = enrich_player_stats(stats_data)

        if enriched_stats:
            results["statistics"]["total_player_stats"] = len(enriched_stats)

            # Calculate stats aggregations
            players_with_stats = [p for p in enriched_stats if p.get("points") is not None]
            if players_with_stats:
                avg_points = sum(float(p["points"] or 0) for p in players_with_stats) / len(
                    players_with_stats
                )
                avg_rebounds = sum(float(p["rebounds"] or 0) for p in players_with_stats) / len(
                    players_with_stats
                )
                avg_assists = sum(float(p["assists"] or 0) for p in players_with_stats) / len(
                    players_with_stats
                )

                results["statistics"]["avg_points_per_game"] = round(avg_points, 2)
                results["statistics"]["avg_rebounds_per_game"] = round(avg_rebounds, 2)
                results["statistics"]["avg_assists_per_game"] = round(avg_assists, 2)

            # Save enriched stats
            transformed_stats_data = {
                "transform_timestamp": datetime.utcnow().isoformat(),
                "season": stats_data.get("season", "unknown"),
                "source": stats_data.get("source", "basketball_reference"),
                "statistics": {
                    "total_players": len(enriched_stats),
                },
                "player_stats": enriched_stats,
            }

            s3_key = f"transformed/stats/{partition}/enriched_player_stats.json"
            if save_to_s3(transformed_stats_data, s3_key):
                results["transformed"].append("enriched_player_stats")
                logger.info(f"Saved enriched stats for {len(enriched_stats)} players")
            else:
                results["errors"].append("Failed to save enriched stats")
        else:
            results["errors"].append("Failed to enrich player stats")

        # 4. Enrich and save team data with aggregations
        if teams_data and teams_data.get("teams"):
            logger.info("Enriching team data with salary and performance aggregations...")

            # Enrich teams with salary and stats aggregations
            enriched_teams = enrich_team_data(
                teams_data["teams"], enriched_salaries, enriched_stats
            )

            # Calculate league-wide team statistics
            team_payrolls = [t.get("total_payroll", 0) for t in enriched_teams]
            results["statistics"]["league_total_payroll"] = sum(team_payrolls)
            results["statistics"]["avg_team_payroll"] = (
                round(sum(team_payrolls) / len(team_payrolls), 2) if team_payrolls else 0
            )
            results["statistics"]["min_team_payroll"] = min(team_payrolls) if team_payrolls else 0
            results["statistics"]["max_team_payroll"] = max(team_payrolls) if team_payrolls else 0

            # Save enriched teams
            s3_key = f"transformed/teams/{partition}/enriched_teams.json"
            transformed_teams_data = {
                "transform_timestamp": datetime.utcnow().isoformat(),
                "statistics": {
                    "total_teams": len(enriched_teams),
                    "league_total_payroll": results["statistics"].get("league_total_payroll", 0),
                },
                "teams": enriched_teams,
            }
            if save_to_s3(transformed_teams_data, s3_key):
                results["transformed"].append("enriched_teams")
                logger.info(f"Saved {len(enriched_teams)} enriched teams")

        # Generate summary
        results["summary"] = {
            "timestamp": datetime.utcnow().isoformat(),
            "environment": ENVIRONMENT,
            "partition": partition,
            "successful_transforms": len(results["transformed"]),
            "errors_count": len(results["errors"]),
        }

        logger.info(f"Data transformation completed: {results['summary']}")

        # Validate that no NaN values exist in statistics
        def contains_nan(obj):
            """Recursively check if object contains NaN values."""
            if isinstance(obj, dict):
                return any(contains_nan(v) for v in obj.values())
            elif isinstance(obj, list):
                return any(contains_nan(item) for item in obj)
            elif isinstance(obj, float):
                return math.isnan(obj)
            return False

        if contains_nan(results["statistics"]):
            logger.error("NaN values detected in statistics output")
            return {
                "statusCode": 400,
                "body": json.dumps(
                    {
                        "error": "Invalid statistics",
                        "message": "NaN values detected in transformation output",
                        "statistics": results["statistics"],
                    }
                ),
            }

        # Return results for Step Functions
        return {
            "statusCode": 200,
            "body": json.dumps(results),
            "data_location": event["data_location"],  # Pass through for next step
            "transformation_successful": len(results["errors"]) == 0,
            "statistics": results["statistics"],
        }

    except Exception as e:
        logger.error(f"Unexpected error in data transformation: {e}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e), "message": "Data transformation failed"}),
        }
