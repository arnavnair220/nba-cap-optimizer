"""
Fetch NBA data from multiple APIs and sources.
This Lambda function fetches player stats, game data, and team information
from the NBA Stats API and stores raw data in S3.

TODO: Add backfilling functionality to fetch historical data for previous seasons.
This will require:
- Adding optional 'season' parameters to all fetch functions (defaults to current season)
- Modifying the handler to support backfill requests with season ranges
- Ensuring S3 storage paths properly partition historical data
- Example: fetch_player_stats(season="2023-24") for historical data
"""

import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, cast

import boto3
import pandas as pd
import requests
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup

# Keep nba_api imports only for static data (players, teams)
# Stats fetching now uses Basketball-Reference.com scraping
from nba_api.stats.static import players, teams

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client
s3_client = boto3.client("s3")

# Environment variables (validated in handler to allow module imports for testing)
S3_BUCKET = os.environ.get("DATA_BUCKET")
ENVIRONMENT = os.environ.get("ENVIRONMENT")


def get_date_partition(date: datetime) -> str:
    """Generate S3 partition path based on date."""
    return f"year={date.year}/month={date.month:02d}/day={date.day:02d}"


def save_to_s3(data: Dict[str, Any], s3_key: str) -> bool:
    """
    Save data to S3 as JSON with proper Unicode handling.

    Args:
        data: Data to save
        s3_key: S3 object key

    Returns:
        Success status
    """
    try:
        # Use ensure_ascii=False to preserve Unicode characters instead of escape sequences
        # This will save characters like é, ć, č as actual Unicode characters
        json_str = json.dumps(data, default=str, ensure_ascii=False)

        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json_str.encode("utf-8"),  # Explicitly encode to UTF-8
            ContentType="application/json; charset=utf-8",  # Specify UTF-8 charset
        )
        logger.info(f"Successfully saved data to s3://{S3_BUCKET}/{s3_key}")
        return True
    except ClientError as e:
        logger.error(f"Failed to save to S3: {e}")
        return False


def fetch_active_players() -> List[Dict[str, Any]]:
    """
    Fetch all active NBA players.

    Returns raw player data from NBA API with original Unicode names preserved.
    Name normalization is handled in transform_data for matching purposes.

    Returns:
        List of player dictionaries with original Unicode names
    """
    logger.info("Fetching active players...")

    try:
        # Get all players from static data (preserves Unicode names)
        all_players = players.get_active_players()

        logger.info(f"Found {len(all_players)} active players")
        return cast(List[Dict[str, Any]], all_players)
    except Exception as e:
        logger.error(f"Failed to fetch players: {e}")
        return []


def fetch_player_stats(season: str = "2025-26", max_retries: int = 3) -> Optional[Dict[str, Any]]:
    """
    Fetch comprehensive player stats from Basketball-Reference.com with retry logic.

    This function scrapes both per-game and advanced stats from Basketball-Reference
    using pandas.read_html(), which bypasses anti-scraping measures.

    Args:
        season: NBA season (e.g., "2024-25", "2025-26")
        max_retries: Maximum number of retry attempts for transient failures

    Returns:
        Player stats data with both per-game and advanced metrics
    """
    logger.info(f"Fetching player stats from Basketball-Reference for season {season}...")

    # Convert season format: "2025-26" -> "2026" (use ending year for B-R URL)
    season_year = season.split("-")[1]
    if len(season_year) == 2:
        season_year = "20" + season_year

    for attempt in range(max_retries):
        try:
            # Add delay to respect rate limits (longer on retries)
            if attempt > 0:
                backoff_delay = 2**attempt  # Exponential backoff: 2s, 4s, 8s
                logger.info(
                    f"Retry attempt {attempt + 1}/{max_retries} after {backoff_delay}s delay"
                )
                time.sleep(backoff_delay)
            else:
                time.sleep(1)

            # Fetch per-game stats
            pergame_url = (
                f"https://www.basketball-reference.com/leagues/NBA_{season_year}_per_game.html"
            )
            logger.info(f"Fetching per-game stats from {pergame_url}")
            pergame_tables = pd.read_html(pergame_url)
            df_pergame = pergame_tables[0]

            # Remove header rows that sometimes appear in the middle of data
            df_pergame = df_pergame[df_pergame["Player"] != "Player"]

            # Add delay between requests to be respectful
            time.sleep(1)

            # Fetch advanced stats
            advanced_url = (
                f"https://www.basketball-reference.com/leagues/NBA_{season_year}_advanced.html"
            )
            logger.info(f"Fetching advanced stats from {advanced_url}")
            advanced_tables = pd.read_html(advanced_url)
            df_advanced = advanced_tables[0]

            # Remove header rows
            df_advanced = df_advanced[df_advanced["Player"] != "Player"]

            # Convert DataFrames to list of dictionaries
            pergame_records = df_pergame.to_dict("records")
            advanced_records = df_advanced.to_dict("records")

            data = {
                "season": season,
                "fetch_timestamp": datetime.utcnow().isoformat(),
                "source": "basketball_reference",
                "per_game_stats": pergame_records,
                "advanced_stats": advanced_records,
                "per_game_columns": list(df_pergame.columns),
                "advanced_columns": list(df_advanced.columns),
            }

            logger.info(
                f"Successfully fetched {len(pergame_records)} players (per-game) "
                f"and {len(advanced_records)} players (advanced)"
            )
            return data

        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt == max_retries - 1:
                logger.error(f"All {max_retries} attempts failed to fetch player stats: {e}")
                return None
            # Continue to next retry attempt

    return None


def fetch_player_game_logs(player_id: str, season: str = "2025-26") -> Optional[Dict[str, Any]]:
    """
    Fetch game logs for a specific player.

    NOTE: This function is currently disabled due to NBA Stats API blocking AWS IPs.
    Game logs are optional (only used in 'full' fetch mode).
    Future enhancement: Implement game log scraping from Basketball-Reference.

    Args:
        player_id: NBA player ID
        season: NBA season

    Returns:
        None (disabled)
    """
    logger.warning(
        "Game logs fetching is currently disabled due to NBA API blocking. "
        "This is an optional feature only used in 'full' fetch mode."
    )
    return None


def fetch_team_data() -> List[Dict[str, Any]]:
    """
    Fetch all NBA teams information.

    Returns:
        List of team dictionaries
    """
    logger.info("Fetching NBA teams...")

    try:
        all_teams = teams.get_teams()
        logger.info(f"Found {len(all_teams)} teams")
        return cast(List[Dict[str, Any]], all_teams)
    except Exception as e:
        logger.error(f"Failed to fetch teams: {e}")
        return []


def fetch_espn_salaries(season: str = "2025-26") -> List[Dict[str, Any]]:
    """
    Fetch player salary data from ESPN with pagination support.

    Args:
        season: NBA season (e.g., "2025-26")

    Returns:
        List of salary dictionaries
    """
    base_url = "https://www.espn.com/nba/salaries"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )
    }

    logger.info(f"Fetching ESPN salaries from {base_url}")

    all_salaries = []

    # ESPN uses _/page/X for pagination
    for page in range(1, 15):  # Try up to page 15 (should cover all ~530 players)
        if page == 1:
            url = base_url
        else:
            url = f"{base_url}/_/page/{page}"

        logger.info(f"Fetching page {page}...")
        time.sleep(1)  # Be respectful with rate limiting

        try:
            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code != 200:
                logger.warning(f"Page {page} returned status {response.status_code}")
                break

            soup = BeautifulSoup(response.content, "html.parser")

            # Find the salary table
            table = soup.find("table")
            if not table:
                logger.info(f"No table found on page {page}, stopping")
                break

            rows = table.find_all("tr")[1:]  # Skip header

            if not rows or len(rows) == 0:
                logger.info(f"No more data on page {page}")
                break

            page_salaries = 0

            for row in rows:
                cells = row.find_all("td")

                if len(cells) >= 4:
                    # Structure: [RK, NAME, TEAM, SALARY]
                    # Index 1 = NAME (the player)
                    # Index 3 = SALARY

                    name_cell = cells[1]
                    salary_cell = cells[3]

                    # Get player name (may have position after comma)
                    player_text = name_cell.get_text(strip=True)
                    # Remove position (e.g., "Stephen Curry, G" -> "Stephen Curry")
                    player_name = player_text.split(",")[0].strip()

                    # Get salary
                    salary_text = salary_cell.get_text(strip=True)

                    # Skip header rows (where salary column contains text like "SALARY")
                    if salary_text.upper() in ["SALARY", "SAL", ""]:
                        continue

                    salary_clean = salary_text.replace("$", "").replace(",", "").strip()

                    try:
                        salary = int(salary_clean)

                        if player_name and salary > 0:
                            all_salaries.append(
                                {
                                    "player_name": player_name,
                                    "annual_salary": salary,
                                    "season": season,
                                    "source": "espn",
                                }
                            )
                            page_salaries += 1
                    except ValueError:
                        # Log with more detail for debugging
                        logger.warning(
                            f"Could not parse salary for player '{player_name}': '{salary_text}'"
                        )
                        continue

            logger.info(f"Page {page}: {page_salaries} salaries")

            if page_salaries == 0:
                break

        except requests.RequestException as e:
            logger.error(f"Request error on page {page}: {e}")
            break
        except Exception as e:
            logger.error(f"Error parsing page {page}: {e}")
            break

    logger.info(f"Total from ESPN: {len(all_salaries)} salaries")
    return all_salaries


def fetch_salary_data(season: str = "2025-26") -> Dict[str, Any]:
    """
    Fetch player salary data from ESPN.

    Args:
        season: NBA season (e.g., "2025-26")

    Returns:
        Raw salary data dictionary (player names and salaries only)
    """
    logger.info("Fetching salary data...")

    try:
        # Fetch ESPN salaries
        espn_salaries = fetch_espn_salaries(season)

        if not espn_salaries:
            logger.warning("No salary data fetched from ESPN")
            return {
                "fetch_timestamp": datetime.utcnow().isoformat(),
                "source": "espn",
                "salaries": [],
            }

        return {
            "fetch_timestamp": datetime.utcnow().isoformat(),
            "source": "espn",
            "salaries": espn_salaries,
        }

    except Exception as e:
        logger.error(f"Error fetching salary data: {e}")
        return {
            "fetch_timestamp": datetime.utcnow().isoformat(),
            "source": "espn",
            "error": str(e),
            "salaries": [],
        }


def handler(event, context):
    """
    Lambda handler for fetching NBA data.

    This function orchestrates the data fetching process based on fetch_type:

    - 'stats_only' (daily): Fetches only player stats
    - 'monthly' (1st of month): Fetches players, salaries, teams, and stats
    - 'full' (manual): Fetches everything including detailed game logs

    All data is stored in S3 with date partitioning.
    """
    logger.info("Starting NBA data fetch Lambda")
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

    # Get execution parameters
    current_date = datetime.utcnow()
    date_partition = get_date_partition(current_date)

    # Determine what to fetch based on event
    fetch_type = event.get("fetch_type", "stats_only")  # stats_only, monthly, or full
    season = event.get("season", "2025-26")

    results = {"statusCode": 200, "fetched": [], "errors": []}

    players_data = None

    try:
        # 1. Fetch and store active players (monthly or full only)
        if fetch_type in ["monthly", "full"]:
            logger.info("Fetching active players...")
            players_data = fetch_active_players()
            if players_data:
                s3_key = f"raw/players/{date_partition}/active_players.json"
                if save_to_s3({"players": players_data}, s3_key):
                    results["fetched"].append("active_players")
            else:
                results["errors"].append("Failed to fetch active players")

        # 2. Fetch and store player stats (always)
        logger.info("Fetching player stats...")
        stats_data = fetch_player_stats(season)
        if stats_data:
            s3_key = f"raw/stats/{date_partition}/league_player_stats.json"
            if save_to_s3(stats_data, s3_key):
                results["fetched"].append("player_stats")
        else:
            results["errors"].append("Failed to fetch player stats")

        # 3. Fetch and store team data (monthly or full only)
        if fetch_type in ["monthly", "full"]:
            logger.info("Fetching team data...")
            teams_data = fetch_team_data()
            if teams_data:
                s3_key = f"raw/teams/{date_partition}/nba_teams.json"
                if save_to_s3({"teams": teams_data}, s3_key):
                    results["fetched"].append("teams")
            else:
                results["errors"].append("Failed to fetch teams")

        # 4. Fetch and store salary data from ESPN (monthly or full only)
        if fetch_type in ["monthly", "full"]:
            logger.info("Fetching salary data...")
            salary_data = fetch_salary_data(season)
            s3_key = f"raw/salaries/{date_partition}/player_salaries.json"
            if save_to_s3(salary_data, s3_key):
                results["fetched"].append("salaries")
            else:
                results["errors"].append("Failed to save salary data")

        # 5. Fetch detailed game logs for top players (optional, for full fetch)
        if fetch_type == "full" and stats_data:
            logger.info("Fetching detailed game logs for top players...")
            # Get top 50 players by minutes played
            player_stats = stats_data["players"]["resultSets"][0]["rowSet"]
            top_players = sorted(player_stats, key=lambda x: x[9] if x[9] else 0, reverse=True)[:50]

            game_logs = []
            for player in top_players[:10]:  # Limit to 10 for MVP
                player_id = player[0]
                player_name = player[1]
                logger.info(f"Fetching game logs for {player_name}")

                logs = fetch_player_game_logs(player_id, season)
                if logs:
                    game_logs.append(
                        {"player_id": player_id, "player_name": player_name, "logs": logs}
                    )

            if game_logs:
                s3_key = f"raw/game_logs/{date_partition}/top_players_game_logs.json"
                if save_to_s3({"game_logs": game_logs}, s3_key):
                    results["fetched"].append("game_logs")

        # Generate summary
        results["summary"] = {
            "timestamp": current_date.isoformat(),
            "environment": ENVIRONMENT,
            "season": season,
            "fetch_type": fetch_type,
            "successful_fetches": len(results["fetched"]),
            "errors_count": len(results["errors"]),
        }

        logger.info(f"Data fetch completed: {results['summary']}")

        # Return results for Step Functions
        return {
            "statusCode": 200,
            "body": json.dumps(results),
            "data_location": {"bucket": S3_BUCKET, "partition": date_partition},
        }

    except Exception as e:
        logger.error(f"Unexpected error in data fetch: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e), "message": "Data fetch failed"}),
        }


# For local testing
if __name__ == "__main__":
    # Test event
    test_event = {"fetch_type": "monthly", "season": "2025-26"}  # or 'stats_only', 'full'

    # Mock context
    class Context:
        function_name = "fetch_data"
        aws_request_id = "test-123"

    result = handler(test_event, Context())
    print(json.dumps(result, indent=2))
