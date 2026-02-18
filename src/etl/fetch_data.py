"""
Fetch NBA data from multiple APIs and sources.
This Lambda function fetches player stats, game data, and team information
from the NBA Stats API and stores raw data in S3.
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
import time
import unicodedata

import boto3
import requests
from bs4 import BeautifulSoup
from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import (
    playergamelog,
    playerindex,
    leaguedashplayerstats,
    commonplayerinfo,
    playercareerstats,
)
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client
s3_client = boto3.client("s3")

# Environment variables
S3_BUCKET = os.environ.get("DATA_BUCKET_NAME", "dev-nba-cap-optimizer-data")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")


def get_date_partition(date: datetime) -> str:
    """Generate S3 partition path based on date."""
    return f"year={date.year}/month={date.month:02d}/day={date.day:02d}"


def normalize_to_ascii(text: str) -> str:
    """
    Convert Unicode text to ASCII by removing diacritical marks.

    Examples:
        'Diabaté' -> 'Diabate'
        'Jokić' -> 'Jokic'
        'Dončić' -> 'Doncic'

    Args:
        text: Text with potential Unicode characters

    Returns:
        ASCII-only version of the text
    """
    if not text:
        return text

    # Normalize to NFD (decomposed form) - separates base letters from accents
    nfd = unicodedata.normalize("NFD", text)

    # Filter out combining characters (the accent marks)
    # Category 'Mn' is "Mark, Nonspacing" (accents, diacritics, etc.)
    ascii_text = "".join(char for char in nfd if unicodedata.category(char) != "Mn")

    return ascii_text


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
    Fetch all active NBA players and normalize names to ASCII.

    Returns:
        List of player dictionaries with ASCII-normalized names
    """
    logger.info("Fetching active players...")

    try:
        # Get all players from static data
        all_players = players.get_active_players()

        # Normalize all name fields to ASCII
        for player in all_players:
            if "full_name" in player:
                player["full_name"] = normalize_to_ascii(player["full_name"])
            if "first_name" in player:
                player["first_name"] = normalize_to_ascii(player["first_name"])
            if "last_name" in player:
                player["last_name"] = normalize_to_ascii(player["last_name"])

        logger.info(f"Found {len(all_players)} active players")
        return all_players
    except Exception as e:
        logger.error(f"Failed to fetch players: {e}")
        return []


def fetch_player_stats(season: str = "2025-26") -> Optional[Dict[str, Any]]:
    """
    Fetch comprehensive player stats for the season.

    Args:
        season: NBA season (e.g., "2024-25")

    Returns:
        Player stats data
    """
    logger.info(f"Fetching player stats for season {season}...")

    try:
        # Add delay to respect rate limits
        time.sleep(1)

        # Fetch league-wide player stats
        stats = leaguedashplayerstats.LeagueDashPlayerStats(
            season=season, season_type_all_star="Regular Season", per_mode_detailed="PerGame"
        )

        # Get the data as dictionaries
        data = {
            "season": season,
            "fetch_timestamp": datetime.utcnow().isoformat(),
            "players": stats.get_dict(),
        }

        logger.info(
            f"Successfully fetched stats for {len(data['players']['resultSets'][0]['rowSet'])} players"
        )
        return data

    except Exception as e:
        logger.error(f"Failed to fetch player stats: {e}")
        return None


def fetch_player_game_logs(player_id: str, season: str = "2025-26") -> Optional[Dict[str, Any]]:
    """
    Fetch game logs for a specific player.

    Args:
        player_id: NBA player ID
        season: NBA season

    Returns:
        Game log data
    """
    try:
        # Add delay to respect rate limits
        time.sleep(1)

        gamelog = playergamelog.PlayerGameLog(
            player_id=player_id, season=season, season_type_all_star="Regular Season"
        )

        return gamelog.get_dict()

    except Exception as e:
        logger.error(f"Failed to fetch game logs for player {player_id}: {e}")
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
        return all_teams
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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
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
                        logger.warning(f"Could not parse salary: {salary_text}")
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


def match_salaries_with_players(
    salaries: List[Dict[str, Any]], active_players: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Match salary data with NBA API player IDs.

    Args:
        salaries: List of salary dictionaries
        active_players: List of active player dictionaries from NBA API

    Returns:
        List of salary dictionaries with matched player_id
    """
    logger.info("Matching salaries with player IDs...")

    # Create lookup - names are already normalized (accents removed)
    player_lookup = {}
    for player in active_players:
        normalized_key = " ".join(player["full_name"].lower().split())
        player_lookup[normalized_key] = player["id"]

    # Match using normalized names
    matched = 0

    for salary in salaries:
        # Normalize ESPN name
        normalized = " ".join(salary["player_name"].lower().split())

        if normalized in player_lookup:
            salary["player_id"] = player_lookup[normalized]
            matched += 1
        else:
            salary["player_id"] = None

    match_rate = (matched / len(salaries) * 100) if salaries else 0
    logger.info(f"Matched: {matched}/{len(salaries)} ({match_rate:.1f}%)")

    return salaries


def fetch_salary_data(
    season: str = "2025-26", active_players: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """
    Fetch player salary data from ESPN and match with player IDs.

    Args:
        season: NBA season (e.g., "2025-26")
        active_players: List of active players for matching (optional)

    Returns:
        Salary data dictionary with matched player IDs
    """
    logger.info("Fetching salary data...")

    try:
        # Fetch ESPN salaries
        espn_salaries = fetch_espn_salaries(season)

        if not espn_salaries:
            logger.warning("No salary data fetched from ESPN")
            return {
                "fetch_timestamp": datetime.utcnow().isoformat(),
                "season": season,
                "source": "espn",
                "total_players": 0,
                "salaries": [],
            }

        # Match with player IDs if active_players provided
        if active_players:
            espn_salaries = match_salaries_with_players(espn_salaries, active_players)

        # Calculate statistics
        salaries_list = [s["annual_salary"] for s in espn_salaries]
        avg_salary = sum(salaries_list) / len(salaries_list) if salaries_list else 0
        matched_count = sum(1 for s in espn_salaries if s.get("player_id") is not None)

        return {
            "fetch_timestamp": datetime.utcnow().isoformat(),
            "season": season,
            "source": "espn",
            "total_players": len(espn_salaries),
            "matched_players": matched_count,
            "avg_salary": avg_salary,
            "salaries": espn_salaries,
        }

    except Exception as e:
        logger.error(f"Error fetching salary data: {e}")
        return {
            "fetch_timestamp": datetime.utcnow().isoformat(),
            "season": season,
            "source": "espn",
            "error": str(e),
            "total_players": 0,
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
            # If we didn't fetch players above, load from most recent S3 for matching
            if not players_data:
                logger.info("Loading recent player data from S3 for salary matching...")
                # For stats_only runs, we skip salary matching

            # Pass active players for matching if available
            salary_data = fetch_salary_data(season, players_data if players_data else None)
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
