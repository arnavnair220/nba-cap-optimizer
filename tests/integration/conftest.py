"""
Shared fixtures and helpers for integration tests.
"""

from datetime import datetime
from typing import Any, Dict

import pytest


def create_basketball_reference_player_stats(
    player_name: str = "LeBron James",
    position: str = "SF",
    age: int = 40,
    team: str = "LAL",
    games: int = 50,
    points: float = 25.0,
    rebounds: float = 7.5,
    assists: float = 8.0,
) -> Dict[str, Any]:
    """
    Create a complete Basketball Reference per-game stat dictionary.
    All fields match the actual structure scraped from Basketball-Reference.com using pandas.read_html().

    Columns from: https://www.basketball-reference.com/leagues/NBA_2026_per_game.html
    ['Rk', 'Player', 'Age', 'Team', 'Pos', 'G', 'GS', 'MP', 'FG', 'FGA', 'FG%',
     '3P', '3PA', '3P%', '2P', '2PA', '2P%', 'eFG%', 'FT', 'FTA', 'FT%',
     'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'Awards']
    """
    return {
        "Player": player_name,
        "Pos": position,
        "Age": age,
        "Team": team,
        "G": games,
        "GS": games - 2,  # Games started (usually slightly less than games played)
        "MP": 35.0,  # Minutes per game
        "FG": 9.0,  # Field goals made
        "FGA": 18.0,  # Field goal attempts
        "FG%": 0.50,  # Field goal percentage
        "3P": 2.0,  # 3-pointers made
        "3PA": 6.0,  # 3-point attempts
        "3P%": 0.333,  # 3-point percentage
        "2P": 7.0,  # 2-pointers made
        "2PA": 12.0,  # 2-point attempts
        "2P%": 0.583,  # 2-point percentage
        "eFG%": 0.556,  # Effective field goal percentage
        "FT": 3.0,  # Free throws made
        "FTA": 4.0,  # Free throw attempts
        "FT%": 0.750,  # Free throw percentage
        "ORB": 1.0,  # Offensive rebounds
        "DRB": rebounds - 1.0,  # Defensive rebounds
        "TRB": rebounds,  # Total rebounds
        "AST": assists,
        "STL": 1.2,  # Steals
        "BLK": 0.8,  # Blocks
        "TOV": 2.5,  # Turnovers
        "PF": 1.8,  # Personal fouls
        "PTS": points,
    }


def create_basketball_reference_advanced_stats(
    player_name: str = "LeBron James",
    position: str = "SF",
    age: int = 40,
    team: str = "LAL",
    games: int = 50,
    per: float = 24.5,
    vorp: float = 4.0,
) -> Dict[str, Any]:
    """
    Create a complete Basketball Reference advanced stat dictionary.

    Columns from: https://www.basketball-reference.com/leagues/NBA_2026_advanced.html
    ['Rk', 'Player', 'Age', 'Team', 'Pos', 'G', 'GS', 'MP', 'PER', 'TS%', '3PAr', 'FTr',
     'ORB%', 'DRB%', 'TRB%', 'AST%', 'STL%', 'BLK%', 'TOV%', 'USG%',
     'OWS', 'DWS', 'WS', 'WS/48', 'OBPM', 'DBPM', 'BPM', 'VORP', 'Awards']
    """
    return {
        "Player": player_name,
        "Age": age,
        "Team": team,
        "Pos": position,
        "G": games,
        "GS": games - 2,
        "MP": games * 35,  # Total minutes
        "PER": per,  # Player efficiency rating
        "TS%": 0.623,  # True shooting percentage
        "3PAr": 0.333,  # 3-point attempt rate
        "FTr": 0.222,  # Free throw attempt rate
        "ORB%": 3.5,  # Offensive rebound percentage
        "DRB%": 18.0,  # Defensive rebound percentage
        "TRB%": 10.5,  # Total rebound percentage
        "AST%": 35.0,  # Assist percentage
        "STL%": 1.5,  # Steal percentage
        "BLK%": 1.0,  # Block percentage
        "TOV%": 12.0,  # Turnover percentage
        "USG%": 29.2,  # Usage percentage
        "OWS": 4.5,  # Offensive win shares
        "DWS": 3.0,  # Defensive win shares
        "WS": 7.5,  # Total win shares
        "WS/48": 0.214,  # Win shares per 48 minutes
        "OBPM": 4.5,  # Offensive box plus/minus
        "DBPM": 2.3,  # Defensive box plus/minus
        "BPM": 6.8,  # Box plus/minus
        "VORP": vorp,  # Value over replacement player
    }


@pytest.fixture
def mock_complete_stats_data():
    """
    Fixture providing complete Basketball Reference stats data for 350 players.
    This matches the structure returned by fetch_data.fetch_player_stats().
    """
    player_count = 350

    per_game_stats = [
        create_basketball_reference_player_stats(
            player_name=f"Player {i}",
            position="PG" if i % 5 == 0 else "SG",
            age=25 + (i % 10),
            team="LAL",
            games=60,
            points=15.0 + (i % 10),
            rebounds=5.0,
            assists=4.0,
        )
        for i in range(player_count)
    ]

    advanced_stats = [
        create_basketball_reference_advanced_stats(
            player_name=f"Player {i}", per=15.0 + (i % 5), vorp=2.0 + (i % 3)
        )
        for i in range(player_count)
    ]

    return {
        "season": "2025-26",
        "fetch_timestamp": datetime.utcnow().isoformat(),
        "source": "basketball_reference",
        "per_game_stats": per_game_stats,
        "advanced_stats": advanced_stats,
        "per_game_columns": [
            "Player",
            "Pos",
            "Age",
            "Team",
            "G",
            "GS",
            "MP",
            "FG",
            "FGA",
            "FG%",
            "3P",
            "3PA",
            "3P%",
            "2P",
            "2PA",
            "2P%",
            "eFG%",
            "FT",
            "FTA",
            "FT%",
            "ORB",
            "DRB",
            "TRB",
            "AST",
            "STL",
            "BLK",
            "TOV",
            "PF",
            "PTS",
        ],
        "advanced_columns": [
            "Player",
            "Pos",
            "Age",
            "Team",
            "G",
            "GS",
            "MP",
            "PER",
            "TS%",
            "3PAr",
            "FTr",
            "ORB%",
            "DRB%",
            "TRB%",
            "AST%",
            "STL%",
            "BLK%",
            "TOV%",
            "USG%",
            "OWS",
            "DWS",
            "WS",
            "WS/48",
            "OBPM",
            "DBPM",
            "BPM",
            "VORP",
        ],
    }


@pytest.fixture
def mock_active_players():
    """Fixture providing active players data matching NBA API format."""
    return {
        "players": [
            {"id": i, "full_name": f"Player {i}", "first_name": "Player", "last_name": f"{i}"}
            for i in range(400)
        ]
    }


@pytest.fixture
def mock_nba_teams():
    """Fixture providing NBA teams data."""
    return {
        "teams": [
            {
                "id": 1610612740 + i,
                "full_name": f"Team {i}",
                "abbreviation": f"T{i:02d}",
                "nickname": f"Team{i}",
                "city": "City",
                "state": "State",
                "year_founded": 1946 + i,
            }
            for i in range(30)
        ]
    }


@pytest.fixture
def mock_salary_data():
    """Fixture providing salary data matching ESPN format with realistic NBA salaries."""
    # Generate realistic NBA salaries totaling ~$4.2B (within valid $3.5B-$7B range)
    # Range from $1M to $20M across 400 players
    return {
        "fetch_timestamp": datetime.utcnow().isoformat(),
        "source": "espn",
        "salaries": [
            {
                "player_name": f"Player {i}",
                "annual_salary": 1_000_000 + (i * 47_619),  # Linear scale 1M to 20M
                "season": "2025-26",
                "source": "espn",
            }
            for i in range(400)
        ],
    }


@pytest.fixture
def mock_realistic_monthly_data():
    """
    Fixture providing realistic monthly data with star players.
    Useful for end-to-end tests with recognizable player names.
    Includes enough players to meet salary validation thresholds ($3.5B-$7B).
    """
    # Star players
    star_players = [
        {"id": 2544, "full_name": "LeBron James"},
        {"id": 203076, "full_name": "Anthony Davis"},
        {"id": 203999, "full_name": "Nikola Jokic"},
        {"id": 1629029, "full_name": "Luka Doncic"},
    ]

    # Add 396 mock players to meet validation requirements
    mock_players = [
        {"id": 1000000 + i, "full_name": f"Player {chr(65 + (i % 26))}{i}"} for i in range(396)
    ]

    players = {"players": star_players + mock_players}

    # Star player stats
    star_per_game = [
        create_basketball_reference_player_stats(
            "LeBron James", "SF", 40, "LAL", 50, 25.0, 7.5, 8.0
        ),
        create_basketball_reference_player_stats(
            "Anthony Davis", "C", 31, "LAL", 55, 27.0, 12.0, 3.5
        ),
        create_basketball_reference_player_stats(
            "Nikola Jokic", "C", 29, "DEN", 60, 26.0, 12.0, 9.0
        ),
        create_basketball_reference_player_stats(
            "Luka Doncic", "PG", 25, "DAL", 58, 29.0, 9.0, 8.5
        ),
    ]

    star_advanced = [
        create_basketball_reference_advanced_stats("LeBron James", 24.5, 4.0),
        create_basketball_reference_advanced_stats("Anthony Davis", 26.0, 5.0),
        create_basketball_reference_advanced_stats("Nikola Jokic", 31.0, 8.0),
        create_basketball_reference_advanced_stats("Luka Doncic", 28.0, 6.5),
    ]

    # Mock player stats (uniform data)
    mock_per_game = [
        create_basketball_reference_player_stats(
            f"Player {chr(65 + (i % 26))}{i}", "F", 25, "LAL", 50, 10.0, 5.0, 3.0
        )
        for i in range(396)
    ]

    mock_advanced = [
        create_basketball_reference_advanced_stats(f"Player {chr(65 + (i % 26))}{i}", 15.0, 2.0)
        for i in range(396)
    ]

    per_game_stats = star_per_game + mock_per_game
    advanced_stats = star_advanced + mock_advanced

    stats = {
        "season": "2025-26",
        "fetch_timestamp": datetime.utcnow().isoformat(),
        "source": "basketball_reference",
        "per_game_stats": per_game_stats,
        "advanced_stats": advanced_stats,
        "per_game_columns": list(per_game_stats[0].keys()),
        "advanced_columns": list(advanced_stats[0].keys()),
    }

    # Star player salaries
    star_salaries = [
        {
            "player_name": "LeBron James",
            "annual_salary": 48000000,
            "season": "2025-26",
            "source": "espn",
        },
        {
            "player_name": "Anthony Davis",
            "annual_salary": 55000000,
            "season": "2025-26",
            "source": "espn",
        },
        {
            "player_name": "Nikola Jokic",
            "annual_salary": 51000000,
            "season": "2025-26",
            "source": "espn",
        },
        {
            "player_name": "Luka Doncic",
            "annual_salary": 43000000,
            "season": "2025-26",
            "source": "espn",
        },
    ]

    # Mock player salaries (all paid $10M each)
    # 396 players * $10M + $197M (stars) = ~$4.16B (within $3.5B-$7B range)
    mock_salaries = [
        {
            "player_name": f"Player {chr(65 + (i % 26))}{i}",
            "annual_salary": 10_000_000,
            "season": "2025-26",
            "source": "espn",
        }
        for i in range(396)
    ]

    salaries = {
        "fetch_timestamp": datetime.utcnow().isoformat(),
        "source": "espn",
        "salaries": star_salaries + mock_salaries,
    }

    teams = {
        "teams": [
            {
                "id": 1610612747,
                "full_name": "Los Angeles Lakers",
                "abbreviation": "LAL",
                "nickname": "Lakers",
                "city": "Los Angeles",
                "state": "California",
                "year_founded": 1947,
            },
            {
                "id": 1610612743,
                "full_name": "Denver Nuggets",
                "abbreviation": "DEN",
                "nickname": "Nuggets",
                "city": "Denver",
                "state": "Colorado",
                "year_founded": 1976,
            },
            {
                "id": 1610612742,
                "full_name": "Dallas Mavericks",
                "abbreviation": "DAL",
                "nickname": "Mavericks",
                "city": "Dallas",
                "state": "Texas",
                "year_founded": 1980,
            },
        ]
    }

    return {
        "players": players,
        "stats": stats,
        "salaries": salaries,
        "teams": teams,
    }


def create_s3_storage_mock():
    """
    Helper function to create an S3 storage simulation for integration tests.
    Returns save and load functions that operate on an in-memory dictionary.
    """
    storage = {}

    def save_impl(data, key):
        storage[key] = data
        return True

    def load_impl(key):
        return storage.get(key)

    return storage, save_impl, load_impl
