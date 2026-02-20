"""
Tests for transform_data Lambda function (Basketball Reference version).
"""

import json
from unittest.mock import MagicMock, patch

from src.etl import transform_data


class TestNormalizeTeamAbbreviation:
    """Test team abbreviation normalization."""

    def test_normalizes_brk_to_bkn(self):
        """Test Brooklyn Nets abbreviation is normalized."""
        result = transform_data.normalize_team_abbreviation("BRK")
        assert result == "BKN"

    def test_normalizes_cho_to_cha(self):
        """Test Charlotte Hornets abbreviation is normalized."""
        result = transform_data.normalize_team_abbreviation("CHO")
        assert result == "CHA"

    def test_normalizes_pho_to_phx(self):
        """Test Phoenix Suns abbreviation is normalized."""
        result = transform_data.normalize_team_abbreviation("PHO")
        assert result == "PHX"

    def test_leaves_other_abbreviations_unchanged(self):
        """Test other abbreviations are not modified."""
        result = transform_data.normalize_team_abbreviation("LAL")
        assert result == "LAL"

        result = transform_data.normalize_team_abbreviation("GSW")
        assert result == "GSW"

        result = transform_data.normalize_team_abbreviation("BOS")
        assert result == "BOS"


class TestMatchSalariesWithPlayers:
    """Test salary matching with player IDs."""

    def test_exact_name_match(self):
        """Test matching with exact name match."""
        salaries = [{"player_name": "LeBron James", "annual_salary": 50000000, "season": "2025-26"}]
        players = [{"id": 2544, "full_name": "LeBron James"}]

        result = transform_data.match_salaries_with_players(salaries, players)

        assert result[0]["player_id"] == 2544
        assert result[0]["player_name"] == "LeBron James"

    def test_no_match_sets_none(self):
        """Test player_id is None when no match found."""
        salaries = [
            {"player_name": "Unknown Player", "annual_salary": 10000000, "season": "2025-26"}
        ]
        players = [{"id": 1, "full_name": "Known Player"}]

        result = transform_data.match_salaries_with_players(salaries, players)

        assert result[0]["player_id"] is None


class TestEnrichPlayerStats:
    """Test player stats enrichment with Basketball Reference data."""

    def test_enrichment_merges_per_game_and_advanced_stats(self):
        """Test enrichment merges per-game and advanced stats."""
        stats_data = {
            "season": "2025-26",
            "source": "basketball_reference",
            "per_game_stats": [
                {
                    "Player": "LeBron James",
                    "Pos": "SF",
                    "Age": 39,
                    "Team": "LAL",
                    "G": 50,
                    "GS": 48,
                    "MP": 35.2,
                    "PTS": 25.8,
                    "TRB": 7.7,
                    "AST": 8.2,
                }
            ],
            "advanced_stats": [
                {
                    "Player": "LeBron James",
                    "PER": 24.5,
                    "TS%": 0.623,
                    "USG%": 29.2,
                    "WS": 7.5,
                    "BPM": 6.8,
                    "VORP": 4.2,
                }
            ],
        }

        result = transform_data.enrich_player_stats(stats_data)

        assert len(result) == 1
        player = result[0]

        # Check per-game stats
        assert player["player_name"] == "LeBron James"
        assert player["age"] == 39
        assert player["team_abbreviation"] == "LAL"
        assert player["points"] == 25.8

        # Check advanced stats
        assert player["per"] == 24.5
        assert player["ts_pct"] == 0.623
        assert player["vorp"] == 4.2

    def test_enrichment_normalizes_team_abbreviations(self):
        """Test enrichment normalizes Basketball Reference team abbreviations."""
        stats_data = {
            "season": "2025-26",
            "source": "basketball_reference",
            "per_game_stats": [
                {"Player": "Kevin Durant", "Team": "PHO", "PTS": 28.5, "TRB": 7.0, "AST": 5.5},
                {"Player": "Mikal Bridges", "Team": "BRK", "PTS": 20.1, "TRB": 4.5, "AST": 3.7},
                {"Player": "LaMelo Ball", "Team": "CHO", "PTS": 23.9, "TRB": 6.2, "AST": 8.4},
            ],
            "advanced_stats": [],
        }

        result = transform_data.enrich_player_stats(stats_data)

        assert len(result) == 3
        assert result[0]["team_abbreviation"] == "PHX"  # PHO -> PHX
        assert result[1]["team_abbreviation"] == "BKN"  # BRK -> BKN
        assert result[2]["team_abbreviation"] == "CHA"  # CHO -> CHA

    def test_enrichment_handles_none_team_abbreviation(self):
        """Test enrichment handles missing team abbreviation."""
        stats_data = {
            "season": "2025-26",
            "source": "basketball_reference",
            "per_game_stats": [
                {"Player": "Free Agent", "PTS": 15.0, "TRB": 5.0, "AST": 3.0},
            ],
            "advanced_stats": [],
        }

        result = transform_data.enrich_player_stats(stats_data)

        assert len(result) == 1
        assert result[0]["team_abbreviation"] is None

    def test_enrichment_handles_empty_stats(self):
        """Test enrichment with no players."""
        stats_data = {
            "season": "2025-26",
            "source": "basketball_reference",
            "per_game_stats": [],
            "advanced_stats": [],
        }

        result = transform_data.enrich_player_stats(stats_data)

        assert result == []


class TestEnrichTeamData:
    """Test team data enrichment."""

    def test_enrichment_calculates_salary_metrics(self):
        """Test team enrichment calculates salary aggregations."""
        teams = [{"id": 1, "abbreviation": "GSW", "full_name": "Golden State Warriors"}]
        salaries = [
            {"player_name": "Player 1", "annual_salary": 50000000},
            {"player_name": "Player 2", "annual_salary": 30000000},
        ]
        stats = [
            {
                "player_name": "Player 1",
                "team_abbreviation": "GSW",
                "points": 25.0,
                "rebounds": 7.0,
                "assists": 8.0,
            },
            {
                "player_name": "Player 2",
                "team_abbreviation": "GSW",
                "points": 15.0,
                "rebounds": 5.0,
                "assists": 4.0,
            },
        ]

        result = transform_data.enrich_team_data(teams, salaries, stats)

        assert result[0]["total_payroll"] == 80000000
        assert result[0]["roster_count"] == 2

    def test_enrichment_matches_normalized_team_abbreviations(self):
        """Test team enrichment works with normalized abbreviations from Basketball Reference."""
        # Phoenix Suns - NBA API uses PHX, Basketball Reference uses PHO (normalized to PHX)
        teams = [{"id": 1, "abbreviation": "PHX", "full_name": "Phoenix Suns"}]
        salaries = [
            {"player_name": "Kevin Durant", "annual_salary": 47000000},
            {"player_name": "Devin Booker", "annual_salary": 36000000},
        ]
        # Stats would have PHX after normalization from PHO
        stats = [
            {
                "player_name": "Kevin Durant",
                "team_abbreviation": "PHX",
                "points": 28.5,
                "rebounds": 7.0,
                "assists": 5.5,
            },
            {
                "player_name": "Devin Booker",
                "team_abbreviation": "PHX",
                "points": 27.1,
                "rebounds": 4.5,
                "assists": 6.9,
            },
        ]

        result = transform_data.enrich_team_data(teams, salaries, stats)

        assert result[0]["total_payroll"] == 83000000
        assert result[0]["roster_count"] == 2
        assert result[0]["roster_with_salary"] == 2


class TestHandlerValidationCheck:
    """Test handler validation gate logic."""

    @patch("src.etl.transform_data.ENVIRONMENT", "test")
    @patch("src.etl.transform_data.S3_BUCKET", "test-bucket")
    def test_handler_skips_transformation_when_validation_fails(self):
        """Test handler returns 400 when validation_passed is false."""
        event = {
            "validation_passed": False,
            "data_location": {"bucket": "test", "partition": "year=2024/month=02/day=17"},
        }

        result = transform_data.handler(event, MagicMock())

        assert result["statusCode"] == 400


class TestHandlerTransformation:
    """Test handler transformation workflow."""

    @patch("src.etl.transform_data.ENVIRONMENT", "test")
    @patch("src.etl.transform_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.transform_data.save_to_s3")
    @patch("src.etl.transform_data.load_from_s3")
    def test_handler_successful_transformation(self, mock_load, mock_save):
        """Test handler successfully transforms Basketball Reference data."""

        def mock_load_side_effect(s3_key):
            if "players" in s3_key:
                return {
                    "players": [
                        {"id": 2544, "full_name": "LeBron James"},
                    ]
                }
            elif "stats" in s3_key:
                return {
                    "season": "2025-26",
                    "source": "basketball_reference",
                    "per_game_stats": [
                        {
                            "Player": "LeBron James",
                            "Team": "LAL",
                            "PTS": 25.8,
                            "TRB": 7.7,
                            "AST": 8.2,
                        }
                    ],
                    "advanced_stats": [
                        {
                            "Player": "LeBron James",
                            "PER": 24.5,
                            "TS%": 0.623,
                            "VORP": 4.2,
                        }
                    ],
                }
            elif "salaries" in s3_key:
                return {
                    "fetch_timestamp": "2024-02-17T00:00:00",
                    "source": "espn",
                    "salaries": [
                        {
                            "player_name": "LeBron James",
                            "annual_salary": 47607350,
                            "season": "2025-26",
                            "source": "espn",
                        }
                    ],
                }
            elif "teams" in s3_key:
                return {
                    "teams": [
                        {
                            "id": 1610612747,
                            "full_name": "Los Angeles Lakers",
                            "abbreviation": "LAL",
                        }
                    ]
                }
            return None

        mock_load.side_effect = mock_load_side_effect
        mock_save.return_value = True

        event = {
            "validation_passed": True,
            "data_location": {"bucket": "test", "partition": "year=2024/month=02/day=17"},
        }

        result = transform_data.handler(event, MagicMock())

        assert result["statusCode"] == 200
        assert result["transformation_successful"] is True
        body = json.loads(result["body"])
        assert "enriched_salaries" in body["transformed"]
        assert "enriched_player_stats" in body["transformed"]
        assert "enriched_teams" in body["transformed"]
