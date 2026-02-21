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

    def test_filters_out_league_average_summary_row(self):
        """Test that 'League Average' summary row is filtered out."""
        stats_data = {
            "season": "2025-26",
            "source": "basketball_reference",
            "per_game_stats": [
                {
                    "Player": "LeBron James",
                    "Team": "LAL",
                    "PTS": 25.8,
                    "TRB": 7.7,
                    "AST": 8.2,
                },
                {
                    "Player": "League Average",
                    "Team": None,
                    "PTS": float("nan"),
                    "TRB": float("nan"),
                    "AST": float("nan"),
                },
            ],
            "advanced_stats": [],
        }

        result = transform_data.enrich_player_stats(stats_data)

        assert len(result) == 1
        assert result[0]["player_name"] == "LeBron James"

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
        assert player["points"] == 25.8

        # Check multi-team fields
        assert player["is_multi_team"] is False
        assert player["teams_played_for"] == ["LAL"]
        assert len(player["stats_by_team"]) == 1
        assert player["stats_by_team"][0]["team_abbreviation"] == "LAL"
        assert player["stats_by_team"][0]["points"] == 25.8

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
        assert result[0]["teams_played_for"] == ["PHX"]  # PHO -> PHX
        assert result[1]["teams_played_for"] == ["BKN"]  # BRK -> BKN
        assert result[2]["teams_played_for"] == ["CHA"]  # CHO -> CHA

    def test_enrichment_handles_multi_team_players(self):
        """Test enrichment handles multi-team players correctly."""
        stats_data = {
            "season": "2025-26",
            "source": "basketball_reference",
            "per_game_stats": [
                {
                    "Player": "Kevin Huerter",
                    "Age": 27,
                    "Pos": "SF",
                    "Team": "2TM",
                    "G": 48,
                    "PTS": 10.3,
                    "TRB": 3.7,
                    "AST": 2.5,
                },
                {
                    "Player": "Kevin Huerter",
                    "Age": 27,
                    "Pos": "SF",
                    "Team": "CHI",
                    "G": 44,
                    "PTS": 10.9,
                    "TRB": 3.8,
                    "AST": 2.6,
                },
                {
                    "Player": "Kevin Huerter",
                    "Age": 27,
                    "Pos": "SG",
                    "Team": "DET",
                    "G": 4,
                    "PTS": 4.3,
                    "TRB": 1.8,
                    "AST": 0.8,
                },
            ],
            "advanced_stats": [],
        }

        result = transform_data.enrich_player_stats(stats_data)

        assert len(result) == 1
        player = result[0]

        # Check multi-team metadata
        assert player["is_multi_team"] is True
        assert player["teams_played_for"] == ["CHI", "DET"]

        # Check main stats use aggregate row (2TM)
        assert player["games_played"] == 48
        assert player["points"] == 10.3

        # Check stats_by_team has individual team breakdowns
        assert len(player["stats_by_team"]) == 2
        chi_stats = next(s for s in player["stats_by_team"] if s["team_abbreviation"] == "CHI")
        det_stats = next(s for s in player["stats_by_team"] if s["team_abbreviation"] == "DET")

        assert chi_stats["games_played"] == 44
        assert chi_stats["points"] == 10.9
        assert det_stats["games_played"] == 4
        assert det_stats["points"] == 4.3

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
                "is_multi_team": False,
                "teams_played_for": ["GSW"],
                "points": 25.0,
                "rebounds": 7.0,
                "assists": 8.0,
                "stats_by_team": [
                    {
                        "team_abbreviation": "GSW",
                        "points": 25.0,
                        "rebounds": 7.0,
                        "assists": 8.0,
                    }
                ],
            },
            {
                "player_name": "Player 2",
                "is_multi_team": False,
                "teams_played_for": ["GSW"],
                "points": 15.0,
                "rebounds": 5.0,
                "assists": 4.0,
                "stats_by_team": [
                    {
                        "team_abbreviation": "GSW",
                        "points": 15.0,
                        "rebounds": 5.0,
                        "assists": 4.0,
                    }
                ],
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
                "is_multi_team": False,
                "teams_played_for": ["PHX"],
                "points": 28.5,
                "rebounds": 7.0,
                "assists": 5.5,
                "stats_by_team": [
                    {
                        "team_abbreviation": "PHX",
                        "points": 28.5,
                        "rebounds": 7.0,
                        "assists": 5.5,
                    }
                ],
            },
            {
                "player_name": "Devin Booker",
                "is_multi_team": False,
                "teams_played_for": ["PHX"],
                "points": 27.1,
                "rebounds": 4.5,
                "assists": 6.9,
                "stats_by_team": [
                    {
                        "team_abbreviation": "PHX",
                        "points": 27.1,
                        "rebounds": 4.5,
                        "assists": 6.9,
                    }
                ],
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


class TestHandlerNaNValidation:
    """Test handler NaN validation logic."""

    @patch("src.etl.transform_data.ENVIRONMENT", "test")
    @patch("src.etl.transform_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.transform_data.enrich_player_stats")
    @patch("src.etl.transform_data.load_from_s3")
    def test_handler_returns_400_when_nan_in_statistics(self, mock_load, mock_enrich_player_stats):
        """Test handler returns 400 when NaN values detected in output statistics."""

        # Mock enrichment to return stats with NaN that would cause NaN in aggregates
        mock_enrich_player_stats.return_value = [
            {
                "player_name": "Player 1",
                "points": float("nan"),
                "rebounds": 5.0,
                "assists": 3.0,
            }
        ]

        def mock_load_side_effect(s3_key):
            if "stats" in s3_key:
                return {
                    "season": "2025-26",
                    "source": "basketball_reference",
                    "per_game_stats": [{"Player": "Test"}],
                    "advanced_stats": [],
                }
            return None

        mock_load.side_effect = mock_load_side_effect

        event = {
            "validation_passed": True,
            "data_location": {"bucket": "test", "partition": "year=2024/month=02/day=17"},
        }

        result = transform_data.handler(event, MagicMock())

        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert body["error"] == "Invalid statistics"
        assert "NaN values detected" in body["message"]


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
