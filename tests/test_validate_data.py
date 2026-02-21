"""
Tests for validate_data Lambda function.
"""

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

from src.etl import validate_data


class TestEmptyDataValidation:
    """Test validation behavior when no data files exist."""

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    def test_stats_only_missing_required_file(self, mock_load, mock_save):
        """Test stats_only fetch fails when stats file is missing."""
        mock_load.return_value = None

        event = {
            "data_location": {"partition": "year=2026/month=02/day=19"},
            "fetch_type": "stats_only",
        }

        result = validate_data.handler(event, MagicMock())

        assert result["statusCode"] == 422
        assert result["validation_passed"] is False
        body = json.loads(result["body"])
        assert body["valid"] is False
        assert body["error_count"] == 1

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    def test_monthly_missing_all_files(self, mock_load, mock_save):
        """Test monthly fetch fails when all files are missing."""
        mock_load.return_value = None

        event = {
            "data_location": {"partition": "year=2026/month=02/day=19"},
            "fetch_type": "monthly",
        }

        result = validate_data.handler(event, MagicMock())

        assert result["statusCode"] == 422
        assert result["validation_passed"] is False
        body = json.loads(result["body"])
        assert body["valid"] is False
        assert body["error_count"] == 4  # players, stats, teams, salaries

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    def test_monthly_missing_one_required_file(self, mock_load, mock_save):
        """Test monthly fetch fails when one required file is missing."""

        def mock_load_side_effect(s3_key):
            if "salaries" in s3_key:
                return None
            elif "stats" in s3_key:
                return {
                    "season": "2025-26",
                    "fetch_timestamp": datetime.utcnow().isoformat(),
                    "source": "basketball_reference",
                    "per_game_stats": [],
                    "advanced_stats": [],
                    "per_game_columns": [],
                    "advanced_columns": [],
                }
            elif "players" in s3_key:
                return {"players": [{"id": 1, "full_name": "Test Player"}]}
            elif "teams" in s3_key:
                return {
                    "teams": [
                        {
                            "id": 1,
                            "full_name": "Test Team",
                            "abbreviation": "TST",
                            "year_founded": 2000,
                        }
                    ]
                }
            return None

        mock_load.side_effect = mock_load_side_effect

        event = {
            "data_location": {"partition": "year=2026/month=02/day=19"},
            "fetch_type": "monthly",
        }

        result = validate_data.handler(event, MagicMock())

        assert result["statusCode"] == 422
        assert result["validation_passed"] is False
        body = json.loads(result["body"])
        assert body["valid"] is False
        assert body["error_count"] >= 1

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    def test_stats_only_ignores_missing_monthly_files(self, mock_load, mock_save):
        """Test stats_only fetch doesn't fail when monthly files are missing."""

        def mock_load_side_effect(s3_key):
            if "stats" in s3_key:
                return {
                    "season": "2025-26",
                    "fetch_timestamp": datetime.utcnow().isoformat(),
                    "source": "basketball_reference",
                    "per_game_stats": [],
                    "advanced_stats": [],
                    "per_game_columns": [],
                    "advanced_columns": [],
                }
            return None

        mock_load.side_effect = mock_load_side_effect

        event = {
            "data_location": {"partition": "year=2026/month=02/day=19"},
            "fetch_type": "stats_only",
        }

        result = validate_data.handler(event, MagicMock())

        assert result["statusCode"] == 200
        assert result["validation_passed"] is True
        body = json.loads(result["body"])
        assert body["valid"] is True


class TestEmptyDataArrays:
    """Test validation behavior when data files exist but contain empty arrays."""

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    def test_empty_players_array_fails(self, mock_load, mock_save):
        """Test validation fails when players array is empty."""
        mock_load.return_value = {"players": []}

        event = {
            "data_location": {"partition": "year=2026/month=02/day=19"},
            "fetch_type": "monthly",
        }

        # Only load players data
        def mock_load_side_effect(s3_key):
            if "players" in s3_key:
                return {"players": []}
            return None

        mock_load.side_effect = mock_load_side_effect

        result = validate_data.handler(event, MagicMock())

        assert result["validation_passed"] is False

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    def test_empty_salaries_array_with_warning(self, mock_load, mock_save):
        """Test validation warns when salaries array is empty."""

        def mock_load_side_effect(s3_key):
            if "salaries" in s3_key:
                return {
                    "fetch_timestamp": datetime.utcnow().isoformat(),
                    "source": "espn",
                    "salaries": [],
                }
            elif "stats" in s3_key:
                return {
                    "season": "2025-26",
                    "fetch_timestamp": datetime.utcnow().isoformat(),
                    "source": "basketball_reference",
                    "per_game_stats": [{"Player": "Test Player", "PTS": 20.0}],
                    "advanced_stats": [{"Player": "Test Player", "PER": 25.0}],
                    "per_game_columns": [
                        "Player",
                        "Pos",
                        "Age",
                        "Team",
                        "G",
                        "MP",
                        "PTS",
                        "TRB",
                        "AST",
                    ],
                    "advanced_columns": ["Player", "Pos", "Age", "Team", "G", "MP", "PER"],
                }
            elif "players" in s3_key:
                return {"players": [{"id": 1, "full_name": "Test Player"}]}
            elif "teams" in s3_key:
                return {
                    "teams": [
                        {
                            "id": 1,
                            "full_name": "Test Team",
                            "abbreviation": "TST",
                            "year_founded": 2000,
                        }
                        for _ in range(30)
                    ]
                }
            return None

        mock_load.side_effect = mock_load_side_effect

        event = {
            "data_location": {"partition": "year=2026/month=02/day=19"},
            "fetch_type": "monthly",
        }

        result = validate_data.handler(event, MagicMock())

        body = json.loads(result["body"])
        assert body["warning_count"] > 0


class TestMissingDataLocationEvent:
    """Test validation behavior when event is missing required fields."""

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    def test_missing_data_location(self):
        """Test validation fails when data_location is missing from event."""
        event = {}

        result = validate_data.handler(event, MagicMock())

        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "data_location" in body["error"].lower()


class TestBasketballReferenceValidation:
    """Test Basketball Reference specific validation logic."""

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    def test_valid_basketball_reference_data(self, mock_load, mock_save):
        """Test validation passes with properly formatted Basketball Reference data."""

        def mock_load_side_effect(s3_key):
            if "stats" in s3_key:
                # Create realistic data with 350 players
                per_game = [
                    {
                        "Player": f"Player {i}",
                        "Pos": "PG",
                        "Age": 25,
                        "Team": "LAL",
                        "G": 70,
                        "MP": 30.0,
                        "PTS": 15.0,
                        "TRB": 5.0,
                        "AST": 5.0,
                        "FG%": 0.45,
                        "3P%": 0.35,
                        "FT%": 0.80,
                    }
                    for i in range(350)
                ]
                advanced = [
                    {
                        "Player": f"Player {i}",
                        "Pos": "PG",
                        "Age": 25,
                        "Team": "LAL",
                        "G": 70,
                        "MP": 30.0,
                        "PER": 15.0,
                    }
                    for i in range(350)
                ]
                return {
                    "season": "2025-26",
                    "fetch_timestamp": datetime.utcnow().isoformat(),
                    "source": "basketball_reference",
                    "per_game_stats": per_game,
                    "advanced_stats": advanced,
                    "per_game_columns": [
                        "Player",
                        "Pos",
                        "Age",
                        "Team",
                        "G",
                        "MP",
                        "PTS",
                        "TRB",
                        "AST",
                        "FG%",
                        "3P%",
                        "FT%",
                    ],
                    "advanced_columns": ["Player", "Pos", "Age", "Team", "G", "MP", "PER"],
                }
            return None

        mock_load.side_effect = mock_load_side_effect

        event = {
            "data_location": {"partition": "year=2026/month=02/day=19"},
            "fetch_type": "stats_only",
        }

        result = validate_data.handler(event, MagicMock())

        assert result["statusCode"] == 200
        assert result["validation_passed"] is True
        body = json.loads(result["body"])
        assert body["valid"] is True
        assert body["error_count"] == 0

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    def test_player_count_mismatch_warning(self, mock_load, mock_save):
        """Test warning when player counts differ by more than 2.5%."""

        def mock_load_side_effect(s3_key):
            if "stats" in s3_key:
                # 400 per-game, 385 advanced = 3.75% difference (should warn)
                per_game = [{"Player": f"Player {i}", "PTS": 15.0} for i in range(400)]
                advanced = [{"Player": f"Player {i}", "PER": 15.0} for i in range(385)]
                return {
                    "season": "2025-26",
                    "fetch_timestamp": datetime.utcnow().isoformat(),
                    "source": "basketball_reference",
                    "per_game_stats": per_game,
                    "advanced_stats": advanced,
                    "per_game_columns": [
                        "Player",
                        "Pos",
                        "Age",
                        "Team",
                        "G",
                        "MP",
                        "PTS",
                        "TRB",
                        "AST",
                    ],
                    "advanced_columns": ["Player", "Pos", "Age", "Team", "G", "MP", "PER"],
                }
            return None

        mock_load.side_effect = mock_load_side_effect

        event = {
            "data_location": {"partition": "year=2026/month=02/day=19"},
            "fetch_type": "stats_only",
        }

        result = validate_data.handler(event, MagicMock())

        body = json.loads(result["body"])
        assert body["warning_count"] > 0
        # Should still pass validation (warning, not error)
        assert result["statusCode"] == 200

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    def test_player_count_within_threshold(self, mock_load, mock_save):
        """Test no warning when player counts are within 2.5%."""

        def mock_load_side_effect(s3_key):
            if "stats" in s3_key:
                # 400 per-game, 395 advanced = 1.25% difference (no warning)
                per_game = [{"Player": f"Player {i}", "PTS": 15.0} for i in range(400)]
                advanced = [{"Player": f"Player {i}", "PER": 15.0} for i in range(395)]
                return {
                    "season": "2025-26",
                    "fetch_timestamp": datetime.utcnow().isoformat(),
                    "source": "basketball_reference",
                    "per_game_stats": per_game,
                    "advanced_stats": advanced,
                    "per_game_columns": [
                        "Player",
                        "Pos",
                        "Age",
                        "Team",
                        "G",
                        "MP",
                        "PTS",
                        "TRB",
                        "AST",
                    ],
                    "advanced_columns": ["Player", "Pos", "Age", "Team", "G", "MP", "PER"],
                }
            return None

        mock_load.side_effect = mock_load_side_effect

        event = {
            "data_location": {"partition": "year=2026/month=02/day=19"},
            "fetch_type": "stats_only",
        }

        result = validate_data.handler(event, MagicMock())

        # Check validation report to ensure no mismatch warning
        assert "Player count mismatch" not in str(mock_save.call_args)
        assert result["statusCode"] == 200

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    def test_missing_expected_columns(self, mock_load, mock_save):
        """Test error when expected columns are missing."""

        def mock_load_side_effect(s3_key):
            if "stats" in s3_key:
                per_game = [{"Player": "Test Player", "PTS": 15.0}]
                advanced = [{"Player": "Test Player", "PER": 15.0}]
                return {
                    "season": "2025-26",
                    "fetch_timestamp": datetime.utcnow().isoformat(),
                    "source": "basketball_reference",
                    "per_game_stats": per_game,
                    "advanced_stats": advanced,
                    # Missing key columns like "TRB", "AST"
                    "per_game_columns": ["Player", "PTS"],
                    # Missing "PER"
                    "advanced_columns": ["Player"],
                }
            return None

        mock_load.side_effect = mock_load_side_effect

        event = {
            "data_location": {"partition": "year=2026/month=02/day=19"},
            "fetch_type": "stats_only",
        }

        result = validate_data.handler(event, MagicMock())

        assert result["statusCode"] == 422
        assert result["validation_passed"] is False
        body = json.loads(result["body"])
        assert body["valid"] is False
        assert body["error_count"] > 0

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    def test_unrealistic_stat_values(self, mock_load, mock_save):
        """Test warning for unrealistic stat values."""

        def mock_load_side_effect(s3_key):
            if "stats" in s3_key:
                # Create 350 players, with first one having unrealistic stats
                per_game = [
                    {
                        "Player": "Test Player 1",
                        "Pos": "PG",
                        "Age": 25,
                        "Team": "LAL",
                        "G": 70,
                        "MP": 30.0,
                        "PTS": 150.0,  # Unrealistic - over 80
                        "TRB": 5.0,
                        "AST": 5.0,
                        "FG%": 2.5,  # Unrealistic - over 1.0
                    }
                ]
                # Add normal players to reach 350
                per_game.extend(
                    [
                        {
                            "Player": f"Player {i}",
                            "Pos": "PG",
                            "Age": 25,
                            "Team": "LAL",
                            "G": 70,
                            "MP": 30.0,
                            "PTS": 15.0,
                            "TRB": 5.0,
                            "AST": 5.0,
                            "FG%": 0.45,
                        }
                        for i in range(2, 351)
                    ]
                )
                # Create advanced stats with matching count
                advanced = [
                    {
                        "Player": "Test Player 1",
                        "Pos": "PG",
                        "Age": 25,
                        "Team": "LAL",
                        "G": 70,
                        "MP": 30.0,
                        "PER": 15.0,
                    }
                ]
                advanced.extend(
                    [
                        {
                            "Player": f"Player {i}",
                            "Pos": "PG",
                            "Age": 25,
                            "Team": "LAL",
                            "G": 70,
                            "MP": 30.0,
                            "PER": 15.0,
                        }
                        for i in range(2, 351)
                    ]
                )
                return {
                    "season": "2025-26",
                    "fetch_timestamp": datetime.utcnow().isoformat(),
                    "source": "basketball_reference",
                    "per_game_stats": per_game,
                    "advanced_stats": advanced,
                    "per_game_columns": [
                        "Player",
                        "Pos",
                        "Age",
                        "Team",
                        "G",
                        "MP",
                        "PTS",
                        "TRB",
                        "AST",
                        "FG%",
                    ],
                    "advanced_columns": ["Player", "Pos", "Age", "Team", "G", "MP", "PER"],
                }
            return None

        mock_load.side_effect = mock_load_side_effect

        event = {
            "data_location": {"partition": "year=2026/month=02/day=19"},
            "fetch_type": "stats_only",
        }

        result = validate_data.handler(event, MagicMock())

        body = json.loads(result["body"])
        assert body["warning_count"] > 0
        # Should still pass validation (warning, not error)
        assert result["statusCode"] == 200

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    def test_low_player_count_warning(self, mock_load, mock_save):
        """Test warning when player count is below 300."""

        def mock_load_side_effect(s3_key):
            if "stats" in s3_key:
                # Only 100 players - should warn
                per_game = [{"Player": f"Player {i}", "PTS": 15.0} for i in range(100)]
                advanced = [{"Player": f"Player {i}", "PER": 15.0} for i in range(100)]
                return {
                    "season": "2025-26",
                    "fetch_timestamp": datetime.utcnow().isoformat(),
                    "source": "basketball_reference",
                    "per_game_stats": per_game,
                    "advanced_stats": advanced,
                    "per_game_columns": [
                        "Player",
                        "Pos",
                        "Age",
                        "Team",
                        "G",
                        "MP",
                        "PTS",
                        "TRB",
                        "AST",
                    ],
                    "advanced_columns": ["Player", "Pos", "Age", "Team", "G", "MP", "PER"],
                }
            return None

        mock_load.side_effect = mock_load_side_effect

        event = {
            "data_location": {"partition": "year=2026/month=02/day=19"},
            "fetch_type": "stats_only",
        }

        result = validate_data.handler(event, MagicMock())

        body = json.loads(result["body"])
        assert body["warning_count"] >= 2  # Should warn for both per_game and advanced
        # Should still pass validation (warning, not error)
        assert result["statusCode"] == 200

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    def test_wrong_source_field(self, mock_load, mock_save):
        """Test validation fails when source is not basketball_reference."""

        def mock_load_side_effect(s3_key):
            if "stats" in s3_key:
                return {
                    "season": "2025-26",
                    "fetch_timestamp": datetime.utcnow().isoformat(),
                    "source": "nba_api",  # Wrong source
                    "per_game_stats": [],
                    "advanced_stats": [],
                    "per_game_columns": [],
                    "advanced_columns": [],
                }
            return None

        mock_load.side_effect = mock_load_side_effect

        event = {
            "data_location": {"partition": "year=2026/month=02/day=19"},
            "fetch_type": "stats_only",
        }

        result = validate_data.handler(event, MagicMock())

        assert result["statusCode"] == 422
        assert result["validation_passed"] is False
        body = json.loads(result["body"])
        assert body["valid"] is False

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    def test_valid_null_percentages_when_attempts_zero(self, mock_load, mock_save):
        """Test that null percentages are valid when corresponding attempts are zero."""

        def mock_load_side_effect(s3_key):
            if "stats" in s3_key:
                # Create players with 0 attempts and null percentages - should be valid
                per_game = [
                    {
                        "Player": f"Player {i}",
                        "Pos": "C",
                        "Age": 25,
                        "Team": "LAL",
                        "G": 10,
                        "MP": 15.0,
                        "PTS": 5.0,
                        "TRB": 3.0,
                        "AST": 1.0,
                        "FGA": 0,  # Zero attempts
                        "FG%": None,  # Null percentage - should be valid
                        "3PA": 0,  # Zero 3-point attempts
                        "3P%": None,  # Null percentage - should be valid
                        "FTA": 0,  # Zero free throw attempts
                        "FT%": None,  # Null percentage - should be valid
                        "2PA": 0,
                        "2P%": None,
                    }
                    for i in range(350)
                ]
                advanced = [
                    {
                        "Player": f"Player {i}",
                        "Pos": "C",
                        "Age": 25,
                        "Team": "LAL",
                        "G": 10,
                        "MP": 15.0,
                        "PER": 10.0,
                        "FGA": 0,
                        "FTA": 0,
                        "TS%": None,  # Null when no shooting attempts
                        "3PAr": None,  # Null when no FGA
                        "FTr": None,  # Null when no FGA
                    }
                    for i in range(350)
                ]
                return {
                    "season": "2025-26",
                    "fetch_timestamp": datetime.utcnow().isoformat(),
                    "source": "basketball_reference",
                    "per_game_stats": per_game,
                    "advanced_stats": advanced,
                    "per_game_columns": [
                        "Player",
                        "Pos",
                        "Age",
                        "Team",
                        "G",
                        "MP",
                        "PTS",
                        "TRB",
                        "AST",
                        "FGA",
                        "FG%",
                        "3PA",
                        "3P%",
                        "FTA",
                        "FT%",
                        "2PA",
                        "2P%",
                    ],
                    "advanced_columns": [
                        "Player",
                        "Pos",
                        "Age",
                        "Team",
                        "G",
                        "MP",
                        "PER",
                        "FGA",
                        "FTA",
                        "TS%",
                        "3PAr",
                        "FTr",
                    ],
                }
            return None

        mock_load.side_effect = mock_load_side_effect

        event = {
            "data_location": {"partition": "year=2026/month=02/day=19"},
            "fetch_type": "stats_only",
        }

        result = validate_data.handler(event, MagicMock())

        assert result["statusCode"] == 200
        assert result["validation_passed"] is True
        body = json.loads(result["body"])
        assert body["valid"] is True
        assert body["error_count"] == 0

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    def test_invalid_null_percentages_when_attempts_nonzero(self, mock_load, mock_save):
        """Test that null percentages are invalid when corresponding attempts are non-zero."""

        def mock_load_side_effect(s3_key):
            if "stats" in s3_key:
                # Create players with non-zero attempts but null percentages - should be invalid
                per_game = [
                    {
                        "Player": "Bad Player 1",
                        "Pos": "PG",
                        "Age": 25,
                        "Team": "LAL",
                        "G": 70,
                        "MP": 30.0,
                        "PTS": 15.0,
                        "TRB": 5.0,
                        "AST": 5.0,
                        "FGA": 10.5,  # Non-zero attempts
                        "FG%": None,  # Null percentage - INVALID
                        "3PA": 5.0,  # Non-zero 3-point attempts
                        "3P%": None,  # Null percentage - INVALID
                        "FTA": 4.0,  # Non-zero free throw attempts
                        "FT%": None,  # Null percentage - INVALID
                        "2PA": 5.5,
                        "2P%": None,
                    }
                ]
                # Add enough normal players to reach 350
                per_game.extend(
                    [
                        {
                            "Player": f"Player {i}",
                            "Pos": "PG",
                            "Age": 25,
                            "Team": "LAL",
                            "G": 70,
                            "MP": 30.0,
                            "PTS": 15.0,
                            "TRB": 5.0,
                            "AST": 5.0,
                            "FGA": 10.0,
                            "FG%": 0.45,
                            "3PA": 5.0,
                            "3P%": 0.35,
                            "FTA": 4.0,
                            "FT%": 0.80,
                            "2PA": 5.0,
                            "2P%": 0.50,
                        }
                        for i in range(349)
                    ]
                )
                advanced = [
                    {
                        "Player": "Bad Player 1",
                        "Pos": "PG",
                        "Age": 25,
                        "Team": "LAL",
                        "G": 70,
                        "MP": 30.0,
                        "PER": 15.0,
                        "FGA": 10.5,
                        "FTA": 4.0,
                        "TS%": None,  # INVALID - has attempts
                        "3PAr": None,  # INVALID - has FGA
                        "FTr": None,  # INVALID - has FGA
                    }
                ]
                advanced.extend(
                    [
                        {
                            "Player": f"Player {i}",
                            "Pos": "PG",
                            "Age": 25,
                            "Team": "LAL",
                            "G": 70,
                            "MP": 30.0,
                            "PER": 15.0,
                            "FGA": 10.0,
                            "FTA": 4.0,
                            "TS%": 0.55,
                            "3PAr": 0.30,
                            "FTr": 0.25,
                        }
                        for i in range(349)
                    ]
                )
                return {
                    "season": "2025-26",
                    "fetch_timestamp": datetime.utcnow().isoformat(),
                    "source": "basketball_reference",
                    "per_game_stats": per_game,
                    "advanced_stats": advanced,
                    "per_game_columns": [
                        "Player",
                        "Pos",
                        "Age",
                        "Team",
                        "G",
                        "MP",
                        "PTS",
                        "TRB",
                        "AST",
                        "FGA",
                        "FG%",
                        "3PA",
                        "3P%",
                        "FTA",
                        "FT%",
                        "2PA",
                        "2P%",
                    ],
                    "advanced_columns": [
                        "Player",
                        "Pos",
                        "Age",
                        "Team",
                        "G",
                        "MP",
                        "PER",
                        "FGA",
                        "FTA",
                        "TS%",
                        "3PAr",
                        "FTr",
                    ],
                }
            return None

        mock_load.side_effect = mock_load_side_effect

        event = {
            "data_location": {"partition": "year=2026/month=02/day=19"},
            "fetch_type": "stats_only",
        }

        result = validate_data.handler(event, MagicMock())

        assert result["statusCode"] == 422
        assert result["validation_passed"] is False
        body = json.loads(result["body"])
        assert body["valid"] is False
        assert body["error_count"] > 0

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    def test_mixed_null_and_zero_attempts(self, mock_load, mock_save):
        """Test players with some zero attempts (null %) and some non-zero attempts (valid %)."""

        def mock_load_side_effect(s3_key):
            if "stats" in s3_key:
                # Create players who shoot FGs but not 3Ps
                per_game = [
                    {
                        "Player": f"Player {i}",
                        "Pos": "C",
                        "Age": 28,
                        "Team": "LAL",
                        "G": 60,
                        "MP": 25.0,
                        "PTS": 12.0,
                        "TRB": 8.0,
                        "AST": 2.0,
                        "FGA": 8.0,  # Takes field goals
                        "FG%": 0.55,  # Has FG% - required
                        "3PA": 0,  # Doesn't take 3s
                        "3P%": None,  # Null 3P% is valid
                        "FTA": 3.0,  # Takes free throws
                        "FT%": 0.70,  # Has FT% - required
                        "2PA": 8.0,
                        "2P%": 0.55,
                    }
                    for i in range(350)
                ]
                advanced = [
                    {
                        "Player": f"Player {i}",
                        "Pos": "C",
                        "Age": 28,
                        "Team": "LAL",
                        "G": 60,
                        "MP": 25.0,
                        "PER": 18.0,
                        "FGA": 8.0,
                        "FTA": 3.0,
                        "TS%": 0.58,  # Required when has attempts
                        "3PAr": 0.0,  # Can be 0 when has FGA
                        "FTr": 0.375,  # Required when has FGA
                    }
                    for i in range(350)
                ]
                return {
                    "season": "2025-26",
                    "fetch_timestamp": datetime.utcnow().isoformat(),
                    "source": "basketball_reference",
                    "per_game_stats": per_game,
                    "advanced_stats": advanced,
                    "per_game_columns": [
                        "Player",
                        "Pos",
                        "Age",
                        "Team",
                        "G",
                        "MP",
                        "PTS",
                        "TRB",
                        "AST",
                        "FGA",
                        "FG%",
                        "3PA",
                        "3P%",
                        "FTA",
                        "FT%",
                        "2PA",
                        "2P%",
                    ],
                    "advanced_columns": [
                        "Player",
                        "Pos",
                        "Age",
                        "Team",
                        "G",
                        "MP",
                        "PER",
                        "FGA",
                        "FTA",
                        "TS%",
                        "3PAr",
                        "FTr",
                    ],
                }
            return None

        mock_load.side_effect = mock_load_side_effect

        event = {
            "data_location": {"partition": "year=2026/month=02/day=19"},
            "fetch_type": "stats_only",
        }

        result = validate_data.handler(event, MagicMock())

        assert result["statusCode"] == 200
        assert result["validation_passed"] is True
        body = json.loads(result["body"])
        assert body["valid"] is True
        assert body["error_count"] == 0


class TestNaNHandling:
    """Test NaN value handling in validation."""

    def test_is_nan_with_float_nan(self):
        """Test _is_nan correctly identifies float('nan')."""
        assert validate_data._is_nan(float("nan")) is True

    def test_is_nan_with_regular_float(self):
        """Test _is_nan returns False for regular float values."""
        assert validate_data._is_nan(5.0) is False

    def test_is_nan_with_none(self):
        """Test _is_nan returns False for None."""
        assert validate_data._is_nan(None) is False

    def test_is_nan_with_string(self):
        """Test _is_nan handles string 'NaN'."""
        assert validate_data._is_nan("NaN") is True

    def test_is_value_zero_or_null_with_nan(self):
        """Test _is_value_zero_or_null handles NaN."""
        assert validate_data._is_value_zero_or_null(float("nan")) is True

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    def test_nan_in_percentage_allowed_when_attempts_zero(self, mock_load, mock_save):
        """Test NaN in percentage columns is valid when attempts are 0."""

        def mock_load_side_effect(s3_key):
            if "stats" in s3_key:
                per_game = [
                    {
                        "Player": f"Player {i}",
                        "Pos": "C",
                        "Age": 25,
                        "Team": "LAL",
                        "G": 10,
                        "MP": 15.0,
                        "PTS": 5.0,
                        "TRB": 3.0,
                        "AST": 1.0,
                        "FGA": 0,
                        "FG%": float("nan"),
                    }
                    for i in range(350)
                ]
                advanced = [
                    {
                        "Player": f"Player {i}",
                        "Pos": "C",
                        "Age": 25,
                        "Team": "LAL",
                        "G": 10,
                        "MP": 15.0,
                        "PER": 10.0,
                    }
                    for i in range(350)
                ]
                return {
                    "season": "2025-26",
                    "fetch_timestamp": datetime.utcnow().isoformat(),
                    "source": "basketball_reference",
                    "per_game_stats": per_game,
                    "advanced_stats": advanced,
                    "per_game_columns": [
                        "Player",
                        "Pos",
                        "Age",
                        "Team",
                        "G",
                        "MP",
                        "PTS",
                        "TRB",
                        "AST",
                        "FGA",
                        "FG%",
                    ],
                    "advanced_columns": ["Player", "Pos", "Age", "Team", "G", "MP", "PER"],
                }
            return None

        mock_load.side_effect = mock_load_side_effect
        event = {
            "data_location": {"partition": "year=2026/month=02/day=19"},
            "fetch_type": "stats_only",
        }
        result = validate_data.handler(event, MagicMock())
        assert result["statusCode"] == 200

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    def test_nan_in_percentage_invalid_when_attempts_nonzero(self, mock_load, mock_save):
        """Test NaN in percentage columns is invalid when attempts > 0."""

        def mock_load_side_effect(s3_key):
            if "stats" in s3_key:
                per_game = [
                    {
                        "Player": "Bad Player",
                        "Pos": "PG",
                        "Age": 25,
                        "Team": "LAL",
                        "G": 70,
                        "MP": 30.0,
                        "PTS": 15.0,
                        "TRB": 5.0,
                        "AST": 5.0,
                        "FGA": 10.5,
                        "FG%": float("nan"),
                    }
                ]
                per_game.extend(
                    [
                        {
                            "Player": f"Player {i}",
                            "Pos": "PG",
                            "Age": 25,
                            "Team": "LAL",
                            "G": 70,
                            "MP": 30.0,
                            "PTS": 15.0,
                            "TRB": 5.0,
                            "AST": 5.0,
                            "FGA": 10.0,
                            "FG%": 0.45,
                        }
                        for i in range(349)
                    ]
                )
                advanced = [
                    {
                        "Player": f"Player {i}",
                        "Pos": "PG",
                        "Age": 25,
                        "Team": "LAL",
                        "G": 70,
                        "MP": 30.0,
                        "PER": 15.0,
                    }
                    for i in range(350)
                ]
                return {
                    "season": "2025-26",
                    "fetch_timestamp": datetime.utcnow().isoformat(),
                    "source": "basketball_reference",
                    "per_game_stats": per_game,
                    "advanced_stats": advanced,
                    "per_game_columns": [
                        "Player",
                        "Pos",
                        "Age",
                        "Team",
                        "G",
                        "MP",
                        "PTS",
                        "TRB",
                        "AST",
                        "FGA",
                        "FG%",
                    ],
                    "advanced_columns": ["Player", "Pos", "Age", "Team", "G", "MP", "PER"],
                }
            return None

        mock_load.side_effect = mock_load_side_effect
        event = {
            "data_location": {"partition": "year=2026/month=02/day=19"},
            "fetch_type": "stats_only",
        }
        result = validate_data.handler(event, MagicMock())
        assert result["statusCode"] == 422


class TestLeagueAverageAndAwardsExclusion:
    """Test that League Average rows and Awards columns are excluded from validation."""

    def test_league_average_row_excluded_from_validation(self):
        """Test that League Average rows with missing critical data don't fail validation."""
        stats_data = {
            "season": "2025-26",
            "fetch_timestamp": datetime.utcnow().isoformat(),
            "source": "basketball_reference",
            "per_game_stats": [
                {
                    "Player": "LeBron James",
                    "Pos": "SF",
                    "Age": 39,
                    "Team": "LAL",
                    "G": 50,
                    "MP": 35.0,
                    "PTS": 25.0,
                    "TRB": 8.0,
                    "AST": 7.0,
                },
                {
                    "Player": "League Average",
                    "Pos": None,
                    "Age": None,
                    "Team": None,
                    "G": None,
                    "MP": 24.0,
                    "PTS": 12.5,
                    "TRB": 5.0,
                    "AST": 3.0,
                },
            ],
            "advanced_stats": [
                {
                    "Player": "LeBron James",
                    "Pos": "SF",
                    "Age": 39,
                    "Team": "LAL",
                    "G": 50,
                    "MP": 35.0,
                    "PER": 25.0,
                },
                {
                    "Player": "League Average",
                    "Pos": None,
                    "Age": None,
                    "Team": None,
                    "G": None,
                    "MP": 24.0,
                    "PER": 15.0,
                },
            ],
            "per_game_columns": ["Player", "Pos", "Age", "Team", "G", "MP", "PTS", "TRB", "AST"],
            "advanced_columns": ["Player", "Pos", "Age", "Team", "G", "MP", "PER"],
        }

        result = validate_data.validate_stats_data(stats_data)

        assert result["valid"] is True
        assert result["errors"] == []

    def test_awards_column_excluded_from_validation(self):
        """Test that null/NaN values in Awards column don't generate warnings."""
        stats_data = {
            "season": "2025-26",
            "fetch_timestamp": datetime.utcnow().isoformat(),
            "source": "basketball_reference",
            "per_game_stats": [
                {
                    "Player": "LeBron James",
                    "Pos": "SF",
                    "Age": 39,
                    "Team": "LAL",
                    "G": 50,
                    "MP": 35.0,
                    "PTS": 25.0,
                    "TRB": 8.0,
                    "AST": 7.0,
                    "Awards": None,
                },
                {
                    "Player": "Nikola Jokic",
                    "Pos": "C",
                    "Age": 29,
                    "Team": "DEN",
                    "G": 55,
                    "MP": 36.0,
                    "PTS": 28.0,
                    "TRB": 13.0,
                    "AST": 9.0,
                    "Awards": None,
                },
            ],
            "advanced_stats": [
                {
                    "Player": "LeBron James",
                    "Pos": "SF",
                    "Age": 39,
                    "Team": "LAL",
                    "G": 50,
                    "MP": 35.0,
                    "PER": 25.0,
                    "Awards": None,
                },
                {
                    "Player": "Nikola Jokic",
                    "Pos": "C",
                    "Age": 29,
                    "Team": "DEN",
                    "G": 55,
                    "MP": 36.0,
                    "PER": 30.0,
                    "Awards": None,
                },
            ],
            "per_game_columns": [
                "Player",
                "Pos",
                "Age",
                "Team",
                "G",
                "MP",
                "PTS",
                "TRB",
                "AST",
                "Awards",
            ],
            "advanced_columns": ["Player", "Pos", "Age", "Team", "G", "MP", "PER", "Awards"],
        }

        result = validate_data.validate_stats_data(stats_data)

        assert result["valid"] is True
        assert result["errors"] == []
        awards_warnings = [w for w in result["warnings"] if "Awards" in w]
        assert len(awards_warnings) == 0

    def test_league_average_and_awards_together(self):
        """Test that both League Average and Awards are properly handled together."""
        stats_data = {
            "season": "2025-26",
            "fetch_timestamp": datetime.utcnow().isoformat(),
            "source": "basketball_reference",
            "per_game_stats": [
                {
                    "Player": "LeBron James",
                    "Pos": "SF",
                    "Age": 39,
                    "Team": "LAL",
                    "G": 50,
                    "MP": 35.0,
                    "PTS": 25.0,
                    "TRB": 8.0,
                    "AST": 7.0,
                    "Awards": None,
                },
                {
                    "Player": "League Average",
                    "Pos": None,
                    "Age": None,
                    "Team": None,
                    "G": None,
                    "MP": 24.0,
                    "PTS": 12.5,
                    "TRB": 5.0,
                    "AST": 3.0,
                    "Awards": None,
                },
            ],
            "advanced_stats": [
                {
                    "Player": "LeBron James",
                    "Pos": "SF",
                    "Age": 39,
                    "Team": "LAL",
                    "G": 50,
                    "MP": 35.0,
                    "PER": 25.0,
                    "Awards": None,
                },
                {
                    "Player": "League Average",
                    "Pos": None,
                    "Age": None,
                    "Team": None,
                    "G": None,
                    "MP": 24.0,
                    "PER": 15.0,
                    "Awards": None,
                },
            ],
            "per_game_columns": [
                "Player",
                "Pos",
                "Age",
                "Team",
                "G",
                "MP",
                "PTS",
                "TRB",
                "AST",
                "Awards",
            ],
            "advanced_columns": ["Player", "Pos", "Age", "Team", "G", "MP", "PER", "Awards"],
        }

        result = validate_data.validate_stats_data(stats_data)

        assert result["valid"] is True
        assert result["errors"] == []
        awards_warnings = [w for w in result["warnings"] if "Awards" in w]
        assert len(awards_warnings) == 0
