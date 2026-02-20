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
