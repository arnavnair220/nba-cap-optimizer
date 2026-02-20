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
                    "players": {"resultSets": [{"headers": [], "rowSet": []}]},
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
                    "players": {"resultSets": [{"headers": [], "rowSet": []}]},
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
                    "players": {
                        "resultSets": [
                            {
                                "headers": ["PLAYER_ID", "PLAYER_NAME"],
                                "rowSet": [[1, "Test Player"]],
                            }
                        ]
                    },
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
