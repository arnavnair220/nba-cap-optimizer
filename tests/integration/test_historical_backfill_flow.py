"""
Integration tests for historical data backfill end-to-end flow.

Tests the complete pipeline with historical seasons:
- Fetch historical data (2022-23, 2023-24, 2024-25)
- Validate historical data
- Transform historical data
- Load to RDS with correct season

These tests verify the entire backfill workflow.
"""

import json
from unittest.mock import MagicMock, Mock, patch

import pandas as pd

from src.etl import fetch_data, transform_data, validate_data
from tests.integration.conftest import (
    create_basketball_reference_advanced_stats,
    create_basketball_reference_player_stats,
)


class TestHistoricalBackfillPipeline:
    """Test complete backfill pipeline for historical seasons."""

    @patch("src.etl.fetch_data.pd.read_html")
    @patch("src.etl.fetch_data.requests.get")
    @patch("src.etl.fetch_data.time.sleep")
    @patch("src.etl.fetch_data.save_to_s3")
    @patch("src.etl.validate_data.load_from_s3")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.transform_data.load_from_s3")
    @patch("src.etl.transform_data.save_to_s3")
    @patch("src.etl.fetch_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.fetch_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.transform_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.transform_data.ENVIRONMENT", "test")
    def test_backfill_2022_23_season_complete_pipeline(
        self,
        mock_transform_save,
        mock_transform_load,
        mock_validate_save_report,
        mock_validate_load,
        mock_fetch_save,
        mock_sleep,
        mock_requests_get,
        mock_read_html,
    ):
        """
        Test complete backfill for 2022-23 season:
        Fetch (with 2023 URLs) → Validate → Transform.
        """
        # Mock Basketball Reference stats for 2022-23 season
        per_game_stats = [
            create_basketball_reference_player_stats(
                "LeBron James", "SF", 38, "LAL", 56, 28.9, 8.3, 6.8
            ),
            create_basketball_reference_player_stats(
                "Giannis Antetokounmpo", "PF", 28, "MIL", 63, 31.1, 11.8, 5.7
            ),
        ] + [
            create_basketball_reference_player_stats(
                f"Player {i}", "PG", 25, "LAL", 60, 15.0, 5.0, 4.0
            )
            for i in range(348)
        ]
        mock_pergame_df = pd.DataFrame(per_game_stats)

        advanced_stats = [
            create_basketball_reference_advanced_stats(
                "LeBron James", "SF", 38, "LAL", 56, 26.3, 4.5
            ),
            create_basketball_reference_advanced_stats(
                "Giannis Antetokounmpo", "PF", 28, "MIL", 63, 29.4, 7.2
            ),
        ] + [
            create_basketball_reference_advanced_stats(
                f"Player {i}", "PG", 25, "LAL", 60, 15.0, 2.0
            )
            for i in range(348)
        ]
        mock_advanced_df = pd.DataFrame(advanced_stats)
        mock_read_html.side_effect = [[mock_pergame_df], [mock_advanced_df]]

        # Mock ESPN salaries (not available for historical, will be empty)
        mock_salary_response = Mock()
        mock_salary_response.status_code = 200
        mock_salary_response.content = "<html><table></table></html>"
        mock_requests_get.return_value = mock_salary_response

        # Simulate S3 storage
        s3_storage = {}

        def save_impl(data, key):
            s3_storage[key] = data
            return True

        def load_impl(key):
            return s3_storage.get(key)

        mock_fetch_save.side_effect = save_impl
        mock_validate_load.side_effect = load_impl
        mock_transform_load.side_effect = load_impl
        mock_transform_save.side_effect = save_impl

        # Stage 1: Fetch data for 2022-23
        fetch_result = fetch_data.handler(
            {"fetch_type": "stats_only", "season": "2022-23"}, MagicMock()
        )

        assert fetch_result["statusCode"] == 200
        assert fetch_result["season"] == "2022-23"

        # Verify Basketball Reference was called with 2023 URLs
        assert mock_read_html.call_count == 2
        pergame_url = mock_read_html.call_args_list[0][0][0]
        assert "NBA_2023" in pergame_url

        # Stage 2: Validate
        validate_result = validate_data.handler(
            {
                "data_location": fetch_result["data_location"],
                "season": fetch_result["season"],
                "fetch_type": "stats_only",
            },
            MagicMock(),
        )

        assert validate_result["statusCode"] == 200
        assert validate_result["validation_passed"] is True
        assert validate_result["season"] == "2022-23"

        # Stage 3: Transform
        transform_result = transform_data.handler(
            {
                "validation_passed": validate_result["validation_passed"],
                "data_location": validate_result["data_location"],
                "season": validate_result["season"],
            },
            MagicMock(),
        )

        assert transform_result["statusCode"] == 200
        assert transform_result["transformation_successful"] is True
        assert transform_result["season"] == "2022-23"

        # Verify the transformed data has correct season
        stats_key = [k for k in s3_storage.keys() if "enriched_player_stats" in k][0]
        enriched_stats = s3_storage[stats_key]
        assert enriched_stats["season"] == "2022-23"

    @patch("src.etl.fetch_data.pd.read_html")
    @patch("src.etl.fetch_data.requests.get")
    @patch("src.etl.fetch_data.time.sleep")
    @patch("src.etl.fetch_data.save_to_s3")
    @patch("src.etl.validate_data.load_from_s3")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.transform_data.load_from_s3")
    @patch("src.etl.transform_data.save_to_s3")
    @patch("src.etl.fetch_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.fetch_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.transform_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.transform_data.ENVIRONMENT", "test")
    def test_season_parameter_flows_through_all_stages(
        self,
        mock_transform_save,
        mock_transform_load,
        mock_validate_save_report,
        mock_validate_load,
        mock_fetch_save,
        mock_sleep,
        mock_requests_get,
        mock_read_html,
    ):
        """
        Test that season parameter is correctly passed through all pipeline stages.
        """
        # Mock data
        per_game_stats = [
            create_basketball_reference_player_stats(
                f"Player {i}", "PG", 25, "LAL", 60, 15.0, 5.0, 4.0
            )
            for i in range(350)
        ]
        mock_pergame_df = pd.DataFrame(per_game_stats)
        advanced_stats = [
            create_basketball_reference_advanced_stats(
                f"Player {i}", "PG", 25, "LAL", 60, 15.0, 2.0
            )
            for i in range(350)
        ]
        mock_advanced_df = pd.DataFrame(advanced_stats)
        mock_read_html.side_effect = [[mock_pergame_df], [mock_advanced_df]]

        mock_salary_response = Mock()
        mock_salary_response.status_code = 200
        mock_salary_response.content = "<html><table></table></html>"
        mock_requests_get.return_value = mock_salary_response

        # Simulate S3 storage
        s3_storage = {}

        def save_impl(data, key):
            s3_storage[key] = data
            return True

        def load_impl(key):
            return s3_storage.get(key)

        mock_fetch_save.side_effect = save_impl
        mock_validate_load.side_effect = load_impl
        mock_transform_load.side_effect = load_impl
        mock_transform_save.side_effect = save_impl

        test_season = "2023-24"

        # Fetch
        fetch_result = fetch_data.handler(
            {"fetch_type": "stats_only", "season": test_season}, MagicMock()
        )
        assert fetch_result["season"] == test_season

        # Validate
        validate_result = validate_data.handler(
            {
                "data_location": fetch_result["data_location"],
                "season": fetch_result["season"],
                "fetch_type": "stats_only",
            },
            MagicMock(),
        )
        assert validate_result["season"] == test_season

        # Transform
        transform_result = transform_data.handler(
            {
                "validation_passed": validate_result["validation_passed"],
                "data_location": validate_result["data_location"],
                "season": validate_result["season"],
            },
            MagicMock(),
        )
        assert transform_result["season"] == test_season

        # Verify enriched stats contain correct season metadata
        stats_key = [k for k in s3_storage.keys() if "enriched_player_stats" in k][0]
        enriched_stats = s3_storage[stats_key]
        assert enriched_stats["season"] == test_season


class TestMultipleSeasonBackfill:
    """Test backfilling multiple seasons sequentially."""

    @patch("src.etl.fetch_data.pd.read_html")
    @patch("src.etl.fetch_data.requests.get")
    @patch("src.etl.fetch_data.time.sleep")
    @patch("src.etl.fetch_data.save_to_s3")
    @patch("src.etl.validate_data.load_from_s3")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.fetch_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.fetch_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    def test_backfill_three_seasons_independently(
        self,
        mock_validate_save_report,
        mock_validate_load,
        mock_fetch_save,
        mock_sleep,
        mock_requests_get,
        mock_read_html,
    ):
        """
        Test that three different seasons can be backfilled without conflicts.
        Simulates running backfill three times with different seasons.
        """
        seasons = ["2022-23", "2023-24", "2024-25"]
        results = {}

        for season in seasons:
            # Mock data for this season
            per_game_stats = [
                create_basketball_reference_player_stats(
                    f"Player {i} Season {season}", "PG", 25, "LAL", 60, 15.0, 5.0, 4.0
                )
                for i in range(350)
            ]
            mock_pergame_df = pd.DataFrame(per_game_stats)
            advanced_stats = [
                create_basketball_reference_advanced_stats(
                    f"Player {i} Season {season}", "PG", 25, "LAL", 60, 15.0, 2.0
                )
                for i in range(350)
            ]
            mock_advanced_df = pd.DataFrame(advanced_stats)
            mock_read_html.side_effect = [[mock_pergame_df], [mock_advanced_df]]

            mock_salary_response = Mock()
            mock_salary_response.status_code = 200
            mock_salary_response.content = "<html><table></table></html>"
            mock_requests_get.return_value = mock_salary_response

            # Simulate S3 storage for this season
            s3_storage = {}

            def save_impl(data, key):
                s3_storage[key] = data
                return True

            def load_impl(key):
                return s3_storage.get(key)

            mock_fetch_save.side_effect = save_impl
            mock_validate_load.side_effect = load_impl

            # Fetch for this season
            fetch_result = fetch_data.handler(
                {"fetch_type": "stats_only", "season": season}, MagicMock()
            )

            assert fetch_result["statusCode"] == 200
            assert fetch_result["season"] == season

            # Validate for this season
            validate_result = validate_data.handler(
                {
                    "data_location": fetch_result["data_location"],
                    "season": fetch_result["season"],
                    "fetch_type": "stats_only",
                },
                MagicMock(),
            )

            assert validate_result["validation_passed"] is True
            assert validate_result["season"] == season

            results[season] = {
                "fetch": fetch_result,
                "validate": validate_result,
                "storage": s3_storage,
            }

        # Verify all three seasons processed successfully
        assert len(results) == 3
        for season in seasons:
            assert results[season]["fetch"]["season"] == season
            assert results[season]["validate"]["season"] == season


class TestBackfillErrorHandling:
    """Test error handling during backfill operations."""

    @patch("src.etl.fetch_data.fetch_player_stats")
    @patch("src.etl.fetch_data.save_to_s3")
    @patch("src.etl.fetch_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.fetch_data.ENVIRONMENT", "test")
    def test_backfill_handles_missing_historical_data(self, mock_save, mock_fetch_stats):
        """
        Test that backfill gracefully handles when historical data is unavailable.
        E.g., Basketball Reference might not have very old seasons.
        """
        # Simulate missing data for very old season
        mock_fetch_stats.return_value = None
        mock_save.return_value = False

        result = fetch_data.handler({"fetch_type": "stats_only", "season": "1999-00"}, MagicMock())

        # Should return errors
        assert result["statusCode"] == 200  # Handler completes but with errors
        body = json.loads(result["body"])
        assert len(body["errors"]) > 0

    @patch("src.etl.fetch_data.pd.read_html")
    @patch("src.etl.fetch_data.time.sleep")
    @patch("src.etl.fetch_data.save_to_s3")
    @patch("src.etl.validate_data.load_from_s3")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.fetch_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.fetch_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    def test_backfill_validation_catches_data_quality_issues(
        self,
        mock_validate_save_report,
        mock_validate_load,
        mock_save,
        mock_sleep,
        mock_read_html,
    ):
        """
        Test that validation catches data quality issues in historical data.
        E.g., incomplete player stats from old seasons.
        """
        # Mock incomplete historical data (missing required fields)
        mock_pergame_df = pd.DataFrame(
            {"Player": ["Old Player"], "PTS": [20.0]}  # Missing required fields
        )
        mock_advanced_df = pd.DataFrame({"Player": ["Old Player"], "PER": [15.0]})
        mock_read_html.side_effect = [[mock_pergame_df], [mock_advanced_df]]

        s3_storage = {}

        def save_impl(data, key):
            s3_storage[key] = data
            return True

        def load_impl(key):
            return s3_storage.get(key)

        mock_save.side_effect = save_impl
        mock_validate_load.side_effect = load_impl

        # Fetch incomplete data
        fetch_result = fetch_data.handler(
            {"fetch_type": "stats_only", "season": "2010-11"}, MagicMock()
        )

        # Validation should fail
        validate_result = validate_data.handler(
            {
                "data_location": fetch_result["data_location"],
                "season": fetch_result["season"],
                "fetch_type": "stats_only",
            },
            MagicMock(),
        )

        assert validate_result["statusCode"] == 422
        assert validate_result["validation_passed"] is False
