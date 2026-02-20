"""
Integration tests for validate_data and transform_data Lambda interaction.

These tests focus on the DATA FLOW and CONTRACT between validate and transform lambdas,
NOT on individual function behavior (covered in unit tests).

Integration test scope:
- Event output from validate_data matches input expected by transform_data
- Data validated by validate_data can be successfully transformed by transform_data
- S3 partition paths are consistent between lambdas
- Data quality and statistics remain consistent across stages
"""

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

from src.etl import transform_data, validate_data


class TestValidateTransformEventContract:
    """Test the event contract between validate and transform lambdas."""

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    @patch("src.etl.transform_data.ENVIRONMENT", "test")
    @patch("src.etl.transform_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.transform_data.save_to_s3")
    @patch("src.etl.transform_data.load_from_s3")
    def test_validate_output_event_is_valid_transform_input(
        self,
        mock_transform_load,
        mock_transform_save,
        mock_validate_load,
        mock_validate_save,
        mock_complete_stats_data,
    ):
        """
        Test that the event output structure from validate_data handler
        matches the event input structure expected by transform_data handler.
        """

        # Mock valid data for validation
        def mock_validate_load_impl(s3_key):
            if "stats" in s3_key:
                return mock_complete_stats_data
            return None

        mock_validate_load.side_effect = mock_validate_load_impl
        mock_transform_load.side_effect = mock_validate_load_impl
        mock_transform_save.return_value = True

        # Execute validate
        validate_event = {
            "data_location": {"bucket": "test-bucket", "partition": "year=2025/month=01/day=01"},
            "fetch_type": "stats_only",
        }
        validate_result = validate_data.handler(validate_event, MagicMock())

        # Verify validate returns required fields for transform
        assert validate_result["statusCode"] == 200
        assert "validation_passed" in validate_result
        assert "data_location" in validate_result
        assert validate_result["validation_passed"] is True

        # Execute transform using validate's output
        transform_event = {
            "validation_passed": validate_result["validation_passed"],
            "data_location": validate_result["data_location"],
        }
        transform_result = transform_data.handler(transform_event, MagicMock())

        # Verify transform accepts and processes the event
        assert transform_result["statusCode"] == 200
        assert "transformation_successful" in transform_result
        assert transform_result["transformation_successful"] is True

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    @patch("src.etl.transform_data.ENVIRONMENT", "test")
    @patch("src.etl.transform_data.S3_BUCKET", "test-bucket")
    def test_transform_skips_when_validation_fails(self, mock_validate_load, mock_validate_save):
        """
        Test that transform_data returns 400 and skips processing
        when validation_passed=False from validate_data.
        """

        # Mock invalid data (missing required columns)
        def mock_load_impl(s3_key):
            if "stats" in s3_key:
                return {
                    "season": "2025-26",
                    "fetch_timestamp": datetime.utcnow().isoformat(),
                    "source": "basketball_reference",
                    "per_game_stats": [{"Player": "Test"}],
                    "advanced_stats": [{"Player": "Test"}],
                    "per_game_columns": ["Player"],  # Missing required columns
                    "advanced_columns": ["Player"],
                }
            return None

        mock_validate_load.side_effect = mock_load_impl

        # Execute validate (should fail)
        validate_event = {
            "data_location": {"bucket": "test-bucket", "partition": "year=2025/month=01/day=01"},
            "fetch_type": "stats_only",
        }
        validate_result = validate_data.handler(validate_event, MagicMock())

        assert validate_result["validation_passed"] is False

        # Execute transform with failed validation
        transform_event = {
            "validation_passed": validate_result["validation_passed"],
            "data_location": validate_result["data_location"],
        }
        transform_result = transform_data.handler(transform_event, MagicMock())

        # Verify transform skips processing
        assert transform_result["statusCode"] == 400
        body = json.loads(transform_result["body"])
        assert "validation failed" in body["error"].lower()

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    @patch("src.etl.transform_data.ENVIRONMENT", "test")
    @patch("src.etl.transform_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.transform_data.save_to_s3")
    @patch("src.etl.transform_data.load_from_s3")
    def test_partition_paths_consistent_across_lambdas(
        self,
        mock_transform_load,
        mock_transform_save,
        mock_validate_load,
        mock_validate_save,
        mock_complete_stats_data,
    ):
        """
        Test that both validate and transform use the same partition path
        for loading and saving S3 objects.
        """
        partition = "year=2025/month=01/day=15"
        validate_loaded_keys = []
        transform_loaded_keys = []
        transform_saved_keys = []

        def mock_validate_load_impl(s3_key):
            validate_loaded_keys.append(s3_key)
            if "stats" in s3_key:
                return mock_complete_stats_data
            return None

        def mock_transform_load_impl(s3_key):
            transform_loaded_keys.append(s3_key)
            return mock_validate_load_impl(s3_key)

        def mock_transform_save_impl(data, s3_key):
            transform_saved_keys.append(s3_key)
            return True

        mock_validate_load.side_effect = mock_validate_load_impl
        mock_transform_load.side_effect = mock_transform_load_impl
        mock_transform_save.side_effect = mock_transform_save_impl

        # Execute validate
        validate_event = {
            "data_location": {"bucket": "test-bucket", "partition": partition},
            "fetch_type": "stats_only",
        }
        validate_result = validate_data.handler(validate_event, MagicMock())

        # Verify validate loaded from partition
        assert len(validate_loaded_keys) > 0
        assert all(partition in key for key in validate_loaded_keys)

        # Execute transform
        transform_event = {
            "validation_passed": validate_result["validation_passed"],
            "data_location": validate_result["data_location"],
        }
        transform_data.handler(transform_event, MagicMock())

        # Verify transform loaded from same partition
        assert len(transform_loaded_keys) > 0
        assert all(partition in key for key in transform_loaded_keys)

        # Verify transform saved to same partition
        assert len(transform_saved_keys) > 0
        assert all(partition in key for key in transform_saved_keys)


class TestDataEnrichmentThroughPipeline:
    """Test data enrichment from validate through transform."""

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    @patch("src.etl.transform_data.ENVIRONMENT", "test")
    @patch("src.etl.transform_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.transform_data.save_to_s3")
    @patch("src.etl.transform_data.load_from_s3")
    def test_validate_to_transform_stats_only_mode(
        self,
        mock_transform_load,
        mock_transform_save,
        mock_validate_load,
        mock_validate_save,
        mock_realistic_monthly_data,
    ):
        """
        Test stats_only mode: validate stats → transform with missing salary/player data.
        Verify transform handles partial data gracefully.
        """

        def mock_load_impl(s3_key):
            if "stats" in s3_key:
                return mock_realistic_monthly_data["stats"]
            return None  # No players or salaries data

        mock_validate_load.side_effect = mock_load_impl
        mock_transform_load.side_effect = mock_load_impl

        saved_data = {}

        def mock_save_impl(data, s3_key):
            saved_data[s3_key] = data
            return True

        mock_transform_save.side_effect = mock_save_impl

        # Execute validate
        validate_result = validate_data.handler(
            {
                "data_location": {
                    "bucket": "test-bucket",
                    "partition": "year=2025/month=01/day=01",
                },
                "fetch_type": "stats_only",
            },
            MagicMock(),
        )

        assert validate_result["validation_passed"] is True

        # Execute transform
        transform_result = transform_data.handler(
            {
                "validation_passed": validate_result["validation_passed"],
                "data_location": validate_result["data_location"],
            },
            MagicMock(),
        )

        assert transform_result["statusCode"] == 200
        assert transform_result["transformation_successful"] is True

        # Verify enriched stats were created despite missing salary/player data
        body = json.loads(transform_result["body"])
        assert "enriched_player_stats" in body["transformed"]

        # Verify enriched stats data structure
        stats_key = [k for k in saved_data.keys() if "enriched_player_stats" in k][0]
        enriched_stats = saved_data[stats_key]
        assert len(enriched_stats["player_stats"]) == 4

        # Find LeBron in the stats
        lebron = next(
            p for p in enriched_stats["player_stats"] if p["player_name"] == "LeBron James"
        )

        # Verify per-game stats are present
        assert lebron["points"] == 25.0
        assert lebron["rebounds"] == 7.5

        # Verify advanced stats are merged
        assert lebron["per"] == 24.5
        assert lebron["vorp"] == 4.0

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    @patch("src.etl.transform_data.ENVIRONMENT", "test")
    @patch("src.etl.transform_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.transform_data.save_to_s3")
    @patch("src.etl.transform_data.load_from_s3")
    def test_validate_to_transform_monthly_all_data(
        self,
        mock_transform_load,
        mock_transform_save,
        mock_validate_load,
        mock_validate_save,
        mock_realistic_monthly_data,
    ):
        """
        Test monthly mode: validate all 4 data types → transform enriches all.
        Verify: enriched_salaries has player_id,
                enriched_stats merges per-game + advanced,
                enriched_teams has aggregations.
        """

        def mock_load_impl(s3_key):
            if "players" in s3_key:
                return mock_realistic_monthly_data["players"]
            elif "stats" in s3_key:
                return mock_realistic_monthly_data["stats"]
            elif "salaries" in s3_key:
                return mock_realistic_monthly_data["salaries"]
            elif "teams" in s3_key:
                return mock_realistic_monthly_data["teams"]
            return None

        mock_validate_load.side_effect = mock_load_impl
        mock_transform_load.side_effect = mock_load_impl

        saved_data = {}

        def mock_save_impl(data, s3_key):
            saved_data[s3_key] = data
            return True

        mock_transform_save.side_effect = mock_save_impl

        # Execute validate
        validate_result = validate_data.handler(
            {
                "data_location": {
                    "bucket": "test-bucket",
                    "partition": "year=2025/month=01/day=01",
                },
                "fetch_type": "monthly",
            },
            MagicMock(),
        )

        assert validate_result["validation_passed"] is True

        # Execute transform
        transform_result = transform_data.handler(
            {
                "validation_passed": validate_result["validation_passed"],
                "data_location": validate_result["data_location"],
            },
            MagicMock(),
        )

        assert transform_result["statusCode"] == 200
        assert transform_result["transformation_successful"] is True

        body = json.loads(transform_result["body"])
        assert "enriched_salaries" in body["transformed"]
        assert "enriched_player_stats" in body["transformed"]
        assert "enriched_teams" in body["transformed"]

        # Verify enriched_salaries has player_id
        salary_key = [k for k in saved_data.keys() if "enriched_salaries" in k][0]
        enriched_salaries = saved_data[salary_key]
        assert len(enriched_salaries["salaries"]) == 4
        assert all(s.get("player_id") is not None for s in enriched_salaries["salaries"])

        lebron_salary = next(
            s for s in enriched_salaries["salaries"] if s["player_name"] == "LeBron James"
        )
        assert lebron_salary["player_id"] == 2544

        # Verify enriched_stats merges per-game + advanced
        stats_key = [k for k in saved_data.keys() if "enriched_player_stats" in k][0]
        enriched_stats = saved_data[stats_key]
        assert len(enriched_stats["player_stats"]) == 4

        lebron = next(
            p for p in enriched_stats["player_stats"] if p["player_name"] == "LeBron James"
        )
        assert lebron["points"] == 25.0  # per-game
        assert lebron["per"] == 24.5  # advanced

        # Verify enriched_teams has aggregations (3 teams: LAL, DEN, DAL)
        teams_key = [k for k in saved_data.keys() if "enriched_teams" in k][0]
        enriched_teams = saved_data[teams_key]
        assert len(enriched_teams["teams"]) == 3

        lakers = next(t for t in enriched_teams["teams"] if t["abbreviation"] == "LAL")
        # Verify Lakers has team aggregation fields
        assert "total_payroll" in lakers
        assert "roster_count" in lakers
        assert "roster_with_salary" in lakers
        assert lakers["roster_count"] >= 0

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    @patch("src.etl.transform_data.ENVIRONMENT", "test")
    @patch("src.etl.transform_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.transform_data.save_to_s3")
    @patch("src.etl.transform_data.load_from_s3")
    def test_salary_player_matching_with_unicode_names(
        self,
        mock_transform_load,
        mock_transform_save,
        mock_validate_load,
        mock_validate_save,
        mock_realistic_monthly_data,
    ):
        """
        Test salary-player matching with Unicode names (Jokić, Dončić).
        Verify ASCII normalization works correctly across validate→transform.
        """

        def mock_load_impl(s3_key):
            if "players" in s3_key:
                return mock_realistic_monthly_data["players"]
            elif "stats" in s3_key:
                return mock_realistic_monthly_data["stats"]
            elif "salaries" in s3_key:
                return mock_realistic_monthly_data["salaries"]
            elif "teams" in s3_key:
                return mock_realistic_monthly_data["teams"]
            return None

        mock_validate_load.side_effect = mock_load_impl
        mock_transform_load.side_effect = mock_load_impl

        saved_data = {}

        def mock_save_impl(data, s3_key):
            saved_data[s3_key] = data
            return True

        mock_transform_save.side_effect = mock_save_impl

        # Execute validate
        validate_result = validate_data.handler(
            {
                "data_location": {
                    "bucket": "test-bucket",
                    "partition": "year=2025/month=01/day=01",
                },
                "fetch_type": "monthly",
            },
            MagicMock(),
        )

        assert validate_result["validation_passed"] is True

        # Execute transform
        transform_result = transform_data.handler(
            {
                "validation_passed": validate_result["validation_passed"],
                "data_location": validate_result["data_location"],
            },
            MagicMock(),
        )

        assert transform_result["statusCode"] == 200

        # Verify salary matching worked despite ASCII normalization
        salary_key = [k for k in saved_data.keys() if "enriched_salaries" in k][0]
        enriched_salaries = saved_data[salary_key]

        assert len(enriched_salaries["salaries"]) == 4  # All 4 players
        jokic_salary = next(s for s in enriched_salaries["salaries"] if "Jokic" in s["player_name"])
        doncic_salary = next(
            s for s in enriched_salaries["salaries"] if "Doncic" in s["player_name"]
        )

        # Both should have matched player IDs (tests Unicode name matching)
        assert jokic_salary["player_id"] == 203999
        assert doncic_salary["player_id"] == 1629029

        # Verify match rate is 100%
        assert enriched_salaries["statistics"]["match_rate"] == 100.0


class TestDataConsistencyAcrossStages:
    """Test data quality and consistency from validate through transform."""

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    @patch("src.etl.transform_data.ENVIRONMENT", "test")
    @patch("src.etl.transform_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.transform_data.save_to_s3")
    @patch("src.etl.transform_data.load_from_s3")
    def test_player_counts_consistent_validate_to_transform(
        self,
        mock_transform_load,
        mock_transform_save,
        mock_validate_load,
        mock_validate_save,
        mock_complete_stats_data,
    ):
        """
        Verify player counts from validated data equal player counts in transformed data.
        Check statistics dictionary consistency.
        """
        player_count = 350  # mock_complete_stats_data has 350 players

        def mock_load_impl(s3_key):
            if "stats" in s3_key:
                return mock_complete_stats_data
            return None

        mock_validate_load.side_effect = mock_load_impl
        mock_transform_load.side_effect = mock_load_impl

        saved_data = {}

        def mock_save_impl(data, s3_key):
            saved_data[s3_key] = data
            return True

        mock_transform_save.side_effect = mock_save_impl

        # Execute validate
        validate_result = validate_data.handler(
            {
                "data_location": {
                    "bucket": "test-bucket",
                    "partition": "year=2025/month=01/day=01",
                },
                "fetch_type": "stats_only",
            },
            MagicMock(),
        )

        # Execute transform
        transform_data.handler(
            {
                "validation_passed": validate_result["validation_passed"],
                "data_location": validate_result["data_location"],
            },
            MagicMock(),
        )

        # Verify player counts are consistent
        stats_key = [k for k in saved_data.keys() if "enriched_player_stats" in k][0]
        enriched_stats = saved_data[stats_key]

        assert len(enriched_stats["player_stats"]) == player_count
        assert enriched_stats["statistics"]["total_players"] == player_count

    @patch("src.etl.validate_data.ENVIRONMENT", "test")
    @patch("src.etl.validate_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.validate_data.save_validation_report")
    @patch("src.etl.validate_data.load_from_s3")
    @patch("src.etl.transform_data.ENVIRONMENT", "test")
    @patch("src.etl.transform_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.transform_data.save_to_s3")
    @patch("src.etl.transform_data.load_from_s3")
    def test_salary_totals_consistent_validate_to_transform(
        self,
        mock_transform_load,
        mock_transform_save,
        mock_validate_load,
        mock_validate_save,
        mock_complete_stats_data,
        mock_active_players,
        mock_salary_data,
        mock_nba_teams,
    ):
        """
        Verify total salaries validated equal total salaries in transformed output.
        Check league_total_payroll matches raw salary sum.
        """
        total_salary = sum(s["annual_salary"] for s in mock_salary_data["salaries"])

        def mock_load_impl(s3_key):
            if "players" in s3_key:
                return mock_active_players
            elif "stats" in s3_key:
                return mock_complete_stats_data
            elif "salaries" in s3_key:
                return mock_salary_data
            elif "teams" in s3_key:
                return mock_nba_teams
            return None

        mock_validate_load.side_effect = mock_load_impl
        mock_transform_load.side_effect = mock_load_impl

        saved_data = {}

        def mock_save_impl(data, s3_key):
            saved_data[s3_key] = data
            return True

        mock_transform_save.side_effect = mock_save_impl

        # Execute validate
        validate_result = validate_data.handler(
            {
                "data_location": {
                    "bucket": "test-bucket",
                    "partition": "year=2025/month=01/day=01",
                },
                "fetch_type": "monthly",
            },
            MagicMock(),
        )

        # Execute transform
        transform_result = transform_data.handler(
            {
                "validation_passed": validate_result["validation_passed"],
                "data_location": validate_result["data_location"],
            },
            MagicMock(),
        )

        # Verify salary totals are tracked and match expected total
        transform_body = json.loads(transform_result["body"])
        assert "total_salary_cap" in transform_body["statistics"]
        assert transform_body["statistics"]["total_salary_cap"] == total_salary

        # Verify team payroll structure exists
        teams_key = [k for k in saved_data.keys() if "enriched_teams" in k][0]
        enriched_teams = saved_data[teams_key]
        assert "teams" in enriched_teams
        assert len(enriched_teams["teams"]) > 0
        # Verify teams have payroll field (value depends on name matching between sources)
        assert all("total_payroll" in t for t in enriched_teams["teams"])
