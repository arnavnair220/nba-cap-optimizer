"""
Tests for load_to_rds Lambda function.
"""

import json
from unittest.mock import MagicMock, Mock, patch

from src.etl import load_to_rds


class TestDatabaseConnection:
    """Test database connection management."""

    @patch("src.etl.load_to_rds.get_secretsmanager_client")
    @patch("src.etl.load_to_rds.DB_SECRET_ARN", "test-arn")
    def test_get_db_credentials_success(self, mock_get_sm_client):
        """Test successful retrieval of database credentials."""
        mock_sm_client = Mock()
        mock_sm_client.get_secret_value.return_value = {
            "SecretString": json.dumps(
                {
                    "host": "localhost",
                    "port": 5432,
                    "username": "test_user",
                    "password": "test_pass",
                    "dbname": "test_db",
                }
            )
        }
        mock_get_sm_client.return_value = mock_sm_client

        creds = load_to_rds.get_db_credentials()

        assert creds["host"] == "localhost"
        assert creds["port"] == 5432
        assert creds["username"] == "test_user"
        assert creds["password"] == "test_pass"
        assert creds["database"] == "test_db"


class TestUpsertFunctions:
    """Test individual upsert functions."""

    @patch("src.etl.load_to_rds.execute_batch")
    def test_upsert_salaries(self, mock_execute_batch):
        """Test upserting salaries."""
        mock_cursor = Mock()
        salaries = [
            {
                "player_name": "LeBron James",
                "annual_salary": 47607350,
                "season": "2025-26",
                "source": "espn",
            }
        ]

        count = load_to_rds.upsert_salaries(mock_cursor, salaries)

        assert count == 1
        mock_execute_batch.assert_called_once()

    @patch("src.etl.load_to_rds.execute_batch")
    def test_upsert_player_stats(self, mock_execute_batch):
        """Test upserting player stats."""
        mock_cursor = Mock()
        stats = [
            {
                "player_name": "LeBron James",
                "team_abbreviation": "LAL",
                "age": 39,
                "position": "SF",
                "games_played": 50,
                "games_started": 48,
                "minutes": 35.2,
                "points": 25.8,
                "rebounds": 7.7,
                "assists": 8.2,
                "per": 24.5,
                "vorp": 4.2,
            }
        ]

        count = load_to_rds.upsert_player_stats(mock_cursor, stats, "2025-26")

        assert count == 1
        mock_execute_batch.assert_called_once()

    @patch("src.etl.load_to_rds.execute_batch")
    def test_upsert_teams(self, mock_execute_batch):
        """Test upserting teams."""
        mock_cursor = Mock()
        teams = [
            {
                "id": 1610612747,
                "full_name": "Los Angeles Lakers",
                "abbreviation": "LAL",
                "total_payroll": 180000000,
                "roster_count": 15,
                "roster_with_salary": 15,
                "avg_salary": 12000000,
                "min_salary": 1000000,
                "max_salary": 47607350,
                "top_paid_player": "LeBron James",
                "top_paid_salary": 47607350,
            }
        ]

        count = load_to_rds.upsert_teams(mock_cursor, teams)

        assert count == 1
        mock_execute_batch.assert_called_once()


class TestHandlerValidation:
    """Test handler validation logic."""

    @patch("src.etl.load_to_rds.ENVIRONMENT", "test")
    @patch("src.etl.load_to_rds.S3_BUCKET", "test-bucket")
    @patch("src.etl.load_to_rds.DB_SECRET_ARN", "test-arn")
    def test_handler_skips_when_transformation_fails(self):
        """Test handler returns 400 when transformation_successful is false."""
        event = {
            "transformation_successful": False,
            "data_location": {"bucket": "test", "partition": "year=2024/month=02/day=17"},
        }

        result = load_to_rds.handler(event, MagicMock())

        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "error" in body

    @patch("src.etl.load_to_rds.ENVIRONMENT", "test")
    @patch("src.etl.load_to_rds.S3_BUCKET", "test-bucket")
    @patch("src.etl.load_to_rds.DB_SECRET_ARN", "test-arn")
    def test_handler_requires_data_location(self):
        """Test handler returns 400 when data_location is missing."""
        event = {
            "transformation_successful": True,
        }

        result = load_to_rds.handler(event, MagicMock())

        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "data_location" in body["error"]


class TestHandlerDataLoad:
    """Test handler data loading workflow."""

    @patch("src.etl.load_to_rds.ENVIRONMENT", "test")
    @patch("src.etl.load_to_rds.S3_BUCKET", "test-bucket")
    @patch("src.etl.load_to_rds.DB_SECRET_ARN", "test-arn")
    @patch("src.etl.load_to_rds.execute_batch")
    @patch("src.etl.load_to_rds.get_db_connection")
    @patch("src.etl.load_to_rds.load_from_s3")
    def test_handler_successful_load(self, mock_load_s3, mock_db_conn, mock_execute_batch):
        """Test handler successfully loads all data to RDS."""
        # Mock database connection
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        # Mock S3 data loading
        def mock_load_impl(s3_key):
            if "salaries" in s3_key:
                return {
                    "salaries": [
                        {
                            "player_name": "LeBron James",
                            "annual_salary": 47607350,
                            "season": "2025-26",
                            "source": "espn",
                        }
                    ]
                }
            elif "stats" in s3_key:
                return {
                    "player_stats": [
                        {
                            "player_name": "LeBron James",
                            "team_abbreviation": "LAL",
                            "points": 25.8,
                            "rebounds": 7.7,
                            "assists": 8.2,
                        }
                    ]
                }
            elif "teams" in s3_key:
                return {
                    "teams": [
                        {
                            "id": 1610612747,
                            "full_name": "Los Angeles Lakers",
                            "abbreviation": "LAL",
                            "total_payroll": 180000000,
                        }
                    ]
                }
            return None

        mock_load_s3.side_effect = mock_load_impl

        event = {
            "transformation_successful": True,
            "data_location": {"bucket": "test", "partition": "year=2024/month=02/day=17"},
        }

        result = load_to_rds.handler(event, MagicMock())

        # Verify success
        assert result["statusCode"] == 200
        assert result["load_successful"] is True
        assert "salaries" in result["records_loaded"]
        assert "player_stats" in result["records_loaded"]
        assert "teams" in result["records_loaded"]

        # Verify database operations (commit called for data)
        assert mock_conn.commit.call_count == 1  # Data loading only (schema managed by Terraform)
        mock_conn.close.assert_called_once()

    @patch("src.etl.load_to_rds.ENVIRONMENT", "test")
    @patch("src.etl.load_to_rds.S3_BUCKET", "test-bucket")
    @patch("src.etl.load_to_rds.DB_SECRET_ARN", "test-arn")
    @patch("src.etl.load_to_rds.execute_batch")
    @patch("src.etl.load_to_rds.get_db_connection")
    @patch("src.etl.load_to_rds.load_from_s3")
    def test_handler_fails_without_player_stats(
        self, mock_load_s3, mock_db_conn, mock_execute_batch
    ):
        """Test handler fails when player stats are missing (critical data)."""
        # Mock database connection
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        # Mock S3 data loading - no player stats
        def mock_load_impl(s3_key):
            if "players" in s3_key:
                return {"players": [{"id": 2544, "full_name": "LeBron James"}]}
            elif "stats" in s3_key:
                return None  # Missing critical data
            return None

        mock_load_s3.side_effect = mock_load_impl

        event = {
            "transformation_successful": True,
            "data_location": {"bucket": "test", "partition": "year=2024/month=02/day=17"},
        }

        result = load_to_rds.handler(event, MagicMock())

        # Verify failure
        assert result["statusCode"] == 500
        body = json.loads(result["body"])
        assert "player stats" in body["errors"][0].lower()

    @patch("src.etl.load_to_rds.ENVIRONMENT", "test")
    @patch("src.etl.load_to_rds.S3_BUCKET", "test-bucket")
    @patch("src.etl.load_to_rds.DB_SECRET_ARN", "test-arn")
    @patch("src.etl.load_to_rds.get_db_connection")
    @patch("src.etl.load_to_rds.load_from_s3")
    def test_handler_rolls_back_on_error(self, mock_load_s3, mock_db_conn):
        """Test handler rolls back transaction on error."""
        # Mock database connection that fails
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Database error")
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        # Mock S3 data loading
        mock_load_s3.return_value = {
            "player_stats": [{"player_name": "Test Player", "points": 10.0}]
        }

        event = {
            "transformation_successful": True,
            "data_location": {"bucket": "test", "partition": "year=2024/month=02/day=17"},
        }

        result = load_to_rds.handler(event, MagicMock())

        # Verify rollback was called
        assert result["statusCode"] == 500
        mock_conn.rollback.assert_called_once()
        mock_conn.close.assert_called_once()


class TestUpsertSalaryCapHistory:
    """Test upserting salary cap history data."""

    @patch("src.etl.load_to_rds.execute_batch")
    def test_upsert_salary_cap_history_success(self, mock_execute_batch):
        """Test successful upsert of salary cap history."""
        mock_cursor = Mock()
        cap_history = [
            {
                "season": "2025-2026",
                "salary_cap": 154647000,
                "luxury_tax": 187895000,
                "first_apron": 178655000,
                "second_apron": 189495000,
                "bae": 5168000,
                "non_taxpayer_mle": 13040000,
                "taxpayer_mle": 5685000,
                "team_room_mle": 8781000,
            },
            {
                "season": "2024-2025",
                "salary_cap": 140588000,
                "luxury_tax": 170814000,
                "first_apron": 172346000,
                "second_apron": 182794000,
                "bae": 4700000,
                "non_taxpayer_mle": 12405000,
                "taxpayer_mle": 5183000,
                "team_room_mle": 7981000,
            },
        ]

        count = load_to_rds.upsert_salary_cap_history(mock_cursor, cap_history)

        assert count == 2
        mock_execute_batch.assert_called_once()
        # Verify the SQL contains ON CONFLICT
        sql_call = mock_execute_batch.call_args[0][1]
        assert "ON CONFLICT" in sql_call
        assert "salary_cap_history" in sql_call

    @patch("src.etl.load_to_rds.execute_batch")
    def test_upsert_salary_cap_history_empty_data(self, mock_execute_batch):
        """Test handling of empty salary cap data."""
        mock_cursor = Mock()

        count = load_to_rds.upsert_salary_cap_history(mock_cursor, [])

        assert count == 0
        mock_execute_batch.assert_not_called()


class TestUpsertContractLimits:
    """Test upserting contract limits data."""

    @patch("src.etl.load_to_rds.execute_batch")
    def test_upsert_contract_limits_success(self, mock_execute_batch):
        """Test successful upsert of contract limits."""
        mock_cursor = Mock()
        contract_limits = [
            {
                "season": "2025-2026",
                "max_0_6_yos": 38661750,
                "max_7_9_yos": 46394100,
                "max_10_plus_yos": 54126450,
                "min_0_yos": 1157153,
                "min_1_yos": 1862265,
                "min_2_yos": 2296274,
                "min_10_plus_yos": 3634153,
            }
        ]

        count = load_to_rds.upsert_contract_limits(mock_cursor, contract_limits)

        assert count == 1
        mock_execute_batch.assert_called_once()
        # Verify the SQL contains ON CONFLICT
        sql_call = mock_execute_batch.call_args[0][1]
        assert "ON CONFLICT" in sql_call
        assert "contract_limits" in sql_call

    @patch("src.etl.load_to_rds.execute_batch")
    def test_upsert_contract_limits_empty_data(self, mock_execute_batch):
        """Test handling of empty contract limits data."""
        mock_cursor = Mock()

        count = load_to_rds.upsert_contract_limits(mock_cursor, [])

        assert count == 0
        mock_execute_batch.assert_not_called()
