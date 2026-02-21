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


class TestSchemaCreation:
    """Test database schema creation."""

    def test_ensure_schema_exists_when_tables_exist(self):
        """Test schema check when tables already exist."""
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = [True]  # Tables exist

        result = load_to_rds.ensure_schema_exists(mock_cursor)

        assert result is True
        # Should only execute check query, not create tables
        assert mock_cursor.execute.call_count == 1

    def test_ensure_schema_exists_creates_tables(self):
        """Test schema creation when tables don't exist."""
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = [False]  # Tables don't exist

        result = load_to_rds.ensure_schema_exists(mock_cursor)

        assert result is False
        # Should execute check query + create tables query
        assert mock_cursor.execute.call_count == 2


class TestUpsertFunctions:
    """Test individual upsert functions."""

    @patch("src.etl.load_to_rds.execute_batch")
    def test_upsert_players(self, mock_execute_batch):
        """Test upserting players."""
        mock_cursor = Mock()
        players = [
            {"id": 2544, "full_name": "LeBron James"},
            {"id": 203999, "full_name": "Nikola Jokic"},
        ]

        count = load_to_rds.upsert_players(mock_cursor, players)

        assert count == 2
        mock_execute_batch.assert_called_once()

    def test_upsert_players_empty(self):
        """Test upserting empty players list."""
        mock_cursor = Mock()

        count = load_to_rds.upsert_players(mock_cursor, [])

        assert count == 0

    @patch("src.etl.load_to_rds.execute_batch")
    def test_upsert_salaries(self, mock_execute_batch):
        """Test upserting salaries."""
        mock_cursor = Mock()
        salaries = [
            {
                "player_id": 2544,
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
                "player_id": 2544,
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
        # Mock schema check - tables already exist
        mock_cursor.fetchone.return_value = [True]
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn

        # Mock S3 data loading
        def mock_load_impl(s3_key):
            if "players" in s3_key:
                return {"players": [{"id": 2544, "full_name": "LeBron James"}]}
            elif "salaries" in s3_key:
                return {
                    "salaries": [
                        {
                            "player_id": 2544,
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
                            "player_id": 2544,
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
        assert "players" in result["records_loaded"]
        assert "salaries" in result["records_loaded"]
        assert "player_stats" in result["records_loaded"]
        assert "teams" in result["records_loaded"]

        # Verify database operations (commit called for schema + data)
        assert mock_conn.commit.call_count == 2  # Schema creation + data loading
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
        # Mock schema check - tables already exist
        mock_cursor.fetchone.return_value = [True]
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
