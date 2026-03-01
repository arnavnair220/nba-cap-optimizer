"""
Unit tests for API handler.

Tests route handling, query parameter parsing, response formatting,
and error cases with mocked database connections.
"""

import json
from unittest.mock import MagicMock, patch

from src.api.handler import (
    cors_headers,
    error_response,
    get_all_predictions,
    get_all_teams,
    get_overvalued_predictions,
    get_player_prediction,
    get_team_detail,
    get_undervalued_predictions,
    handler,
    success_response,
)


class TestResponseHelpers:
    """Test response helper functions."""

    def test_cors_headers(self):
        """Test CORS headers are correct."""
        headers = cors_headers()
        assert headers["Access-Control-Allow-Origin"] == "*"
        assert headers["Access-Control-Allow-Methods"] == "GET,OPTIONS"
        assert "Content-Type" in headers["Access-Control-Allow-Headers"]

    def test_success_response(self):
        """Test success response formatting."""
        data = {"test": "data"}
        response = success_response(data)

        assert response["statusCode"] == 200
        assert "Access-Control-Allow-Origin" in response["headers"]
        assert json.loads(response["body"]) == data

    def test_success_response_custom_status(self):
        """Test success response with custom status code."""
        response = success_response({"ok": True}, status_code=201)
        assert response["statusCode"] == 201

    def test_error_response(self):
        """Test error response formatting."""
        response = error_response("Test error", 400)

        assert response["statusCode"] == 400
        assert "Access-Control-Allow-Origin" in response["headers"]
        body = json.loads(response["body"])
        assert body["error"] == "Test error"


class TestDatabaseQueries:
    """Test database query functions with mocked connections."""

    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_get_all_predictions(self):
        """Test get_all_predictions query."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.return_value = [
            {
                "player_name": "Test Player",
                "season": "2025-26",
                "predicted_fmv": 30000000,
                "actual_salary": 25000000,
                "inefficiency_score": -0.167,
                "value_category": "Bargain",
                "vorp": 4.5,
            }
        ]

        query_params = {"limit": "10", "offset": "0"}
        results = get_all_predictions(mock_conn, query_params)

        assert len(results) == 1
        assert results[0]["player_name"] == "Test Player"
        assert results[0]["value_category"] == "Bargain"
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()

    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_get_all_predictions_with_filters(self):
        """Test get_all_predictions with filtering."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        query_params = {
            "value_category": "Bargain",
            "team": "LAL",
            "position": "PG",
            "sort_by": "vorp",
            "limit": "25",
        }

        get_all_predictions(mock_conn, query_params)

        call_args = mock_cursor.execute.call_args
        sql_query = call_args[0][0]
        params = call_args[0][1]

        assert "WHERE p.season = %s" in sql_query
        assert "AND p.value_category = %s" in sql_query
        assert "AND ps.team_abbreviation = %s" in sql_query
        assert "AND ps.position = %s" in sql_query
        assert "ORDER BY vorp" in sql_query

        assert "2025-26" in params
        assert "Bargain" in params
        assert "LAL" in params
        assert "PG" in params

    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_get_undervalued_predictions(self):
        """Test get_undervalued_predictions query."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        query_params = {"limit": "25"}
        get_undervalued_predictions(mock_conn, query_params)

        call_args = mock_cursor.execute.call_args
        sql_query = call_args[0][0]
        params = call_args[0][1]

        assert "WHERE p.season = %s" in sql_query
        assert "AND p.value_category = 'Bargain'" in sql_query
        assert "ORDER BY p.inefficiency_score ASC" in sql_query
        assert params == ["2025-26", 25]

    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_get_overvalued_predictions(self):
        """Test get_overvalued_predictions query."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        query_params = {"limit": "10"}
        get_overvalued_predictions(mock_conn, query_params)

        call_args = mock_cursor.execute.call_args
        sql_query = call_args[0][0]
        params = call_args[0][1]

        assert "AND p.value_category = 'Overpaid'" in sql_query
        assert "ORDER BY p.inefficiency_score DESC" in sql_query
        assert params == ["2025-26", 10]

    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_get_player_prediction(self):
        """Test get_player_prediction query."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchone.return_value = {
            "player_name": "LeBron James",
            "season": "2025-26",
            "predicted_fmv": 45000000,
            "actual_salary": 48000000,
        }

        result = get_player_prediction(mock_conn, "LeBron James")

        assert result is not None
        assert result["player_name"] == "LeBron James"
        assert result["predicted_fmv"] == 45000000

        call_args = mock_cursor.execute.call_args
        params = call_args[0][1]
        assert params == ["LeBron James", "2025-26"]

    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_get_player_prediction_not_found(self):
        """Test get_player_prediction when player not found."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None

        result = get_player_prediction(mock_conn, "Unknown Player")

        assert result is None

    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_get_all_teams(self):
        """Test get_all_teams query."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.return_value = [
            {
                "team_abbreviation": "LAL",
                "full_name": "Los Angeles Lakers",
                "player_count": 15,
                "total_payroll": 185000000,
                "avg_inefficiency_score": 0.12,
            }
        ]

        query_params = {"sort_by": "avg_inefficiency"}
        results = get_all_teams(mock_conn, query_params)

        assert len(results) == 1
        assert results[0]["team_abbreviation"] == "LAL"

        call_args = mock_cursor.execute.call_args
        sql_query = call_args[0][0]
        assert "ORDER BY avg_inefficiency_score ASC" in sql_query

    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_get_team_detail(self):
        """Test get_team_detail query."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        team_data = {
            "team_abbreviation": "LAL",
            "full_name": "Los Angeles Lakers",
            "player_count": 15,
        }

        roster_data = [
            {
                "player_name": "LeBron James",
                "predicted_fmv": 45000000,
                "actual_salary": 48000000,
            }
        ]

        mock_cursor.fetchone.return_value = team_data
        mock_cursor.fetchall.return_value = roster_data

        result = get_team_detail(mock_conn, "LAL")

        assert result is not None
        assert result["team_abbreviation"] == "LAL"
        assert len(result["players"]) == 1
        assert result["players"][0]["player_name"] == "LeBron James"

    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_get_team_detail_not_found(self):
        """Test get_team_detail when team not found."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None

        result = get_team_detail(mock_conn, "INVALID")

        assert result is None


class TestAPIHandler:
    """Test main Lambda handler routing."""

    @patch("src.api.handler.get_db_connection")
    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_handler_get_predictions(self, mock_get_db):
        """Test handler routes /predictions correctly."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        mock_get_db.return_value = mock_conn

        event = {
            "httpMethod": "GET",
            "path": "/predictions",
            "queryStringParameters": {"limit": "10"},
            "pathParameters": {},
        }

        response = handler(event, None)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert "predictions" in body
        assert "count" in body
        mock_conn.close.assert_called_once()

    @patch("src.api.handler.get_db_connection")
    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_handler_get_undervalued(self, mock_get_db):
        """Test handler routes /predictions/undervalued correctly."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        mock_get_db.return_value = mock_conn

        event = {
            "httpMethod": "GET",
            "path": "/predictions/undervalued",
            "queryStringParameters": {},
            "pathParameters": {},
        }

        response = handler(event, None)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert "predictions" in body

    @patch("src.api.handler.get_db_connection")
    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_handler_get_overvalued(self, mock_get_db):
        """Test handler routes /predictions/overvalued correctly."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        mock_get_db.return_value = mock_conn

        event = {
            "httpMethod": "GET",
            "path": "/predictions/overvalued",
            "queryStringParameters": {},
            "pathParameters": {},
        }

        response = handler(event, None)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert "predictions" in body

    @patch("src.api.handler.get_db_connection")
    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_handler_get_player(self, mock_get_db):
        """Test handler routes /predictions/{player_name} correctly."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = {
            "player_name": "LeBron James",
            "predicted_fmv": 45000000,
        }
        mock_get_db.return_value = mock_conn

        event = {
            "httpMethod": "GET",
            "path": "/predictions/LeBron James",
            "queryStringParameters": {},
            "pathParameters": {"player_name": "LeBron James"},
        }

        response = handler(event, None)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["player_name"] == "LeBron James"

    @patch("src.api.handler.get_db_connection")
    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_handler_get_player_not_found(self, mock_get_db):
        """Test handler returns 404 for unknown player."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None
        mock_get_db.return_value = mock_conn

        event = {
            "httpMethod": "GET",
            "path": "/predictions/Unknown Player",
            "queryStringParameters": {},
            "pathParameters": {"player_name": "Unknown Player"},
        }

        response = handler(event, None)

        assert response["statusCode"] == 404
        body = json.loads(response["body"])
        assert "not found" in body["error"]

    @patch("src.api.handler.get_db_connection")
    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_handler_get_player_missing_name(self, mock_get_db):
        """Test handler returns 400 when player name missing."""
        mock_conn = MagicMock()
        mock_get_db.return_value = mock_conn

        event = {
            "httpMethod": "GET",
            "path": "/predictions/",
            "queryStringParameters": {},
            "pathParameters": {},
        }

        response = handler(event, None)

        assert response["statusCode"] == 400
        body = json.loads(response["body"])
        assert "required" in body["error"]

    @patch("src.api.handler.get_db_connection")
    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_handler_get_teams(self, mock_get_db):
        """Test handler routes /teams correctly."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        mock_get_db.return_value = mock_conn

        event = {
            "httpMethod": "GET",
            "path": "/teams",
            "queryStringParameters": {},
            "pathParameters": {},
        }

        response = handler(event, None)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert "teams" in body
        assert "count" in body

    @patch("src.api.handler.get_db_connection")
    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_handler_get_team_detail(self, mock_get_db):
        """Test handler routes /teams/{team_abbreviation} correctly."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchone.return_value = {
            "team_abbreviation": "LAL",
            "full_name": "Los Angeles Lakers",
        }
        mock_cursor.fetchall.return_value = []

        mock_get_db.return_value = mock_conn

        event = {
            "httpMethod": "GET",
            "path": "/teams/LAL",
            "queryStringParameters": {},
            "pathParameters": {"team_abbreviation": "LAL"},
        }

        response = handler(event, None)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["team_abbreviation"] == "LAL"
        assert "players" in body

    @patch("src.api.handler.get_db_connection")
    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_handler_get_team_not_found(self, mock_get_db):
        """Test handler returns 404 for unknown team."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None
        mock_get_db.return_value = mock_conn

        event = {
            "httpMethod": "GET",
            "path": "/teams/INVALID",
            "queryStringParameters": {},
            "pathParameters": {"team_abbreviation": "INVALID"},
        }

        response = handler(event, None)

        assert response["statusCode"] == 404
        body = json.loads(response["body"])
        assert "not found" in body["error"]

    @patch("src.api.handler.get_db_connection")
    def test_handler_options_request(self, mock_get_db):
        """Test handler handles OPTIONS (CORS preflight) correctly."""
        event = {
            "httpMethod": "OPTIONS",
            "path": "/predictions",
            "queryStringParameters": {},
            "pathParameters": {},
        }

        response = handler(event, None)

        assert response["statusCode"] == 200
        assert "Access-Control-Allow-Origin" in response["headers"]
        mock_get_db.assert_not_called()

    @patch("src.api.handler.get_db_connection")
    def test_handler_invalid_route(self, mock_get_db):
        """Test handler returns 404 for invalid route."""
        mock_conn = MagicMock()
        mock_get_db.return_value = mock_conn

        event = {
            "httpMethod": "GET",
            "path": "/invalid",
            "queryStringParameters": {},
            "pathParameters": {},
        }

        response = handler(event, None)

        assert response["statusCode"] == 404
        body = json.loads(response["body"])
        assert "not found" in body["error"].lower()

    @patch("src.api.handler.get_db_connection")
    def test_handler_database_error(self, mock_get_db):
        """Test handler returns 500 on database error."""
        mock_get_db.side_effect = Exception("Database connection failed")

        event = {
            "httpMethod": "GET",
            "path": "/predictions",
            "queryStringParameters": {},
            "pathParameters": {},
        }

        response = handler(event, None)

        assert response["statusCode"] == 500
        body = json.loads(response["body"])
        assert "error" in body


class TestQueryParameterParsing:
    """Test query parameter parsing and validation."""

    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_limit_default_value(self):
        """Test limit defaults to 100."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        get_all_predictions(mock_conn, {})

        call_args = mock_cursor.execute.call_args
        params = call_args[0][1]
        assert 100 in params

    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_limit_custom_value(self):
        """Test custom limit value."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        get_all_predictions(mock_conn, {"limit": "50"})

        call_args = mock_cursor.execute.call_args
        params = call_args[0][1]
        assert 50 in params

    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_offset_default_value(self):
        """Test offset defaults to 0."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        get_all_predictions(mock_conn, {})

        call_args = mock_cursor.execute.call_args
        params = call_args[0][1]
        assert 0 in params

    @patch.dict("os.environ", {"CURRENT_SEASON": "2025-26"})
    def test_sort_by_validation(self):
        """Test sort_by defaults to inefficiency_score for invalid values."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        get_all_predictions(mock_conn, {"sort_by": "invalid_field"})

        call_args = mock_cursor.execute.call_args
        sql_query = call_args[0][0]
        assert "ORDER BY inefficiency_score" in sql_query
