"""
Unit tests for historical data backfill functionality.

Tests the season parameter flow and URL construction for historical data.
"""

from unittest.mock import MagicMock, Mock, patch

import pandas as pd

from src.etl import fetch_data


class TestHistoricalSeasonURLConstruction:
    """Test URL construction for different seasons."""

    @patch("src.etl.fetch_data.time.sleep")
    @patch("src.etl.fetch_data.pd.read_html")
    def test_basketball_reference_url_for_2022_23_season(self, mock_read_html, mock_sleep):
        """Test that 2022-23 season uses correct B-R URL (2023)."""
        mock_pergame_df = pd.DataFrame({"Player": ["Test"], "PTS": [20.0]})
        mock_advanced_df = pd.DataFrame({"Player": ["Test"], "PER": [15.0]})
        mock_read_html.side_effect = [[mock_pergame_df], [mock_advanced_df]]

        result = fetch_data.fetch_player_stats("2022-23")

        assert result is not None
        assert result["season"] == "2022-23"

        # Verify URLs called contain "2023"
        assert mock_read_html.call_count == 2
        calls = mock_read_html.call_args_list
        pergame_url = calls[0][0][0]
        advanced_url = calls[1][0][0]

        assert "NBA_2023" in pergame_url
        assert "per_game.html" in pergame_url
        assert "NBA_2023" in advanced_url
        assert "advanced.html" in advanced_url

    @patch("src.etl.fetch_data.time.sleep")
    @patch("src.etl.fetch_data.pd.read_html")
    def test_basketball_reference_url_for_2023_24_season(self, mock_read_html, mock_sleep):
        """Test that 2023-24 season uses correct B-R URL (2024)."""
        mock_pergame_df = pd.DataFrame({"Player": ["Test"], "PTS": [20.0]})
        mock_advanced_df = pd.DataFrame({"Player": ["Test"], "PER": [15.0]})
        mock_read_html.side_effect = [[mock_pergame_df], [mock_advanced_df]]

        result = fetch_data.fetch_player_stats("2023-24")

        assert result is not None
        assert result["season"] == "2023-24"

        # Verify URLs
        calls = mock_read_html.call_args_list
        pergame_url = calls[0][0][0]
        advanced_url = calls[1][0][0]

        assert "NBA_2024" in pergame_url
        assert "NBA_2024" in advanced_url

    @patch("src.etl.fetch_data.time.sleep")
    @patch("src.etl.fetch_data.pd.read_html")
    def test_basketball_reference_url_for_2024_25_season(self, mock_read_html, mock_sleep):
        """Test that 2024-25 season uses correct B-R URL (2025)."""
        mock_pergame_df = pd.DataFrame({"Player": ["Test"], "PTS": [20.0]})
        mock_advanced_df = pd.DataFrame({"Player": ["Test"], "PER": [15.0]})
        mock_read_html.side_effect = [[mock_pergame_df], [mock_advanced_df]]

        fetch_data.fetch_player_stats("2024-25")

        calls = mock_read_html.call_args_list
        pergame_url = calls[0][0][0]

        assert "NBA_2025" in pergame_url

    @patch("src.etl.fetch_data.time.sleep")
    @patch("src.etl.fetch_data.requests.get")
    def test_espn_salaries_url_for_historical_season_2022_23(self, mock_get, mock_sleep):
        """Test that ESPN uses historical URL format for 2022-23 season."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"""
        <html>
            <table>
                <tr>
                    <td>RK</td>
                    <td>NAME</td>
                    <td>TEAM</td>
                    <td>SALARY</td>
                </tr>
                <tr>
                    <td>1</td>
                    <td>Stephen Curry, G</td>
                    <td>GSW</td>
                    <td>$48,000,000</td>
                </tr>
            </table>
        </html>
        """
        # Return empty response for second page to stop pagination
        mock_response_empty = Mock()
        mock_response_empty.status_code = 200
        mock_response_empty.content = b"<html><table></table></html>"

        mock_get.side_effect = [mock_response, mock_response_empty]

        result = fetch_data.fetch_espn_salaries("2022-23")

        # Verify first request used historical URL format with year 2023
        first_call_url = mock_get.call_args_list[0][0][0]
        assert "/year/2023" in first_call_url
        assert len(result) == 1

    @patch("src.etl.fetch_data.time.sleep")
    @patch("src.etl.fetch_data.requests.get")
    def test_espn_salaries_url_for_historical_season_2023_24(self, mock_get, mock_sleep):
        """Test that ESPN uses historical URL format for 2023-24 season."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = """
        <html>
            <table>
                <tr>
                    <td>1</td>
                    <td>Stephen Curry</td>
                    <td>GSW</td>
                    <td>$48000000</td>
                </tr>
            </table>
        </html>
        """
        mock_response_empty = Mock()
        mock_response_empty.status_code = 200
        mock_response_empty.content = "<html><table></table></html>"

        mock_get.side_effect = [mock_response, mock_response_empty]

        fetch_data.fetch_espn_salaries("2023-24")

        # Verify URL uses year 2024
        first_call_url = mock_get.call_args_list[0][0][0]
        assert "/year/2024" in first_call_url

    @patch("src.etl.fetch_data.time.sleep")
    @patch("src.etl.fetch_data.requests.get")
    @patch("src.etl.fetch_data.datetime")
    def test_espn_salaries_url_for_current_season(self, mock_datetime, mock_get, mock_sleep):
        """Test that ESPN uses base URL (no /year/) for current/future seasons."""
        # Mock current year as 2025
        mock_now = MagicMock()
        mock_now.year = 2025
        mock_datetime.utcnow.return_value = mock_now

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = """
        <html>
            <table>
                <tr>
                    <td>1</td>
                    <td>Stephen Curry</td>
                    <td>GSW</td>
                    <td>$48000000</td>
                </tr>
            </table>
        </html>
        """
        mock_response_empty = Mock()
        mock_response_empty.status_code = 200
        mock_response_empty.content = "<html><table></table></html>"

        mock_get.side_effect = [mock_response, mock_response_empty]

        # 2025-26 season (ending year 2026 > current year 2025)
        fetch_data.fetch_espn_salaries("2025-26")

        # Verify URL does NOT include /year/ for current season
        first_call_url = mock_get.call_args_list[0][0][0]
        assert "/year/" not in first_call_url
        assert first_call_url == "https://www.espn.com/nba/salaries"

    @patch("src.etl.fetch_data.time.sleep")
    @patch("src.etl.fetch_data.requests.get")
    def test_espn_salaries_pagination_with_historical_year(self, mock_get, mock_sleep):
        """Test that pagination works correctly with historical year URLs."""
        # Page 1 with data
        mock_response_page1 = Mock()
        mock_response_page1.status_code = 200
        mock_response_page1.content = b"""
        <html>
            <table>
                <tr>
                    <td>RK</td>
                    <td>NAME</td>
                    <td>TEAM</td>
                    <td>SALARY</td>
                </tr>
                <tr>
                    <td>1</td>
                    <td>Player 1, G</td>
                    <td>LAL</td>
                    <td>$1,000,000</td>
                </tr>
            </table>
        </html>
        """

        # Page 2 with data
        mock_response_page2 = Mock()
        mock_response_page2.status_code = 200
        mock_response_page2.content = b"""
        <html>
            <table>
                <tr>
                    <td>RK</td>
                    <td>NAME</td>
                    <td>TEAM</td>
                    <td>SALARY</td>
                </tr>
                <tr>
                    <td>51</td>
                    <td>Player 51, F</td>
                    <td>BOS</td>
                    <td>$2,000,000</td>
                </tr>
            </table>
        </html>
        """

        # Page 3 empty
        mock_response_page3 = Mock()
        mock_response_page3.status_code = 200
        mock_response_page3.content = b"<html><table></table></html>"

        mock_get.side_effect = [
            mock_response_page1,
            mock_response_page2,
            mock_response_page3,
        ]

        result = fetch_data.fetch_espn_salaries("2022-23")

        # Verify pagination URLs
        assert mock_get.call_count == 3
        page1_url = mock_get.call_args_list[0][0][0]
        page2_url = mock_get.call_args_list[1][0][0]

        # Page 1 should be base historical URL
        assert page1_url == "https://www.espn.com/nba/salaries/_/year/2023"

        # Page 2 should include pagination and year
        assert "/year/2023" in page2_url
        assert "/_/page/2" in page2_url

        # Should have fetched 2 players
        assert len(result) == 2


class TestSeasonParameterInSalaryData:
    """Test that season parameter is correctly set in fetched salary data."""

    @patch("src.etl.fetch_data.time.sleep")
    @patch("src.etl.fetch_data.requests.get")
    def test_salary_data_contains_correct_season(self, mock_get, mock_sleep):
        """Test that each salary entry has the correct season field."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"""
        <html>
            <table>
                <tr>
                    <td>RK</td>
                    <td>NAME</td>
                    <td>TEAM</td>
                    <td>SALARY</td>
                </tr>
                <tr>
                    <td>1</td>
                    <td>Stephen Curry, G</td>
                    <td>GSW</td>
                    <td>$48,000,000</td>
                </tr>
                <tr>
                    <td>2</td>
                    <td>LeBron James, F</td>
                    <td>LAL</td>
                    <td>$45,000,000</td>
                </tr>
            </table>
        </html>
        """
        mock_response_empty = Mock()
        mock_response_empty.status_code = 200
        mock_response_empty.content = b"<html><table></table></html>"

        mock_get.side_effect = [mock_response, mock_response_empty]

        result = fetch_data.fetch_espn_salaries("2022-23")

        assert len(result) == 2
        assert all(s["season"] == "2022-23" for s in result)
        assert all(s["source"] == "espn" for s in result)


class TestHandlerSeasonParameter:
    """Test that handler correctly uses and passes season parameter."""

    @patch("src.etl.fetch_data.fetch_player_stats")
    @patch("src.etl.fetch_data.save_to_s3")
    @patch("src.etl.fetch_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.fetch_data.ENVIRONMENT", "test")
    def test_handler_passes_season_to_fetch_functions(self, mock_save, mock_fetch_stats):
        """Test that handler passes custom season to fetch functions."""
        mock_fetch_stats.return_value = {
            "season": "2022-23",
            "per_game_stats": [],
            "advanced_stats": [],
            "per_game_columns": [],
            "advanced_columns": [],
            "fetch_timestamp": "2025-01-01T00:00:00",
            "source": "basketball_reference",
        }
        mock_save.return_value = True

        event = {"fetch_type": "stats_only", "season": "2022-23"}

        result = fetch_data.handler(event, MagicMock())

        # Verify fetch_player_stats was called with correct season
        mock_fetch_stats.assert_called_once_with("2022-23")
        assert result["statusCode"] == 200
        assert result["season"] == "2022-23"

    @patch("src.etl.fetch_data.fetch_espn_salaries")
    @patch("src.etl.fetch_data.teams.get_teams")
    @patch("src.etl.fetch_data.players.get_active_players")
    @patch("src.etl.fetch_data.fetch_player_stats")
    @patch("src.etl.fetch_data.save_to_s3")
    @patch("src.etl.fetch_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.fetch_data.ENVIRONMENT", "test")
    def test_handler_monthly_passes_season_to_all_functions(
        self,
        mock_save,
        mock_fetch_stats,
        mock_get_players,
        mock_get_teams,
        mock_fetch_salaries,
    ):
        """Test that monthly fetch passes season to both stats and salaries."""
        mock_get_players.return_value = []
        mock_get_teams.return_value = []
        mock_fetch_stats.return_value = {
            "season": "2023-24",
            "per_game_stats": [],
            "advanced_stats": [],
            "per_game_columns": [],
            "advanced_columns": [],
            "fetch_timestamp": "2025-01-01T00:00:00",
            "source": "basketball_reference",
        }
        mock_fetch_salaries.return_value = []
        mock_save.return_value = True

        event = {"fetch_type": "monthly", "season": "2023-24"}

        result = fetch_data.handler(event, MagicMock())

        # Verify both functions called with correct season
        mock_fetch_stats.assert_called_once_with("2023-24")
        mock_fetch_salaries.assert_called_once_with("2023-24")
        assert result["season"] == "2023-24"

    @patch("src.etl.fetch_data.fetch_player_stats")
    @patch("src.etl.fetch_data.save_to_s3")
    @patch("src.etl.fetch_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.fetch_data.ENVIRONMENT", "test")
    def test_handler_defaults_to_2025_26_when_no_season_provided(self, mock_save, mock_fetch_stats):
        """Test that handler defaults to 2025-26 when season not in event."""
        mock_fetch_stats.return_value = {
            "season": "2025-26",
            "per_game_stats": [],
            "advanced_stats": [],
            "per_game_columns": [],
            "advanced_columns": [],
            "fetch_timestamp": "2025-01-01T00:00:00",
            "source": "basketball_reference",
        }
        mock_save.return_value = True

        event = {"fetch_type": "stats_only"}  # No season specified

        result = fetch_data.handler(event, MagicMock())

        # Should default to current season
        mock_fetch_stats.assert_called_once_with("2025-26")
        assert result["season"] == "2025-26"

    @patch("src.etl.fetch_data.fetch_player_stats")
    @patch("src.etl.fetch_data.save_to_s3")
    @patch("src.etl.fetch_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.fetch_data.ENVIRONMENT", "test")
    def test_handler_returns_season_in_response(self, mock_save, mock_fetch_stats):
        """Test that handler includes season in return value for next step."""
        mock_fetch_stats.return_value = {
            "season": "2024-25",
            "per_game_stats": [],
            "advanced_stats": [],
            "per_game_columns": [],
            "advanced_columns": [],
            "fetch_timestamp": "2025-01-01T00:00:00",
            "source": "basketball_reference",
        }
        mock_save.return_value = True

        event = {"fetch_type": "stats_only", "season": "2024-25"}

        result = fetch_data.handler(event, MagicMock())

        # Verify season is in response for Step Functions
        assert "season" in result
        assert result["season"] == "2024-25"
        assert "fetch_type" in result
        assert result["fetch_type"] == "stats_only"
