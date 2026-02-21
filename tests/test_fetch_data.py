"""
Tests for fetch_data Lambda function.
"""

from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import requests

from src.etl import fetch_data


class TestFetchPlayerStatsBasketballReference:
    """Test Basketball-Reference scraping for player stats."""

    @patch("src.etl.fetch_data.pd.read_html")
    @patch("src.etl.fetch_data.time.sleep")
    def test_fetch_player_stats_success_first_attempt(self, mock_sleep, mock_read_html):
        """Test successful fetch from Basketball-Reference on first attempt."""
        # Mock per-game stats DataFrame
        mock_pergame_df = pd.DataFrame(
            {
                "Player": ["LeBron James", "Stephen Curry"],
                "Pos": ["SF", "PG"],
                "Age": [40, 36],
                "Tm": ["LAL", "GSW"],
                "G": [50, 48],
                "PTS": [25.0, 28.5],
            }
        )

        # Mock advanced stats DataFrame
        mock_advanced_df = pd.DataFrame(
            {
                "Player": ["LeBron James", "Stephen Curry"],
                "Pos": ["SF", "PG"],
                "Age": [40, 36],
                "Tm": ["LAL", "GSW"],
                "PER": [24.5, 28.2],
                "TS%": [0.585, 0.625],
            }
        )

        # read_html is called twice: once for per-game, once for advanced
        mock_read_html.side_effect = [[mock_pergame_df], [mock_advanced_df]]

        result = fetch_data.fetch_player_stats("2025-26")

        assert result is not None
        assert result["season"] == "2025-26"
        assert result["source"] == "basketball_reference"
        assert "per_game_stats" in result
        assert "advanced_stats" in result
        assert "fetch_timestamp" in result
        assert len(result["per_game_stats"]) == 2
        assert len(result["advanced_stats"]) == 2
        assert mock_read_html.call_count == 2
        # Should sleep 1s initially, then 1s between requests
        assert mock_sleep.call_count == 2

    @patch("src.etl.fetch_data.pd.read_html")
    @patch("src.etl.fetch_data.time.sleep")
    def test_fetch_player_stats_retry_on_error(self, mock_sleep, mock_read_html):
        """Test retry logic when first scraping attempt fails."""
        mock_pergame_df = pd.DataFrame({"Player": ["LeBron James"], "PTS": [25.0]})
        mock_advanced_df = pd.DataFrame({"Player": ["LeBron James"], "PER": [24.5]})

        # First call raises error, second and third succeed
        mock_read_html.side_effect = [
            requests.exceptions.ConnectionError("Network error"),
            [mock_pergame_df],
            [mock_advanced_df],
        ]

        result = fetch_data.fetch_player_stats("2025-26", max_retries=3)

        assert result is not None
        assert result["season"] == "2025-26"
        assert mock_read_html.call_count == 3
        # Should sleep: 1s (initial), 2s (backoff for retry 1), 1s (between requests)
        assert mock_sleep.call_count == 3

    @patch("src.etl.fetch_data.pd.read_html")
    @patch("src.etl.fetch_data.time.sleep")
    def test_fetch_player_stats_all_retries_fail(self, mock_sleep, mock_read_html):
        """Test that function returns None after all retries fail."""
        mock_read_html.side_effect = requests.exceptions.ConnectionError("Network error")

        result = fetch_data.fetch_player_stats("2025-26", max_retries=3)

        assert result is None
        assert mock_read_html.call_count == 3

    @patch("src.etl.fetch_data.pd.read_html")
    @patch("src.etl.fetch_data.time.sleep")
    def test_fetch_player_stats_exponential_backoff(self, mock_sleep, mock_read_html):
        """Test that retry delays use exponential backoff."""
        mock_read_html.side_effect = requests.exceptions.ConnectionError("Network error")

        fetch_data.fetch_player_stats("2025-26", max_retries=3)

        # Should sleep: 1s (initial), 2s (retry 1), 4s (retry 2)
        expected_calls = [((1,),), ((2,),), ((4,),)]
        assert mock_sleep.call_args_list == expected_calls

    @patch("src.etl.fetch_data.pd.read_html")
    @patch("src.etl.fetch_data.time.sleep")
    def test_fetch_player_stats_filters_header_rows(self, mock_sleep, mock_read_html):
        """Test that duplicate header rows are filtered out."""
        # Mock DataFrame with header row in the middle (common in B-R tables)
        mock_pergame_df = pd.DataFrame(
            {
                "Player": ["LeBron James", "Player", "Stephen Curry"],  # "Player" is header
                "PTS": [25.0, "PTS", 28.5],
            }
        )

        mock_advanced_df = pd.DataFrame(
            {
                "Player": ["LeBron James", "Player", "Stephen Curry"],
                "PER": [24.5, "PER", 28.2],
            }
        )

        mock_read_html.side_effect = [[mock_pergame_df], [mock_advanced_df]]

        result = fetch_data.fetch_player_stats("2025-26")

        # Should filter out the "Player" header row
        assert len(result["per_game_stats"]) == 2
        assert len(result["advanced_stats"]) == 2
        player_names = [p["Player"] for p in result["per_game_stats"]]
        assert "Player" not in player_names

    @patch("src.etl.fetch_data.pd.read_html")
    @patch("src.etl.fetch_data.time.sleep")
    def test_fetch_player_stats_season_format_conversion(self, mock_sleep, mock_read_html):
        """Test that season format is correctly converted for B-R URLs."""
        mock_pergame_df = pd.DataFrame({"Player": ["Test Player"], "PTS": [20.0]})
        mock_advanced_df = pd.DataFrame({"Player": ["Test Player"], "PER": [20.0]})

        mock_read_html.side_effect = [[mock_pergame_df], [mock_advanced_df]]

        # Test with "2024-25" format (should convert to "2025")
        result = fetch_data.fetch_player_stats("2024-25")

        assert result is not None
        # Check that the URLs called were for 2025
        assert mock_read_html.call_count == 2
        calls = mock_read_html.call_args_list
        assert "2025" in calls[0][0][0]  # First arg of first call
        assert "2025" in calls[1][0][0]  # First arg of second call


class TestFetchESPNSalariesHeaderFiltering:
    """Test header row filtering in ESPN salary scraping."""

    @patch("src.etl.fetch_data.time.sleep")
    @patch("src.etl.fetch_data.requests.get")
    def test_fetch_espn_salaries_skips_header_rows(self, mock_get, mock_sleep):
        """Test that header rows with 'SALARY' text are skipped."""
        # First page has data, second page is empty to stop pagination
        mock_response_page1 = Mock()
        mock_response_page1.status_code = 200
        mock_response_page1.content = """
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
                    <td>$51,915,615</td>
                </tr>
                <tr>
                    <td>2</td>
                    <td>Nikola Jokic, C</td>
                    <td>DEN</td>
                    <td>$51,415,938</td>
                </tr>
            </table>
        </html>
        """

        mock_response_page2 = Mock()
        mock_response_page2.status_code = 200
        mock_response_page2.content = "<html><table><tr></tr></table></html>"

        mock_get.side_effect = [mock_response_page1, mock_response_page2]

        result = fetch_data.fetch_espn_salaries("2025-26")

        assert len(result) == 2
        assert result[0]["player_name"] == "Stephen Curry"
        assert result[0]["annual_salary"] == 51915615
        assert result[1]["player_name"] == "Nikola Jokic"
        assert result[1]["annual_salary"] == 51415938

    @patch("src.etl.fetch_data.time.sleep")
    @patch("src.etl.fetch_data.requests.get")
    def test_fetch_espn_salaries_skips_empty_salary(self, mock_get, mock_sleep):
        """Test that rows with empty salary text are skipped."""
        mock_response_page1 = Mock()
        mock_response_page1.status_code = 200
        mock_response_page1.content = """
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
                    <td>Valid Player</td>
                    <td>LAL</td>
                    <td>$1,000,000</td>
                </tr>
                <tr>
                    <td>2</td>
                    <td>Invalid Player</td>
                    <td>LAL</td>
                    <td></td>
                </tr>
            </table>
        </html>
        """

        mock_response_page2 = Mock()
        mock_response_page2.status_code = 200
        mock_response_page2.content = "<html><table><tr></tr></table></html>"

        mock_get.side_effect = [mock_response_page1, mock_response_page2]

        result = fetch_data.fetch_espn_salaries("2025-26")

        assert len(result) == 1
        assert result[0]["player_name"] == "Valid Player"

    @patch("src.etl.fetch_data.time.sleep")
    @patch("src.etl.fetch_data.requests.get")
    def test_fetch_espn_salaries_handles_invalid_format(self, mock_get, mock_sleep):
        """Test that invalid salary formats are logged and skipped."""
        mock_response_page1 = Mock()
        mock_response_page1.status_code = 200
        mock_response_page1.content = """
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
                    <td>Valid Player</td>
                    <td>LAL</td>
                    <td>$1,000,000</td>
                </tr>
                <tr>
                    <td>2</td>
                    <td>Invalid Salary Player</td>
                    <td>LAL</td>
                    <td>Not a number</td>
                </tr>
            </table>
        </html>
        """

        mock_response_page2 = Mock()
        mock_response_page2.status_code = 200
        mock_response_page2.content = "<html><table><tr></tr></table></html>"

        mock_get.side_effect = [mock_response_page1, mock_response_page2]

        with patch("src.etl.fetch_data.logger") as mock_logger:
            result = fetch_data.fetch_espn_salaries("2025-26")

            assert len(result) == 1
            assert result[0]["player_name"] == "Valid Player"
            # Should log warning for invalid salary
            assert mock_logger.warning.called
            warning_message = mock_logger.warning.call_args[0][0]
            assert "Invalid Salary Player" in warning_message
            assert "Not a number" in warning_message


class TestHandlerEnvironmentValidation:
    """Test handler validates required environment variables."""

    @patch("src.etl.fetch_data.ENVIRONMENT", None)
    @patch("src.etl.fetch_data.S3_BUCKET", None)
    @patch("src.etl.fetch_data.fetch_player_stats")
    def test_handler_fails_without_data_bucket(self, mock_fetch):
        """Test handler returns error when DATA_BUCKET is not set."""
        event = {"fetch_type": "stats_only", "season": "2025-26"}

        result = fetch_data.handler(event, MagicMock())

        assert result["statusCode"] == 500
        assert "DATA_BUCKET" in result["body"]
        mock_fetch.assert_not_called()

    @patch("src.etl.fetch_data.ENVIRONMENT", None)
    @patch("src.etl.fetch_data.S3_BUCKET", "test-bucket")
    @patch("src.etl.fetch_data.fetch_player_stats")
    def test_handler_fails_without_environment(self, mock_fetch):
        """Test handler returns error when ENVIRONMENT is not set but DATA_BUCKET is."""
        event = {"fetch_type": "stats_only", "season": "2025-26"}

        result = fetch_data.handler(event, MagicMock())

        assert result["statusCode"] == 500
        assert "ENVIRONMENT" in result["body"]
        mock_fetch.assert_not_called()
