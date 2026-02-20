"""
Tests for fetch_data Lambda function.
"""

from unittest.mock import MagicMock, Mock, patch

import requests

from src.etl import fetch_data


class TestNBAAPIConfiguration:
    """Test NBA API timeout and header configuration."""

    def test_nba_api_timeout_increased(self):
        """Test that NBA API timeout is increased to 90 seconds."""
        from nba_api.stats.library.http import NBAStatsHTTP

        assert NBAStatsHTTP.timeout == 90

    def test_nba_api_headers_configured(self):
        """Test that NBA API headers are properly configured."""
        from nba_api.stats.library.http import NBAStatsHTTP

        assert "User-Agent" in NBAStatsHTTP.headers
        assert "Chrome" in NBAStatsHTTP.headers["User-Agent"]
        assert NBAStatsHTTP.headers["Referer"] == "https://www.nba.com/"
        assert NBAStatsHTTP.headers["Origin"] == "https://www.nba.com"


class TestFetchPlayerStatsRetry:
    """Test retry logic for fetch_player_stats."""

    @patch("src.etl.fetch_data.leaguedashplayerstats.LeagueDashPlayerStats")
    @patch("src.etl.fetch_data.time.sleep")
    def test_fetch_player_stats_success_first_attempt(self, mock_sleep, mock_stats_api):
        """Test successful fetch on first attempt."""
        mock_stats = Mock()
        mock_stats.get_dict.return_value = {
            "resultSets": [{"headers": ["ID", "NAME"], "rowSet": [[1, "Player 1"]]}]
        }
        mock_stats_api.return_value = mock_stats

        result = fetch_data.fetch_player_stats("2025-26")

        assert result is not None
        assert result["season"] == "2025-26"
        assert "players" in result
        assert "fetch_timestamp" in result
        mock_stats_api.assert_called_once()
        mock_sleep.assert_called_once_with(1)

    @patch("src.etl.fetch_data.leaguedashplayerstats.LeagueDashPlayerStats")
    @patch("src.etl.fetch_data.time.sleep")
    def test_fetch_player_stats_retry_on_timeout(self, mock_sleep, mock_stats_api):
        """Test retry logic when first attempt times out."""
        mock_stats = Mock()
        mock_stats.get_dict.return_value = {
            "resultSets": [{"headers": ["ID", "NAME"], "rowSet": [[1, "Player 1"]]}]
        }

        # First call raises timeout, second succeeds
        mock_stats_api.side_effect = [
            requests.exceptions.ReadTimeout("Read timed out"),
            mock_stats,
        ]

        result = fetch_data.fetch_player_stats("2025-26", max_retries=3)

        assert result is not None
        assert result["season"] == "2025-26"
        assert mock_stats_api.call_count == 2
        # Should sleep 1s initially, then 2s for first retry
        assert mock_sleep.call_count == 2

    @patch("src.etl.fetch_data.leaguedashplayerstats.LeagueDashPlayerStats")
    @patch("src.etl.fetch_data.time.sleep")
    def test_fetch_player_stats_all_retries_fail(self, mock_sleep, mock_stats_api):
        """Test that function returns None after all retries fail."""
        mock_stats_api.side_effect = requests.exceptions.ReadTimeout("Read timed out")

        result = fetch_data.fetch_player_stats("2025-26", max_retries=3)

        assert result is None
        assert mock_stats_api.call_count == 3

    @patch("src.etl.fetch_data.leaguedashplayerstats.LeagueDashPlayerStats")
    @patch("src.etl.fetch_data.time.sleep")
    def test_fetch_player_stats_exponential_backoff(self, mock_sleep, mock_stats_api):
        """Test that retry delays use exponential backoff."""
        mock_stats_api.side_effect = requests.exceptions.ReadTimeout("Read timed out")

        fetch_data.fetch_player_stats("2025-26", max_retries=3)

        # Should sleep: 1s (initial), 2s (retry 1), 4s (retry 2)
        expected_calls = [((1,),), ((2,),), ((4,),)]
        assert mock_sleep.call_args_list == expected_calls


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
