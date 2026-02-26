"""
Tests for feature engineering pipeline (train vs predict modes).

Tests the main engineer_features() function with different modes,
load_prediction_data(), and data loading logic.
"""

from unittest.mock import Mock, patch

import pandas as pd
import pytest

from src.sagemaker.feature_engineering import engineer_features, load_prediction_data


class TestLoadPredictionData:
    """Test load_prediction_data function."""

    @patch("src.sagemaker.feature_engineering.get_db_connection")
    def test_load_prediction_data_queries_latest_season_and_week(self, mock_get_conn):
        """Test that load_prediction_data queries for latest season and last 7 days."""
        mock_conn = Mock()
        mock_get_conn.return_value = mock_conn

        mock_df = pd.DataFrame(
            {
                "player_name": ["LeBron James", "Stephen Curry"],
                "season": ["2024-25", "2024-25"],
                "points": [1800, 2000],
                "minutes": [2500, 2400],
                "games_played": [70, 72],
                "age": [39, 36],
                "position": ["SF", "PG"],
                "created_at": ["2025-01-15", "2025-01-15"],
            }
        )

        with patch("src.sagemaker.feature_engineering.pd.read_sql", return_value=mock_df):
            result = load_prediction_data()

        # Verify query filters by latest season and last 7 days
        assert result is not None
        assert len(result) == 2
        assert all(result["season"] == "2024-25")

        # Verify connection was closed
        mock_conn.close.assert_called_once()

    @patch("src.sagemaker.feature_engineering.get_db_connection")
    def test_load_prediction_data_filters_by_minutes_and_games(self, mock_get_conn):
        """Test that load_prediction_data filters out players with 0 minutes/games."""
        mock_conn = Mock()
        mock_get_conn.return_value = mock_conn

        # Empty dataframe (players filtered out)
        mock_df = pd.DataFrame(
            columns=["player_name", "season", "points", "minutes", "games_played"]
        )

        with patch("src.sagemaker.feature_engineering.pd.read_sql", return_value=mock_df):
            result = load_prediction_data()

        assert len(result) == 0


class TestEngineerFeaturesPredictMode:
    """Test engineer_features in predict mode."""

    @patch("src.sagemaker.feature_engineering.load_prediction_data")
    def test_predict_mode_does_not_calculate_targets(self, mock_load_prediction):
        """Test that predict mode doesn't try to calculate salary targets."""
        mock_load_prediction.return_value = pd.DataFrame(
            {
                "player_name": ["Test Player"],
                "season": ["2024-25"],
                "points": [1800],
                "rebounds": [400],
                "assists": [500],
                "turnovers": [150],
                "minutes": [2500],
                "fg3a": [400],
                "blocks": [50],
                "steals": [70],
                "vorp": [3.5],
                "ws": [7.0],
                "per": [20.5],
                "ts_pct": [0.58],
                "efg_pct": [0.55],
                "usg_pct": [0.25],
                "fg_pct": [0.46],
                "fg2_pct": [0.48],
                "fg3_pct": [0.38],
                "ft_pct": [0.85],
                "bpm": [5.2],
                "obpm": [3.5],
                "dbpm": [1.7],
                "ws_per_48": [0.18],
                "orb_pct": [0.05],
                "drb_pct": [0.15],
                "trb_pct": [0.10],
                "ast_pct": [0.30],
                "stl_pct": [0.02],
                "blk_pct": [0.03],
                "tov_pct": [0.12],
                "ows": [4.5],
                "dws": [2.5],
                "age": [28],
                "position": ["SF"],
                "games_played": [70],
                "games_started": [68],
                "fouls": [150],
            }
        )

        df, features = engineer_features(mode="predict")

        # Verify no target columns
        assert "log_salary_cap_pct" not in df.columns
        assert "log_salary_pct_of_max" not in df.columns
        assert "annual_salary" not in df.columns

        # Verify feature columns exist
        assert len(features) == 60
        assert "log_points" in df.columns
        assert "pos_SF" in df.columns

    @patch("src.sagemaker.feature_engineering.load_prediction_data")
    @patch("src.sagemaker.feature_engineering.boto3.client")
    def test_predict_mode_saves_to_s3_without_targets(self, mock_boto3, mock_load_prediction):
        """Test that predict mode saves features to S3 without target columns."""
        mock_load_prediction.return_value = pd.DataFrame(
            {
                "player_name": ["Test Player"],
                "season": ["2024-25"],
                "points": [1800],
                "rebounds": [400],
                "assists": [500],
                "turnovers": [150],
                "minutes": [2500],
                "fg3a": [400],
                "blocks": [50],
                "steals": [70],
                "vorp": [3.5],
                "ws": [7.0],
                "per": [20.5],
                "ts_pct": [0.58],
                "efg_pct": [0.55],
                "usg_pct": [0.25],
                "fg_pct": [0.46],
                "fg2_pct": [0.48],
                "fg3_pct": [0.38],
                "ft_pct": [0.85],
                "bpm": [5.2],
                "obpm": [3.5],
                "dbpm": [1.7],
                "ws_per_48": [0.18],
                "orb_pct": [0.05],
                "drb_pct": [0.15],
                "trb_pct": [0.10],
                "ast_pct": [0.30],
                "stl_pct": [0.02],
                "blk_pct": [0.03],
                "tov_pct": [0.12],
                "ows": [4.5],
                "dws": [2.5],
                "age": [28],
                "position": ["SF"],
                "games_played": [70],
                "games_started": [68],
                "fouls": [150],
            }
        )

        mock_s3_client = Mock()
        mock_boto3.return_value = mock_s3_client

        engineer_features(mode="predict", output_path="s3://test-bucket/features.csv")

        # Verify S3 upload was called
        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args[1]

        # Verify CSV content doesn't include target columns
        csv_content = call_args["Body"]
        assert "log_salary_cap_pct" not in csv_content
        assert "log_salary_pct_of_max" not in csv_content
        assert "player_name" in csv_content
        assert "season" in csv_content


class TestEngineerFeaturesTrainMode:
    """Test engineer_features in train mode."""

    @patch("src.sagemaker.feature_engineering.load_training_data")
    @patch("src.sagemaker.feature_engineering.load_salary_cap_data")
    def test_train_mode_uses_seasons_before_parameter(self, mock_load_cap, mock_load_training):
        """Test that train mode passes seasons_before to load_training_data."""
        mock_load_training.return_value = pd.DataFrame(
            {
                "player_name": ["Test Player"],
                "season": ["2023-24"],
                "annual_salary": [30000000],
                "points": [1800],
                "rebounds": [400],
                "assists": [500],
                "turnovers": [150],
                "minutes": [2500],
                "fg3a": [400],
                "blocks": [50],
                "steals": [70],
                "vorp": [3.5],
                "ws": [7.0],
                "per": [20.5],
                "ts_pct": [0.58],
                "efg_pct": [0.55],
                "usg_pct": [0.25],
                "fg_pct": [0.46],
                "fg2_pct": [0.48],
                "fg3_pct": [0.38],
                "ft_pct": [0.85],
                "bpm": [5.2],
                "obpm": [3.5],
                "dbpm": [1.7],
                "ws_per_48": [0.18],
                "orb_pct": [0.05],
                "drb_pct": [0.15],
                "trb_pct": [0.10],
                "ast_pct": [0.30],
                "stl_pct": [0.02],
                "blk_pct": [0.03],
                "tov_pct": [0.12],
                "ows": [4.5],
                "dws": [2.5],
                "age": [28],
                "position": ["SF"],
                "games_played": [70],
                "games_started": [68],
                "fouls": [150],
            }
        )

        mock_load_cap.return_value = pd.DataFrame(
            {"season": ["2023-24"], "salary_cap": [136021000]}
        )

        df, features = engineer_features(mode="train", seasons_before="2024-25")

        # Verify load_training_data was called with correct parameter
        mock_load_training.assert_called_once_with(seasons_before="2024-25")

        # Verify target columns exist in train mode
        assert "log_salary_cap_pct" in df.columns
        assert "log_salary_pct_of_max" in df.columns

    @patch("src.sagemaker.feature_engineering.load_training_data")
    @patch("src.sagemaker.feature_engineering.load_salary_cap_data")
    def test_train_mode_calculates_targets(self, mock_load_cap, mock_load_training):
        """Test that train mode calculates salary targets."""
        mock_load_training.return_value = pd.DataFrame(
            {
                "player_name": ["Test Player"],
                "season": ["2023-24"],
                "annual_salary": [30000000],
                "points": [1800],
                "rebounds": [400],
                "assists": [500],
                "turnovers": [150],
                "minutes": [2500],
                "fg3a": [400],
                "blocks": [50],
                "steals": [70],
                "vorp": [3.5],
                "ws": [7.0],
                "per": [20.5],
                "ts_pct": [0.58],
                "efg_pct": [0.55],
                "usg_pct": [0.25],
                "fg_pct": [0.46],
                "fg2_pct": [0.48],
                "fg3_pct": [0.38],
                "ft_pct": [0.85],
                "bpm": [5.2],
                "obpm": [3.5],
                "dbpm": [1.7],
                "ws_per_48": [0.18],
                "orb_pct": [0.05],
                "drb_pct": [0.15],
                "trb_pct": [0.10],
                "ast_pct": [0.30],
                "stl_pct": [0.02],
                "blk_pct": [0.03],
                "tov_pct": [0.12],
                "ows": [4.5],
                "dws": [2.5],
                "age": [28],
                "position": ["SF"],
                "games_played": [70],
                "games_started": [68],
                "fouls": [150],
            }
        )

        mock_load_cap.return_value = pd.DataFrame(
            {"season": ["2023-24"], "salary_cap": [136021000]}
        )

        df, features = engineer_features(mode="train")

        # Verify targets were calculated
        assert "log_salary_cap_pct" in df.columns
        assert "log_salary_pct_of_max" in df.columns
        assert df.loc[0, "log_salary_cap_pct"] > 0

    def test_invalid_mode_raises_error(self):
        """Test that invalid mode raises ValueError."""
        with pytest.raises(ValueError, match="Invalid mode"):
            engineer_features(mode="invalid")
