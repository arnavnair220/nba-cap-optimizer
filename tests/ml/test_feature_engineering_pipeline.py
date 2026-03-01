"""
Tests for feature engineering pipeline (train vs predict modes).

Tests the main engineer_features() function with different modes and S3 data loading.
"""

from unittest.mock import Mock, patch

import pandas as pd
import pytest

from src.sagemaker.feature_engineering import engineer_features


class TestEngineerFeaturesPredictMode:
    """Test engineer_features in predict mode."""

    @patch("src.sagemaker.feature_engineering.load_data_from_s3")
    def test_predict_mode_does_not_calculate_targets(self, mock_load_data):
        """Test that predict mode doesn't try to calculate salary targets."""
        mock_load_data.return_value = pd.DataFrame(
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

        df, features = engineer_features(mode="predict", input_path="s3://test-bucket/input.csv")

        # Verify no target columns
        assert "log_salary_cap_pct" not in df.columns
        assert "log_salary_pct_of_max" not in df.columns
        assert "annual_salary" not in df.columns

        # Verify feature columns exist
        assert len(features) == 60
        assert "log_points" in df.columns
        assert "pos_SF" in df.columns

    @patch("src.sagemaker.feature_engineering.load_data_from_s3")
    @patch("src.sagemaker.feature_engineering.boto3.client")
    def test_predict_mode_saves_to_s3_without_targets(self, mock_boto3, mock_load_data):
        """Test that predict mode saves features to S3 without target columns."""
        mock_load_data.return_value = pd.DataFrame(
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

        engineer_features(
            mode="predict",
            input_path="s3://test-bucket/input.csv",
            output_path="s3://test-bucket/features.csv",
        )

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

    @patch("src.sagemaker.feature_engineering.load_data_from_s3")
    @patch("src.sagemaker.feature_engineering.load_salary_cap_data_from_s3")
    def test_train_mode_requires_data_bucket(self, mock_load_cap, mock_load_data):
        """Test that train mode requires data_bucket parameter."""
        mock_load_data.return_value = pd.DataFrame(
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

        df, features = engineer_features(
            mode="train",
            input_path="s3://test-bucket/training.csv",
            data_bucket="test-bucket",
        )

        # Verify data was loaded from S3
        mock_load_data.assert_called_once_with("s3://test-bucket/training.csv")
        mock_load_cap.assert_called_once_with("test-bucket")

        # Verify target columns exist in train mode
        assert "log_salary_cap_pct" in df.columns
        assert "log_salary_pct_of_max" in df.columns

    @patch("src.sagemaker.feature_engineering.load_data_from_s3")
    @patch("src.sagemaker.feature_engineering.load_salary_cap_data_from_s3")
    def test_train_mode_calculates_targets(self, mock_load_cap, mock_load_data):
        """Test that train mode calculates salary targets."""
        mock_load_data.return_value = pd.DataFrame(
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

        df, features = engineer_features(
            mode="train",
            input_path="s3://test-bucket/training.csv",
            data_bucket="test-bucket",
        )

        # Verify targets were calculated
        assert "log_salary_cap_pct" in df.columns
        assert "log_salary_pct_of_max" in df.columns
        assert df.loc[0, "log_salary_cap_pct"] > 0

    @patch("src.sagemaker.feature_engineering.load_data_from_s3")
    def test_invalid_mode_raises_error(self, mock_load_data):
        """Test that invalid mode raises ValueError."""
        mock_load_data.return_value = pd.DataFrame(
            {
                "player_name": ["Test"],
                "points": [100],
                "rebounds": [50],
                "assists": [30],
                "turnovers": [10],
                "minutes": [500],
                "fg3a": [50],
                "blocks": [5],
                "steals": [7],
                "vorp": [1.0],
                "ws": [2.0],
                "per": [15.0],
                "ts_pct": [0.55],
                "efg_pct": [0.52],
                "usg_pct": [0.20],
                "fg_pct": [0.45],
                "fg2_pct": [0.47],
                "fg3_pct": [0.35],
                "ft_pct": [0.80],
                "bpm": [2.0],
                "obpm": [1.5],
                "dbpm": [0.5],
                "ws_per_48": [0.10],
                "orb_pct": [0.03],
                "drb_pct": [0.12],
                "trb_pct": [0.08],
                "ast_pct": [0.20],
                "stl_pct": [0.01],
                "blk_pct": [0.02],
                "tov_pct": [0.10],
                "ows": [1.5],
                "dws": [0.5],
                "age": [25],
                "position": ["PG"],
                "games_played": [30],
                "games_started": [25],
                "fouls": [50],
            }
        )
        with pytest.raises(ValueError, match="Invalid mode"):
            engineer_features(mode="invalid", input_path="s3://test-bucket/input.csv")
