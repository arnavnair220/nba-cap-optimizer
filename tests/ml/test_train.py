"""
Tests for model training module.

Tests data loading, splitting, and model setup without running
full training (which would be too slow for unit tests).
"""

import os
import tempfile

import numpy as np
import pandas as pd
import pytest
from sklearn.ensemble import RandomForestRegressor

from src.sagemaker.train import (
    evaluate_model,
    load_data,
    save_model,
    split_data,
    train_model,
)


class TestLoadData:
    """Test data loading from CSV."""

    @pytest.fixture
    def sample_features_csv(self):
        """Create temporary CSV with feature data."""
        data = {
            "player_name": ["Player A", "Player B", "Player C"],
            "season": ["2024-25", "2024-25", "2024-25"],
            "games_played": [70, 65, 60],
            "log_points": [7.5, 7.3, 7.0],
            "per": [20.5, 18.0, 15.5],
            "pos_PG": [1, 0, 0],
            "pos_C": [0, 1, 0],
            "exp_tier_0-6_years": [1, 0, 1],
            "exp_tier_10+_years": [0, 1, 0],
            "annual_salary": [25000000, 30000000, 15000000],
            "salary_cap": [140000000, 140000000, 140000000],
            "log_salary_cap_pct": [2.88, 3.04, 2.49],
            "log_salary_pct_of_max": [2.70, 2.85, 2.30],
        }
        df = pd.DataFrame(data)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            yield f.name

        os.unlink(f.name)

    def test_load_data_returns_dataframe(self, sample_features_csv):
        """Test that load_data returns a dataframe."""
        df, feature_cols = load_data(sample_features_csv)
        assert isinstance(df, pd.DataFrame)
        assert isinstance(feature_cols, list)

    def test_load_data_row_count(self, sample_features_csv):
        """Test that all rows are loaded."""
        df, _ = load_data(sample_features_csv)
        assert len(df) == 3

    def test_feature_columns_exclude_metadata(self, sample_features_csv):
        """Test that feature columns exclude metadata and targets."""
        _, feature_cols = load_data(sample_features_csv)

        exclude_cols = [
            "player_name",
            "season",
            "annual_salary",
            "salary_cap",
            "log_salary_cap_pct",
            "log_salary_pct_of_max",
        ]

        for col in exclude_cols:
            assert col not in feature_cols

    def test_feature_columns_include_stats(self, sample_features_csv):
        """Test that feature columns include actual features."""
        _, feature_cols = load_data(sample_features_csv)

        expected_features = [
            "games_played",
            "log_points",
            "per",
            "pos_PG",
            "pos_C",
            "exp_tier_0-6_years",
            "exp_tier_10+_years",
        ]

        for col in expected_features:
            assert col in feature_cols


class TestSplitData:
    """Test data splitting."""

    @pytest.fixture
    def sample_dataframe(self):
        """Create sample dataframe with 100 records."""
        np.random.seed(42)
        return pd.DataFrame(
            {
                "feature1": np.random.randn(100),
                "feature2": np.random.randn(100),
                "feature3": np.random.randn(100),
                "log_salary_cap_pct": np.random.randn(100) + 3.0,
            }
        )

    def test_split_data_ratio(self, sample_dataframe):
        """Test that data is split according to test_size."""
        feature_cols = ["feature1", "feature2", "feature3"]
        X_train, X_test, y_train, y_test = split_data(
            sample_dataframe, feature_cols, "log_salary_cap_pct", test_size=0.2, random_state=42
        )

        assert len(X_train) == 80
        assert len(X_test) == 20
        assert len(y_train) == 80
        assert len(y_test) == 20

    def test_split_data_returns_correct_types(self, sample_dataframe):
        """Test that split returns DataFrames and Series."""
        feature_cols = ["feature1", "feature2", "feature3"]
        X_train, X_test, y_train, y_test = split_data(
            sample_dataframe, feature_cols, "log_salary_cap_pct", test_size=0.2, random_state=42
        )

        assert isinstance(X_train, pd.DataFrame)
        assert isinstance(X_test, pd.DataFrame)
        assert isinstance(y_train, pd.Series)
        assert isinstance(y_test, pd.Series)

    def test_split_data_removes_missing_targets(self):
        """Test that rows with missing targets are removed."""
        df = pd.DataFrame(
            {
                "feature1": [1, 2, 3, 4, 5],
                "feature2": [5, 4, 3, 2, 1],
                "log_salary_cap_pct": [2.5, np.nan, 3.0, 2.8, np.nan],
            }
        )

        feature_cols = ["feature1", "feature2"]
        X_train, X_test, y_train, y_test = split_data(
            df, feature_cols, "log_salary_cap_pct", test_size=0.2, random_state=42
        )

        total_samples = len(X_train) + len(X_test)
        assert total_samples == 3  # Only 3 valid records

    def test_split_reproducibility(self, sample_dataframe):
        """Test that same random_state produces same splits."""
        feature_cols = ["feature1", "feature2", "feature3"]

        X_train1, X_test1, y_train1, y_test1 = split_data(
            sample_dataframe, feature_cols, "log_salary_cap_pct", test_size=0.2, random_state=42
        )

        X_train2, X_test2, y_train2, y_test2 = split_data(
            sample_dataframe, feature_cols, "log_salary_cap_pct", test_size=0.2, random_state=42
        )

        pd.testing.assert_frame_equal(X_train1, X_train2)
        pd.testing.assert_frame_equal(X_test1, X_test2)


class TestTrainModel:
    """Test model training (mocked to avoid long training time)."""

    @pytest.fixture
    def sample_training_data(self):
        """Create sample training data."""
        np.random.seed(42)
        X_train = pd.DataFrame(
            {
                "feature1": np.random.randn(80),
                "feature2": np.random.randn(80),
                "feature3": np.random.randn(80),
            }
        )
        y_train = pd.Series(np.random.randn(80) + 3.0)

        X_test = pd.DataFrame(
            {
                "feature1": np.random.randn(20),
                "feature2": np.random.randn(20),
                "feature3": np.random.randn(20),
            }
        )
        y_test = pd.Series(np.random.randn(20) + 3.0)

        return X_train, X_test, y_train, y_test

    def test_train_model_returns_random_forest(self, sample_training_data):
        """Test that train_model returns Random Forest model."""
        X_train, X_test, y_train, y_test = sample_training_data

        params = {
            "n_estimators": 10,
            "max_depth": 3,
            "random_state": 42,
        }

        model = train_model(X_train, y_train, X_test, y_test, params)

        assert isinstance(model, RandomForestRegressor)
        assert model.n_estimators == 10
        assert model.max_depth == 3

    def test_train_model_integration(self, sample_training_data):
        """Test actual model training with minimal trees."""
        X_train, X_test, y_train, y_test = sample_training_data

        params = {
            "n_estimators": 10,
            "max_depth": 5,
            "random_state": 42,
        }

        model = train_model(X_train, y_train, X_test, y_test, params)

        assert isinstance(model, RandomForestRegressor)
        assert model.n_estimators == 10

        # Test that model can make predictions
        predictions = model.predict(X_test)
        assert len(predictions) == len(y_test)


class TestEvaluateModel:
    """Test model evaluation."""

    @pytest.fixture
    def sample_model_and_data(self):
        """Create a simple trained model and test data."""
        np.random.seed(42)

        X_train = pd.DataFrame(
            {
                "feature1": np.random.randn(100),
                "feature2": np.random.randn(100),
            }
        )
        y_train = pd.Series(
            X_train["feature1"] * 2 + X_train["feature2"] + np.random.randn(100) * 0.1 + 3.0
        )

        X_test = pd.DataFrame(
            {
                "feature1": np.random.randn(20),
                "feature2": np.random.randn(20),
            }
        )
        y_test = pd.Series(
            X_test["feature1"] * 2 + X_test["feature2"] + np.random.randn(20) * 0.1 + 3.0
        )

        model = RandomForestRegressor(n_estimators=10, max_depth=5, random_state=42)
        model.fit(X_train, y_train)

        return model, X_test, y_test

    def test_evaluate_model_returns_metrics(self, sample_model_and_data):
        """Test that evaluate_model returns expected metrics."""
        model, X_test, y_test = sample_model_and_data

        metrics = evaluate_model(model, X_test, y_test, "log_salary_cap_pct")

        assert "target" in metrics
        assert "rmse" in metrics
        assert "mae" in metrics
        assert "r2" in metrics
        assert "within_20pct" in metrics
        assert "test_samples" in metrics

    def test_evaluate_model_metrics_types(self, sample_model_and_data):
        """Test that metrics are correct types."""
        model, X_test, y_test = sample_model_and_data

        metrics = evaluate_model(model, X_test, y_test, "log_salary_cap_pct")

        assert isinstance(metrics["rmse"], float)
        assert isinstance(metrics["mae"], float)
        assert isinstance(metrics["r2"], float)
        assert isinstance(metrics["within_20pct"], float)
        assert isinstance(metrics["test_samples"], int)

    def test_evaluate_model_test_samples(self, sample_model_and_data):
        """Test that test_samples count is correct."""
        model, X_test, y_test = sample_model_and_data

        metrics = evaluate_model(model, X_test, y_test, "log_salary_cap_pct")

        assert metrics["test_samples"] == len(y_test)


class TestSaveModel:
    """Test model saving."""

    @pytest.fixture
    def sample_model(self):
        """Create a simple trained model."""
        np.random.seed(42)
        X = pd.DataFrame({"feature1": np.random.randn(50), "feature2": np.random.randn(50)})
        y = pd.Series(np.random.randn(50) + 3.0)

        model = RandomForestRegressor(n_estimators=5, max_depth=3, random_state=42)
        model.fit(X, y)

        return model

    def test_save_model_creates_files(self, sample_model):
        """Test that save_model creates expected files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            feature_cols = ["feature1", "feature2"]
            metrics = {"rmse": 0.5, "mae": 0.4, "r2": 0.8}

            save_model(sample_model, feature_cols, metrics, tmpdir, "test_model")

            assert os.path.exists(os.path.join(tmpdir, "test_model.pkl"))
            assert os.path.exists(os.path.join(tmpdir, "test_model_features.json"))
            assert os.path.exists(os.path.join(tmpdir, "test_model_metrics.json"))
            assert os.path.exists(os.path.join(tmpdir, "test_model_metadata.json"))

    def test_save_model_features_content(self, sample_model):
        """Test that features file contains correct data."""
        import json

        with tempfile.TemporaryDirectory() as tmpdir:
            feature_cols = ["feature1", "feature2"]
            metrics = {"rmse": 0.5}

            save_model(sample_model, feature_cols, metrics, tmpdir, "test_model")

            with open(os.path.join(tmpdir, "test_model_features.json")) as f:
                data = json.load(f)

            assert "features" in data
            assert data["features"] == ["feature1", "feature2"]

    def test_save_model_metrics_content(self, sample_model):
        """Test that metrics file contains correct data."""
        import json

        with tempfile.TemporaryDirectory() as tmpdir:
            feature_cols = ["feature1", "feature2"]
            metrics = {"rmse": 0.5, "mae": 0.4, "r2": 0.8}

            save_model(sample_model, feature_cols, metrics, tmpdir, "test_model")

            with open(os.path.join(tmpdir, "test_model_metrics.json")) as f:
                data = json.load(f)

            assert data["rmse"] == 0.5
            assert data["mae"] == 0.4
            assert data["r2"] == 0.8
