"""
Tests for SageMaker inference script (MVP level).
"""

import pickle

import numpy as np
import pandas as pd
import pytest
from sklearn.ensemble import RandomForestRegressor


class TestInferenceScript:
    """Test SageMaker inference functions."""

    @pytest.fixture
    def sample_model(self):
        """Create a simple trained Random Forest model."""
        # Train on dummy data
        X = np.random.randn(50, 5)
        y = np.random.randn(50)

        model = RandomForestRegressor(n_estimators=5, max_depth=3, random_state=42)
        model.fit(X, y)

        return model

    @pytest.fixture
    def model_artifacts(self, tmp_path, sample_model):
        """Create model artifacts directory like SageMaker expects."""
        model_dir = tmp_path / "model"
        model_dir.mkdir()

        # Save both models
        with open(model_dir / "model_salary_cap_pct.pkl", "wb") as f:
            pickle.dump(sample_model, f)

        with open(model_dir / "model_salary_pct_of_max.pkl", "wb") as f:
            pickle.dump(sample_model, f)

        # Save features metadata
        features = {"features": ["feature_1", "feature_2", "feature_3", "feature_4", "feature_5"]}
        import json

        with open(model_dir / "model_salary_cap_pct_features.json", "w") as f:
            json.dump(features, f)

        return str(model_dir)

    def test_model_fn_loads_models(self, model_artifacts):
        """Test that model_fn loads model artifacts correctly."""
        from src.sagemaker.inference import model_fn

        result = model_fn(model_artifacts)

        assert "model_cap" in result
        assert "model_fmv" in result
        assert "features" in result
        assert isinstance(result["model_cap"], RandomForestRegressor)
        assert len(result["features"]) == 5

    def test_input_fn_parses_csv(self):
        """Test that input_fn parses CSV request body."""
        from src.sagemaker.inference import input_fn

        csv_data = "player_name,season,feature_1,feature_2\nPlayer A,2024-25,1.5,2.5\nPlayer B,2024-25,3.0,4.0"

        result = input_fn(csv_data, content_type="text/csv")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "player_name" in result.columns

    def test_predict_fn_makes_predictions(self, model_artifacts):
        """Test that predict_fn returns predictions."""
        from src.sagemaker.inference import model_fn, predict_fn

        # Load models
        model_dict = model_fn(model_artifacts)

        # Create sample input
        input_data = pd.DataFrame(
            {
                "player_name": ["Player A", "Player B"],
                "season": ["2024-25", "2024-25"],
                "feature_1": [1.5, 2.0],
                "feature_2": [2.5, 3.0],
                "feature_3": [3.5, 4.0],
                "feature_4": [4.5, 5.0],
                "feature_5": [5.5, 6.0],
            }
        )

        # Make predictions
        result = predict_fn(input_data, model_dict)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "predicted_log_salary_cap_pct" in result.columns
        assert "predicted_salary_cap_pct" in result.columns

    def test_output_fn_formats_csv(self):
        """Test that output_fn returns CSV string."""
        from src.sagemaker.inference import output_fn

        predictions = pd.DataFrame(
            {
                "player_name": ["Player A"],
                "predicted_salary_cap_pct": [15.5],
            }
        )

        result, content_type = output_fn(predictions, accept="text/csv")

        assert content_type == "text/csv"
        assert isinstance(result, str)
        assert "Player A" in result
        assert "predicted_salary_cap_pct" in result

    def test_input_fn_rejects_invalid_content_type(self):
        """Test that input_fn raises error for unsupported content types."""
        from src.sagemaker.inference import input_fn

        with pytest.raises(ValueError, match="Unsupported content type"):
            input_fn("data", content_type="application/json")

    def test_output_fn_rejects_invalid_accept_type(self):
        """Test that output_fn raises error for unsupported accept types."""
        from src.sagemaker.inference import output_fn

        predictions = pd.DataFrame({"col": [1, 2]})

        with pytest.raises(ValueError, match="Unsupported accept type"):
            output_fn(predictions, accept="application/json")
