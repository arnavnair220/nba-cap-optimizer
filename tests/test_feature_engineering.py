"""
Tests for feature engineering module.

Tests core feature calculation logic, experience tier mapping,
and position interaction features.
"""

import numpy as np
import pandas as pd

from src.models.feature_engineering import (
    calculate_efficiency_features,
    calculate_position_features,
    calculate_position_interaction_features,
    calculate_volume_features,
    estimate_experience_from_age,
    get_max_salary_for_tier,
    map_experience_to_tier,
    signed_log,
)


class TestSignedLog:
    """Test signed logarithm transformation."""

    def test_positive_value(self):
        """Test signed_log with positive value."""
        result = signed_log(10.0)
        expected = np.log(10.0 + 1e-6)
        assert abs(result - expected) < 1e-5

    def test_negative_value(self):
        """Test signed_log with negative value."""
        result = signed_log(-10.0)
        expected = -np.log(10.0 + 1e-6)
        assert abs(result - expected) < 1e-5

    def test_zero_value(self):
        """Test signed_log with zero."""
        result = signed_log(0.0)
        assert result == 0.0

    def test_small_positive(self):
        """Test signed_log with very small positive value."""
        result = signed_log(0.001)
        assert result < 0  # log of small number is negative

    def test_small_negative(self):
        """Test signed_log with very small negative value."""
        result = signed_log(-0.001)
        assert result > 0  # negative of log of small number is positive


class TestExperienceTiers:
    """Test experience tier mapping based on NBA CBA."""

    def test_estimate_experience_rookie(self):
        """Test experience estimation for rookie (age 19)."""
        experience = estimate_experience_from_age(19)
        assert experience == 0

    def test_estimate_experience_veteran(self):
        """Test experience estimation for veteran (age 30)."""
        experience = estimate_experience_from_age(30)
        assert experience == 11

    def test_estimate_experience_underage(self):
        """Test experience estimation for age < 19 returns 0."""
        experience = estimate_experience_from_age(18)
        assert experience == 0

    def test_map_to_tier_rookie(self):
        """Test mapping 0-6 years to rookie tier."""
        for years in range(0, 7):
            tier = map_experience_to_tier(years)
            assert tier == "0-6_years"

    def test_map_to_tier_mid(self):
        """Test mapping 7-9 years to mid-level tier."""
        for years in range(7, 10):
            tier = map_experience_to_tier(years)
            assert tier == "7-9_years"

    def test_map_to_tier_veteran(self):
        """Test mapping 10+ years to veteran tier."""
        for years in [10, 15, 20]:
            tier = map_experience_to_tier(years)
            assert tier == "10+_years"


class TestMaxSalaryCalculation:
    """Test max salary calculation based on CBA tiers."""

    def test_rookie_tier_max(self):
        """Test 0-6 years tier gets 25% of cap."""
        salary_cap = 140000000
        max_salary = get_max_salary_for_tier("0-6_years", salary_cap)
        expected = salary_cap * 0.25
        assert max_salary == expected

    def test_mid_tier_max(self):
        """Test 7-9 years tier gets 30% of cap."""
        salary_cap = 140000000
        max_salary = get_max_salary_for_tier("7-9_years", salary_cap)
        expected = salary_cap * 0.30
        assert max_salary == expected

    def test_veteran_tier_max(self):
        """Test 10+ years tier gets 35% of cap."""
        salary_cap = 140000000
        max_salary = get_max_salary_for_tier("10+_years", salary_cap)
        expected = salary_cap * 0.35
        assert max_salary == expected


class TestVolumeFeatures:
    """Test volume feature calculation."""

    def test_calculate_volume_features(self):
        """Test that volume features are calculated correctly."""
        df = pd.DataFrame(
            {
                "points": [1800, 1500],
                "rebounds": [350, 800],
                "assists": [600, 150],
                "turnovers": [200, 100],
                "minutes": [2500, 2200],
                "fg3a": [500, 50],
                "blocks": [20, 120],
                "steals": [80, 40],
                "vorp": [4.5, -1.2],
                "ws": [8.5, -0.5],
            }
        )

        result = calculate_volume_features(df)

        # Check log transforms
        expected_log_points = np.log(1800 + 1e-6)
        assert abs(result.loc[0, "log_points"] - expected_log_points) < 1e-3

        # Check sqrt transforms
        expected_sqrt_blocks = np.sqrt(120)
        assert abs(result.loc[1, "sqrt_blocks"] - expected_sqrt_blocks) < 1e-3

        # Check signed log for negative VORP
        assert result.loc[1, "vorp_signedlog"] < 0


class TestEfficiencyFeatures:
    """Test efficiency feature preservation."""

    def test_calculate_efficiency_features(self):
        """Test that efficiency features are preserved."""
        df = pd.DataFrame(
            {
                "per": [20.5, 18.0],
                "ts_pct": [0.58, 0.62],
                "bpm": [5.2, 3.8],
                "obpm": [3.5, 2.0],
                "dbpm": [1.7, 1.8],
            }
        )

        result = calculate_efficiency_features(df)

        assert result.loc[0, "per"] == 20.5
        assert result.loc[1, "ts_pct"] == 0.62


class TestPositionFeatures:
    """Test position one-hot encoding."""

    def test_calculate_position_features(self):
        """Test that position features are one-hot encoded."""
        df = pd.DataFrame({"position": ["PG", "C", "SF"]})

        result = calculate_position_features(df)

        # Check columns exist
        assert "pos_PG" in result.columns
        assert "pos_C" in result.columns
        assert "pos_SF" in result.columns

        # Check one-hot encoding
        assert result.loc[0, "pos_PG"] == 1
        assert result.loc[0, "pos_C"] == 0
        assert result.loc[1, "pos_C"] == 1
        assert result.loc[1, "pos_PG"] == 0


class TestPositionInteractions:
    """Test position interaction features."""

    def test_calculate_position_interaction_features(self):
        """Test that position interactions are calculated."""
        df = pd.DataFrame(
            {
                "pos_PG": [1, 0],
                "pos_C": [0, 1],
                "pos_SG": [0, 0],
                "pos_SF": [0, 0],
                "pos_PF": [0, 0],
                "log_fg3a": [6.2, 3.9],
                "ts_pct": [0.58, 0.62],
                "ast_pct": [0.35, 0.10],
                "obpm": [3.5, 2.0],
                "fg_pct": [0.46, 0.52],
                "efg_pct": [0.55, 0.60],
                "tov_pct": [0.12, 0.08],
                "age": [25, 32],
                "sqrt_steals": [8.9, 6.3],
                "dws_signedlog": [1.25, 1.10],
                "sqrt_blocks": [4.5, 11.0],
                "orb_pct": [0.03, 0.12],
                "log_rebounds": [5.9, 6.7],
                "dbpm": [1.7, 1.8],
            }
        )

        result = calculate_position_interaction_features(df)

        # Check interaction columns exist
        assert "PG_x_ast_pct" in result.columns
        assert "PG_x_fg3a" in result.columns
        assert "C_x_dbpm" in result.columns
        assert "C_x_blocks" in result.columns

        # PG interactions should be non-zero for first row (pos_PG=1)
        assert result.loc[0, "PG_x_ast_pct"] == 0.35
        assert result.loc[0, "PG_x_fg3a"] == 6.2

        # PG interactions should be zero for second row (pos_PG=0)
        assert result.loc[1, "PG_x_ast_pct"] == 0

        # C interactions should be non-zero for second row (pos_C=1)
        assert result.loc[1, "C_x_dbpm"] == 1.8
        assert result.loc[1, "C_x_blocks"] == 11.0

        # C interactions should be zero for first row (pos_C=0)
        assert result.loc[0, "C_x_dbpm"] == 0
