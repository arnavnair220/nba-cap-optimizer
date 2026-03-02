"""
Feature engineering for NBA salary prediction model.

This script transforms raw player stats into 60 model features including:
- Volume stats (log/sqrt transforms)
- Efficiency percentages
- Advanced metrics (PER, BPM, VORP, Win Shares)
- Experience tier indicators (based on CBA salary structure)
- Position indicators and interactions

Target variables:
- log_salary_cap_pct: Salary as % of cap (real contracts with CBA constraints)
- log_salary_pct_of_max: Salary as % of personal max (Fair Market Value)
"""

import io
import logging
import os
from typing import List, Optional, Tuple

import boto3
import numpy as np
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def signed_log(x: float, epsilon: float = 1e-6) -> float:
    """
    Signed logarithm transformation for handling negative values.

    This preserves sign while applying log transform to magnitude:
    - Positive values: log(x + epsilon)
    - Negative values: -log(|x| + epsilon)
    - Zero: 0

    Args:
        x: Value to transform
        epsilon: Small constant to avoid log(0)

    Returns:
        Signed log-transformed value
    """
    if x > 0:
        return np.log(x + epsilon)
    elif x < 0:
        return -np.log(abs(x) + epsilon)
    else:
        return 0.0


def load_data_from_s3(s3_path: str) -> pd.DataFrame:
    """
    Load data from S3 CSV file.

    Args:
        s3_path: S3 path in format s3://bucket/key

    Returns:
        DataFrame with loaded data
    """
    logger.info(f"Loading data from {s3_path}")

    # Parse S3 path
    s3_path_parts = s3_path.replace("s3://", "").split("/", 1)
    bucket = s3_path_parts[0]
    key = s3_path_parts[1]

    # Read from S3
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(response["Body"].read()))

    logger.info(f"Loaded {len(df)} records from S3")
    return df


def load_salary_cap_data_from_s3(data_bucket: str) -> pd.DataFrame:
    """
    Load salary cap history from S3.

    For now, we'll embed the salary cap data as it's small and static.
    In production, this could be stored in S3 or queried from RDS.

    Args:
        data_bucket: S3 bucket name (not used currently, for future extension)

    Returns:
        DataFrame with salary cap by season
    """
    logger.info("Loading salary cap history")

    # Salary cap data (2015-2026)
    # Source: Basketball Reference and NBA CBA
    salary_cap_data = {
        "season": [
            "2015-16",
            "2016-17",
            "2017-18",
            "2018-19",
            "2019-20",
            "2020-21",
            "2021-22",
            "2022-23",
            "2023-24",
            "2024-25",
            "2025-26",
        ],
        "salary_cap": [
            70000000,
            94143000,
            99093000,
            101869000,
            109140000,
            109140000,
            112414000,
            123655000,
            136021000,
            140588000,
            141000000,
        ],
    }

    df = pd.DataFrame(salary_cap_data)
    logger.info(f"Loaded salary cap data for {len(df)} seasons")
    return df


def estimate_experience_from_age(age: int) -> int:
    """
    Estimate years of NBA experience from age.

    Assumes players enter NBA at age 19 on average.
    This is a rough estimate - actual experience varies by player.

    Args:
        age: Player age

    Returns:
        Estimated years of experience (capped at 0 minimum)
    """
    return max(0, age - 19)


def map_experience_to_tier(experience: int) -> str:
    """
    Map years of experience to CBA salary tier.

    Tiers based on NBA CBA:
    - 0-6 years: Rookie scale (max ~$38.7M, 25% of cap)
    - 7-9 years: Early free agency (max ~$46.4M, 30% of cap)
    - 10+ years: Veteran max (max ~$54.1M, 35% of cap)

    Args:
        experience: Years of NBA experience

    Returns:
        Tier string: "0-6_years", "7-9_years", or "10+_years"
    """
    if experience <= 6:
        return "0-6_years"
    elif experience <= 9:
        return "7-9_years"
    else:
        return "10+_years"


def get_max_salary_for_tier(tier: str, salary_cap: float) -> float:
    """
    Get maximum salary for experience tier based on CBA percentages.

    Args:
        tier: Experience tier string
        salary_cap: Season salary cap

    Returns:
        Maximum salary for tier
    """
    max_percentages = {
        "0-6_years": 0.25,  # 25% of cap
        "7-9_years": 0.30,  # 30% of cap
        "10+_years": 0.35,  # 35% of cap
    }
    return salary_cap * max_percentages[tier]


def prorate_games_to_full_season(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pro-rate games_played and games_started to a full 82-game season.

    This handles the distribution shift between training data (complete seasons)
    and prediction data (incomplete current season). By projecting to 82 games,
    we preserve the availability/durability signal while normalizing for season progress.

    Algorithm:
    1. Calculate team max games using only single-team players (excludes multi-team indicators)
    2. For multi-team players, use max games from any team they played for
    3. Pro-rate: projected_games = actual_games * (82 / team_games_played)

    Args:
        df: DataFrame with games_played, games_started, team_abbreviation,
            is_multi_team, and teams_played_for columns

    Returns:
        DataFrame with games_played and games_started updated to projected values
    """
    logger.info("Pro-rating games_played and games_started to full 82-game season...")

    # Multi-team indicators that should be excluded from team max calculation
    MULTI_TEAM_INDICATORS = ["TOT", "2TM", "3TM", "4TM", "5TM"]

    # Calculate team max games using only single-team players
    single_team_mask = ~df["team_abbreviation"].isin(MULTI_TEAM_INDICATORS)
    single_team_df = df[single_team_mask]
    team_max_games = single_team_df.groupby("team_abbreviation")["games_played"].max()

    logger.info(f"Calculated max games for {len(team_max_games)} teams")

    # For each player, determine their team's games played
    def get_team_games_played(row):
        """Get the number of games the player's team(s) have played."""
        if row.get("is_multi_team", False):
            # Multi-team player: use max games from any team they played for
            teams_list = row.get("teams_played_for", [])
            if teams_list:
                team_games = [team_max_games.get(team, 82) for team in teams_list]
                return max(team_games) if team_games else 82
            else:
                return 82  # Fallback
        else:
            # Single-team player
            team = row.get("team_abbreviation")
            return team_max_games.get(team, 82) if team else 82

    df["team_games_played"] = df.apply(get_team_games_played, axis=1)

    # Pro-rate to 82 games
    df["games_played"] = (df["games_played"].fillna(0) * (82 / df["team_games_played"])).round(1)
    df["games_started"] = (df["games_started"].fillna(0) * (82 / df["team_games_played"])).round(1)

    logger.info(
        f"Pro-rated games - mean games_played: {df['games_played'].mean():.1f}, "
        f"mean games_started: {df['games_started'].mean():.1f}"
    )

    # Drop temporary column
    df = df.drop(columns=["team_games_played"])

    return df


def calculate_volume_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate volume features with transformations.

    Features:
    - Raw: games_played, games_started, fouls (3)
    - Log transforms: points, rebounds, assists, turnovers, minutes, fg3a (6)
    - Sqrt transforms: blocks, steals (2)
    - Signed log: vorp, ws (2)

    Total: 13 features
    """
    logger.info("Calculating volume features...")

    # Raw volume (already in dataframe)
    # games_played, games_started, fouls

    # Log transforms (add small epsilon to avoid log(0))
    epsilon = 1e-6
    df["log_points"] = np.log(df["points"].fillna(0) + epsilon)
    df["log_rebounds"] = np.log(df["rebounds"].fillna(0) + epsilon)
    df["log_assists"] = np.log(df["assists"].fillna(0) + epsilon)
    df["log_turnovers"] = np.log(df["turnovers"].fillna(0) + epsilon)
    df["log_minutes"] = np.log(df["minutes"].fillna(0) + epsilon)
    df["log_fg3a"] = np.log(df["fg3a"].fillna(0) + epsilon)

    # Sqrt transforms
    df["sqrt_blocks"] = np.sqrt(df["blocks"].fillna(0))
    df["sqrt_steals"] = np.sqrt(df["steals"].fillna(0))

    # Signed log for metrics that can be negative
    df["vorp_signedlog"] = df["vorp"].fillna(0).apply(signed_log)
    df["ws_signedlog"] = df["ws"].fillna(0).apply(signed_log)

    return df


def calculate_efficiency_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate efficiency features (already in raw data from Basketball Reference).

    Features: per, ts_pct, efg_pct, usg_pct, fg_pct, fg2_pct, fg3_pct,
              ft_pct, bpm, obpm, dbpm, ws_per_48

    Total: 12 features
    """
    logger.info("Validating efficiency features...")

    # These are already calculated by Basketball Reference
    # Just ensure they're present and handle missing values
    efficiency_cols = [
        "per",
        "ts_pct",
        "efg_pct",
        "usg_pct",
        "fg_pct",
        "fg2_pct",
        "fg3_pct",
        "ft_pct",
        "bpm",
        "obpm",
        "dbpm",
        "ws_per_48",
    ]

    for col in efficiency_cols:
        if col not in df.columns:
            logger.warning(f"Missing efficiency column: {col}")
            df[col] = 0.0

    return df


def calculate_percentage_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate percentage features (already in raw data).

    Features: orb_pct, drb_pct, trb_pct, ast_pct, stl_pct, blk_pct, tov_pct

    Total: 7 features
    """
    logger.info("Validating percentage features...")

    percentage_cols = ["orb_pct", "drb_pct", "trb_pct", "ast_pct", "stl_pct", "blk_pct", "tov_pct"]

    for col in percentage_cols:
        if col not in df.columns:
            logger.warning(f"Missing percentage column: {col}")
            df[col] = 0.0

    return df


def calculate_advanced_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate advanced features (signed log transforms).

    Features: ows_signedlog, dws_signedlog

    Total: 2 features
    """
    logger.info("Calculating advanced features...")

    df["ows_signedlog"] = df["ows"].fillna(0).apply(signed_log)
    df["dws_signedlog"] = df["dws"].fillna(0).apply(signed_log)

    return df


def calculate_experience_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate experience tier features based on age.

    Uses one-hot encoding for 3 CBA salary tiers:
    - exp_tier_0-6_years
    - exp_tier_7-9_years
    - exp_tier_10+_years

    Total: 3 features
    """
    logger.info("Calculating experience tier features...")

    # Estimate experience from age
    df["estimated_experience"] = df["age"].apply(estimate_experience_from_age)

    # Map to tier
    df["experience_tier"] = df["estimated_experience"].apply(map_experience_to_tier)

    # One-hot encode tiers
    tier_dummies = pd.get_dummies(df["experience_tier"], prefix="exp_tier")

    # Ensure all 3 tiers exist (even if no players in that tier)
    for tier in ["exp_tier_0-6_years", "exp_tier_7-9_years", "exp_tier_10+_years"]:
        if tier not in tier_dummies.columns:
            tier_dummies[tier] = 0

    df = pd.concat([df, tier_dummies], axis=1)

    return df


def calculate_position_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate position features (one-hot encoding).

    Handles multi-position players (e.g., "PG-SG") by using primary position.

    Features: pos_PG, pos_SG, pos_SF, pos_PF, pos_C

    Total: 5 features
    """
    logger.info("Calculating position features...")

    # Extract primary position (first position if hyphenated)
    df["primary_position"] = df["position"].fillna("").str.split("-").str[0]

    # One-hot encode positions
    position_dummies = pd.get_dummies(df["primary_position"], prefix="pos")

    # Ensure all 5 positions exist
    for pos in ["pos_PG", "pos_SG", "pos_SF", "pos_PF", "pos_C"]:
        if pos not in position_dummies.columns:
            position_dummies[pos] = 0

    df = pd.concat([df, position_dummies], axis=1)

    return df


def calculate_position_interaction_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate position-specific interaction features.

    Interactions capture position-specific value drivers:
    - PG: 3PT volume, efficiency, playmaking, offensive impact
    - SG: Shooting efficiency, low turnovers
    - SF: Experience premium, defense
    - PF: Rebounding, rim protection
    - C: Defensive impact, playmaking, rim protection

    Total: 18 features
    """
    logger.info("Calculating position interaction features...")

    # PG interactions (4)
    df["PG_x_fg3a"] = df["pos_PG"] * df["log_fg3a"]
    df["PG_x_ts_pct"] = df["pos_PG"] * df["ts_pct"].fillna(0)
    df["PG_x_ast_pct"] = df["pos_PG"] * df["ast_pct"].fillna(0)
    df["PG_x_obpm"] = df["pos_PG"] * df["obpm"].fillna(0)

    # SG interactions (4)
    df["SG_x_fg_pct"] = df["pos_SG"] * df["fg_pct"].fillna(0)
    df["SG_x_ts_pct"] = df["pos_SG"] * df["ts_pct"].fillna(0)
    df["SG_x_efg_pct"] = df["pos_SG"] * df["efg_pct"].fillna(0)
    df["SG_x_tov_pct"] = df["pos_SG"] * df["tov_pct"].fillna(0)

    # SF interactions (3)
    df["SF_x_age"] = df["pos_SF"] * df["age"]
    df["SF_x_steals"] = df["pos_SF"] * df["sqrt_steals"]
    df["SF_x_dws"] = df["pos_SF"] * df["dws_signedlog"]

    # PF interactions (3)
    df["PF_x_blocks"] = df["pos_PF"] * df["sqrt_blocks"]
    df["PF_x_orb_pct"] = df["pos_PF"] * df["orb_pct"].fillna(0)
    df["PF_x_rebounds"] = df["pos_PF"] * df["log_rebounds"]

    # C interactions (4)
    df["C_x_dbpm"] = df["pos_C"] * df["dbpm"].fillna(0)
    df["C_x_ast_pct"] = df["pos_C"] * df["ast_pct"].fillna(0)
    df["C_x_blocks"] = df["pos_C"] * df["sqrt_blocks"]
    df["C_x_steals"] = df["pos_C"] * df["sqrt_steals"]

    return df


def calculate_targets(df: pd.DataFrame, salary_cap_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate target variables.

    Targets:
    1. log_salary_cap_pct: Salary as % of salary cap (actual contracts with CBA)
    2. log_salary_pct_of_max: Salary as % of personal max (Fair Market Value)

    Args:
        df: Feature dataframe with annual_salary and experience_tier
        salary_cap_df: Salary cap by season

    Returns:
        DataFrame with target columns added
    """
    logger.info("Calculating target variables...")

    # Merge salary cap data
    df = df.merge(salary_cap_df, on="season", how="left")

    # Calculate personal max salary based on experience tier
    df["personal_max_salary"] = df.apply(
        lambda row: get_max_salary_for_tier(row["experience_tier"], row["salary_cap"]), axis=1
    )

    # Target 1: Salary as % of cap
    df["salary_cap_pct"] = (df["annual_salary"] / df["salary_cap"]) * 100
    df["log_salary_cap_pct"] = np.log(df["salary_cap_pct"] + 1e-6)

    # Target 2: Salary as % of personal max (capped at 100%)
    df["salary_pct_of_max"] = np.minimum(
        (df["annual_salary"] / df["personal_max_salary"]) * 100, 100.0
    )
    df["log_salary_pct_of_max"] = np.log(df["salary_pct_of_max"] + 1e-6)

    logger.info(
        f"Target stats - salary_cap_pct: mean={df['salary_cap_pct'].mean():.2f}, "
        f"median={df['salary_cap_pct'].median():.2f}"
    )
    logger.info(
        f"Target stats - salary_pct_of_max: mean={df['salary_pct_of_max'].mean():.2f}, "
        f"median={df['salary_pct_of_max'].median():.2f}"
    )

    return df


def get_feature_columns() -> List[str]:
    """
    Get list of all 60 feature column names.

    Returns:
        List of feature column names in order
    """
    features = []

    # Raw volume (3)
    features.extend(["games_played", "games_started", "fouls"])

    # Transformed volume (10)
    features.extend(
        [
            "log_points",
            "log_rebounds",
            "log_assists",
            "log_turnovers",
            "log_minutes",
            "log_fg3a",
            "sqrt_blocks",
            "sqrt_steals",
            "vorp_signedlog",
            "ws_signedlog",
        ]
    )

    # Efficiency (12)
    features.extend(
        [
            "per",
            "ts_pct",
            "efg_pct",
            "usg_pct",
            "fg_pct",
            "fg2_pct",
            "fg3_pct",
            "ft_pct",
            "bpm",
            "obpm",
            "dbpm",
            "ws_per_48",
        ]
    )

    # Percentage stats (7)
    features.extend(["orb_pct", "drb_pct", "trb_pct", "ast_pct", "stl_pct", "blk_pct", "tov_pct"])

    # Advanced (2)
    features.extend(["ows_signedlog", "dws_signedlog"])

    # Experience tiers (3)
    features.extend(["exp_tier_0-6_years", "exp_tier_7-9_years", "exp_tier_10+_years"])

    # Positions (5)
    features.extend(["pos_PG", "pos_SG", "pos_SF", "pos_PF", "pos_C"])

    # Position interactions (18)
    features.extend(
        [
            # PG (4)
            "PG_x_fg3a",
            "PG_x_ts_pct",
            "PG_x_ast_pct",
            "PG_x_obpm",
            # SG (4)
            "SG_x_fg_pct",
            "SG_x_ts_pct",
            "SG_x_efg_pct",
            "SG_x_tov_pct",
            # SF (3)
            "SF_x_age",
            "SF_x_steals",
            "SF_x_dws",
            # PF (3)
            "PF_x_blocks",
            "PF_x_orb_pct",
            "PF_x_rebounds",
            # C (4)
            "C_x_dbpm",
            "C_x_ast_pct",
            "C_x_blocks",
            "C_x_steals",
        ]
    )

    return features


def engineer_features(
    mode: str = "train",
    input_path: Optional[str] = None,
    output_path: Optional[str] = None,
    data_bucket: Optional[str] = None,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Main feature engineering pipeline.

    Loads data from S3, calculates all features (and targets for training), saves to S3.

    Args:
        mode: "train" or "predict"
        input_path: S3 path to input CSV file (e.g., "s3://bucket/raw_data.csv")
        output_path: S3 path to save features (e.g., "s3://bucket/features.csv")
        data_bucket: S3 bucket name for loading salary cap data

    Returns:
        Tuple of (feature_df, feature_columns)
    """
    logger.info(f"Starting feature engineering pipeline in {mode} mode...")

    # Load data from S3
    if not input_path:
        raise ValueError("input_path is required")

    df = load_data_from_s3(input_path)
    logger.info(f"Loaded {len(df)} records from {input_path}")

    # Load salary cap data (train mode only)
    if mode == "train":
        if not data_bucket:
            raise ValueError("data_bucket is required in train mode")
        salary_cap_df = load_salary_cap_data_from_s3(data_bucket)
    elif mode == "predict":
        salary_cap_df = None
    else:
        raise ValueError(f"Invalid mode: {mode}. Must be 'train' or 'predict'")

    # Pro-rate games to full season (handles incomplete seasons in predict mode)
    df = prorate_games_to_full_season(df)

    # Calculate features
    df = calculate_volume_features(df)
    df = calculate_efficiency_features(df)
    df = calculate_percentage_features(df)
    df = calculate_advanced_features(df)
    df = calculate_experience_features(df)
    df = calculate_position_features(df)
    df = calculate_position_interaction_features(df)

    # Calculate targets (train mode only)
    if mode == "train":
        df = calculate_targets(df, salary_cap_df)

    # Get feature columns
    feature_cols = get_feature_columns()

    # Verify all features exist
    missing_features = [f for f in feature_cols if f not in df.columns]
    if missing_features:
        logger.error(f"Missing features: {missing_features}")
        raise ValueError(f"Missing features: {missing_features}")

    logger.info(f"Feature engineering complete! Generated {len(feature_cols)} features")
    logger.info(f"Dataset shape: {df.shape}")
    logger.info(f"Seasons included: {sorted(df['season'].unique())}")

    # Handle missing values
    df[feature_cols] = df[feature_cols].fillna(0)

    # Save to S3 if output path provided
    if output_path:
        logger.info(f"Saving features to {output_path}")

        # Prepare output dataframe
        if mode == "train":
            output_cols = (
                ["player_name", "season"]
                + feature_cols
                + ["log_salary_cap_pct", "log_salary_pct_of_max", "annual_salary", "salary_cap"]
            )
        else:  # predict mode
            output_cols = ["player_name", "season"] + feature_cols

        output_df = df[output_cols]

        if output_path.startswith("s3://"):
            # Parse S3 path
            s3_parts = output_path.replace("s3://", "").split("/", 1)
            bucket = s3_parts[0]
            key = s3_parts[1]

            # Save to S3
            s3_client = boto3.client("s3")
            csv_buffer = output_df.to_csv(index=False)
            s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer)
            logger.info(f"Saved {len(output_df)} records to {output_path}")
        else:
            # Save locally
            output_df.to_csv(output_path, index=False)
            logger.info(f"Saved {len(output_df)} records to {output_path}")

    return df, feature_cols


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NBA Feature Engineering for SageMaker")
    parser.add_argument(
        "--mode",
        type=str,
        default="train",
        choices=["train", "predict"],
        help="Mode: train (all historical data) or predict (latest season + week)",
    )
    parser.add_argument(
        "--input-path",
        type=str,
        default=None,
        help="S3 path to input CSV file (e.g., s3://bucket/raw_data.csv)",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        default=None,
        help="S3 path to save features (e.g., s3://bucket/features.csv)",
    )
    parser.add_argument(
        "--data-bucket",
        type=str,
        default=None,
        help="S3 data bucket name",
    )

    args = parser.parse_args()

    # Use environment variables or CLI args
    input_path = args.input_path or os.environ.get("INPUT_PATH")
    output_path = args.output_path or os.environ.get("OUTPUT_PATH", "features.csv")
    data_bucket = args.data_bucket or os.environ.get("DATA_BUCKET")

    if not input_path:
        raise ValueError("input_path is required (via --input-path or INPUT_PATH env var)")

    df, feature_cols = engineer_features(
        mode=args.mode,
        input_path=input_path,
        output_path=output_path,
        data_bucket=data_bucket,
    )

    print(f"\nFeature engineering complete ({args.mode} mode)!")
    print(f"Total features: {len(feature_cols)}")
    print(f"Total records: {len(df)}")
    print("\nFeature columns:")
    for i, col in enumerate(feature_cols, 1):
        print(f"  {i}. {col}")
