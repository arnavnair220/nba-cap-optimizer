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

import json
import logging
import os
from typing import List, Optional, Tuple

import boto3
import numpy as np
import pandas as pd
import psycopg2

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


def get_db_connection():
    """
    Get database connection using credentials from environment or Secrets Manager.

    Returns:
        psycopg2 connection object
    """
    db_secret_arn = os.environ.get("DB_SECRET_ARN")

    if db_secret_arn:
        # Production: Load from Secrets Manager
        secrets_client = boto3.client("secretsmanager")
        secret_value = secrets_client.get_secret_value(SecretId=db_secret_arn)
        credentials = json.loads(secret_value["SecretString"])

        conn = psycopg2.connect(
            host=credentials["host"],
            port=credentials["port"],
            database=credentials["dbname"],
            user=credentials["username"],
            password=credentials["password"],
        )
    else:
        # Local: Use environment variables
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST", "localhost"),
            port=os.environ.get("DB_PORT", 5432),
            database=os.environ.get("DB_NAME", "nba_cap_optimizer"),
            user=os.environ.get("DB_USER", "postgres"),
            password=os.environ.get("DB_PASSWORD", ""),
        )

    return conn


def load_training_data(seasons_before: str = "2025-26") -> pd.DataFrame:
    """
    Load player stats and salaries from database for all seasons before target season.

    Args:
        seasons_before: Exclude this season and later (default: "2025-26")

    Returns:
        DataFrame with player stats and salaries joined
    """
    logger.info(f"Loading training data for seasons before {seasons_before}")

    conn = get_db_connection()

    # Query to join player_stats with salaries
    # Filters out seasons >= seasons_before
    query = """
    SELECT
        ps.*,
        s.annual_salary,
        s.season as salary_season
    FROM player_stats ps
    INNER JOIN salaries s
        ON ps.player_name = s.player_name
        AND ps.season = s.season
    WHERE ps.season < %s
        AND s.annual_salary > 0
        AND ps.minutes > 0
        AND ps.games_played > 0
    ORDER BY ps.season, ps.player_name
    """

    df = pd.read_sql(query, conn, params=(seasons_before,))
    conn.close()

    logger.info(f"Loaded {len(df)} player-season records")
    logger.info(f"Seasons: {sorted(df['season'].unique())}")
    logger.info(f"Date range: {df['season'].min()} to {df['season'].max()}")

    return df


def load_prediction_data() -> pd.DataFrame:
    """
    Load player stats from latest season and last 7 days for predictions.

    Returns:
        DataFrame with recent player stats (no salary data)
    """
    logger.info("Loading prediction data for latest season and last 7 days")

    conn = get_db_connection()

    # Get latest season and filter by last 7 days
    query = """
    SELECT ps.*
    FROM player_stats ps
    WHERE ps.season = (SELECT MAX(season) FROM player_stats)
        AND ps.created_at >= NOW() - INTERVAL '7 days'
        AND ps.minutes > 0
        AND ps.games_played > 0
    ORDER BY ps.player_name
    """

    df = pd.read_sql(query, conn)
    conn.close()

    logger.info(f"Loaded {len(df)} player records for predictions")
    if len(df) > 0:
        logger.info(f"Season: {df['season'].iloc[0]}")
        logger.info(f"Date range: {df['created_at'].min()} to {df['created_at'].max()}")

    return df


def load_salary_cap_data() -> pd.DataFrame:
    """
    Load salary cap history from database.

    Returns:
        DataFrame with salary cap by season
    """
    logger.info("Loading salary cap history")

    conn = get_db_connection()
    query = "SELECT season, salary_cap FROM salary_cap_history ORDER BY season"
    df = pd.read_sql(query, conn)
    conn.close()

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
    seasons_before: str = "2025-26",
    output_path: Optional[str] = None,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Main feature engineering pipeline.

    Loads data, calculates all features (and targets for training), saves to S3.

    Args:
        mode: "train" or "predict"
        seasons_before: Train on seasons before this (default: "2025-26", train mode only)
        output_path: Optional S3 path to save features (e.g., "s3://bucket/features.csv")

    Returns:
        Tuple of (feature_df, feature_columns)
    """
    logger.info(f"Starting feature engineering pipeline in {mode} mode...")

    # Load data based on mode
    if mode == "train":
        df = load_training_data(seasons_before=seasons_before)
        salary_cap_df = load_salary_cap_data()
    elif mode == "predict":
        df = load_prediction_data()
        salary_cap_df = None
    else:
        raise ValueError(f"Invalid mode: {mode}. Must be 'train' or 'predict'")

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
        "--seasons-before",
        type=str,
        default=None,
        help="For train mode: exclude this season and later",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        default=None,
        help="S3 path to save features (e.g., s3://bucket/features.csv)",
    )

    args = parser.parse_args()

    # Use environment variable or CLI arg for output path and seasons_before
    output_path = args.output_path or os.environ.get("OUTPUT_PATH", "features.csv")
    seasons_before = args.seasons_before or os.environ.get("SEASONS_BEFORE", "2025-26")

    df, feature_cols = engineer_features(
        mode=args.mode, seasons_before=seasons_before, output_path=output_path
    )

    print(f"\nFeature engineering complete ({args.mode} mode)!")
    print(f"Total features: {len(feature_cols)}")
    print(f"Total records: {len(df)}")
    print("\nFeature columns:")
    for i, col in enumerate(feature_cols, 1):
        print(f"  {i}. {col}")
