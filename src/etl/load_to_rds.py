"""
Load transformed data to RDS PostgreSQL.

This Lambda function is the final step in the ETL pipeline.
It loads enriched data from S3 and upserts it into PostgreSQL tables.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional

import boto3
import psycopg2
from botocore.exceptions import ClientError
from psycopg2.extras import execute_batch

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
S3_BUCKET = os.environ.get("DATA_BUCKET")
ENVIRONMENT = os.environ.get("ENVIRONMENT")
DB_SECRET_ARN = os.environ.get("DB_SECRET_ARN")

# Lazy-initialized clients (initialized on first use)
_s3_client = None
_secretsmanager_client = None


def get_s3_client():
    """Get or create S3 client (lazy initialization for testing)."""
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def get_secretsmanager_client():
    """Get or create Secrets Manager client (lazy initialization for testing)."""
    global _secretsmanager_client
    if _secretsmanager_client is None:
        _secretsmanager_client = boto3.client("secretsmanager")
    return _secretsmanager_client


def get_db_credentials() -> Dict[str, str]:
    """
    Retrieve database credentials from AWS Secrets Manager.

    Returns:
        Dictionary with host, port, username, password, database
    """
    try:
        sm_client = get_secretsmanager_client()
        response = sm_client.get_secret_value(SecretId=DB_SECRET_ARN)
        secret = json.loads(response["SecretString"])

        return {
            "host": secret.get("host"),
            "port": secret.get("port", 5432),
            "username": secret.get("username"),
            "password": secret.get("password"),
            "database": secret.get("dbname", "nba_cap_optimizer"),
        }
    except ClientError as e:
        logger.error(f"Failed to retrieve database credentials: {e}")
        raise


def get_db_connection():
    """
    Create a connection to RDS PostgreSQL.

    Returns:
        psycopg2 connection object
    """
    creds = get_db_credentials()

    try:
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            user=creds["username"],
            password=creds["password"],
            database=creds["database"],
            connect_timeout=10,
        )
        logger.info("Successfully connected to RDS")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to RDS: {e}")
        raise


def load_from_s3(s3_key: str) -> Optional[Dict[str, Any]]:
    """
    Load JSON data from S3.

    Args:
        s3_key: S3 object key

    Returns:
        Parsed JSON data or None if error
    """
    try:
        s3_client = get_s3_client()
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        data = json.loads(response["Body"].read().decode("utf-8"))
        logger.info(f"Successfully loaded data from s3://{S3_BUCKET}/{s3_key}")
        return data
    except ClientError as e:
        logger.error(f"Failed to load from S3 {s3_key}: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in S3 object {s3_key}: {e}")
        return None


def ensure_schema_exists(cursor) -> bool:
    """
    Check if database schema exists and create it if needed.
    This makes the Lambda idempotent - safe to run even on fresh RDS instance.

    Args:
        cursor: Database cursor

    Returns:
        True if schema already existed, False if it was created
    """
    try:
        # Check if any of our tables exist
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = 'players'
            );
        """)

        schema_exists = cursor.fetchone()[0]

        if schema_exists:
            logger.info("Database schema already exists")
            return True

        # Schema doesn't exist - create it
        logger.info("Database schema not found, creating tables...")

        # Execute schema creation SQL
        schema_sql = """
-- Players table: Master player list from NBA API
CREATE TABLE IF NOT EXISTS players (
    id INTEGER PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_players_full_name ON players(full_name);

-- Salaries table: Annual salary per player per season
CREATE TABLE IF NOT EXISTS salaries (
    id SERIAL PRIMARY KEY,
    player_id INTEGER REFERENCES players(id),
    player_name VARCHAR(255) NOT NULL,
    annual_salary INTEGER NOT NULL,
    season VARCHAR(20) NOT NULL,
    source VARCHAR(50),
    UNIQUE(player_name, season)
);

CREATE INDEX IF NOT EXISTS idx_salaries_player_id ON salaries(player_id);
CREATE INDEX IF NOT EXISTS idx_salaries_player_name ON salaries(player_name);
CREATE INDEX IF NOT EXISTS idx_salaries_season ON salaries(season);

-- Player stats table: Per-game and advanced statistics from Basketball Reference
CREATE TABLE IF NOT EXISTS player_stats (
    id SERIAL PRIMARY KEY,
    player_id INTEGER REFERENCES players(id),
    player_name VARCHAR(255) NOT NULL,
    season VARCHAR(20) NOT NULL,
    team_abbreviation VARCHAR(10),
    age INTEGER,
    position VARCHAR(10),
    games_played INTEGER,
    games_started INTEGER,
    minutes REAL,
    points REAL,
    fgm REAL,
    fga REAL,
    fg_pct REAL,
    fg3m REAL,
    fg3a REAL,
    fg3_pct REAL,
    fg2m REAL,
    fg2a REAL,
    fg2_pct REAL,
    ftm REAL,
    fta REAL,
    ft_pct REAL,
    oreb REAL,
    dreb REAL,
    rebounds REAL,
    assists REAL,
    steals REAL,
    blocks REAL,
    turnovers REAL,
    fouls REAL,
    per REAL,
    ts_pct REAL,
    efg_pct REAL,
    usg_pct REAL,
    ws REAL,
    ws_per_48 REAL,
    bpm REAL,
    obpm REAL,
    dbpm REAL,
    vorp REAL,
    orb_pct REAL,
    drb_pct REAL,
    trb_pct REAL,
    ast_pct REAL,
    stl_pct REAL,
    blk_pct REAL,
    tov_pct REAL,
    ows REAL,
    dws REAL,
    UNIQUE(player_name, season, team_abbreviation)
);

CREATE INDEX IF NOT EXISTS idx_player_stats_player_id ON player_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_player_stats_player_name ON player_stats(player_name);
CREATE INDEX IF NOT EXISTS idx_player_stats_season ON player_stats(season);
CREATE INDEX IF NOT EXISTS idx_player_stats_team ON player_stats(team_abbreviation);

-- Teams table: Team data with aggregated metrics
CREATE TABLE IF NOT EXISTS teams (
    id INTEGER PRIMARY KEY,
    full_name VARCHAR(255),
    abbreviation VARCHAR(10) UNIQUE NOT NULL,
    total_payroll BIGINT,
    roster_count INTEGER,
    roster_with_salary INTEGER,
    avg_salary REAL,
    min_salary INTEGER,
    max_salary INTEGER,
    top_paid_player VARCHAR(255),
    top_paid_salary INTEGER,
    total_players_with_stats INTEGER,
    team_total_points REAL,
    team_total_rebounds REAL,
    team_total_assists REAL,
    avg_player_points REAL,
    avg_player_rebounds REAL,
    avg_player_assists REAL
);

CREATE INDEX IF NOT EXISTS idx_teams_abbreviation ON teams(abbreviation);
        """

        cursor.execute(schema_sql)
        logger.info("Successfully created database schema")
        return False

    except Exception as e:
        logger.error(f"Failed to ensure schema exists: {e}")
        raise


def upsert_players(cursor, players_data: List[Dict[str, Any]]) -> int:
    """
    Upsert players into the players table.

    Args:
        cursor: Database cursor
        players_data: List of player dictionaries with 'id' and 'full_name'

    Returns:
        Number of players upserted
    """
    if not players_data:
        logger.warning("No players data to upsert")
        return 0

    sql = """
        INSERT INTO players (id, full_name)
        VALUES (%s, %s)
        ON CONFLICT (id) DO UPDATE
        SET full_name = EXCLUDED.full_name
    """

    data = [(p["id"], p["full_name"]) for p in players_data]

    execute_batch(cursor, sql, data)
    logger.info(f"Upserted {len(data)} players")
    return len(data)


def upsert_salaries(cursor, salaries_data: List[Dict[str, Any]]) -> int:
    """
    Upsert salaries into the salaries table.

    Args:
        cursor: Database cursor
        salaries_data: List of salary dictionaries from enriched_salaries.json

    Returns:
        Number of salaries upserted
    """
    if not salaries_data:
        logger.warning("No salaries data to upsert")
        return 0

    sql = """
        INSERT INTO salaries (player_id, player_name, annual_salary, season, source)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (player_name, season) DO UPDATE
        SET
            player_id = EXCLUDED.player_id,
            annual_salary = EXCLUDED.annual_salary,
            source = EXCLUDED.source
    """

    data = [
        (
            s.get("player_id"),
            s["player_name"],
            s["annual_salary"],
            s["season"],
            s.get("source"),
        )
        for s in salaries_data
    ]

    execute_batch(cursor, sql, data)
    logger.info(f"Upserted {len(data)} salaries")
    return len(data)


def upsert_player_stats(cursor, stats_data: List[Dict[str, Any]]) -> int:
    """
    Upsert player statistics into the player_stats table.

    Args:
        cursor: Database cursor
        stats_data: List of player stat dictionaries from enriched_player_stats.json

    Returns:
        Number of player stats upserted
    """
    if not stats_data:
        logger.warning("No player stats data to upsert")
        return 0

    sql = """
        INSERT INTO player_stats (
            player_id, player_name, season, team_abbreviation,
            age, position, games_played, games_started, minutes,
            points, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
            fg2m, fg2a, fg2_pct, ftm, fta, ft_pct,
            oreb, dreb, rebounds,
            assists, steals, blocks, turnovers, fouls,
            per, ts_pct, efg_pct, usg_pct, ws, ws_per_48,
            bpm, obpm, dbpm, vorp,
            orb_pct, drb_pct, trb_pct, ast_pct, stl_pct, blk_pct, tov_pct,
            ows, dws
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s
        )
        ON CONFLICT (player_name, season, team_abbreviation) DO UPDATE
        SET
            player_id = EXCLUDED.player_id,
            age = EXCLUDED.age,
            position = EXCLUDED.position,
            games_played = EXCLUDED.games_played,
            games_started = EXCLUDED.games_started,
            minutes = EXCLUDED.minutes,
            points = EXCLUDED.points,
            fgm = EXCLUDED.fgm,
            fga = EXCLUDED.fga,
            fg_pct = EXCLUDED.fg_pct,
            fg3m = EXCLUDED.fg3m,
            fg3a = EXCLUDED.fg3a,
            fg3_pct = EXCLUDED.fg3_pct,
            fg2m = EXCLUDED.fg2m,
            fg2a = EXCLUDED.fg2a,
            fg2_pct = EXCLUDED.fg2_pct,
            ftm = EXCLUDED.ftm,
            fta = EXCLUDED.fta,
            ft_pct = EXCLUDED.ft_pct,
            oreb = EXCLUDED.oreb,
            dreb = EXCLUDED.dreb,
            rebounds = EXCLUDED.rebounds,
            assists = EXCLUDED.assists,
            steals = EXCLUDED.steals,
            blocks = EXCLUDED.blocks,
            turnovers = EXCLUDED.turnovers,
            fouls = EXCLUDED.fouls,
            per = EXCLUDED.per,
            ts_pct = EXCLUDED.ts_pct,
            efg_pct = EXCLUDED.efg_pct,
            usg_pct = EXCLUDED.usg_pct,
            ws = EXCLUDED.ws,
            ws_per_48 = EXCLUDED.ws_per_48,
            bpm = EXCLUDED.bpm,
            obpm = EXCLUDED.obpm,
            dbpm = EXCLUDED.dbpm,
            vorp = EXCLUDED.vorp,
            orb_pct = EXCLUDED.orb_pct,
            drb_pct = EXCLUDED.drb_pct,
            trb_pct = EXCLUDED.trb_pct,
            ast_pct = EXCLUDED.ast_pct,
            stl_pct = EXCLUDED.stl_pct,
            blk_pct = EXCLUDED.blk_pct,
            tov_pct = EXCLUDED.tov_pct,
            ows = EXCLUDED.ows,
            dws = EXCLUDED.dws
    """

    data = [
        (
            s.get("player_id"),
            s["player_name"],
            "2025-26",  # TODO: Get from event or stats metadata
            s.get("team_abbreviation"),
            s.get("age"),
            s.get("position"),
            s.get("games_played"),
            s.get("games_started"),
            s.get("minutes"),
            s.get("points"),
            s.get("fgm"),
            s.get("fga"),
            s.get("fg_pct"),
            s.get("fg3m"),
            s.get("fg3a"),
            s.get("fg3_pct"),
            s.get("fg2m"),
            s.get("fg2a"),
            s.get("fg2_pct"),
            s.get("ftm"),
            s.get("fta"),
            s.get("ft_pct"),
            s.get("oreb"),
            s.get("dreb"),
            s.get("rebounds"),
            s.get("assists"),
            s.get("steals"),
            s.get("blocks"),
            s.get("turnovers"),
            s.get("fouls"),
            s.get("per"),
            s.get("ts_pct"),
            s.get("efg_pct"),
            s.get("usg_pct"),
            s.get("ws"),
            s.get("ws_per_48"),
            s.get("bpm"),
            s.get("obpm"),
            s.get("dbpm"),
            s.get("vorp"),
            s.get("orb_pct"),
            s.get("drb_pct"),
            s.get("trb_pct"),
            s.get("ast_pct"),
            s.get("stl_pct"),
            s.get("blk_pct"),
            s.get("tov_pct"),
            s.get("ows"),
            s.get("dws"),
        )
        for s in stats_data
    ]

    execute_batch(cursor, sql, data)
    logger.info(f"Upserted {len(data)} player stats")
    return len(data)


def upsert_teams(cursor, teams_data: List[Dict[str, Any]]) -> int:
    """
    Upsert teams into the teams table.

    Args:
        cursor: Database cursor
        teams_data: List of team dictionaries from enriched_teams.json

    Returns:
        Number of teams upserted
    """
    if not teams_data:
        logger.warning("No teams data to upsert")
        return 0

    sql = """
        INSERT INTO teams (
            id, full_name, abbreviation,
            total_payroll, roster_count, roster_with_salary,
            avg_salary, min_salary, max_salary,
            top_paid_player, top_paid_salary,
            total_players_with_stats,
            team_total_points, team_total_rebounds, team_total_assists,
            avg_player_points, avg_player_rebounds, avg_player_assists
        )
        VALUES (
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s,
            %s, %s, %s,
            %s, %s, %s
        )
        ON CONFLICT (id) DO UPDATE
        SET
            full_name = EXCLUDED.full_name,
            abbreviation = EXCLUDED.abbreviation,
            total_payroll = EXCLUDED.total_payroll,
            roster_count = EXCLUDED.roster_count,
            roster_with_salary = EXCLUDED.roster_with_salary,
            avg_salary = EXCLUDED.avg_salary,
            min_salary = EXCLUDED.min_salary,
            max_salary = EXCLUDED.max_salary,
            top_paid_player = EXCLUDED.top_paid_player,
            top_paid_salary = EXCLUDED.top_paid_salary,
            total_players_with_stats = EXCLUDED.total_players_with_stats,
            team_total_points = EXCLUDED.team_total_points,
            team_total_rebounds = EXCLUDED.team_total_rebounds,
            team_total_assists = EXCLUDED.team_total_assists,
            avg_player_points = EXCLUDED.avg_player_points,
            avg_player_rebounds = EXCLUDED.avg_player_rebounds,
            avg_player_assists = EXCLUDED.avg_player_assists
    """

    data = [
        (
            t["id"],
            t.get("full_name"),
            t.get("abbreviation"),
            t.get("total_payroll"),
            t.get("roster_count"),
            t.get("roster_with_salary"),
            t.get("avg_salary"),
            t.get("min_salary"),
            t.get("max_salary"),
            t.get("top_paid_player"),
            t.get("top_paid_salary"),
            t.get("total_players_with_stats"),
            t.get("team_total_points"),
            t.get("team_total_rebounds"),
            t.get("team_total_assists"),
            t.get("avg_player_points"),
            t.get("avg_player_rebounds"),
            t.get("avg_player_assists"),
        )
        for t in teams_data
    ]

    execute_batch(cursor, sql, data)
    logger.info(f"Upserted {len(data)} teams")
    return len(data)


def handler(event, context):
    """
    Lambda handler for loading transformed data to RDS.

    Expected event structure from transform_data:
    {
        "statusCode": 200,
        "data_location": {"bucket": "...", "partition": "year=2024/month=02/day=17"},
        "transformation_successful": true,
        "statistics": {...}
    }

    Returns:
        Status with counts of records loaded
    """
    logger.info("Starting data load to RDS Lambda")
    logger.info(f"Event: {json.dumps(event)}")

    # Validate required environment variables
    if not S3_BUCKET:
        logger.error("DATA_BUCKET environment variable is not set")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "DATA_BUCKET environment variable is required"}),
        }
    if not ENVIRONMENT:
        logger.error("ENVIRONMENT environment variable is not set")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "ENVIRONMENT environment variable is required"}),
        }
    if not DB_SECRET_ARN:
        logger.error("DB_SECRET_ARN environment variable is not set")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "DB_SECRET_ARN environment variable is required"}),
        }

    # Check if transformation was successful
    transformation_successful = event.get("transformation_successful", False)
    if not transformation_successful:
        logger.error("Data transformation failed in previous step. Skipping RDS load.")
        return {
            "statusCode": 400,
            "body": json.dumps(
                {
                    "error": "Transformation failed",
                    "message": "RDS load skipped due to transformation failure",
                }
            ),
        }

    # Get data location from previous step
    if "data_location" not in event:
        logger.error("Missing data_location in event")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing data_location in event"}),
        }

    partition = event["data_location"]["partition"]
    logger.info(f"Loading data from partition: {partition}")

    conn = None
    results = {
        "statusCode": 200,
        "loaded": {},
        "errors": [],
    }

    try:
        # Connect to RDS
        conn = get_db_connection()
        cursor = conn.cursor()

        # Ensure database schema exists (idempotent - creates tables if needed)
        ensure_schema_exists(cursor)
        conn.commit()  # Commit schema creation

        # Load transformed data from S3
        logger.info("Loading transformed data from S3...")

        players_s3 = load_from_s3(f"raw/players/{partition}/active_players.json")
        salaries_s3 = load_from_s3(f"transformed/salaries/{partition}/enriched_salaries.json")
        stats_s3 = load_from_s3(f"transformed/stats/{partition}/enriched_player_stats.json")
        teams_s3 = load_from_s3(f"transformed/teams/{partition}/enriched_teams.json")

        # Begin transaction
        logger.info("Beginning database transaction...")

        # 1. Upsert players (if available)
        if players_s3 and players_s3.get("players"):
            try:
                count = upsert_players(cursor, players_s3["players"])
                results["loaded"]["players"] = count
            except Exception as e:
                logger.error(f"Failed to upsert players: {e}")
                results["errors"].append(f"Players upsert failed: {str(e)}")
        else:
            logger.warning("No players data found - skipping players upsert")

        # 2. Upsert salaries (if available)
        if salaries_s3 and salaries_s3.get("salaries"):
            try:
                count = upsert_salaries(cursor, salaries_s3["salaries"])
                results["loaded"]["salaries"] = count
            except Exception as e:
                logger.error(f"Failed to upsert salaries: {e}")
                results["errors"].append(f"Salaries upsert failed: {str(e)}")
        else:
            logger.warning("No salaries data found - skipping salaries upsert")

        # 3. Upsert player stats (required)
        if stats_s3 and stats_s3.get("player_stats"):
            try:
                count = upsert_player_stats(cursor, stats_s3["player_stats"])
                results["loaded"]["player_stats"] = count
            except Exception as e:
                logger.error(f"Failed to upsert player stats: {e}")
                results["errors"].append(f"Player stats upsert failed: {str(e)}")
                raise  # Stats are critical
        else:
            error_msg = "No player stats data found - this is critical"
            logger.error(error_msg)
            results["errors"].append(error_msg)
            results["statusCode"] = 500
            return {
                "statusCode": 500,
                "body": json.dumps(results),
            }

        # 4. Upsert teams (if available)
        if teams_s3 and teams_s3.get("teams"):
            try:
                count = upsert_teams(cursor, teams_s3["teams"])
                results["loaded"]["teams"] = count
            except Exception as e:
                logger.error(f"Failed to upsert teams: {e}")
                results["errors"].append(f"Teams upsert failed: {str(e)}")
        else:
            logger.warning("No teams data found - skipping teams upsert")

        # Commit transaction
        conn.commit()
        logger.info("Successfully committed all data to RDS")

        # Generate summary
        results["summary"] = {
            "environment": ENVIRONMENT,
            "partition": partition,
            "records_loaded": sum(results["loaded"].values()),
            "tables_updated": len(results["loaded"]),
            "errors_count": len(results["errors"]),
        }

        logger.info(f"Data load completed: {results['summary']}")

        return {
            "statusCode": 200,
            "body": json.dumps(results),
            "load_successful": len(results["errors"]) == 0,
            "records_loaded": results["loaded"],
        }

    except Exception as e:
        # Rollback on error
        if conn:
            conn.rollback()
            logger.error("Transaction rolled back due to error")

        logger.error(f"Unexpected error in data load: {e}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "error": str(e),
                    "message": "Data load to RDS failed",
                    "errors": results.get("errors", []),
                }
            ),
        }

    finally:
        # Close connection
        if conn:
            conn.close()
            logger.info("Database connection closed")
