"""
API Lambda handler for NBA Cap Optimizer predictions API.

Provides public REST API endpoints for querying player value predictions
and team efficiency metrics.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional, Union
from urllib.parse import unquote

import boto3
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DB_SECRET_ARN = os.environ.get("DB_SECRET_ARN")
CURRENT_SEASON = os.environ.get("CURRENT_SEASON", "2025-26")

# Lazy-load secrets client to avoid import-time AWS config errors in tests
_secrets_client = None


def get_secrets_client():
    """Get or create secrets manager client."""
    global _secrets_client
    if _secrets_client is None:
        _secrets_client = boto3.client("secretsmanager")
    return _secrets_client


def get_db_connection():
    """Get database connection from Secrets Manager."""
    logger.info(f"Loading DB credentials from {DB_SECRET_ARN}")
    secrets_client = get_secrets_client()
    secret_value = secrets_client.get_secret_value(SecretId=DB_SECRET_ARN)
    credentials = json.loads(secret_value["SecretString"])

    conn = psycopg2.connect(
        host=credentials["host"],
        port=credentials["port"],
        database=credentials["dbname"],
        user=credentials["username"],
        password=credentials["password"],
        cursor_factory=RealDictCursor,
    )
    return conn


def cors_headers():
    """Return CORS headers for public access."""
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Methods": "GET,OPTIONS",
    }


def success_response(data: Any, status_code: int = 200) -> Dict:
    """Format success response."""
    return {
        "statusCode": status_code,
        "headers": cors_headers(),
        "body": json.dumps(data, default=str),
    }


def error_response(message: str, status_code: int = 500) -> Dict:
    """Format error response."""
    return {
        "statusCode": status_code,
        "headers": cors_headers(),
        "body": json.dumps({"error": message}),
    }


def get_all_predictions(conn, query_params: Dict) -> List[Dict]:
    """
    GET /predictions
    Query all predictions with optional filtering.

    Query params:
    - value_category: Filter by Bargain/Fair/Overpaid
    - team: Filter by team abbreviation
    - position: Filter by player position
    - sort_by: Sort field (default: inefficiency_score)
    - limit: Max results (default: 1000, enough for all NBA players)
    - offset: Pagination offset (default: 0)
    """
    value_category = query_params.get("value_category")
    team = query_params.get("team")
    position = query_params.get("position")
    sort_by = query_params.get("sort_by", "inefficiency_score")
    limit = int(query_params.get("limit", 1000))
    offset = int(query_params.get("offset", 0))

    allowed_sorts = [
        "inefficiency_score",
        "predicted_fmv",
        "actual_salary",
        "vorp",
        "player_name",
    ]
    if sort_by not in allowed_sorts:
        sort_by = "inefficiency_score"

    query = """
        WITH latest_run AS (
            SELECT run_id, prediction_date
            FROM predictions
            WHERE season = %s
            ORDER BY prediction_date DESC
            LIMIT 1
        ),
        previous_run AS (
            SELECT run_id
            FROM predictions
            WHERE season = %s
            ORDER BY prediction_date DESC
            LIMIT 1 OFFSET 1
        ),
        current_ranks AS (
            SELECT
                player_name,
                ROW_NUMBER() OVER (ORDER BY inefficiency_score ASC) as current_rank
            FROM predictions
            WHERE season = %s
              AND run_id = (SELECT run_id FROM latest_run)
        ),
        previous_ranks AS (
            SELECT
                player_name,
                ROW_NUMBER() OVER (ORDER BY inefficiency_score ASC) as previous_rank
            FROM predictions
            WHERE season = %s
              AND run_id = (SELECT run_id FROM previous_run)
        )
        SELECT
            p.player_name,
            p.season,
            p.predicted_fmv,
            p.actual_salary,
            p.predicted_salary_cap_pct,
            p.actual_salary_cap_pct,
            p.inefficiency_score,
            p.value_category,
            p.value_over_replacement as vorp,
            p.model_version,
            p.run_id,
            p.prediction_date,
            cr.current_rank as rank,
            pr.previous_rank,
            CASE
                WHEN pr.previous_rank IS NOT NULL
                THEN pr.previous_rank - cr.current_rank
                ELSE NULL
            END as rank_change,
            ps.team_abbreviation,
            ps.position,
            ps.age,
            ps.games_played,
            ps.points,
            ps.rebounds,
            ps.assists
        FROM predictions p
        LEFT JOIN current_ranks cr ON p.player_name = cr.player_name
        LEFT JOIN previous_ranks pr ON p.player_name = pr.player_name
        LEFT JOIN player_stats ps
            ON p.player_name = ps.player_name
            AND p.season = ps.season
        WHERE p.season = %s
          AND p.run_id = (SELECT run_id FROM latest_run)
    """

    params: List[Union[str, int]] = [
        CURRENT_SEASON,
        CURRENT_SEASON,
        CURRENT_SEASON,
        CURRENT_SEASON,
        CURRENT_SEASON,
    ]

    if value_category:
        query += " AND p.value_category = %s"
        params.append(value_category)

    if team:
        query += " AND ps.team_abbreviation = %s"
        params.append(team.upper())

    if position:
        query += " AND ps.position = %s"
        params.append(position.upper())

    query += f" ORDER BY {sort_by} ASC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    cur = conn.cursor()
    cur.execute(query, params)
    results = cur.fetchall()
    cur.close()

    return [dict(row) for row in results]


def get_undervalued_predictions(conn, query_params: Dict) -> List[Dict]:
    """
    GET /predictions/undervalued
    Get top undervalued players (bargain contracts).

    Query params:
    - limit: Max results (default: 25)
    """
    limit = int(query_params.get("limit", 25))

    query = """
        WITH latest_run AS (
            SELECT run_id
            FROM predictions
            WHERE season = %s
            ORDER BY prediction_date DESC
            LIMIT 1
        ),
        previous_run AS (
            SELECT run_id
            FROM predictions
            WHERE season = %s
            ORDER BY prediction_date DESC
            LIMIT 1 OFFSET 1
        ),
        current_ranks AS (
            SELECT
                player_name,
                ROW_NUMBER() OVER (ORDER BY inefficiency_score ASC) as current_rank
            FROM predictions
            WHERE season = %s
              AND run_id = (SELECT run_id FROM latest_run)
        ),
        previous_ranks AS (
            SELECT
                player_name,
                ROW_NUMBER() OVER (ORDER BY inefficiency_score ASC) as previous_rank
            FROM predictions
            WHERE season = %s
              AND run_id = (SELECT run_id FROM previous_run)
        )
        SELECT
            p.player_name,
            p.season,
            p.predicted_fmv,
            p.actual_salary,
            p.predicted_salary_cap_pct,
            p.actual_salary_cap_pct,
            p.inefficiency_score,
            p.value_category,
            p.value_over_replacement as vorp,
            cr.current_rank as rank,
            pr.previous_rank,
            CASE
                WHEN pr.previous_rank IS NOT NULL
                THEN pr.previous_rank - cr.current_rank
                ELSE NULL
            END as rank_change,
            ps.team_abbreviation,
            ps.position,
            ps.age,
            ps.points,
            ps.rebounds,
            ps.assists
        FROM predictions p
        LEFT JOIN current_ranks cr ON p.player_name = cr.player_name
        LEFT JOIN previous_ranks pr ON p.player_name = pr.player_name
        LEFT JOIN player_stats ps
            ON p.player_name = ps.player_name
            AND p.season = ps.season
        WHERE p.season = %s
          AND p.run_id = (SELECT run_id FROM latest_run)
          AND p.value_category = 'Bargain'
        ORDER BY p.inefficiency_score ASC
        LIMIT %s
    """

    cur = conn.cursor()
    cur.execute(
        query,
        [CURRENT_SEASON, CURRENT_SEASON, CURRENT_SEASON, CURRENT_SEASON, CURRENT_SEASON, limit],
    )
    results = cur.fetchall()
    cur.close()

    return [dict(row) for row in results]


def get_overvalued_predictions(conn, query_params: Dict) -> List[Dict]:
    """
    GET /predictions/overvalued
    Get top overvalued players (overpaid contracts).

    Query params:
    - limit: Max results (default: 25)
    """
    limit = int(query_params.get("limit", 25))

    query = """
        WITH latest_run AS (
            SELECT run_id
            FROM predictions
            WHERE season = %s
            ORDER BY prediction_date DESC
            LIMIT 1
        ),
        previous_run AS (
            SELECT run_id
            FROM predictions
            WHERE season = %s
            ORDER BY prediction_date DESC
            LIMIT 1 OFFSET 1
        ),
        current_ranks AS (
            SELECT
                player_name,
                ROW_NUMBER() OVER (ORDER BY inefficiency_score ASC) as current_rank
            FROM predictions
            WHERE season = %s
              AND run_id = (SELECT run_id FROM latest_run)
        ),
        previous_ranks AS (
            SELECT
                player_name,
                ROW_NUMBER() OVER (ORDER BY inefficiency_score ASC) as previous_rank
            FROM predictions
            WHERE season = %s
              AND run_id = (SELECT run_id FROM previous_run)
        )
        SELECT
            p.player_name,
            p.season,
            p.predicted_fmv,
            p.actual_salary,
            p.predicted_salary_cap_pct,
            p.actual_salary_cap_pct,
            p.inefficiency_score,
            p.value_category,
            p.value_over_replacement as vorp,
            cr.current_rank as rank,
            pr.previous_rank,
            CASE
                WHEN pr.previous_rank IS NOT NULL
                THEN pr.previous_rank - cr.current_rank
                ELSE NULL
            END as rank_change,
            ps.team_abbreviation,
            ps.position,
            ps.age,
            ps.points,
            ps.rebounds,
            ps.assists
        FROM predictions p
        LEFT JOIN current_ranks cr ON p.player_name = cr.player_name
        LEFT JOIN previous_ranks pr ON p.player_name = pr.player_name
        LEFT JOIN player_stats ps
            ON p.player_name = ps.player_name
            AND p.season = ps.season
        WHERE p.season = %s
          AND p.run_id = (SELECT run_id FROM latest_run)
          AND p.value_category = 'Overpaid'
        ORDER BY p.inefficiency_score DESC
        LIMIT %s
    """

    cur = conn.cursor()
    cur.execute(
        query,
        [CURRENT_SEASON, CURRENT_SEASON, CURRENT_SEASON, CURRENT_SEASON, CURRENT_SEASON, limit],
    )
    results = cur.fetchall()
    cur.close()

    return [dict(row) for row in results]


def get_player_prediction(conn, player_name: str) -> Optional[Dict]:
    """
    GET /predictions/{player_name}
    Get prediction for specific player in current season.
    """
    query = """
        WITH latest_run AS (
            SELECT run_id
            FROM predictions
            WHERE season = %s
            ORDER BY prediction_date DESC
            LIMIT 1
        ),
        previous_run AS (
            SELECT run_id
            FROM predictions
            WHERE season = %s
            ORDER BY prediction_date DESC
            LIMIT 1 OFFSET 1
        ),
        current_ranks AS (
            SELECT
                player_name,
                ROW_NUMBER() OVER (ORDER BY inefficiency_score ASC) as current_rank
            FROM predictions
            WHERE season = %s
              AND run_id = (SELECT run_id FROM latest_run)
        ),
        previous_ranks AS (
            SELECT
                player_name,
                ROW_NUMBER() OVER (ORDER BY inefficiency_score ASC) as previous_rank
            FROM predictions
            WHERE season = %s
              AND run_id = (SELECT run_id FROM previous_run)
        )
        SELECT
            p.player_name,
            p.season,
            p.predicted_fmv,
            p.actual_salary,
            p.predicted_salary_cap_pct,
            p.actual_salary_cap_pct,
            p.predicted_salary_pct_of_max,
            p.inefficiency_score,
            p.value_category,
            p.value_over_replacement as vorp,
            p.model_version,
            p.run_id,
            p.prediction_date,
            cr.current_rank as rank,
            pr.previous_rank,
            CASE
                WHEN pr.previous_rank IS NOT NULL
                THEN pr.previous_rank - cr.current_rank
                ELSE NULL
            END as rank_change,
            ps.team_abbreviation,
            ps.position,
            ps.age,
            ps.games_played,
            ps.games_started,
            ps.minutes,
            ps.points,
            ps.rebounds,
            ps.assists,
            ps.steals,
            ps.blocks,
            ps.fg_pct,
            ps.fg3_pct,
            ps.ft_pct,
            ps.per,
            ps.ts_pct,
            ps.usg_pct,
            ps.ws,
            ps.bpm
        FROM predictions p
        LEFT JOIN current_ranks cr ON p.player_name = cr.player_name
        LEFT JOIN previous_ranks pr ON p.player_name = pr.player_name
        LEFT JOIN player_stats ps
            ON p.player_name = ps.player_name
            AND p.season = ps.season
        WHERE p.player_name = %s
          AND p.season = %s
          AND p.run_id = (SELECT run_id FROM latest_run)
    """

    cur = conn.cursor()
    cur.execute(
        query,
        [
            CURRENT_SEASON,
            CURRENT_SEASON,
            CURRENT_SEASON,
            CURRENT_SEASON,
            player_name,
            CURRENT_SEASON,
        ],
    )
    result = cur.fetchone()
    cur.close()

    return dict(result) if result else None


def get_all_teams(conn, query_params: Dict) -> List[Dict]:
    """
    GET /teams
    Get team efficiency rankings with aggregated metrics.

    Query params:
    - sort_by: Sort field (avg_inefficiency, net_efficiency, bargain_count)
    """
    sort_by = query_params.get("sort_by", "avg_inefficiency")

    sort_mapping = {
        "avg_inefficiency": "avg_inefficiency_score",
        "net_efficiency": "net_efficiency",
        "bargain_count": "bargain_count",
        "overpaid_count": "overpaid_count",
    }

    sort_column = sort_mapping.get(sort_by, "avg_inefficiency_score")

    query = f"""
        WITH latest_run AS (
            SELECT run_id
            FROM predictions
            WHERE season = %s
            ORDER BY prediction_date DESC
            LIMIT 1
        ),
        team_metrics AS (
            SELECT
                ps.team_abbreviation,
                COUNT(*) as player_count,
                AVG(p.inefficiency_score) as avg_inefficiency_score,
                SUM(CASE
                    WHEN p.inefficiency_score > 0
                    THEN p.actual_salary - p.predicted_fmv
                    ELSE 0
                END) as total_overspend,
                SUM(CASE
                    WHEN p.inefficiency_score < 0
                    THEN p.actual_salary - p.predicted_fmv
                    ELSE 0
                END) as total_underspend,
                SUM(p.actual_salary - p.predicted_fmv) as net_efficiency,
                SUM(CASE WHEN p.value_category = 'Bargain' THEN 1 ELSE 0 END) as bargain_count,
                SUM(CASE WHEN p.value_category = 'Fair' THEN 1 ELSE 0 END) as fair_count,
                SUM(CASE WHEN p.value_category = 'Overpaid' THEN 1 ELSE 0 END) as overpaid_count,
                SUM(p.actual_salary) as total_payroll
            FROM predictions p
            INNER JOIN player_stats ps
                ON p.player_name = ps.player_name
                AND p.season = ps.season
            WHERE p.season = %s
              AND p.run_id = (SELECT run_id FROM latest_run)
            GROUP BY ps.team_abbreviation
        )
        SELECT
            t.abbreviation as team_abbreviation,
            t.full_name,
            tm.player_count,
            tm.total_payroll,
            tm.avg_inefficiency_score,
            tm.total_overspend,
            tm.total_underspend,
            tm.net_efficiency,
            tm.bargain_count,
            tm.fair_count,
            tm.overpaid_count
        FROM team_metrics tm
        LEFT JOIN teams t ON tm.team_abbreviation = t.abbreviation
        ORDER BY {sort_column} ASC
    """

    cur = conn.cursor()
    cur.execute(query, [CURRENT_SEASON, CURRENT_SEASON])
    results = cur.fetchall()
    cur.close()

    return [dict(row) for row in results]


def get_team_detail(conn, team_abbreviation: str) -> Optional[Dict]:
    """
    GET /teams/{team_abbreviation}
    Get detailed team efficiency breakdown with roster.
    """
    team_abbr = team_abbreviation.upper()

    team_query = """
        WITH latest_run AS (
            SELECT run_id
            FROM predictions
            WHERE season = %s
            ORDER BY prediction_date DESC
            LIMIT 1
        ),
        team_metrics AS (
            SELECT
                ps.team_abbreviation,
                COUNT(*) as player_count,
                AVG(p.inefficiency_score) as avg_inefficiency_score,
                SUM(CASE
                    WHEN p.inefficiency_score > 0
                    THEN p.actual_salary - p.predicted_fmv
                    ELSE 0
                END) as total_overspend,
                SUM(CASE
                    WHEN p.inefficiency_score < 0
                    THEN p.actual_salary - p.predicted_fmv
                    ELSE 0
                END) as total_underspend,
                SUM(p.actual_salary - p.predicted_fmv) as net_efficiency,
                SUM(CASE WHEN p.value_category = 'Bargain' THEN 1 ELSE 0 END) as bargain_count,
                SUM(CASE WHEN p.value_category = 'Fair' THEN 1 ELSE 0 END) as fair_count,
                SUM(CASE WHEN p.value_category = 'Overpaid' THEN 1 ELSE 0 END) as overpaid_count,
                SUM(p.actual_salary) as total_payroll
            FROM predictions p
            INNER JOIN player_stats ps
                ON p.player_name = ps.player_name
                AND p.season = ps.season
            WHERE p.season = %s
              AND p.run_id = (SELECT run_id FROM latest_run)
              AND ps.team_abbreviation = %s
            GROUP BY ps.team_abbreviation
        )
        SELECT
            t.abbreviation as team_abbreviation,
            t.full_name,
            tm.player_count,
            tm.total_payroll,
            tm.avg_inefficiency_score,
            tm.total_overspend,
            tm.total_underspend,
            tm.net_efficiency,
            tm.bargain_count,
            tm.fair_count,
            tm.overpaid_count
        FROM team_metrics tm
        LEFT JOIN teams t ON tm.team_abbreviation = t.abbreviation
    """

    cur = conn.cursor()
    cur.execute(team_query, [CURRENT_SEASON, CURRENT_SEASON, team_abbr])
    team_result = cur.fetchone()

    if not team_result:
        cur.close()
        return None

    team_data = dict(team_result)

    roster_query = """
        WITH latest_run AS (
            SELECT run_id
            FROM predictions
            WHERE season = %s
            ORDER BY prediction_date DESC
            LIMIT 1
        ),
        previous_run AS (
            SELECT run_id
            FROM predictions
            WHERE season = %s
            ORDER BY prediction_date DESC
            LIMIT 1 OFFSET 1
        ),
        current_ranks AS (
            SELECT
                player_name,
                ROW_NUMBER() OVER (ORDER BY inefficiency_score ASC) as current_rank
            FROM predictions
            WHERE season = %s
              AND run_id = (SELECT run_id FROM latest_run)
        ),
        previous_ranks AS (
            SELECT
                player_name,
                ROW_NUMBER() OVER (ORDER BY inefficiency_score ASC) as previous_rank
            FROM predictions
            WHERE season = %s
              AND run_id = (SELECT run_id FROM previous_run)
        )
        SELECT
            p.player_name,
            p.predicted_fmv,
            p.actual_salary,
            p.predicted_salary_cap_pct,
            p.actual_salary_cap_pct,
            p.inefficiency_score,
            p.value_category,
            p.value_over_replacement as vorp,
            cr.current_rank as rank,
            pr.previous_rank,
            CASE
                WHEN pr.previous_rank IS NOT NULL
                THEN pr.previous_rank - cr.current_rank
                ELSE NULL
            END as rank_change,
            ps.position,
            ps.age,
            ps.points,
            ps.rebounds,
            ps.assists
        FROM predictions p
        LEFT JOIN current_ranks cr ON p.player_name = cr.player_name
        LEFT JOIN previous_ranks pr ON p.player_name = pr.player_name
        INNER JOIN player_stats ps
            ON p.player_name = ps.player_name
            AND p.season = ps.season
        WHERE p.season = %s
          AND p.run_id = (SELECT run_id FROM latest_run)
          AND ps.team_abbreviation = %s
        ORDER BY p.actual_salary DESC
    """

    cur.execute(
        roster_query,
        [CURRENT_SEASON, CURRENT_SEASON, CURRENT_SEASON, CURRENT_SEASON, CURRENT_SEASON, team_abbr],
    )
    roster_results = cur.fetchall()
    cur.close()

    team_data["players"] = [dict(row) for row in roster_results]

    return team_data


def handler(event, context):
    """
    Main Lambda handler for API Gateway requests.

    Routes requests to appropriate handler based on path.
    """
    logger.info(f"API request: {json.dumps(event)}")

    http_method = event.get("httpMethod", "GET")
    path = event.get("path", "/")
    query_params = event.get("queryStringParameters") or {}
    path_params = event.get("pathParameters") or {}

    if http_method == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": cors_headers(),
            "body": json.dumps({"message": "OK"}),
        }

    try:
        conn = get_db_connection()
        try:
            if path == "/predictions" and http_method == "GET":
                results = get_all_predictions(conn, query_params)
                return success_response({"predictions": results, "count": len(results)})

            elif path == "/predictions/undervalued" and http_method == "GET":
                results = get_undervalued_predictions(conn, query_params)
                return success_response({"predictions": results, "count": len(results)})

            elif path == "/predictions/overvalued" and http_method == "GET":
                results = get_overvalued_predictions(conn, query_params)
                return success_response({"predictions": results, "count": len(results)})

            elif path.startswith("/predictions/") and http_method == "GET":
                player_name = unquote(path_params.get("player_name", ""))
                if not player_name:
                    return error_response("Player name required", 400)

                result = get_player_prediction(conn, player_name)

                if not result:
                    return error_response(f"Player '{player_name}' not found", 404)

                return success_response(result)

            elif path == "/teams" and http_method == "GET":
                results = get_all_teams(conn, query_params)
                return success_response({"teams": results, "count": len(results)})

            elif path.startswith("/teams/") and http_method == "GET":
                team_abbr = path_params.get("team_abbreviation", "")
                if not team_abbr:
                    return error_response("Team abbreviation required", 400)

                result = get_team_detail(conn, team_abbr)

                if not result:
                    return error_response(f"Team '{team_abbr}' not found", 404)

                return success_response(result)

            else:
                return error_response(f"Route not found: {http_method} {path}", 404)

        finally:
            conn.close()

    except Exception as e:
        logger.error(f"API error: {e}", exc_info=True)
        return error_response(str(e), 500)


if __name__ == "__main__":
    test_event = {
        "httpMethod": "GET",
        "path": "/predictions/undervalued",
        "queryStringParameters": {"limit": "10"},
        "pathParameters": {},
    }

    class Context:
        function_name = "api_handler"
        aws_request_id = "test-123"

    result = handler(test_event, Context())
    print(json.dumps(json.loads(result["body"]), indent=2))
