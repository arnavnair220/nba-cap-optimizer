"""
Tests for transform_data Lambda function (Basketball Reference version).
"""

import json
from unittest.mock import MagicMock, patch

from src.lambdas.etl import transform_data


class TestNormalizeToAscii:
    """Test Unicode to ASCII normalization."""

    def test_removes_accent_from_jokic(self):
        """Test normalization removes accent from Jokić."""
        result = transform_data.normalize_to_ascii("Nikola Jokić")
        assert result == "Nikola Jokic"

    def test_removes_accent_from_doncic(self):
        """Test normalization removes accent from Dončić."""
        result = transform_data.normalize_to_ascii("Luka Dončić")
        assert result == "Luka Doncic"

    def test_removes_accent_from_diabate(self):
        """Test normalization removes accent from Diabaté."""
        result = transform_data.normalize_to_ascii("Ousmane Diabaté")
        assert result == "Ousmane Diabate"

    def test_handles_multiple_accents(self):
        """Test normalization handles multiple accented characters."""
        result = transform_data.normalize_to_ascii("José María Álvarez")
        assert result == "Jose Maria Alvarez"

    def test_leaves_ascii_unchanged(self):
        """Test normalization doesn't affect ASCII text."""
        result = transform_data.normalize_to_ascii("LeBron James")
        assert result == "LeBron James"

    def test_handles_empty_string(self):
        """Test normalization handles empty string."""
        result = transform_data.normalize_to_ascii("")
        assert result == ""

    def test_handles_none(self):
        """Test normalization handles None."""
        result = transform_data.normalize_to_ascii(None)
        assert result is None


class TestNormalizeTeamAbbreviation:
    """Test team abbreviation normalization."""

    def test_normalizes_brk_to_bkn(self):
        """Test Brooklyn Nets abbreviation is normalized."""
        result = transform_data.normalize_team_abbreviation("BRK")
        assert result == "BKN"

    def test_normalizes_cho_to_cha(self):
        """Test Charlotte Hornets abbreviation is normalized."""
        result = transform_data.normalize_team_abbreviation("CHO")
        assert result == "CHA"

    def test_normalizes_pho_to_phx(self):
        """Test Phoenix Suns abbreviation is normalized."""
        result = transform_data.normalize_team_abbreviation("PHO")
        assert result == "PHX"

    def test_leaves_other_abbreviations_unchanged(self):
        """Test other abbreviations are not modified."""
        result = transform_data.normalize_team_abbreviation("LAL")
        assert result == "LAL"

        result = transform_data.normalize_team_abbreviation("GSW")
        assert result == "GSW"

        result = transform_data.normalize_team_abbreviation("BOS")
        assert result == "BOS"


class TestEnrichPlayerStats:
    """Test player stats enrichment with Basketball Reference data."""

    def test_filters_out_league_average_summary_row(self):
        """Test that 'League Average' summary row is filtered out."""
        stats_data = {
            "season": "2025-26",
            "source": "basketball_reference",
            "per_game_stats": [
                {
                    "Player": "LeBron James",
                    "Team": "LAL",
                    "PTS": 25.8,
                    "TRB": 7.7,
                    "AST": 8.2,
                },
                {
                    "Player": "League Average",
                    "Team": None,
                    "PTS": float("nan"),
                    "TRB": float("nan"),
                    "AST": float("nan"),
                },
            ],
            "advanced_stats": [],
        }

        result = transform_data.enrich_player_stats(stats_data)

        assert len(result) == 1
        assert result[0]["player_name"] == "LeBron James"

    def test_enrichment_merges_per_game_and_advanced_stats(self):
        """Test enrichment merges per-game and advanced stats."""
        stats_data = {
            "season": "2025-26",
            "source": "basketball_reference",
            "per_game_stats": [
                {
                    "Player": "LeBron James",
                    "Pos": "SF",
                    "Age": 39,
                    "Team": "LAL",
                    "G": 50,
                    "GS": 48,
                    "MP": 35.2,
                    "PTS": 25.8,
                    "TRB": 7.7,
                    "AST": 8.2,
                    "eFG%": 0.578,
                }
            ],
            "advanced_stats": [
                {
                    "Player": "LeBron James",
                    "PER": 24.5,
                    "TS%": 0.623,
                    "USG%": 29.2,
                    "WS": 7.5,
                    "BPM": 6.8,
                    "VORP": 4.2,
                }
            ],
        }

        result = transform_data.enrich_player_stats(stats_data)

        assert len(result) == 1
        player = result[0]

        # Check per-game stats
        assert player["player_name"] == "LeBron James"
        assert player["age"] == 39
        assert player["points"] == 25.8

        # Check multi-team fields
        assert player["is_multi_team"] is False
        assert player["team_abbreviation"] == "LAL"
        assert player["teams_played_for"] == ["LAL"]
        assert len(player["stats_by_team"]) == 1
        assert player["stats_by_team"][0]["team_abbreviation"] == "LAL"
        assert player["stats_by_team"][0]["points"] == 25.8

        # Check efg_pct from per-game stats
        assert player["efg_pct"] == 0.578

        # Check advanced stats
        assert player["per"] == 24.5
        assert player["ts_pct"] == 0.623
        assert player["vorp"] == 4.2

    def test_enrichment_normalizes_team_abbreviations(self):
        """Test enrichment normalizes Basketball Reference team abbreviations."""
        stats_data = {
            "season": "2025-26",
            "source": "basketball_reference",
            "per_game_stats": [
                {"Player": "Kevin Durant", "Team": "PHO", "PTS": 28.5, "TRB": 7.0, "AST": 5.5},
                {"Player": "Mikal Bridges", "Team": "BRK", "PTS": 20.1, "TRB": 4.5, "AST": 3.7},
                {"Player": "LaMelo Ball", "Team": "CHO", "PTS": 23.9, "TRB": 6.2, "AST": 8.4},
            ],
            "advanced_stats": [],
        }

        result = transform_data.enrich_player_stats(stats_data)

        assert len(result) == 3
        assert result[0]["team_abbreviation"] == "PHX"  # PHO -> PHX
        assert result[0]["teams_played_for"] == ["PHX"]  # PHO -> PHX
        assert result[1]["team_abbreviation"] == "BKN"  # BRK -> BKN
        assert result[1]["teams_played_for"] == ["BKN"]  # BRK -> BKN
        assert result[2]["team_abbreviation"] == "CHA"  # CHO -> CHA
        assert result[2]["teams_played_for"] == ["CHA"]  # CHO -> CHA

    def test_enrichment_handles_multi_team_players(self):
        """Test enrichment handles multi-team players correctly."""
        stats_data = {
            "season": "2025-26",
            "source": "basketball_reference",
            "per_game_stats": [
                {
                    "Player": "Kevin Huerter",
                    "Age": 27,
                    "Pos": "SF",
                    "Team": "2TM",
                    "G": 48,
                    "PTS": 10.3,
                    "TRB": 3.7,
                    "AST": 2.5,
                },
                {
                    "Player": "Kevin Huerter",
                    "Age": 27,
                    "Pos": "SF",
                    "Team": "CHI",
                    "G": 44,
                    "PTS": 10.9,
                    "TRB": 3.8,
                    "AST": 2.6,
                },
                {
                    "Player": "Kevin Huerter",
                    "Age": 27,
                    "Pos": "SG",
                    "Team": "DET",
                    "G": 4,
                    "PTS": 4.3,
                    "TRB": 1.8,
                    "AST": 0.8,
                },
            ],
            "advanced_stats": [],
        }

        result = transform_data.enrich_player_stats(stats_data)

        assert len(result) == 1
        player = result[0]

        # Check multi-team metadata
        assert player["is_multi_team"] is True
        assert player["team_abbreviation"] == "2TM"
        assert player["teams_played_for"] == ["CHI", "DET"]

        # Check main stats use aggregate row (2TM)
        assert player["games_played"] == 48
        assert player["points"] == 10.3

        # Check stats_by_team has individual team breakdowns
        assert len(player["stats_by_team"]) == 2
        chi_stats = next(s for s in player["stats_by_team"] if s["team_abbreviation"] == "CHI")
        det_stats = next(s for s in player["stats_by_team"] if s["team_abbreviation"] == "DET")

        assert chi_stats["games_played"] == 44
        assert chi_stats["points"] == 10.9
        assert det_stats["games_played"] == 4
        assert det_stats["points"] == 4.3

    def test_enrichment_handles_empty_stats(self):
        """Test enrichment with no players."""
        stats_data = {
            "season": "2025-26",
            "source": "basketball_reference",
            "per_game_stats": [],
            "advanced_stats": [],
        }

        result = transform_data.enrich_player_stats(stats_data)

        assert result == []

    def test_enrichment_normalizes_unicode_player_names(self):
        """Test that Unicode player names are normalized to ASCII during enrichment."""
        stats_data = {
            "season": "2025-26",
            "source": "basketball_reference",
            "per_game_stats": [
                {
                    "Player": "Nikola Jokić",  # Unicode input
                    "Pos": "C",
                    "Age": 29,
                    "Team": "DEN",
                    "G": 60,
                    "PTS": 26.0,
                    "TRB": 12.0,
                    "AST": 9.0,
                },
                {
                    "Player": "Luka Dončić",  # Unicode input
                    "Pos": "PG",
                    "Age": 25,
                    "Team": "DAL",
                    "G": 58,
                    "PTS": 29.0,
                    "TRB": 9.0,
                    "AST": 8.5,
                },
            ],
            "advanced_stats": [
                {"Player": "Nikola Jokić", "PER": 31.0, "VORP": 8.0},
                {"Player": "Luka Dončić", "PER": 28.0, "VORP": 6.5},
            ],
        }

        result = transform_data.enrich_player_stats(stats_data)

        assert len(result) == 2

        # Verify player names are normalized to ASCII
        jokic = next(p for p in result if "Jokic" in p["player_name"])
        doncic = next(p for p in result if "Doncic" in p["player_name"])

        assert jokic["player_name"] == "Nikola Jokic"  # Normalized from Jokić
        assert doncic["player_name"] == "Luka Doncic"  # Normalized from Dončić

        # Verify advanced stats still match after normalization
        assert jokic["per"] == 31.0
        assert jokic["vorp"] == 8.0
        assert doncic["per"] == 28.0
        assert doncic["vorp"] == 6.5


class TestEnrichTeamData:
    """Test team data enrichment."""

    def test_enrichment_calculates_salary_metrics(self):
        """Test team enrichment calculates salary aggregations."""
        teams = [{"id": 1, "abbreviation": "GSW", "full_name": "Golden State Warriors"}]
        salaries = [
            {"player_name": "Player 1", "annual_salary": 50000000},
            {"player_name": "Player 2", "annual_salary": 30000000},
        ]
        stats = [
            {
                "player_name": "Player 1",
                "is_multi_team": False,
                "teams_played_for": ["GSW"],
                "points": 25.0,
                "rebounds": 7.0,
                "assists": 8.0,
                "stats_by_team": [
                    {
                        "team_abbreviation": "GSW",
                        "points": 25.0,
                        "rebounds": 7.0,
                        "assists": 8.0,
                    }
                ],
            },
            {
                "player_name": "Player 2",
                "is_multi_team": False,
                "teams_played_for": ["GSW"],
                "points": 15.0,
                "rebounds": 5.0,
                "assists": 4.0,
                "stats_by_team": [
                    {
                        "team_abbreviation": "GSW",
                        "points": 15.0,
                        "rebounds": 5.0,
                        "assists": 4.0,
                    }
                ],
            },
        ]

        result = transform_data.enrich_team_data(teams, salaries, stats)

        assert result[0]["total_payroll"] == 80000000
        assert result[0]["roster_count"] == 2

    def test_enrichment_matches_normalized_team_abbreviations(self):
        """Test team enrichment works with normalized abbreviations from Basketball Reference."""
        # Phoenix Suns - NBA API uses PHX, Basketball Reference uses PHO (normalized to PHX)
        teams = [{"id": 1, "abbreviation": "PHX", "full_name": "Phoenix Suns"}]
        salaries = [
            {"player_name": "Kevin Durant", "annual_salary": 47000000},
            {"player_name": "Devin Booker", "annual_salary": 36000000},
        ]
        # Stats would have PHX after normalization from PHO
        stats = [
            {
                "player_name": "Kevin Durant",
                "is_multi_team": False,
                "teams_played_for": ["PHX"],
                "points": 28.5,
                "rebounds": 7.0,
                "assists": 5.5,
                "stats_by_team": [
                    {
                        "team_abbreviation": "PHX",
                        "points": 28.5,
                        "rebounds": 7.0,
                        "assists": 5.5,
                    }
                ],
            },
            {
                "player_name": "Devin Booker",
                "is_multi_team": False,
                "teams_played_for": ["PHX"],
                "points": 27.1,
                "rebounds": 4.5,
                "assists": 6.9,
                "stats_by_team": [
                    {
                        "team_abbreviation": "PHX",
                        "points": 27.1,
                        "rebounds": 4.5,
                        "assists": 6.9,
                    }
                ],
            },
        ]

        result = transform_data.enrich_team_data(teams, salaries, stats)

        assert result[0]["total_payroll"] == 83000000
        assert result[0]["roster_count"] == 2
        assert result[0]["roster_with_salary"] == 2

    def test_enrichment_matches_unicode_to_ascii_names(self):
        """
        Test that players with Unicode names in stats are normalized to ASCII.
        This verifies the normalize_to_ascii function is being used correctly.
        """
        teams = [
            {"id": 1, "abbreviation": "DEN", "full_name": "Denver Nuggets"},
            {"id": 2, "abbreviation": "DAL", "full_name": "Dallas Mavericks"},
        ]

        # Salary data uses ASCII names (like ESPN provides)
        salaries = [
            {"player_name": "Nikola Jokic", "annual_salary": 51000000},
            {"player_name": "Luka Doncic", "annual_salary": 43000000},
        ]

        # Stats data already normalized to ASCII (normalized during enrichment)
        stats = [
            {
                "player_name": "Nikola Jokic",  # Normalized to ASCII
                "is_multi_team": False,
                "teams_played_for": ["DEN"],
                "points": 26.0,
                "rebounds": 12.0,
                "assists": 9.0,
                "stats_by_team": [
                    {
                        "team_abbreviation": "DEN",
                        "points": 26.0,
                        "rebounds": 12.0,
                        "assists": 9.0,
                    }
                ],
            },
            {
                "player_name": "Luka Doncic",  # Normalized to ASCII
                "is_multi_team": False,
                "teams_played_for": ["DAL"],
                "points": 29.0,
                "rebounds": 9.0,
                "assists": 8.5,
                "stats_by_team": [
                    {
                        "team_abbreviation": "DAL",
                        "points": 29.0,
                        "rebounds": 9.0,
                        "assists": 8.5,
                    }
                ],
            },
        ]

        result = transform_data.enrich_team_data(teams, salaries, stats)

        # Verify DEN (Nuggets) has correct salary and roster
        den_team = next(t for t in result if t["abbreviation"] == "DEN")
        assert den_team["total_payroll"] == 51000000
        assert den_team["roster_count"] == 1
        assert den_team["roster_with_salary"] == 1
        assert den_team["top_paid_player"] == "Nikola Jokic"  # ASCII in output
        assert den_team["top_paid_salary"] == 51000000

        # Verify DAL (Mavericks) has correct salary and roster
        dal_team = next(t for t in result if t["abbreviation"] == "DAL")
        assert dal_team["total_payroll"] == 43000000
        assert dal_team["roster_count"] == 1
        assert dal_team["roster_with_salary"] == 1
        assert dal_team["top_paid_player"] == "Luka Doncic"  # ASCII in output
        assert dal_team["top_paid_salary"] == 43000000


class TestHandlerValidationCheck:
    """Test handler validation gate logic."""

    @patch("src.lambdas.etl.transform_data.ENVIRONMENT", "test")
    @patch("src.lambdas.etl.transform_data.S3_BUCKET", "test-bucket")
    def test_handler_skips_transformation_when_validation_fails(self):
        """Test handler returns 400 when validation_passed is false."""
        event = {
            "validation_passed": False,
            "data_location": {"bucket": "test", "partition": "year=2024/month=02/day=17"},
        }

        result = transform_data.handler(event, MagicMock())

        assert result["statusCode"] == 400


class TestHandlerNaNValidation:
    """Test handler NaN validation logic."""

    @patch("src.lambdas.etl.transform_data.ENVIRONMENT", "test")
    @patch("src.lambdas.etl.transform_data.S3_BUCKET", "test-bucket")
    @patch("src.lambdas.etl.transform_data.save_to_s3")
    @patch("src.lambdas.etl.transform_data.enrich_player_stats")
    @patch("src.lambdas.etl.transform_data.load_from_s3")
    def test_handler_returns_400_when_nan_in_statistics(
        self, mock_load, mock_enrich_player_stats, mock_save
    ):
        """Test handler returns 400 when NaN values detected in output statistics."""

        # Mock enrichment to return stats with NaN that would cause NaN in aggregates
        mock_enrich_player_stats.return_value = [
            {
                "player_name": "Player 1",
                "points": float("nan"),
                "rebounds": 5.0,
                "assists": 3.0,
            }
        ]

        def mock_load_side_effect(s3_key):
            if "stats" in s3_key:
                return {
                    "season": "2025-26",
                    "source": "basketball_reference",
                    "per_game_stats": [{"Player": "Test"}],
                    "advanced_stats": [],
                }
            return None

        mock_load.side_effect = mock_load_side_effect
        mock_save.return_value = True

        event = {
            "validation_passed": True,
            "data_location": {"bucket": "test", "partition": "year=2024/month=02/day=17"},
        }

        result = transform_data.handler(event, MagicMock())

        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert body["error"] == "Invalid statistics"
        assert "NaN values detected" in body["message"]


class TestHandlerTransformation:
    """Test handler transformation workflow."""

    @patch("src.lambdas.etl.transform_data.ENVIRONMENT", "test")
    @patch("src.lambdas.etl.transform_data.S3_BUCKET", "test-bucket")
    @patch("src.lambdas.etl.transform_data.save_to_s3")
    @patch("src.lambdas.etl.transform_data.load_from_s3")
    def test_handler_successful_transformation(self, mock_load, mock_save):
        """Test handler successfully transforms Basketball Reference data."""

        def mock_load_side_effect(s3_key):
            if "players" in s3_key:
                return {
                    "players": [
                        {"id": 2544, "full_name": "LeBron James"},
                    ]
                }
            elif "stats" in s3_key:
                return {
                    "season": "2025-26",
                    "source": "basketball_reference",
                    "per_game_stats": [
                        {
                            "Player": "LeBron James",
                            "Team": "LAL",
                            "PTS": 25.8,
                            "TRB": 7.7,
                            "AST": 8.2,
                        }
                    ],
                    "advanced_stats": [
                        {
                            "Player": "LeBron James",
                            "PER": 24.5,
                            "TS%": 0.623,
                            "VORP": 4.2,
                        }
                    ],
                }
            elif "salaries" in s3_key:
                return {
                    "fetch_timestamp": "2024-02-17T00:00:00",
                    "source": "espn",
                    "salaries": [
                        {
                            "player_name": "LeBron James",
                            "annual_salary": 47607350,
                            "season": "2025-26",
                            "source": "espn",
                        }
                    ],
                }
            elif "teams" in s3_key:
                return {
                    "teams": [
                        {
                            "id": 1610612747,
                            "full_name": "Los Angeles Lakers",
                            "abbreviation": "LAL",
                        }
                    ]
                }
            return None

        mock_load.side_effect = mock_load_side_effect
        mock_save.return_value = True

        event = {
            "validation_passed": True,
            "data_location": {"bucket": "test", "partition": "year=2024/month=02/day=17"},
        }

        result = transform_data.handler(event, MagicMock())

        assert result["statusCode"] == 200
        assert result["transformation_successful"] is True
        body = json.loads(result["body"])
        assert "enriched_salaries" in body["transformed"]
        assert "enriched_player_stats" in body["transformed"]
        assert "enriched_teams" in body["transformed"]


class TestNormalizeSeasonFormat:
    """Test season format normalization."""

    def test_normalize_season_format_converts_short_to_long(self):
        """Test converting 2025-26 to 2025-2026."""
        result = transform_data._normalize_season_format("2025-26")
        assert result == "2025-2026"

    def test_normalize_season_format_handles_already_normalized(self):
        """Test that already normalized season is unchanged."""
        result = transform_data._normalize_season_format("2025-2026")
        assert result == "2025-2026"

    def test_normalize_season_format_handles_no_dash(self):
        """Test handling of season without dash."""
        result = transform_data._normalize_season_format("2025")
        assert result == "2025"

    def test_normalize_season_format_handles_different_years(self):
        """Test various year formats."""
        assert transform_data._normalize_season_format("2024-25") == "2024-2025"
        assert transform_data._normalize_season_format("2023-24") == "2023-2024"
        assert transform_data._normalize_season_format("2026-27") == "2026-2027"


class TestTransformSalaryCapHistory:
    """Test transformation of salary cap history data from RealGM."""

    def test_transform_salary_cap_history_success(self):
        """Test successful transformation of salary cap data."""
        cap_data = {
            "source": "realgm",
            "salary_cap_history": [
                {
                    "Season": "2025-2026",
                    "Salary Cap": "$154,647,000",
                    "Luxury Tax": "$187,895,000",
                    "1st Apron": "$178,655,000",
                    "2nd Apron": "$189,495,000",
                    "BAE": "$5,168,000",
                    "Non-Taxpayer MLE": "$13,040,000",
                    "Taxpayer MLE": "$5,685,000",
                    "Team Room MLE": "$8,781,000",
                },
                {
                    "Season": "2024-2025",
                    "Salary Cap": "$140,588,000",
                    "Luxury Tax": "$170,814,000",
                    "1st Apron": "$172,346,000",
                    "2nd Apron": "$182,794,000",
                    "BAE": "$4,700,000",
                    "Non-Taxpayer MLE": "$12,405,000",
                    "Taxpayer MLE": "$5,183,000",
                    "Team Room MLE": "$7,981,000",
                },
            ],
        }

        result = transform_data.transform_salary_cap_history(cap_data)

        assert len(result) == 2
        assert result[0]["season"] == "2025-26"
        assert result[0]["salary_cap"] == 154647000
        assert result[0]["luxury_tax"] == 187895000
        assert result[0]["first_apron"] == 178655000
        assert result[0]["second_apron"] == 189495000
        assert result[0]["bae"] == 5168000
        assert result[0]["non_taxpayer_mle"] == 13040000
        assert result[0]["taxpayer_mle"] == 5685000
        assert result[0]["team_room_mle"] == 8781000

    def test_transform_salary_cap_history_handles_empty_values(self):
        """Test handling of None and empty values."""
        cap_data = {
            "salary_cap_history": [
                {
                    "Season": "2025-2026",
                    "Salary Cap": "$154,647,000",
                    "Luxury Tax": None,
                    "1st Apron": "",
                    "2nd Apron": "$189,495,000",
                }
            ]
        }

        result = transform_data.transform_salary_cap_history(cap_data)

        assert len(result) == 1
        assert result[0]["season"] == "2025-26"
        assert result[0]["salary_cap"] == 154647000
        assert result[0]["luxury_tax"] is None
        assert result[0]["first_apron"] is None
        assert result[0]["second_apron"] == 189495000

    def test_transform_salary_cap_history_handles_empty_data(self):
        """Test handling when no salary cap history is provided."""
        cap_data = {"salary_cap_history": []}

        result = transform_data.transform_salary_cap_history(cap_data)

        assert result == []

    def test_transform_salary_cap_history_skips_invalid_records(self):
        """Test that records without a season are skipped."""
        cap_data = {
            "salary_cap_history": [
                {"Season": "2025-2026", "Salary Cap": "$154,647,000"},
                {"Salary Cap": "$140,588,000"},  # Missing Season
                {"Season": "2023-2024", "Salary Cap": "$136,021,000"},
            ]
        }

        result = transform_data.transform_salary_cap_history(cap_data)

        assert len(result) == 2
        assert result[0]["season"] == "2025-26"
        assert result[1]["season"] == "2023-24"

    def test_transform_salary_cap_history_filters_by_season(self):
        """Test filtering by specific season."""
        cap_data = {
            "salary_cap_history": [
                {"Season": "2025-2026", "Salary Cap": "$154,647,000"},
                {"Season": "2024-2025", "Salary Cap": "$140,588,000"},
                {"Season": "2023-2024", "Salary Cap": "$136,021,000"},
            ]
        }

        # Filter for 2025-26 (should match 2025-2026 in data)
        result = transform_data.transform_salary_cap_history(cap_data, season="2025-26")

        assert len(result) == 1
        assert result[0]["season"] == "2025-26"
        assert result[0]["salary_cap"] == 154647000

    def test_transform_salary_cap_history_no_match_returns_empty(self):
        """Test that filtering with no match returns empty list."""
        cap_data = {
            "salary_cap_history": [
                {"Season": "2025-2026", "Salary Cap": "$154,647,000"},
                {"Season": "2024-2025", "Salary Cap": "$140,588,000"},
            ]
        }

        result = transform_data.transform_salary_cap_history(cap_data, season="2022-23")

        assert result == []


class TestTransformContractLimits:
    """Test transformation of contract limits data from RealGM."""

    def test_transform_contract_limits_success(self):
        """Test successful transformation of contract limits."""
        cap_data = {
            "contract_limits": [
                {
                    "Season": "2025-2026",
                    "0-6 YOS Max": "$38,661,750",
                    "7-9 YOS Max": "$46,394,100",
                    "10+ YOS Max": "$54,126,450",
                    "0 YOS Min": "$1,157,153",
                    "1 YOS Min": "$1,862,265",
                    "2 YOS Min": "$2,296,274",
                    "10+ YOS Min": "$3,634,153",
                }
            ]
        }

        result = transform_data.transform_contract_limits(cap_data)

        assert len(result) == 1
        assert result[0]["season"] == "2025-26"
        assert result[0]["max_0_6_yos"] == 38661750
        assert result[0]["max_7_9_yos"] == 46394100
        assert result[0]["max_10_plus_yos"] == 54126450
        assert result[0]["min_0_yos"] == 1157153
        assert result[0]["min_1_yos"] == 1862265
        assert result[0]["min_2_yos"] == 2296274
        assert result[0]["min_10_plus_yos"] == 3634153

    def test_transform_contract_limits_handles_empty_data(self):
        """Test handling when no contract limits are provided."""
        cap_data = {"contract_limits": []}

        result = transform_data.transform_contract_limits(cap_data)

        assert result == []

    def test_transform_contract_limits_handles_missing_values(self):
        """Test handling of missing values in contract limits."""
        cap_data = {
            "contract_limits": [
                {
                    "Season": "2025-2026",
                    "0-6 YOS Max": "$38,661,750",
                    "7-9 YOS Max": None,
                    "10+ YOS Max": "",
                }
            ]
        }

        result = transform_data.transform_contract_limits(cap_data)

        assert len(result) == 1
        assert result[0]["season"] == "2025-26"
        assert result[0]["max_0_6_yos"] == 38661750
        assert result[0]["max_7_9_yos"] is None
        assert result[0]["max_10_plus_yos"] is None

    def test_transform_contract_limits_filters_by_season(self):
        """Test filtering contract limits by specific season."""
        cap_data = {
            "contract_limits": [
                {
                    "Season": "2025-2026",
                    "0-6 YOS Max": "$38,661,750",
                    "7-9 YOS Max": "$46,394,100",
                    "10+ YOS Max": "$54,126,450",
                },
                {
                    "Season": "2024-2025",
                    "0-6 YOS Max": "$35,147,500",
                    "7-9 YOS Max": "$42,177,000",
                    "10+ YOS Max": "$49,206,500",
                },
            ]
        }

        # Filter for 2025-26 (should match 2025-2026 in data)
        result = transform_data.transform_contract_limits(cap_data, season="2025-26")

        assert len(result) == 1
        assert result[0]["season"] == "2025-26"
        assert result[0]["max_0_6_yos"] == 38661750

    def test_transform_contract_limits_no_match_returns_empty(self):
        """Test that filtering with no match returns empty list."""
        cap_data = {
            "contract_limits": [
                {
                    "Season": "2025-2026",
                    "0-6 YOS Max": "$38,661,750",
                },
                {
                    "Season": "2024-2025",
                    "0-6 YOS Max": "$35,147,500",
                },
            ]
        }

        result = transform_data.transform_contract_limits(cap_data, season="2022-23")

        assert result == []


class TestBadRowFiltering:
    """Test bad row filtering in transform_data."""

    def test_filter_bad_rows_removes_rows_with_null_critical_columns(self):
        """Test that rows with null critical columns are filtered out."""
        stats = [
            {"Player": "Good Player", "Pos": "PG", "Age": 25, "Team": "LAL", "PTS": 20.0},
            {"Player": None, "Pos": "SG", "Age": 28, "Team": "GSW", "PTS": 15.0},
            {"Player": "Another Good", "Pos": "SF", "Age": 30, "Team": "BOS", "PTS": 18.0},
        ]

        result = transform_data._filter_bad_rows(stats, "per_game")

        assert len(result) == 2
        assert result[0]["Player"] == "Good Player"
        assert result[1]["Player"] == "Another Good"

    def test_filter_bad_rows_removes_rows_with_null_stats(self):
        """Test that rows with null stats (non-critical columns) are filtered out."""
        stats = [
            {
                "Player": "Good Player",
                "Pos": "PG",
                "Age": 25,
                "Team": "LAL",
                "PTS": 20.0,
                "AST": 5.0,
            },
            {
                "Player": "Bad Player",
                "Pos": "SG",
                "Age": 28,
                "Team": "GSW",
                "PTS": None,
                "AST": 3.0,
            },
        ]

        result = transform_data._filter_bad_rows(stats, "per_game")

        assert len(result) == 1
        assert result[0]["Player"] == "Good Player"

    def test_filter_bad_rows_allows_null_percentage_when_attempts_zero(self):
        """Test that null percentages are allowed when attempts = 0."""
        stats = [
            {
                "Player": "Player 1",
                "Pos": "PG",
                "Age": 25,
                "Team": "LAL",
                "FG%": None,
                "FGA": 0,
            },
            {
                "Player": "Player 2",
                "Pos": "SG",
                "Age": 28,
                "Team": "GSW",
                "3P%": None,
                "3PA": 0,
            },
        ]

        result = transform_data._filter_bad_rows(stats, "per_game")

        assert len(result) == 2

    def test_filter_bad_rows_skips_warn_only_columns(self):
        """Test that warn_only_columns don't cause rows to be filtered."""
        stats = [
            {
                "Player": "Player 1",
                "Pos": "PG",
                "Age": 25,
                "Team": "LAL",
                "TOV%": None,
                "TS%": None,
            }
        ]

        result = transform_data._filter_bad_rows(stats, "advanced")

        assert len(result) == 1

    def test_filter_bad_rows_filters_league_average(self):
        """Test that League Average rows are filtered out."""
        stats = [
            {"Player": "LeBron James", "Pos": "PF", "Age": 39, "Team": "LAL", "PTS": 25.0},
            {"Player": "League Average", "Pos": None, "Age": None, "Team": None, "PTS": 18.5},
        ]

        result = transform_data._filter_bad_rows(stats, "per_game")

        assert len(result) == 1
        assert result[0]["Player"] == "LeBron James"

    def test_enrich_player_stats_applies_bad_row_filtering(self):
        """Test that enrich_player_stats filters out bad rows."""
        stats_data = {
            "season": "2025-26",
            "source": "basketball_reference",
            "per_game_stats": [
                {
                    "Player": "Good Player",
                    "Team": "LAL",
                    "Pos": "PG",
                    "Age": 25,
                    "G": 50,
                    "PTS": 25.8,
                },
                {
                    "Player": None,
                    "Team": "GSW",
                    "Pos": "SG",
                    "Age": 28,
                    "G": 45,
                    "PTS": 20.0,
                },
            ],
            "advanced_stats": [
                {
                    "Player": "Good Player",
                    "Team": "LAL",
                    "Pos": "PG",
                    "Age": 25,
                    "G": 50,
                    "PER": 28.5,
                }
            ],
        }

        result = transform_data.enrich_player_stats(stats_data)

        assert len(result) == 1
        assert result[0]["player_name"] == "Good Player"
