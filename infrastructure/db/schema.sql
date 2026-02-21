-- NBA Cap Optimizer Database Schema
-- PostgreSQL schema for storing NBA player stats, salaries, and team data

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

    -- Basic info
    age INTEGER,
    position VARCHAR(10),
    games_played INTEGER,
    games_started INTEGER,
    minutes REAL,

    -- Shooting - Per Game
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

    -- Rebounds
    oreb REAL,
    dreb REAL,
    rebounds REAL,

    -- Other stats
    assists REAL,
    steals REAL,
    blocks REAL,
    turnovers REAL,
    fouls REAL,

    -- Advanced metrics
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

    -- Salary metrics
    total_payroll BIGINT,
    roster_count INTEGER,
    roster_with_salary INTEGER,
    avg_salary REAL,
    min_salary INTEGER,
    max_salary INTEGER,
    top_paid_player VARCHAR(255),
    top_paid_salary INTEGER,

    -- Performance metrics
    total_players_with_stats INTEGER,
    team_total_points REAL,
    team_total_rebounds REAL,
    team_total_assists REAL,
    avg_player_points REAL,
    avg_player_rebounds REAL,
    avg_player_assists REAL
);

CREATE INDEX IF NOT EXISTS idx_teams_abbreviation ON teams(abbreviation);
