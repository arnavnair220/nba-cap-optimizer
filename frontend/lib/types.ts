export interface PlayerPrediction {
  player_name: string;
  season: string;
  predicted_fmv: number;
  actual_salary: number;
  predicted_salary_cap_pct: number;
  actual_salary_cap_pct: number;
  predicted_salary_pct_of_max?: number;
  inefficiency_score: number;
  value_category: 'Bargain' | 'Fair' | 'Overpaid';
  vorp: number;
  model_version?: string;
  prediction_date?: string;
  rank?: number;
  previous_rank?: number | null;
  rank_change?: number | null;
  team_abbreviation: string;
  position: string;
  age: number;
  games_played?: number;
  games_started?: number;
  minutes?: number;
  points: number;
  rebounds: number;
  assists: number;
  steals?: number;
  blocks?: number;
  fg_pct?: number;
  fg3_pct?: number;
  ft_pct?: number;
  per?: number;
  ts_pct?: number;
  usg_pct?: number;
  ws?: number;
  bpm?: number;
}

export interface PredictionsResponse {
  predictions: PlayerPrediction[];
  count: number;
}

export interface TeamEfficiency {
  team_abbreviation: string;
  full_name: string;
  player_count: number;
  total_payroll: number;
  avg_inefficiency_score: number;
  total_overspend: number;
  total_underspend: number;
  net_efficiency: number;
  bargain_count: number;
  fair_count: number;
  overpaid_count: number;
}

export interface TeamDetail extends TeamEfficiency {
  players: PlayerPrediction[];
}

export interface TeamsResponse {
  teams: TeamEfficiency[];
  count: number;
}

export interface PredictionsQueryParams {
  value_category?: 'Bargain' | 'Fair' | 'Overpaid';
  team?: string;
  position?: string;
  sort_by?: 'inefficiency_score' | 'predicted_fmv' | 'actual_salary' | 'vorp' | 'player_name';
  limit?: number;
  offset?: number;
}

export interface TeamsQueryParams {
  sort_by?: 'avg_inefficiency' | 'net_efficiency' | 'bargain_count' | 'overpaid_count';
}
