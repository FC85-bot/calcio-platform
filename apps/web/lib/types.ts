export type UUID = string;

export type CompetitionSummary = {
  id: UUID;
  name: string;
  country: string;
};

export type SeasonSummary = {
  id: UUID;
  name: string;
  start_date: string;
  end_date: string;
};

export type TeamSummary = {
  id: UUID;
  name: string;
};

export type MatchScore = {
  home: number | null;
  away: number | null;
};

export type Competition = CompetitionSummary & {
  created_at: string;
};

export type Season = SeasonSummary & {
  created_at: string;
};

export type LatestOdds = {
  provider_id: UUID;
  provider_name: string;
  bookmaker_id: UUID;
  bookmaker_name: string;
  snapshot_timestamp: string;
  home_win: number | null;
  draw: number | null;
  away_win: number | null;
  over_2_5: number | null;
  under_2_5: number | null;
  btts_yes: number | null;
  btts_no: number | null;
};

export type OddsSnapshotRow = {
  id: UUID;
  match_id: UUID;
  provider_id: UUID;
  provider_name: string;
  bookmaker_id: UUID;
  bookmaker_name: string;
  market_id: UUID;
  market_code: string;
  market_name: string;
  selection_code: string;
  line_value: number | null;
  odds_value: number;
  snapshot_timestamp: string;
  ingested_at: string;
};

export type OddsBestRow = OddsSnapshotRow;

export type OddsAdminSummary = {
  total_snapshot_rows: number;
  matches_with_odds_count: number;
  bookmakers_count: number;
  markets_count: number;
  latest_snapshot_timestamp: string | null;
};

export type OddsAdminQuality = {
  duplicate_snapshot_count: number;
  invalid_odds_value_count: number;
  invalid_line_value_count: number;
  missing_bookmaker_count: number;
  missing_market_count: number;
  missing_snapshot_timestamp_count: number;
};

export type Match = {
  id: UUID;
  competition_id: UUID;
  season_id: UUID | null;
  season: string;
  match_date: string;
  home_team_id: UUID;
  away_team_id: UUID;
  home_goals: number | null;
  away_goals: number | null;
  status: string;
  created_at: string | null;
  competition: CompetitionSummary;
  season_detail: SeasonSummary | null;
  home_team: TeamSummary;
  away_team: TeamSummary;
  score: MatchScore | null;
  latest_odds: LatestOdds[] | null;
};

export type StandingRow = {
  position: number;
  team: TeamSummary;
  points: number;
  played: number;
  won: number;
  drawn: number;
  lost: number;
  goals_for: number;
  goals_against: number;
  goal_difference: number;
};

export type CompetitionStandings = {
  competition: CompetitionSummary;
  season: SeasonSummary | null;
  season_name: string | null;
  source: string;
  snapshot_date: string | null;
  standings: StandingRow[];
};

export type TeamVenueStats = {
  matches: number;
  wins: number;
  draws: number;
  losses: number;
  goals_scored: number;
  goals_conceded: number;
};

export type TeamStats = {
  team_id: UUID;
  competition_id: UUID;
  season: string;
  matches_played: number;
  wins: number;
  draws: number;
  losses: number;
  goals_scored: number;
  goals_conceded: number;
  avg_goals_scored: number;
  avg_goals_conceded: number;
  home: TeamVenueStats;
  away: TeamVenueStats;
};

export type TeamForm = {
  team_id: UUID;
  competition_id: UUID;
  season: string;
  last_n: number;
  results: string[];
};

export type TeamStreak = {
  team_id: UUID;
  competition_id: UUID;
  season: string;
  current_streak_type: string | null;
  current_streak_length: number;
};

export type TeamListItem = {
  id: UUID;
  name: string;
  competition_id: UUID;
  created_at: string;
  competition: CompetitionSummary;
};

export type TeamDetail = {
  id: UUID;
  name: string;
  competition_id: UUID;
  created_at: string;
  competition: CompetitionSummary;
  season_id: UUID | null;
  season: string | null;
  season_detail: SeasonSummary | null;
  stats: TeamStats | null;
  form: TeamForm | null;
  streak: TeamStreak | null;
};

export type IngestionRun = {
  id: UUID;
  provider: string | null;
  run_type: string;
  entity_type: string;
  started_at: string;
  finished_at: string | null;
  status: string;
  row_count: number;
  raw_record_count: number;
  created_count: number;
  updated_count: number;
  skipped_count: number;
  error_count: number;
  error_message: string | null;
};

export type RawIngestionRecord = {
  id: UUID;
  run_id: UUID | null;
  normalization_run_id: UUID | null;
  provider: string;
  entity_type: string;
  endpoint: string;
  raw_path: string | null;
  payload_sha256: string | null;
  payload_size_bytes: number | null;
  request_params: Record<string, unknown> | null;
  response_metadata: Record<string, unknown> | null;
  payload: Record<string, unknown>;
  normalization_status: string;
  normalized_at: string | null;
  normalization_error: string | null;
  ingested_at: string;
};

export type Health = {
  status: string;
  service: string;
  environment: string;
  database: string;
  timestamp: string;
};


export type MatchReportContext = {
  match_id: UUID;
  competition: CompetitionSummary;
  season: SeasonSummary | null;
  season_label: string | null;
  match_date: string;
  home_team: TeamSummary;
  away_team: TeamSummary;
  status: string;
  score: MatchScore | null;
};

export type TeamReportBlock = {
  team: TeamSummary;
  last_results: string[];
  stats: TeamStats | null;
  streak: TeamStreak | null;
  venue_split_label: string | null;
  venue_split: TeamVenueStats | null;
};

export type StandingsReportBlock = {
  position: number;
  points: number;
  goal_difference: number;
  played: number;
};

export type StandingsContext = {
  available: boolean;
  source: string | null;
  snapshot_date: string | null;
  home_team: StandingsReportBlock | null;
  away_team: StandingsReportBlock | null;
};

export type OddsReportBlock = {
  market_code: string;
  available: boolean;
  latest_snapshot_timestamp: string | null;
  latest: OddsSnapshotRow[];
  best: OddsBestRow[];
  opening: OddsSnapshotRow[];
};

export type PredictionSelection = {
  id: UUID | null;
  selection_code: string;
  predicted_probability: number;
  fair_odds: number;
  market_best_odds: number | null;
  edge_pct: number | null;
  confidence_score: number | null;
  created_at: string | null;
};

export type PredictionReportBlock = {
  market_code: string;
  available: boolean;
  prediction_id: UUID | null;
  feature_snapshot_id: UUID | null;
  feature_set_version: string | null;
  model_version_id: UUID | null;
  model_version: string | null;
  model_code: string | null;
  model_name: string | null;
  prediction_horizon: string | null;
  as_of_ts: string | null;
  data_quality_score: number | null;
  selections: PredictionSelection[];
};

export type WarningRow = {
  code: string;
  section: string;
  severity: string;
  detail: string | null;
};

export type MatchReport = {
  context: MatchReportContext;
  home_team: TeamReportBlock;
  away_team: TeamReportBlock;
  standings_context: StandingsContext;
  odds: OddsReportBlock[];
  predictions: PredictionReportBlock[];
  warnings: WarningRow[];
  generated_at: string;
  report_version: string;
  feature_set_version: string | null;
};
