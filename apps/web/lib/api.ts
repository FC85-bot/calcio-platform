import { fetchJson, type ApiResult } from "./fetcher";
import type {
  Competition,
  CompetitionStandings,
  Health,
  IngestionRun,
  Match,
  MatchReport,
  OddsAdminQuality,
  OddsAdminSummary,
  OddsBestRow,
  OddsSnapshotRow,
  RawIngestionRecord,
  Season,
  TeamDetail,
  TeamListItem,
  UUID,
} from "./types";

export type ListMatchesParams = {
  competition_id?: UUID;
  season_id?: UUID;
  season?: string;
  date_from?: string;
  date_to?: string;
  team_id?: UUID;
  status?: string;
  limit?: number;
  offset?: number;
  include_latest_odds?: boolean;
};

export type ListCompetitionsParams = {
  season_id?: UUID;
  season?: string;
  limit?: number;
  offset?: number;
};

export type ListSeasonsParams = {
  competition_id?: UUID;
  limit?: number;
  offset?: number;
};

export type ListTeamsParams = {
  competition_id?: UUID;
  search?: string;
  limit?: number;
  offset?: number;
};

export type ListIngestionRunsParams = {
  limit?: number;
  offset?: number;
  provider?: string;
  status?: string;
  run_type?: string;
  entity_type?: string;
};

export type ListRawIngestionParams = {
  limit?: number;
  provider?: string;
  entity_type?: string;
  normalization_status?: string;
};

function toQueryString(params: Record<string, string | number | boolean | undefined | null>): string {
  const searchParams = new URLSearchParams();

  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") {
      return;
    }

    searchParams.set(key, String(value));
  });

  const query = searchParams.toString();
  return query ? `?${query}` : "";
}

export function getHealth(): Promise<ApiResult<Health>> {
  return fetchJson<Health>("/health");
}

export function listCompetitions(params: ListCompetitionsParams = {}): Promise<ApiResult<Competition[]>> {
  return fetchJson<Competition[]>(
    `/competitions${toQueryString({
      season_id: params.season_id,
      season: params.season,
      limit: params.limit ?? 100,
      offset: params.offset ?? 0,
    })}`,
  );
}

export function getCompetitionStandings(
  competitionId: UUID,
  params: { season_id?: UUID; season?: string } = {},
): Promise<ApiResult<CompetitionStandings>> {
  return fetchJson<CompetitionStandings>(
    `/competitions/${competitionId}/standings${toQueryString({
      season_id: params.season_id,
      season: params.season,
    })}`,
  );
}

export function listSeasons(params: ListSeasonsParams = {}): Promise<ApiResult<Season[]>> {
  return fetchJson<Season[]>(
    `/seasons${toQueryString({
      competition_id: params.competition_id,
      limit: params.limit ?? 100,
      offset: params.offset ?? 0,
    })}`,
  );
}

export function listMatches(params: ListMatchesParams = {}): Promise<ApiResult<Match[]>> {
  return fetchJson<Match[]>(
    `/matches${toQueryString({
      competition_id: params.competition_id,
      season_id: params.season_id,
      season: params.season,
      date_from: params.date_from,
      date_to: params.date_to,
      team_id: params.team_id,
      status: params.status,
      limit: params.limit ?? 50,
      offset: params.offset ?? 0,
      include_latest_odds: params.include_latest_odds ?? false,
    })}`,
  );
}


export function getMatchReport(
  matchId: UUID,
  params: { prediction_horizon?: string; form_last_n?: number } = {},
): Promise<ApiResult<MatchReport>> {
  return fetchJson<MatchReport>(
    `/matches/${matchId}/report${toQueryString({
      prediction_horizon: params.prediction_horizon ?? "pre_match",
      form_last_n: params.form_last_n ?? 5,
    })}`,
  );
}

export function getMatch(matchId: UUID, includeLatestOdds = true): Promise<ApiResult<Match>> {
  return fetchJson<Match>(
    `/matches/${matchId}${toQueryString({ include_latest_odds: includeLatestOdds })}`,
  );
}

export function getMatchOddsLatest(
  matchId: UUID,
  params: { market_code?: string } = {},
): Promise<ApiResult<OddsSnapshotRow[]>> {
  return fetchJson<OddsSnapshotRow[]>(
    `/matches/${matchId}/odds/latest${toQueryString({ market_code: params.market_code })}`,
  );
}

export function getMatchOddsHistory(
  matchId: UUID,
  params: { market_code?: string; bookmaker_id?: UUID; selection_code?: string; limit?: number } = {},
): Promise<ApiResult<OddsSnapshotRow[]>> {
  return fetchJson<OddsSnapshotRow[]>(
    `/matches/${matchId}/odds/history${toQueryString({
      market_code: params.market_code,
      bookmaker_id: params.bookmaker_id,
      selection_code: params.selection_code,
      limit: params.limit ?? 200,
    })}`,
  );
}

export function getMatchOddsBest(
  matchId: UUID,
  params: { market_code?: string } = {},
): Promise<ApiResult<OddsBestRow[]>> {
  return fetchJson<OddsBestRow[]>(
    `/matches/${matchId}/odds/best${toQueryString({ market_code: params.market_code })}`,
  );
}

export function getMatchOddsOpening(
  matchId: UUID,
  params: { market_code?: string } = {},
): Promise<ApiResult<OddsSnapshotRow[]>> {
  return fetchJson<OddsSnapshotRow[]>(
    `/matches/${matchId}/odds/opening${toQueryString({ market_code: params.market_code })}`,
  );
}

export function listTeams(params: ListTeamsParams = {}): Promise<ApiResult<TeamListItem[]>> {
  return fetchJson<TeamListItem[]>(
    `/teams${toQueryString({
      competition_id: params.competition_id,
      search: params.search,
      limit: params.limit ?? 200,
      offset: params.offset ?? 0,
    })}`,
  );
}

export function getTeam(
  teamId: UUID,
  params: { season_id?: UUID; season?: string; form_last_n?: number } = {},
): Promise<ApiResult<TeamDetail>> {
  return fetchJson<TeamDetail>(
    `/teams/${teamId}${toQueryString({
      season_id: params.season_id,
      season: params.season,
      form_last_n: params.form_last_n ?? 5,
    })}`,
  );
}

export function listIngestionRuns(
  params: ListIngestionRunsParams = {},
): Promise<ApiResult<IngestionRun[]>> {
  return fetchJson<IngestionRun[]>(
    `/admin/ingestion-runs${toQueryString({
      limit: params.limit ?? 50,
      offset: params.offset ?? 0,
      provider: params.provider,
      status: params.status,
      run_type: params.run_type,
      entity_type: params.entity_type,
    })}`,
  );
}

export function listRawIngestion(
  params: ListRawIngestionParams = {},
): Promise<ApiResult<RawIngestionRecord[]>> {
  return fetchJson<RawIngestionRecord[]>(
    `/admin/raw-ingestion${toQueryString({
      limit: params.limit ?? 20,
      provider: params.provider,
      entity_type: params.entity_type,
      normalization_status: params.normalization_status,
    })}`,
  );
}

export function getOddsAdminSummary(): Promise<ApiResult<OddsAdminSummary>> {
  return fetchJson<OddsAdminSummary>("/admin/odds/summary");
}

export function getOddsAdminQuality(): Promise<ApiResult<OddsAdminQuality>> {
  return fetchJson<OddsAdminQuality>("/admin/odds/quality");
}
