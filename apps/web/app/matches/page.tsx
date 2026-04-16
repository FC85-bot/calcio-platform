import Link from "next/link";
import { Card } from "@/components/card";
import { PageHeader } from "@/components/page-header";
import { EmptyState, ErrorState } from "@/components/state-block";
import { StatusPill } from "@/components/status-pill";
import { formatDateTime, formatNumber } from "@/lib/format";
import { listCompetitions, listMatches, listSeasons, listTeams } from "@/lib/api";
import type { LatestOdds } from "@/lib/types";

type SearchParams = Promise<Record<string, string | string[] | undefined>>;

function firstValue(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

function renderOddsSummary(latestOdds: LatestOdds[] | null): string {
  if (!latestOdds || latestOdds.length === 0) {
    return "—";
  }

  const first = latestOdds[0];
  const base = `${first.provider_name}: 1 ${formatNumber(first.home_win, 2)} · X ${formatNumber(first.draw, 2)} · 2 ${formatNumber(first.away_win, 2)}`;
  if (latestOdds.length === 1) {
    return base;
  }
  return `${base} (+${latestOdds.length - 1})`;
}

export default async function MatchesPage({ searchParams }: { searchParams: SearchParams }) {
  const resolvedSearchParams = await searchParams;
  const competitionId = firstValue(resolvedSearchParams.competition_id);
  const seasonId = firstValue(resolvedSearchParams.season_id);
  const season = firstValue(resolvedSearchParams.season);
  const teamId = firstValue(resolvedSearchParams.team_id);
  const status = firstValue(resolvedSearchParams.status);
  const dateFrom = firstValue(resolvedSearchParams.date_from);
  const dateTo = firstValue(resolvedSearchParams.date_to);

  const [competitionsResult, seasonsResult, teamsResult, matchesResult] = await Promise.all([
    listCompetitions({ limit: 200 }),
    listSeasons({ competition_id: competitionId, limit: 200 }),
    listTeams({ competition_id: competitionId, limit: 500 }),
    listMatches({
      competition_id: competitionId,
      season_id: seasonId,
      season,
      date_from: dateFrom,
      date_to: dateTo,
      team_id: teamId,
      status,
      limit: 100,
      include_latest_odds: true,
    }),
  ]);

  return (
    <div className="stack-lg">
      <PageHeader
        title="Matches"
        description="Lista match filtrabile collegata agli endpoint reali, con dettaglio e latest odds sintetiche."
      />

      <Card title="Filtri match">
        <form className="filters-grid filters-grid-wide" method="GET">
          <label>
            <span>Competition</span>
            <select name="competition_id" defaultValue={competitionId ?? ""}>
              <option value="">Tutte</option>
              {competitionsResult.data?.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.name}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>Season</span>
            <select name="season" defaultValue={season ?? ""}>
              <option value="">Tutte</option>
              {seasonsResult.data?.map((item) => (
                <option key={item.id} value={item.name}>
                  {item.name}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>Team</span>
            <select name="team_id" defaultValue={teamId ?? ""}>
              <option value="">Tutte</option>
              {teamsResult.data?.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.name}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>Status</span>
            <select name="status" defaultValue={status ?? ""}>
              <option value="">Tutti</option>
              {["scheduled", "live", "finished", "postponed", "cancelled"].map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>Data da</span>
            <input type="datetime-local" name="date_from" defaultValue={dateFrom ?? ""} />
          </label>
          <label>
            <span>Data a</span>
            <input type="datetime-local" name="date_to" defaultValue={dateTo ?? ""} />
          </label>
          <div className="filters-actions">
            <button type="submit">Applica filtri</button>
            <Link href="/matches" className="secondary-button">
              Reset
            </Link>
          </div>
        </form>
      </Card>

      <Card title="Lista match" description="Campi minimi MVP: data, competition, squadre, status, score e odds latest sintetiche.">
        {matchesResult.error ? (
          <ErrorState title="Errore caricamento matches" message={matchesResult.error} />
        ) : matchesResult.data && matchesResult.data.length > 0 ? (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Data match</th>
                  <th>Competition</th>
                  <th>Home</th>
                  <th>Away</th>
                  <th>Status</th>
                  <th>Score</th>
                  <th>Latest odds</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {matchesResult.data.map((match) => (
                  <tr key={match.id}>
                    <td>{formatDateTime(match.match_date)}</td>
                    <td>{match.competition.name}</td>
                    <td>
                      <Link
                        href={
                          match.season
                            ? `/teams/${match.home_team.id}?season=${encodeURIComponent(match.season)}`
                            : `/teams/${match.home_team.id}`
                        }
                        className="text-link"
                      >
                        {match.home_team.name}
                      </Link>
                    </td>
                    <td>
                      <Link
                        href={
                          match.season
                            ? `/teams/${match.away_team.id}?season=${encodeURIComponent(match.season)}`
                            : `/teams/${match.away_team.id}`
                        }
                        className="text-link"
                      >
                        {match.away_team.name}
                      </Link>
                    </td>
                    <td>
                      <StatusPill value={match.status} />
                    </td>
                    <td>
                      {match.status === "finished" && match.score
                        ? `${match.score.home ?? 0} - ${match.score.away ?? 0}`
                        : "—"}
                    </td>
                    <td>{renderOddsSummary(match.latest_odds)}</td>
                    <td>
                      <Link href={`/matches/${match.id}`} className="text-link">
                        Apri
                      </Link>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <EmptyState
            title="Nessun match trovato"
            message="Nessun risultato per i filtri selezionati."
          />
        )}
      </Card>
    </div>
  );
}
