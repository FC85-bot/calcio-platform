import Link from "next/link";
import { Card } from "@/components/card";
import { PageHeader } from "@/components/page-header";
import { EmptyState, ErrorState } from "@/components/state-block";
import { formatDateOnly } from "@/lib/format";
import { getCompetitionStandings, listMatches, listSeasons } from "@/lib/api";

type Params = Promise<{ competitionId: string }>;
type SearchParams = Promise<Record<string, string | string[] | undefined>>;

function firstValue(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

export default async function CompetitionDetailPage({
  params,
  searchParams,
}: {
  params: Params;
  searchParams: SearchParams;
}) {
  const { competitionId } = await params;
  const resolvedSearchParams = await searchParams;
  const season = firstValue(resolvedSearchParams.season);
  const seasonId = firstValue(resolvedSearchParams.season_id);

  const [standingsResult, seasonsResult, matchesResult] = await Promise.all([
    getCompetitionStandings(competitionId, { season, season_id: seasonId }),
    listSeasons({ competition_id: competitionId, limit: 100 }),
    listMatches({ competition_id: competitionId, season, season_id: seasonId, limit: 10 }),
  ]);

  const competitionName = standingsResult.data?.competition.name ?? "Competition";

  return (
    <div className="stack-lg">
      <PageHeader
        title={competitionName}
        description="Vista standings MVP basata sugli endpoint reali già presenti nel backend."
        actions={
          <Link href="/competitions" className="secondary-button">
            Torna a competitions
          </Link>
        }
      />

      <Card title="Season selector">
        <form className="filters-grid" method="GET">
          <label>
            <span>Season</span>
            <select name="season" defaultValue={season ?? ""}>
              <option value="">Latest disponibile</option>
              {seasonsResult.data?.map((item) => (
                <option key={item.id} value={item.name}>
                  {item.name}
                </option>
              ))}
            </select>
          </label>
          <div className="filters-actions">
            <button type="submit">Aggiorna</button>
            <Link href={`/competitions/${competitionId}`} className="secondary-button">
              Reset
            </Link>
          </div>
        </form>
      </Card>

      <Card
        title="Standings"
        description={`Source: ${standingsResult.data?.source ?? "unknown"} · Snapshot: ${formatDateOnly(
          standingsResult.data?.snapshot_date,
        )}`}
      >
        {standingsResult.error ? (
          <ErrorState title="Errore standings" message={standingsResult.error} />
        ) : standingsResult.data && standingsResult.data.standings.length > 0 ? (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Pos</th>
                  <th>Team</th>
                  <th>Pts</th>
                  <th>P</th>
                  <th>W</th>
                  <th>D</th>
                  <th>L</th>
                  <th>GF</th>
                  <th>GA</th>
                  <th>GD</th>
                </tr>
              </thead>
              <tbody>
                {standingsResult.data.standings.map((row) => (
                  <tr key={row.team.id}>
                    <td>{row.position}</td>
                    <td>
                      <Link
                        href={
                          season
                            ? `/teams/${row.team.id}?season=${encodeURIComponent(season)}`
                            : `/teams/${row.team.id}`
                        }
                        className="text-link"
                      >
                        {row.team.name}
                      </Link>
                    </td>
                    <td>{row.points}</td>
                    <td>{row.played}</td>
                    <td>{row.won}</td>
                    <td>{row.drawn}</td>
                    <td>{row.lost}</td>
                    <td>{row.goals_for}</td>
                    <td>{row.goals_against}</td>
                    <td>{row.goal_difference}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <EmptyState
            title="Standings non disponibili"
            message="Per questa competition/season non risultano standings popolati o computabili al momento."
          />
        )}
      </Card>

      <Card title="Recent matches" description="Ultimi match della competition per navigazione rapida.">
        {matchesResult.error ? (
          <ErrorState title="Errore recent matches" message={matchesResult.error} />
        ) : matchesResult.data && matchesResult.data.length > 0 ? (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Data</th>
                  <th>Match</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {matchesResult.data.map((match) => (
                  <tr key={match.id}>
                    <td>{formatDateOnly(match.match_date)}</td>
                    <td>
                      <Link href={`/matches/${match.id}`} className="text-link">
                        {match.home_team.name} vs {match.away_team.name}
                      </Link>
                    </td>
                    <td>{match.status}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="muted">Nessun match recente disponibile.</p>
        )}
      </Card>
    </div>
  );
}
