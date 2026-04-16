import Link from "next/link";
import { Card } from "@/components/card";
import { PageHeader } from "@/components/page-header";
import { EmptyState, ErrorState } from "@/components/state-block";
import { formatNumber } from "@/lib/format";
import { getTeam, listSeasons } from "@/lib/api";

type Params = Promise<{ teamId: string }>;
type SearchParams = Promise<Record<string, string | string[] | undefined>>;

function firstValue(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

export default async function TeamDetailPage({
  params,
  searchParams,
}: {
  params: Params;
  searchParams: SearchParams;
}) {
  const { teamId } = await params;
  const resolvedSearchParams = await searchParams;
  const season = firstValue(resolvedSearchParams.season);

  const teamResult = await getTeam(teamId, { season, form_last_n: 5 });

  if (teamResult.error || !teamResult.data) {
    return (
      <div className="stack-lg">
        <PageHeader title="Team detail" description="Vista team MVP." />
        <ErrorState title="Team non disponibile" message={teamResult.error ?? "Not found"} />
      </div>
    );
  }

  const team = teamResult.data;
  const seasonsResult = await listSeasons({ competition_id: team.competition_id, limit: 100 });
  const standingsHref = team.season
    ? `/competitions/${team.competition.id}?season=${encodeURIComponent(team.season)}`
    : `/competitions/${team.competition.id}`;

  return (
    <div className="stack-lg">
      <PageHeader
        title={team.name}
        description={`Competition: ${team.competition.name}`}
        actions={
          <Link href={standingsHref} className="secondary-button">
            Vai agli standings
          </Link>
        }
      />

      <Card title="Season selector">
        <form className="filters-grid" method="GET">
          <label>
            <span>Season</span>
            <select name="season" defaultValue={team.season ?? ""}>
              <option value="">Auto</option>
              {seasonsResult.data?.map((item) => (
                <option key={item.id} value={item.name}>
                  {item.name}
                </option>
              ))}
            </select>
          </label>
          <div className="filters-actions">
            <button type="submit">Aggiorna</button>
            <Link href={`/teams/${team.id}`} className="secondary-button">
              Reset
            </Link>
          </div>
        </form>
      </Card>

      <div className="grid-2">
        <Card title="Overview">
          <dl className="detail-grid">
            <div>
              <dt>Team</dt>
              <dd>{team.name}</dd>
            </div>
            <div>
              <dt>Competition</dt>
              <dd>
                <Link href={standingsHref} className="text-link">
                  {team.competition.name}
                </Link>
              </dd>
            </div>
            <div>
              <dt>Season</dt>
              <dd>{team.season_detail?.name ?? team.season ?? "—"}</dd>
            </div>
          </dl>
        </Card>

        <Card title="Recent form">
          {team.form && team.form.results.length > 0 ? (
            <div className="form-badges">
              {team.form.results.map((result, index) => (
                <span key={`${result}-${index}`} className={`form-badge form-${result.toLowerCase()}`}>
                  {result}
                </span>
              ))}
            </div>
          ) : (
            <p className="muted">Forma recente non disponibile.</p>
          )}

          <div className="spacer-sm" />

          {team.streak ? (
            <p>
              Streak attuale: <strong>{team.streak.current_streak_type ?? "none"}</strong> · lunghezza{" "}
              <strong>{team.streak.current_streak_length}</strong>
            </p>
          ) : (
            <p className="muted">Streak non disponibile.</p>
          )}
        </Card>
      </div>

      <Card title="Stats base" description="Solo dati già esposti dalle API, senza ricostruzioni lato frontend.">
        {team.stats ? (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Metric</th>
                  <th>Overall</th>
                  <th>Home</th>
                  <th>Away</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>Matches played</td>
                  <td>{team.stats.matches_played}</td>
                  <td>{team.stats.home.matches}</td>
                  <td>{team.stats.away.matches}</td>
                </tr>
                <tr>
                  <td>Wins</td>
                  <td>{team.stats.wins}</td>
                  <td>{team.stats.home.wins}</td>
                  <td>{team.stats.away.wins}</td>
                </tr>
                <tr>
                  <td>Draws</td>
                  <td>{team.stats.draws}</td>
                  <td>{team.stats.home.draws}</td>
                  <td>{team.stats.away.draws}</td>
                </tr>
                <tr>
                  <td>Losses</td>
                  <td>{team.stats.losses}</td>
                  <td>{team.stats.home.losses}</td>
                  <td>{team.stats.away.losses}</td>
                </tr>
                <tr>
                  <td>Goals scored</td>
                  <td>{team.stats.goals_scored}</td>
                  <td>{team.stats.home.goals_scored}</td>
                  <td>{team.stats.away.goals_scored}</td>
                </tr>
                <tr>
                  <td>Goals conceded</td>
                  <td>{team.stats.goals_conceded}</td>
                  <td>{team.stats.home.goals_conceded}</td>
                  <td>{team.stats.away.goals_conceded}</td>
                </tr>
                <tr>
                  <td>Avg goals scored</td>
                  <td>{formatNumber(team.stats.avg_goals_scored, 2)}</td>
                  <td>—</td>
                  <td>—</td>
                </tr>
                <tr>
                  <td>Avg goals conceded</td>
                  <td>{formatNumber(team.stats.avg_goals_conceded, 2)}</td>
                  <td>—</td>
                  <td>—</td>
                </tr>
              </tbody>
            </table>
          </div>
        ) : (
          <EmptyState
            title="Stats non disponibili"
            message="Per questa squadra non risultano stats base disponibili dalla API per la season selezionata."
          />
        )}
      </Card>
    </div>
  );
}
