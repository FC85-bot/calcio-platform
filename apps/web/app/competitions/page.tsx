import Link from "next/link";
import { Card } from "@/components/card";
import { PageHeader } from "@/components/page-header";
import { EmptyState, ErrorState } from "@/components/state-block";
import { listCompetitions, listSeasons } from "@/lib/api";

type SearchParams = Promise<Record<string, string | string[] | undefined>>;

function firstValue(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

export default async function CompetitionsPage({
  searchParams,
}: {
  searchParams: SearchParams;
}) {
  const resolvedSearchParams = await searchParams;
  const season = firstValue(resolvedSearchParams.season);
  const seasonId = firstValue(resolvedSearchParams.season_id);

  const [competitionsResult, seasonsResult] = await Promise.all([
    listCompetitions({ season, season_id: seasonId, limit: 200 }),
    listSeasons({ limit: 200 }),
  ]);

  return (
    <div className="stack-lg">
      <PageHeader
        title="Competitions"
        description="Elenco competizioni disponibile nel backend reale, con accesso rapido a standings e matches."
      />

      <Card title="Filtri">
        <form className="filters-grid" method="GET">
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
          <div className="filters-actions">
            <button type="submit">Applica filtri</button>
            <Link href="/competitions" className="secondary-button">
              Reset
            </Link>
          </div>
        </form>
      </Card>

      <Card title="Lista competizioni" description="Nome, paese e navigazione verso standings e matches.">
        {competitionsResult.error ? (
          <ErrorState title="Errore caricamento competizioni" message={competitionsResult.error} />
        ) : competitionsResult.data && competitionsResult.data.length > 0 ? (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Nome</th>
                  <th>Paese</th>
                  <th>Azioni</th>
                </tr>
              </thead>
              <tbody>
                {competitionsResult.data.map((competition) => {
                  const standingsHref = season
                    ? `/competitions/${competition.id}?season=${encodeURIComponent(season)}`
                    : `/competitions/${competition.id}`;
                  const matchesHref = season
                    ? `/matches?competition_id=${competition.id}&season=${encodeURIComponent(season)}`
                    : `/matches?competition_id=${competition.id}`;

                  return (
                    <tr key={competition.id}>
                      <td>{competition.name}</td>
                      <td>{competition.country}</td>
                      <td>
                        <div className="inline-links">
                          <Link href={standingsHref} className="text-link">
                            Standings
                          </Link>
                          <Link href={matchesHref} className="text-link">
                            Matches
                          </Link>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <EmptyState
            title="Nessuna competition disponibile"
            message="Le API non hanno restituito competizioni per il filtro selezionato."
          />
        )}
      </Card>
    </div>
  );
}
