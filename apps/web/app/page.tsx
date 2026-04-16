import Link from "next/link";
import { Card } from "@/components/card";
import { ErrorState } from "@/components/state-block";
import { StatusPill } from "@/components/status-pill";
import { formatDateTime } from "@/lib/format";
import { getHealth, listCompetitions, listIngestionRuns, listMatches } from "@/lib/api";

export default async function HomePage() {
  const [healthResult, competitionsResult, matchesResult, runsResult] = await Promise.all([
    getHealth(),
    listCompetitions({ limit: 200 }),
    listMatches({ limit: 8 }),
    listIngestionRuns({ limit: 8 }),
  ]);

  const quickLinks = [
    {
      href: "/competitions",
      label: "Competitions",
      description: "Esplora competizioni e standings disponibili.",
    },
    {
      href: "/matches",
      label: "Matches",
      description: "Consulta lista match e apri il dettaglio di ciascun evento.",
    },
    {
      href: "/admin/freshness",
      label: "Admin freshness",
      description: "Controlla ingestion runs, raw ingestion e stato del layer dati.",
    },
  ];

  return (
    <div className="stack-lg">
      <section className="hero-section">
        <div>
          <h1>Dashboard dati piattaforma</h1>
          <p className="page-description">
            Frontend collegato alle API esistenti per consultare competitions, matches, team page, standings e data freshness, senza estensioni di prodotto fuori scope.
          </p>
        </div>
        <div className="hero-metrics">
          <div className="metric-card">
            <span className="metric-label">API status</span>
            <div className="metric-value-row">
              <span className="metric-value">{healthResult.data?.status ?? "error"}</span>
              <StatusPill value={healthResult.data?.status} />
            </div>
          </div>
          <div className="metric-card">
            <span className="metric-label">Competitions disponibili</span>
            <span className="metric-value">{competitionsResult.data?.length ?? 0}</span>
          </div>
          <div className="metric-card">
            <span className="metric-label">Latest matches mostrati</span>
            <span className="metric-value">{matchesResult.data?.length ?? 0}</span>
          </div>
          <div className="metric-card">
            <span className="metric-label">Recent ingestion runs</span>
            <span className="metric-value">{runsResult.data?.length ?? 0}</span>
          </div>
        </div>
      </section>

      <div className="grid-3">
        {quickLinks.map((link) => (
          <Card key={link.href}>
            <h2>{link.label}</h2>
            <p className="muted">{link.description}</p>
            <Link href={link.href} className="text-link">
              Apri pagina
            </Link>
          </Card>
        ))}
      </div>

      {healthResult.error ? (
        <ErrorState title="Backend non raggiungibile" message={healthResult.error} />
      ) : null}

      <div className="grid-2">
        <Card title="Latest matches" description="Ultimi match restituiti dalle API.">
          {matchesResult.error ? (
            <ErrorState title="Errore matches" message={matchesResult.error} />
          ) : matchesResult.data && matchesResult.data.length > 0 ? (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Data</th>
                    <th>Competition</th>
                    <th>Match</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {matchesResult.data.map((match) => (
                    <tr key={match.id}>
                      <td>{formatDateTime(match.match_date)}</td>
                      <td>{match.competition.name}</td>
                      <td>
                        <Link href={`/matches/${match.id}`} className="text-link">
                          {match.home_team.name} vs {match.away_team.name}
                        </Link>
                      </td>
                      <td>
                        <StatusPill value={match.status} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="muted">Nessun match disponibile.</p>
          )}
        </Card>

        <Card title="Recent ingestion runs" description="Ultime esecuzioni del layer dati.">
          {runsResult.error ? (
            <ErrorState title="Errore ingestion runs" message={runsResult.error} />
          ) : runsResult.data && runsResult.data.length > 0 ? (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Started</th>
                    <th>Provider</th>
                    <th>Entity</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {runsResult.data.map((run) => (
                    <tr key={run.id}>
                      <td>{formatDateTime(run.started_at)}</td>
                      <td>{run.provider ?? "—"}</td>
                      <td>{run.entity_type}</td>
                      <td>
                        <StatusPill value={run.status} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="muted">Nessuna ingestion run disponibile.</p>
          )}
        </Card>
      </div>
    </div>
  );
}
