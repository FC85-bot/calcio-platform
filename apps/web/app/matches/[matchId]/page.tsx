import Link from "next/link";
import { Card } from "@/components/card";
import { PageHeader } from "@/components/page-header";
import { ErrorState } from "@/components/state-block";
import { StatusPill } from "@/components/status-pill";
import { getMatchOddsHistory, getMatchReport } from "@/lib/api";
import { formatDateOnly, formatDateTime, formatNumber, formatPercent, formatStatusLabel } from "@/lib/format";
import type { OddsBestRow, OddsReportBlock, OddsSnapshotRow, PredictionReportBlock, TeamReportBlock } from "@/lib/types";

type Params = Promise<{ matchId: string }>;

const MARKET_LABELS: Record<string, string> = {
  "1X2": "1X2",
  OU25: "Over/Under 2.5",
  BTTS: "Both Teams To Score",
};

function renderOddsRows(rows: OddsSnapshotRow[] | OddsBestRow[]) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Bookmaker</th>
            <th>Selection</th>
            <th>Line</th>
            <th>Quota</th>
            <th>Snapshot</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.id}>
              <td>{row.bookmaker_name}</td>
              <td>{row.selection_code}</td>
              <td>{formatNumber(row.line_value, 2)}</td>
              <td>{formatNumber(row.odds_value, 2)}</td>
              <td>{formatDateTime(row.snapshot_timestamp)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function renderPredictionRows(block: PredictionReportBlock) {
  if (!block.available || block.selections.length === 0) {
    return <p className="muted">Prediction non disponibile per questo mercato.</p>;
  }

  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Selection</th>
            <th>Prob.</th>
            <th>Fair odds</th>
            <th>Best odds</th>
            <th>Edge</th>
            <th>Confidence</th>
          </tr>
        </thead>
        <tbody>
          {block.selections.map((selection) => (
            <tr key={`${block.market_code}-${selection.selection_code}`}>
              <td>{selection.selection_code}</td>
              <td>{formatPercent(selection.predicted_probability, 1)}</td>
              <td>{formatNumber(selection.fair_odds, 2)}</td>
              <td>{formatNumber(selection.market_best_odds, 2)}</td>
              <td>{formatPercent(selection.edge_pct, 1)}</td>
              <td>{formatNumber(selection.confidence_score, 0)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function renderTeamBlock(teamBlock: TeamReportBlock) {
  return (
    <Card
      title={teamBlock.team.name}
      description={`Ultimi risultati, stats base e split ${teamBlock.venue_split_label ?? "venue"} già disponibili nel backend.`}
    >
      <div className="stack-sm">
        <div>
          <p className="muted">Recent form</p>
          {teamBlock.last_results.length > 0 ? (
            <div className="form-badges">
              {teamBlock.last_results.map((result, index) => (
                <span key={`${teamBlock.team.id}-${result}-${index}`} className={`form-badge form-${result.toLowerCase()}`}>
                  {result}
                </span>
              ))}
            </div>
          ) : (
            <p className="muted">Forma non disponibile.</p>
          )}
        </div>

        <dl className="detail-grid compact-detail-grid">
          <div>
            <dt>Streak</dt>
            <dd>
              {teamBlock.streak?.current_streak_type
                ? `${teamBlock.streak.current_streak_type} · ${teamBlock.streak.current_streak_length}`
                : "—"}
            </dd>
          </div>
          <div>
            <dt>Matches played</dt>
            <dd>{teamBlock.stats?.matches_played ?? "—"}</dd>
          </div>
          <div>
            <dt>Goals scored</dt>
            <dd>{teamBlock.stats?.goals_scored ?? "—"}</dd>
          </div>
          <div>
            <dt>Goals conceded</dt>
            <dd>{teamBlock.stats?.goals_conceded ?? "—"}</dd>
          </div>
          <div>
            <dt>Avg goals scored</dt>
            <dd>{formatNumber(teamBlock.stats?.avg_goals_scored, 2)}</dd>
          </div>
          <div>
            <dt>Avg goals conceded</dt>
            <dd>{formatNumber(teamBlock.stats?.avg_goals_conceded, 2)}</dd>
          </div>
        </dl>

        <div>
          <p className="muted split-title">{teamBlock.venue_split_label === "home" ? "Split casa" : "Split trasferta"}</p>
          {teamBlock.venue_split ? (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>P</th>
                    <th>W</th>
                    <th>D</th>
                    <th>L</th>
                    <th>GF</th>
                    <th>GA</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td>{teamBlock.venue_split.matches}</td>
                    <td>{teamBlock.venue_split.wins}</td>
                    <td>{teamBlock.venue_split.draws}</td>
                    <td>{teamBlock.venue_split.losses}</td>
                    <td>{teamBlock.venue_split.goals_scored}</td>
                    <td>{teamBlock.venue_split.goals_conceded}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          ) : (
            <p className="muted">Split non disponibile.</p>
          )}
        </div>
      </div>
    </Card>
  );
}

function renderOddsMarketBlock(block: OddsReportBlock) {
  return (
    <Card
      key={block.market_code}
      title={MARKET_LABELS[block.market_code] ?? block.market_code}
      description={`Latest snapshot: ${formatDateTime(block.latest_snapshot_timestamp)}`}
    >
      <div className="stack-lg">
        <div>
          <p className="section-subtitle">Latest odds</p>
          {block.latest.length > 0 ? renderOddsRows(block.latest) : <p className="muted">Nessuna latest odds disponibile.</p>}
        </div>
        <div>
          <p className="section-subtitle">Best odds</p>
          {block.best.length > 0 ? renderOddsRows(block.best) : <p className="muted">Nessuna best odds disponibile.</p>}
        </div>
        <div>
          <p className="section-subtitle">Opening odds</p>
          {block.opening.length > 0 ? renderOddsRows(block.opening) : <p className="muted">Nessuna opening odds disponibile.</p>}
        </div>
      </div>
    </Card>
  );
}

export default async function MatchDetailPage({ params }: { params: Params }) {
  const { matchId } = await params;
  const [reportResult, historyResult] = await Promise.all([
    getMatchReport(matchId, { prediction_horizon: "pre_match", form_last_n: 5 }),
    getMatchOddsHistory(matchId, { limit: 120 }),
  ]);

  if (reportResult.error || !reportResult.data) {
    return (
      <div className="stack-lg">
        <PageHeader title="Match detail" description="Match center automatico MVP." />
        <ErrorState title="Report match non disponibile" message={reportResult.error ?? "Not found"} />
      </div>
    );
  }

  const report = reportResult.data;
  const context = report.context;
  const standingsHref = context.season_label
    ? `/competitions/${context.competition.id}?season=${encodeURIComponent(context.season_label)}`
    : `/competitions/${context.competition.id}`;

  return (
    <div className="stack-lg">
      <PageHeader
        title={`${context.home_team.name} vs ${context.away_team.name}`}
        description="Match center MVP generato da dati strutturati già presenti nel sistema: context, form, standings, odds, predictions e warnings."
        actions={
          <Link href="/matches" className="secondary-button">
            Torna ai matches
          </Link>
        }
      />

      <div className="grid-2">
        <Card title="Context">
          <dl className="detail-grid">
            <div>
              <dt>Competition</dt>
              <dd>
                <Link href={standingsHref} className="text-link">
                  {context.competition.name}
                </Link>
              </dd>
            </div>
            <div>
              <dt>Season</dt>
              <dd>{context.season?.name ?? context.season_label ?? "—"}</dd>
            </div>
            <div>
              <dt>Data / Ora</dt>
              <dd>{formatDateTime(context.match_date)}</dd>
            </div>
            <div>
              <dt>Status</dt>
              <dd>
                <StatusPill value={context.status} />
              </dd>
            </div>
            <div>
              <dt>Home team</dt>
              <dd>
                <Link
                  href={
                    context.season_label
                      ? `/teams/${context.home_team.id}?season=${encodeURIComponent(context.season_label)}`
                      : `/teams/${context.home_team.id}`
                  }
                  className="text-link"
                >
                  {context.home_team.name}
                </Link>
              </dd>
            </div>
            <div>
              <dt>Away team</dt>
              <dd>
                <Link
                  href={
                    context.season_label
                      ? `/teams/${context.away_team.id}?season=${encodeURIComponent(context.season_label)}`
                      : `/teams/${context.away_team.id}`
                  }
                  className="text-link"
                >
                  {context.away_team.name}
                </Link>
              </dd>
            </div>
            <div>
              <dt>Score</dt>
              <dd>
                {context.status === "finished" && context.score
                  ? `${context.score.home ?? 0} - ${context.score.away ?? 0}`
                  : "Non disponibile"}
              </dd>
            </div>
          </dl>
        </Card>

        <Card title="Report metadata">
          <dl className="detail-grid compact-detail-grid">
            <div>
              <dt>Report version</dt>
              <dd>{report.report_version}</dd>
            </div>
            <div>
              <dt>Generated at</dt>
              <dd>{formatDateTime(report.generated_at)}</dd>
            </div>
            <div>
              <dt>Feature set version</dt>
              <dd>{report.feature_set_version ?? "—"}</dd>
            </div>
            <div>
              <dt>Warnings</dt>
              <dd>{report.warnings.length}</dd>
            </div>
          </dl>
        </Card>
      </div>

      <div className="grid-2">
        {renderTeamBlock(report.home_team)}
        {renderTeamBlock(report.away_team)}
      </div>

      <Card
        title="Standings context"
        description={`Source: ${report.standings_context.source ?? "missing"} · Snapshot: ${formatDateOnly(report.standings_context.snapshot_date)}`}
      >
        {report.standings_context.available ? (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Team</th>
                  <th>Pos</th>
                  <th>Pts</th>
                  <th>P</th>
                  <th>GD</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>{context.home_team.name}</td>
                  <td>{report.standings_context.home_team?.position ?? "—"}</td>
                  <td>{report.standings_context.home_team?.points ?? "—"}</td>
                  <td>{report.standings_context.home_team?.played ?? "—"}</td>
                  <td>{report.standings_context.home_team?.goal_difference ?? "—"}</td>
                </tr>
                <tr>
                  <td>{context.away_team.name}</td>
                  <td>{report.standings_context.away_team?.position ?? "—"}</td>
                  <td>{report.standings_context.away_team?.points ?? "—"}</td>
                  <td>{report.standings_context.away_team?.played ?? "—"}</td>
                  <td>{report.standings_context.away_team?.goal_difference ?? "—"}</td>
                </tr>
              </tbody>
            </table>
          </div>
        ) : (
          <p className="muted">Standings non disponibili o non affidabili per questo match.</p>
        )}
      </Card>

      <div className="stack-lg">
        <div>
          <h2 className="section-heading">Odds principali</h2>
          <p className="muted">Latest, best e opening odds per i mercati MVP del report.</p>
        </div>
        <div className="grid-3 report-grid">
          {report.odds.map((block) => renderOddsMarketBlock(block))}
        </div>
      </div>

      <Card title="Prediction block" description="Probabilità, fair odds, market best odds, edge e confidence score per i mercati MVP.">
        <div className="stack-lg">
          {report.predictions.map((block) => (
            <div key={block.market_code} className="report-subsection">
              <div className="report-subsection-header">
                <div>
                  <h3>{MARKET_LABELS[block.market_code] ?? block.market_code}</h3>
                  <p className="muted">
                    Model: {block.model_code ?? "—"} · Version: {block.model_version ?? "—"} · Horizon: {block.prediction_horizon ?? "—"}
                  </p>
                </div>
                <div className="report-pill-row">
                  <span className="status-pill status-pending">quality {formatNumber(block.data_quality_score, 2)}</span>
                  <span className="status-pill status-pending">as_of {block.as_of_ts ? formatDateTime(block.as_of_ts) : "—"}</span>
                </div>
              </div>
              {renderPredictionRows(block)}
            </div>
          ))}
        </div>
      </Card>

      <Card title="Warnings" description="Warning sintetici, machine-friendly e leggibili dal report service.">
        {report.warnings.length > 0 ? (
          <div className="warning-list">
            {report.warnings.map((warning) => (
              <div key={`${warning.section}-${warning.code}`} className="warning-item">
                <div className="warning-item-header">
                  <strong>{warning.code}</strong>
                  <span className="muted">
                    {warning.section} · {formatStatusLabel(warning.severity)}
                  </span>
                </div>
                <p className="muted">{warning.detail ?? "—"}</p>
              </div>
            ))}
          </div>
        ) : (
          <p className="muted">Nessun warning tecnico sul report corrente.</p>
        )}
      </Card>

      <Card title="Mini history odds" description="Storico tabellare semplice, append-only, ordinato per snapshot discendente.">
        {historyResult.data && historyResult.data.length > 0 ? (
          renderOddsRows(historyResult.data)
        ) : (
          <p className="muted">Nessuna history odds disponibile per questo match.</p>
        )}
      </Card>
    </div>
  );
}
