import { Card } from "@/components/card";
import { PageHeader } from "@/components/page-header";
import { EmptyState, ErrorState } from "@/components/state-block";
import { StatusPill } from "@/components/status-pill";
import { formatDateTime, truncateText } from "@/lib/format";
import { listIngestionRuns, listRawIngestion } from "@/lib/api";

type SearchParams = Promise<Record<string, string | string[] | undefined>>;

function firstValue(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

export default async function AdminFreshnessPage({ searchParams }: { searchParams: SearchParams }) {
  const resolvedSearchParams = await searchParams;
  const provider = firstValue(resolvedSearchParams.provider);
  const status = firstValue(resolvedSearchParams.status);
  const entityType = firstValue(resolvedSearchParams.entity_type);

  const [runsResult, rawResult] = await Promise.all([
    listIngestionRuns({ limit: 50, provider, status, entity_type: entityType }),
    listRawIngestion({ limit: 20, provider, entity_type: entityType }),
  ]);

  return (
    <div className="stack-lg">
      <PageHeader
        title="Admin data freshness"
        description="Vista tecnica minima per controllare ultime ingestion runs e raw ingestion recenti."
      />

      <Card title="Filtri admin">
        <form className="filters-grid filters-grid-admin" method="GET">
          <label>
            <span>Provider</span>
            <input type="text" name="provider" defaultValue={provider ?? ""} placeholder="football_data" />
          </label>
          <label>
            <span>Status</span>
            <select name="status" defaultValue={status ?? ""}>
              <option value="">Tutti</option>
              <option value="running">running</option>
              <option value="success">success</option>
              <option value="failed">failed</option>
            </select>
          </label>
          <label>
            <span>Entity type</span>
            <input type="text" name="entity_type" defaultValue={entityType ?? ""} placeholder="matches" />
          </label>
          <div className="filters-actions">
            <button type="submit">Applica filtri</button>
          </div>
        </form>
      </Card>

      <Card title="Ingestion runs" description="Ultime run del layer dati con esito e contatori base.">
        {runsResult.error ? (
          <ErrorState title="Errore ingestion runs" message={runsResult.error} />
        ) : runsResult.data && runsResult.data.length > 0 ? (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Started</th>
                  <th>Finished</th>
                  <th>Provider</th>
                  <th>Entity</th>
                  <th>Status</th>
                  <th>Rows</th>
                  <th>Error</th>
                </tr>
              </thead>
              <tbody>
                {runsResult.data.map((run) => (
                  <tr key={run.id}>
                    <td>{formatDateTime(run.started_at)}</td>
                    <td>{formatDateTime(run.finished_at)}</td>
                    <td>{run.provider ?? "—"}</td>
                    <td>{run.entity_type}</td>
                    <td>
                      <StatusPill value={run.status} />
                    </td>
                    <td>{run.row_count}</td>
                    <td>{truncateText(run.error_message, 120)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <EmptyState title="Nessuna ingestion run" message="Nessun record disponibile con i filtri selezionati." />
        )}
      </Card>

      <Card title="Recent raw ingestion metadata" description="Metadati recenti del layer raw, solo se già esposti dal backend.">
        {rawResult.error ? (
          <ErrorState title="Errore raw ingestion" message={rawResult.error} />
        ) : rawResult.data && rawResult.data.length > 0 ? (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Ingested at</th>
                  <th>Provider</th>
                  <th>Entity</th>
                  <th>Endpoint</th>
                  <th>Normalization</th>
                  <th>Payload size</th>
                </tr>
              </thead>
              <tbody>
                {rawResult.data.map((item) => (
                  <tr key={item.id}>
                    <td>{formatDateTime(item.ingested_at)}</td>
                    <td>{item.provider}</td>
                    <td>{item.entity_type}</td>
                    <td>{truncateText(item.endpoint, 60)}</td>
                    <td>
                      <StatusPill value={item.normalization_status} />
                    </td>
                    <td>{item.payload_size_bytes ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <EmptyState
            title="Nessun raw ingestion recente"
            message="Nessun metadata raw disponibile con i filtri selezionati."
          />
        )}
      </Card>
    </div>
  );
}
