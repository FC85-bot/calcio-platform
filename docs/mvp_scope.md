# MVP Scope

## Obiettivo MVP

Il MVP di Calcio Platform deve validare una base dati e API read-only coerente per analisi calcistica essenziale, partendo da un dataset controllato e da un solo provider mock.

L'obiettivo non è coprire l'intero dominio football, ma dimostrare in modo stabile:
- ingestion minima ma idempotente
- persistenza relazionale coerente
- mapping provider -> internal IDs
- consultazione API di fixtures, odds e statistiche base
- output dati pronti per un futuro layer dashboard

## Competizioni iniziali

Nel MVP iniziale è inclusa una sola competizione:
- Serie A Mock

Scelta intenzionale:
- una sola competizione riduce variabilità e complessità
- consente di validare schema, ingestion, filtri API e query aggregate
- evita di introdurre problemi multi-country e multi-calendar fuori fase

## Stagioni iniziali

Stagione iniziale supportata nel dataset mock:
- 2026/2027

Il MVP assume una singola stagione iniziale controllata.
La gestione multi-season estesa non è obiettivo di Sprint 4.

## Mercati iniziali

Mercati odds iniziali inclusi:
- 1X2
  - home_win
  - draw
  - away_win
- Over/Under 2.5
  - over_2_5
  - under_2_5

Non sono inclusi altri mercati in questa fase.

## Provider iniziali

Provider iniziale incluso:
- `mock_provider`

Il provider mock è il solo provider supportato nel MVP perché serve a validare:
- pipeline ingestion
- mapping provider entities
- persistenza raw payload
- query su matches e odds
- statistiche aggregate minime

## Feature iniziali del prodotto dati

Feature incluse nel MVP:
- anagrafica competizioni
- anagrafica squadre per competizione
- fixtures/matches con stagione, data e stato
- risultati base per match conclusi
- storico odds per match/provider/timestamp
- latest odds per match
- standings aggregate per competizione e stagione
- statistiche base per team e stagione
- form recente del team
- streak corrente del team
- persistenza raw payload di ingestion
- mapping stabile tra external IDs provider e internal IDs piattaforma
- health liveness e readiness

## Output iniziali della dashboard dati

Il MVP non include ancora un frontend dashboard.
Include però gli output dati minimi da cui una dashboard può essere costruita:
- elenco competizioni disponibili
- elenco matches filtrabile per:
  - competition_id
  - season
  - intervallo date
  - team_id
  - status
- dettaglio match
- latest odds per match
- storico odds per match
- standings per competizione/stagione
- statistiche aggregate per team/stagione
- form team
- streak team

Questi output sono esposti tramite API read-only e costituiscono il primo dataset consultabile del prodotto.

## Fuori MVP in modo esplicito

Sono fuori MVP:
- provider reali esterni
- multi-provider production-grade
- live ingestion continua
- websocket o streaming live
- prediction, forecasting o modelli ML
- quote avanzate oltre 1X2 e Over/Under 2.5
- xG, xA, eventi granulari partita, player analytics
- dashboard frontend
- autenticazione, autorizzazione, utenti e ruoli
- betting logic, alerting o recommendation engine
- export/reporting avanzato
- orchestration distribuita o job scheduling complesso
- caching avanzato
- multi-tenant
- supporto completo per più competizioni, più paesi e più stagioni reali

## Confine operativo del MVP

Il MVP deve essere considerato completato quando esistono in modo stabile:
- schema relazionale coerente
- migration applicabili
- dataset mock ingestibile end-to-end
- endpoint read-only minimi funzionanti
- convenzioni architetturali e backlog formalizzati
- repository pulito e documentato
