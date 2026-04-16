# Architecture Conventions

Queste convenzioni valgono per l'attuale baseline tecnica e per gli sprint successivi, salvo modifica esplicita approvata.

## 1. Naming tabelle

Regole:
- nomi tabella in `snake_case`
- nomi tabella al plurale
- nomi stabili e descrittivi, senza abbreviazioni inutili

Esempi attuali:
- `competitions`
- `teams`
- `matches`
- `providers`
- `provider_entities`
- `odds`
- `raw_ingestion`
- `app_metadata`

Regole aggiuntive:
- colonne FK nominate `<entity>_id`
- indici nominati `ix_<table>_<column>`
- unique constraints nominate `uq_<table>_<semantic>`
- check constraints nominate `ck_<table>_<semantic>`

## 2. Naming endpoint

Regole:
- endpoint REST in `kebab-case` evitato; usare path semplici e consistenti in lowercase
- prefisso versionato obbligatorio: `/api/v1`
- risorse principali al plurale
- sotto-risorse solo quando il legame è esplicito

Pattern attuali:
- `/api/v1/competitions`
- `/api/v1/competitions/{competition_id}/standings`
- `/api/v1/matches`
- `/api/v1/matches/{match_id}`
- `/api/v1/matches/{match_id}/odds/latest`
- `/api/v1/odds/{match_id}`
- `/api/v1/teams/{team_id}/stats`
- `/api/v1/teams/{team_id}/form`
- `/api/v1/teams/{team_id}/streak`

Regola health:
- `/health` = liveness minima processo
- `/api/v1/health` = readiness applicativa con check DB

## 3. Timezone database

Regola obbligatoria:
- tutti i timestamp persistiti a database devono essere in UTC
- le colonne datetime devono essere timezone-aware quando possibile
- nessuna persistenza di timestamp naive

Regole operative:
- conversione verso timezone locali solo nel layer di presentazione, mai nel DB
- `created_at`, `updated_at`, `match_date`, `timestamp`, `ingested_at` restano in UTC
- provider che consegnano date non UTC devono essere normalizzati prima della persistenza

## 4. Internal IDs

Regole:
- gli internal IDs sono generati internamente dalla piattaforma
- per il dominio corrente si usano UUID come chiavi tecniche principali
- gli external IDs dei provider non devono essere riutilizzati come primary key interna

Uso corretto:
- ID interni = riferimento stabile nei modelli e nelle relazioni interne
- external IDs = input di integrazione, non chiavi di dominio interne

## 5. Provider mapping

Regole:
- ogni mapping provider è identificato da:
  - `provider_id`
  - `entity_type`
  - `external_id`
- il mapping punta sempre a un `internal_id`
- il mapping è necessario per tutte le entità sincronizzate da provider esterni

Entità attualmente ammesse:
- `competition`
- `team`
- `match`

Regole aggiuntive:
- nessun accesso applicativo deve assumere che due provider condividano gli stessi external IDs
- il mapping provider deve essere idempotente e stabile nei rerun

## 6. Odds append-only

Regola di base:
- la tabella `odds` è storica e va trattata come append-only per nuovi snapshot temporali

Conseguenze:
- un nuovo snapshot odds genera una nuova riga con nuovo `timestamp`
- non si sovrascrive la storia modificando timestamp precedenti
- la combinazione `(match_id, provider_id, timestamp)` identifica uno snapshot

Eccezione ammessa:
- in rerun idempotente, la stessa riga può essere riallineata solo se match/provider/timestamp coincidono già
- non è ammesso cancellare o riscrivere la timeline storica per "compattarla"

## 7. Principio snapshot / as-of

Regola:
- i dati time-variant devono poter essere letti secondo due semantiche:
  - latest snapshot
  - snapshot as-of un istante di riferimento

Stato attuale:
- il progetto espone già il concetto di `latest odds`

Convenzione da mantenere:
- quando una query usa dati time-variant, la semantica deve essere esplicita
- `latest` significa ultimo record disponibile per chiave logica
- `as_of` significa ultimo record con timestamp `<= reference_time`

Anche se non tutte le query as-of sono ancora esposte via API, il principio deve guidare modellazione e query future.

## 8. Migration

Regole base:
- ogni modifica schema passa da Alembic
- vietate modifiche manuali allo schema fuori migration
- una migration deve essere piccola, leggibile e reversibile quando possibile
- naming della revision descrittivo e coerente

Regole operative:
- verificare sempre `upgrade head`
- non mischiare refactor non richiesti con cambi schema
- una migration non deve introdurre dati demo o dati business non richiesti
- gli script devono restare robusti in ambiente locale e Docker

## 9. Servizi e query layer

Regole:
- gli endpoint devono rimanere sottili
- logica query e aggregazioni nel service layer
- accesso DB centralizzato tramite sessione SQLAlchemy
- provider layer separato dal query layer

Distribuzione responsabilità:
- endpoint: parsing input, validazione base, status code
- services: query, aggregazioni, serializzazione applicativa minima
- providers: fetch e shape del payload esterno/mock
- models: schema ORM e vincoli
- schemas: shape di output/input API

Da evitare:
- SQL complesso direttamente negli endpoint
- logica provider dentro gli endpoint
- serializzazioni duplicate sparse nel codice

## 10. Logging tecnico

Regole base:
- log applicativi con eventi tecnici leggibili e consistenti
- messaggi evento in `snake_case`
- campi variabili passati come metadata/extra
- niente log rumorosi di debug permanente in flussi standard

Eventi attuali coerenti da mantenere:
- `application_starting`
- `application_started`
- `application_stopped`
- `ingestion_started`
- `ingestion_step_completed`
- `ingestion_finished`

Regole operative:
- loggare in modo chiaro start/fine operazioni tecniche rilevanti
- non loggare segreti, password o connection string complete
- i log devono aiutare a capire:
  - quale componente ha eseguito l'azione
  - quale provider/endpoint è coinvolto
  - quanti record sono stati processati
  - se un controllo infrastrutturale è fallito

## 11. Convenzioni di modifica

Per modifiche future:
- niente refactor estesi fuori sprint approvato
- niente nuove entità o nuovi provider fuori scope dichiarato
- ogni cambiamento deve preservare leggibilità e semplicità
- README e docs di governance devono essere aggiornati quando cambia il comportamento operativo del progetto
