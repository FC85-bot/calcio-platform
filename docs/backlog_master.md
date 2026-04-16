# Backlog Master

Legenda priorità:
- P0 = bloccante / essenziale
- P1 = alta
- P2 = media
- P3 = bassa

Legenda stato:
- done
- planned
- blocked
- deferred

## Sprint 4

| ID task | Titolo | Fase/Sprint target | Priorità | Dipendenze | Stato | Definition of Done |
|---|---|---|---|---|---|---|
| S4-01 | Formalizzare perimetro MVP | Sprint 4 | P0 | Nessuna | done | Documento MVP scritto e coerente con la baseline reale |
| S4-02 | Formalizzare convenzioni architetturali | Sprint 4 | P0 | S4-01 | done | Convenzioni tecniche versionate in `docs/architecture_conventions.md` |
| S4-03 | Creare backlog master iniziale | Sprint 4 | P0 | S4-01 | done | Backlog leggibile da Sprint 4 a Sprint 15 presente in repo |
| S4-04 | Pulizia repository e `.gitignore` | Sprint 4 | P0 | Nessuna | done | `.env`, `.venv`, cache e artefatti locali esclusi e rimossi dalla base consegnata |
| S4-05 | Allineare README tecnico | Sprint 4 | P0 | S4-01, S4-02 | done | README root aggiornato allo stato reale del progetto |
| S4-06 | Aggiungere CI minima backend | Sprint 4 | P0 | S4-05 | done | Workflow GitHub Actions esegue install, lint/check e verifica migration base |

## Sprint 5

| ID task | Titolo | Fase/Sprint target | Priorità | Dipendenze | Stato | Definition of Done |
|---|---|---|---|---|---|---|
| S5-01 | Stabilizzare test suite backend minima | Sprint 5 | P0 | S4-06 | planned | Esistono test smoke su health e endpoint read-only principali |
| S5-02 | Hardening query filters API | Sprint 5 | P1 | S4-05 | planned | Filtri e casi edge documentati e coperti da test minimi |
| S5-03 | Allineare documentazione endpoint e response shapes | Sprint 5 | P1 | S4-05 | planned | Contratti API correnti descritti in modo consistente |

## Sprint 6

| ID task | Titolo | Fase/Sprint target | Priorità | Dipendenze | Stato | Definition of Done |
|---|---|---|---|---|---|---|
| S6-01 | Rafforzare controlli qualità ingestion | Sprint 6 | P1 | S5-01 | planned | Validazioni e failure mode di ingestion coperte da test e logging coerente |
| S6-02 | Verificare idempotenza completa pipeline mock | Sprint 6 | P0 | S5-01 | planned | Rerun ingestion non genera duplicazioni indesiderate |
| S6-03 | Definire dataset seed/mock repeatable | Sprint 6 | P1 | S6-02 | planned | Dataset mock riproducibile e documentato per ambienti locali e CI |

## Sprint 7

| ID task | Titolo | Fase/Sprint target | Priorità | Dipendenze | Stato | Definition of Done |
|---|---|---|---|---|---|---|
| S7-01 | Consolidare contract del layer provider | Sprint 7 | P1 | S6-01 | planned | Interfaccia provider stabile e documentata per provider futuri |
| S7-02 | Preparare estensione controllata a provider reali | Sprint 7 | P2 | S7-01 | planned | Esistono checklist e vincoli tecnici per integrare provider reali senza refactor massivi |

## Sprint 8

| ID task | Titolo | Fase/Sprint target | Priorità | Dipendenze | Stato | Definition of Done |
|---|---|---|---|---|---|---|
| S8-01 | Estendere coverage competizioni e stagioni reali | Sprint 8 | P1 | S7-02 | planned | La piattaforma supporta più dataset calcistici oltre il mock iniziale |
| S8-02 | Rafforzare provider mapping multi-sorgente | Sprint 8 | P1 | S7-02 | planned | Regole di mapping multi-provider validate su casi reali |

## Sprint 9

| ID task | Titolo | Fase/Sprint target | Priorità | Dipendenze | Stato | Definition of Done |
|---|---|---|---|---|---|---|
| S9-01 | Storico odds robusto e query latest/as-of | Sprint 9 | P1 | S8-01 | planned | Query layer supporta letture latest e as-of in modo coerente |
| S9-02 | Verifica prestazioni query odds e matches | Sprint 9 | P2 | S9-01 | planned | Tempi query e indici rivisti sui casi principali |

## Sprint 10

| ID task | Titolo | Fase/Sprint target | Priorità | Dipendenze | Stato | Definition of Done |
|---|---|---|---|---|---|---|
| S10-01 | Espandere dataset statistico di team e competition | Sprint 10 | P1 | S8-01 | planned | Nuove metriche aggregate disponibili e documentate |
| S10-02 | Consolidare read models per consumo analytics | Sprint 10 | P1 | S9-01 | planned | Read models coerenti e riutilizzabili per API e dashboard |

## Sprint 11

| ID task | Titolo | Fase/Sprint target | Priorità | Dipendenze | Stato | Definition of Done |
|---|---|---|---|---|---|---|
| S11-01 | Primo layer dashboard dati | Sprint 11 | P1 | S10-02 | planned | Dataset e query necessari per dashboard dati definiti e stabili |
| S11-02 | KPI iniziali di copertura e qualità dati | Sprint 11 | P2 | S10-02 | planned | Sono esposti indicatori minimi di qualità del dataset |

## Sprint 12

| ID task | Titolo | Fase/Sprint target | Priorità | Dipendenze | Stato | Definition of Done |
|---|---|---|---|---|---|---|
| S12-01 | Introduzione frontend/dashboard MVP | Sprint 12 | P2 | S11-01 | planned | Esiste un frontend minimo che consuma le API già stabilizzate |
| S12-02 | Hardening contratti API per frontend | Sprint 12 | P1 | S12-01 | planned | API usate dal frontend hanno shape e semantica stabili |

## Sprint 13

| ID task | Titolo | Fase/Sprint target | Priorità | Dipendenze | Stato | Definition of Done |
|---|---|---|---|---|---|---|
| S13-01 | Packaging tecnico e ambienti deploy | Sprint 13 | P2 | S4-06 | planned | Build, variabili ambiente e bootstrap deploy sono documentati e ripetibili |
| S13-02 | Separazione config ambiente locale/test/prod | Sprint 13 | P1 | S13-01 | planned | Configurazione ambiente chiara e senza ambiguità operative |

## Sprint 14

| ID task | Titolo | Fase/Sprint target | Priorità | Dipendenze | Stato | Definition of Done |
|---|---|---|---|---|---|---|
| S14-01 | Observability e monitoring tecnico | Sprint 14 | P1 | S13-02 | planned | Logging, health e segnali tecnici sufficienti per gestione ambiente stabile |
| S14-02 | Profilazione performance query/API | Sprint 14 | P2 | S9-02 | planned | Collo di bottiglia principali identificati e misurati |

## Sprint 15

| ID task | Titolo | Fase/Sprint target | Priorità | Dipendenze | Stato | Definition of Done |
|---|---|---|---|---|---|---|
| S15-01 | Chiusura MVP release candidate | Sprint 15 | P0 | S11-01, S13-02, S14-01 | planned | MVP candidato al rilascio con checklist tecnica completata |
| S15-02 | Audit finale qualità tecnica e documentazione | Sprint 15 | P1 | S15-01 | planned | Repo, documenti e comandi operativi risultano coerenti e ripetibili |
