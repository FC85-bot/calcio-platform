# Sprint 11.1 — Feature Contract MVP Hardening

## Versione feature set
- `feature_set_version`: `sprint11_mvp_v1`
- `prediction_horizon`: `pre_match`
- livello: team-level + match-context minimale
- persistenza: tabella `feature_snapshots`
- nessun prediction engine in questo sprint

## Regola no leakage
La regola è rigida, leggibile e difesa due volte nel codice:
- ogni snapshot usa un `as_of_ts` esplicito in UTC
- il target match è valido solo se `as_of_ts <= match_date`
- ogni match storico usato per le feature deve soddisfare **tutte** queste condizioni:
  - stesso contesto competitivo del match target (`competition_id` + `season`)
  - `status = 'finished'`
  - `home_goals` e `away_goals` valorizzati
  - `match_date < as_of_ts`
- la finestra storica finale viene sempre rifiltrata con controllo **strictly-before cutoff**
- nessuna feature legge il risultato del match target
- nessuna feature legge match futuri rispetto a `as_of_ts`
- Sprint 11.1 non usa odds come feature
- standings context usato solo da `standings_snapshots` con regola conservativa:
  - `snapshot_date < as_of_ts::date`
  - snapshot dello stesso giorno non ammessi, anche se presenti a DB

## Hardening input / skip rules
Validazioni minime robuste introdotte:
- `match_missing_home_team_id`
- `match_missing_away_team_id`
- `match_missing_match_date`
- `match_missing_competition_id`
- `match_missing_season`
- `unsupported_prediction_horizon:<value>`
- `as_of_ts_after_match_kickoff`
- `duplicate_snapshot`

Gestione:
- snapshot duplicato => `skipped`
- input target incoerente => `skipped`
- errore tecnico inatteso => `error`
- storico insufficiente o standings mancanti => snapshot creato ma con audit e completezza ridotta

## Chiave tecnica snapshot
Una riga è univoca per:
- `match_id`
- `as_of_ts`
- `prediction_horizon`
- `feature_set_version`

## Completezza
Scala: `0.0` → `1.0`

Formula MVP:
- gruppi overall ultimi 5 pesano `10` feature per squadra
- split venue pesano `3` feature per squadra
- standings context pesa `3` feature per squadra solo quando `season_id` del match target consente lookup snapshot
- ogni gruppo usa un rapporto di copertura semplice:
  - esempio `3 match disponibili su 5` => copertura `0.60`
- lo score finale è:
  - `somma(pesi * copertura_gruppo) / somma(pesi_attesi)`

Effetto pratico:
- pochi match storici => score più basso anche se le feature numeriche sono comunque calcolate
- standings mancanti => score più basso solo quando il contesto standings è effettivamente supportabile

## Breakdown delle mancanze
Ogni snapshot salva in `features_json.feature_audit`:
- `missing_fields`
- `missing_feature_groups`
- `data_warnings`

Esempi tipici:
- `home_team_overall_last_5_insufficient_history:3_of_5`
- `away_team_standings_missing`
- `home_team_standings_excluded_by_cutoff`
- `season_id_missing_on_match`

Questi campi sono audit metadata e non feature numeriche da usare in un eventuale training futuro.

## Contratto feature operativo

### Gruppo 1 — forma ultime 5, team home
| feature | definizione | livello | finestra |
|---|---|---|---|
| `home_team_last_5_points` | punti ottenuti nelle ultime 5 gare complessive pre `as_of_ts` | team-level | ultime 5 gare |
| `home_team_last_5_wins` | vittorie nelle ultime 5 gare complessive | team-level | ultime 5 gare |
| `home_team_last_5_draws` | pareggi nelle ultime 5 gare complessive | team-level | ultime 5 gare |
| `home_team_last_5_losses` | sconfitte nelle ultime 5 gare complessive | team-level | ultime 5 gare |
| `home_team_last_5_goals_scored` | gol segnati nelle ultime 5 gare complessive | team-level | ultime 5 gare |
| `home_team_last_5_goals_conceded` | gol subiti nelle ultime 5 gare complessive | team-level | ultime 5 gare |
| `home_team_avg_goals_scored_last_5` | media gol segnati ultime 5 gare complessive | team-level | ultime 5 gare |
| `home_team_avg_goals_conceded_last_5` | media gol subiti ultime 5 gare complessive | team-level | ultime 5 gare |
| `home_team_clean_sheet_rate_last_5` | quota gare senza subire gol nelle ultime 5 | team-level | ultime 5 gare |
| `home_team_failed_to_score_rate_last_5` | quota gare senza segnare nelle ultime 5 | team-level | ultime 5 gare |

### Gruppo 2 — forma ultime 5, team away
| feature | definizione | livello | finestra |
|---|---|---|---|
| `away_team_last_5_points` | punti ottenuti nelle ultime 5 gare complessive pre `as_of_ts` | team-level | ultime 5 gare |
| `away_team_last_5_wins` | vittorie nelle ultime 5 gare complessive | team-level | ultime 5 gare |
| `away_team_last_5_draws` | pareggi nelle ultime 5 gare complessive | team-level | ultime 5 gare |
| `away_team_last_5_losses` | sconfitte nelle ultime 5 gare complessive | team-level | ultime 5 gare |
| `away_team_last_5_goals_scored` | gol segnati nelle ultime 5 gare complessive | team-level | ultime 5 gare |
| `away_team_last_5_goals_conceded` | gol subiti nelle ultime 5 gare complessive | team-level | ultime 5 gare |
| `away_team_avg_goals_scored_last_5` | media gol segnati ultime 5 gare complessive | team-level | ultime 5 gare |
| `away_team_avg_goals_conceded_last_5` | media gol subiti ultime 5 gare complessive | team-level | ultime 5 gare |
| `away_team_clean_sheet_rate_last_5` | quota gare senza subire gol nelle ultime 5 | team-level | ultime 5 gare |
| `away_team_failed_to_score_rate_last_5` | quota gare senza segnare nelle ultime 5 | team-level | ultime 5 gare |

### Gruppo 3 — split casa / trasferta
| feature | definizione | livello | finestra |
|---|---|---|---|
| `home_team_home_last_5_points` | punti del team home nelle ultime 5 gare giocate in casa | team-level | ultime 5 home |
| `home_team_home_last_5_goals_scored` | gol segnati dal team home nelle ultime 5 gare in casa | team-level | ultime 5 home |
| `home_team_home_last_5_goals_conceded` | gol subiti dal team home nelle ultime 5 gare in casa | team-level | ultime 5 home |
| `away_team_away_last_5_points` | punti del team away nelle ultime 5 gare giocate in trasferta | team-level | ultime 5 away |
| `away_team_away_last_5_goals_scored` | gol segnati dal team away nelle ultime 5 gare in trasferta | team-level | ultime 5 away |
| `away_team_away_last_5_goals_conceded` | gol subiti dal team away nelle ultime 5 gare in trasferta | team-level | ultime 5 away |

### Gruppo 4 — standings context condizionale
| feature | definizione | livello | finestra |
|---|---|---|---|
| `home_team_league_position` | posizione del team home nell’ultimo standings snapshot valido prima di `as_of_ts` | match-context | ultimo snapshot valido |
| `home_team_points_per_game` | punti/partite dal medesimo snapshot standings | match-context | ultimo snapshot valido |
| `home_team_goal_difference` | differenza reti dal medesimo snapshot standings | match-context | ultimo snapshot valido |
| `away_team_league_position` | posizione del team away nell’ultimo standings snapshot valido prima di `as_of_ts` | match-context | ultimo snapshot valido |
| `away_team_points_per_game` | punti/partite dal medesimo snapshot standings | match-context | ultimo snapshot valido |
| `away_team_goal_difference` | differenza reti dal medesimo snapshot standings | match-context | ultimo snapshot valido |

## Test automatici minimi disponibili
- `test_feature_form_last_5_is_calculated_correctly`
- `test_no_leakage_ignores_matches_after_as_of_ts`
- `test_duplicate_snapshot_is_not_recreated`
- `test_standings_conservative_cutoff_excludes_same_day_snapshots`
- `test_completeness_and_missing_breakdown_with_insufficient_history`

## Supporto minimo dati
Per costruire uno snapshot servono almeno:
- `match_id`
- `home_team_id`
- `away_team_id`
- `competition_id`
- `season`
- `match_date`

Se i dati storici sono pochi:
- le feature vengono comunque calcolate sui match realmente disponibili
- `completeness_score` scende
- `feature_audit` espone il perché
- il logger emette warning tecnici

## Limiti ancora aperti
- niente odds features
- niente feature player-level
- niente feature H2H
- niente caching
- niente prediction engine
- standings context ancora daily-level, quindi il cutoff è volutamente conservativo per evitare leakage
