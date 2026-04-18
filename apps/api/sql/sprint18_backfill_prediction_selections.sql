-- Sprint 18 backfill: confidence_score and edge_pct
-- Run on the target DB after replacing the service files.

UPDATE prediction_selections
SET confidence_score = 0
WHERE confidence_score IS NULL;

UPDATE prediction_selections
SET edge_pct = 0
WHERE edge_pct IS NULL;
