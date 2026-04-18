-- 1) confidence_score null check
SELECT COUNT(*) AS confidence_score_nulls
FROM prediction_selections
WHERE confidence_score IS NULL;

-- 2) edge_pct null check
SELECT COUNT(*) AS edge_pct_nulls
FROM prediction_selections
WHERE edge_pct IS NULL;

-- 3) coverage metric persisted in evaluation_metrics
SELECT evaluation_run_id, metric_code, metric_value, segment_key
FROM evaluation_metrics
WHERE metric_code = 'coverage_rate'
ORDER BY created_at DESC;

-- 4) calibration proxy persisted in evaluation_metrics
SELECT evaluation_run_id, metric_code, metric_value, segment_key
FROM evaluation_metrics
WHERE metric_code IN ('calibration_accuracy', 'sample_size')
  AND segment_key LIKE 'calibration_bucket=%'
ORDER BY created_at DESC, segment_key ASC, metric_code ASC;

-- 5) per-horizon metrics persisted in evaluation_metrics
SELECT evaluation_run_id, metric_code, metric_value, segment_key
FROM evaluation_metrics
WHERE segment_key LIKE 'prediction_horizon=%'
ORDER BY created_at DESC, segment_key ASC, metric_code ASC;

-- 6) edge realization persisted in evaluation_metrics
SELECT evaluation_run_id, metric_code, metric_value, segment_key
FROM evaluation_metrics
WHERE metric_code IN ('edge_positive_sample_size', 'edge_positive_win_rate')
ORDER BY created_at DESC, segment_key ASC;
