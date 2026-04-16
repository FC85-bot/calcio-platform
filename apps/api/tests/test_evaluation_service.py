from __future__ import annotations

import pytest
from sqlalchemy import select

from app.models.evaluation_metric import EvaluationMetric
from app.models.evaluation_run import EvaluationRun
from app.services.evaluation_service import EvaluationService
from app.services.feature_engineering_service import FeatureEngineeringService
from app.services.prediction_service import PredictionService, seed_prediction_model_registry
from tests.test_feature_engineering_service import _build_full_history_context
from tests.test_prediction_service import _seed_odds_context


def test_evaluation_service_creates_backtest_run_and_metrics(db_session):
    context = _build_full_history_context(db_session)
    target_match = context["target_match"]

    feature_service = FeatureEngineeringService(db_session)
    feature_result = feature_service.build_feature_snapshot_for_match(
        match_id=target_match.id, persist=True
    )
    assert feature_result["status"] == "created"

    seed_prediction_model_registry(db_session)
    _seed_odds_context(db_session, target_match.id)

    prediction_service = PredictionService(db_session)
    prediction_result = prediction_service.build_predictions_for_match(
        match_id=target_match.id, persist=True
    )
    assert prediction_result["created"] == 3

    target_match.status = "finished"
    target_match.home_goals = 2
    target_match.away_goals = 1
    db_session.commit()

    service = EvaluationService(db_session)
    result = service.run_backtest(
        period_start=target_match.match_date.replace(hour=0, minute=0),
        period_end=target_match.match_date.replace(hour=23, minute=59),
    )

    assert result["status"] == "success"
    assert result["sample_size"] == 3
    assert result["market_code"] == "ALL"
    assert any(metric["metric_code"] == "log_loss" for metric in result["global_metrics"])
    assert any(metric["segment_key"] == "market_code=1X2" for metric in result["metrics"])

    run = db_session.execute(select(EvaluationRun)).scalar_one()
    metrics = db_session.execute(select(EvaluationMetric)).scalars().all()
    assert run.status == "success"
    assert len(metrics) >= 7


def test_evaluation_service_marks_run_failed_when_sample_is_empty(db_session):
    context = _build_full_history_context(db_session)
    target_match = context["target_match"]
    target_match.status = "finished"
    target_match.home_goals = 1
    target_match.away_goals = 0
    db_session.commit()

    service = EvaluationService(db_session)
    with pytest.raises(ValueError, match="evaluation_sample_empty"):
        service.run_backtest(
            period_start=target_match.match_date.replace(hour=0, minute=0),
            period_end=target_match.match_date.replace(hour=23, minute=59),
        )

    runs = db_session.execute(select(EvaluationRun)).scalars().all()
    assert len(runs) == 1
    assert runs[0].status == "failed"
    assert runs[0].config_json["error"] == "evaluation_sample_empty"
