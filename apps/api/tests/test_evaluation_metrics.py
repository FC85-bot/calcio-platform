from __future__ import annotations

from uuid import uuid4

from app.services.evaluation_metrics import (
    EvaluatedPredictionRow,
    EvaluatedSelection,
    build_metric_map,
    compute_hit_rate,
    compute_log_loss,
)


def _build_binary_row(
    *, actual: str, top: str, p_yes: float, odds_yes: float | None = None
) -> EvaluatedPredictionRow:
    yes_selection = EvaluatedSelection(
        selection_code="YES",
        predicted_probability=p_yes,
        fair_odds=round(1 / p_yes, 4),
        market_best_odds=odds_yes,
        edge_pct=4.5,
        confidence_score=71.0,
    )
    no_selection = EvaluatedSelection(
        selection_code="NO",
        predicted_probability=1 - p_yes,
        fair_odds=round(1 / (1 - p_yes), 4),
        market_best_odds=1.95,
        edge_pct=-1.0,
        confidence_score=71.0,
    )
    return EvaluatedPredictionRow(
        prediction_id=uuid4(),
        match_id=uuid4(),
        market_code="BTTS",
        competition_id=None,
        competition_name="Serie A",
        season_id=None,
        season_name="2025/2026",
        model_version_id=uuid4(),
        model_code="baseline_poisson_btts",
        model_version="v1",
        actual_selection_code=actual,
        top_selection_code=top,
        top_probability=max(p_yes, 1 - p_yes),
        top_market_best_odds=odds_yes if top == "YES" else 1.95,
        top_edge_pct=4.5 if top == "YES" else -1.0,
        top_confidence_score=71.0,
        selections=(yes_selection, no_selection),
    )


def test_log_loss_and_hit_rate_are_computed_consistently():
    rows = [
        _build_binary_row(actual="YES", top="YES", p_yes=0.70, odds_yes=2.10),
        _build_binary_row(actual="NO", top="YES", p_yes=0.60, odds_yes=2.05),
    ]

    log_loss = compute_log_loss(rows)
    hit_rate = compute_hit_rate(rows)
    metric_map = build_metric_map(rows)

    assert log_loss is not None
    assert round(log_loss, 4) == round(
        ((-__import__("math").log(0.70)) + (-__import__("math").log(0.40))) / 2, 4
    )
    assert hit_rate == 0.5
    assert metric_map["sample_size"] == 2.0
    assert metric_map["simulated_roi"] is not None
