from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Iterable, Sequence
from uuid import UUID

PROBABILITY_EPSILON = 1e-12
SUPPORTED_EVALUATION_MARKETS: tuple[str, ...] = ("1X2", "OU25", "BTTS")
EXPECTED_SELECTION_CODES: dict[str, tuple[str, ...]] = {
    "1X2": ("HOME", "DRAW", "AWAY"),
    "OU25": ("OVER", "UNDER"),
    "BTTS": ("YES", "NO"),
}


@dataclass(frozen=True)
class EvaluatedSelection:
    selection_code: str
    predicted_probability: float
    fair_odds: float | None = None
    market_best_odds: float | None = None
    edge_pct: float | None = None
    confidence_score: float | None = None


@dataclass(frozen=True)
class EvaluatedPredictionRow:
    prediction_id: UUID
    match_id: UUID
    market_code: str
    competition_id: UUID | None
    competition_name: str | None
    season_id: UUID | None
    season_name: str | None
    model_version_id: UUID
    model_code: str
    model_version: str
    actual_selection_code: str
    top_selection_code: str
    top_probability: float
    top_market_best_odds: float | None
    top_edge_pct: float | None
    top_confidence_score: float | None
    selections: tuple[EvaluatedSelection, ...]


def clamp_probability(value: float, *, epsilon: float = PROBABILITY_EPSILON) -> float:
    return max(min(float(value), 1.0 - epsilon), epsilon)


def probability_sum_is_valid(
    market_code: str, selections: Sequence[EvaluatedSelection], *, tolerance: float = 0.02
) -> bool:
    expected = EXPECTED_SELECTION_CODES.get(market_code)
    if expected is None:
        return False
    if tuple(selection.selection_code for selection in selections) != expected:
        return False
    total = sum(float(selection.predicted_probability) for selection in selections)
    return abs(total - 1.0) <= tolerance


def compute_log_loss(rows: Sequence[EvaluatedPredictionRow]) -> float | None:
    if not rows:
        return None

    total = 0.0
    for row in rows:
        probability = next(
            (
                selection.predicted_probability
                for selection in row.selections
                if selection.selection_code == row.actual_selection_code
            ),
            None,
        )
        if probability is None:
            return None
        total += -math.log(clamp_probability(probability))
    return total / len(rows)


def compute_brier_score(rows: Sequence[EvaluatedPredictionRow]) -> float | None:
    if not rows:
        return None

    total = 0.0
    classes = 0
    for row in rows:
        if not row.selections:
            return None
        classes += len(row.selections)
        for selection in row.selections:
            observed = 1.0 if selection.selection_code == row.actual_selection_code else 0.0
            total += (float(selection.predicted_probability) - observed) ** 2

    if classes == 0:
        return None
    return total / classes


def compute_hit_rate(rows: Sequence[EvaluatedPredictionRow]) -> float | None:
    if not rows:
        return None
    hits = sum(1 for row in rows if row.top_selection_code == row.actual_selection_code)
    return hits / len(rows)


def compute_avg_confidence_score(rows: Sequence[EvaluatedPredictionRow]) -> float | None:
    values = [
        float(row.top_confidence_score) for row in rows if row.top_confidence_score is not None
    ]
    if not values:
        return None
    return sum(values) / len(values)


def compute_avg_edge_pct(rows: Sequence[EvaluatedPredictionRow]) -> float | None:
    values = [float(row.top_edge_pct) for row in rows if row.top_edge_pct is not None]
    if not values:
        return None
    return sum(values) / len(values)


def compute_simulated_roi(rows: Sequence[EvaluatedPredictionRow]) -> float | None:
    returns: list[float] = []
    for row in rows:
        odds = row.top_market_best_odds
        if odds is None or odds <= 1.0:
            continue
        if row.top_selection_code == row.actual_selection_code:
            returns.append(float(odds) - 1.0)
        else:
            returns.append(-1.0)

    if not returns:
        return None
    return sum(returns) / len(returns)


def build_metric_map(rows: Sequence[EvaluatedPredictionRow]) -> dict[str, float | None]:
    return {
        "sample_size": float(len(rows)),
        "avg_confidence_score": compute_avg_confidence_score(rows),
        "avg_edge_pct": compute_avg_edge_pct(rows),
        "log_loss": compute_log_loss(rows),
        "brier_score": compute_brier_score(rows),
        "hit_rate": compute_hit_rate(rows),
        "simulated_roi": compute_simulated_roi(rows),
    }


def band_confidence(value: float | None) -> str | None:
    if value is None:
        return None
    if value < 50:
        return "lt_50"
    if value < 65:
        return "50_64"
    if value < 80:
        return "65_79"
    return "80_plus"


def band_edge(value: float | None) -> str | None:
    if value is None:
        return None
    if value < 0:
        return "lt_0"
    if value < 5:
        return "0_4"
    if value < 10:
        return "5_9"
    return "10_plus"


def group_rows_by_segment(
    rows: Sequence[EvaluatedPredictionRow],
) -> dict[str, list[EvaluatedPredictionRow]]:
    buckets: dict[str, list[EvaluatedPredictionRow]] = {}

    for row in rows:
        segment_keys = {
            f"market_code={row.market_code}",
        }
        if row.competition_id is not None or row.competition_name is not None:
            segment_keys.add(f"competition={row.competition_name or row.competition_id}")
        if row.season_name is not None:
            segment_keys.add(f"season={row.season_name}")

        confidence_band = band_confidence(row.top_confidence_score)
        if confidence_band is not None:
            segment_keys.add(f"confidence_band={confidence_band}")

        edge_band = band_edge(row.top_edge_pct)
        if edge_band is not None:
            segment_keys.add(f"edge_band={edge_band}")

        for segment_key in segment_keys:
            buckets.setdefault(segment_key, []).append(row)

    return buckets


def iter_metric_rows(
    rows: Sequence[EvaluatedPredictionRow],
) -> Iterable[tuple[str | None, str, float]]:
    global_metrics = build_metric_map(rows)
    for metric_code, metric_value in global_metrics.items():
        if metric_value is None:
            continue
        yield None, metric_code, float(metric_value)

    for segment_key, segment_rows in sorted(group_rows_by_segment(rows).items()):
        segment_metrics = build_metric_map(segment_rows)
        for metric_code, metric_value in segment_metrics.items():
            if metric_value is None:
                continue
            yield segment_key, metric_code, float(metric_value)
