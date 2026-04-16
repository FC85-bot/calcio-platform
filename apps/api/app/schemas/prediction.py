from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class PredictionSelectionRowRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID | None = None
    selection_code: str
    predicted_probability: float
    fair_odds: float
    market_best_odds: float | None = None
    edge_pct: float | None = None
    confidence_score: float | None = None
    created_at: datetime | None = None


class PredictionRowRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID | None = None
    match_id: UUID
    feature_snapshot_id: UUID
    model_version_id: UUID
    model_version: str
    model_code: str
    model_name: str
    market_code: str
    prediction_horizon: str
    as_of_ts: datetime
    data_quality_score: float
    created_at: datetime | None = None
    selections: list[PredictionSelectionRowRead] = Field(default_factory=list)


class PredictionDetailRead(PredictionRowRead):
    pass
