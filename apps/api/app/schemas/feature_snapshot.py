from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class FeatureSnapshotListRowRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    match_id: UUID
    as_of_ts: datetime
    feature_set_version: str
    prediction_horizon: str
    completeness_score: float
    features_json: dict[str, Any]
    missing_fields: list[str] = Field(default_factory=list)
    missing_feature_groups: list[str] = Field(default_factory=list)
    data_warnings: list[str] = Field(default_factory=list)
    created_at: datetime


class FeatureSnapshotDetailRead(FeatureSnapshotListRowRead):
    home_team_id: UUID
    away_team_id: UUID
