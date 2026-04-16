from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class EvaluationMetricRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    evaluation_run_id: UUID
    metric_code: str
    metric_value: float
    segment_key: str | None = None
    created_at: datetime


class EvaluationRunRowRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    code: str
    name: str
    evaluation_type: str
    model_version_id: UUID | None = None
    market_code: str
    period_start: datetime
    period_end: datetime
    started_at: datetime
    finished_at: datetime | None = None
    status: str
    created_at: datetime
    sample_size: int | None = None
    global_metrics: list[EvaluationMetricRead] = Field(default_factory=list)
    available_segments: list[str] = Field(default_factory=list)


class EvaluationRunDetailRead(EvaluationRunRowRead):
    config_json: dict[str, Any] | None = None
    metrics: list[EvaluationMetricRead] = Field(default_factory=list)
