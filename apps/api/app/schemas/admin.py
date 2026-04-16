from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class IngestionRunAuditRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    provider: str | None
    run_type: str
    entity_type: str
    started_at: datetime
    finished_at: datetime | None
    status: str
    row_count: int
    raw_record_count: int
    created_count: int
    updated_count: int
    skipped_count: int
    error_count: int
    error_message: str | None


class RawIngestionAuditRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    run_id: UUID | None
    normalization_run_id: UUID | None
    provider: str
    entity_type: str
    endpoint: str
    raw_path: str | None
    payload_sha256: str | None
    payload_size_bytes: int | None
    request_params: dict[str, Any] | None
    response_metadata: dict[str, Any] | None
    payload: dict[str, Any]
    normalization_status: str
    normalized_at: datetime | None
    normalization_error: str | None
    ingested_at: datetime


class ProviderMappingRead(BaseModel):
    id: UUID
    provider: str
    entity_type: str
    external_id: str
    canonical_id: UUID
    created_at: datetime


class NormalizationEntityBucketRead(BaseModel):
    entity_type: str
    pending: int
    success: int
    failed: int
    skipped: int
    total: int


class QualityChecksRead(BaseModel):
    match_missing_team_count: int
    match_missing_competition_count: int
    team_duplicates_in_scope_count: int
    mapping_missing_count: int
    invalid_match_status_count: int
    odds_inconsistent_count: int
    raw_not_normalized_count: int


class NormalizationStatusRead(BaseModel):
    raw_pending_count: int
    raw_failed_count: int
    raw_success_count: int
    by_entity_type: list[NormalizationEntityBucketRead]
    recent_runs: list[IngestionRunAuditRead]
    quality_checks: QualityChecksRead


class MonitoringCheckRead(BaseModel):
    code: str
    status: str
    detail: str | None = None
    observed_value: str | float | int | None = None
    threshold_value: str | float | int | None = None


class MonitoringPipelineStateRead(BaseModel):
    pipeline: str
    status: str
    detail: str | None = None
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
    lag_minutes: float | None = None
    failed_runs_last_window: int = 0


class MonitoringProviderLatencyRead(BaseModel):
    provider: str
    entity_type: str
    sample_size: int
    avg_latency_ms: float | None = None
    max_latency_ms: float | None = None
    last_latency_ms: float | None = None
    last_observed_at: datetime | None = None


class MonitoringSummaryRead(BaseModel):
    service: str
    environment: str
    generated_at: datetime
    database: str
    raw_pending_count: int
    failed_runs_last_window: int
    checks: list[MonitoringCheckRead] = Field(default_factory=list)
    pipelines: list[MonitoringPipelineStateRead] = Field(default_factory=list)
    provider_latency: list[MonitoringProviderLatencyRead] = Field(default_factory=list)
