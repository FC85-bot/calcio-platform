from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.db.session import get_db
from app.models.feature_snapshot import FeatureSnapshot
from app.models.provider import Provider
from app.models.provider_entity import ProviderEntity
from app.models.raw_ingestion import RawIngestion
from app.schemas.admin import (
    IngestionRunAuditRead,
    MonitoringSummaryRead,
    NormalizationStatusRead,
    ProviderMappingRead,
    RawIngestionAuditRead,
)
from app.schemas.evaluation import EvaluationRunDetailRead, EvaluationRunRowRead
from app.schemas.feature_snapshot import FeatureSnapshotDetailRead, FeatureSnapshotListRowRead
from app.schemas.odds import OddsAdminQualityRead, OddsAdminSummaryRead
from app.schemas.prediction import PredictionDetailRead, PredictionRowRead
from app.services.evaluation_service import EvaluationService
from app.services.monitoring_service import MonitoringService
from app.services.normalization_service import NormalizationService
from app.services.odds_query_service import OddsQueryService
from app.services.prediction_service import load_prediction_rows
from app.services.query_service import QueryService

logger = get_logger(__name__)
router = APIRouter()


def _extract_feature_audit(features_json: dict | None) -> dict[str, list[str]]:
    if not isinstance(features_json, dict):
        return {
            "missing_fields": [],
            "missing_feature_groups": [],
            "data_warnings": [],
        }

    raw_audit = features_json.get("feature_audit", {})
    if not isinstance(raw_audit, dict):
        raw_audit = {}

    return {
        "missing_fields": list(raw_audit.get("missing_fields", [])),
        "missing_feature_groups": list(raw_audit.get("missing_feature_groups", [])),
        "data_warnings": list(raw_audit.get("data_warnings", [])),
    }


def _serialize_feature_snapshot_row(snapshot: FeatureSnapshot) -> FeatureSnapshotListRowRead:
    features_json = snapshot.features_json if isinstance(snapshot.features_json, dict) else {}
    audit = _extract_feature_audit(features_json)
    return FeatureSnapshotListRowRead(
        id=snapshot.id,
        match_id=snapshot.match_id,
        as_of_ts=snapshot.as_of_ts,
        feature_set_version=snapshot.feature_set_version,
        prediction_horizon=snapshot.prediction_horizon,
        completeness_score=float(snapshot.completeness_score),
        features_json=features_json,
        missing_fields=audit["missing_fields"],
        missing_feature_groups=audit["missing_feature_groups"],
        data_warnings=audit["data_warnings"],
        created_at=snapshot.created_at,
    )


def _serialize_feature_snapshot_detail(snapshot: FeatureSnapshot) -> FeatureSnapshotDetailRead:
    features_json = snapshot.features_json if isinstance(snapshot.features_json, dict) else {}
    audit = _extract_feature_audit(features_json)
    return FeatureSnapshotDetailRead(
        id=snapshot.id,
        match_id=snapshot.match_id,
        as_of_ts=snapshot.as_of_ts,
        feature_set_version=snapshot.feature_set_version,
        prediction_horizon=snapshot.prediction_horizon,
        completeness_score=float(snapshot.completeness_score),
        features_json=features_json,
        missing_fields=audit["missing_fields"],
        missing_feature_groups=audit["missing_feature_groups"],
        data_warnings=audit["data_warnings"],
        created_at=snapshot.created_at,
        home_team_id=snapshot.home_team_id,
        away_team_id=snapshot.away_team_id,
    )


@router.get("/admin/evaluation-runs", response_model=list[EvaluationRunRowRead])
def list_evaluation_runs(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    market_code: str | None = Query(default=None, pattern="^(ALL|1X2|OU25|BTTS)$"),
    status: str | None = Query(default=None, pattern="^(running|success|failed)$"),
    db: Session = Depends(get_db),
) -> list[EvaluationRunRowRead]:
    return EvaluationService(db).list_runs(
        limit=limit,
        offset=offset,
        market_code=market_code,
        status=status,
    )


@router.get("/admin/evaluation-runs/{run_id}", response_model=EvaluationRunDetailRead)
def get_evaluation_run_detail(
    run_id: UUID,
    db: Session = Depends(get_db),
) -> EvaluationRunDetailRead:
    try:
        return EvaluationService(db).get_run_detail(run_id)
    except ValueError as exc:
        if str(exc) == "evaluation_run_not_found":
            raise HTTPException(status_code=404, detail="evaluation_run_not_found") from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/admin/feature-snapshots", response_model=list[FeatureSnapshotListRowRead])
def list_feature_snapshots(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    match_id: UUID | None = Query(default=None),
    feature_set_version: str | None = Query(default=None, min_length=1),
    prediction_horizon: str | None = Query(default=None, min_length=1),
    db: Session = Depends(get_db),
) -> list[FeatureSnapshotListRowRead]:
    statement = select(FeatureSnapshot)

    if match_id is not None:
        statement = statement.where(FeatureSnapshot.match_id == match_id)
    if feature_set_version is not None:
        statement = statement.where(FeatureSnapshot.feature_set_version == feature_set_version)
    if prediction_horizon is not None:
        statement = statement.where(FeatureSnapshot.prediction_horizon == prediction_horizon)

    statement = (
        statement.order_by(FeatureSnapshot.as_of_ts.desc(), FeatureSnapshot.created_at.desc())
        .offset(offset)
        .limit(limit)
    )

    rows = db.execute(statement).scalars().all()
    return [_serialize_feature_snapshot_row(row) for row in rows]


@router.get("/admin/feature-snapshots/{match_id}", response_model=list[FeatureSnapshotDetailRead])
def list_feature_snapshots_for_match(
    match_id: UUID,
    feature_set_version: str | None = Query(default=None, min_length=1),
    prediction_horizon: str | None = Query(default=None, min_length=1),
    db: Session = Depends(get_db),
) -> list[FeatureSnapshotDetailRead]:
    statement = select(FeatureSnapshot).where(FeatureSnapshot.match_id == match_id)

    if feature_set_version is not None:
        statement = statement.where(FeatureSnapshot.feature_set_version == feature_set_version)
    if prediction_horizon is not None:
        statement = statement.where(FeatureSnapshot.prediction_horizon == prediction_horizon)

    statement = statement.order_by(
        FeatureSnapshot.as_of_ts.desc(), FeatureSnapshot.created_at.desc()
    )

    rows = db.execute(statement).scalars().all()
    return [_serialize_feature_snapshot_detail(row) for row in rows]


@router.get("/admin/ingestion-runs", response_model=list[IngestionRunAuditRead])
def list_ingestion_runs(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    provider: str | None = Query(default=None),
    status: str | None = Query(default=None, pattern="^(running|success|failed)$"),
    run_type: str | None = Query(default=None, pattern="^(raw_ingestion|normalization)$"),
    entity_type: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[dict]:
    query_service = QueryService(db)
    return query_service.list_ingestion_runs(
        limit=limit,
        offset=offset,
        provider=provider,
        status=status,
        run_type=run_type,
        entity_type=entity_type,
    )


@router.get("/admin/raw-ingestion", response_model=list[RawIngestionAuditRead])
def list_raw_ingestion(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    provider: str | None = Query(default=None),
    entity_type: str | None = Query(default=None),
    normalization_status: str | None = Query(
        default=None, pattern="^(pending|success|failed|skipped)$"
    ),
    run_id: UUID | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[RawIngestion]:
    statement = (
        select(RawIngestion).order_by(RawIngestion.ingested_at.desc()).offset(offset).limit(limit)
    )

    if provider:
        statement = statement.where(RawIngestion.provider == provider)
    if entity_type:
        statement = statement.where(RawIngestion.entity_type == entity_type)
    if normalization_status:
        statement = statement.where(RawIngestion.normalization_status == normalization_status)
    if run_id:
        statement = statement.where(RawIngestion.run_id == run_id)

    return list(db.execute(statement).scalars().all())


@router.get("/admin/provider-mappings", response_model=list[ProviderMappingRead])
def list_provider_mappings(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    provider: str | None = Query(default=None),
    entity_type: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[dict]:
    statement = (
        select(ProviderEntity, Provider.name.label("provider_name"))
        .join(Provider, Provider.id == ProviderEntity.provider_id)
        .order_by(
            Provider.name.asc(), ProviderEntity.entity_type.asc(), ProviderEntity.created_at.desc()
        )
        .offset(offset)
        .limit(limit)
    )

    if provider:
        statement = statement.where(Provider.name == provider)
    if entity_type:
        statement = statement.where(ProviderEntity.entity_type == entity_type)

    rows = db.execute(statement).all()
    return [
        {
            "id": mapping.id,
            "provider": provider_name,
            "entity_type": mapping.entity_type,
            "external_id": mapping.external_id,
            "canonical_id": mapping.internal_id,
            "created_at": mapping.created_at,
        }
        for mapping, provider_name in rows
    ]


@router.get("/admin/monitoring/summary", response_model=MonitoringSummaryRead)
def get_monitoring_summary(db: Session = Depends(get_db)) -> dict[str, object]:
    return MonitoringService(db).get_summary()


@router.get("/admin/normalization-status", response_model=NormalizationStatusRead)
def get_normalization_status(db: Session = Depends(get_db)) -> dict:
    return NormalizationService(db).get_normalization_status()


@router.get("/admin/odds/summary", response_model=OddsAdminSummaryRead)
def get_odds_summary(db: Session = Depends(get_db)) -> dict:
    return OddsQueryService(db).get_summary()


@router.get("/admin/odds/quality", response_model=OddsAdminQualityRead)
def get_odds_quality(db: Session = Depends(get_db)) -> dict:
    return OddsQueryService(db).get_quality()


@router.get("/admin/predictions", response_model=list[PredictionRowRead])
def list_predictions(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    match_id: UUID | None = Query(default=None),
    market_code: str | None = Query(default=None, pattern="^(1X2|OU25|BTTS)$"),
    prediction_horizon: str | None = Query(default=None, min_length=1),
    db: Session = Depends(get_db),
) -> list[dict]:
    return load_prediction_rows(
        db,
        match_id=match_id,
        market_code=market_code,
        prediction_horizon=prediction_horizon,
        limit=limit,
        offset=offset,
    )


@router.get("/admin/predictions/{match_id}", response_model=list[PredictionDetailRead])
def list_predictions_for_match(
    match_id: UUID,
    market_code: str | None = Query(default=None, pattern="^(1X2|OU25|BTTS)$"),
    prediction_horizon: str | None = Query(default=None, min_length=1),
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
) -> list[dict]:
    return load_prediction_rows(
        db,
        match_id=match_id,
        market_code=market_code,
        prediction_horizon=prediction_horizon,
        limit=limit,
        offset=0,
    )