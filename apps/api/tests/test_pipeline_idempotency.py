from __future__ import annotations

from pathlib import Path

from sqlalchemy import select

from app.models.competition import Competition
from app.models.evaluation_run import EvaluationRun
from app.models.ingestion_run import IngestionRun
from app.models.prediction import Prediction
from app.models.prediction_selection import PredictionSelection
from app.models.provider_entity import ProviderEntity
from app.models.raw_ingestion import RawIngestion
from app.providers.base import BaseProvider, ProviderFetchResult
from app.services.evaluation_service import EvaluationService
from app.services.feature_engineering_service import FeatureEngineeringService
from app.services.normalization_service import NormalizationService
from app.services.prediction_service import PredictionService, seed_prediction_model_registry
from app.services.raw_ingestion_service import RawIngestionService
from app.services.raw_storage_service import RawStorageService
from tests.helpers.feature_factory import count_feature_snapshots
from tests.test_feature_engineering_service import _build_full_history_context
from tests.test_prediction_service import _seed_odds_context


class StubRawProvider(BaseProvider):
    name = "stub_provider"
    supports_odds = False

    @property
    def _base_url(self) -> str:
        return "https://stub.local"

    def fetch_competitions(self) -> list[ProviderFetchResult]:
        payload = {
            "competitions": [
                {
                    "id": 101,
                    "name": "Stub League",
                    "country": "IT",
                    "code": "STUB",
                }
            ]
        }
        return [
            ProviderFetchResult(
                entity_type="competitions",
                endpoint="/competitions",
                payload=payload,
                items=[payload["competitions"][0]],
                request_params=None,
                response_metadata={"status_code": 200, "latency_ms": 12.5},
            )
        ]

    def fetch_seasons(self) -> list[ProviderFetchResult]:
        return []

    def fetch_teams(self) -> list[ProviderFetchResult]:
        return []

    def fetch_matches(self) -> list[ProviderFetchResult]:
        return []

    def fetch_odds(self) -> list[ProviderFetchResult]:
        return []


def test_raw_ingestion_rerun_is_idempotent(db_session, tmp_path: Path):
    service = RawIngestionService(
        db=db_session,
        provider=StubRawProvider(),
        raw_storage_service=RawStorageService(base_path=tmp_path / "raw"),
    )

    first = service.ingest_competitions()
    second = service.ingest_competitions()

    raw_rows = db_session.execute(select(RawIngestion)).scalars().all()
    runs = (
        db_session.execute(select(IngestionRun).order_by(IngestionRun.started_at.asc()))
        .scalars()
        .all()
    )

    assert first["created_count"] == 1
    assert first["skipped_count"] == 0
    assert second["created_count"] == 0
    assert second["skipped_count"] == 1
    assert len(raw_rows) == 1
    assert len(runs) == 2
    assert runs[1].skipped_count == 1


def test_normalization_rerun_is_idempotent(db_session, tmp_path: Path):
    ingestion_service = RawIngestionService(
        db=db_session,
        provider=StubRawProvider(),
        raw_storage_service=RawStorageService(base_path=tmp_path / "raw"),
    )
    ingestion_service.ingest_competitions()

    service = NormalizationService(db_session)
    first = service.run(
        entity_types=["competitions"], provider="stub_provider", include_processed=True
    )
    second = service.run(
        entity_types=["competitions"], provider="stub_provider", include_processed=True
    )

    competitions = db_session.execute(select(Competition)).scalars().all()
    mappings = db_session.execute(select(ProviderEntity)).scalars().all()

    assert len(first) == 1
    assert len(second) == 1
    assert first[0]["created_count"] == 1
    assert second[0]["created_count"] == 0
    assert second[0]["error_count"] == 0
    assert len(competitions) == 1
    assert len(mappings) == 1


def test_feature_snapshot_rerun_is_idempotent(db_session):
    context = _build_full_history_context(db_session)
    target_match = context["target_match"]
    service = FeatureEngineeringService(db_session)

    first = service.build_feature_snapshot_for_match(match_id=target_match.id, persist=True)
    second = service.build_feature_snapshot_for_match(match_id=target_match.id, persist=True)

    assert first["status"] == "created"
    assert second["status"] == "skipped"
    assert second["warning"] == "duplicate_snapshot"
    assert count_feature_snapshots(db_session) == 1


def test_prediction_rerun_is_idempotent(db_session):
    context = _build_full_history_context(db_session)
    target_match = context["target_match"]

    FeatureEngineeringService(db_session).build_feature_snapshot_for_match(
        match_id=target_match.id, persist=True
    )
    seed_prediction_model_registry(db_session)
    _seed_odds_context(db_session, target_match.id)

    service = PredictionService(db_session)
    first = service.build_predictions_for_match(match_id=target_match.id, persist=True)
    second = service.build_predictions_for_match(match_id=target_match.id, persist=True)

    predictions = db_session.execute(select(Prediction)).scalars().all()
    selections = db_session.execute(select(PredictionSelection)).scalars().all()

    assert first["created"] == 3
    assert second["created"] == 0
    assert second["skipped"] == 3
    assert all(item["warning"] == "duplicate_prediction" for item in second["results"])
    assert len(predictions) == 3
    assert len(selections) == 7


def test_evaluation_rerun_reuses_existing_code(db_session):
    context = _build_full_history_context(db_session)
    target_match = context["target_match"]

    FeatureEngineeringService(db_session).build_feature_snapshot_for_match(
        match_id=target_match.id, persist=True
    )
    seed_prediction_model_registry(db_session)
    _seed_odds_context(db_session, target_match.id)
    PredictionService(db_session).build_predictions_for_match(
        match_id=target_match.id, persist=True
    )

    target_match.status = "finished"
    target_match.home_goals = 2
    target_match.away_goals = 1
    db_session.commit()

    service = EvaluationService(db_session)
    first = service.run_backtest(
        period_start=target_match.match_date.replace(hour=0, minute=0),
        period_end=target_match.match_date.replace(hour=23, minute=59),
        code="eval-idempotency",
    )
    second = service.run_backtest(
        period_start=target_match.match_date.replace(hour=0, minute=0),
        period_end=target_match.match_date.replace(hour=23, minute=59),
        code="eval-idempotency",
    )

    runs = db_session.execute(select(EvaluationRun)).scalars().all()

    assert first["id"] == second["id"]
    assert len(runs) == 1
    assert runs[0].code == "eval-idempotency"
