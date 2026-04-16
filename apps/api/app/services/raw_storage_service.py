from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from app.core.config import get_settings


@dataclass(slots=True)
class RawPayloadMetadata:
    raw_path: str
    payload_sha256: str
    payload_size_bytes: int
    payload_summary: dict[str, Any]


@dataclass(slots=True)
class PreparedRawPayload:
    file_path: Path
    payload_bytes: bytes
    metadata: RawPayloadMetadata


class RawStorageService:
    def __init__(self, base_path: Path | None = None) -> None:
        settings = get_settings()
        self.base_path = (base_path or settings.raw_storage_abs_path).resolve()

    def prepare_payload(
        self,
        *,
        provider: str,
        entity_type: str,
        run_id: UUID,
        payload: dict[str, Any],
    ) -> PreparedRawPayload:
        payload_bytes = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        payload_hash = hashlib.sha256(payload_bytes).hexdigest()
        now = datetime.now(UTC)
        directory = (
            self.base_path / provider / entity_type / f"{now:%Y}" / f"{now:%m}" / f"{now:%d}"
        )
        directory.mkdir(parents=True, exist_ok=True)
        file_path = directory / f"{run_id}_{payload_hash}.json"

        metadata = RawPayloadMetadata(
            raw_path=str(file_path),
            payload_sha256=payload_hash,
            payload_size_bytes=len(payload_bytes),
            payload_summary=self._build_payload_summary(payload),
        )
        return PreparedRawPayload(
            file_path=file_path, payload_bytes=payload_bytes, metadata=metadata
        )

    def save_prepared_payload(self, prepared_payload: PreparedRawPayload) -> RawPayloadMetadata:
        prepared_payload.file_path.write_bytes(prepared_payload.payload_bytes)
        return prepared_payload.metadata

    def save_payload(
        self,
        *,
        provider: str,
        entity_type: str,
        run_id: UUID,
        payload: dict[str, Any],
    ) -> RawPayloadMetadata:
        prepared_payload = self.prepare_payload(
            provider=provider,
            entity_type=entity_type,
            run_id=run_id,
            payload=payload,
        )
        return self.save_prepared_payload(prepared_payload)

    def _build_payload_summary(self, payload: dict[str, Any]) -> dict[str, Any]:
        summary: dict[str, Any] = {"storage": "file"}

        for candidate_key in (
            "count",
            "resultSet",
            "competitions",
            "teams",
            "matches",
            "seasons",
            "odds",
            "items",
        ):
            if candidate_key not in payload:
                continue
            value = payload[candidate_key]
            if isinstance(value, list):
                summary["item_count"] = len(value)
                summary["item_key"] = candidate_key
                break
            if isinstance(value, dict) and "count" in value:
                summary["item_count"] = value.get("count")
                summary["item_key"] = candidate_key
                break

        if "item_count" not in summary:
            summary["item_count"] = 1

        return summary
