from __future__ import annotations

from datetime import date, datetime
from uuid import UUID

from pydantic import BaseModel


class SeasonRead(BaseModel):
    id: UUID
    name: str
    start_date: date
    end_date: date
    created_at: datetime
