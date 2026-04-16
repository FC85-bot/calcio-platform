from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import and_, cast, func, literal_column, or_, select
from sqlalchemy.orm import Session
from sqlalchemy.sql.sqltypes import Numeric

from app.models.bookmaker import Bookmaker
from app.models.market import Market
from app.models.odds import Odds
from app.models.provider import Provider


class OddsQueryService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def _resolve_market_filters(self, market_code: str | None) -> tuple[str | None, Decimal | None]:
        if market_code is None:
            return None, None

        normalized = market_code.upper()
        if normalized == "OU25":
            return "OU", Decimal("2.500")
        if normalized in {"1X2", "OU", "BTTS"}:
            return normalized, None
        raise ValueError(f"Unsupported market_code={market_code}")

    def _public_market_code(self, market_code: str, line_value: Decimal | None) -> str:
        if market_code == "OU" and line_value == Decimal("2.500"):
            return "OU25"
        return market_code

    def get_latest_odds(
        self, match_id: UUID, *, market_code: str | None = None, limit: int | None = None
    ) -> list[dict[str, Any]]:
        statement = self._ranked_odds_statement(
            match_id=match_id, order="desc", market_code=market_code
        )
        if limit is not None:
            statement = statement.limit(limit)
        return self._serialize_ranked_rows(statement)

    def get_opening_odds(
        self, match_id: UUID, *, market_code: str | None = None
    ) -> list[dict[str, Any]]:
        statement = self._ranked_odds_statement(
            match_id=match_id, order="asc", market_code=market_code
        )
        return self._serialize_ranked_rows(statement)

    def get_history_odds(
        self,
        match_id: UUID,
        *,
        market_code: str | None = None,
        bookmaker_id: UUID | None = None,
        selection_code: str | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        statement = self._base_history_statement(
            match_id=match_id,
            market_code=market_code,
            bookmaker_id=bookmaker_id,
            selection_code=selection_code,
        )
        statement = statement.order_by(
            Odds.snapshot_timestamp.desc(),
            Bookmaker.name.asc(),
            Market.code.asc(),
            Odds.selection_code.asc(),
        ).limit(limit)
        return self._serialize_rows(statement)

    def get_best_odds(
        self, match_id: UUID, *, market_code: str | None = None
    ) -> list[dict[str, Any]]:
        latest_subquery = self._latest_subquery(match_id=match_id, market_code=market_code)
        line_key = func.coalesce(
            latest_subquery.c.line_value, cast(literal_column("-999.000"), Numeric(10, 3))
        )
        best_ranked = select(
            latest_subquery,
            func.row_number()
            .over(
                partition_by=(
                    latest_subquery.c.market_id,
                    latest_subquery.c.selection_code,
                    line_key,
                ),
                order_by=(
                    latest_subquery.c.odds_value.desc(),
                    latest_subquery.c.snapshot_timestamp.desc(),
                    latest_subquery.c.bookmaker_name.asc(),
                    latest_subquery.c.id.desc(),
                ),
            )
            .label("best_rank"),
        ).subquery()
        statement = (
            select(best_ranked)
            .where(best_ranked.c.best_rank == 1)
            .order_by(
                best_ranked.c.market_code.asc(),
                best_ranked.c.selection_code.asc(),
                best_ranked.c.line_value.asc().nullsfirst(),
            )
        )
        return [
            self._serialize_row_mapping(row) for row in self.db.execute(statement).mappings().all()
        ]

    def get_latest_compact_map(self, match_ids: list[UUID]) -> dict[UUID, list[dict[str, Any]]]:
        if not match_ids:
            return {}

        latest_subquery = self._latest_subquery_for_matches(match_ids)
        rows = (
            self.db.execute(
                select(latest_subquery).order_by(
                    latest_subquery.c.match_id.asc(),
                    latest_subquery.c.bookmaker_name.asc(),
                    latest_subquery.c.snapshot_timestamp.desc(),
                )
            )
            .mappings()
            .all()
        )

        compact_map: dict[UUID, dict[tuple[UUID, UUID], dict[str, Any]]] = defaultdict(dict)
        for row in rows:
            key = (row["provider_id"], row["bookmaker_id"])
            bookmaker_bucket = compact_map[row["match_id"]].setdefault(
                key,
                {
                    "provider_id": row["provider_id"],
                    "provider_name": row["provider_name"],
                    "bookmaker_id": row["bookmaker_id"],
                    "bookmaker_name": row["bookmaker_name"],
                    "snapshot_timestamp": row["snapshot_timestamp"],
                    "home_win": None,
                    "draw": None,
                    "away_win": None,
                    "over_2_5": None,
                    "under_2_5": None,
                    "btts_yes": None,
                    "btts_no": None,
                },
            )
            if (
                row["snapshot_timestamp"]
                and row["snapshot_timestamp"] > bookmaker_bucket["snapshot_timestamp"]
            ):
                bookmaker_bucket["snapshot_timestamp"] = row["snapshot_timestamp"]

            market_code = row["market_code"]
            selection_code = row["selection_code"]
            line_value = row["line_value"]
            value = self._to_float(row["odds_value"])

            if market_code == "1X2":
                if selection_code == "HOME":
                    bookmaker_bucket["home_win"] = value
                elif selection_code == "DRAW":
                    bookmaker_bucket["draw"] = value
                elif selection_code == "AWAY":
                    bookmaker_bucket["away_win"] = value
            elif market_code == "OU" and line_value == Decimal("2.500"):
                if selection_code == "OVER":
                    bookmaker_bucket["over_2_5"] = value
                elif selection_code == "UNDER":
                    bookmaker_bucket["under_2_5"] = value
            elif market_code == "BTTS":
                if selection_code == "YES":
                    bookmaker_bucket["btts_yes"] = value
                elif selection_code == "NO":
                    bookmaker_bucket["btts_no"] = value

        return {match_id: list(bookmakers.values()) for match_id, bookmakers in compact_map.items()}

    def get_summary(self) -> dict[str, Any]:
        latest_snapshot_timestamp = self.db.execute(
            select(func.max(Odds.snapshot_timestamp))
        ).scalar_one()
        return {
            "total_snapshot_rows": int(
                self.db.execute(select(func.count(Odds.id))).scalar_one() or 0
            ),
            "matches_with_odds_count": int(
                self.db.execute(select(func.count(func.distinct(Odds.match_id)))).scalar_one() or 0
            ),
            "bookmakers_count": int(
                self.db.execute(select(func.count(func.distinct(Odds.bookmaker_id)))).scalar_one()
                or 0
            ),
            "markets_count": int(
                self.db.execute(select(func.count(func.distinct(Odds.market_id)))).scalar_one() or 0
            ),
            "latest_snapshot_timestamp": latest_snapshot_timestamp,
        }

    def get_quality(self) -> dict[str, int]:
        line_key = func.coalesce(Odds.line_value, cast(literal_column("-999.000"), Numeric(10, 3)))
        duplicate_snapshot_count = sum(
            int(row[0])
            for row in self.db.execute(
                select(func.count(Odds.id))
                .group_by(
                    Odds.match_id,
                    Odds.provider_id,
                    Odds.bookmaker_id,
                    Odds.market_id,
                    Odds.selection_code,
                    line_key,
                    Odds.snapshot_timestamp,
                    Odds.odds_value,
                )
                .having(func.count(Odds.id) > 1)
            ).all()
        )
        return {
            "duplicate_snapshot_count": duplicate_snapshot_count,
            "invalid_odds_value_count": int(
                self.db.execute(
                    select(func.count(Odds.id)).where(
                        or_(Odds.odds_value < 1.01, Odds.odds_value > 1000)
                    )
                ).scalar_one()
                or 0
            ),
            "invalid_line_value_count": int(
                self.db.execute(
                    select(func.count(Odds.id))
                    .join(Market, Market.id == Odds.market_id)
                    .where(and_(Market.code == "OU", Odds.line_value.is_(None)))
                ).scalar_one()
                or 0
            ),
            "missing_bookmaker_count": int(
                self.db.execute(
                    select(func.count(Odds.id))
                    .select_from(Odds)
                    .outerjoin(Bookmaker, Bookmaker.id == Odds.bookmaker_id)
                    .where(Bookmaker.id.is_(None))
                ).scalar_one()
                or 0
            ),
            "missing_market_count": int(
                self.db.execute(
                    select(func.count(Odds.id))
                    .select_from(Odds)
                    .outerjoin(Market, Market.id == Odds.market_id)
                    .where(Market.id.is_(None))
                ).scalar_one()
                or 0
            ),
            "missing_snapshot_timestamp_count": int(
                self.db.execute(
                    select(func.count(Odds.id)).where(Odds.snapshot_timestamp.is_(None))
                ).scalar_one()
                or 0
            ),
        }

    def _latest_subquery(self, match_id: UUID, market_code: str | None = None):
        ranked = self._ranked_subquery(match_id=match_id, order="desc", market_code=market_code)
        return select(ranked).where(ranked.c.rn == 1).subquery()

    def _latest_subquery_for_matches(self, match_ids: list[UUID]):
        line_key = func.coalesce(Odds.line_value, cast(literal_column("-999.000"), Numeric(10, 3)))
        ranked = (
            select(
                Odds.id,
                Odds.match_id,
                Odds.provider_id,
                Provider.name.label("provider_name"),
                Odds.bookmaker_id,
                Bookmaker.name.label("bookmaker_name"),
                Odds.market_id,
                Market.code.label("market_code"),
                Market.name.label("market_name"),
                Odds.selection_code,
                Odds.line_value,
                Odds.odds_value,
                Odds.snapshot_timestamp,
                Odds.ingested_at,
                func.row_number()
                .over(
                    partition_by=(
                        Odds.match_id,
                        Odds.provider_id,
                        Odds.bookmaker_id,
                        Odds.market_id,
                        Odds.selection_code,
                        line_key,
                    ),
                    order_by=(
                        Odds.snapshot_timestamp.desc(),
                        Odds.ingested_at.desc(),
                        Odds.id.desc(),
                    ),
                )
                .label("rn"),
            )
            .join(Provider, Provider.id == Odds.provider_id)
            .join(Bookmaker, Bookmaker.id == Odds.bookmaker_id)
            .join(Market, Market.id == Odds.market_id)
            .where(Odds.match_id.in_(match_ids))
            .subquery()
        )
        return select(ranked).where(ranked.c.rn == 1).subquery()

    def _ranked_odds_statement(self, *, match_id: UUID, order: str, market_code: str | None = None):
        ranked = self._ranked_subquery(match_id=match_id, order=order, market_code=market_code)
        return (
            select(ranked)
            .where(ranked.c.rn == 1)
            .order_by(
                ranked.c.bookmaker_name.asc(),
                ranked.c.market_code.asc(),
                ranked.c.selection_code.asc(),
                ranked.c.line_value.asc().nullsfirst(),
            )
        )

    def _ranked_subquery(self, *, match_id: UUID, order: str, market_code: str | None = None):
        resolved_market_code, required_line_value = self._resolve_market_filters(market_code)
        order_by = (
            (Odds.snapshot_timestamp.desc(), Odds.ingested_at.desc(), Odds.id.desc())
            if order == "desc"
            else (Odds.snapshot_timestamp.asc(), Odds.ingested_at.asc(), Odds.id.asc())
        )
        line_key = func.coalesce(Odds.line_value, cast(literal_column("-999.000"), Numeric(10, 3)))
        statement = (
            select(
                Odds.id,
                Odds.match_id,
                Odds.provider_id,
                Provider.name.label("provider_name"),
                Odds.bookmaker_id,
                Bookmaker.name.label("bookmaker_name"),
                Odds.market_id,
                Market.code.label("market_code"),
                Market.name.label("market_name"),
                Odds.selection_code,
                Odds.line_value,
                Odds.odds_value,
                Odds.snapshot_timestamp,
                Odds.ingested_at,
                func.row_number()
                .over(
                    partition_by=(
                        Odds.provider_id,
                        Odds.bookmaker_id,
                        Odds.market_id,
                        Odds.selection_code,
                        line_key,
                    ),
                    order_by=order_by,
                )
                .label("rn"),
            )
            .join(Provider, Provider.id == Odds.provider_id)
            .join(Bookmaker, Bookmaker.id == Odds.bookmaker_id)
            .join(Market, Market.id == Odds.market_id)
            .where(Odds.match_id == match_id)
        )
        if resolved_market_code is not None:
            statement = statement.where(Market.code == resolved_market_code)
        if required_line_value is not None:
            statement = statement.where(Odds.line_value == required_line_value)
        return statement.subquery()

    def _base_history_statement(
        self,
        *,
        match_id: UUID,
        market_code: str | None = None,
        bookmaker_id: UUID | None = None,
        selection_code: str | None = None,
    ):
        resolved_market_code, required_line_value = self._resolve_market_filters(market_code)
        statement = (
            select(
                Odds.id,
                Odds.match_id,
                Odds.provider_id,
                Provider.name.label("provider_name"),
                Odds.bookmaker_id,
                Bookmaker.name.label("bookmaker_name"),
                Odds.market_id,
                Market.code.label("market_code"),
                Market.name.label("market_name"),
                Odds.selection_code,
                Odds.line_value,
                Odds.odds_value,
                Odds.snapshot_timestamp,
                Odds.ingested_at,
            )
            .join(Provider, Provider.id == Odds.provider_id)
            .join(Bookmaker, Bookmaker.id == Odds.bookmaker_id)
            .join(Market, Market.id == Odds.market_id)
            .where(Odds.match_id == match_id)
        )
        if resolved_market_code is not None:
            statement = statement.where(Market.code == resolved_market_code)
        if required_line_value is not None:
            statement = statement.where(Odds.line_value == required_line_value)
        if bookmaker_id is not None:
            statement = statement.where(Odds.bookmaker_id == bookmaker_id)
        if selection_code is not None:
            statement = statement.where(Odds.selection_code == selection_code)
        return statement

    def _serialize_rows(self, statement) -> list[dict[str, Any]]:
        return [
            self._serialize_row_mapping(row) for row in self.db.execute(statement).mappings().all()
        ]

    def _serialize_ranked_rows(self, statement) -> list[dict[str, Any]]:
        return [
            self._serialize_row_mapping(row) for row in self.db.execute(statement).mappings().all()
        ]

    def _serialize_row_mapping(self, row: Any) -> dict[str, Any]:
        return {
            "id": row["id"],
            "match_id": row["match_id"],
            "provider_id": row["provider_id"],
            "provider_name": row["provider_name"],
            "bookmaker_id": row["bookmaker_id"],
            "bookmaker_name": row["bookmaker_name"],
            "market_id": row["market_id"],
            "market_code": self._public_market_code(row["market_code"], row["line_value"]),
            "market_name": row["market_name"],
            "selection_code": row["selection_code"],
            "line_value": self._to_float(row["line_value"]),
            "odds_value": self._to_float(row["odds_value"]),
            "snapshot_timestamp": row["snapshot_timestamp"],
            "ingested_at": row["ingested_at"],
        }

    def _to_float(self, value: Decimal | None) -> float | None:
        return float(value) if value is not None else None
