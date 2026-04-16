"""sprint10 odds append only layer

Revision ID: 20260415_0006
Revises: 20260414_0005
Create Date: 2026-04-15 09:10:00.000000

"""
from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "20260415_0006"
down_revision: str | None = "20260414_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


LEGACY_BOOKMAKER_EXTERNAL_PREFIX = "legacy_bookmaker:"
LEGACY_MARKET_MAP = {
    "h2h": ("1X2", "1X2"),
    "totals": ("OU", "Over/Under"),
    "btts": ("BTTS", "Both Teams To Score"),
}


def upgrade() -> None:
    op.drop_index("ix_odds_timestamp", table_name="odds")
    op.drop_index("ix_odds_provider_id", table_name="odds")
    op.drop_index("ix_odds_match_id", table_name="odds")
    op.rename_table("odds", "odds_legacy")

    op.create_table(
        "odds",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("match_id", sa.Uuid(), nullable=False),
        sa.Column("provider_id", sa.Uuid(), nullable=False),
        sa.Column("bookmaker_id", sa.Uuid(), nullable=False),
        sa.Column("market_id", sa.Uuid(), nullable=False),
        sa.Column("selection_code", sa.String(length=16), nullable=False),
        sa.Column("line_value", sa.Numeric(10, 3), nullable=True),
        sa.Column("odds_value", sa.Numeric(10, 4), nullable=False),
        sa.Column("snapshot_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "selection_code IN ('HOME', 'DRAW', 'AWAY', 'OVER', 'UNDER', 'YES', 'NO')",
            name="ck_odds_selection_code",
        ),
        sa.CheckConstraint("odds_value >= 1.01 AND odds_value <= 1000", name="ck_odds_value_range"),
        sa.ForeignKeyConstraint(["bookmaker_id"], ["bookmakers.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["market_id"], ["markets.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["match_id"], ["matches.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["provider_id"], ["providers.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "match_id",
            "provider_id",
            "bookmaker_id",
            "market_id",
            "selection_code",
            "line_value",
            "odds_value",
            "snapshot_timestamp",
            name="uq_odds_snapshot_identity",
        ),
    )
    op.create_index("ix_odds_match_id", "odds", ["match_id"], unique=False)
    op.create_index("ix_odds_provider_id", "odds", ["provider_id"], unique=False)
    op.create_index("ix_odds_bookmaker_id", "odds", ["bookmaker_id"], unique=False)
    op.create_index("ix_odds_market_id", "odds", ["market_id"], unique=False)
    op.create_index("ix_odds_snapshot_timestamp", "odds", ["snapshot_timestamp"], unique=False)
    op.create_index(
        "ix_odds_match_market_snapshot",
        "odds",
        ["match_id", "market_id", "snapshot_timestamp"],
        unique=False,
    )

    bind = op.get_bind()
    metadata = sa.MetaData()

    providers = sa.Table("providers", metadata, autoload_with=bind)
    provider_entities = sa.Table("provider_entities", metadata, autoload_with=bind)
    bookmakers = sa.Table("bookmakers", metadata, autoload_with=bind)
    markets = sa.Table("markets", metadata, autoload_with=bind)
    odds_legacy = sa.Table("odds_legacy", metadata, autoload_with=bind)
    odds = sa.Table("odds", metadata, autoload_with=bind)

    market_ids: dict[str, object] = {}
    for market_code, market_name in [("1X2", "1X2"), ("OU", "Over/Under"), ("BTTS", "Both Teams To Score")]:
        existing_market_id = bind.execute(
            sa.select(markets.c.id).where(markets.c.code == market_code)
        ).scalar_one_or_none()
        if existing_market_id is None:
            existing_market_id = uuid4()
            bind.execute(
                sa.insert(markets).values(
                    id=existing_market_id,
                    code=market_code,
                    name=market_name,
                )
            )
        market_ids[market_code] = existing_market_id

    provider_rows = bind.execute(
        sa.select(sa.distinct(odds_legacy.c.provider_id), providers.c.name)
        .select_from(odds_legacy.join(providers, providers.c.id == odds_legacy.c.provider_id))
    ).all()

    provider_bookmaker_ids: dict[object, object] = {}
    provider_market_mapping_inserts: list[dict[str, object]] = []
    now = datetime.now(timezone.utc)

    for provider_id, provider_name in provider_rows:
        bookmaker_name = str(provider_name)
        bookmaker_id = bind.execute(
            sa.select(bookmakers.c.id).where(bookmakers.c.name == bookmaker_name)
        ).scalar_one_or_none()
        if bookmaker_id is None:
            bookmaker_id = uuid4()
            bind.execute(sa.insert(bookmakers).values(id=bookmaker_id, name=bookmaker_name))
        provider_bookmaker_ids[provider_id] = bookmaker_id

        mapping_exists = bind.execute(
            sa.select(provider_entities.c.id).where(
                provider_entities.c.provider_id == provider_id,
                provider_entities.c.entity_type == "bookmaker",
                provider_entities.c.external_id == f"{LEGACY_BOOKMAKER_EXTERNAL_PREFIX}{provider_name}",
            )
        ).scalar_one_or_none()
        if mapping_exists is None:
            bind.execute(
                sa.insert(provider_entities).values(
                    id=uuid4(),
                    provider_id=provider_id,
                    entity_type="bookmaker",
                    external_id=f"{LEGACY_BOOKMAKER_EXTERNAL_PREFIX}{provider_name}",
                    internal_id=bookmaker_id,
                    created_at=now,
                )
            )

        for external_market_key, (market_code, _) in LEGACY_MARKET_MAP.items():
            existing_mapping = bind.execute(
                sa.select(provider_entities.c.id).where(
                    provider_entities.c.provider_id == provider_id,
                    provider_entities.c.entity_type == "market",
                    provider_entities.c.external_id == external_market_key,
                )
            ).scalar_one_or_none()
            if existing_mapping is None:
                provider_market_mapping_inserts.append(
                    {
                        "id": uuid4(),
                        "provider_id": provider_id,
                        "entity_type": "market",
                        "external_id": external_market_key,
                        "internal_id": market_ids[market_code],
                        "created_at": now,
                    }
                )

    if provider_market_mapping_inserts:
        bind.execute(sa.insert(provider_entities), provider_market_mapping_inserts)

    legacy_rows = bind.execute(sa.select(odds_legacy)).mappings().all()
    odds_rows: list[dict[str, object]] = []
    for row in legacy_rows:
        bookmaker_id = provider_bookmaker_ids[row["provider_id"]]
        snapshot_timestamp = row["timestamp"]
        odds_rows.extend(
            [
                _build_odds_row(
                    match_id=row["match_id"],
                    provider_id=row["provider_id"],
                    bookmaker_id=bookmaker_id,
                    market_id=market_ids["1X2"],
                    selection_code="HOME",
                    line_value=None,
                    odds_value=row["home_win"],
                    snapshot_timestamp=snapshot_timestamp,
                ),
                _build_odds_row(
                    match_id=row["match_id"],
                    provider_id=row["provider_id"],
                    bookmaker_id=bookmaker_id,
                    market_id=market_ids["1X2"],
                    selection_code="DRAW",
                    line_value=None,
                    odds_value=row["draw"],
                    snapshot_timestamp=snapshot_timestamp,
                ),
                _build_odds_row(
                    match_id=row["match_id"],
                    provider_id=row["provider_id"],
                    bookmaker_id=bookmaker_id,
                    market_id=market_ids["1X2"],
                    selection_code="AWAY",
                    line_value=None,
                    odds_value=row["away_win"],
                    snapshot_timestamp=snapshot_timestamp,
                ),
            ]
        )
        if row["over_2_5"] is not None:
            odds_rows.append(
                _build_odds_row(
                    match_id=row["match_id"],
                    provider_id=row["provider_id"],
                    bookmaker_id=bookmaker_id,
                    market_id=market_ids["OU"],
                    selection_code="OVER",
                    line_value=Decimal("2.500"),
                    odds_value=row["over_2_5"],
                    snapshot_timestamp=snapshot_timestamp,
                )
            )
        if row["under_2_5"] is not None:
            odds_rows.append(
                _build_odds_row(
                    match_id=row["match_id"],
                    provider_id=row["provider_id"],
                    bookmaker_id=bookmaker_id,
                    market_id=market_ids["OU"],
                    selection_code="UNDER",
                    line_value=Decimal("2.500"),
                    odds_value=row["under_2_5"],
                    snapshot_timestamp=snapshot_timestamp,
                )
            )

    if odds_rows:
        bind.execute(sa.insert(odds), odds_rows)

    op.drop_table("odds_legacy")


def downgrade() -> None:
    bind = op.get_bind()
    metadata = sa.MetaData()
    odds = sa.Table("odds", metadata, autoload_with=bind)
    markets = sa.Table("markets", metadata, autoload_with=bind)

    op.create_table(
        "odds_legacy_restore",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("match_id", sa.Uuid(), nullable=False),
        sa.Column("provider_id", sa.Uuid(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("home_win", sa.Float(), nullable=False),
        sa.Column("draw", sa.Float(), nullable=False),
        sa.Column("away_win", sa.Float(), nullable=False),
        sa.Column("over_2_5", sa.Float(), nullable=True),
        sa.Column("under_2_5", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(["match_id"], ["matches.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["provider_id"], ["providers.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("match_id", "provider_id", "timestamp", name="uq_odds_match_provider_timestamp"),
    )
    restored = sa.Table("odds_legacy_restore", metadata, autoload_with=bind)

    rows = bind.execute(
        sa.select(
            odds.c.match_id,
            odds.c.provider_id,
            odds.c.snapshot_timestamp,
            markets.c.code.label("market_code"),
            odds.c.selection_code,
            odds.c.line_value,
            odds.c.odds_value,
        ).join(markets, markets.c.id == odds.c.market_id)
    ).mappings().all()

    grouped: dict[tuple[object, object, object], dict[str, object]] = {}
    for row in rows:
        key = (row["match_id"], row["provider_id"], row["snapshot_timestamp"])
        bucket = grouped.setdefault(
            key,
            {
                "id": uuid4(),
                "match_id": row["match_id"],
                "provider_id": row["provider_id"],
                "timestamp": row["snapshot_timestamp"],
                "home_win": None,
                "draw": None,
                "away_win": None,
                "over_2_5": None,
                "under_2_5": None,
            },
        )
        value = float(row["odds_value"])
        if row["market_code"] == "1X2":
            if row["selection_code"] == "HOME":
                bucket["home_win"] = value
            elif row["selection_code"] == "DRAW":
                bucket["draw"] = value
            elif row["selection_code"] == "AWAY":
                bucket["away_win"] = value
        elif row["market_code"] == "OU" and row["line_value"] == Decimal("2.500"):
            if row["selection_code"] == "OVER":
                bucket["over_2_5"] = value
            elif row["selection_code"] == "UNDER":
                bucket["under_2_5"] = value

    insert_rows = [
        row
        for row in grouped.values()
        if row["home_win"] is not None and row["draw"] is not None and row["away_win"] is not None
    ]
    if insert_rows:
        bind.execute(sa.insert(restored), insert_rows)

    op.drop_index("ix_odds_match_market_snapshot", table_name="odds")
    op.drop_index("ix_odds_snapshot_timestamp", table_name="odds")
    op.drop_index("ix_odds_market_id", table_name="odds")
    op.drop_index("ix_odds_bookmaker_id", table_name="odds")
    op.drop_index("ix_odds_provider_id", table_name="odds")
    op.drop_index("ix_odds_match_id", table_name="odds")
    op.drop_table("odds")
    op.rename_table("odds_legacy_restore", "odds")
    op.create_index("ix_odds_match_id", "odds", ["match_id"], unique=False)
    op.create_index("ix_odds_provider_id", "odds", ["provider_id"], unique=False)
    op.create_index("ix_odds_timestamp", "odds", ["timestamp"], unique=False)


def _build_odds_row(
    *,
    match_id: object,
    provider_id: object,
    bookmaker_id: object,
    market_id: object,
    selection_code: str,
    line_value: Decimal | None,
    odds_value: object,
    snapshot_timestamp: object,
) -> dict[str, object]:
    return {
        "id": uuid4(),
        "match_id": match_id,
        "provider_id": provider_id,
        "bookmaker_id": bookmaker_id,
        "market_id": market_id,
        "selection_code": selection_code,
        "line_value": line_value,
        "odds_value": Decimal(str(odds_value)).quantize(Decimal("0.0001")),
        "snapshot_timestamp": snapshot_timestamp,
    }
