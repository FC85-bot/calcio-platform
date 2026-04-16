from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any

CANONICAL_MARKET_DEFINITIONS: dict[str, dict[str, Any]] = {
    "1X2": {
        "name": "1X2",
        "requires_line": False,
        "allowed_selection_codes": {"HOME", "DRAW", "AWAY"},
    },
    "OU": {
        "name": "Over/Under",
        "requires_line": True,
        "allowed_selection_codes": {"OVER", "UNDER"},
    },
    "BTTS": {
        "name": "Both Teams To Score",
        "requires_line": False,
        "allowed_selection_codes": {"YES", "NO"},
    },
}

SUPPORTED_SELECTION_CODES = {"HOME", "DRAW", "AWAY", "OVER", "UNDER", "YES", "NO"}

THE_ODDS_API_MARKET_MAP = {
    "h2h": "1X2",
    "h2h_3_way": "1X2",
    "totals": "OU",
    "btts": "BTTS",
}

GENERIC_MARKET_MAP = {
    "1x2": "1X2",
    "match_result": "1X2",
    "h2h": "1X2",
    "h2h_3_way": "1X2",
    "ou": "OU",
    "over_under": "OU",
    "ou_2_5": "OU",
    "totals": "OU",
    "btts": "BTTS",
    "both_teams_to_score": "BTTS",
}


def provider_market_to_canonical(*, provider_name: str, market_key: str) -> str | None:
    normalized_market_key = normalize_provider_token(market_key)
    if provider_name == "the_odds_api":
        return THE_ODDS_API_MARKET_MAP.get(normalized_market_key)
    return GENERIC_MARKET_MAP.get(normalized_market_key) or CANONICAL_MARKET_DEFINITIONS.get(
        market_key, {}
    ).get("code")


def provider_selection_to_canonical(
    *,
    canonical_market_code: str,
    selection_name: str,
    home_team_name: str | None,
    away_team_name: str | None,
) -> str | None:
    normalized_selection = normalize_provider_token(selection_name)
    home_key = normalize_name_key(home_team_name)
    away_key = normalize_name_key(away_team_name)

    if canonical_market_code == "1X2":
        if normalized_selection in {"draw", "tie", "x"}:
            return "DRAW"
        if normalized_selection == home_key:
            return "HOME"
        if normalized_selection == away_key:
            return "AWAY"
        if normalized_selection in {"home", "1"}:
            return "HOME"
        if normalized_selection in {"away", "2"}:
            return "AWAY"
        return None

    if canonical_market_code == "OU":
        if normalized_selection in {"over", "o"}:
            return "OVER"
        if normalized_selection in {"under", "u"}:
            return "UNDER"
        return None

    if canonical_market_code == "BTTS":
        if normalized_selection in {"yes", "y"}:
            return "YES"
        if normalized_selection in {"no", "n"}:
            return "NO"
        return None

    return None


def normalize_line_value(*, canonical_market_code: str, raw_line_value: Any) -> Decimal | None:
    if canonical_market_code == "OU":
        if raw_line_value in (None, "", "null"):
            return None
        return coerce_decimal(raw_line_value, places="0.001")
    return None


def validate_line_value(*, canonical_market_code: str, line_value: Decimal | None) -> bool:
    definition = CANONICAL_MARKET_DEFINITIONS[canonical_market_code]
    if definition["requires_line"]:
        return line_value is not None
    return line_value is None


def validate_odds_value(odds_value: Decimal) -> bool:
    return Decimal("1.01") <= odds_value <= Decimal("1000")


def normalize_provider_token(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")


def normalize_name_key(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


def normalize_bookmaker_name(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", value.strip())
    if not cleaned:
        raise ValueError("bookmaker_name_missing")
    return cleaned


def coerce_decimal(value: Any, *, places: str) -> Decimal:
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid_decimal_value={value}") from exc
    return decimal_value.quantize(Decimal(places))
