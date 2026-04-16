from __future__ import annotations

from datetime import date, datetime
from typing import Any

from app.core.logging import get_logger
from app.providers.base import BaseProvider, ProviderFetchResult

logger = get_logger(__name__)


class FootballDataProvider(BaseProvider):
    name = "football_data"
    supports_odds = False

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._competition_cache: list[dict[str, Any]] | None = None

    @property
    def _base_url(self) -> str:
        return self.settings.football_data_base_url

    def fetch_competitions(self) -> list[ProviderFetchResult]:
        _, preloaded_results = self._ensure_competition_cache()
        return preloaded_results

    def fetch_seasons(self) -> list[ProviderFetchResult]:
        competitions, preloaded_results = self._ensure_competition_cache()
        results: list[ProviderFetchResult] = list(preloaded_results)
        for competition in competitions:
            competition_identifier = self._competition_identifier(competition)
            endpoint = f"/competitions/{competition_identifier}"
            payload, metadata = self._request(endpoint=endpoint)
            seasons = payload.get("seasons") or []
            if not seasons and payload.get("currentSeason"):
                seasons = [payload["currentSeason"]]

            items = [
                self._normalize_season_item(
                    competition=competition,
                    payload=payload,
                    season=season,
                )
                for season in seasons
                if season.get("id") is not None
                and season.get("startDate")
                and season.get("endDate")
            ]

            results.append(
                ProviderFetchResult(
                    entity_type="seasons",
                    endpoint=endpoint,
                    payload=payload,
                    items=items,
                    request_params=None,
                    response_metadata=metadata,
                )
            )
        return results

    def fetch_teams(self) -> list[ProviderFetchResult]:
        competitions, preloaded_results = self._ensure_competition_cache()
        results: list[ProviderFetchResult] = list(preloaded_results)
        for competition in competitions:
            competition_identifier = self._competition_identifier(competition)
            endpoint = f"/competitions/{competition_identifier}/teams"
            params = self._season_params()
            payload, metadata = self._request(endpoint=endpoint, params=params)

            items = [
                {
                    "external_id": str(team["id"]),
                    "name": team.get("name") or team.get("shortName") or str(team["id"]),
                    "competition_external_id": str(competition["id"]),
                    "competition_name": payload.get("competition", {}).get("name")
                    or competition.get("name"),
                    "competition_country": (
                        self._competition_country(payload)
                        or self._competition_country(competition)
                        or "Unknown"
                    ),
                }
                for team in payload.get("teams", [])
                if team.get("id") is not None
            ]

            results.append(
                ProviderFetchResult(
                    entity_type="teams",
                    endpoint=endpoint,
                    payload=payload,
                    items=items,
                    request_params=params,
                    response_metadata=metadata,
                )
            )
        return results

    def fetch_matches(self) -> list[ProviderFetchResult]:
        competitions, preloaded_results = self._ensure_competition_cache()
        results: list[ProviderFetchResult] = list(preloaded_results)
        for competition in competitions:
            competition_identifier = self._competition_identifier(competition)
            endpoint = f"/competitions/{competition_identifier}/matches"
            params = self._season_params()
            payload, metadata = self._request(endpoint=endpoint, params=params)

            items = [
                self._normalize_match_item(
                    competition=competition,
                    payload=payload,
                    match=match,
                )
                for match in payload.get("matches", [])
                if match.get("id") is not None and match.get("homeTeam") and match.get("awayTeam")
            ]

            results.append(
                ProviderFetchResult(
                    entity_type="matches",
                    endpoint=endpoint,
                    payload=payload,
                    items=items,
                    request_params=params,
                    response_metadata=metadata,
                )
            )
        return results

    def fetch_odds(self) -> list[ProviderFetchResult]:
        raise NotImplementedError(
            "football-data.org v4 exposes competitions, seasons, teams and matches, but not bookmaker odds."
        )

    def _request(
        self,
        *,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if not self.settings.football_data_api_key:
            raise ValueError("Missing FOOTBALL_DATA_API_KEY configuration")

        headers = {"X-Auth-Token": self.settings.football_data_api_key}
        payload, metadata = self._get_json(endpoint=endpoint, params=params, headers=headers)
        logger.info(
            "provider_request_succeeded",
            extra={
                "provider": self.name,
                "endpoint": endpoint,
                "status_code": metadata.get("status_code"),
                "latency_ms": metadata.get("latency_ms"),
                "attempt": metadata.get("attempt"),
            },
        )
        return payload, metadata

    def _ensure_competition_cache(self) -> tuple[list[dict[str, Any]], list[ProviderFetchResult]]:
        if self._competition_cache is not None:
            return self._competition_cache, []

        payload, metadata = self._request(endpoint="/competitions")
        competitions = self._extract_target_competitions(payload)
        self._competition_cache = competitions
        results = [
            ProviderFetchResult(
                entity_type="competitions",
                endpoint="/competitions",
                payload=payload,
                items=[self._normalize_competition_item(item) for item in competitions],
                request_params=None,
                response_metadata=metadata,
            )
        ]
        return competitions, results

    def _extract_target_competitions(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        competitions = payload.get("competitions") or []
        configured_codes = {
            code.strip().upper()
            for code in self.settings.football_data_competition_codes
            if code.strip()
        }
        if not configured_codes:
            return [item for item in competitions if item.get("id") is not None]
        return [
            item
            for item in competitions
            if item.get("id") is not None and str(item.get("code", "")).upper() in configured_codes
        ]

    def _competition_identifier(self, competition: dict[str, Any]) -> str:
        code = competition.get("code")
        if code:
            return str(code)
        return str(competition["id"])

    def _season_params(self) -> dict[str, Any] | None:
        if self.settings.football_data_season_year is None:
            return None
        return {"season": self.settings.football_data_season_year}

    def _normalize_competition_item(self, item: dict[str, Any]) -> dict[str, Any]:
        return {
            "external_id": str(item["id"]),
            "name": item.get("name") or str(item["id"]),
            "country": self._competition_country(item) or "Unknown",
            "code": item.get("code"),
        }

    def _normalize_season_item(
        self,
        *,
        competition: dict[str, Any],
        payload: dict[str, Any],
        season: dict[str, Any],
    ) -> dict[str, Any]:
        start_date = self._parse_date(season["startDate"])
        end_date = self._parse_date(season["endDate"])
        return {
            "external_id": str(season["id"]),
            "competition_external_id": str(payload.get("id") or competition["id"]),
            "competition_name": payload.get("name") or competition.get("name"),
            "competition_country": (
                self._competition_country(payload)
                or self._competition_country(competition)
                or "Unknown"
            ),
            "name": self._build_season_name(start_date, end_date),
            "start_date": start_date,
            "end_date": end_date,
        }

    def _normalize_match_item(
        self,
        *,
        competition: dict[str, Any],
        payload: dict[str, Any],
        match: dict[str, Any],
    ) -> dict[str, Any]:
        season = match.get("season") or {}
        season_start_date = (
            self._parse_date(season["startDate"]) if season.get("startDate") else None
        )
        season_end_date = self._parse_date(season["endDate"]) if season.get("endDate") else None
        season_name = None
        if season_start_date and season_end_date:
            season_name = self._build_season_name(season_start_date, season_end_date)

        score = match.get("score") or {}
        full_time = score.get("fullTime") or {}
        home_team = match.get("homeTeam") or {}
        away_team = match.get("awayTeam") or {}

        return {
            "external_id": str(match["id"]),
            "competition_external_id": str(
                payload.get("competition", {}).get("id") or competition["id"]
            ),
            "competition_name": payload.get("competition", {}).get("name")
            or competition.get("name"),
            "competition_country": (
                self._competition_country(payload.get("competition", {}))
                or self._competition_country(competition)
                or "Unknown"
            ),
            "season_external_id": str(season["id"]) if season.get("id") is not None else None,
            "season_name": season_name,
            "season_start_date": season_start_date,
            "season_end_date": season_end_date,
            "match_date": self._parse_datetime(match["utcDate"]),
            "home_team_external_id": str(home_team["id"]),
            "home_team_name": home_team.get("name")
            or home_team.get("shortName")
            or str(home_team["id"]),
            "away_team_external_id": str(away_team["id"]),
            "away_team_name": away_team.get("name")
            or away_team.get("shortName")
            or str(away_team["id"]),
            "home_goals": full_time.get("home"),
            "away_goals": full_time.get("away"),
            "provider_status": str(match.get("status") or "SCHEDULED"),
        }

    def _competition_country(self, payload: dict[str, Any]) -> str:
        area = payload.get("area") or {}
        return area.get("name") or payload.get("country") or ""

    def _parse_date(self, raw_value: str) -> date:
        return date.fromisoformat(raw_value[:10])

    def _parse_datetime(self, raw_value: str) -> datetime:
        normalized_value = raw_value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized_value)

    def _build_season_name(self, start_date: date, end_date: date) -> str:
        return f"{start_date.year}/{end_date.year}"
