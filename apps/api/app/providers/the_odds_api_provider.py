from __future__ import annotations

from typing import Any

from app.core.logging import get_logger
from app.providers.base import BaseProvider, ProviderFetchResult

logger = get_logger(__name__)


class TheOddsApiProvider(BaseProvider):
    name = "the_odds_api"
    supports_odds = True

    @property
    def _base_url(self) -> str:
        return self.settings.the_odds_api_base_url

    def fetch_competitions(self) -> list[ProviderFetchResult]:
        return []

    def fetch_seasons(self) -> list[ProviderFetchResult]:
        return []

    def fetch_teams(self) -> list[ProviderFetchResult]:
        return []

    def fetch_matches(self) -> list[ProviderFetchResult]:
        return []

    def fetch_odds(self) -> list[ProviderFetchResult]:
        if not self.settings.the_odds_api_api_key:
            raise ValueError("Missing THE_ODDS_API_API_KEY configuration")

        results: list[ProviderFetchResult] = []
        for sport_key in self.settings.the_odds_api_sport_keys:
            sport_key = sport_key.strip()
            if not sport_key:
                continue

            primary_payload, primary_metadata = self._request(
                endpoint=f"/sports/{sport_key}/odds",
                params=self._build_params(markets=["h2h", "totals"]),
            )
            primary_odds = self._extract_odds_items(sport_key=sport_key, events=primary_payload)
            results.append(
                ProviderFetchResult(
                    entity_type="odds",
                    endpoint=f"/sports/{sport_key}/odds",
                    payload={
                        "sport_key": sport_key,
                        "provider_payload": primary_payload,
                        "response_metadata": primary_metadata,
                        "odds": primary_odds,
                    },
                    items=primary_odds,
                    request_params=self._build_params(markets=["h2h", "totals"]),
                    response_metadata=primary_metadata,
                )
            )

            for event in primary_payload:
                event_id = event.get("id")
                if not event_id:
                    continue
                event_payload, event_metadata = self._request(
                    endpoint=f"/sports/{sport_key}/events/{event_id}/odds",
                    params=self._build_params(markets=["btts"]),
                )
                event_odds = self._extract_odds_items(
                    sport_key=sport_key,
                    events=[event_payload] if isinstance(event_payload, dict) else event_payload,
                )
                if not event_odds:
                    continue
                results.append(
                    ProviderFetchResult(
                        entity_type="odds",
                        endpoint=f"/sports/{sport_key}/events/{event_id}/odds",
                        payload={
                            "sport_key": sport_key,
                            "provider_payload": event_payload,
                            "response_metadata": event_metadata,
                            "odds": event_odds,
                        },
                        items=event_odds,
                        request_params=self._build_params(markets=["btts"]),
                        response_metadata=event_metadata,
                    )
                )
        return results

    def _request(self, *, endpoint: str, params: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
        payload, metadata = self._get_json(endpoint=endpoint, params=params)
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

    def _build_params(self, *, markets: list[str]) -> dict[str, Any]:
        params: dict[str, Any] = {
            "apiKey": self.settings.the_odds_api_api_key,
            "markets": ",".join(markets),
            "oddsFormat": "decimal",
            "dateFormat": "iso",
        }
        bookmakers = [
            item.strip() for item in self.settings.the_odds_api_bookmakers if item.strip()
        ]
        if bookmakers:
            params["bookmakers"] = ",".join(bookmakers)
        else:
            regions = [item.strip() for item in self.settings.the_odds_api_regions if item.strip()]
            params["regions"] = ",".join(regions or ["eu"])
        return params

    def _extract_odds_items(
        self, *, sport_key: str, events: list[dict[str, Any]] | dict[str, Any]
    ) -> list[dict[str, Any]]:
        if isinstance(events, dict):
            iterable = [events]
        else:
            iterable = events

        items: list[dict[str, Any]] = []
        for event in iterable:
            home_team = event.get("home_team")
            away_team = event.get("away_team")
            commence_time = event.get("commence_time")
            event_id = event.get("id")
            if not home_team or not away_team or not commence_time or not event_id:
                continue

            for bookmaker in event.get("bookmakers") or []:
                bookmaker_key = bookmaker.get("key")
                bookmaker_name = bookmaker.get("title")
                snapshot_timestamp = (
                    bookmaker.get("last_update") or event.get("last_update") or commence_time
                )
                if not bookmaker_key or not bookmaker_name or not snapshot_timestamp:
                    continue
                for market in bookmaker.get("markets") or []:
                    market_key = market.get("key")
                    if not market_key:
                        continue
                    for outcome in market.get("outcomes") or []:
                        price = outcome.get("price")
                        if price is None:
                            continue
                        items.append(
                            {
                                "match_external_id": str(event_id),
                                "sport_key": sport_key,
                                "match_date": commence_time,
                                "home_team_name": home_team,
                                "away_team_name": away_team,
                                "bookmaker_key": str(bookmaker_key),
                                "bookmaker_name": str(bookmaker_name),
                                "market_key": str(market_key),
                                "selection_name": str(outcome.get("name") or ""),
                                "line_value": outcome.get("point"),
                                "odds_value": price,
                                "snapshot_timestamp": snapshot_timestamp,
                            }
                        )
        return items
