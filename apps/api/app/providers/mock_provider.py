from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

from app.providers.base import BaseProvider, ProviderFetchResult


class MockProvider(BaseProvider):
    name = "mock_provider"
    supports_odds = True

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._competition_external_id = "competition_serie_a_mock"
        self._season_external_id = "season_2026_2027"
        self._season = "2026/2027"
        self._teams = [
            {"external_id": "team_inter", "name": "Inter"},
            {"external_id": "team_milan", "name": "Milan"},
            {"external_id": "team_juventus", "name": "Juventus"},
            {"external_id": "team_napoli", "name": "Napoli"},
            {"external_id": "team_roma", "name": "Roma"},
            {"external_id": "team_atalanta", "name": "Atalanta"},
        ]
        self._bookmakers = [
            {"key": "mock_alpha", "name": "Mock Alpha"},
            {"key": "mock_beta", "name": "Mock Beta"},
        ]

    @property
    def _base_url(self) -> str:
        return "https://mock.invalid"

    def fetch_competitions(self) -> list[ProviderFetchResult]:
        items = self.get_competitions()
        payload = {"competitions": [self._competition_payload()]}
        return [
            ProviderFetchResult(
                entity_type="competitions",
                endpoint="/mock/competitions",
                payload=payload,
                items=items,
            )
        ]

    def fetch_seasons(self) -> list[ProviderFetchResult]:
        items = [
            {
                "external_id": self._season_external_id,
                "competition_external_id": self._competition_external_id,
                "competition_name": "Serie A Mock",
                "competition_country": "Italy",
                "name": self._season,
                "start_date": date(2026, 7, 1),
                "end_date": date(2027, 6, 30),
            }
        ]
        payload = {
            **self._competition_payload(),
            "seasons": [self._season_payload()],
        }
        return [
            ProviderFetchResult(
                entity_type="seasons",
                endpoint="/mock/seasons",
                payload=payload,
                items=items,
            )
        ]

    def fetch_teams(self) -> list[ProviderFetchResult]:
        items = self.get_teams()
        payload = {
            "competition": self._competition_payload(),
            "teams": [self._team_payload(team) for team in self._teams],
        }
        return [
            ProviderFetchResult(
                entity_type="teams",
                endpoint="/mock/teams",
                payload=payload,
                items=items,
            )
        ]

    def fetch_matches(self) -> list[ProviderFetchResult]:
        items = self.get_matches()
        payload = {
            "competition": self._competition_payload(),
            "matches": [self._match_payload(item) for item in items],
        }
        return [
            ProviderFetchResult(
                entity_type="matches",
                endpoint="/mock/matches",
                payload=payload,
                items=items,
            )
        ]

    def fetch_odds(self) -> list[ProviderFetchResult]:
        items, snapshots = self.get_odds()
        return [
            ProviderFetchResult(
                entity_type="odds",
                endpoint="/mock/odds",
                payload={
                    "provider_payload": snapshots,
                    "odds": self._serialize_items(items),
                },
                items=items,
            )
        ]

    def get_competitions(self) -> list[dict[str, Any]]:
        return [
            {
                "external_id": self._competition_external_id,
                "name": "Serie A Mock",
                "country": "Italy",
                "code": "SA",
            }
        ]

    def get_teams(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for team in self._teams:
            items.append(
                {
                    "external_id": team["external_id"],
                    "name": team["name"],
                    "competition_external_id": self._competition_external_id,
                    "competition_name": "Serie A Mock",
                    "competition_country": "Italy",
                }
            )
        return items

    def get_matches(self) -> list[dict[str, Any]]:
        base_date = datetime(2026, 8, 20, 18, 45, tzinfo=UTC)
        matches: list[dict[str, Any]] = []
        match_number = 1

        for home_index, home_team in enumerate(self._teams):
            for away_team in self._teams[home_index + 1 :]:
                match_date = base_date + timedelta(days=(match_number - 1) * 2)
                is_finished = match_number <= 8
                home_goals = (match_number + home_index) % 3 if is_finished else None
                away_goals = (match_number + len(away_team["name"])) % 2 if is_finished else None

                matches.append(
                    {
                        "external_id": f"match_{match_number:03d}",
                        "competition_external_id": self._competition_external_id,
                        "competition_name": "Serie A Mock",
                        "competition_country": "Italy",
                        "season_external_id": self._season_external_id,
                        "season_name": self._season,
                        "season_start_date": date(2026, 7, 1),
                        "season_end_date": date(2027, 6, 30),
                        "match_date": match_date,
                        "home_team_external_id": home_team["external_id"],
                        "home_team_name": home_team["name"],
                        "away_team_external_id": away_team["external_id"],
                        "away_team_name": away_team["name"],
                        "home_goals": home_goals,
                        "away_goals": away_goals,
                        "provider_status": "FINISHED" if is_finished else "SCHEDULED",
                    }
                )
                match_number += 1

        return matches

    def get_odds(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        odds_items: list[dict[str, Any]] = []
        snapshots: list[dict[str, Any]] = []
        for index, match in enumerate(self.get_matches(), start=1):
            base_timestamp = match["match_date"] - timedelta(days=1)
            for bookmaker_index, bookmaker in enumerate(self._bookmakers):
                for snapshot_offset in range(2):
                    snapshot_timestamp = base_timestamp + timedelta(
                        hours=snapshot_offset * 8 + bookmaker_index
                    )
                    price_shift = 0.03 * snapshot_offset + 0.05 * bookmaker_index
                    home_price = round(1.65 + (index % 4) * 0.18 + price_shift, 2)
                    draw_price = round(3.1 + (index % 3) * 0.12 + price_shift, 2)
                    away_price = round(2.05 + (index % 5) * 0.2 + price_shift, 2)
                    over_price = round(1.72 + (index % 4) * 0.08 + price_shift, 2)
                    under_price = round(1.88 + (index % 4) * 0.07 + price_shift, 2)
                    yes_price = round(1.68 + (index % 4) * 0.05 + price_shift, 2)
                    no_price = round(2.02 + (index % 4) * 0.06 + price_shift, 2)

                    snapshots.append(
                        {
                            "match_external_id": match["external_id"],
                            "bookmaker": bookmaker["name"],
                            "snapshot_timestamp": snapshot_timestamp.isoformat(),
                        }
                    )

                    odds_items.extend(
                        [
                            self._odds_item(
                                match,
                                bookmaker,
                                snapshot_timestamp,
                                "h2h",
                                match["home_team_name"],
                                home_price,
                            ),
                            self._odds_item(
                                match, bookmaker, snapshot_timestamp, "h2h", "Draw", draw_price
                            ),
                            self._odds_item(
                                match,
                                bookmaker,
                                snapshot_timestamp,
                                "h2h",
                                match["away_team_name"],
                                away_price,
                            ),
                            self._odds_item(
                                match,
                                bookmaker,
                                snapshot_timestamp,
                                "totals",
                                "Over",
                                over_price,
                                2.5,
                            ),
                            self._odds_item(
                                match,
                                bookmaker,
                                snapshot_timestamp,
                                "totals",
                                "Under",
                                under_price,
                                2.5,
                            ),
                            self._odds_item(
                                match, bookmaker, snapshot_timestamp, "btts", "Yes", yes_price
                            ),
                            self._odds_item(
                                match, bookmaker, snapshot_timestamp, "btts", "No", no_price
                            ),
                        ]
                    )
        return odds_items, snapshots

    def _competition_payload(self) -> dict[str, Any]:
        return {
            "id": self._competition_external_id,
            "name": "Serie A Mock",
            "code": "SA",
            "area": {"name": "Italy"},
        }

    def _season_payload(self) -> dict[str, Any]:
        return {
            "id": self._season_external_id,
            "startDate": date(2026, 7, 1).isoformat(),
            "endDate": date(2027, 6, 30).isoformat(),
        }

    def _team_payload(self, team: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": team["external_id"],
            "name": team["name"],
            "shortName": team["name"],
        }

    def _match_payload(self, match: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": match["external_id"],
            "utcDate": self._serialize_value(match["match_date"]),
            "status": match["provider_status"],
            "season": {
                "id": match["season_external_id"],
                "startDate": self._serialize_value(match["season_start_date"]),
                "endDate": self._serialize_value(match["season_end_date"]),
            },
            "homeTeam": {
                "id": match["home_team_external_id"],
                "name": match["home_team_name"],
                "shortName": match["home_team_name"],
            },
            "awayTeam": {
                "id": match["away_team_external_id"],
                "name": match["away_team_name"],
                "shortName": match["away_team_name"],
            },
            "score": {
                "fullTime": {
                    "home": match["home_goals"],
                    "away": match["away_goals"],
                }
            },
        }

    def _odds_item(
        self,
        match: dict[str, Any],
        bookmaker: dict[str, Any],
        snapshot_timestamp: datetime,
        market_key: str,
        selection_name: str,
        odds_value: float,
        line_value: float | None = None,
    ) -> dict[str, Any]:
        return {
            "match_external_id": match["external_id"],
            "sport_key": "soccer_italy_serie_a",
            "match_date": match["match_date"],
            "home_team_name": match["home_team_name"],
            "away_team_name": match["away_team_name"],
            "bookmaker_key": bookmaker["key"],
            "bookmaker_name": bookmaker["name"],
            "market_key": market_key,
            "selection_name": selection_name,
            "line_value": line_value,
            "odds_value": odds_value,
            "snapshot_timestamp": snapshot_timestamp,
        }

    def _serialize_value(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        return value

    def _serialize_items(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        serialized: list[dict[str, Any]] = []
        for item in items:
            serialized_item: dict[str, Any] = {}
            for key, value in item.items():
                serialized_item[key] = self._serialize_value(value)
            serialized.append(serialized_item)
        return serialized
