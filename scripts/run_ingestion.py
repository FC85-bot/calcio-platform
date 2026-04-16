from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

from sqlalchemy import select

ROOT_DIR = Path(__file__).resolve().parents[1]
API_DIR = ROOT_DIR / "apps" / "api"
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

from app.core.logging import configure_logging, get_logger
from app.db.session import SessionLocal
from app.models.bookmaker import Bookmaker
from app.models.competition import Competition
from app.models.competition_season import CompetitionSeason
from app.models.market import Market
from app.models.season import Season
from app.providers.mock_provider import MockProvider
from app.services.ingestion_service import IngestionService

configure_logging()
logger = get_logger(__name__)


MARKET_SEEDS: tuple[tuple[str, str], ...] = (
    ("1X2", "1X2"),
    ("OU", "Over/Under"),
    ("BTTS", "Both Teams To Score"),
)
BOOKMAKER_NAME = "MockBook"


def _parse_season_bounds(season_name: str) -> tuple[date, date]:
    try:
        start_year_text, end_year_text = season_name.split("/", maxsplit=1)
        start_year = int(start_year_text)
        end_year = int(end_year_text)
        return date(start_year, 7, 1), date(end_year, 6, 30)
    except (ValueError, TypeError):
        today = date.today()
        return today.replace(month=1, day=1), today.replace(month=12, day=31)


def _extract_season_name(match_items: list[dict]) -> str:
    if not match_items:
        raise ValueError("Mock provider returned no match items")

    first_match = match_items[0]
    season_name = first_match.get("season_name") or first_match.get("season")
    if not season_name:
        raise KeyError("season_name")

    return season_name


def bootstrap_sprint5_core(db, provider: MockProvider) -> dict[str, int]:
    competition_items = provider.get_competitions()
    if not competition_items:
        raise ValueError("Mock provider returned no competitions")

    competition_item = competition_items[0]
    match_items = provider.get_matches()
    season_name = _extract_season_name(match_items)
    season_start, season_end = _parse_season_bounds(season_name)

    with db.begin():
        competition = db.execute(
            select(Competition).where(
                Competition.name == competition_item["name"],
                Competition.country == competition_item["country"],
            )
        ).scalar_one_or_none()

        if competition is None:
            raise ValueError(
                f"Competition not found after ingestion: "
                f"name={competition_item['name']} country={competition_item['country']}"
            )

        season = db.execute(
            select(Season).where(Season.name == season_name)
        ).scalar_one_or_none()
        seasons_created = 0
        if season is None:
            season = Season(
                name=season_name, start_date=season_start, end_date=season_end
            )
            db.add(season)
            db.flush()
            seasons_created = 1

        link = db.execute(
            select(CompetitionSeason).where(
                CompetitionSeason.competition_id == competition.id,
                CompetitionSeason.season_id == season.id,
            )
        ).scalar_one_or_none()
        competition_seasons_created = 0
        if link is None:
            db.add(
                CompetitionSeason(competition_id=competition.id, season_id=season.id)
            )
            competition_seasons_created = 1

        bookmaker = db.execute(
            select(Bookmaker).where(Bookmaker.name == BOOKMAKER_NAME)
        ).scalar_one_or_none()
        bookmakers_created = 0
        if bookmaker is None:
            db.add(Bookmaker(name=BOOKMAKER_NAME))
            bookmakers_created = 1

        markets_created = 0
        for code, name in MARKET_SEEDS:
            market = db.execute(
                select(Market).where(Market.code == code)
            ).scalar_one_or_none()
            if market is None:
                db.add(Market(code=code, name=name))
                markets_created += 1

    return {
        "seasons_created": seasons_created,
        "competition_seasons_created": competition_seasons_created,
        "bookmakers_created": bookmakers_created,
        "markets_created": markets_created,
    }


def main() -> int:
    provider = MockProvider()
    with SessionLocal() as db:
        service = IngestionService(db=db, provider=provider)
        results = service.run_full_ingestion(include_odds=True)
        bootstrap_results = bootstrap_sprint5_core(db=db, provider=provider)

    combined_results = {**results, **bootstrap_results}
    logger.info("ingestion_script_completed", extra=combined_results)

    print("Ingestion completed:")
    for key, value in combined_results.items():
        print(f"- {key}: {value}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
