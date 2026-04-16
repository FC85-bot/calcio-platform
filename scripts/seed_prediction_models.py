from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
API_DIR = ROOT_DIR / "apps" / "api"
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

from app.core.logging import configure_logging, get_logger
from app.db.session import SessionLocal
from app.services.prediction_service import seed_prediction_model_registry

configure_logging()
logger = get_logger(__name__)


def main() -> int:
    with SessionLocal() as db:
        result = seed_prediction_model_registry(db)

    logger.info("prediction_model_seed_completed", extra=result)
    print("Prediction model seed completed:")
    for key, value in result.items():
        print(f"- {key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
