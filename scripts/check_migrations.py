from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

ROOT_DIR = Path(__file__).resolve().parents[1]
API_DIR = ROOT_DIR / "apps" / "api"
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

ALEMBIC_INI = API_DIR / "alembic.ini"
VERSIONS_DIR = API_DIR / "alembic" / "versions"


def _import_migration_modules() -> list[str]:
    imported: list[str] = []
    for path in sorted(VERSIONS_DIR.glob("*.py")):
        if path.name == "__init__.py":
            continue

        module_name = f"migration_check_{path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Unable to load migration module: {path.name}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        revision = getattr(module, "revision", None)
        if not revision:
            raise RuntimeError(f"Migration {path.name} does not define revision")
        imported.append(str(revision))
    return imported


def main() -> int:
    config = Config(str(ALEMBIC_INI))
    script = ScriptDirectory.from_config(config)

    heads = script.get_heads()
    if not heads:
        raise RuntimeError("Alembic has no migration heads")
    if len(heads) != 1:
        raise RuntimeError(
            f"Expected a single Alembic head, found {len(heads)}: {heads}"
        )

    imported = _import_migration_modules()
    revisions = list(script.walk_revisions(base="base", head="heads"))
    if not revisions:
        raise RuntimeError("Alembic revision graph is empty")

    revision_ids = {revision.revision for revision in revisions}
    missing = sorted(set(imported) - revision_ids)
    if missing:
        raise RuntimeError(f"Imported migrations missing from Alembic graph: {missing}")

    print(
        "migration_check_ok",
        {
            "head": heads[0],
            "revisions": len(revisions),
            "imported_modules": len(imported),
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
