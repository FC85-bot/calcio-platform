from __future__ import annotations

from pathlib import Path
import runpy

ROOT_DIR = Path(__file__).resolve().parent
SCRIPT_PATH = ROOT_DIR / "scripts" / "run_feature_snapshots.py"

if __name__ == "__main__":
    runpy.run_path(str(SCRIPT_PATH), run_name="__main__")
