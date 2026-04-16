from __future__ import annotations

from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]

FORBIDDEN_DIR_NAMES = {
    ".venv",
    ".pytest_cache",
    ".ruff_cache",
    ".next",
    "node_modules",
    "__pycache__",
    "logs",
}

FORBIDDEN_FILE_NAMES = {
    "package-lock.json",
    "tsconfig.tsbuildinfo",
}

FORBIDDEN_SUFFIXES = {
    ".db",
    ".sqlite",
    ".sqlite3",
    ".log",
}

ALLOWED_DOTENV_PATHS = {
    ".env.example",
    "apps/web/.env.example",
}

ALLOWED_PACKAGE_LOCK_PATHS = {
    "apps/web/package-lock.json",
}


def _is_forbidden_env_file(path: Path, relative: str) -> bool:
    return (
        path.is_file()
        and path.name.startswith(".env")
        and relative not in ALLOWED_DOTENV_PATHS
    )


def _is_forbidden_package_artifact(path: Path, relative: str) -> bool:
    return path.name.endswith(".egg-info") or ".egg-info/" in relative


def _is_forbidden_named_file(path: Path, relative: str) -> bool:
    if not path.is_file():
        return False

    if path.name not in FORBIDDEN_FILE_NAMES:
        return False

    if path.name == "package-lock.json" and relative in ALLOWED_PACKAGE_LOCK_PATHS:
        return False

    return True


def _is_forbidden_suffix_file(path: Path) -> bool:
    return path.is_file() and path.suffix in FORBIDDEN_SUFFIXES


def main() -> int:
    violations: list[str] = []

    for path in ROOT_DIR.rglob("*"):
        relative = path.relative_to(ROOT_DIR).as_posix()

        if relative.startswith(".git/"):
            continue

        if path.name in FORBIDDEN_DIR_NAMES:
            violations.append(relative)

        if _is_forbidden_env_file(path, relative):
            violations.append(relative)

        if _is_forbidden_named_file(path, relative):
            violations.append(relative)

        if _is_forbidden_package_artifact(path, relative):
            violations.append(relative)

        if _is_forbidden_suffix_file(path):
            violations.append(relative)

    unique_violations = sorted(set(violations))

    if unique_violations:
        print("repo_hygiene_check_failed")
        for item in unique_violations:
            print(f"- {item}")
        return 1

    print("repo_hygiene_check_ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
