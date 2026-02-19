"""Utility to load the project level .env file exactly once."""
from __future__ import annotations

from functools import lru_cache
import os
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - handled by requirements install
    load_dotenv = None  # type: ignore

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"
ENV_TEMPLATE = PROJECT_ROOT / ".env.template"

_ALLOWED_ONEDRIVE_SEGMENT = (
    "/Library/CloudStorage/OneDrive-FreigegebeneBibliotheken–MFDRailGmbH/"
    "ICT - Dokumente/AUTOMATE/workspaces/"
)


def _is_ci_runtime() -> bool:
    return os.getenv("CI") == "true" or os.getenv("GITHUB_ACTIONS") == "true"


def _enforce_onedrive_workspace() -> None:
    if _is_ci_runtime():
        return
    enforce = os.getenv("MFDAPPS_ENFORCE_ONEDRIVE", "1").strip().lower()
    if enforce in {"0", "false", "no", "off"}:
        return
    root = str(PROJECT_ROOT.resolve())
    if _ALLOWED_ONEDRIVE_SEGMENT in root and "/MFDApps" in root:
        return
    raise RuntimeError(
        "Dieses Workspace ist nicht im erlaubten OneDrive-Pfad. "
        f"Aktuell: {root}"
    )


@lru_cache(maxsize=1)
def load_project_dotenv() -> Optional[Path]:
    """Load the repository-wide .env file if present."""
    _enforce_onedrive_workspace()
    target = ENV_PATH if ENV_PATH.exists() else ENV_TEMPLATE if ENV_TEMPLATE.exists() else None
    if not target:
        return None
    if load_dotenv is None:
        raise RuntimeError(
            "python-dotenv ist nicht installiert. Bitte `pip install python-dotenv` ausführen."
        )
    load_dotenv(target, override=False)
    return target


def _resolve_override(value: str | None, *, base_dir: Path) -> Optional[Path]:
    raw = (value or "").strip()
    if not raw:
        return None
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path


@lru_cache(maxsize=1)
def get_mfdapps_home() -> Path:
    override = _resolve_override(os.getenv("MFDAPPS_HOME"), base_dir=PROJECT_ROOT)
    return override if override else PROJECT_ROOT


@lru_cache(maxsize=1)
def get_runtime_root() -> Path:
    override = _resolve_override(os.getenv("MFDAPPS_RUNTIME_ROOT"), base_dir=PROJECT_ROOT)
    if override:
        return override
    # Keep runtime data under the christian workspace area by default.
    return get_mfdapps_home() / "apps" / "christian" / "data"


@lru_cache(maxsize=1)
def get_credentials_root() -> Path:
    override = _resolve_override(os.getenv("MFDAPPS_CREDENTIALS_DIR"), base_dir=PROJECT_ROOT)
    if override:
        return override
    preferred = get_mfdapps_home() / "apps" / "AppMFD" / "credentials"
    legacy = PROJECT_ROOT / "credentials"
    return preferred if preferred.exists() else legacy


@lru_cache(maxsize=1)
def get_frontend_root() -> Path:
    override = _resolve_override(os.getenv("MFDAPPS_FRONTEND_DIR"), base_dir=PROJECT_ROOT)
    if override:
        return override
    return PROJECT_ROOT / "frontend"
