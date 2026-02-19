#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

# Optional guards/bootstrap from larger monorepo setups.
if [[ -f "$ROOT_DIR/scripts/lib/workspace_guard.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/workspace_guard.sh"
  enforce_onedrive_workspace "$ROOT_DIR"
fi
if [[ -x "$ROOT_DIR/scripts/bootstrap.sh" ]]; then
  "$ROOT_DIR/scripts/bootstrap.sh"
fi
if [[ -f "$ROOT_DIR/.venv/bin/activate" ]]; then
  source "$ROOT_DIR/.venv/bin/activate"
fi

export PYTHONPATH="$ROOT_DIR:$ROOT_DIR/packages/sparepart-shared/src:$ROOT_DIR/apps/christian/AppRSRD/src:${PYTHONPATH:-}"
export MFDAPPS_HOME="$ROOT_DIR"
export MFDAPPS_RUNTIME_ROOT="$ROOT_DIR/apps/christian/data"
export MFDAPPS_CREDENTIALS_DIR="$ROOT_DIR/credentials"
export MFDAPPS_FRONTEND_DIR="$ROOT_DIR/apps/christian/AppRSRD/frontend"
export MFDAPPS_ENFORCE_ONEDRIVE=0
export SQLITE_PATH="$MFDAPPS_RUNTIME_ROOT/cache.db"

mkdir -p "$MFDAPPS_RUNTIME_ROOT" "$MFDAPPS_CREDENTIALS_DIR"
if [[ ! -d "$MFDAPPS_CREDENTIALS_DIR/ionapi" ]]; then
  echo "Fehlendes Verzeichnis: $MFDAPPS_CREDENTIALS_DIR/ionapi"
  echo "Lege alle benoetigten Credentials unter $ROOT_DIR/credentials an."
  exit 1
fi

exec uvicorn app_rsrd.main:app --reload "$@"
