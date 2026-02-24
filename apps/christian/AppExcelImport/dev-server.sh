#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

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

export PYTHONPATH="$ROOT_DIR:$ROOT_DIR/packages/sparepart-shared/src:$ROOT_DIR/apps/christian/AppExcelImport/src:${PYTHONPATH:-}"
export MFDAPPS_HOME="$ROOT_DIR"
export MFDAPPS_RUNTIME_ROOT="$ROOT_DIR/apps/christian/data"
export MFDAPPS_CREDENTIALS_DIR="$ROOT_DIR/credentials"

mkdir -p "$MFDAPPS_RUNTIME_ROOT" "$MFDAPPS_CREDENTIALS_DIR"

host_set=0
port_set=0
for arg in "$@"; do
  [[ "$arg" == "--host" ]] && host_set=1
  [[ "$arg" == "--port" ]] && port_set=1
done

extra=()
[[ $host_set -eq 0 ]] && extra+=(--host 127.0.0.1)
[[ $port_set -eq 0 ]] && extra+=(--port 8000)

if [[ ${#extra[@]} -eq 0 ]]; then
  exec uvicorn app_excel_import.main:app --reload "$@"
fi

exec uvicorn app_excel_import.main:app --reload "${extra[@]}" "$@"
