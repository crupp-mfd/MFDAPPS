#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

host_set=0
port_set=0
for arg in "$@"; do
  [[ "$arg" == "--host" ]] && host_set=1
  [[ "$arg" == "--port" ]] && port_set=1
done

extra=()
[[ $host_set -eq 0 ]] && extra+=(--host 127.0.0.1)
[[ $port_set -eq 0 ]] && extra+=(--port 8000)

exec "$PYTHON_BIN" "$ROOT_DIR/app.py" "${extra[@]}" "$@"
