#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="${ROOT_DIR}/.run"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8000}"
TARGET_PATH="${TARGET_PATH:-index.html}"
PID_FILE="${RUN_DIR}/http-server-${PORT}.pid"
LOG_FILE="${RUN_DIR}/http-server-${PORT}.log"
URL="http://${HOST}:${PORT}/${TARGET_PATH}"

mkdir -p "${RUN_DIR}"

stop_pid_if_running() {
  local pid="$1"
  if [[ -z "${pid}" ]]; then
    return 0
  fi

  if kill -0 "${pid}" >/dev/null 2>&1; then
    kill "${pid}" >/dev/null 2>&1 || true
    for _ in {1..20}; do
      if ! kill -0 "${pid}" >/dev/null 2>&1; then
        break
      fi
      sleep 0.15
    done
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill -9 "${pid}" >/dev/null 2>&1 || true
    fi
  fi
}

if [[ -f "${PID_FILE}" ]]; then
  old_pid="$(cat "${PID_FILE}" 2>/dev/null || true)"
  stop_pid_if_running "${old_pid}"
  rm -f "${PID_FILE}"
fi

port_pid="$(lsof -tiTCP:"${PORT}" -sTCP:LISTEN 2>/dev/null | head -n1 || true)"
if [[ -n "${port_pid}" ]]; then
  stop_pid_if_running "${port_pid}"
fi

cd "${ROOT_DIR}"
nohup python3 -m http.server "${PORT}" --bind "${HOST}" >"${LOG_FILE}" 2>&1 &
new_pid="$!"
echo "${new_pid}" > "${PID_FILE}"

ready="false"
for _ in {1..40}; do
  if curl -fsS "${URL}" >/dev/null 2>&1; then
    ready="true"
    break
  fi
  sleep 0.2
done

if [[ "${ready}" != "true" ]]; then
  echo "Fehler: Server wurde nicht erreichbar unter ${URL}"
  echo "Log (letzte Zeilen):"
  tail -n 30 "${LOG_FILE}" || true
  exit 1
fi

echo "Server neu gestartet."
echo "URL: ${URL}"
echo "PID: ${new_pid}"
echo "Log: ${LOG_FILE}"
