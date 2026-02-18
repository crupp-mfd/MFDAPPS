#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="${PID_FILE:-/tmp/nameflight.pid}"
LOG_FILE="${LOG_FILE:-/tmp/nameflight.log}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8000}"
PORT_TRIES="${PORT_TRIES:-20}"

is_running() {
  if [[ ! -f "${PID_FILE}" ]]; then
    return 1
  fi

  local pid
  pid="$(cat "${PID_FILE}" 2>/dev/null || true)"
  if [[ ! "${pid}" =~ ^[0-9]+$ ]]; then
    return 1
  fi

  kill -0 "${pid}" 2>/dev/null
}

cleanup_stale_pid() {
  if [[ -f "${PID_FILE}" ]] && ! is_running; then
    rm -f "${PID_FILE}"
  fi
}

start_server() {
  cleanup_stale_pid
  if is_running; then
    local pid
    pid="$(cat "${PID_FILE}")"
    echo "Server laeuft bereits (PID ${pid})."
    return 0
  fi

  cd "${ROOT_DIR}"
  nohup "${PYTHON_BIN}" -u app.py --host "${HOST}" --port "${PORT}" --port-tries "${PORT_TRIES}" >>"${LOG_FILE}" 2>&1 &
  local pid=$!
  echo "${pid}" >"${PID_FILE}"

  for _ in {1..25}; do
    if ! kill -0 "${pid}" 2>/dev/null; then
      break
    fi
    sleep 0.2
  done

  if kill -0 "${pid}" 2>/dev/null; then
    echo "Server gestartet (PID ${pid})."
    echo "Log: ${LOG_FILE}"
    local url_line
    url_line="$(tail -n 60 "${LOG_FILE}" | rg -o "http://[0-9\\.]+:[0-9]+" | tail -n 1 || true)"
    if [[ -n "${url_line}" ]]; then
      echo "URL: ${url_line}"
    fi
    return 0
  fi

  rm -f "${PID_FILE}"
  echo "Start fehlgeschlagen. Letzte Log-Zeilen:"
  tail -n 30 "${LOG_FILE}" || true
  return 1
}

stop_server() {
  cleanup_stale_pid
  if ! is_running; then
    echo "Server laeuft nicht."
    return 0
  fi

  local pid
  pid="$(cat "${PID_FILE}")"
  kill "${pid}" 2>/dev/null || true

  for _ in {1..20}; do
    if ! kill -0 "${pid}" 2>/dev/null; then
      rm -f "${PID_FILE}"
      echo "Server gestoppt."
      return 0
    fi
    sleep 0.2
  done

  kill -9 "${pid}" 2>/dev/null || true
  rm -f "${PID_FILE}"
  echo "Server hart gestoppt (kill -9)."
}

status_server() {
  cleanup_stale_pid
  if is_running; then
    local pid
    pid="$(cat "${PID_FILE}")"
    echo "Server laeuft (PID ${pid})."
    echo "Log: ${LOG_FILE}"
  else
    echo "Server ist gestoppt."
  fi
}

show_logs() {
  if [[ ! -f "${LOG_FILE}" ]]; then
    echo "Noch kein Log vorhanden: ${LOG_FILE}"
    return 0
  fi
  tail -n 60 "${LOG_FILE}"
}

usage() {
  cat <<EOF
Verwendung: $(basename "$0") {start|stop|restart|status|logs}

Umgebungsvariablen (optional):
  HOST=127.0.0.1
  PORT=8000
  PORT_TRIES=20
  PID_FILE=/tmp/nameflight.pid
  LOG_FILE=/tmp/nameflight.log
  PYTHON_BIN=python3
EOF
}

cmd="${1:-restart}"
case "${cmd}" in
  start) start_server ;;
  stop) stop_server ;;
  restart)
    stop_server
    start_server
    ;;
  status) status_server ;;
  logs) show_logs ;;
  *) usage; exit 1 ;;
esac
