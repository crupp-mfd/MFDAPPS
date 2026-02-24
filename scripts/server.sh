#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="${PID_FILE:-/tmp/nameflight.pid}"
LOG_FILE="${LOG_FILE:-/tmp/nameflight.log}"
DEFAULT_PYTHON_BIN="python3"
if [[ -x "/Users/crupp/dev/MFDAPPS/.venv/bin/python" ]]; then
  DEFAULT_PYTHON_BIN="/Users/crupp/dev/MFDAPPS/.venv/bin/python"
fi
if [[ -x "/Users/crupp/SPAREPART/.venv/bin/python" ]]; then
  # Nur als Fallback nutzen, falls das Projekt-venv fehlt.
  if [[ ! -x "/Users/crupp/dev/MFDAPPS/.venv/bin/python" ]]; then
    DEFAULT_PYTHON_BIN="/Users/crupp/SPAREPART/.venv/bin/python"
  fi
fi
PYTHON_BIN="${PYTHON_BIN:-${DEFAULT_PYTHON_BIN}}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8000}"
PORT_TRIES="${PORT_TRIES:-20}"
OS_NAME="$(uname -s)"
USE_LAUNCHD_DEFAULT=0
if [[ "${OS_NAME}" == "Darwin" ]]; then
  USE_LAUNCHD_DEFAULT=1
fi
USE_LAUNCHD="${USE_LAUNCHD:-${USE_LAUNCHD_DEFAULT}}"
LAUNCHD_LABEL="${LAUNCHD_LABEL:-com.mfdapps.nameflight}"
LAUNCHD_DOMAIN="gui/$(id -u)"
LAUNCHD_TARGET="${LAUNCHD_DOMAIN}/${LAUNCHD_LABEL}"
LAUNCHD_PLIST="${LAUNCHD_PLIST:-${HOME}/Library/LaunchAgents/${LAUNCHD_LABEL}.plist}"
LAUNCHD_MAXFILES_SOFT="${LAUNCHD_MAXFILES_SOFT:-65536}"
LAUNCHD_MAXFILES_HARD="${LAUNCHD_MAXFILES_HARD:-65536}"

set_runtime_env_defaults() {
  export MFDAPPS_ENFORCE_ONEDRIVE="${MFDAPPS_ENFORCE_ONEDRIVE:-0}"
  export MFDAPPS_HOME="${MFDAPPS_HOME:-${ROOT_DIR}}"
  export MFDAPPS_RUNTIME_ROOT="${MFDAPPS_RUNTIME_ROOT:-${ROOT_DIR}/apps/christian/data}"
  export MFDAPPS_CREDENTIALS_DIR="${MFDAPPS_CREDENTIALS_DIR:-${ROOT_DIR}/credentials}"
  export MFDAPPS_FRONTEND_DIR="${MFDAPPS_FRONTEND_DIR:-${ROOT_DIR}}"
  export SPAREPART_PRD_DRY_RUN="${SPAREPART_PRD_DRY_RUN:-1}"
  export SPAREPART_TST_DRY_RUN="${SPAREPART_TST_DRY_RUN:-0}"
  export SPAREPART_M3_CONO_PRD="${SPAREPART_M3_CONO_PRD:-860}"
  export SPAREPART_M3_CONO_TST="${SPAREPART_M3_CONO_TST:-883}"
  mkdir -p "${MFDAPPS_RUNTIME_ROOT}" "${MFDAPPS_CREDENTIALS_DIR}"
}

write_launchd_plist() {
  mkdir -p "$(dirname "${LAUNCHD_PLIST}")"
  cat >"${LAUNCHD_PLIST}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LAUNCHD_LABEL}</string>
  <key>WorkingDirectory</key>
  <string>${ROOT_DIR}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${PYTHON_BIN}</string>
    <string>-m</string>
    <string>uvicorn</string>
    <string>python.web_server:app</string>
    <string>--host</string>
    <string>${HOST}</string>
    <string>--port</string>
    <string>${PORT}</string>
    <string>--timeout-keep-alive</string>
    <string>2</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>SoftResourceLimits</key>
  <dict>
    <key>NumberOfFiles</key>
    <integer>${LAUNCHD_MAXFILES_SOFT}</integer>
  </dict>
  <key>HardResourceLimits</key>
  <dict>
    <key>NumberOfFiles</key>
    <integer>${LAUNCHD_MAXFILES_HARD}</integer>
  </dict>
  <key>StandardOutPath</key>
  <string>${LOG_FILE}</string>
  <key>StandardErrorPath</key>
  <string>${LOG_FILE}</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>MFDAPPS_ENFORCE_ONEDRIVE</key>
    <string>${MFDAPPS_ENFORCE_ONEDRIVE}</string>
    <key>MFDAPPS_HOME</key>
    <string>${MFDAPPS_HOME}</string>
    <key>MFDAPPS_RUNTIME_ROOT</key>
    <string>${MFDAPPS_RUNTIME_ROOT}</string>
    <key>MFDAPPS_CREDENTIALS_DIR</key>
    <string>${MFDAPPS_CREDENTIALS_DIR}</string>
    <key>MFDAPPS_FRONTEND_DIR</key>
    <string>${MFDAPPS_FRONTEND_DIR}</string>
    <key>SPAREPART_PRD_DRY_RUN</key>
    <string>${SPAREPART_PRD_DRY_RUN}</string>
    <key>SPAREPART_TST_DRY_RUN</key>
    <string>${SPAREPART_TST_DRY_RUN}</string>
    <key>SPAREPART_M3_CONO_PRD</key>
    <string>${SPAREPART_M3_CONO_PRD}</string>
    <key>SPAREPART_M3_CONO_TST</key>
    <string>${SPAREPART_M3_CONO_TST}</string>
  </dict>
</dict>
</plist>
EOF
}

launchd_pid() {
  launchctl print "${LAUNCHD_TARGET}" 2>/dev/null \
    | awk '/pid = [0-9]+/ {gsub(/;/, "", $3); print $3; exit}'
}

wait_for_health() {
  local health_url="http://${HOST}:${PORT}/api/health"
  for _ in {1..40}; do
    if curl -fsS "${health_url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

is_running_launchd() {
  local pid
  pid="$(launchd_pid || true)"
  [[ "${pid}" =~ ^[0-9]+$ ]] && [[ "${pid}" -gt 1 ]]
}

is_running() {
  if [[ ! -f "${PID_FILE}" ]]; then
    return 1
  fi

  local pid
  pid="$(cat "${PID_FILE}" 2>/dev/null || true)"
  if [[ ! "${pid}" =~ ^[0-9]+$ ]]; then
    return 1
  fi
  if [[ "${pid}" -le 1 ]]; then
    return 1
  fi

  kill -0 "${pid}" 2>/dev/null
}

cleanup_stale_pid() {
  if [[ -f "${PID_FILE}" ]] && ! is_running; then
    rm -f "${PID_FILE}"
  fi
}

start_server_legacy() {
  cleanup_stale_pid
  if is_running; then
    local pid
    pid="$(cat "${PID_FILE}")"
    echo "Server laeuft bereits (PID ${pid})."
    return 0
  fi

  cd "${ROOT_DIR}"
  set_runtime_env_defaults

  if ! "${PYTHON_BIN}" -c "import uvicorn" >/dev/null 2>&1; then
    echo "uvicorn fehlt fuer ${PYTHON_BIN}. Setze PYTHON_BIN auf eine Python-Umgebung mit uvicorn."
    return 1
  fi

  nohup "${PYTHON_BIN}" -m uvicorn python.web_server:app --host "${HOST}" --port "${PORT}" >>"${LOG_FILE}" 2>&1 &
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

start_server_launchd() {
  set_runtime_env_defaults
  if ! "${PYTHON_BIN}" -c "import uvicorn" >/dev/null 2>&1; then
    echo "uvicorn fehlt fuer ${PYTHON_BIN}. Setze PYTHON_BIN auf eine Python-Umgebung mit uvicorn."
    return 1
  fi

  write_launchd_plist
  launchctl bootout "${LAUNCHD_TARGET}" >/dev/null 2>&1 || true
  local bootstrap_err=""
  local bootstrapped=0
  for _ in {1..8}; do
    bootstrap_err="$(launchctl bootstrap "${LAUNCHD_DOMAIN}" "${LAUNCHD_PLIST}" 2>&1)" && {
      bootstrapped=1
      break
    }
    sleep 0.25
  done
  if [[ "${bootstrapped}" != "1" ]]; then
    echo "launchctl bootstrap fehlgeschlagen: ${bootstrap_err}"
    return 1
  fi
  launchctl kickstart -k "${LAUNCHD_TARGET}" >/dev/null 2>&1 || true

  if wait_for_health; then
    local pid
    pid="$(launchd_pid || true)"
    if [[ "${pid}" =~ ^[0-9]+$ ]] && [[ "${pid}" -gt 1 ]]; then
      echo "${pid}" >"${PID_FILE}"
    else
      rm -f "${PID_FILE}" >/dev/null 2>&1 || true
    fi
    echo "Server gestartet via launchd (${LAUNCHD_LABEL})."
    echo "Log: ${LOG_FILE}"
    echo "URL: http://${HOST}:${PORT}"
    return 0
  fi

  rm -f "${PID_FILE}" >/dev/null 2>&1 || true
  echo "Start via launchd fehlgeschlagen. Letzte Log-Zeilen:"
  tail -n 40 "${LOG_FILE}" || true
  return 1
}

start_server() {
  if [[ "${USE_LAUNCHD}" == "1" ]]; then
    start_server_launchd
  else
    start_server_legacy
  fi
}

stop_server_legacy() {
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

stop_server_launchd() {
  launchctl bootout "${LAUNCHD_TARGET}" >/dev/null 2>&1 || true
  rm -f "${PID_FILE}" >/dev/null 2>&1 || true
  echo "Server gestoppt (launchd)."
}

stop_server() {
  if [[ "${USE_LAUNCHD}" == "1" ]]; then
    stop_server_launchd
  else
    stop_server_legacy
  fi
}

status_server_legacy() {
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

status_server_launchd() {
  if is_running_launchd; then
    local pid
    pid="$(launchd_pid)"
    echo "${pid}" >"${PID_FILE}"
    echo "Server laeuft via launchd (PID ${pid})."
    echo "Label: ${LAUNCHD_LABEL}"
    echo "Log: ${LOG_FILE}"
    echo "URL: http://${HOST}:${PORT}"
  else
    rm -f "${PID_FILE}" >/dev/null 2>&1 || true
    echo "Server ist gestoppt."
  fi
}

status_server() {
  if [[ "${USE_LAUNCHD}" == "1" ]]; then
    status_server_launchd
  else
    status_server_legacy
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
  PORT_TRIES=20 (nicht verwendet durch uvicorn)
  USE_LAUNCHD=1 (macOS-Standard; 0 fuer Legacy nohup)
  LAUNCHD_LABEL=com.mfdapps.nameflight
  LAUNCHD_PLIST=~/Library/LaunchAgents/com.mfdapps.nameflight.plist
  LAUNCHD_MAXFILES_SOFT=65536
  LAUNCHD_MAXFILES_HARD=65536
  PID_FILE=/tmp/nameflight.pid
  LOG_FILE=/tmp/nameflight.log
  PYTHON_BIN=python3
  SPAREPART_PRD_DRY_RUN=1
  SPAREPART_TST_DRY_RUN=0
  SPAREPART_M3_CONO_PRD=860
  SPAREPART_M3_CONO_TST=883
EOF
}

cmd="${1:-restart}"
case "${cmd}" in
  start) start_server ;;
  stop) stop_server ;;
  restart)
    stop_server
    if [[ "${USE_LAUNCHD}" == "1" ]]; then
      sleep 0.5
    fi
    start_server
    ;;
  status) status_server ;;
  logs) show_logs ;;
  *) usage; exit 1 ;;
esac
