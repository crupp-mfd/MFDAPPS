#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_FILE="$ROOT_DIR/app.py"
PID_FILE="$ROOT_DIR/.server.pid"
LOG_FILE="$ROOT_DIR/.server.log"

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8000}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

usage() {
  cat <<EOF
Usage: $0 {start|stop|restart|status|logs}

Optional env vars:
  HOST=127.0.0.1
  PORT=8000
  PYTHON_BIN=python3
EOF
}

read_pid() {
  [[ -f "$PID_FILE" ]] || return 1
  local pid
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  printf "%s\n" "$pid"
}

is_running() {
  local pid
  pid="$(read_pid)" || return 1
  kill -0 "$pid" 2>/dev/null
}

cleanup_stale_pid() {
  if [[ -f "$PID_FILE" ]] && ! is_running; then
    rm -f "$PID_FILE"
  fi
}

port_owner_pid() {
  lsof -t -nP -iTCP:"$PORT" -sTCP:LISTEN 2>/dev/null | head -n1 || true
}

start_server() {
  cleanup_stale_pid

  if is_running; then
    echo "Server laeuft bereits (PID $(read_pid)) auf http://$HOST:$PORT"
    return 0
  fi

  if [[ ! -f "$APP_FILE" ]]; then
    echo "Fehler: $APP_FILE nicht gefunden."
    exit 1
  fi

  local owner_pid
  owner_pid="$(port_owner_pid)"
  if [[ -n "$owner_pid" ]]; then
    echo "Fehler: Port $PORT ist bereits belegt (PID $owner_pid)."
    echo "Stoppe den Prozess oder starte mit anderem Port: PORT=8080 $0 start"
    exit 1
  fi

  touch "$LOG_FILE"
  nohup "$PYTHON_BIN" "$APP_FILE" --host "$HOST" --port "$PORT" >>"$LOG_FILE" 2>&1 &
  local new_pid=$!
  echo "$new_pid" >"$PID_FILE"

  sleep 1
  if kill -0 "$new_pid" 2>/dev/null; then
    echo "Server gestartet (PID $new_pid) auf http://$HOST:$PORT"
    echo "Log: $LOG_FILE"
  else
    echo "Fehler: Server konnte nicht gestartet werden. Letzte Logs:"
    tail -n 40 "$LOG_FILE" || true
    rm -f "$PID_FILE"
    exit 1
  fi
}

stop_server() {
  cleanup_stale_pid

  if ! is_running; then
    echo "Server laeuft nicht."
    return 0
  fi

  local pid
  pid="$(read_pid)"
  kill "$pid" 2>/dev/null || true

  for _ in {1..20}; do
    if ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$PID_FILE"
      echo "Server gestoppt."
      return 0
    fi
    sleep 0.25
  done

  echo "Server reagiert nicht auf SIGTERM, sende SIGKILL an PID $pid."
  kill -9 "$pid" 2>/dev/null || true
  rm -f "$PID_FILE"
  echo "Server hart gestoppt."
}

status_server() {
  cleanup_stale_pid
  if is_running; then
    echo "Server laeuft (PID $(read_pid)) auf http://$HOST:$PORT"
  else
    echo "Server laeuft nicht."
  fi
}

show_logs() {
  touch "$LOG_FILE"
  tail -n 60 "$LOG_FILE"
}

cmd="${1:-}"
case "$cmd" in
  start) start_server ;;
  stop) stop_server ;;
  restart)
    stop_server
    start_server
    ;;
  status) status_server ;;
  logs) show_logs ;;
  *)
    usage
    exit 1
    ;;
esac
