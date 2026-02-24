from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import threading
import time

from app_datalake_sync.services.infor_client import InforDatalakeClient

ENV_ALIASES = {
    "live": "live",
    "prd": "live",
    "prod": "live",
    "tst": "tst",
    "test": "tst",
}


@dataclass
class TableCountEntry:
    table_name: str
    row_count: int | None
    status: str
    error: str | None = None
    duration_ms: int | None = None


@dataclass
class RefreshState:
    env: str
    running: bool = False
    started_at_utc: str | None = None
    finished_at_utc: str | None = None
    last_error: str | None = None
    total_tables: int = 0
    completed_tables: int = 0
    error_tables: int = 0
    entries: dict[str, TableCountEntry] = field(default_factory=dict)


class DatalakeTableCatalogService:
    def __init__(self, infor_client: InforDatalakeClient) -> None:
        self.infor_client = infor_client
        self._states: dict[str, RefreshState] = {
            "live": RefreshState(env="live"),
            "tst": RefreshState(env="tst"),
        }
        self._lock = threading.RLock()

    def get_snapshot(self, env: str) -> dict:
        normalized = self._normalize_env(env)
        with self._lock:
            state = self._states[normalized]
            tables = sorted(state.entries.values(), key=lambda entry: entry.table_name)
            return {
                "env": normalized,
                "running": state.running,
                "started_at_utc": state.started_at_utc,
                "finished_at_utc": state.finished_at_utc,
                "last_error": state.last_error,
                "total_tables": state.total_tables,
                "completed_tables": state.completed_tables,
                "error_tables": state.error_tables,
                "tables": [
                    {
                        "table_name": entry.table_name,
                        "row_count": entry.row_count,
                        "status": entry.status,
                        "error": entry.error,
                        "duration_ms": entry.duration_ms,
                    }
                    for entry in tables
                ],
            }

    def trigger_refresh(self, env: str, force: bool = False) -> dict:
        normalized = self._normalize_env(env)
        with self._lock:
            state = self._states[normalized]
            if state.running and not force:
                return self.get_snapshot(normalized)
            if state.running and force:
                return self.get_snapshot(normalized)

            state.running = True
            state.started_at_utc = _utc_now()
            state.finished_at_utc = None
            state.last_error = None
            state.total_tables = 0
            state.completed_tables = 0
            state.error_tables = 0
            state.entries = {}

            worker = threading.Thread(target=self._refresh_worker, args=(normalized,), daemon=True)
            worker.start()
        return self.get_snapshot(normalized)

    def ensure_started(self, env: str) -> dict:
        snapshot = self.get_snapshot(env)
        if snapshot["running"] or snapshot["total_tables"] > 0:
            return snapshot
        return self.trigger_refresh(env)

    def _refresh_worker(self, env: str) -> None:
        try:
            with self.infor_client.open_session(env) as session:
                tables = session.list_base_tables(schema="default")
                with self._lock:
                    state = self._states[env]
                    state.total_tables = len(tables)
                    for table_name in tables:
                        state.entries[table_name] = TableCountEntry(
                            table_name=table_name,
                            row_count=None,
                            status="pending",
                        )

                for table_name in tables:
                    started = time.perf_counter()
                    try:
                        row_count = session.count_rows(schema="default", table_name=table_name)
                        duration_ms = int((time.perf_counter() - started) * 1000)
                        with self._lock:
                            state = self._states[env]
                            state.completed_tables += 1
                            state.entries[table_name] = TableCountEntry(
                                table_name=table_name,
                                row_count=row_count,
                                status="ok",
                                duration_ms=duration_ms,
                            )
                    except Exception as exc:
                        duration_ms = int((time.perf_counter() - started) * 1000)
                        with self._lock:
                            state = self._states[env]
                            state.completed_tables += 1
                            state.error_tables += 1
                            state.entries[table_name] = TableCountEntry(
                                table_name=table_name,
                                row_count=None,
                                status="error",
                                error=str(exc),
                                duration_ms=duration_ms,
                            )
        except Exception as exc:
            with self._lock:
                state = self._states[env]
                state.last_error = str(exc)
        finally:
            with self._lock:
                state = self._states[env]
                state.running = False
                state.finished_at_utc = _utc_now()

    @staticmethod
    def _normalize_env(env: str) -> str:
        value = (env or "").strip().lower()
        normalized = ENV_ALIASES.get(value)
        if not normalized:
            raise ValueError("Ungueltige Umgebung. Erlaubt: live, tst")
        return normalized


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
