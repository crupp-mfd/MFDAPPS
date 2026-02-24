from __future__ import annotations

from dataclasses import dataclass
import json
import re
from pathlib import Path
from typing import Any

import jaydebeapi

from app_datalake_sync.config import DatalakeSyncSettings

ENV_ALIASES = {
    "live": "prd",
    "prd": "prd",
    "prod": "prd",
    "tst": "tst",
    "test": "tst",
}
SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")


def _sanitize_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        try:
            return isoformat()
        except Exception:
            pass
    return str(value)


@dataclass(frozen=True)
class _CompassConnectionConfig:
    ionapi_path: Path
    jdbc_path: Path
    jdbc_url: str
    properties: dict[str, str]
    jars: list[str]


class CompassSession:
    def __init__(self, config: _CompassConnectionConfig) -> None:
        self._config = config
        self._conn = None
        self._cursor = None

    def __enter__(self) -> "CompassSession":
        self._conn = jaydebeapi.connect(
            "com.infor.idl.jdbc.Driver",
            self._config.jdbc_url,
            self._config.properties,
            jars=self._config.jars,
        )
        self._cursor = self._conn.cursor()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        if self._conn is not None:
            self._conn.close()
        self._conn = None
        self._cursor = None

    def query(self, sql: str) -> list[dict[str, Any]]:
        if self._cursor is None:
            raise RuntimeError("Compass session is not open.")
        self._cursor.execute(sql)
        columns = [desc[0].strip("\"'") if isinstance(desc[0], str) else str(desc[0]) for desc in (self._cursor.description or [])]
        rows = self._cursor.fetchall()
        return [{col: _sanitize_value(val) for col, val in zip(columns, row)} for row in rows]

    def list_base_tables(self, schema: str = "default") -> list[str]:
        escaped_schema = schema.replace("'", "''")
        sql = (
            "SELECT TABLE_NAME "
            "FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = '{escaped_schema}' "
            "AND TABLE_TYPE = 'BASE TABLE' "
            "ORDER BY TABLE_NAME"
        )
        rows = self.query(sql)
        return [str(row.get("TABLE_NAME", "")).lower() for row in rows if row.get("TABLE_NAME")]

    def count_rows(self, schema: str, table_name: str) -> int:
        if not SAFE_IDENTIFIER.match(table_name):
            raise ValueError(f"Invalid table name: {table_name}")
        if not SAFE_IDENTIFIER.match(schema):
            raise ValueError(f"Invalid schema: {schema}")
        sql = f"SELECT COUNT(*) AS row_count FROM {schema}.{table_name}"
        rows = self.query(sql)
        if not rows:
            return 0
        value = rows[0].get("row_count")
        if value is None:
            return 0
        return int(value)


class InforDatalakeClient:
    """Infor Data Lake access via Compass JDBC."""

    def __init__(self, settings: DatalakeSyncSettings) -> None:
        self.settings = settings

    def is_configured(self) -> bool:
        try:
            self._build_connection_config("live")
            self._build_connection_config("tst")
            return True
        except Exception:
            return False

    def open_session(self, env: str) -> CompassSession:
        config = self._build_connection_config(env)
        return CompassSession(config)

    def _build_connection_config(self, env: str) -> _CompassConnectionConfig:
        normalized_env = self._normalize_env(env)
        credentials_root = Path(self.settings.credentials_dir)
        ionapi_path = self._resolve_ionapi(credentials_root, normalized_env)
        jdbc_path = self._resolve_jdbc(credentials_root)
        ion_cfg = json.loads(ionapi_path.read_text(encoding="utf-8-sig"))
        tenant = str(ion_cfg.get("ti") or "").strip()
        if not tenant:
            raise RuntimeError(f"Tenant 'ti' missing in ionapi file: {ionapi_path}")
        jdbc_url = f"jdbc:infordatalake://{tenant}"
        properties = {
            "ION_API_CREDENTIALS": json.dumps(ion_cfg),
            "TENANT": tenant,
        }
        jars = self._collect_support_jars(jdbc_path)
        self._ensure_driver_ionapi(ionapi_path, jdbc_path)
        return _CompassConnectionConfig(
            ionapi_path=ionapi_path,
            jdbc_path=jdbc_path,
            jdbc_url=jdbc_url,
            properties=properties,
            jars=jars,
        )

    @staticmethod
    def _normalize_env(env: str) -> str:
        key = (env or "").strip().lower()
        normalized = ENV_ALIASES.get(key)
        if not normalized:
            raise ValueError(f"Unsupported environment: {env}")
        return normalized

    @staticmethod
    def _resolve_ionapi(credentials_root: Path, env: str) -> Path:
        ionapi_dir = credentials_root / "ionapi"
        tst_env_dir = credentials_root / "TSTEnv"
        if env == "prd":
            path = ionapi_dir / "Infor Compass JDBC Driver.ionapi"
        else:
            path = tst_env_dir / "Infor Compass JDBC Driver.ionapi"
            if not path.exists():
                path = ionapi_dir / "Infor Compass JDBC Driver_TST.ionapi"
        if not path.exists():
            raise FileNotFoundError(f"ionapi file not found for env={env}: {path}")
        return path

    @staticmethod
    def _resolve_jdbc(credentials_root: Path) -> Path:
        jdbc_dir = credentials_root / "jdbc"
        preferred = sorted(jdbc_dir.glob("infor-compass-jdbc-*.jar"), reverse=True)
        if preferred:
            return preferred[0]
        fallback = sorted(jdbc_dir.glob("*.jar"))
        if fallback:
            return fallback[0]
        raise FileNotFoundError(f"No JDBC jar found in {jdbc_dir}")

    @staticmethod
    def _collect_support_jars(jdbc_path: Path) -> list[str]:
        jars = [str(jdbc_path)]
        support = sorted(jdbc_path.parent.glob("slf4j-*.jar"))
        for jar in support:
            if jar.resolve() != jdbc_path.resolve():
                jars.append(str(jar))
        return jars

    @staticmethod
    def _ensure_driver_ionapi(ionapi_path: Path, jdbc_path: Path) -> None:
        target = jdbc_path.parent / ionapi_path.name
        try:
            if ionapi_path.resolve() == target.resolve():
                return
        except OSError:
            pass
        if target.exists():
            return
        target.write_text(ionapi_path.read_text(encoding="utf-8-sig"), encoding="utf-8")
