"""Run Compass SQL and persist the result into a SQLite table."""
from __future__ import annotations

import argparse
import sqlite3
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional

try:  # pragma: no cover - script vs package execution
    from .env_loader import get_runtime_root, load_project_dotenv
except ImportError:  # type: ignore
    from env_loader import get_runtime_root, load_project_dotenv  # type: ignore
from compass_query import (  # type: ignore
    IONAPI_DIR,
    JDBC_DIR,
    PREFERRED_IONAPI,
    PREFERRED_JDBC,
    SCHEME_CONFIG,
    build_jdbc_url,
    build_properties,
    ensure_limit,
    ensure_driver_ionapi,
    load_ionapi,
    run_query,
)

load_project_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SQLITE_PATH = get_runtime_root() / "cache.db"
PROGRESS_CHUNK_SIZE = 100
LOCK_RETRY_MAX = 60
LOCK_RETRY_SLEEP_SEC = 0.5


def find_file(directory: Path, preferred: List[str], pattern: str) -> Path:
    if directory.is_file():
        return directory
    if directory.is_dir():
        for name in preferred:
            candidate = directory / name
            if candidate.exists():
                return candidate
        matches = sorted(directory.glob(pattern))
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            return matches[0]
    raise FileNotFoundError(f"Keine Datei passend zu {pattern} in {directory} gefunden.")


def load_sql(args: argparse.Namespace) -> str:
    if args.sql:
        return args.sql.strip()
    if args.sql_file:
        return Path(args.sql_file).read_text(encoding="utf-8")
    raise ValueError("SQL muss via --sql oder --sql-file angegeben werden.")


def ensure_table(
    conn: sqlite3.Connection,
    table: str,
    columns: List[str],
    mode: str,
) -> None:
    quoted_cols = ", ".join(f'"{col}" TEXT' for col in columns)
    if mode == "replace":
        _execute_sql_with_retry(conn, f'DROP TABLE IF EXISTS "{table}"')
        _execute_sql_with_retry(conn, f'CREATE TABLE "{table}" ({quoted_cols})')
    else:
        _execute_sql_with_retry(
            conn,
            f'CREATE TABLE IF NOT EXISTS "{table}" ({quoted_cols})'
        )


def normalize_value(value: object) -> Optional[object]:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bytes)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    # jaydebeapi liefert java.sql.Timestamp/Date/etc. als JPype-Objekte; str() nutzt toString()
    try:
        return str(value)
    except Exception:
        return None


def insert_rows(
    conn: sqlite3.Connection,
    table: str,
    columns: List[str],
    rows: List[Dict[str, object]],
    mode: str,
) -> int:
    if mode == "replace":
        pass  # already dropped
    elif mode == "truncate":
        _execute_sql_with_retry(conn, f'DELETE FROM "{table}"')
    placeholders = ", ".join("?" for _ in columns)
    column_list = ", ".join(f'"{col}"' for col in columns)
    sql = f'INSERT INTO "{table}" ({column_list}) VALUES ({placeholders})'
    data = [[normalize_value(row.get(col)) for col in columns] for row in rows]
    total = len(data)
    if not total:
        return 0
    for start in range(0, total, PROGRESS_CHUNK_SIZE):
        chunk = data[start : start + PROGRESS_CHUNK_SIZE]
        _executemany_with_retry(conn, sql, chunk)
        done = start + len(chunk)
        print(f"{done}/{total} Datensätze gespeichert ...", flush=True)
    return total


def _is_db_locked_error(exc: sqlite3.OperationalError) -> bool:
    msg = str(exc).lower()
    return "database is locked" in msg or "database schema is locked" in msg


def _execute_sql_with_retry(conn: sqlite3.Connection, sql: str) -> None:
    last_exc: Optional[sqlite3.OperationalError] = None
    for attempt in range(LOCK_RETRY_MAX):
        try:
            conn.execute(sql)
            return
        except sqlite3.OperationalError as exc:
            if not _is_db_locked_error(exc):
                raise
            last_exc = exc
            if attempt == LOCK_RETRY_MAX - 1:
                break
            time.sleep(LOCK_RETRY_SLEEP_SEC)
    if last_exc is not None:
        raise last_exc


def _executemany_with_retry(
    conn: sqlite3.Connection,
    sql: str,
    rows: List[List[Optional[object]]],
) -> None:
    last_exc: Optional[sqlite3.OperationalError] = None
    for attempt in range(LOCK_RETRY_MAX):
        try:
            conn.executemany(sql, rows)
            return
        except sqlite3.OperationalError as exc:
            if not _is_db_locked_error(exc):
                raise
            last_exc = exc
            if attempt == LOCK_RETRY_MAX - 1:
                break
            time.sleep(LOCK_RETRY_SLEEP_SEC)
    if last_exc is not None:
        raise last_exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Führt einen Compass SQL-Query aus und speichert das Ergebnis in SQLite."
    )
    parser.add_argument("--ionapi", help="Pfad zur Compass .ionapi Datei")
    parser.add_argument("--jdbc-jar", help="Pfad zum Compass JDBC JAR")
    parser.add_argument("--scheme", choices=list(SCHEME_CONFIG.keys()), default="datalake")
    parser.add_argument("--catalog", help="Katalog (nur für scheme=sourcedata erforderlich)")
    parser.add_argument("--default-collection", help="Optionaler Collection Name")
    parser.add_argument("--sql", help="SQL-Statement direkt in der CLI")
    parser.add_argument("--sql-file", help="Pfad zu einer SQL-Datei")
    parser.add_argument("--limit", type=int, help="Optionales LIMIT")
    parser.add_argument("--sqlite-db", default=str(DEFAULT_SQLITE_PATH), help="Pfad zur SQLite DB")
    parser.add_argument("--table", required=True, help="Zieltabelle in SQLite")
    parser.add_argument(
        "--mode",
        choices=["replace", "truncate", "append"],
        default="replace",
        help="Wie soll die Tabelle behandelt werden? replace=Drop/Create, truncate=DELETE vor Insert, append=anfügen",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    ionapi_path = (
        Path(args.ionapi) if args.ionapi else find_file(IONAPI_DIR, PREFERRED_IONAPI, "*.ionapi")
    )
    jdbc_path = (
        Path(args.jdbc_jar) if args.jdbc_jar else find_file(JDBC_DIR, PREFERRED_JDBC, "*.jar")
    )

    scheme_cfg = SCHEME_CONFIG[args.scheme]
    if scheme_cfg["requires_catalog"] and not args.catalog:
        raise ValueError("Für scheme 'sourcedata' ist --catalog erforderlich.")

    ensure_driver_ionapi(ionapi_path, jdbc_path)
    sql = ensure_limit(load_sql(args), args.limit)
    ion_cfg = load_ionapi(ionapi_path)
    jdbc_url = build_jdbc_url(ion_cfg, args.scheme, args.catalog)
    props = build_properties(ion_cfg, args.catalog, args.default_collection)
    query_result = run_query(jdbc_url, jdbc_path, props, sql)

    columns = query_result["columns"]
    rows = query_result["rows"]
    if not columns:
        raise RuntimeError("Keine Spalten im Ergebnis gefunden – Alias im SQL vergeben?")

    db_path = Path(args.sqlite_db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path), timeout=60)
    conn.execute("PRAGMA busy_timeout = 60000")
    try:
        ensure_table(conn, args.table, columns, "replace" if args.mode == "replace" else "append")
        inserted = insert_rows(conn, args.table, columns, rows, args.mode)
        _execute_sql_with_retry(conn, "PRAGMA optimize")
        conn.commit()
    finally:
        conn.close()

    print(f"Query erfolgreich gespeichert: {inserted} Zeilen in {db_path} -> {args.table}")


if __name__ == "__main__":
    main()
