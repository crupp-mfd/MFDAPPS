"""Load ERP wagon numbers from Infor Data Lake and store them in SQLite."""
from __future__ import annotations

import argparse
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from compass_query import (  # type: ignore
    IONAPI_DIR,
    JDBC_DIR,
    PREFERRED_IONAPI,
    PREFERRED_JDBC,
    build_jdbc_url,
    build_properties,
    ensure_limit,
    load_ionapi,
    run_query,
)
try:  # pragma: no cover - import helper works both as module and script
    from .env_loader import get_runtime_root, load_project_dotenv
except ImportError:  # type: ignore
    from env_loader import get_runtime_root, load_project_dotenv  # type: ignore

load_project_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = get_runtime_root() / "cache.db"
DEFAULT_ENV = os.getenv("SPAREPART_ENV", "prd").lower()
ENV_ALIASES = {
    "live": "prd",
    "prod": "prd",
    "prd": "prd",
    "test": "tst",
    "tst": "tst",
}
ENV_SUFFIXES = {"prd": "_PRD", "tst": "_TST"}
DEFAULT_TABLE = "RSRD_ERP_WAGONNO"
DEFAULT_SQL = """
SELECT
    A.SERN,
    A.ITNO,
    A.ALII
FROM MILOIN A
LEFT JOIN MITMAS B ON A.ITNO = B.ITNO
WHERE B.ITTY = '100'
  AND A.STAT = '20'
"""


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


def normalize_sern(sern: str | None) -> str:
    if not sern:
        return ""
    return re.sub(r"\D", "", sern)


def _has_wagon_sern_upsert_target(conn: sqlite3.Connection, table: str) -> bool:
    info_rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    for row in info_rows:
        # row schema: cid, name, type, notnull, dflt_value, pk
        if str(row[1]) == "wagon_sern" and int(row[5] or 0) > 0:
            return True

    index_rows = conn.execute(f'PRAGMA index_list("{table}")').fetchall()
    for index_row in index_rows:
        # row schema: seq, name, unique, origin, partial
        if int(index_row[2] or 0) != 1:
            continue
        index_name = str(index_row[1])
        cols = conn.execute(f'PRAGMA index_info("{index_name}")').fetchall()
        col_names = [str(col[2]) for col in cols]
        if col_names == ["wagon_sern"]:
            return True
    return False


def _ensure_wagon_sern_unique_index(conn: sqlite3.Connection, table: str) -> None:
    if _has_wagon_sern_upsert_target(conn, table):
        return
    safe_table = re.sub(r"[^0-9A-Za-z_]", "_", table)
    index_name = f"{safe_table}_wagon_sern_uq"
    create_sql = (
        f'CREATE UNIQUE INDEX IF NOT EXISTS "{index_name}" '
        f'ON "{table}"("wagon_sern")'
    )
    try:
        conn.execute(create_sql)
    except sqlite3.IntegrityError:
        # Legacy tables may already contain duplicate wagon_sern entries.
        conn.execute(
            f'''
            DELETE FROM "{table}"
            WHERE rowid NOT IN (
                SELECT MIN(rowid)
                FROM "{table}"
                GROUP BY "wagon_sern"
            )
            '''
        )
        conn.execute(create_sql)


def ensure_table(conn: sqlite3.Connection, table: str, truncate: bool) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            wagon_sern TEXT PRIMARY KEY,
            wagon_sern_numeric TEXT,
            wagon_itno TEXT,
            wagon_typ TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for column in ("wagon_itno", "wagon_typ"):
        if column in existing:
            continue
        conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{column}" TEXT')
    if truncate:
        conn.execute(f"DELETE FROM {table}")
    _ensure_wagon_sern_unique_index(conn, table)


def insert_rows(conn: sqlite3.Connection, table: str, rows: List[Dict[str, str]]) -> int:
    now = datetime.now(timezone.utc).isoformat()
    conn.executemany(
        f"""
        INSERT INTO {table} (wagon_sern, wagon_sern_numeric, wagon_itno, wagon_typ, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(wagon_sern) DO UPDATE SET
            wagon_sern_numeric=excluded.wagon_sern_numeric,
            wagon_itno=excluded.wagon_itno,
            wagon_typ=excluded.wagon_typ,
            updated_at=excluded.updated_at
        """,
        [
            (
                row["wagon_sern"],
                row["wagon_sern_numeric"],
                row.get("wagon_itno", ""),
                row.get("wagon_typ", ""),
                now,
            )
            for row in rows
        ],
    )
    return len(rows)


def fetch_wagons(args: argparse.Namespace) -> List[Dict[str, str]]:
    ionapi_path = (
        Path(args.ionapi) if args.ionapi else find_file(IONAPI_DIR, PREFERRED_IONAPI, "*.ionapi")
    )
    jdbc_path = (
        Path(args.jdbc_jar) if args.jdbc_jar else find_file(JDBC_DIR, PREFERRED_JDBC, "*.jar")
    )
    ion_cfg = load_ionapi(ionapi_path)
    sql = ensure_limit(args.sql.strip(), args.limit)
    jdbc_url = build_jdbc_url(ion_cfg, args.scheme, args.catalog)
    props = build_properties(ion_cfg, args.catalog, args.default_collection)
    result = run_query(jdbc_url, jdbc_path, props, sql)
    rows = []
    for row in result["rows"]:
        sern = (row.get("SERN") if isinstance(row, dict) else None) or ""
        itno = (row.get("ITNO") if isinstance(row, dict) else None) or ""
        alii = (row.get("ALII") if isinstance(row, dict) else None) or ""
        normalized = normalize_sern(sern)
        if not sern:
            continue
        rows.append(
            {
                "wagon_sern": sern,
                "wagon_sern_numeric": normalized,
                "wagon_itno": itno,
                "wagon_typ": alii,
            }
        )
    return rows


def _table_for_env(env: str | None) -> str:
    value = (env or DEFAULT_ENV).lower()
    normalized = ENV_ALIASES.get(value)
    if not normalized:
        raise ValueError("Ungültige Umgebung.")
    return f"{DEFAULT_TABLE}{ENV_SUFFIXES[normalized]}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lädt ERP-Wagennummern in SQLite")
    parser.add_argument("--ionapi", help="Pfad zur Compass .ionapi Datei")
    parser.add_argument("--jdbc-jar", help="Pfad zum Compass JDBC JAR")
    parser.add_argument("--scheme", choices=["datalake", "datawarehouse", "sourcedata"], default="datalake")
    parser.add_argument("--catalog", help="Katalog (für sourcedata)")
    parser.add_argument("--default-collection", help="Compass Default Collection")
    parser.add_argument("--sql", default=DEFAULT_SQL, help="SQL-Statement für den Wagon-Export")
    parser.add_argument("--limit", type=int, help="Optionales LIMIT für Compass")
    parser.add_argument("--sqlite-db", default=str(DEFAULT_DB_PATH), help="Pfad zur SQLite DB")
    parser.add_argument("--env", default=DEFAULT_ENV, help="Umgebung (prd/tst oder live/test)")
    parser.add_argument("--table", default=None, help="Zieltabelle in SQLite")
    parser.add_argument("--append", action="store_true", help="Nicht truncaten, sondern anhängen")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = fetch_wagons(args)
    if not rows:
        print("Keine Wagennummern gefunden.")
        return
    db_path = Path(args.sqlite_db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    table_name = args.table or _table_for_env(args.env)
    conn = sqlite3.connect(db_path)
    try:
        ensure_table(conn, table_name, truncate=not args.append)
        inserted = insert_rows(conn, table_name, rows)
        conn.commit()
    finally:
        conn.close()
    print(f"{inserted} Wagennummern in {db_path} -> {table_name} gespeichert.")


if __name__ == "__main__":
    main()
