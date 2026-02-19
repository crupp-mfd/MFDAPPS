"""Synchronise rolling-stock data from the RSRD2 web service into SQLite."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
import sqlite3
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence
import re
from uuid import uuid4

import requests
from zeep import Client
from zeep.helpers import serialize_object
from zeep.transports import Transport

try:  # pragma: no cover - script vs package execution
    from .env_loader import get_runtime_root, load_project_dotenv
except ImportError:  # type: ignore
    from env_loader import get_runtime_root, load_project_dotenv  # type: ignore

load_project_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = get_runtime_root() / "cache.db"

BASE_WAGONS_TABLE = "rsrd_wagons"
BASE_SNAPSHOTS_TABLE = "rsrd_wagon_snapshots"
BASE_JSON_TABLE = "RSRD_WAGON_JSON"
BASE_DETAIL_TABLE = "RSRD_WAGON_DATA"
DEFAULT_ENV = os.getenv("SPAREPART_ENV", "prd").lower()
ENV_ALIASES = {
    "live": "prd",
    "prod": "prd",
    "prd": "prd",
    "test": "tst",
    "tst": "tst",
}
ENV_SUFFIXES = {"prd": "_PRD", "tst": "_TST"}


@dataclass(frozen=True)
class RSRDTables:
    wagons: str
    snapshots: str
    json: str
    detail: str


BASE_TABLES = RSRDTables(
    wagons=BASE_WAGONS_TABLE,
    snapshots=BASE_SNAPSHOTS_TABLE,
    json=BASE_JSON_TABLE,
    detail=BASE_DETAIL_TABLE,
)


def tables_for_suffix(suffix: str | None) -> RSRDTables:
    suffix = suffix or ""
    return RSRDTables(
        wagons=f"{BASE_WAGONS_TABLE}{suffix}",
        snapshots=f"{BASE_SNAPSHOTS_TABLE}{suffix}",
        json=f"{BASE_JSON_TABLE}{suffix}",
        detail=f"{BASE_DETAIL_TABLE}{suffix}",
    )


def tables_for_env(env: str | None) -> RSRDTables:
    value = (env or DEFAULT_ENV).lower()
    normalized = ENV_ALIASES.get(value)
    if not normalized:
        raise ValueError("Ungültige Umgebung.")
    return tables_for_suffix(ENV_SUFFIXES[normalized])


def _normalize_env(env: str | None) -> str:
    value = (env or DEFAULT_ENV).lower()
    normalized = ENV_ALIASES.get(value)
    if not normalized:
        raise ValueError("Ungültige Umgebung.")
    return normalized


def resolve_env_value(base: str, env: str | None) -> str:
    normalized = _normalize_env(env)
    suffix = "PRD" if normalized == "prd" else "TST"
    other_suffix = "TST" if suffix == "PRD" else "PRD"
    value = (
        os.getenv(f"{base}_{suffix}")
        or os.getenv(base)
        or os.getenv(f"{base}_{other_suffix}")
    )
    if not value:
        raise RuntimeError(
            f"Umgebungsvariable {base}_{suffix} fehlt "
            f"(auch {base} und {base}_{other_suffix} nicht gesetzt)."
        )
    return value


def resolve_tables(tables: RSRDTables | None) -> RSRDTables:
    return tables or BASE_TABLES
BATCH_SIZE = 50  # laut RSRD-Doku maximal 50 Wagen pro Abruf
DEFAULT_MESSAGE_TYPE = int(os.getenv("RSRD_MESSAGE_TYPE", "6003"))
DEFAULT_MESSAGE_VERSION = os.getenv("RSRD_MESSAGE_VERSION", "1.0")
DEFAULT_SENDER_REFERENCE = os.getenv("RSRD_SENDER_REFERENCE", "MFD-Automation")
SENDER_CODE = int(os.getenv("RSRD_SENDER_CODE", "1"))
RECIPIENT_CODE = int(os.getenv("RSRD_RECIPIENT_CODE", str(SENDER_CODE)))
INSTANCE_NUMBER = os.getenv("RSRD_INSTANCE_NUMBER", "1")


def require_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise RuntimeError(f"Umgebungsvariable {key} fehlt.")
    return value


def init_db(conn: sqlite3.Connection, tables: RSRDTables | None = None) -> None:
    tables = resolve_tables(tables)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {tables.wagons} (
            wagon_id TEXT PRIMARY KEY,
            data_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {tables.snapshots} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wagon_id TEXT NOT NULL,
            snapshot_at TEXT NOT NULL,
            data_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{tables.snapshots}_wagon
        ON {tables.snapshots}(wagon_id)
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {tables.json} (
            wagon_id TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {tables.detail} (
            wagon_id TEXT PRIMARY KEY,
            wagon_number_freight TEXT,
            vehicle_contract_number TEXT,
            external_reference_id TEXT,
            creation_datetime TEXT,
            last_update_datetime TEXT,
            swdb_update_datetime TEXT,
            administrative_json TEXT,
            design_json TEXT,
            documents_json TEXT,
            dataset_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def chunked(items: Sequence[str], size: int) -> Iterable[List[str]]:
    for start in range(0, len(items), size):
        yield list(items[start : start + size])


def make_client(wsdl_url: str, user: str, password: str) -> Client:
    session = requests.Session()
    session.auth = (user, password)
    transport = Transport(session=session, timeout=60)
    return Client(wsdl=wsdl_url, transport=transport)


def build_message_header() -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    header = {
        "MessageReference": {
            "MessageType": DEFAULT_MESSAGE_TYPE,
            "MessageTypeVersion": DEFAULT_MESSAGE_VERSION,
            "MessageIdentifier": str(uuid4()),
            "MessageDateTime": now,
        },
        "MessageRoutingID": 1,
        "SenderReference": DEFAULT_SENDER_REFERENCE,
        "Sender": {"_value_1": SENDER_CODE, "CI_InstanceNumber": INSTANCE_NUMBER},
        "Recipient": {"_value_1": RECIPIENT_CODE, "CI_InstanceNumber": INSTANCE_NUMBER},
    }
    return header


def query_dataset(client: Client, wagon_numbers: Sequence[str]) -> Dict[str, Any]:
    """Aufruf des SOAP-Endpunkts. Parameter ggf. anpassen."""
    header = build_message_header()
    return serialize_object(
        client.service.QueryRollingStockDataset(
            MessageHeader=header,
            WagonNumberFreight=list(wagon_numbers),
        )
    )


def extract_wagon_id(dataset_item: Dict[str, Any]) -> str:
    admin = dataset_item.get("AdministrativeDataSet") or {}
    meta = dataset_item.get("RSRD2MetaData") or {}
    for key in ("WagonNumberFreight", "VehicleContractNumber", "ExternalReferenceID"):
        value = admin.get(key) or meta.get(key)
        if value:
            return str(value)
    raise ValueError("Konnte WagonNumberFreight nicht ermitteln.")


def upsert_wagon(
    conn: sqlite3.Connection,
    wagon_id: str,
    payload: Dict[str, Any],
    keep_snapshot: bool,
    tables: RSRDTables | None = None,
) -> None:
    tables = resolve_tables(tables)
    now = datetime.now(timezone.utc).isoformat()
    data_json = json.dumps(payload, ensure_ascii=False, default=_json_default)
    conn.execute(
        f"""
        INSERT INTO {tables.wagons} (wagon_id, data_json, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(wagon_id) DO UPDATE SET
            data_json=excluded.data_json,
            updated_at=excluded.updated_at
        """,
        (wagon_id, data_json, now),
    )
    if keep_snapshot:
        conn.execute(
            f"""
            INSERT INTO {tables.snapshots} (wagon_id, snapshot_at, data_json)
            VALUES (?, ?, ?)
            """,
            (wagon_id, now, data_json),
        )


def determine_items(response: Any) -> List[Dict[str, Any]]:
    if isinstance(response, list):
        return response
    if isinstance(response, dict):
        for candidate in ("Wagons", "WagonDatasets", "RollingStockDataset", "DatasetItems"):
            value = response.get(candidate)
            if value:
                if isinstance(value, dict):
                    return value.get("Wagon", []) or value.get("Items", []) or []
                return value
    return []


def store_json_dataset(
    conn: sqlite3.Connection,
    dataset: Dict[str, Any],
    tables: RSRDTables | None = None,
) -> str:
    tables = resolve_tables(tables)
    wagon_id = extract_wagon_id(dataset)
    now = datetime.now(timezone.utc).isoformat()
    data_json = json.dumps(dataset, ensure_ascii=False, default=_json_default)
    conn.execute(
        f"""
        INSERT INTO {tables.json} (wagon_id, payload_json, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(wagon_id) DO UPDATE SET
            payload_json=excluded.payload_json,
            updated_at=excluded.updated_at
        """,
        (wagon_id, data_json, now),
    )
    return wagon_id


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def _is_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool, Decimal, datetime, date))


def _format_scalar(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    formatted = _json_default(value)
    if formatted is None:
        return ""
    return str(formatted)


def _flatten_dataset(value: Any, prefix: str = "") -> Dict[str, str]:
    items: Dict[str, str] = {}
    if isinstance(value, dict):
        for key, nested in value.items():
            next_prefix = f"{prefix}.{key}" if prefix else str(key)
            items.update(_flatten_dataset(nested, next_prefix))
    elif isinstance(value, list):
        if not value:
            if prefix:
                items[prefix] = ""
        elif all(_is_scalar(entry) for entry in value):
            items[prefix] = ", ".join(_format_scalar(entry) for entry in value)
        else:
            for idx, entry in enumerate(value):
                next_prefix = f"{prefix}[{idx}]" if prefix else f"[{idx}]"
                items.update(_flatten_dataset(entry, next_prefix))
    else:
        if prefix:
            items[prefix] = _format_scalar(value)
    return items


_COLUMN_SANITIZER = re.compile(r"[^0-9A-Za-z]+")


def _column_name_from_path(path: str) -> str:
    sanitized = _COLUMN_SANITIZER.sub("_", path).strip("_")
    return sanitized.upper()


def _ensure_flat_columns(conn: sqlite3.Connection, columns: Iterable[str], table: str) -> None:
    if not columns:
        return
    existing = {
        row[1].upper()
        for row in conn.execute(f"PRAGMA table_info({table})")
    }
    for column in columns:
        if column.upper() in existing:
            continue
        conn.execute(f'ALTER TABLE {table} ADD COLUMN "{column}" TEXT')
        existing.add(column.upper())


def _update_flat_columns(
    conn: sqlite3.Connection,
    wagon_id: str,
    flat_values: Dict[str, str],
    table: str,
) -> None:
    if not flat_values:
        return
    _ensure_flat_columns(conn, flat_values.keys(), table)
    assignments = ", ".join(f'"{column}" = ?' for column in flat_values)
    params = [flat_values[column] for column in flat_values]
    params.append(wagon_id)
    conn.execute(f"UPDATE {table} SET {assignments} WHERE wagon_id = ?", params)


def _to_json(value: Any | None) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, default=_json_default)


def _normalize_dataset(dataset: Dict[str, Any]) -> Dict[str, Any]:
    meta = dataset.get("RSRD2MetaData") or {}
    admin = dataset.get("AdministrativeDataSet") or {}
    design = dataset.get("DesignDataSet") or {}
    documents = dataset.get("Documents") or {}
    wagon_id = extract_wagon_id(dataset)
    result = {
        "wagon_id": wagon_id,
        "wagon_number_freight": admin.get("WagonNumberFreight") or wagon_id,
        "vehicle_contract_number": meta.get("VehicleContractNumber"),
        "external_reference_id": meta.get("ExternalReferenceID"),
        "creation_datetime": _json_default(meta.get("CreationDateTime"))
        if meta.get("CreationDateTime")
        else None,
        "last_update_datetime": _json_default(meta.get("LastUpdateDateTime"))
        if meta.get("LastUpdateDateTime")
        else None,
        "swdb_update_datetime": _json_default(meta.get("SWDBUpdateDateTime"))
        if meta.get("SWDBUpdateDateTime")
        else None,
        "administrative_json": _to_json(admin) if admin else None,
        "design_json": _to_json(design) if design else None,
        "documents_json": _to_json(documents) if documents else None,
        "dataset_json": _to_json(dataset),
    }
    return result


def upsert_dataset(
    conn: sqlite3.Connection,
    dataset: Dict[str, Any],
    tables: RSRDTables | None = None,
) -> None:
    tables = resolve_tables(tables)
    row = _normalize_dataset(dataset)
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        f"""
        INSERT INTO {tables.detail} (
            wagon_id,
            wagon_number_freight,
            vehicle_contract_number,
            external_reference_id,
            creation_datetime,
            last_update_datetime,
            swdb_update_datetime,
            administrative_json,
            design_json,
            documents_json,
            dataset_json,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(wagon_id) DO UPDATE SET
            wagon_number_freight=excluded.wagon_number_freight,
            vehicle_contract_number=excluded.vehicle_contract_number,
            external_reference_id=excluded.external_reference_id,
            creation_datetime=excluded.creation_datetime,
            last_update_datetime=excluded.last_update_datetime,
            swdb_update_datetime=excluded.swdb_update_datetime,
            administrative_json=excluded.administrative_json,
            design_json=excluded.design_json,
            documents_json=excluded.documents_json,
            dataset_json=excluded.dataset_json,
            updated_at=excluded.updated_at
        """,
        (
            row["wagon_id"],
            row["wagon_number_freight"],
            row["vehicle_contract_number"],
            row["external_reference_id"],
            row["creation_datetime"],
            row["last_update_datetime"],
            row["swdb_update_datetime"],
            row["administrative_json"],
            row["design_json"],
            row["documents_json"],
            row["dataset_json"],
            now,
        ),
    )
    flat_paths = _flatten_dataset(dataset)
    flat_values = {
        _column_name_from_path(path): value
        for path, value in flat_paths.items()
        if path and value is not None
    }
    _update_flat_columns(conn, row["wagon_id"], flat_values, tables.detail)


def stage_wagons(
    wagon_numbers: Sequence[str],
    keep_snapshots: bool = True,
    tables: RSRDTables | None = None,
    env: str | None = None,
) -> List[str]:
    if not wagon_numbers:
        print("Keine Wagennummern angegeben – nichts zu tun.")
        return []

    wsdl_url = resolve_env_value("RSRD_WSDL_URL", env)
    soap_user = resolve_env_value("RSRD_SOAP_USER", env)
    soap_pass = resolve_env_value("RSRD_SOAP_PASS", env)
    db_path = Path(os.getenv("RSRD_DB_PATH", DEFAULT_DB_PATH))
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    tables = resolve_tables(tables)
    init_db(conn, tables)
    client = make_client(wsdl_url, soap_user, soap_pass)
    staged: List[str] = []

    try:
        for idx, batch in enumerate(chunked(list(wagon_numbers), BATCH_SIZE), start=1):
            response = query_dataset(client, batch)
            items = determine_items(response)
            for item in items:
                wagon_id = extract_wagon_id(item)
                upsert_wagon(conn, wagon_id, item, keep_snapshot=keep_snapshots, tables=tables)
                stage_id = store_json_dataset(conn, item, tables=tables)
                staged.append(stage_id)
            conn.commit()
            print(f"[Batch {idx}] {len(batch)} Wagen synchronisiert.")
    finally:
        conn.close()
    return staged


def process_rsrd_json(
    wagon_ids: Sequence[str] | None = None,
    limit: int | None = None,
    tables: RSRDTables | None = None,
) -> int:
    db_path = Path(os.getenv("RSRD_DB_PATH", DEFAULT_DB_PATH))
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    tables = resolve_tables(tables)
    init_db(conn, tables)
    try:
        query = f"SELECT wagon_id, payload_json FROM {tables.json}"
        params: List[Any] = []
        if wagon_ids:
            placeholders = ",".join("?" for _ in wagon_ids)
            query += f" WHERE wagon_id IN ({placeholders})"
            params.extend(wagon_ids)
        query += " ORDER BY updated_at"
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        rows = conn.execute(query, params).fetchall()
        processed = 0
        for row in rows:
            dataset = json.loads(row["payload_json"])
            upsert_dataset(conn, dataset, tables=tables)
            processed += 1
        conn.commit()
    finally:
        conn.close()
    return processed


def sync_wagons(
    wagon_numbers: Sequence[str],
    keep_snapshots: bool = True,
    mode: str = "full",
    process_limit: int | None = None,
    tables: RSRDTables | None = None,
    env: str | None = None,
) -> Dict[str, int]:
    normalized_mode = (mode or "full").lower()
    if normalized_mode not in {"full", "stage", "process"}:
        raise ValueError("Ungültiger Modus. Erlaubt: full, stage, process.")

    staged_ids: List[str] = []
    processed = 0
    if normalized_mode in {"stage", "full"}:
        staged_ids = stage_wagons(
            wagon_numbers,
            keep_snapshots=keep_snapshots,
            tables=tables,
            env=env,
        )
    if normalized_mode == "process":
        processed = process_rsrd_json(limit=process_limit, tables=tables)
    elif normalized_mode == "full" and staged_ids:
        processed = process_rsrd_json(wagon_ids=staged_ids, limit=process_limit, tables=tables)
    return {"staged": len(staged_ids), "processed": processed}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RSRD2 Sync – lädt Wagenstammdaten ins SQLite-Cache.")
    parser.add_argument(
        "--wagons",
        nargs="+",
        help="Liste von Wagennummern (EVN). Alternativ via --wagons-file.",
    )
    parser.add_argument(
        "--wagons-file",
        help="Pfad zu einer Datei (eine Wagennummer pro Zeile).",
    )
    parser.add_argument(
        "--snapshots",
        action="store_true",
        default=False,
        help="Snapshots in historischer Tabelle speichern (Default: nur aktuelle Daten).",
    )
    parser.add_argument(
        "--mode",
        choices=["stage", "process", "full"],
        default="full",
        help="Verarbeitungsmodus: nur JSON laden, nur JSON verarbeiten oder beides.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optionales LIMIT für den Verarbeitungsschritt.",
    )
    parser.add_argument(
        "--env",
        default=DEFAULT_ENV,
        help="Umgebung (prd/tst oder live/test).",
    )
    return parser.parse_args()


def load_wagons_from_file(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"Wagennummern-Datei nicht gefunden: {path}")
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def main() -> None:
    args = parse_args()
    wagons = args.wagons or []
    if args.wagons_file:
        wagons.extend(load_wagons_from_file(Path(args.wagons_file)))
    tables = tables_for_env(args.env)
    stats = sync_wagons(
        wagons,
        keep_snapshots=args.snapshots,
        mode=args.mode,
        process_limit=args.limit,
        tables=tables,
        env=args.env,
    )
    print(
        f"RSRD2 Sync abgeschlossen – JSON geladen: {stats['staged']}, verarbeitet: {stats['processed']}"
    )


if __name__ == "__main__":
    main()
