from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import math
import sys
import time
from typing import Any
from uuid import uuid4

from app_excel_import.config import ExcelImportSettings
from app_excel_import.services.excel_reader import SheetContent, WorkbookContent

_FABRIC_REQUIRED_FIELDS = (
    "fabric_sql_server",
    "fabric_sql_database",
    "fabric_client_id",
    "fabric_tenant_id",
    "fabric_client_secret",
)
_CM_SHEET_NAME = "cm"
_CM_DROP_COLUMNS_FOR_CLEAN = {"column_1"}
_CM_ALLOWED_ROW_TYPES = {"header", "batch", "wagon"}
_CM_KEY_COLUMNS = (
    "contract_number_m3",
    "contract_number_customer",
    "rentalposition_m3",
    "remarks",
    "row_type",
    "wagon_number",
    "customer",
)
_CM_ROW_JSON_ALWAYS_COLUMNS = set(_CM_KEY_COLUMNS)
_CM_TECHNICAL_COLUMNS = {
    "business_key_hash",
    "record_hash",
    "valid_from_utc",
    "valid_to_utc",
    "first_seen_import_utc",
    "last_seen_import_utc",
    "is_current",
    "batch_id",
    "source_file",
    "source_sheet",
}


def _quote_identifier(identifier: str) -> str:
    escaped = str(identifier).replace("]", "]]")
    return f"[{escaped}]"


class FabricSqlWriter:
    def __init__(self, settings: ExcelImportSettings) -> None:
        self.settings = settings

    def missing_configuration(self) -> list[str]:
        missing: list[str] = []
        for field_name in _FABRIC_REQUIRED_FIELDS:
            value = getattr(self.settings, field_name, None)
            if not str(value or "").strip():
                missing.append(field_name)
        return missing

    def is_configured(self) -> bool:
        return len(self.missing_configuration()) == 0

    def insert_workbook(
        self,
        workbook: WorkbookContent,
        target_schema: str,
        target_table: str,
        source_file: str,
    ) -> tuple[str, int]:
        batch_id = f"fabric-{uuid4()}"
        total_rows = 0

        with self._connect() as conn:
            available_columns = self._load_target_columns(conn=conn, schema=target_schema, table=target_table)
            metadata_columns = [col for col in ("source_file", "source_sheet", "source_row_index", "batch_id") if col in available_columns]
            for sheet in workbook.sheets:
                data_columns = [column for column in sheet.headers if column in available_columns]
                insert_columns = metadata_columns + data_columns
                if not insert_columns:
                    continue

                sql = (
                    f"INSERT INTO {_quote_identifier(target_schema)}.{_quote_identifier(target_table)} "
                    f"({', '.join(_quote_identifier(column) for column in insert_columns)}) "
                    f"VALUES ({', '.join('?' for _ in insert_columns)})"
                )

                payload: list[tuple[Any, ...]] = []
                for row_index, row in enumerate(sheet.rows, start=1):
                    values: list[Any] = []
                    for column in metadata_columns:
                        if column == "source_file":
                            values.append(source_file)
                        elif column == "source_sheet":
                            values.append(sheet.name)
                        elif column == "source_row_index":
                            values.append(row_index)
                        elif column == "batch_id":
                            values.append(batch_id)
                    for column in data_columns:
                        values.append(self._to_db_value(column=column, value=row.get(column)))
                    payload.append(tuple(values))

                if payload:
                    cursor = conn.cursor()
                    cursor.fast_executemany = True
                    try:
                        cursor.executemany(sql, payload)
                    except Exception as exc:
                        raise RuntimeError(
                            f"Insert nach {target_schema}.{target_table} fehlgeschlagen: {exc}"
                        ) from exc
                    total_rows += len(payload)

            conn.commit()
        return batch_id, total_rows

    def import_contract_management_daily(
        self,
        workbook: WorkbookContent,
        target_schema: str,
        raw_table: str,
        clean_table: str,
        source_file: str,
    ) -> dict[str, Any]:
        sheet = self._find_sheet(workbook=workbook, sheet_name=_CM_SHEET_NAME)
        type_table_map = self._build_cm_type_table_map(clean_table=clean_table)
        structure_table = self._build_cm_structure_table(clean_table=clean_table)
        batch_id = f"fabric-{uuid4()}"
        import_ts = datetime.now(timezone.utc).replace(microsecond=0)
        hash_columns = self._build_hash_columns(headers=sheet.headers)
        raw_payload: list[tuple[Any, ...]] = []
        structure_payload: list[tuple[Any, ...]] = []
        typed_rows: dict[str, list[dict[str, Any]]] = {name: [] for name in _CM_ALLOWED_ROW_TYPES}
        row_sequence = 0

        for row_index, row in enumerate(sheet.rows, start=1):
            row_type = self._normalize_row_type(row.get("row_type"))
            if row_type not in _CM_ALLOWED_ROW_TYPES:
                continue

            business_key_hash = self._build_business_key_hash(row=row)
            record_hash = self._build_record_hash(row=row, columns=hash_columns)
            row_payload = self._build_row_json_payload(row=row, columns=hash_columns)
            row_json = json.dumps(
                row_payload,
                ensure_ascii=False,
                separators=(",", ":"),
            )

            raw_values = [
                import_ts,
                batch_id,
                source_file,
                sheet.name,
                row_index,
                business_key_hash,
                record_hash,
            ]
            raw_payload.append(tuple(raw_values))

            row_sequence += 1
            structure_payload.append(
                (
                    import_ts,
                    batch_id,
                    source_file,
                    sheet.name,
                    row_index,
                    row_sequence,
                    row_type,
                    type_table_map[row_type],
                    business_key_hash,
                    record_hash,
                    row_json,
                )
            )
            typed_rows[row_type].append(
                {
                    "row": row,
                    "business_key_hash": business_key_hash,
                    "record_hash": record_hash,
                }
            )

        deduplicated_rows = 0
        clean_new_rows = 0
        clean_unchanged_rows = 0
        clean_closed_rows = 0
        typed_stats: dict[str, dict[str, int]] = {}

        with self._connect() as conn:
            self._ensure_raw_table(conn=conn, schema=target_schema, table=raw_table, headers=sheet.headers)
            self._ensure_structure_table(conn=conn, schema=target_schema, table=structure_table)
            raw_inserted_rows = self._insert_raw_rows(
                conn=conn,
                schema=target_schema,
                table=raw_table,
                payload=raw_payload,
            )
            structure_inserted_rows = self._insert_structure_rows(
                conn=conn,
                schema=target_schema,
                table=structure_table,
                payload=structure_payload,
            )

            for row_type in ("header", "batch", "wagon"):
                rows_for_type = typed_rows.get(row_type) or []
                clean_columns = self._build_type_columns(
                    headers=sheet.headers,
                    rows=[record["row"] for record in rows_for_type],
                )
                self._validate_key_columns(clean_columns=clean_columns)
                business_specs = self._build_clean_business_specs(clean_columns=clean_columns)
                clean_by_key: dict[str, dict[str, Any]] = {}
                for record in rows_for_type:
                    row = record["row"]
                    business_key_hash = str(record["business_key_hash"])
                    record_hash = str(record["record_hash"])
                    clean_row = self._build_clean_row_values(
                        row=row,
                        clean_columns=clean_columns,
                        business_specs=business_specs,
                        hash_columns=hash_columns,
                    )
                    clean_by_key[business_key_hash] = {
                        "business_key_hash": business_key_hash,
                        "record_hash": record_hash,
                        "valid_from_utc": import_ts,
                        "valid_to_utc": None,
                        "first_seen_import_utc": import_ts,
                        "last_seen_import_utc": import_ts,
                        "is_current": 1,
                        "batch_id": batch_id,
                        "source_file": source_file,
                        "source_sheet": sheet.name,
                        "values": clean_row,
                    }

                stage_rows = list(clean_by_key.values())
                deduplicated_rows += len(stage_rows)
                typed_table = type_table_map[row_type]
                self._ensure_clean_table(
                    conn=conn,
                    schema=target_schema,
                    table=typed_table,
                    clean_columns=clean_columns,
                    business_specs=business_specs,
                )
                stats = self._merge_clean_rows(
                    conn=conn,
                    schema=target_schema,
                    table=typed_table,
                    clean_columns=clean_columns,
                    business_specs=business_specs,
                    stage_rows=stage_rows,
                )
                clean_new_rows += int(stats["clean_new_rows"])
                clean_unchanged_rows += int(stats["clean_unchanged_rows"])
                clean_closed_rows += int(stats["clean_closed_rows"])
                typed_stats[row_type] = {
                    "input_rows": len(rows_for_type),
                    "deduplicated_rows": len(stage_rows),
                    **stats,
                }

            conn.commit()

        return {
            "batch_id": batch_id,
            "import_timestamp_utc": import_ts.isoformat(),
            "input_rows": len(raw_payload),
            "raw_inserted_rows": raw_inserted_rows,
            "structure_inserted_rows": structure_inserted_rows,
            "deduplicated_rows": deduplicated_rows,
            "clean_new_rows": clean_new_rows,
            "clean_unchanged_rows": clean_unchanged_rows,
            "clean_closed_rows": clean_closed_rows,
            "sheet_name": sheet.name,
            "structure_table": structure_table,
            "header_table": type_table_map["header"],
            "batch_table": type_table_map["batch"],
            "wagon_table": type_table_map["wagon"],
            "typed_stats": typed_stats,
        }

    def export_contract_management_snapshot(
        self,
        target_schema: str,
        clean_table: str,
        snapshot_at_utc: datetime | None = None,
    ) -> dict[str, Any]:
        structure_table = self._build_cm_structure_table(clean_table=clean_table)
        with self._connect() as conn:
            cur = conn.cursor()
            if snapshot_at_utc is None:
                cur.execute(
                    f"""
                    SELECT TOP 1 import_timestamp_utc, batch_id
                    FROM {_quote_identifier(target_schema)}.{_quote_identifier(structure_table)}
                    ORDER BY import_timestamp_utc DESC, batch_id DESC
                    """
                )
                selected = cur.fetchone()
            else:
                target_snapshot = snapshot_at_utc.astimezone(timezone.utc).replace(tzinfo=None)
                cur.execute(
                    f"""
                    SELECT TOP 1 import_timestamp_utc, batch_id
                    FROM {_quote_identifier(target_schema)}.{_quote_identifier(structure_table)}
                    WHERE import_timestamp_utc <= ?
                    ORDER BY import_timestamp_utc DESC, batch_id DESC
                    """,
                    (target_snapshot,),
                )
                selected = cur.fetchone()

            if not selected or selected[0] is None:
                raise RuntimeError(f"Keine Snapshot-Daten in {target_schema}.{structure_table} fuer Export vorhanden.")

            snapshot_value = selected[0]
            selected_batch_id = str(selected[1])
            if isinstance(snapshot_value, datetime):
                effective_snapshot = snapshot_value.replace(tzinfo=timezone.utc)
            else:
                # pragma: no cover - driver dependent types
                effective_snapshot = datetime.fromisoformat(str(snapshot_value)).replace(tzinfo=timezone.utc)

            cur.execute(
                f"""
                SELECT
                    source_file,
                    row_json
                FROM {_quote_identifier(target_schema)}.{_quote_identifier(structure_table)}
                WHERE batch_id = ?
                ORDER BY row_sequence ASC
                """,
                (selected_batch_id,),
            )
            raw_rows = cur.fetchall()
            if not raw_rows:
                raise RuntimeError(
                    f"Snapshot-Batch ohne Zeilen gefunden ({target_schema}.{structure_table}, batch_id={selected_batch_id})."
                )

            headers: list[str] = []
            seen_headers: set[str] = set()
            rows: list[dict[str, Any]] = []
            source_file = None
            for db_row in raw_rows:
                if source_file is None and db_row[0] is not None:
                    source_file = str(db_row[0])
                payload = db_row[1]
                if not isinstance(payload, str) or not payload.strip():
                    continue
                try:
                    decoded = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                if not isinstance(decoded, dict):
                    continue
                ordered_row = {str(key): decoded.get(key) for key in decoded.keys()}
                for key in ordered_row.keys():
                    if key in seen_headers:
                        continue
                    seen_headers.add(key)
                    headers.append(key)
                rows.append(ordered_row)

            if not rows:
                raise RuntimeError(
                    f"Snapshot-Batch enthaelt keine gueltigen Row-Payloads ({target_schema}.{structure_table}, batch_id={selected_batch_id})."
                )

            return {
                "headers": headers,
                "rows": rows,
                "effective_snapshot": effective_snapshot,
                "source_file": source_file,
                "batch_id": selected_batch_id,
                "structure_table": structure_table,
            }

    def _to_db_value(self, column: str, value: Any) -> Any:
        if value is None:
            return None
        if column == "excess_mileage_amount_eur":
            return self._to_decimal(value)
        if column == "excess_mileage_basis_km":
            return self._to_int(value)
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (dict, list, tuple)):
            return str(value)
        return value if isinstance(value, (int, float, Decimal)) else str(value)

    def _to_decimal(self, value: Any) -> float | None:
        text = str(value).strip()
        if not text:
            return None
        text = text.replace("'", "").replace(" ", "").replace(",", ".")
        try:
            number = float(text)
            if not math.isfinite(number):
                return None
            return number
        except ValueError:
            return None

    def _to_int(self, value: Any) -> int | None:
        text = str(value).strip()
        if not text:
            return None
        text = text.replace("'", "").replace(" ", "").replace(".", "")
        if not text.isdigit():
            return None
        return int(text)

    def _to_db_text(self, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, Decimal):
            return format(value, "f")
        if isinstance(value, (int, float)):
            return format(value, "g")
        text = str(value).strip()
        return text or None

    def _find_sheet(self, workbook: WorkbookContent, sheet_name: str) -> SheetContent:
        for sheet in workbook.sheets:
            if sheet.name.strip().lower() == sheet_name.strip().lower():
                return sheet
        available = ", ".join(sheet.name for sheet in workbook.sheets) or "<none>"
        raise RuntimeError(f"CM-Sheet '{sheet_name}' nicht gefunden. Verfuegbar: {available}")

    def _build_hash_columns(self, headers: list[str]) -> list[str]:
        columns = [header for header in headers if header not in _CM_DROP_COLUMNS_FOR_CLEAN]
        if "remarks" in columns:
            columns = [column for column in columns if column != "remarks"] + ["remarks"]
        return columns

    def _build_cm_structure_table(self, clean_table: str) -> str:
        return f"{clean_table}_Structure"

    def _build_cm_type_table_map(self, clean_table: str) -> dict[str, str]:
        return {
            "header": f"{clean_table}_Header",
            "batch": f"{clean_table}_Batch",
            "wagon": f"{clean_table}_Wagon",
        }

    def _normalize_row_type(self, raw_value: Any) -> str:
        value = str(raw_value or "").strip().lower()
        if value in _CM_ALLOWED_ROW_TYPES:
            return value
        return ""

    def _has_business_value(self, value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            return bool(value.strip())
        return True

    def _build_type_columns(
        self,
        headers: list[str],
        rows: list[dict[str, Any]],
    ) -> list[str]:
        if not rows:
            return [column for column in _CM_KEY_COLUMNS if column in headers]

        required_columns = set(_CM_KEY_COLUMNS)
        non_empty_columns: set[str] = set(required_columns)
        for row in rows:
            for column, value in row.items():
                if column in _CM_DROP_COLUMNS_FOR_CLEAN:
                    continue
                if self._has_business_value(value):
                    non_empty_columns.add(column)

        columns: list[str] = [
            column
            for column in headers
            if column not in _CM_DROP_COLUMNS_FOR_CLEAN and column in non_empty_columns
        ]
        for required in _CM_KEY_COLUMNS:
            if required in columns:
                continue
            if required in headers:
                columns.append(required)
        return columns

    def _validate_key_columns(self, clean_columns: list[str]) -> None:
        missing = [column for column in _CM_KEY_COLUMNS if column not in clean_columns]
        if missing:
            raise RuntimeError("CM Key-Spalten fehlen: " + ", ".join(missing))

    def _canonicalize(self, value: Any) -> str:
        text = self._to_db_text(value)
        if text is None:
            return ""
        return " ".join(text.split())

    def _build_business_key_hash(self, row: dict[str, Any]) -> str:
        payload = "\u001f".join(self._canonicalize(row.get(column)).lower() for column in _CM_KEY_COLUMNS)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _build_record_hash(self, row: dict[str, Any], columns: list[str]) -> str:
        payload = "\u001f".join(self._canonicalize(row.get(column)) for column in columns)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _build_row_json_payload(self, row: dict[str, Any], columns: list[str]) -> dict[str, str | None]:
        payload: dict[str, str | None] = {}
        for name in columns:
            text_value = self._to_db_text(row.get(name))
            if text_value is None and name not in _CM_ROW_JSON_ALWAYS_COLUMNS:
                continue
            payload[name] = text_value
        return payload

    def _build_clean_business_specs(self, clean_columns: list[str]) -> dict[str, tuple[str, int | None]]:
        key_limits: dict[str, int] = {
            "contract_number_m3": 120,
            "contract_number_customer": 120,
            "rentalposition_m3": 120,
            "row_type": 30,
            "wagon_number": 120,
            "customer": 200,
            "remarks": 2000,
        }
        long_text_tokens = ("remark", "clause", "assumption", "reason", "security", "jurisdiction")
        specs: dict[str, tuple[str, int | None]] = {}
        for column in clean_columns:
            if column == "row_json":
                specs[column] = ("NVARCHAR(MAX) NULL", None)
                continue
            if column == "excess_mileage_amount_eur":
                specs[column] = ("DECIMAL(18,6) NULL", None)
                continue
            if column == "excess_mileage_basis_km":
                specs[column] = ("INT NULL", None)
                continue
            if column in key_limits:
                limit = key_limits[column]
                specs[column] = (f"NVARCHAR({limit}) NULL", limit)
                continue
            if any(token in column for token in long_text_tokens):
                specs[column] = ("NVARCHAR(2000) NULL", 2000)
                continue
            if "date" in column:
                specs[column] = ("NVARCHAR(40) NULL", 40)
                continue
            if column.endswith("_eur") or "_km" in column or column.endswith("_pa"):
                specs[column] = ("NVARCHAR(80) NULL", 80)
                continue
            specs[column] = ("NVARCHAR(500) NULL", 500)
        return specs

    def _build_clean_row_values(
        self,
        row: dict[str, Any],
        clean_columns: list[str],
        business_specs: dict[str, tuple[str, int | None]],
        hash_columns: list[str],
    ) -> dict[str, Any]:
        values: dict[str, Any] = {}
        for column in clean_columns:
            if column == "row_json":
                values[column] = json.dumps(
                    self._build_row_json_payload(row=row, columns=hash_columns),
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                continue
            if column == "excess_mileage_amount_eur":
                values[column] = self._to_decimal(row.get(column))
                continue
            if column == "excess_mileage_basis_km":
                values[column] = self._to_int(row.get(column))
                continue
            text = self._to_db_text(row.get(column))
            max_len = business_specs[column][1]
            if text is not None and max_len is not None and len(text) > max_len:
                text = text[:max_len]
            values[column] = text
        return values

    def _table_exists(self, conn: Any, schema: str, table: str) -> bool:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            """,
            (schema, table),
        )
        return cur.fetchone() is not None

    def _load_target_columns(self, conn: Any, schema: str, table: str) -> set[str]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT LOWER(COLUMN_NAME)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            """,
            (schema, table),
        )
        rows = cur.fetchall()
        columns = {str(row[0]).strip().lower() for row in rows if row and row[0] is not None}
        if not columns:
            raise RuntimeError(f"Zieltabelle nicht gefunden oder ohne Spalten: {schema}.{table}")
        return columns

    def _load_target_columns_ordered(self, conn: Any, schema: str, table: str) -> list[str]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT LOWER(COLUMN_NAME)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            """,
            (schema, table),
        )
        columns = [str(row[0]).strip().lower() for row in cur.fetchall() if row and row[0] is not None]
        if not columns:
            raise RuntimeError(f"Zieltabelle nicht gefunden oder ohne Spalten: {schema}.{table}")
        return columns

    def _index_exists(self, conn: Any, schema: str, table: str, index_name: str) -> bool:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM sys.indexes i
            INNER JOIN sys.tables t ON i.object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = ? AND t.name = ? AND i.name = ?
            """,
            (schema, table, index_name),
        )
        return cur.fetchone() is not None

    def _create_index_if_missing(self, conn: Any, schema: str, table: str, index_name: str, columns_sql: str) -> None:
        if self._index_exists(conn=conn, schema=schema, table=table, index_name=index_name):
            return
        cur = conn.cursor()
        cur.execute(
            f"CREATE INDEX {_quote_identifier(index_name)} ON {_quote_identifier(schema)}.{_quote_identifier(table)} ({columns_sql})"
        )

    def _ensure_raw_table(self, conn: Any, schema: str, table: str, headers: list[str]) -> None:
        cur = conn.cursor()
        created_table = False
        if not self._table_exists(conn=conn, schema=schema, table=table):
            created_table = True
            base_columns = [
                "[raw_id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY",
                "[import_timestamp_utc] DATETIME2(0) NOT NULL",
                "[batch_id] NVARCHAR(64) NOT NULL",
                "[source_file] NVARCHAR(1024) NULL",
                "[source_sheet] NVARCHAR(255) NULL",
                "[source_row_index] INT NULL",
                "[business_key_hash] CHAR(64) NOT NULL",
                "[record_hash] CHAR(64) NOT NULL",
                "[row_json] NVARCHAR(MAX) NULL",
            ]
            cur.execute(
                f"""
                CREATE TABLE {_quote_identifier(schema)}.{_quote_identifier(table)} (
                    {", ".join(base_columns)}
                )
                """
            )
        else:
            existing = self._load_target_columns(conn=conn, schema=schema, table=table)
            for required in ("row_json",):
                if required in existing:
                    continue
                cur.execute(
                    f"""
                    ALTER TABLE {_quote_identifier(schema)}.{_quote_identifier(table)}
                    ADD {_quote_identifier(required)} NVARCHAR(MAX) NULL
                    """
                )

        if created_table:
            index_prefix = f"IX_{table}"[:80]
            self._create_index_if_missing(
                conn=conn,
                schema=schema,
                table=table,
                index_name=f"{index_prefix}_import_ts",
                columns_sql="[import_timestamp_utc]",
            )
            self._create_index_if_missing(
                conn=conn,
                schema=schema,
                table=table,
                index_name=f"{index_prefix}_key_hash",
                columns_sql="[business_key_hash]",
            )

    def _ensure_structure_table(self, conn: Any, schema: str, table: str) -> None:
        cur = conn.cursor()
        created_table = False
        if not self._table_exists(conn=conn, schema=schema, table=table):
            created_table = True
            base_columns = [
                "[structure_id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY",
                "[import_timestamp_utc] DATETIME2(0) NOT NULL",
                "[batch_id] NVARCHAR(64) NOT NULL",
                "[source_file] NVARCHAR(1024) NULL",
                "[source_sheet] NVARCHAR(255) NULL",
                "[source_row_index] INT NULL",
                "[row_sequence] INT NOT NULL",
                "[row_type] NVARCHAR(30) NOT NULL",
                "[entity_table] NVARCHAR(255) NOT NULL",
                "[business_key_hash] CHAR(64) NOT NULL",
                "[record_hash] CHAR(64) NOT NULL",
                "[row_json] NVARCHAR(MAX) NULL",
            ]
            cur.execute(
                f"""
                CREATE TABLE {_quote_identifier(schema)}.{_quote_identifier(table)} (
                    {", ".join(base_columns)}
                )
                """
            )
        else:
            existing = self._load_target_columns(conn=conn, schema=schema, table=table)
            additions: list[tuple[str, str]] = [
                ("entity_table", "NVARCHAR(255) NULL"),
                ("row_json", "NVARCHAR(MAX) NULL"),
            ]
            for column, sql_type in additions:
                if column in existing:
                    continue
                cur.execute(
                    f"""
                    ALTER TABLE {_quote_identifier(schema)}.{_quote_identifier(table)}
                    ADD {_quote_identifier(column)} {sql_type}
                    """
                )

        if created_table:
            index_prefix = f"IX_{table}"[:80]
            self._create_index_if_missing(
                conn=conn,
                schema=schema,
                table=table,
                index_name=f"{index_prefix}_import_ts",
                columns_sql="[import_timestamp_utc]",
            )
            self._create_index_if_missing(
                conn=conn,
                schema=schema,
                table=table,
                index_name=f"{index_prefix}_batch_seq",
                columns_sql="[batch_id], [row_sequence]",
            )
            self._create_index_if_missing(
                conn=conn,
                schema=schema,
                table=table,
                index_name=f"{index_prefix}_hash",
                columns_sql="[business_key_hash], [record_hash]",
            )

    def _ensure_clean_table(
        self,
        conn: Any,
        schema: str,
        table: str,
        clean_columns: list[str],
        business_specs: dict[str, tuple[str, int | None]],
    ) -> None:
        cur = conn.cursor()
        created_table = False
        if not self._table_exists(conn=conn, schema=schema, table=table):
            created_table = True
            business_defs = [
                f"{_quote_identifier(column)} {business_specs[column][0]}" for column in clean_columns
            ]
            technical_defs = [
                "[business_key_hash] CHAR(64) NOT NULL",
                "[record_hash] CHAR(64) NOT NULL",
                "[valid_from_utc] DATETIME2(0) NOT NULL",
                "[valid_to_utc] DATETIME2(0) NULL",
                "[first_seen_import_utc] DATETIME2(0) NOT NULL",
                "[last_seen_import_utc] DATETIME2(0) NOT NULL",
                "[is_current] BIT NOT NULL",
                "[batch_id] NVARCHAR(64) NOT NULL",
                "[source_file] NVARCHAR(1024) NULL",
                "[source_sheet] NVARCHAR(255) NULL",
                f"CONSTRAINT {_quote_identifier(f'PK_{table}_hash'[:120])} PRIMARY KEY ([business_key_hash], [record_hash])",
            ]
            cur.execute(
                f"""
                CREATE TABLE {_quote_identifier(schema)}.{_quote_identifier(table)} (
                    {", ".join(business_defs + technical_defs)}
                )
                """
            )
        else:
            existing = self._load_target_columns(conn=conn, schema=schema, table=table)
            for column in clean_columns:
                if column in existing:
                    continue
                cur.execute(
                    f"""
                    ALTER TABLE {_quote_identifier(schema)}.{_quote_identifier(table)}
                    ADD {_quote_identifier(column)} {business_specs[column][0]}
                    """
                )
            technical_additions: list[tuple[str, str]] = [
                ("business_key_hash", "CHAR(64) NULL"),
                ("record_hash", "CHAR(64) NULL"),
                ("valid_from_utc", "DATETIME2(0) NULL"),
                ("valid_to_utc", "DATETIME2(0) NULL"),
                ("first_seen_import_utc", "DATETIME2(0) NULL"),
                ("last_seen_import_utc", "DATETIME2(0) NULL"),
                ("is_current", "BIT NULL"),
                ("batch_id", "NVARCHAR(64) NULL"),
                ("source_file", "NVARCHAR(1024) NULL"),
                ("source_sheet", "NVARCHAR(255) NULL"),
            ]
            for column, sql_type in technical_additions:
                if column in existing:
                    continue
                cur.execute(
                    f"""
                    ALTER TABLE {_quote_identifier(schema)}.{_quote_identifier(table)}
                    ADD {_quote_identifier(column)} {sql_type}
                    """
                )

        if created_table:
            index_prefix = f"IX_{table}"[:80]
            self._create_index_if_missing(
                conn=conn,
                schema=schema,
                table=table,
                index_name=f"{index_prefix}_current",
                columns_sql="[is_current], [valid_from_utc]",
            )
            self._create_index_if_missing(
                conn=conn,
                schema=schema,
                table=table,
                index_name=f"{index_prefix}_key_hash",
                columns_sql="[business_key_hash], [record_hash]",
            )
            business_index_columns = [column for column in _CM_KEY_COLUMNS if column in clean_columns]
            if business_index_columns:
                columns_sql = ", ".join(_quote_identifier(column) for column in business_index_columns)
                self._create_index_if_missing(
                    conn=conn,
                    schema=schema,
                    table=table,
                    index_name=f"{index_prefix}_business",
                    columns_sql=columns_sql,
                )

    def _insert_raw_rows(
        self,
        conn: Any,
        schema: str,
        table: str,
        payload: list[tuple[Any, ...]],
    ) -> int:
        if not payload:
            return 0
        columns = [
            "import_timestamp_utc",
            "batch_id",
            "source_file",
            "source_sheet",
            "source_row_index",
            "business_key_hash",
            "record_hash",
        ]
        sql = (
            f"INSERT INTO {_quote_identifier(schema)}.{_quote_identifier(table)} "
            f"({', '.join(_quote_identifier(column) for column in columns)}) "
            f"VALUES ({', '.join('?' for _ in columns)})"
        )
        cursor = conn.cursor()
        cursor.fast_executemany = True
        cursor.executemany(sql, payload)
        return len(payload)

    def _insert_structure_rows(
        self,
        conn: Any,
        schema: str,
        table: str,
        payload: list[tuple[Any, ...]],
    ) -> int:
        if not payload:
            return 0
        columns = [
            "import_timestamp_utc",
            "batch_id",
            "source_file",
            "source_sheet",
            "source_row_index",
            "row_sequence",
            "row_type",
            "entity_table",
            "business_key_hash",
            "record_hash",
            "row_json",
        ]
        sql = (
            f"INSERT INTO {_quote_identifier(schema)}.{_quote_identifier(table)} "
            f"({', '.join(_quote_identifier(column) for column in columns)}) "
            f"VALUES ({', '.join('?' for _ in columns)})"
        )
        cursor = conn.cursor()
        cursor.fast_executemany = True
        cursor.executemany(sql, payload)
        return len(payload)

    def _merge_clean_rows(
        self,
        conn: Any,
        schema: str,
        table: str,
        clean_columns: list[str],
        business_specs: dict[str, tuple[str, int | None]],
        stage_rows: list[dict[str, Any]],
    ) -> dict[str, int]:
        if not stage_rows:
            return {"clean_new_rows": 0, "clean_unchanged_rows": 0, "clean_closed_rows": 0}

        target_ref = f"{_quote_identifier(schema)}.{_quote_identifier(table)}"
        stage_by_pair: dict[tuple[str, str], dict[str, Any]] = {
            (str(row["business_key_hash"]), str(row["record_hash"])): row for row in stage_rows
        }
        stage_cursor = conn.cursor()
        stage_cursor.execute("DROP TABLE IF EXISTS #cm_stage_min")
        stage_cursor.execute("DROP TABLE IF EXISTS #cm_changed_keys")
        stage_cursor.execute(
            """
            CREATE TABLE #cm_stage_min (
                business_key_hash CHAR(64) NOT NULL,
                record_hash CHAR(64) NOT NULL,
                valid_from_utc DATETIME2(0) NOT NULL,
                last_seen_import_utc DATETIME2(0) NOT NULL,
                batch_id NVARCHAR(64) NOT NULL,
                source_file NVARCHAR(1024) NULL,
                source_sheet NVARCHAR(255) NULL
            )
            """
        )
        stage_payload = [
            (
                row["business_key_hash"],
                row["record_hash"],
                row["valid_from_utc"].replace(tzinfo=None),
                row["last_seen_import_utc"].replace(tzinfo=None),
                row["batch_id"],
                row["source_file"],
                row["source_sheet"],
            )
            for row in stage_rows
        ]
        stage_cursor.fast_executemany = True
        stage_cursor.executemany(
            """
            INSERT INTO #cm_stage_min
            (business_key_hash, record_hash, valid_from_utc, last_seen_import_utc, batch_id, source_file, source_sheet)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            stage_payload,
        )

        unchanged_cursor = conn.cursor()
        unchanged_cursor.execute(
            f"""
            UPDATE tgt
            SET
                tgt.last_seen_import_utc = src.last_seen_import_utc,
                tgt.batch_id = src.batch_id,
                tgt.source_file = src.source_file,
                tgt.source_sheet = src.source_sheet,
                tgt.valid_to_utc = NULL,
                tgt.is_current = 1
            FROM {target_ref} tgt
            INNER JOIN #cm_stage_min src
                ON src.business_key_hash = tgt.business_key_hash
               AND src.record_hash = tgt.record_hash
            WHERE tgt.is_current = 1
            """
        )
        unchanged_count = int(unchanged_cursor.rowcount or 0)

        changed_cursor = conn.cursor()
        changed_cursor.execute(
            f"""
            SELECT DISTINCT src.business_key_hash, src.valid_from_utc
            INTO #cm_changed_keys
            FROM #cm_stage_min src
            LEFT JOIN {target_ref} curr
              ON curr.business_key_hash = src.business_key_hash
             AND curr.record_hash = src.record_hash
             AND curr.is_current = 1
            WHERE curr.business_key_hash IS NULL
            """
        )
        changed_count_cursor = conn.cursor()
        changed_count_cursor.execute("SELECT COUNT(*) FROM #cm_changed_keys")
        changed_count = int(changed_count_cursor.fetchone()[0] or 0)

        if changed_count > 0:
            close_cursor = conn.cursor()
            close_cursor.execute(
                f"""
                UPDATE tgt
                SET
                    tgt.is_current = 0,
                    tgt.valid_to_utc = ck.valid_from_utc
                FROM {target_ref} tgt
                INNER JOIN #cm_changed_keys ck
                    ON ck.business_key_hash = tgt.business_key_hash
                WHERE tgt.is_current = 1
                  AND (tgt.valid_to_utc IS NULL OR tgt.valid_to_utc > ck.valid_from_utc)
                """
            )

            reactivate_cursor = conn.cursor()
            reactivate_cursor.execute(
                f"""
                UPDATE tgt
                SET
                    tgt.valid_to_utc = NULL,
                    tgt.last_seen_import_utc = src.last_seen_import_utc,
                    tgt.batch_id = src.batch_id,
                    tgt.source_file = src.source_file,
                    tgt.source_sheet = src.source_sheet,
                    tgt.is_current = 1
                FROM {target_ref} tgt
                INNER JOIN #cm_stage_min src
                    ON src.business_key_hash = tgt.business_key_hash
                   AND src.record_hash = tgt.record_hash
                INNER JOIN #cm_changed_keys ck
                    ON ck.business_key_hash = src.business_key_hash
                WHERE tgt.is_current = 0
                """
            )
            reactivated_count = int(reactivate_cursor.rowcount or 0)
        else:
            reactivated_count = 0

        new_pairs_cursor = conn.cursor()
        new_pairs_cursor.execute(
            f"""
            SELECT src.business_key_hash, src.record_hash
            FROM #cm_stage_min src
            INNER JOIN #cm_changed_keys ck
                ON ck.business_key_hash = src.business_key_hash
            LEFT JOIN {target_ref} existing
              ON existing.business_key_hash = src.business_key_hash
             AND existing.record_hash = src.record_hash
            WHERE existing.business_key_hash IS NULL
            """
        )
        new_pairs = [(str(row[0]), str(row[1])) for row in new_pairs_cursor.fetchall()]

        insert_columns = clean_columns + [
            "business_key_hash",
            "record_hash",
            "valid_from_utc",
            "valid_to_utc",
            "first_seen_import_utc",
            "last_seen_import_utc",
            "is_current",
            "batch_id",
            "source_file",
            "source_sheet",
        ]
        if new_pairs:
            insert_sql = (
                f"INSERT INTO {target_ref} "
                f"({', '.join(_quote_identifier(column) for column in insert_columns)}) "
                f"VALUES ({', '.join('?' for _ in insert_columns)})"
            )
            insert_payload: list[tuple[Any, ...]] = []
            for key_hash, record_hash in new_pairs:
                row = stage_by_pair[(key_hash, record_hash)]
                values = [row["values"].get(column) for column in clean_columns]
                values.extend(
                    [
                        row["business_key_hash"],
                        row["record_hash"],
                        row["valid_from_utc"].replace(tzinfo=None),
                        row["valid_to_utc"],
                        row["first_seen_import_utc"].replace(tzinfo=None),
                        row["last_seen_import_utc"].replace(tzinfo=None),
                        row["is_current"],
                        row["batch_id"],
                        row["source_file"],
                        row["source_sheet"],
                    ]
                )
                insert_payload.append(tuple(values))

            insert_cursor = conn.cursor()
            insert_cursor.fast_executemany = True
            insert_cursor.executemany(insert_sql, insert_payload)

        return {
            "clean_new_rows": len(new_pairs) + reactivated_count,
            "clean_unchanged_rows": unchanged_count,
            "clean_closed_rows": changed_count,
        }

    def _fabric_server_with_port(self) -> str:
        server = str(self.settings.fabric_sql_server or "").strip()
        if server.startswith("tcp:"):
            server = server[4:]
        if "," in server:
            return f"tcp:{server}"
        return f"tcp:{server},{self.settings.fabric_sql_port}"

    def _build_connection_string(self) -> str:
        return (
            f"Driver={{{self.settings.fabric_sql_driver}}};"
            f"Server={self._fabric_server_with_port()};"
            f"Database={str(self.settings.fabric_sql_database or '').strip()};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Authentication=ActiveDirectoryServicePrincipal;"
            f"Authority Id={str(self.settings.fabric_tenant_id or '').strip()};"
            f"UID={str(self.settings.fabric_client_id or '').strip()};"
            f"PWD={str(self.settings.fabric_client_secret or '').strip()};"
        )

    def _connect(self) -> Any:
        missing = self.missing_configuration()
        if missing:
            raise RuntimeError("Fehlende Fabric-Konfiguration: " + ", ".join(missing))
        try:
            import pyodbc  # type: ignore
        except Exception as exc:
            python_bin = sys.executable
            raise RuntimeError(
                "Python-Paket 'pyodbc' kann nicht geladen werden. "
                f"Aktives Python: {python_bin}. "
                f"Installiere mit: {python_bin} -m pip install pyodbc. "
                f"Import-Fehler: {exc}"
            ) from exc

        timeout = self.settings.fabric_sql_timeout
        conn_str = self._build_connection_string()
        retries = 3
        retry_delay = 1.5
        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                return pyodbc.connect(conn_str, timeout=timeout)
            except Exception as exc:  # pragma: no cover - network/driver dependent
                last_exc = exc
                message = str(exc).lower()
                transient = (
                    "hyt00" in message
                    or "login timeout expired" in message
                    or "08001" in message
                    or "network-related" in message
                )
                if not transient or attempt >= retries:
                    break
                time.sleep(retry_delay * attempt)
        raise RuntimeError(f"Fabric SQL Verbindung fehlgeschlagen: {last_exc}") from last_exc
