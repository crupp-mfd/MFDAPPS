from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3
from typing import Any, Literal
from uuid import uuid4

from app_excel_import.services.excel_reader import WorkbookContent


@dataclass(frozen=True)
class SheetWriteResult:
    sheet_name: str
    table_name: str
    inserted_rows: int


@dataclass(frozen=True)
class ImportWriteResult:
    batch_id: str
    database_path: Path
    sheets: list[SheetWriteResult]
    total_rows: int


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _sanitize_identifier(raw: str, fallback: str) -> str:
    text = raw.strip().lower()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9_]", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    if not text:
        text = fallback
    if text[0].isdigit():
        text = f"col_{text}"
    return text


class SQLiteImportWriter:
    def __init__(self, sqlite_path: Path) -> None:
        self.sqlite_path = sqlite_path

    def build_table_name(self, prefix: str, sheet_name: str) -> str:
        safe_prefix = _sanitize_identifier(prefix, "excel")
        safe_sheet = _sanitize_identifier(sheet_name, "sheet")
        combined = f"{safe_prefix}{safe_sheet}" if safe_prefix.endswith("_") else f"{safe_prefix}_{safe_sheet}"
        return _sanitize_identifier(combined, "excel_sheet")

    def write(
        self,
        workbook: WorkbookContent,
        source_file: str,
        table_prefix: str,
        if_exists: Literal["append", "replace"],
    ) -> ImportWriteResult:
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        batch_id = f"batch-{uuid4()}"
        created_at = datetime.now(timezone.utc).isoformat()

        with sqlite3.connect(self.sqlite_path) as connection:
            self._ensure_metadata_tables(connection)

            connection.execute(
                """
                INSERT INTO import_batches (batch_id, source_file, created_at_utc, status, sheet_count, total_rows)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (batch_id, source_file, created_at, "running", len(workbook.sheets), 0),
            )

            write_results: list[SheetWriteResult] = []
            total_rows = 0
            for sheet in workbook.sheets:
                table_name = self.build_table_name(table_prefix, sheet.name)
                if if_exists == "replace":
                    connection.execute(f"DROP TABLE IF EXISTS {_quote_identifier(table_name)}")

                self._ensure_target_table(connection, table_name=table_name, columns=sheet.headers)
                inserted_rows = self._insert_rows(
                    connection=connection,
                    table_name=table_name,
                    columns=sheet.headers,
                    rows=sheet.rows,
                    batch_id=batch_id,
                    sheet_name=sheet.name,
                )
                total_rows += inserted_rows
                write_results.append(SheetWriteResult(sheet_name=sheet.name, table_name=table_name, inserted_rows=inserted_rows))

                connection.execute(
                    """
                    INSERT INTO import_batch_sheets (batch_id, sheet_name, table_name, inserted_rows)
                    VALUES (?, ?, ?, ?)
                    """,
                    (batch_id, sheet.name, table_name, inserted_rows),
                )

            connection.execute(
                """
                UPDATE import_batches
                SET status = ?, total_rows = ?
                WHERE batch_id = ?
                """,
                ("completed", total_rows, batch_id),
            )
            connection.commit()

        return ImportWriteResult(
            batch_id=batch_id,
            database_path=self.sqlite_path,
            sheets=write_results,
            total_rows=total_rows,
        )

    def _ensure_metadata_tables(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS import_batches (
                batch_id TEXT PRIMARY KEY,
                source_file TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,
                status TEXT NOT NULL,
                sheet_count INTEGER NOT NULL,
                total_rows INTEGER NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS import_batch_sheets (
                batch_id TEXT NOT NULL,
                sheet_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                inserted_rows INTEGER NOT NULL
            )
            """
        )

    def _ensure_target_table(self, connection: sqlite3.Connection, table_name: str, columns: list[str]) -> None:
        dynamic_columns = ",\n".join(f"{_quote_identifier(column)} TEXT" for column in columns)
        metadata_columns = '"_batch_id" TEXT NOT NULL,\n"_source_sheet" TEXT NOT NULL,\n"_row_index" INTEGER NOT NULL'
        if dynamic_columns:
            ddl_columns = f"{metadata_columns},\n{dynamic_columns}"
        else:
            ddl_columns = metadata_columns
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_quote_identifier(table_name)} (
                {ddl_columns}
            )
            """
        )

    def _insert_rows(
        self,
        connection: sqlite3.Connection,
        table_name: str,
        columns: list[str],
        rows: list[dict[str, Any]],
        batch_id: str,
        sheet_name: str,
    ) -> int:
        if not rows:
            return 0

        metadata_columns = ["_batch_id", "_source_sheet", "_row_index"]
        insert_columns = metadata_columns + columns
        placeholders = ", ".join(["?"] * len(insert_columns))
        quoted_columns = ", ".join(_quote_identifier(column) for column in insert_columns)
        sql = f"INSERT INTO {_quote_identifier(table_name)} ({quoted_columns}) VALUES ({placeholders})"

        payload: list[tuple[Any, ...]] = []
        for row_index, row in enumerate(rows, start=1):
            values = [self._to_sql_value(row.get(column)) for column in columns]
            payload.append((batch_id, sheet_name, row_index, *values))

        connection.executemany(sql, payload)
        return len(payload)

    def _to_sql_value(self, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (dict, list, tuple)):
            return json.dumps(value, ensure_ascii=False, default=str)
        return str(value)
