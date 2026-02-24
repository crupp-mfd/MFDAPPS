from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field


class PreviewRequest(BaseModel):
    file_path: str = Field(..., description="Absolute or project-relative path to the workbook")
    sheet_names: list[str] | None = Field(default=None, description="Optional explicit list of sheet names")
    header_row: int = Field(default=1, ge=1, description="1-based row number that contains headers")
    skip_rows: int = Field(default=0, ge=0, description="Rows to skip after the header row")
    max_rows_per_sheet: int | None = Field(default=None, ge=1, description="Optional row cap per sheet")


class SheetPreview(BaseModel):
    sheet_name: str
    columns: list[str]
    total_rows: int
    sample_rows: list[dict[str, object | None]]


class WorkbookPreviewResponse(BaseModel):
    file_path: str
    sheet_count: int
    sheets: list[SheetPreview]


class TransferTestResponse(BaseModel):
    status: Literal["ok"]
    message: str
    tested_at_utc: str
    database_path: str


class GoFabricResponse(BaseModel):
    status: Literal["ok"]
    message: str
    started_at_utc: str
    finished_at_utc: str
    workbook_path: str
    sheet_name: str
    raw_table: str
    target_table: str
    batch_id: str
    import_timestamp_utc: str
    input_rows: int
    deduplicated_rows: int
    raw_inserted_rows: int
    clean_new_rows: int
    clean_unchanged_rows: int
    clean_closed_rows: int
    inserted_rows: int


class ImportRequest(PreviewRequest):
    target_table_prefix: str = Field(default="excel_", description="Prefix for generated target table names")
    if_exists: Literal["append", "replace"] = Field(default="append")
    dry_run: bool = Field(default=True)


class SharePointPreviewRequest(BaseModel):
    workbook_path: str = Field(
        ...,
        description=(
            "Path in SharePoint document library (e.g. Shared Documents/Folder/File.xlsx) "
            "or full SharePoint URL (Doc.aspx/Share link)."
        ),
    )
    site_hostname: str | None = Field(
        default=None,
        description="Optional SharePoint hostname override, e.g. company.sharepoint.com",
    )
    site_path: str | None = Field(
        default=None,
        description="Optional SharePoint site path override, e.g. TeamSite",
    )
    sheet_names: list[str] | None = Field(default=None, description="Optional explicit list of sheet names")
    header_row: int = Field(default=1, ge=1, description="1-based row number that contains headers")
    skip_rows: int = Field(default=0, ge=0, description="Rows to skip after the header row")
    max_rows_per_sheet: int | None = Field(default=None, ge=1, description="Optional row cap per sheet")


class SharePointImportRequest(SharePointPreviewRequest):
    target_table_prefix: str = Field(default="excel_", description="Prefix for generated target table names")
    if_exists: Literal["append", "replace"] = Field(default="append")
    dry_run: bool = Field(default=True)


class ImportSheetResult(BaseModel):
    sheet_name: str
    target_table: str
    inserted_rows: int


class ExportSnapshotResponse(BaseModel):
    status: Literal["ok"]
    message: str
    requested_snapshot_utc: str | None
    effective_snapshot_utc: str
    target_table: str
    rows_exported: int
    workbook_path: str


class ExportSnapshotJobStartResponse(BaseModel):
    status: Literal["accepted"]
    message: str
    job_id: str
    created_at_utc: str


class ExportSnapshotJobStatusResponse(BaseModel):
    job_id: str
    status: Literal["running", "completed", "failed"]
    message: str
    target_table: str
    requested_snapshot_utc: str | None
    effective_snapshot_utc: str | None
    started_at_utc: str
    finished_at_utc: str | None
    total_rows: int | None
    exported_rows: int
    progress_pct: float | None
    workbook_path: str | None
    download_url: str | None
    error: str | None


class ImportSummary(BaseModel):
    workbook_path: str
    database_path: str
    processed_sheets: int
    processed_rows: int
    dry_run: bool
    sheets: list[ImportSheetResult]


class ImportJobResponse(BaseModel):
    job_id: str
    status: Literal["dry_run", "completed"]
    message: str
    created_at_utc: str
    summary: ImportSummary

    @classmethod
    def dry_run(cls, summary: ImportSummary) -> "ImportJobResponse":
        return cls(
            job_id=f"job-{uuid4()}",
            status="dry_run",
            message="Dry run completed. No data was written to the database.",
            created_at_utc=datetime.now(timezone.utc).isoformat(),
            summary=summary,
        )

    @classmethod
    def completed(cls, summary: ImportSummary) -> "ImportJobResponse":
        return cls(
            job_id=f"job-{uuid4()}",
            status="completed",
            message="Import completed successfully.",
            created_at_utc=datetime.now(timezone.utc).isoformat(),
            summary=summary,
        )
