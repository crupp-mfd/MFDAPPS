from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


def _load_env_file(path: Path) -> None:
    if not path.exists() or not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if not os.environ.get(key, "").strip():
            os.environ[key] = value


def _load_local_env(mfdapps_home: Path) -> None:
    for file_name in (".ENV", ".env"):
        _load_env_file(mfdapps_home / file_name)


@dataclass(frozen=True)
class ExcelImportSettings:
    mfdapps_home: Path
    runtime_root: Path
    input_root: Path
    sqlite_path: Path
    preview_limit: int
    sharepoint_graph_base_url: str
    sharepoint_request_timeout: int
    sharepoint_tenant_id: str | None
    sharepoint_client_id: str | None
    sharepoint_client_secret: str | None
    sharepoint_use_managed_identity: bool
    sharepoint_managed_identity_client_id: str | None
    sharepoint_site_hostname: str | None
    sharepoint_site_path: str | None
    sharepoint_default_workbook_path: str | None
    sharepoint_default_table_prefix: str
    fabric_sql_server: str | None
    fabric_sql_database: str | None
    fabric_sql_driver: str
    fabric_sql_port: int
    fabric_sql_timeout: int
    fabric_client_id: str | None
    fabric_tenant_id: str | None
    fabric_client_secret: str | None
    go_workbook_path: str
    go_sheet_name: str
    go_header_row: int
    go_skip_rows: int
    go_target_schema: str
    go_target_table: str
    go_raw_table: str
    go_export_dir: Path

    @classmethod
    def from_env(cls) -> "ExcelImportSettings":
        mfdapps_home = Path(os.getenv("MFDAPPS_HOME", ".")).expanduser().resolve()
        _load_local_env(mfdapps_home)
        runtime_root = Path(os.getenv("MFDAPPS_RUNTIME_ROOT", mfdapps_home / "apps/christian/data")).expanduser().resolve()
        input_root = Path(os.getenv("EXCEL_IMPORT_INPUT_ROOT", mfdapps_home)).expanduser().resolve()

        sqlite_raw = os.getenv("EXCEL_IMPORT_SQLITE_PATH")
        sqlite_path = Path(sqlite_raw).expanduser() if sqlite_raw else runtime_root / "excel_import.db"
        if not sqlite_path.is_absolute():
            sqlite_path = (runtime_root / sqlite_path).resolve()
        else:
            sqlite_path = sqlite_path.resolve()

        preview_limit = int(os.getenv("EXCEL_IMPORT_PREVIEW_LIMIT", "20"))
        sharepoint_use_managed_identity = (
            os.getenv("SHAREPOINT_USE_MANAGED_IDENTITY", "1").strip().lower() in {"1", "true", "yes", "y"}
        )
        go_target_schema = os.getenv("EXCEL_IMPORT_GO_TARGET_SCHEMA", "landing")
        go_target_table = os.getenv("EXCEL_IMPORT_GO_TARGET_TABLE", "ContractManagement")
        go_raw_table = os.getenv("EXCEL_IMPORT_GO_RAW_TABLE", f"{go_target_table}_raw")
        go_export_dir = Path(
            os.getenv("EXCEL_IMPORT_GO_EXPORT_DIR", runtime_root / "excel_exports")
        ).expanduser()
        if not go_export_dir.is_absolute():
            go_export_dir = (runtime_root / go_export_dir).resolve()
        else:
            go_export_dir = go_export_dir.resolve()

        return cls(
            mfdapps_home=mfdapps_home,
            runtime_root=runtime_root,
            input_root=input_root,
            sqlite_path=sqlite_path,
            preview_limit=max(1, preview_limit),
            sharepoint_graph_base_url=os.getenv("SHAREPOINT_GRAPH_BASE_URL", "https://graph.microsoft.com/v1.0").rstrip("/"),
            sharepoint_request_timeout=max(5, int(os.getenv("SHAREPOINT_REQUEST_TIMEOUT", "60"))),
            sharepoint_tenant_id=(
                os.getenv("SHAREPOINT_TENANT_ID")
                or os.getenv("FABRIC_TENANT_ID")
                or os.getenv("AZURE_TENANT_ID")
            ),
            sharepoint_client_id=(
                os.getenv("SHAREPOINT_CLIENT_ID")
                or os.getenv("FABRIC_CLIENT_ID")
                or os.getenv("AZURE_CLIENT_ID")
            ),
            sharepoint_client_secret=(
                os.getenv("SHAREPOINT_CLIENT_SECRET")
                or os.getenv("FABRIC_CLIENT_SECRET")
                or os.getenv("AZURE_CLIENT_SECRET")
            ),
            sharepoint_use_managed_identity=sharepoint_use_managed_identity,
            sharepoint_managed_identity_client_id=(
                os.getenv("SHAREPOINT_MANAGED_IDENTITY_CLIENT_ID")
                or os.getenv("AZURE_CLIENT_ID")
                or os.getenv("MANAGED_IDENTITY_CLIENT_ID")
            ),
            sharepoint_site_hostname=os.getenv("SHAREPOINT_SITE_HOSTNAME"),
            sharepoint_site_path=os.getenv("SHAREPOINT_SITE_PATH"),
            sharepoint_default_workbook_path=os.getenv("SHAREPOINT_DEFAULT_WORKBOOK_PATH"),
            sharepoint_default_table_prefix=os.getenv("SHAREPOINT_DEFAULT_TABLE_PREFIX", "excel_"),
            fabric_sql_server=os.getenv("FABRIC_SQL_SERVER"),
            fabric_sql_database=os.getenv("FABRIC_SQL_DATABASE"),
            fabric_sql_driver=os.getenv("FABRIC_SQL_DRIVER", "ODBC Driver 18 for SQL Server"),
            fabric_sql_port=max(1, int(os.getenv("FABRIC_SQL_PORT", "1433"))),
            fabric_sql_timeout=max(5, int(os.getenv("FABRIC_SQL_TIMEOUT", "20"))),
            fabric_client_id=os.getenv("FABRIC_CLIENT_ID"),
            fabric_tenant_id=os.getenv("FABRIC_TENANT_ID"),
            fabric_client_secret=os.getenv("FABRIC_CLIENT_SECRET"),
            go_workbook_path=os.getenv(
                "EXCEL_IMPORT_GO_WORKBOOK_PATH",
                "apps/christian/AppExcelImport/manual_input/Contract Management.xlsx",
            ),
            go_sheet_name=os.getenv("EXCEL_IMPORT_GO_SHEET_NAME", "CM"),
            go_header_row=max(1, int(os.getenv("EXCEL_IMPORT_GO_HEADER_ROW", "3"))),
            go_skip_rows=max(0, int(os.getenv("EXCEL_IMPORT_GO_SKIP_ROWS", "0"))),
            go_target_schema=go_target_schema,
            go_target_table=go_target_table,
            go_raw_table=go_raw_table,
            go_export_dir=go_export_dir,
        )


def load_settings() -> ExcelImportSettings:
    return ExcelImportSettings.from_env()
