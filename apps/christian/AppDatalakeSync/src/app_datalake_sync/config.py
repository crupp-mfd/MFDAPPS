from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class DatalakeSyncSettings:
    infor_datalake_base_url: str | None
    infor_datalake_tenant: str | None
    infor_datalake_client_id: str | None
    infor_datalake_client_secret: str | None
    fabric_sql_server: str | None
    fabric_sql_database: str | None
    fabric_sql_driver: str
    fabric_sql_port: int
    fabric_sql_timeout: int
    fabric_client_id: str | None
    fabric_tenant_id: str | None
    fabric_client_secret: str | None
    credentials_dir: str

    @classmethod
    def from_env(cls) -> "DatalakeSyncSettings":
        return cls(
            infor_datalake_base_url=os.getenv("INFOR_DATALAKE_BASE_URL"),
            infor_datalake_tenant=os.getenv("INFOR_DATALAKE_TENANT"),
            infor_datalake_client_id=os.getenv("INFOR_DATALAKE_CLIENT_ID"),
            infor_datalake_client_secret=os.getenv("INFOR_DATALAKE_CLIENT_SECRET"),
            fabric_sql_server=os.getenv("FABRIC_SQL_SERVER"),
            fabric_sql_database=os.getenv("FABRIC_SQL_DATABASE"),
            fabric_sql_driver=os.getenv("FABRIC_SQL_DRIVER", "ODBC Driver 18 for SQL Server"),
            fabric_sql_port=int(os.getenv("FABRIC_SQL_PORT", "1433")),
            fabric_sql_timeout=int(os.getenv("FABRIC_SQL_TIMEOUT", "20")),
            fabric_client_id=os.getenv("FABRIC_CLIENT_ID"),
            fabric_tenant_id=os.getenv("FABRIC_TENANT_ID"),
            fabric_client_secret=os.getenv("FABRIC_CLIENT_SECRET"),
            credentials_dir=os.getenv(
                "MFDAPPS_CREDENTIALS_DIR",
                str(Path(__file__).resolve().parents[5] / "credentials"),
            ),
        )

    def infor_ready(self) -> bool:
        return all(
            [
                self.infor_datalake_base_url,
                self.infor_datalake_tenant,
                self.infor_datalake_client_id,
                self.infor_datalake_client_secret,
            ]
        )

    def fabric_ready(self) -> bool:
        return all(
            [
                self.fabric_sql_server,
                self.fabric_sql_database,
                self.fabric_client_id,
                self.fabric_tenant_id,
                self.fabric_client_secret,
            ]
        )


def load_settings() -> DatalakeSyncSettings:
    return DatalakeSyncSettings.from_env()
