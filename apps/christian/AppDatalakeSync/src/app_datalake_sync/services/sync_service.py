from __future__ import annotations

from app_datalake_sync.models import SyncJobResponse, SyncRequest
from app_datalake_sync.services.fabric_client import FabricSqlClient
from app_datalake_sync.services.infor_client import InforDatalakeClient


class TableSyncService:
    """Coordinates sync plan and job submission (stub implementation)."""

    def __init__(self, infor_client: InforDatalakeClient, fabric_client: FabricSqlClient) -> None:
        self.infor_client = infor_client
        self.fabric_client = fabric_client

    def configuration_health(self) -> dict:
        return {
            "infor_configured": self.infor_client.is_configured(),
            "fabric_configured": self.fabric_client.is_configured(),
        }

    def supported_plan(self) -> dict:
        return {
            "modes": ["full", "incremental"],
            "job_statuses": ["accepted", "running", "completed", "failed"],
            "note": "Execution pipeline is not implemented yet.",
        }

    def submit_sync_job(self, payload: SyncRequest) -> SyncJobResponse:
        target_table = payload.target_table or payload.source_table
        message = (
            f"Stub job created for {payload.source_table} -> "
            f"{payload.target_schema}.{target_table} (mode={payload.mode}, dry_run={payload.dry_run})."
        )
        return SyncJobResponse.accepted(message=message)
