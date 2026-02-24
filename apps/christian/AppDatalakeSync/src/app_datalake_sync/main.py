from __future__ import annotations

from fastapi import APIRouter, FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path

from app_datalake_sync.config import load_settings
from app_datalake_sync.models import SyncJobResponse, SyncRequest
from app_datalake_sync.services.fabric_client import FabricSqlClient
from app_datalake_sync.services.infor_client import InforDatalakeClient
from app_datalake_sync.services.sync_service import TableSyncService
from app_datalake_sync.services.table_catalog import DatalakeTableCatalogService

app = FastAPI(title="MFDApps AppDatalakeSync")

settings = load_settings()
app_root = Path(__file__).resolve().parents[2]
sync_service = TableSyncService(
    infor_client=InforDatalakeClient(settings),
    fabric_client=FabricSqlClient(settings),
)
table_catalog_service = DatalakeTableCatalogService(sync_service.infor_client)

router = APIRouter(prefix="/api/datalake-sync", tags=["datalake-sync"])


@app.get("/healthz", include_in_schema=False)
def healthz() -> dict:
    return {"status": "ok", "service": "AppDatalakeSync"}


@router.get("/config/health")
def config_health() -> dict:
    health = sync_service.configuration_health()
    return {
        "status": "ok",
        **health,
    }


@router.get("/sync/plans")
def get_sync_plan() -> dict:
    return sync_service.supported_plan()


@router.post("/sync/jobs", response_model=SyncJobResponse)
def create_sync_job(payload: SyncRequest) -> SyncJobResponse:
    return sync_service.submit_sync_job(payload)


@router.post("/datalake/tables/refresh")
def refresh_datalake_tables(env: str = "live", force: bool = False) -> dict:
    try:
        return table_catalog_service.trigger_refresh(env=env, force=force)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/datalake/tables")
def list_datalake_tables(env: str = "live", autostart: bool = True) -> dict:
    try:
        if autostart:
            return table_catalog_service.ensure_started(env=env)
        return table_catalog_service.get_snapshot(env=env)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/", include_in_schema=False)
def root_redirect() -> RedirectResponse:
    return RedirectResponse(url="/apps/christian/AppDatalakeSync/", status_code=302)


app.include_router(router)
app.mount(
    "/apps/christian/AppDatalakeSync",
    StaticFiles(directory=app_root, html=True),
    name="app_datalake_sync_frontend",
)
