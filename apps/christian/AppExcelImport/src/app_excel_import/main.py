from __future__ import annotations

from fastapi import APIRouter, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path

from app_excel_import.config import load_settings
from app_excel_import.models import (
    ExportSnapshotJobStartResponse,
    ExportSnapshotJobStatusResponse,
    ExportSnapshotResponse,
    GoFabricResponse,
    ImportJobResponse,
    ImportRequest,
    PreviewRequest,
    SharePointImportRequest,
    SharePointPreviewRequest,
    TransferTestResponse,
    WorkbookPreviewResponse,
)
from app_excel_import.services.import_service import ExcelImportService

app = FastAPI(title="MFDApps AppExcelImport")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

settings = load_settings()
service = ExcelImportService(settings)
app_root = Path(__file__).resolve().parents[2]

router = APIRouter(prefix="/api/excel-import", tags=["excel-import"])


@app.get("/healthz", include_in_schema=False)
def healthz() -> dict:
    return {"status": "ok", "service": "AppExcelImport"}


@router.get("/config/health")
def config_health() -> dict:
    return {"status": "ok", **service.configuration_health()}


@router.post("/transfer/test", response_model=TransferTestResponse)
def transfer_test() -> TransferTestResponse:
    try:
        return service.run_transfer_test()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/go/fabric", response_model=GoFabricResponse)
def go_fabric() -> GoFabricResponse:
    try:
        return service.run_go_fabric_import()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/export/cm", response_model=ExportSnapshotResponse)
def export_cm(snapshot_at_utc: str | None = Query(default=None)) -> ExportSnapshotResponse:
    try:
        return service.export_contract_management_snapshot(snapshot_iso=snapshot_at_utc)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/export/cm/jobs", response_model=ExportSnapshotJobStartResponse)
def start_export_cm_job(snapshot_at_utc: str | None = Query(default=None)) -> ExportSnapshotJobStartResponse:
    try:
        return service.start_export_snapshot_job(snapshot_iso=snapshot_at_utc)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/export/cm/jobs/{job_id}", response_model=ExportSnapshotJobStatusResponse)
def export_cm_job_status(job_id: str) -> ExportSnapshotJobStatusResponse:
    try:
        return service.get_export_snapshot_job(job_id=job_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/export/cm/jobs/{job_id}/download")
def export_cm_job_download(job_id: str) -> FileResponse:
    try:
        file_path = service.get_export_snapshot_file(job_id=job_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    return FileResponse(
        path=file_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=file_path.name,
    )


@router.post("/workbook/preview", response_model=WorkbookPreviewResponse)
def workbook_preview(payload: PreviewRequest) -> WorkbookPreviewResponse:
    try:
        return service.preview_workbook(payload)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/jobs", response_model=ImportJobResponse)
def create_import_job(payload: ImportRequest) -> ImportJobResponse:
    try:
        return service.submit_import(payload)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/sharepoint/workbook/preview", response_model=WorkbookPreviewResponse)
def sharepoint_workbook_preview(payload: SharePointPreviewRequest) -> WorkbookPreviewResponse:
    try:
        return service.preview_sharepoint_workbook(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/sharepoint/jobs", response_model=ImportJobResponse)
def create_sharepoint_import_job(payload: SharePointImportRequest) -> ImportJobResponse:
    try:
        return service.submit_sharepoint_import(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/sharepoint/jobs/default", response_model=ImportJobResponse)
def create_default_sharepoint_import_job(dry_run: bool = Query(default=False)) -> ImportJobResponse:
    try:
        return service.submit_default_sharepoint_import(dry_run=dry_run)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/", include_in_schema=False)
def root_redirect() -> RedirectResponse:
    return RedirectResponse(url="/apps/christian/AppExcelImport/", status_code=302)


@app.get("/apps/christian/AppExcelImport", include_in_schema=False)
def excelimport_redirect() -> RedirectResponse:
    return RedirectResponse(url="/apps/christian/AppExcelImport/", status_code=302)


app.include_router(router)
app.mount(
    "/apps/christian/AppExcelImport",
    StaticFiles(directory=app_root, html=True),
    name="app_excel_import_frontend",
)
