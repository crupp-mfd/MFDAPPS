from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from python.web_server import app as legacy_app

app = FastAPI(title="MFDApps AppRSRD")


@app.get("/healthz", include_in_schema=False)
def healthz() -> dict:
    return {"status": "ok", "service": "AppRSRD"}


@app.get("/apps/christian/AppRSRD", include_in_schema=False)
@app.get("/apps/christian/AppRSRD/", include_in_schema=False)
@app.get("/apps/christian/AppRSRD/frontend", include_in_schema=False)
@app.get("/apps/christian/AppRSRD/frontend/", include_in_schema=False)
@app.get("/apps/christian/AppRSRD/frontend/rsrd2.html", include_in_schema=False)
def legacy_rsrd_entry_redirect() -> RedirectResponse:
    return RedirectResponse(url="/rsrd2.html", status_code=302)


app.mount("/", legacy_app)
