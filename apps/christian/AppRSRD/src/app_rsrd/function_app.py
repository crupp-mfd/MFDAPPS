"""Azure Functions ASGI adapter for AppRSRD."""

from __future__ import annotations

from .main import app as asgi_app

try:
    import azure.functions as func
    from azure.functions import AsgiMiddleware
except Exception:  # pragma: no cover
    func = None

if func is not None:  # pragma: no cover
    function_app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

    @function_app.function_name(name="app_rsrd_http")
    @function_app.route(route="{*route}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
    def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
        return AsgiMiddleware(asgi_app).handle(req, context)
