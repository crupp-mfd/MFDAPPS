"""ASGI utilities for path-filtered service split."""

from __future__ import annotations

from starlette.responses import JSONResponse

from .service_routing import ServiceName, should_serve_path


class PathFilteredASGI:
    """Forwards only owned paths to a wrapped ASGI app."""

    def __init__(self, app, service: ServiceName) -> None:
        self.app = app
        self.service = service

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        path = str(scope.get("path") or "")
        if should_serve_path(self.service, path):
            await self.app(scope, receive, send)
            return

        response = JSONResponse(
            {
                "detail": "Route not served by this app.",
                "service": self.service,
                "path": path,
            },
            status_code=404,
        )
        await response(scope, receive, send)
