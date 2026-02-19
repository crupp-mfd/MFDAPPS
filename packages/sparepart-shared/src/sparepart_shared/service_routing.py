"""Route ownership rules for split deployment targets."""

from __future__ import annotations

from typing import Literal

CanonicalService = Literal[
    "appmfd",
    "objektstruktur",
    "bremsenumbau",
    "goldenview",
    "mehrkilometer",
    "rsrd",
    "sql_api",
    "teilenummer",
    "wagensuche",
]
ServiceName = Literal[
    "appmfd",
    "objektstruktur",
    "bremsenumbau",
    "goldenview",
    "mehrkilometer",
    "rsrd",
    "sql_api",
    "teilenummer",
    "wagensuche",
    # legacy aliases
    "core",
    "rsrd2",
]

SHARED_STATIC_PREFIXES = ("/bilder/",)
SHARED_STATIC_PATHS = {
    "/styles.css",
    "/api-config.js",
    "/app.js",
    "/wagensuche.js",
    "/rsrd2_compare.js",
}


def _is_shared_static_path(path: str) -> bool:
    if path in SHARED_STATIC_PATHS:
        return True
    return any(path.startswith(prefix) for prefix in SHARED_STATIC_PREFIXES)


def classify_path(path: str) -> CanonicalService:
    if path in {"/", "/index.html", "/favicon.ico", "/portal-config.js"} or path.startswith("/portal/"):
        return "appmfd"
    if path == "/objektstrukturtausch.html":
        return "objektstruktur"
    if path == "/wagenumbau.html":
        return "bremsenumbau"
    if path == "/teilenummer.html":
        return "teilenummer"
    if path in {"/wagensuche.html", "/wagensuche.js"}:
        return "wagensuche"
    if path in {"/rsrd2.html", "/rsrd2_compare.html", "/rsrd2_compare.js"}:
        return "rsrd"
    if path == "/gpt-goldenview.html":
        return "goldenview"
    if path.startswith("/api/mehrkilometer"):
        return "mehrkilometer"
    if path.startswith("/api/rsrd2"):
        return "rsrd"
    if path.startswith("/api/goldenview") or path.startswith("/api/ask_m3_knowledge"):
        return "goldenview"
    if path in {"/query", "/query/", "/M3BRIDGE.html"}:
        return "sql_api"
    if path.startswith("/api/renumber"):
        return "bremsenumbau"
    if path.startswith("/api/teilenummer"):
        return "teilenummer"
    if path.startswith("/api/wagensuche"):
        return "wagensuche"
    if (
        path.startswith("/api/meta")
        or path.startswith("/api/wagons")
        or path.startswith("/api/spareparts")
        or path.startswith("/api/objstrk")
        or path == "/api/reload"
    ):
        return "objektstruktur"
    if path == "/api/health":
        return "appmfd"
    return "appmfd"


def should_serve_path(service: ServiceName, path: str) -> bool:
    if _is_shared_static_path(path):
        return True
    owner = classify_path(path)
    if service == "core":
        return owner in {"appmfd", "objektstruktur", "bremsenumbau", "teilenummer", "wagensuche", "mehrkilometer"}
    if service == "rsrd2":
        return owner == "rsrd"
    return owner == service
