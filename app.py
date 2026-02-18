#!/usr/bin/env python3
import argparse
from datetime import datetime
import errno
import importlib.util
import json
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
import re
import sys
from urllib.parse import parse_qs, quote, urlparse

REPO_ROOT = Path(__file__).resolve().parent
MEHR_ROOT = REPO_ROOT / "apps" / "christian" / "AppMehrkilometer"
MEHR_SOURCE_DIR = MEHR_ROOT / "legacy_source" / "Quellen"
MEHR_OUTPUT_DIR = MEHR_ROOT / "legacy_source" / "Output"
MEHR_LEGACY_SCRIPT = MEHR_ROOT / "legacy_source" / "Script" / "jahresabrechnung.py"

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000
DEFAULT_PORT_TRIES = 20
DEFAULT_YEAR = 2025
_LEGACY_XLSX_MODULE = None
SPECIAL_PRESETS = [
    {
        "customer": "Grampet",
        "customer_contract": "2025-0006",
        "internal_contracts": ["V200051"],
        "free_km_per_wagon": 80000,
        "rate_per_wagon_eur": 0.06,
    },
    {
        "customer": "Railcare",
        "customer_contract": "2024-0019",
        "internal_contracts": ["V200039"],
        "free_km_per_wagon": 90000,
        "rate_per_wagon_eur": 0.05,
    },
    {
        "customer": "Raildox",
        "customer_contract": "2025.0011",
        "internal_contracts": ["V200053", "V720006"],
        "free_km_per_wagon": 90000,
        "rate_per_wagon_eur": 0.06,
    },
]


def _load_legacy_xlsx_module():
    global _LEGACY_XLSX_MODULE
    if _LEGACY_XLSX_MODULE is not None:
        return _LEGACY_XLSX_MODULE

    if not MEHR_LEGACY_SCRIPT.exists():
        raise FileNotFoundError(f"Legacy-Script fehlt: {MEHR_LEGACY_SCRIPT}")

    spec = importlib.util.spec_from_file_location(
        "appmehrkilometer_legacy_jahresabrechnung", MEHR_LEGACY_SCRIPT
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Legacy-Script konnte nicht geladen werden.")

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    _LEGACY_XLSX_MODULE = module
    return module


def _pick_latest_file(directory: Path, predicate) -> Path | None:
    if not directory.exists():
        return None
    candidates = [p for p in directory.iterdir() if p.is_file() and predicate(p.name.lower())]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _build_special_template_rows(module) -> list:
    return [
        module.TemplateRow(
            row_no=0,
            customer=preset["customer"],
            customer_contract=preset["customer_contract"],
            internal_contracts=list(preset["internal_contracts"]),
            free_km=float(preset["free_km_per_wagon"]),
            tariff=module.Tariff(rate=float(preset["rate_per_wagon_eur"]), per_km=1),
            tariff_raw=f"{float(preset['rate_per_wagon_eur']):.2f} je 1 KM",
            is_fleet=False,
            is_wagon=True,
        )
        for preset in SPECIAL_PRESETS
    ]


def _create_special_settlement_files(year: int) -> tuple[Path, Path]:
    module = _load_legacy_xlsx_module()
    MEHR_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    source_km = _pick_latest_file(
        MEHR_SOURCE_DIR, lambda n: n.endswith(".xlsx") and "kilometer" in n
    )
    if source_km is None:
        raise FileNotFoundError(
            f"Kilometerdatei fehlt im Quellen-Ordner: {MEHR_SOURCE_DIR}"
        )

    template_rows = _build_special_template_rows(module)
    kilometer_data = module.read_kilometer(source_km, year)
    overview, details = module.build_overview(template_rows, kilometer_data, year)
    abrechnungs_sheets = module.build_detail_workbook_sheets(overview, details, year)
    details_export = [row[:12] for row in details]
    if not abrechnungs_sheets:
        abrechnungs_sheets = [
            module.SheetSpec(
                name="001_Keine_Daten",
                data=[["Keine Einzelabrechnungen vorhanden."]],
                kind="detail",
                auto_filter=None,
                tab_color="FFD9EAD3",
                highlight_rows=set(),
            )
        ]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    overview_file = MEHR_OUTPUT_DIR / f"special_vertragsuebersicht_{year}_{timestamp}.xlsx"
    detail_file = (
        MEHR_OUTPUT_DIR / f"special_einzelabrechnungen_detail_{year}_{timestamp}.xlsx"
    )
    module.write_xlsx(
        overview_file,
        [
            module.SheetSpec(
                name="Vertragsuebersicht",
                data=overview,
                kind="overview",
                auto_filter="A1:J1",
            ),
            module.SheetSpec(
                name="Wagendetails",
                data=details_export,
                kind="wagendetails",
                auto_filter="A1:L1",
            ),
        ],
    )
    module.write_xlsx(detail_file, abrechnungs_sheets)
    return overview_file, detail_file


def _extract_year_from_filename(file_path: Path | None) -> int | None:
    if file_path is None:
        return None
    match = re.search(r"_(\d{4})_\d{8}_\d{6}\.xlsx$", file_path.name)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


class AppHandler(SimpleHTTPRequestHandler):
    """Serve static files and local helper APIs."""

    def _send_json(self, payload: dict, status: int = 200) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_js(self, text: str, status: int = 200) -> None:
        data = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/javascript; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _safe_file(self, base_dir: Path, filename: str) -> Path | None:
        if "/" in filename or "\\" in filename or ".." in filename:
            return None
        candidate = (base_dir / filename).resolve()
        try:
            candidate.relative_to(base_dir.resolve())
        except ValueError:
            return None
        if not candidate.is_file():
            return None
        return candidate

    def _file_info(self, file_path: Path | None, kind: str) -> dict:
        if file_path is None:
            return {"exists": False}
        return {
            "exists": True,
            "name": file_path.name,
            "download_url": f"/api/mehrkilometer/download?kind={quote(kind)}&name={quote(file_path.name)}",
        }

    def _pick_latest(self, directory: Path, predicate) -> Path | None:
        return _pick_latest_file(directory, predicate)

    def _build_mehr_payload(self, year: int) -> dict:
        source_overview = self._pick_latest(
            MEHR_SOURCE_DIR, lambda n: n.endswith(".xlsx") and "vorlage" in n
        )
        source_km = self._pick_latest(
            MEHR_SOURCE_DIR, lambda n: n.endswith(".xlsx") and "kilometer" in n
        )
        output_overview = self._pick_latest(
            MEHR_OUTPUT_DIR,
            lambda n: n.endswith(".xlsx") and n.startswith(f"vertragsuebersicht_{year}_"),
        )
        output_details = self._pick_latest(
            MEHR_OUTPUT_DIR,
            lambda n: n.endswith(".xlsx")
            and n.startswith(f"einzelabrechnungen_detail_{year}_"),
        )
        special_output_overview = self._pick_latest(
            MEHR_OUTPUT_DIR,
            lambda n: n.endswith(".xlsx")
            and n.startswith(f"special_vertragsuebersicht_{year}_"),
        )
        special_output_details = self._pick_latest(
            MEHR_OUTPUT_DIR,
            lambda n: n.endswith(".xlsx")
            and n.startswith(f"special_einzelabrechnungen_detail_{year}_"),
        )
        special_from_other_year = False
        if special_output_overview is None:
            special_output_overview = self._pick_latest(
                MEHR_OUTPUT_DIR,
                lambda n: n.endswith(".xlsx")
                and n.startswith("special_vertragsuebersicht_"),
            )
            special_from_other_year = special_output_overview is not None
        if special_output_details is None:
            special_output_details = self._pick_latest(
                MEHR_OUTPUT_DIR,
                lambda n: n.endswith(".xlsx")
                and n.startswith("special_einzelabrechnungen_detail_"),
            )
            special_from_other_year = special_from_other_year or (
                special_output_details is not None
            )

        payload = {
            "recommended_year": year,
            "sources": {
                "overview": self._file_info(source_overview, "source_overview"),
                "kilometer": self._file_info(source_km, "source_kilometer"),
            },
            "outputs": {
                "overview": self._file_info(output_overview, "output_overview"),
                "details": self._file_info(output_details, "output_details"),
            },
            "special_outputs": {
                "overview": self._file_info(
                    special_output_overview, "special_output_overview"
                ),
                "details": self._file_info(
                    special_output_details, "special_output_details"
                ),
            },
        }

        if source_overview is None or source_km is None:
            payload["warning"] = "Eine oder mehrere Quelldateien fehlen."
        elif output_overview is None or output_details is None:
            payload["warning"] = (
                "Für dieses Jahr liegen noch keine lokalen Output-Dateien vor."
            )
        elif special_from_other_year:
            fallback_year = _extract_year_from_filename(special_output_overview)
            if fallback_year is None:
                fallback_year = _extract_year_from_filename(special_output_details)
            if fallback_year is not None and fallback_year != year:
                payload["warning"] = (
                    f"Für {year} wurden keine Spezialdateien gefunden. "
                    f"Es werden die letzten verfügbaren Spezialdateien aus {fallback_year} angezeigt."
                )
        return payload

    def _serve_mehr_download(self, query: dict[str, list[str]]) -> None:
        kind = (query.get("kind") or [""])[0]
        name = (query.get("name") or [""])[0]

        dir_map = {
            "source_overview": MEHR_SOURCE_DIR,
            "source_kilometer": MEHR_SOURCE_DIR,
            "output_overview": MEHR_OUTPUT_DIR,
            "output_details": MEHR_OUTPUT_DIR,
            "special_output_overview": MEHR_OUTPUT_DIR,
            "special_output_details": MEHR_OUTPUT_DIR,
        }
        base_dir = dir_map.get(kind)
        if base_dir is None:
            self._send_json({"detail": "Ungültiger Download-Typ."}, status=400)
            return

        file_path = self._safe_file(base_dir, name)
        if file_path is None:
            self._send_json({"detail": "Datei nicht gefunden."}, status=404)
            return

        data = file_path.read_bytes()
        self.send_response(200)
        self.send_header(
            "Content-Type",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        self.send_header(
            "Content-Disposition", f'attachment; filename="{file_path.name}"'
        )
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        path = parsed.path

        if path == "/apps/christian/AppMehrkilometer/":
            self.send_response(302)
            self.send_header("Location", "/apps/christian/AppMehrkilometer/frontend/")
            self.end_headers()
            return

        if path == "/api-config.js":
            self._send_js(
                "window.__SPAREPART_API_CONFIG__ = { CORE_API_BASE_URL: '' };"
            )
            return

        if path == "/api/mehrkilometer/sources":
            year_raw = (query.get("year") or [""])[0]
            try:
                year = int(year_raw) if year_raw else DEFAULT_YEAR
            except ValueError:
                year = DEFAULT_YEAR
            year = max(2000, min(3000, year))
            self._send_json(self._build_mehr_payload(year))
            return

        if path == "/api/mehrkilometer/download":
            self._serve_mehr_download(query)
            return

        super().do_GET()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        path = parsed.path

        if path == "/api/mehrkilometer/create-special":
            year_raw = (query.get("year") or [""])[0]
            try:
                year = int(year_raw) if year_raw else DEFAULT_YEAR
            except ValueError:
                year = DEFAULT_YEAR
            year = max(2000, min(3000, year))

            try:
                created_overview, created_details = _create_special_settlement_files(year)
            except Exception as exc:
                self._send_json(
                    {"detail": f"Spezialabrechnung fehlgeschlagen: {exc}"},
                    status=500,
                )
                return

            payload = self._build_mehr_payload(year)
            payload["special_outputs"] = {
                "overview": self._file_info(
                    created_overview, "special_output_overview"
                ),
                "details": self._file_info(
                    created_details, "special_output_details"
                ),
            }
            payload["warning"] = (
                "Spezialabrechnung für Grampet, Railcare und Raildox wurde erzeugt."
            )
            self._send_json(payload)
            return

        if path == "/api/mehrkilometer/create":
            year_raw = (query.get("year") or [""])[0]
            try:
                year = int(year_raw) if year_raw else DEFAULT_YEAR
            except ValueError:
                year = DEFAULT_YEAR
            year = max(2000, min(3000, year))
            payload = self._build_mehr_payload(year)
            payload["warning"] = (
                "Lokaler Modus: Es wird keine neue Abrechnung berechnet, "
                "es werden vorhandene Dateien angezeigt."
            )
            self._send_json(payload)
            return

        self._send_json({"detail": "Not found"}, status=404)


class ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Papperlapapp web app.")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Host/IP to bind to.")
    parser.add_argument(
        "--port", type=int, default=DEFAULT_PORT, help="Preferred start port."
    )
    parser.add_argument(
        "--port-tries",
        type=int,
        default=DEFAULT_PORT_TRIES,
        help="How many consecutive ports to try if the preferred port is busy.",
    )
    return parser.parse_args()


def create_server(
    host: str, preferred_port: int, port_tries: int
) -> tuple[ReusableThreadingHTTPServer, int]:
    for offset in range(max(1, port_tries)):
        candidate_port = preferred_port + offset
        try:
            server = ReusableThreadingHTTPServer((host, candidate_port), AppHandler)
            return server, candidate_port
        except OSError as exc:
            if exc.errno == errno.EADDRINUSE:
                continue
            raise
    raise OSError(
        errno.EADDRINUSE,
        f"No free port found in range {preferred_port}-{preferred_port + port_tries - 1}.",
    )


def main() -> None:
    args = parse_args()
    try:
        server, bound_port = create_server(args.host, args.port, args.port_tries)
    except OSError as exc:
        print(f"Start fehlgeschlagen: {exc}", file=sys.stderr)
        print(
            "Tipp: Starte mit einem anderen Port, z.B. `python3 app.py --port 8080`.",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc

    if bound_port != args.port:
        print(f"Port {args.port} ist belegt, nutze stattdessen Port {bound_port}.", flush=True)
    print(f"WebApp running at http://{args.host}:{bound_port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
