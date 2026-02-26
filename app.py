#!/usr/bin/env python3
import argparse
from decimal import Decimal
from datetime import date, datetime, timedelta, timezone
import errno
import importlib.util
import json
import os
import signal
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
import re
import socket
import sqlite3
import subprocess
import sys
import time
import shutil
import threading
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError

REPO_ROOT = Path(__file__).resolve().parent
LEGACY_LOCAL_RUNTIME_DIR = REPO_ROOT / "apps" / "christian" / "data"
LEGACY_LOCAL_CACHE_DB = LEGACY_LOCAL_RUNTIME_DIR / "cache.db"


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
        if not key:
            continue
        if value == "":
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        existing = os.environ.get(key)
        if existing is None or existing.strip() == "":
            os.environ[key] = value


def _load_local_env() -> None:
    for file_name in (".ENV", ".env"):
        _load_env_file(REPO_ROOT / file_name)


_load_local_env()


def _ensure_local_legacy_cache_db() -> None:
    try:
        LEGACY_LOCAL_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
        LEGACY_LOCAL_CACHE_DB.touch(exist_ok=True)
    except Exception:
        pass


_ensure_local_legacy_cache_db()


def _resolve_mehr_root() -> Path:
    candidates = (
        REPO_ROOT / "apps" / "christian" / "AppMehrkilometer",
        REPO_ROOT / "apps" / "Christian" / "AppMehrkilometer",
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


MEHR_ROOT = _resolve_mehr_root()
MEHR_SOURCE_DIR = MEHR_ROOT / "legacy_source" / "Quellen"
MEHR_OUTPUT_DIR = MEHR_ROOT / "legacy_source" / "Output"
MEHR_LEGACY_SCRIPT = MEHR_ROOT / "legacy_source" / "Script" / "jahresabrechnung.py"
MEHR_FABRIC_SQLITE = MEHR_ROOT / "runtime" / "mehrkilometer_fabric.db"
MEHR_SOURCE_DIRS_ENV = "MEHR_QUELLEN_DIRS"
MEHR_SOURCE_EXTENSIONS = {".xlsx", ".xlsm", ".xls"}
MEHR_OUTPUT_PREFIXES = (
    "vertragsuebersicht_",
    "einzelabrechnungen_detail_",
    "special_vertragsuebersicht_",
    "special_einzelabrechnungen_detail_",
)

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000
DEFAULT_PORT_TRIES = 20
DEFAULT_YEAR = 2025
_LEGACY_XLSX_MODULE = None
BACKEND_HOST = os.getenv("MFDAPPS_BACKEND_HOST", os.getenv("MFDAPPS_RSRD_HOST", "127.0.0.1"))
RSRD_BACKEND_PORT = int(os.getenv("MFDAPPS_RSRD_PORT", os.getenv("MFDAPPS_BACKEND_PORT_RSRD", "8001")))
TEILENUMMER_BACKEND_PORT = int(
    os.getenv("MFDAPPS_TEILENUMMER_PORT", os.getenv("MFDAPPS_BACKEND_PORT_TEILENUMMER", "8002"))
)
DATALAKE_BACKEND_PORT = int(
    os.getenv("MFDAPPS_DATALAKE_PORT", os.getenv("MFDAPPS_BACKEND_PORT_DATALAKE", "8003"))
)

RSRD_API_PREFIXES = ("/api/rsrd2", "/api/meta")
TEILENUMMER_API_PREFIXES = (
    "/api/teilenummer",
    "/api/spareparts",
    "/api/wagons",
    "/api/wagensuche",
    "/api/objstrk",
    "/api/renumber",
)
DATALAKE_API_PREFIXES = ("/api/datalake-sync", "/api/datalake", "/api/datalake/")

BACKEND_SPECS: dict[str, dict[str, str | int]] = {
    "rsrd": {
        "key": "rsrd",
        "name": "AppRSRD",
        "host": BACKEND_HOST,
        "port": RSRD_BACKEND_PORT,
        "sqlite_file": "rsrd.db",
        "log_path": "/tmp/apprsrd-backend.log",
    },
    "teilenummer": {
        "key": "teilenummer",
        "name": "AppTeilenummer",
        "host": BACKEND_HOST,
        "port": TEILENUMMER_BACKEND_PORT,
        "sqlite_file": "teilenummer.db",
        "log_path": "/tmp/appteilenummer-backend.log",
    },
    "datalake": {
        "key": "datalake",
        "name": "AppDataLakeSync",
        "host": BACKEND_HOST,
        "port": DATALAKE_BACKEND_PORT,
        "sqlite_file": "datalake.db",
        "log_path": "/tmp/appdatalake-backend.log",
    },
}
_job_backend_lock = threading.Lock()
_job_backend_map: dict[str, str] = {}
FABRIC_REQUIRED_ENV_VARS = (
    "FABRIC_SQL_SERVER",
    "FABRIC_SQL_DATABASE",
    "FABRIC_CLIENT_ID",
    "FABRIC_CLIENT_SECRET",
    "FABRIC_TENANT_ID",
)
MEHR_FABRIC_COLUMNS = (
    "kundennummer",
    "kundenname",
    "vertragsnummer",
    "vertragsposition",
    "seriennummer",
    "vertragsstart",
    "vertragsende",
    "abrechnungstart",
    "abrechnungsende",
    "km_start_datum_echt",
    "km_ende_datum_echt",
    "km_abrechnungstart",
    "km_abrechnungsende",
    "km_differenz",
    "km_daten_ok",
)
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


def _is_tcp_reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.3):
            return True
    except OSError:
        return False


def _backend_url(spec: dict[str, str | int], path: str, query: str) -> str:
    suffix = f"?{query}" if query else ""
    return f"http://{spec['host']}:{spec['port']}{path}{suffix}"


def _resolve_runtime_root() -> Path:
    runtime_root_env = os.environ.get("MFDAPPS_RUNTIME_ROOT", "").strip()
    if runtime_root_env:
        runtime_root = Path(runtime_root_env).expanduser()
    else:
        runtime_root = Path("/runtime")
        if not runtime_root.exists():
            runtime_root = REPO_ROOT / "apps" / "christian" / "data"
    runtime_root = runtime_root.resolve()
    runtime_root.mkdir(parents=True, exist_ok=True)
    return runtime_root


def _remember_job_backend(job_id: str, spec: dict[str, str | int]) -> None:
    key = str(spec.get("key") or "")
    if not job_id or not key:
        return
    with _job_backend_lock:
        _job_backend_map[job_id] = key


def _backend_for_job(job_id: str) -> dict[str, str | int] | None:
    if not job_id:
        return None
    with _job_backend_lock:
        key = _job_backend_map.get(job_id)
    if not key:
        return None
    return BACKEND_SPECS.get(key)


def _extract_job_id_from_path(path: str) -> str:
    prefix = "/api/rsrd2/jobs/"
    if not path.startswith(prefix):
        return ""
    rest = path[len(prefix) :]
    if not rest:
        return ""
    return rest.split("/", 1)[0].strip()


def _backend_spec_for_path(path: str) -> dict[str, str | int] | None:
    if path.startswith("/api/rsrd2/jobs/"):
        mapped = _backend_for_job(_extract_job_id_from_path(path))
        if mapped is not None:
            return mapped
    if any(path.startswith(prefix) for prefix in DATALAKE_API_PREFIXES):
        return BACKEND_SPECS["datalake"]
    if any(path.startswith(prefix) for prefix in TEILENUMMER_API_PREFIXES):
        return BACKEND_SPECS["teilenummer"]
    if any(path.startswith(prefix) for prefix in RSRD_API_PREFIXES):
        return BACKEND_SPECS["rsrd"]
    return None


def _probe_backend_sqlite_path(spec: dict[str, str | int], timeout: float = 1.2) -> str:
    try:
        req = Request(_backend_url(spec, "/api/rsrd2/sqlite_locks", ""), method="GET")
        with urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            if isinstance(payload, dict):
                db_path = payload.get("db_path")
                if isinstance(db_path, str):
                    return db_path
    except Exception:
        return ""
    return ""


def _terminate_listener_on_port(port: int) -> None:
    try:
        proc = subprocess.run(
            ["lsof", "-ti", f"tcp:{port}", "-sTCP:LISTEN"],
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception:
        return
    pids = []
    for raw in (proc.stdout or "").splitlines():
        value = raw.strip()
        if value.isdigit():
            pids.append(int(value))
    if not pids:
        return
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass
    time.sleep(0.3)
    for pid in pids:
        try:
            os.kill(pid, 0)
        except Exception:
            continue
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:
            pass


def _ensure_backend(spec: dict[str, str | int]) -> None:
    backend_host = str(spec["host"])
    backend_port = int(spec["port"])

    preferred_python = Path("/Users/crupp/SPAREPART/.venv/bin/python")
    python_bin = os.environ.get("MFDAPPS_RSRD_PYTHON")
    if not python_bin:
        python_bin = str(preferred_python if preferred_python.exists() else Path(sys.executable))

    persistent_runtime_root = _resolve_runtime_root()
    default_local_runtime = (
        "/tmp/mfdapps-runtime"
        if persistent_runtime_root == Path("/runtime")
        else str(persistent_runtime_root)
    )
    local_runtime_raw = os.environ.get("MFDAPPS_LOCAL_RUNTIME_ROOT", default_local_runtime).strip()
    local_runtime_root = Path(local_runtime_raw).expanduser().resolve()
    local_runtime_root.mkdir(parents=True, exist_ok=True)
    sqlite_dir = local_runtime_root / "sqlite"
    sqlite_dir.mkdir(parents=True, exist_ok=True)
    sqlite_backup_dir = persistent_runtime_root / "sqlite"
    sqlite_backup_dir.mkdir(parents=True, exist_ok=True)
    sqlite_path = sqlite_dir / str(spec["sqlite_file"])
    sqlite_backup_path = sqlite_backup_dir / str(spec["sqlite_file"])
    if (not sqlite_path.exists()) and sqlite_backup_path.exists():
        try:
            shutil.copy2(sqlite_backup_path, sqlite_path)
        except Exception:
            pass
    sqlite_path.touch(exist_ok=True)

    if _is_tcp_reachable(backend_host, backend_port):
        running_db_path = _probe_backend_sqlite_path(spec)
        expected = str(sqlite_path.resolve())
        if running_db_path:
            try:
                if str(Path(running_db_path).resolve()) == expected:
                    return
            except Exception:
                if running_db_path == expected:
                    return
        if backend_host in {"127.0.0.1", "localhost"}:
            _terminate_listener_on_port(backend_port)
            if _is_tcp_reachable(backend_host, backend_port):
                return
        else:
            return

    env = os.environ.copy()
    env["MFDAPPS_ENFORCE_ONEDRIVE"] = "0"
    env["MFDAPPS_HOME"] = str(REPO_ROOT)
    env["MFDAPPS_RUNTIME_ROOT"] = str(local_runtime_root)
    env["SQLITE_PATH"] = str(sqlite_path)
    env["SQLITE_BACKUP_PATH"] = str(sqlite_backup_path)
    env["MFDAPPS_CREDENTIALS_DIR"] = str(REPO_ROOT / "credentials")
    env["MFDAPPS_FRONTEND_DIR"] = str(REPO_ROOT / "apps" / "christian" / "AppRSRD" / "frontend")
    env["PYTHONPATH"] = (
        f"{REPO_ROOT}:"
        f"{REPO_ROOT / 'packages' / 'sparepart-shared' / 'src'}:"
        f"{REPO_ROOT / 'apps' / 'christian' / 'AppRSRD' / 'src'}"
    )

    log_path = Path(str(spec["log_path"]))
    with log_path.open("ab") as log_file:
        subprocess.Popen(
            [
                python_bin,
                "-m",
                "uvicorn",
                "app_rsrd.main:app",
                "--host",
                backend_host,
                "--port",
                str(backend_port),
            ],
            cwd=REPO_ROOT,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )

    for _ in range(150):
        if _is_tcp_reachable(backend_host, backend_port):
            return
        time.sleep(0.2)
    tail_text = ""
    try:
        if log_path.exists():
            lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
            tail_text = "\n".join(lines[-25:])
    except Exception:
        tail_text = ""
    detail = f"{spec['name']}-Backend konnte nicht gestartet werden. Siehe {log_path}"
    if tail_text:
        detail = f"{detail}\n--- log tail ---\n{tail_text}"
    raise RuntimeError(
        detail
    )


def _read_backend_log_tail(spec: dict[str, str | int], max_lines: int = 40) -> str:
    log_path = Path(str(spec["log_path"]))
    try:
        if not log_path.exists():
            return ""
        lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        return "\n".join(lines[-max_lines:])
    except Exception:
        return ""


def _source_search_dirs() -> list[Path]:
    configured = os.environ.get(MEHR_SOURCE_DIRS_ENV, "").strip()
    raw_dirs: list[Path] = []
    if configured:
        for entry in configured.split(os.pathsep):
            text = entry.strip()
            if not text:
                continue
            path = Path(text).expanduser()
            if not path.is_absolute():
                path = REPO_ROOT / path
            raw_dirs.append(path)

    raw_dirs.extend(
        [
            MEHR_SOURCE_DIR,
            MEHR_ROOT / "Quellen",
            REPO_ROOT / "data" / "mehrkilometer",
            REPO_ROOT / "data",
        ]
    )

    unique: list[Path] = []
    seen: set[str] = set()
    for directory in raw_dirs:
        resolved = directory.resolve()
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        unique.append(resolved)
    return unique


def _is_under_directory(path: Path, base_dir: Path) -> bool:
    try:
        path.resolve().relative_to(base_dir.resolve())
    except ValueError:
        return False
    return True


def _is_under_any_directory(path: Path, directories: list[Path]) -> bool:
    return any(_is_under_directory(path, base) for base in directories)


def _collect_source_excel_files() -> list[Path]:
    files: list[Path] = []
    for base_dir in _source_search_dirs():
        if not base_dir.exists() or not base_dir.is_dir():
            continue
        for candidate in base_dir.rglob("*"):
            if not candidate.is_file():
                continue
            if candidate.suffix.lower() not in MEHR_SOURCE_EXTENSIONS:
                continue
            name = candidate.name.lower()
            if name.startswith("~$"):
                continue
            if name.startswith(MEHR_OUTPUT_PREFIXES):
                continue
            files.append(candidate.resolve())
    return files


def _guess_source_year(name: str) -> int | None:
    years = re.findall(r"(20\d{2})", name)
    if not years:
        return None
    try:
        return max(int(year) for year in years)
    except ValueError:
        return None


def _source_match_score(path: Path, kind: str, year: int) -> int:
    name = path.name.lower()
    stem = path.stem.lower()
    tokens = [token for token in re.split(r"[^a-z0-9]+", stem) if token]
    score = 0

    if kind == "overview":
        if "vorlage" in name:
            score += 140
        if "template" in name:
            score += 60
        if "kilometer" in name:
            score -= 120
    if kind == "kilometer":
        if "kilometer" in name:
            score += 140
        if "vorlage" in name or "template" in name:
            score -= 120
    if "km" in tokens:
        score += 20

    if str(year) in name:
        score += 50
    guessed_year = _guess_source_year(name)
    if guessed_year is not None and guessed_year != year:
        score -= min(30, abs(year - guessed_year))

    if _is_under_directory(path, MEHR_SOURCE_DIR):
        score += 30
    return score


def _select_best_source_file(
    candidates: list[Path], kind: str, year: int, exclude: Path | None = None
) -> Path | None:
    ranked: list[tuple[int, float, Path]] = []
    for candidate in candidates:
        if exclude is not None and candidate == exclude:
            continue
        score = _source_match_score(candidate, kind, year)
        if score <= 0:
            continue
        ranked.append((score, candidate.stat().st_mtime, candidate))

    if not ranked:
        return None
    ranked.sort(key=lambda item: (item[0], item[1], str(item[2])), reverse=True)
    return ranked[0][2]


def _discover_source_files(year: int) -> tuple[Path | None, Path | None]:
    candidates = _collect_source_excel_files()
    source_overview = _select_best_source_file(candidates, "overview", year)
    source_km = _select_best_source_file(
        candidates, "kilometer", year, exclude=source_overview
    )
    return source_overview, source_km


def _source_search_dirs_text() -> str:
    existing = [str(path) for path in _source_search_dirs() if path.exists()]
    if not existing:
        return "(keine vorhandenen Suchordner)"
    return ", ".join(existing)


def _fabric_missing_env() -> list[str]:
    return [key for key in FABRIC_REQUIRED_ENV_VARS if not os.environ.get(key, "").strip()]


def _fabric_server_with_port() -> str:
    server = os.environ["FABRIC_SQL_SERVER"].strip()
    if server.startswith("tcp:"):
        server = server[4:]
    if "," in server:
        return f"tcp:{server}"
    port = os.environ.get("FABRIC_SQL_PORT", "1433").strip() or "1433"
    return f"tcp:{server},{port}"


def _connect_fabric_sql():
    missing = _fabric_missing_env()
    if missing:
        raise RuntimeError(
            "Fehlende Fabric-Variablen: " + ", ".join(missing) + ". Bitte /Users/crupp/dev/MFDAPPS/.ENV prüfen."
        )

    try:
        import pyodbc  # type: ignore
    except Exception as exc:  # pragma: no cover - env dependent
        raise RuntimeError(
            "Python-Paket 'pyodbc' fehlt. Bitte lokal installieren: "
            "/Users/crupp/dev/MFDAPPS/.venv/bin/pip install pyodbc"
        ) from exc

    timeout = int((os.environ.get("FABRIC_SQL_TIMEOUT") or "20").strip())
    connect_retries_raw = (os.environ.get("FABRIC_SQL_CONNECT_RETRIES") or "3").strip()
    retry_delay_raw = (os.environ.get("FABRIC_SQL_RETRY_DELAY_SEC") or "1.5").strip()
    try:
        connect_retries = int(connect_retries_raw)
    except ValueError:
        connect_retries = 3
    try:
        retry_delay = float(retry_delay_raw)
    except ValueError:
        retry_delay = 1.5
    connect_retries = max(1, min(8, connect_retries))
    retry_delay = max(0.2, min(15.0, retry_delay))
    driver = (os.environ.get("FABRIC_SQL_DRIVER") or "ODBC Driver 18 for SQL Server").strip()
    conn_str = (
        f"Driver={{{driver}}};"
        f"Server={_fabric_server_with_port()};"
        f"Database={os.environ['FABRIC_SQL_DATABASE'].strip()};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Authentication=ActiveDirectoryServicePrincipal;"
        f"Authority Id={os.environ['FABRIC_TENANT_ID'].strip()};"
        f"UID={os.environ['FABRIC_CLIENT_ID'].strip()};"
        f"PWD={os.environ['FABRIC_CLIENT_SECRET'].strip()};"
    )
    last_exc = None
    for attempt in range(1, connect_retries + 1):
        try:
            return pyodbc.connect(conn_str, timeout=timeout)
        except Exception as exc:
            last_exc = exc
            message = str(exc).lower()
            transient = (
                "hyt00" in message
                or "login timeout expired" in message
                or "08001" in message
                or "network-related" in message
            )
            if not transient or attempt >= connect_retries:
                break
            sleep_seconds = retry_delay * attempt
            time.sleep(sleep_seconds)
    raise RuntimeError(
        f"Fabric SQL Verbindung fehlgeschlagen: {last_exc} "
        f"(Versuche: {connect_retries}, Timeout je Versuch: {timeout}s)"
    ) from last_exc


def _fabric_health_check() -> dict:
    with _connect_fabric_sql() as conn:
        cur = conn.cursor()
        cur.execute("SELECT TOP 1 1 AS ok")
        row = cur.fetchone()
        return {
            "status": "ok" if row else "error",
            "probe": int(row[0]) if row else None,
            "server": os.environ.get("FABRIC_SQL_SERVER", ""),
            "database": os.environ.get("FABRIC_SQL_DATABASE", ""),
        }


def _fetch_stagli_scnm(limit: int) -> list[str]:
    with _connect_fabric_sql() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT TOP ({limit}) SCNM FROM landing.stagli")
        rows = cur.fetchall()
        return ["" if row[0] is None else str(row[0]) for row in rows]


def _mehr_fabric_table_name(year: int) -> str:
    safe_year = max(2000, min(3000, int(year)))
    return f"Mehr_Kiloemter_{safe_year}"


def _sqlite_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _ensure_mehr_fabric_sqlite_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mehrkilometer_fabric_status (
            year INTEGER PRIMARY KEY,
            table_name TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            refreshed_at TEXT NOT NULL,
            sqlite_path TEXT NOT NULL,
            source_query TEXT NOT NULL
        )
        """
    )
    status_columns = {
        str(row[1]).lower() for row in conn.execute("PRAGMA table_info(mehrkilometer_fabric_status)")
    }
    if "table_name" not in status_columns:
        conn.execute(
            "ALTER TABLE mehrkilometer_fabric_status ADD COLUMN table_name TEXT NOT NULL DEFAULT ''"
        )


def _build_mehrkilometer_fabric_sql(year: int) -> str:
    year_start_date = date(year, 1, 1)
    year_end_date = date(year, 12, 31)
    year_start = int(year_start_date.strftime("%Y%m%d"))
    year_end = int(year_end_date.strftime("%Y%m%d"))
    year_start_minus_1 = int((year_start_date - timedelta(days=1)).strftime("%Y%m%d"))

    return f"""
WITH vertraege_jahr AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY AGNB, PONR, BANO, FVDT, TEDA) AS vertrag_id,
        CUPL AS Kundennummer,
        SCNM AS Kundenname,
        AGNB AS Vertragsnummer,
        PONR AS Vertragsposition,
        BANO AS Seriennummer,
        FVDT AS Vertragsstart,
        TEDA AS Vertragsende,
        CASE
            WHEN FVDT < {year_start} THEN {year_start}
            ELSE FVDT
        END AS Abrechnungstart,
        CASE
            WHEN ISNULL(NULLIF(TEDA, 0), {year_end}) > {year_end} THEN {year_end}
            ELSE ISNULL(NULLIF(TEDA, 0), {year_end})
        END AS Abrechnungsende
    FROM landing.stagli
    WHERE BANO <> ''
      AND FVDT <= {year_end}
      AND ISNULL(NULLIF(TEDA, 0), {year_end}) >= {year_start}
),
vertraege_mit_suchdatum AS (
    SELECT
        v.*,
        CAST(
            CONVERT(
                char(8),
                DATEADD(day, -1, CONVERT(date, CAST(v.Abrechnungstart AS char(8)), 112)),
                112
            ) AS int
        ) AS Start_KM_Suchdatum
    FROM vertraege_jahr v
),
km_pro_tag AS (
    SELECT
        SERIENNUMMER,
        ZEIT,
        MAX(KILOMETER) AS Kilometer
    FROM landing.AWSNOTIFICATIONSHORT
    WHERE SERIENNUMMER <> ''
      AND ZEIT > 0
    GROUP BY SERIENNUMMER, ZEIT
),
start_km_ranked AS (
    SELECT
        v.vertrag_id,
        k.ZEIT AS KM_Start_Datum_Echt,
        k.Kilometer AS KM_Abrechnungstart,
        ROW_NUMBER() OVER (PARTITION BY v.vertrag_id ORDER BY k.ZEIT ASC) AS rn
    FROM vertraege_mit_suchdatum v
    INNER JOIN km_pro_tag k
        ON k.SERIENNUMMER = v.Seriennummer
       AND k.ZEIT >= v.Start_KM_Suchdatum
),
start_km AS (
    SELECT
        vertrag_id,
        KM_Start_Datum_Echt,
        KM_Abrechnungstart
    FROM start_km_ranked
    WHERE rn = 1
),
ende_km_ranked AS (
    SELECT
        v.vertrag_id,
        k.ZEIT AS KM_Ende_Datum_Echt,
        k.Kilometer AS KM_Abrechnungsende,
        ROW_NUMBER() OVER (PARTITION BY v.vertrag_id ORDER BY k.ZEIT DESC) AS rn
    FROM vertraege_mit_suchdatum v
    INNER JOIN km_pro_tag k
        ON k.SERIENNUMMER = v.Seriennummer
       AND k.ZEIT <= v.Abrechnungsende
),
ende_km AS (
    SELECT
        vertrag_id,
        KM_Ende_Datum_Echt,
        KM_Abrechnungsende
    FROM ende_km_ranked
    WHERE rn = 1
)
SELECT
    v.Kundennummer,
    v.Kundenname,
    v.Vertragsnummer,
    v.Vertragsposition,
    v.Seriennummer,
    v.Vertragsstart,
    v.Vertragsende,
    v.Abrechnungstart,
    v.Abrechnungsende,
    s.KM_Start_Datum_Echt,
    e.KM_Ende_Datum_Echt,
    CASE
        WHEN s.KM_Start_Datum_Echt >= {year_start_minus_1}
         AND e.KM_Ende_Datum_Echt BETWEEN {year_start} AND {year_end}
        THEN s.KM_Abrechnungstart
        ELSE 0
    END AS KM_Abrechnungstart,
    CASE
        WHEN s.KM_Start_Datum_Echt >= {year_start_minus_1}
         AND e.KM_Ende_Datum_Echt BETWEEN {year_start} AND {year_end}
        THEN e.KM_Abrechnungsende
        ELSE 0
    END AS KM_Abrechnungsende,
    CASE
        WHEN s.KM_Start_Datum_Echt >= {year_start_minus_1}
         AND e.KM_Ende_Datum_Echt BETWEEN {year_start} AND {year_end}
         AND s.KM_Abrechnungstart IS NOT NULL
         AND e.KM_Abrechnungsende IS NOT NULL
        THEN e.KM_Abrechnungsende - s.KM_Abrechnungstart
        ELSE 0
    END AS KM_Differenz,
    CASE
        WHEN s.KM_Start_Datum_Echt >= {year_start_minus_1}
         AND e.KM_Ende_Datum_Echt BETWEEN {year_start} AND {year_end}
        THEN 1 ELSE 0
    END AS KM_Daten_OK
FROM vertraege_mit_suchdatum v
LEFT JOIN start_km s ON s.vertrag_id = v.vertrag_id
LEFT JOIN ende_km e ON e.vertrag_id = v.vertrag_id
"""


def _refresh_mehrkilometer_fabric_sqlite(year: int, progress=None) -> dict:
    def _emit(stage: str, message: str, rows_total: int | None = None, rows_written: int | None = None) -> None:
        if progress is None:
            return
        payload = {
            "stage": stage,
            "message": message,
        }
        if rows_total is not None:
            payload["rows_total"] = int(rows_total)
        if rows_written is not None:
            payload["rows_written"] = int(rows_written)
        try:
            progress(payload)
        except Exception:
            # Progress reporting must never break the import flow.
            pass

    query = _build_mehrkilometer_fabric_sql(year)
    _emit("query_start", "Fabric SQL wird ausgeführt ...")
    with _connect_fabric_sql() as conn:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
    _emit("query_done", "Fabric SQL abgeschlossen.", rows_total=len(rows))

    refreshed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    db_path = Path(os.environ.get("MEHR_FABRIC_SQLITE_PATH", str(MEHR_FABRIC_SQLITE))).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    def _to_sqlite_value(value):
        if isinstance(value, Decimal):
            return float(value)
        return value

    _emit("sqlite_prepare", "SQLite-Tabelle wird vorbereitet.", rows_total=len(rows))
    with sqlite3.connect(db_path) as sqlite_conn:
        _ensure_mehr_fabric_sqlite_schema(sqlite_conn)
        table_name = _mehr_fabric_table_name(year)
        sqlite_conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        sqlite_conn.execute(
            f"""
            CREATE TABLE "{table_name}" (
                kundennummer TEXT,
                kundenname TEXT,
                vertragsnummer TEXT,
                vertragsposition INTEGER,
                seriennummer TEXT,
                vertragsstart INTEGER,
                vertragsende INTEGER,
                abrechnungstart INTEGER,
                abrechnungsende INTEGER,
                km_start_datum_echt INTEGER,
                km_ende_datum_echt INTEGER,
                km_abrechnungstart REAL,
                km_abrechnungsende REAL,
                km_differenz REAL,
                km_daten_ok INTEGER,
                loaded_at TEXT NOT NULL
            )
            """
        )
        insert_sql = f"""
            INSERT INTO "{table_name}" (
                {", ".join(MEHR_FABRIC_COLUMNS)},
                loaded_at
            ) VALUES (
                {", ".join(["?"] * len(MEHR_FABRIC_COLUMNS))}, ?
            )
        """
        payload = [
            (*[_to_sqlite_value(value) for value in row], refreshed_at)
            for row in rows
        ]
        chunk_size_raw = (os.environ.get("MEHR_FABRIC_INSERT_BATCH") or "500").strip()
        try:
            chunk_size = int(chunk_size_raw)
        except ValueError:
            chunk_size = 500
        chunk_size = max(100, min(5000, chunk_size))
        if payload:
            written = 0
            for idx in range(0, len(payload), chunk_size):
                chunk = payload[idx : idx + chunk_size]
                sqlite_conn.executemany(insert_sql, chunk)
                written += len(chunk)
                _emit(
                    "sqlite_insert",
                    f"SQLite schreibt Daten ({written}/{len(payload)}) ...",
                    rows_total=len(payload),
                    rows_written=written,
                )
        else:
            _emit(
                "sqlite_insert",
                "Keine Zeilen für den Import vorhanden.",
                rows_total=0,
                rows_written=0,
            )
        sqlite_conn.execute(
            """
            INSERT INTO mehrkilometer_fabric_status (
                year, table_name, row_count, refreshed_at, sqlite_path, source_query
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(year) DO UPDATE SET
                table_name=excluded.table_name,
                row_count=excluded.row_count,
                refreshed_at=excluded.refreshed_at,
                sqlite_path=excluded.sqlite_path,
                source_query=excluded.source_query
            """,
            (year, table_name, len(rows), refreshed_at, str(db_path), query),
        )
        sqlite_conn.commit()
    _emit("done", "Import abgeschlossen.", rows_total=len(rows), rows_written=len(rows))

    return {
        "year": year,
        "table_name": _mehr_fabric_table_name(year),
        "row_count": len(rows),
        "refreshed_at": refreshed_at,
        "sqlite_path": str(db_path),
        "status": "ok",
    }


def _mehrkilometer_fabric_status(year: int) -> dict:
    db_path = Path(os.environ.get("MEHR_FABRIC_SQLITE_PATH", str(MEHR_FABRIC_SQLITE))).resolve()
    if not db_path.exists():
        return {
            "year": year,
            "exists": False,
            "table_name": _mehr_fabric_table_name(year),
            "sqlite_path": str(db_path),
            "row_count": 0,
            "refreshed_at": None,
        }

    with sqlite3.connect(db_path) as sqlite_conn:
        _ensure_mehr_fabric_sqlite_schema(sqlite_conn)
        row = sqlite_conn.execute(
            """
            SELECT table_name, row_count, refreshed_at, sqlite_path
            FROM mehrkilometer_fabric_status
            WHERE year = ?
            """,
            (year,),
        ).fetchone()
        if row is None:
            return {
                "year": year,
                "exists": False,
                "table_name": _mehr_fabric_table_name(year),
                "sqlite_path": str(db_path),
                "row_count": 0,
                "refreshed_at": None,
            }
        table_name = str(row[0] or _mehr_fabric_table_name(year))
        exists = _sqlite_table_exists(sqlite_conn, table_name)
        return {
            "year": year,
            "exists": exists,
            "table_name": table_name,
            "sqlite_path": str(row[3] or db_path),
            "row_count": int(row[1] or 0),
            "refreshed_at": row[2],
        }


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


def _create_settlement_files(year: int) -> tuple[Path, Path]:
    module = _load_legacy_xlsx_module()
    MEHR_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    source_overview, source_km = _discover_source_files(year)
    if source_overview is None:
        raise FileNotFoundError(
            "Vorlagen-Datei nicht gefunden. "
            f"Durchsuchte Ordner: {_source_search_dirs_text()}"
        )
    if source_km is None:
        raise FileNotFoundError(
            "Kilometer-Datei nicht gefunden. "
            f"Durchsuchte Ordner: {_source_search_dirs_text()}"
        )

    template_rows = module.read_template(source_overview)
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
    output_file = MEHR_OUTPUT_DIR / f"vertragsuebersicht_{year}_{timestamp}.xlsx"
    detail_file = MEHR_OUTPUT_DIR / f"einzelabrechnungen_detail_{year}_{timestamp}.xlsx"
    module.write_xlsx(
        output_file,
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
    return output_file, detail_file


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

    _, source_km = _discover_source_files(year)
    if source_km is None:
        raise FileNotFoundError(
            "Kilometerdatei fehlt. "
            f"Durchsuchte Ordner: {_source_search_dirs_text()}"
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
        download_url = (
            f"/api/mehrkilometer/download?kind={quote(kind)}&name={quote(file_path.name)}"
        )
        try:
            rel_path = file_path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
            download_url += f"&rel={quote(rel_path)}"
        except ValueError:
            pass
        return {
            "exists": True,
            "name": file_path.name,
            "download_url": download_url,
        }

    def _pick_latest(self, directory: Path, predicate) -> Path | None:
        return _pick_latest_file(directory, predicate)

    def _build_mehr_payload(self, year: int) -> dict:
        source_overview, source_km = _discover_source_files(year)
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
            "paths": {
                "output_dir": {
                    "path": str(MEHR_OUTPUT_DIR),
                    "browser_url": f"/{MEHR_OUTPUT_DIR.resolve().relative_to(REPO_ROOT.resolve()).as_posix()}/",
                },
                "source_dirs": [str(path) for path in _source_search_dirs()],
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

    def _safe_repo_relative_file(self, rel_path: str) -> Path | None:
        if not rel_path or rel_path.startswith("/") or rel_path.startswith("\\"):
            return None
        if ".." in rel_path:
            return None
        candidate = (REPO_ROOT / rel_path).resolve()
        try:
            candidate.relative_to(REPO_ROOT.resolve())
        except ValueError:
            return None
        if not candidate.is_file():
            return None
        return candidate

    def _resolve_source_file_for_download(self, filename: str, kind: str) -> Path | None:
        if "/" in filename or "\\" in filename or ".." in filename:
            return None
        candidates = [path for path in _collect_source_excel_files() if path.name == filename]
        if not candidates:
            return None
        year_hint = _guess_source_year(filename) or DEFAULT_YEAR
        source_kind = "overview" if kind == "source_overview" else "kilometer"
        return _select_best_source_file(candidates, source_kind, year_hint)

    def _is_allowed_download_file(self, path: Path, kind: str) -> bool:
        if kind in {"source_overview", "source_kilometer"}:
            return _is_under_any_directory(path, _source_search_dirs())
        return _is_under_directory(path, MEHR_OUTPUT_DIR)

    def _serve_mehr_download(self, query: dict[str, list[str]]) -> None:
        kind = (query.get("kind") or [""])[0]
        name = (query.get("name") or [""])[0]
        rel = (query.get("rel") or [""])[0]

        dir_map = {
            "output_overview": MEHR_OUTPUT_DIR,
            "output_details": MEHR_OUTPUT_DIR,
            "special_output_overview": MEHR_OUTPUT_DIR,
            "special_output_details": MEHR_OUTPUT_DIR,
        }
        if kind not in {
            "source_overview",
            "source_kilometer",
            "output_overview",
            "output_details",
            "special_output_overview",
            "special_output_details",
        }:
            self._send_json({"detail": "Ungültiger Download-Typ."}, status=400)
            return

        file_path = None
        if rel:
            file_path = self._safe_repo_relative_file(rel)
            if file_path is not None and file_path.name != name:
                file_path = None
            if file_path is not None and not self._is_allowed_download_file(file_path, kind):
                file_path = None

        if file_path is None and kind in {"source_overview", "source_kilometer"}:
            file_path = self._resolve_source_file_for_download(name, kind)

        if file_path is None and kind in dir_map:
            base_dir = dir_map[kind]
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

    def _proxy_to_backend(
        self,
        method: str,
        path: str,
        query: str,
        backend_spec: dict[str, str | int],
    ) -> None:
        try:
            _ensure_backend(backend_spec)
        except Exception as exc:
            self._send_json({"detail": str(exc)}, status=502)
            return

        target_url = _backend_url(backend_spec, path, query)
        request_body = None
        if method in {"POST", "PUT", "PATCH"}:
            content_len = int(self.headers.get("Content-Length", "0") or "0")
            request_body = self.rfile.read(content_len) if content_len > 0 else None

        req = Request(target_url, data=request_body, method=method)
        for key, value in self.headers.items():
            low = key.lower()
            if low in {"host", "connection", "content-length", "transfer-encoding"}:
                continue
            req.add_header(key, value)

        hop_by_hop = {
            "connection",
            "keep-alive",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailers",
            "transfer-encoding",
            "upgrade",
        }

        timeout_seconds = 120
        if method.upper() == "POST" and (
            path.endswith("/reload")
            or path.startswith("/api/datalake/")
            or path.startswith("/api/datalake-sync/")
            or path.startswith("/api/teilenummer/")
            or path.startswith("/api/wagensuche/")
        ):
            timeout_seconds = 1800

        try:
            with urlopen(req, timeout=timeout_seconds) as upstream:
                status = upstream.getcode()
                response_body = upstream.read()
                headers = upstream.getheaders()
        except HTTPError as exc:
            status = exc.code
            response_body = exc.read()
            headers = exc.headers.items() if exc.headers else []
            generic_500 = status >= 500 and response_body.strip().lower() in {
                b"",
                b"internal server error",
                b'{"detail":"internal server error"}',
                b'{"detail":"internal server error."}',
            }
            if generic_500:
                tail_text = _read_backend_log_tail(backend_spec)
                payload = {
                    "detail": f"Backend-Fehler {status}",
                    "backend_log_tail": tail_text or "Kein Log-Tail verfügbar.",
                }
                response_body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                headers = [("Content-Type", "application/json; charset=utf-8")]
        except Exception as exc:
            tail_text = _read_backend_log_tail(backend_spec)
            payload = {"detail": f"Backend-Proxy fehlgeschlagen: {exc}"}
            if tail_text:
                payload["backend_log_tail"] = tail_text
            self._send_json(payload, status=502)
            return

        # Remember which backend owns async jobs so /api/rsrd2/jobs/{id} can be routed correctly.
        try:
            if response_body:
                payload = json.loads(response_body.decode("utf-8"))
                if isinstance(payload, dict):
                    job_id = payload.get("job_id") or payload.get("id")
                    if isinstance(job_id, str) and job_id:
                        _remember_job_backend(job_id, backend_spec)
            path_job_id = _extract_job_id_from_path(path)
            if path_job_id and status < 400:
                _remember_job_backend(path_job_id, backend_spec)
        except Exception:
            pass

        self.send_response(status)
        for key, value in headers:
            low = key.lower()
            if low in hop_by_hop or low == "content-length":
                continue
            if low == "location":
                parsed_loc = urlparse(value)
                if parsed_loc.hostname in {"127.0.0.1", "localhost", str(backend_spec["host"])}:
                    local_path = parsed_loc.path or "/"
                    if parsed_loc.query:
                        local_path = f"{local_path}?{parsed_loc.query}"
                    value = local_path
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        if response_body:
            self.wfile.write(response_body)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        path = parsed.path

        if path in {"/apps/christian/AppRSRD", "/apps/christian/AppRSRD/"}:
            self.send_response(302)
            self.send_header("Location", "/apps/christian/AppRSRD/frontend/rsrd2.html")
            self.end_headers()
            return

        backend_spec = _backend_spec_for_path(path)
        if backend_spec is not None:
            self._proxy_to_backend("GET", path, parsed.query, backend_spec)
            return

        if path == "/apps/christian/AppMehrkilometer/":
            self.send_response(302)
            self.send_header("Location", "/apps/christian/AppMehrkilometer/frontend/")
            self.end_headers()
            return

        if path == "/apps/christian/AppTeilenummer/":
            self.send_response(302)
            self.send_header(
                "Location", "/apps/christian/AppTeilenummer/frontend/teilenummer.html"
            )
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

        if path == "/api/mehrkilometer/fabric/health":
            try:
                self._send_json(_fabric_health_check())
            except Exception as exc:
                self._send_json({"detail": str(exc)}, status=500)
            return

        if path == "/api/mehrkilometer/fabric/status":
            year_raw = (query.get("year") or [""])[0]
            try:
                year = int(year_raw) if year_raw else DEFAULT_YEAR
            except ValueError:
                year = DEFAULT_YEAR
            year = max(2000, min(3000, year))
            try:
                self._send_json(_mehrkilometer_fabric_status(year))
            except Exception as exc:
                self._send_json({"detail": str(exc)}, status=500)
            return

        if path == "/api/mehrkilometer/fabric/stagli":
            raw_limit = (query.get("limit") or ["100"])[0]
            try:
                limit = int(raw_limit)
            except ValueError:
                limit = 100
            limit = max(1, min(5000, limit))
            try:
                values = _fetch_stagli_scnm(limit)
                self._send_json(
                    {
                        "count": len(values),
                        "query": f"SELECT TOP ({limit}) SCNM FROM landing.stagli",
                        "rows": values,
                    }
                )
            except Exception as exc:
                self._send_json({"detail": str(exc)}, status=500)
            return

        if path == "/api/mehrkilometer/download":
            self._serve_mehr_download(query)
            return

        super().do_GET()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        path = parsed.path

        backend_spec = _backend_spec_for_path(path)
        if backend_spec is not None:
            self._proxy_to_backend("POST", path, parsed.query, backend_spec)
            return

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

        if path == "/api/mehrkilometer/fabric/import":
            year_raw = (query.get("year") or [""])[0]
            try:
                year = int(year_raw) if year_raw else DEFAULT_YEAR
            except ValueError:
                year = DEFAULT_YEAR
            year = max(2000, min(3000, year))
            try:
                payload = _refresh_mehrkilometer_fabric_sqlite(year)
            except Exception as exc:
                self._send_json({"detail": str(exc)}, status=500)
                return
            self._send_json(payload)
            return

        if path == "/api/mehrkilometer/vertragsexcel/import":
            year_raw = (query.get("year") or [""])[0]
            try:
                year = int(year_raw) if year_raw else DEFAULT_YEAR
            except ValueError:
                year = DEFAULT_YEAR
            year = max(2000, min(3000, year))
            self._send_json(
                {
                    "year": year,
                    "status": "pending",
                    "detail": "Vertragsexcel-SQL noch nicht hinterlegt. Bitte SQL liefern.",
                },
                status=501,
            )
            return

        if path == "/api/mehrkilometer/create":
            year_raw = (query.get("year") or [""])[0]
            try:
                year = int(year_raw) if year_raw else DEFAULT_YEAR
            except ValueError:
                year = DEFAULT_YEAR
            year = max(2000, min(3000, year))
            try:
                created_overview, created_details = _create_settlement_files(year)
            except Exception as exc:
                self._send_json(
                    {"detail": f"Abrechnung fehlgeschlagen: {exc}"},
                    status=500,
                )
                return

            payload = self._build_mehr_payload(year)
            payload["outputs"] = {
                "overview": self._file_info(created_overview, "output_overview"),
                "details": self._file_info(created_details, "output_details"),
            }
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
