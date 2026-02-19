"""Run SQL against Infor Data Fabric (Compass JDBC)."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import shutil
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import jaydebeapi

try:  # pragma: no cover - script vs package execution
    from .env_loader import get_credentials_root, load_project_dotenv
except ImportError:  # type: ignore
    from env_loader import get_credentials_root, load_project_dotenv  # type: ignore

load_project_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CREDENTIALS_ROOT = get_credentials_root()
IONAPI_DIR = CREDENTIALS_ROOT / "ionapi"
JDBC_DIR = CREDENTIALS_ROOT / "jdbc"

PREFERRED_IONAPI = [
    "Infor Compass JDBC Driver.ionapi",
    "MFD_Backend_Python.ionapi",
]

PREFERRED_JDBC = [
    "infor-compass-jdbc-2025.11.jar",
]

SCHEME_CONFIG = {
    "datalake": {
        "scheme": "infordatalake",
        "path": "",
        "requires_catalog": False,
        "use_tenant_host": True,
    },
    "datawarehouse": {
        "scheme": "infordatawarehouse",
        "path": "",
        "requires_catalog": False,
        "use_tenant_host": True,
    },
    "sourcedata": {
        "scheme": "inforsourcedata",
        "path": "/DATAFABRIC/compass/v2/jdbc",
        "requires_catalog": True,
        "use_tenant_host": False,
    },
}


def _find_file(directory: Path, preferred_names: List[str], pattern: str) -> Path:
    """Return a matching file from directory according to preferred order."""
    if directory.is_file():
        return directory
    if directory.is_dir():
        for name in preferred_names:
            candidate = directory / name
            if candidate.exists():
                return candidate
        matches = sorted(directory.glob(pattern))
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            return matches[0]
    raise FileNotFoundError(f"Keine Datei in {directory} gefunden (Pattern {pattern}).")


def load_ionapi(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def build_jdbc_url(ion_cfg: Dict[str, Any], scheme_key: str, catalog: Optional[str] = None) -> str:
    scheme_info = SCHEME_CONFIG[scheme_key]
    parsed = urlparse(ion_cfg["iu"])
    if scheme_info.get("use_tenant_host"):
        tenant = ion_cfg.get("ti")
        if not tenant:
            raise ValueError("Tenant 'ti' fehlt in der .ionapi Datei.")
        base = tenant
    else:
        if not parsed.netloc:
            raise ValueError(f"Ionapi 'iu' enthält keinen Host: {ion_cfg['iu']}")
        base = parsed.netloc

    if scheme_info["requires_catalog"]:
        if not catalog:
            raise ValueError("Für dieses Scheme ist --catalog erforderlich.")
        return f"jdbc:{scheme_info['scheme']}:{catalog}://{base}{scheme_info['path']}"

    path = scheme_info.get("path", "")
    if path:
        return f"jdbc:{scheme_info['scheme']}://{base}{path}"
    return f"jdbc:{scheme_info['scheme']}://{base}"


def build_properties(
    ion_cfg: Dict[str, Any],
    catalog: Optional[str],
    default_collection: Optional[str],
) -> Dict[str, str]:
    props = {
        "ION_API_CREDENTIALS": json.dumps(ion_cfg),
        "TENANT": ion_cfg.get("ti", ""),
    }
    if catalog:
        props["CATALOG"] = catalog
    if default_collection:
        props["DEFAULT_COLLECTION"] = default_collection
    return props


def load_sql(args: argparse.Namespace) -> str:
    if args.sql:
        return args.sql.strip()
    if args.sql_file:
        return Path(args.sql_file).read_text(encoding="utf-8")
    raise ValueError("SQL muss über --sql oder --sql-file angegeben werden.")


def ensure_limit(sql: str, limit: Optional[int]) -> str:
    if limit is None:
        return sql
    return f"SELECT * FROM ({sql.rstrip().rstrip(';')}) AS sub LIMIT {limit}"


def ensure_driver_ionapi(ionapi_path: Path, jdbc_path: Path) -> None:
    target = jdbc_path.parent / ionapi_path.name
    try:
        if ionapi_path.resolve() == target.resolve():
            return
    except OSError:
        pass
    if target.exists():
        return
    target.write_text(ionapi_path.read_text(encoding="utf-8-sig"), encoding="utf-8")


def _collect_support_jars(jdbc_path: Path) -> List[str]:
    jars = [str(jdbc_path)]
    support = sorted(jdbc_path.parent.glob("slf4j-*.jar"))
    for extra in support:
        if extra.resolve() != jdbc_path.resolve():
            jars.append(str(extra))
    return jars


def _sanitize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8")
        except Exception:
            return value.decode("utf-8", "ignore")
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        try:
            return isoformat()
        except Exception:
            pass
    return str(value)


def run_query(jdbc_url: str, jdbc_path: Path, props: Dict[str, str], sql: str) -> Dict[str, Any]:
    warnings.filterwarnings(
        "ignore",
        message="No type mapping for JDBC type 'TIMESTAMP_WITH_TIMEZONE'",
        category=UserWarning,
    )
    conn = jaydebeapi.connect(
        "com.infor.idl.jdbc.Driver",
        jdbc_url,
        props,
        jars=_collect_support_jars(jdbc_path),
    )
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = (
            [ (desc[0].strip("\"'") if isinstance(desc[0], str) else desc[0]) for desc in cursor.description ]
            if cursor.description
            else []
        )
        rows = cursor.fetchall()
        if columns:
            data = []
            for row in rows:
                record = {col: _sanitize_value(val) for col, val in zip(columns, row)}
                data.append(record)
        else:
            data = [[_sanitize_value(val) for val in row] for row in rows]
        return {"columns": columns, "rows": data}
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run SQL against Infor Data Fabric via Compass JDBC.")
    parser.add_argument("--ionapi", help="Pfad zur Compass .ionapi Datei", default=None)
    parser.add_argument("--jdbc-jar", help="Pfad zum Compass JDBC JAR", default=None)
    parser.add_argument("--scheme", choices=list(SCHEME_CONFIG.keys()), default="datalake")
    parser.add_argument("--catalog", help="Katalog (nur für scheme=sourcedata erforderlich, z.B. M3BE).")
    parser.add_argument("--default-collection", help="Optionaler Compass Collection Name.")
    parser.add_argument("--sql", help="SQL Text direkt in der CLI.")
    parser.add_argument("--sql-file", help="Pfad zu einer Datei mit SQL.")
    parser.add_argument("--limit", type=int, help="Optionales LIMIT (wird um das Statement herum gebaut).")
    parser.add_argument("--output", choices=["json", "table"], default="json")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        ionapi_path = Path(args.ionapi) if args.ionapi else _find_file(IONAPI_DIR, PREFERRED_IONAPI, "*.ionapi")
    except FileNotFoundError as err:
        print(str(err), file=sys.stderr)
        sys.exit(2)

    try:
        jdbc_path = Path(args.jdbc_jar) if args.jdbc_jar else _find_file(JDBC_DIR, PREFERRED_JDBC, "*.jar")
    except FileNotFoundError as err:
        print(str(err), file=sys.stderr)
        sys.exit(2)

    if not jdbc_path.exists():
        print(f"JDBC JAR nicht gefunden: {jdbc_path}", file=sys.stderr)
        sys.exit(2)

    scheme_info = SCHEME_CONFIG[args.scheme]
    if scheme_info["requires_catalog"] and not args.catalog:
        print("Für scheme 'sourcedata' ist --catalog erforderlich (z.B. M3BE).", file=sys.stderr)
        sys.exit(2)

    ensure_driver_ionapi(ionapi_path, jdbc_path)

    try:
        ion_cfg = load_ionapi(ionapi_path)
        jdbc_url = build_jdbc_url(ion_cfg, args.scheme, args.catalog)
        sql = ensure_limit(load_sql(args), args.limit)
        props = build_properties(ion_cfg, args.catalog, args.default_collection)
        result = run_query(jdbc_url, jdbc_path, props, sql)
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": str(exc)}), file=sys.stdout)
        sys.exit(1)

    if args.output == "json":
        print(json.dumps({"jdbc_url": jdbc_url, "result": result}, ensure_ascii=False, indent=2))
    else:
        columns = result["columns"]
        print("\t".join(columns))
        for row in result["rows"]:
            values = [str(row.get(col, "")) for col in columns]
            print("\t".join(values))


if __name__ == "__main__":
    main()
