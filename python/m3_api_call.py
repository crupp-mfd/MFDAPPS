"""CLI zum Aufruf von Infor M3 MI-Transaktionen.

Das Skript lädt Service-Account-Credentials aus einer `.ionapi` Datei
(standardmäßig aus `credentials/ionapi/`) und gibt die API-Antwort als JSON
auf stdout aus, damit andere Prozesse (z.B. Node.js) das Ergebnis direkt
weiterverarbeiten können.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, Optional

import requests

try:  # pragma: no cover - script vs package execution
    from .env_loader import get_credentials_root, load_project_dotenv
except ImportError:  # type: ignore
    from env_loader import get_credentials_root, load_project_dotenv  # type: ignore

load_project_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_IONAPI_DIR = get_credentials_root() / "ionapi"
DEFAULT_IONAPI_PATH = DEFAULT_IONAPI_DIR / "service_account.ionapi"
PREFERRED_IONAPI_FILES = [
    "Infor Compass JDBC Driver.ionapi",
    "MFD_Backend_Python_vNEW.ionapi",
    "MFD_Backend_Python.ionapi",
    "service_account.ionapi",
]

EXAMPLE_PARAMS = {
    "MTRL": "EXAMPLE_WAGON",
    "SERN": "00 00 0000 000-0",
    "EXPA": "1",
    "MEVA": "1",
}
READ_ONLY_M3 = os.getenv("SPAREPART_DISABLE_M3_CALLS", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
}
READ_CALLS = {
    ("MOS256MI", "LstAsBuild"),
    ("CMS100MI", "Lst_PLPN_MWNO"),
}


def _log(message: str, verbose: bool = False) -> None:
    if verbose:
        print(message, file=sys.stderr)


def _ensure_m3_calls_allowed(program: str, transaction: str) -> None:
    if READ_ONLY_M3 and (program, transaction) not in READ_CALLS:
        allowed = ", ".join(f"{prog}/{tx}" for prog, tx in sorted(READ_CALLS))
        raise RuntimeError(
            "M3-Write-Calls sind deaktiviert. "
            f"Erlaubte Read-Calls: {allowed}"
        )


def find_ionapi_path(explicit_path: Optional[str] = None) -> str:
    env_path = os.getenv("IONAPI_PATH")

    def _validate(path: Path) -> str:
        if path.exists():
            return str(path)
        raise FileNotFoundError(f".ionapi Datei nicht gefunden: {path}")

    if explicit_path:
        return _validate(Path(explicit_path))
    if env_path:
        return _validate(Path(env_path))

    if DEFAULT_IONAPI_PATH.exists():
        return str(DEFAULT_IONAPI_PATH)

    candidates = list(DEFAULT_IONAPI_DIR.glob("*.ionapi")) if DEFAULT_IONAPI_DIR.exists() else []
    if len(candidates) == 1:
        return str(candidates[0])
    if len(candidates) > 1:
        preferred_map = {c.name: c for c in candidates}
        for preferred in PREFERRED_IONAPI_FILES:
            if preferred in preferred_map:
                return str(preferred_map[preferred])
        chosen = sorted(candidates)[0]
        print(
            "Hinweis: Mehrere .ionapi Dateien gefunden, "
            f"{chosen.name} wird automatisch verwendet. "
            "Setze IONAPI_PATH, um eine andere Datei zu wählen.",
            file=sys.stderr,
        )
        return str(chosen)

    raise FileNotFoundError(
        "Keine .ionapi Datei gefunden. Lege eine Datei unter "
        f"{DEFAULT_IONAPI_DIR} an oder setze IONAPI_PATH."
    )


def load_ionapi_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def get_access_token_service_account(ion_cfg: dict) -> str:
    token_url = ion_cfg["pu"] + ion_cfg["ot"]
    client_id = ion_cfg["ci"]
    client_secret = ion_cfg["cs"]
    username = ion_cfg["saak"]
    password = ion_cfg["sask"]

    data = {
        "grant_type": "password",
        "username": username,
        "password": password,
    }

    resp = requests.post(token_url, data=data, auth=(client_id, client_secret), timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]


def build_base_url(ion_cfg: dict) -> str:
    base = ion_cfg.get("iu", "").rstrip("/")
    tenant = ion_cfg.get("ti")
    if not base or not tenant:
        raise ValueError("Ionapi Datei enthält keine 'iu' oder 'ti' Einträge.")
    return f"{base}/{tenant}"


def call_m3_mi_get(base_url: str, access_token: str, program: str, transaction: str, params: Optional[Dict] = None) -> dict:
    _ensure_m3_calls_allowed(program, transaction)
    url = f"{base_url}/M3/m3api-rest/execute/{program}/{transaction}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    resp = requests.get(url, headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    try:
        return resp.json()
    except ValueError:
        return {"status_code": resp.status_code, "text": resp.text}


def _load_params(args: argparse.Namespace) -> Optional[Dict[str, str]]:
    payload: Dict[str, str] = {}
    if args.params_json:
        payload.update(json.loads(args.params_json))
    if args.params_file:
        with open(args.params_file, "r", encoding="utf-8") as handle:
            payload.update(json.load(handle))
    if args.use_example:
        payload.update(EXAMPLE_PARAMS)
    return payload or None


def main() -> None:
    parser = argparse.ArgumentParser(description="Infor M3 MI Caller")
    parser.add_argument("--ionapi", help="Pfad zur .ionapi Datei")
    parser.add_argument("--program", required=True, help="MI-Programm, z.B. MOS256MI")
    parser.add_argument("--transaction", required=True, help="MI-Transaktion, z.B. LstAsBuild")
    parser.add_argument("--params-json", help="JSON-String mit Parametern")
    parser.add_argument("--params-file", help="Pfad zu einer JSON-Datei mit Parametern")
    parser.add_argument("--use-example", action="store_true", help="Beispielparameter hinzufügen")
    parser.add_argument("--verbose", action="store_true", help="Zusätzliche Logs auf stderr")
    args = parser.parse_args()

    try:
        ionapi_path = find_ionapi_path(args.ionapi)
    except FileNotFoundError as err:
        print(str(err), file=sys.stderr)
        sys.exit(2)

    _log(f"Verwendete .ionapi: {ionapi_path}", args.verbose)

    try:
        ion_cfg = load_ionapi_config(ionapi_path)
        _ensure_m3_calls_allowed(args.program, args.transaction)
        token = get_access_token_service_account(ion_cfg)
        base_url = build_base_url(ion_cfg)
        _log("Access Token erfolgreich erhalten", args.verbose)
        params = _load_params(args)
        result = call_m3_mi_get(base_url, token, args.program, args.transaction, params)
    except Exception as exc:  # noqa: BLE001
        error_payload = {"error": str(exc)}
        print(json.dumps(error_payload, ensure_ascii=False), file=sys.stdout)
        if args.verbose:
            raise
        sys.exit(1)

    output = {
        "program": args.program,
        "transaction": args.transaction,
        "parameters": params or {},
        "response": result,
    }
    print(json.dumps(output, ensure_ascii=False))


if __name__ == "__main__":
    main()
