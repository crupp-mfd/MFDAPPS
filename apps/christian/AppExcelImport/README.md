# AppExcelImport

Startprojekt fuer den Import komplexer Excel-Dateien in eine Datenbank.

## Ziel

- Excel-Workbooks (mehrere Sheets, viele Spalten) robust einlesen
- Daten strukturiert in eine DB laden
- Import-Pipeline ueber API ansprechbar machen

## Aktueller Stand

- Projektstruktur angelegt
- FastAPI-Endpunkte fuer Health, Preview und Import vorhanden
- Excel-Reader und SQLite-Writer als austauschbare Services vorhanden
- Import-Jobs laufen aktuell synchron

## Lokal starten

Standardbetrieb ist integriert im Hauptserver auf Port `8000`:

- `http://127.0.0.1:8000/apps/christian/AppExcelImport/`
- API unter `http://127.0.0.1:8000/api/excel-import/...`

Der lokale Einzelbetrieb auf `8020` ist optional (nur fuer isoliertes Debugging):

```bash
./apps/christian/AppExcelImport/dev-server.sh
```

Danach erreichbar unter:

- `http://127.0.0.1:8020/healthz`
- `http://127.0.0.1:8020/api/excel-import/config/health`
- `http://127.0.0.1:8020/api/excel-import/transfer/test`
- `http://127.0.0.1:8020/api/excel-import/workbook/preview`

## GO-Import nach Fabric

In der Seite `apps/christian/AppExcelImport/` gibt es einen Button `GO`.

- Klick auf `GO` ruft `POST /api/excel-import/go/fabric` auf.
- Der Endpoint liest standardmaessig das Sheet `CM` und schreibt zuerst nach `landing.ContractManagement_raw`.
- Danach wird nach `row_type` aufgeteilt in:
  - `landing.ContractManagement_Header`
  - `landing.ContractManagement_Batch`
  - `landing.ContractManagement_Wagon`
- Zusaetzlich wird `landing.ContractManagement_Structure` gefuellt (Row-Reihenfolge + Payload fuer Excel-Rebuild).
- Delta-Logik wird je Datentyp-Tabelle ausgefuehrt:
  - unveraenderte Zeilen: nur `last_seen_import_utc` aktualisieren
  - geaenderte Zeilen: alte Version historisieren (`valid_to_utc`), neue Version aktiv setzen
- Die Datei wird ueber `EXCEL_IMPORT_GO_WORKBOOK_PATH` konfiguriert.
- Technisches Detailkonzept: `apps/christian/AppExcelImport/docs/contract_management_split_concept.md`

## Snapshot-Export nach Excel

- Endpoint: `GET /api/excel-import/export/cm`
- Optionaler Parameter: `snapshot_at_utc` (`YYYY-MM-DD` oder ISO-8601)
- Ohne Parameter wird automatisch der letzte Importzeitpunkt exportiert.
- Ergebnisdatei landet in `EXCEL_IMPORT_GO_EXPORT_DIR`.
- Exportquelle ist `ContractManagement_Structure` (stabile Reihenfolge pro Import-Batch).
- Wenn die Quell-Datei lokal verfuegbar ist, wird deren Formatvorlage fuer den Export wiederverwendet.

Asynchron mit Fortschritt und Download-Link:

- `POST /api/excel-import/export/cm/jobs` (Job starten)
- `GET /api/excel-import/export/cm/jobs/{job_id}` (Status + Fortschritt)
- `GET /api/excel-import/export/cm/jobs/{job_id}/download` (Excel-Datei)

## Beispiel: Workbook preview

```bash
curl -sS -X POST "http://127.0.0.1:8020/api/excel-import/workbook/preview" \
  -H "Content-Type: application/json" \
  -d '{
    "file_path": "apps/christian/AppMehrkilometer/legacy_source/Quellen/KILOMETER.xlsx",
    "header_row": 1,
    "skip_rows": 0
  }'
```

Hinweis fuer `Contract Management.xlsx` (Sheet `CM`):

- `header_row` ist `3` (nicht `1`).
- `#N/A` und `00:00:00` werden automatisch als leer (`NULL`) behandelt.
- Summenzeilen (z. B. `Total`) werden in `CM` automatisch herausgefiltert.
- Die Spalte `Excess Mileage EUR per km` bleibt als Text erhalten und wird zusaetzlich aufgespalten in:
  - `excess_mileage_amount_eur`
  - `excess_mileage_basis_km`

## Beispiel: Import in SQLite

```bash
curl -sS -X POST "http://127.0.0.1:8020/api/excel-import/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "file_path": "apps/christian/AppMehrkilometer/legacy_source/Quellen/KILOMETER.xlsx",
    "header_row": 1,
    "skip_rows": 0,
    "target_table_prefix": "xl_",
    "if_exists": "append",
    "dry_run": false
  }'
```

## Beispiel: SharePoint Preview

```bash
curl -sS -X POST "http://127.0.0.1:8020/api/excel-import/sharepoint/workbook/preview" \
  -H "Content-Type: application/json" \
  -d '{
    "workbook_path": "Shared Documents/Reporting/KILOMETER.xlsx",
    "header_row": 1,
    "skip_rows": 0
  }'
```

Du kannst statt `workbook_path` auch direkt eine komplette SharePoint-URL (Doc.aspx/Share-Link) angeben.

## Beispiel: SharePoint Import

```bash
curl -sS -X POST "http://127.0.0.1:8020/api/excel-import/sharepoint/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "workbook_path": "Shared Documents/Reporting/KILOMETER.xlsx",
    "header_row": 1,
    "skip_rows": 0,
    "target_table_prefix": "sp_",
    "if_exists": "append",
    "dry_run": false
  }'
```

## Beispiel: Daily Default Job (Scheduler)

Wenn die Defaults per ENV gesetzt sind, kann der Scheduler jeden Tag einfach diesen Endpoint aufrufen:

```bash
curl -sS -X POST "http://127.0.0.1:8020/api/excel-import/sharepoint/jobs/default?dry_run=false"
```

## Wichtige Umgebungsvariablen

- `MFDAPPS_HOME` (Default: Repo-Root)
- `MFDAPPS_RUNTIME_ROOT` (Default: `<repo>/apps/christian/data`)
- `EXCEL_IMPORT_SQLITE_PATH` (Default: `<MFDAPPS_RUNTIME_ROOT>/excel_import.db`)
- `EXCEL_IMPORT_INPUT_ROOT` (Default: `MFDAPPS_HOME`)
- `EXCEL_IMPORT_PREVIEW_LIMIT` (Default: `20`)
- `EXCEL_IMPORT_GO_WORKBOOK_PATH` (Default: `apps/christian/AppExcelImport/manual_input/Contract Management.xlsx`)
- `EXCEL_IMPORT_GO_SHEET_NAME` (Default: `CM`)
- `EXCEL_IMPORT_GO_HEADER_ROW` (Default: `3`)
- `EXCEL_IMPORT_GO_SKIP_ROWS` (Default: `0`)
- `EXCEL_IMPORT_GO_TARGET_SCHEMA` (Default: `landing`)
- `EXCEL_IMPORT_GO_TARGET_TABLE` (Default: `ContractManagement`)
- `EXCEL_IMPORT_GO_RAW_TABLE` (Default: `ContractManagement_raw`)
- `EXCEL_IMPORT_GO_EXPORT_DIR` (Default: `<MFDAPPS_RUNTIME_ROOT>/excel_exports`)

SharePoint:

- `SHAREPOINT_SITE_HOSTNAME` (z. B. `company.sharepoint.com`)
- `SHAREPOINT_SITE_PATH` (z. B. `TeamSite`)
- `SHAREPOINT_DEFAULT_WORKBOOK_PATH` (Pfad in der Dokumentbibliothek)
- `SHAREPOINT_DEFAULT_TABLE_PREFIX` (Default: `excel_`)
- `SHAREPOINT_USE_MANAGED_IDENTITY` (`1` oder `0`, Default: `1`)
- `SHAREPOINT_MANAGED_IDENTITY_CLIENT_ID` (optional, falls User Assigned MI)
- `SHAREPOINT_TENANT_ID` / `SHAREPOINT_CLIENT_ID` / `SHAREPOINT_CLIENT_SECRET` (Fallback auf Client Credentials)
- `SHAREPOINT_REQUEST_TIMEOUT` (Sekunden, Default: `60`)

Hinweis:

- Kein manuelles Token noetig. Die App holt Access-Tokens pro Lauf automatisch (Managed Identity oder Client Credentials).
- Wenn `SHAREPOINT_*` nicht gesetzt ist, nutzt die App automatisch `FABRIC_TENANT_ID` / `FABRIC_CLIENT_ID` / `FABRIC_CLIENT_SECRET` als Fallback.

Fabric SQL (fuer GO-Import):

- `FABRIC_SQL_SERVER`
- `FABRIC_SQL_DATABASE`
- `FABRIC_SQL_DRIVER` (Default: `ODBC Driver 18 for SQL Server`)
- `FABRIC_SQL_PORT` (Default: `1433`)
- `FABRIC_SQL_TIMEOUT` (Default: `20`)
- `FABRIC_CLIENT_ID`
- `FABRIC_TENANT_ID`
- `FABRIC_CLIENT_SECRET`

## Deploy (Azure Container Apps)

```bash
./apps/christian/AppExcelImport/deploy.sh
```
