# AppDatalakeSync

Startprojekt fuer die Synchronisation von Datenbanktabellen zwischen Infor Data Lake und Microsoft Fabric SQL.

## Ziel

- Tabellensynchronisation als API-Service bereitstellen
- Spaeter Full- und Incremental-Sync unterstuetzen
- Sync-Jobs nachvollziehbar starten und ueberwachen

## Aktueller Stand

- Projektstruktur angelegt
- FastAPI-App mit Health-Endpoint vorhanden
- UI mit Kopfbereich (Home + ERP LIVE/TST Switch) vorhanden
- Tabelleninventar aus Infor Data Lake wird geladen
- Datensatzanzahl pro Tabelle wird im Hintergrund ermittelt und laufend aktualisiert
- API-Stubs fuer Sync-Plan und Sync-Start vorhanden

## Lokal starten

```bash
./apps/christian/AppDatalakeSync/dev-server.sh
```

Danach erreichbar unter:

- `http://127.0.0.1:8010/apps/christian/AppDatalakeSync/`
- `http://127.0.0.1:8010/healthz`
- `http://127.0.0.1:8010/api/datalake-sync/datalake/tables?env=live`

## Umgebungsvariablen (geplant)

Infor:

- `INFOR_DATALAKE_BASE_URL`
- `INFOR_DATALAKE_TENANT`
- `INFOR_DATALAKE_CLIENT_ID`
- `INFOR_DATALAKE_CLIENT_SECRET`

Fabric SQL:

- `FABRIC_SQL_SERVER`
- `FABRIC_SQL_DATABASE`
- `FABRIC_SQL_DRIVER`
- `FABRIC_SQL_PORT`
- `FABRIC_SQL_TIMEOUT`
- `FABRIC_CLIENT_ID`
- `FABRIC_TENANT_ID`
- `FABRIC_CLIENT_SECRET`

Hinweis zu Credentials:

- Standardpfad: `<repo>/credentials`
- LIVE: `credentials/ionapi/Infor Compass JDBC Driver.ionapi`
- TST: `credentials/TSTEnv/Infor Compass JDBC Driver.ionapi` (Fallback: `credentials/ionapi/Infor Compass JDBC Driver_TST.ionapi`)
- JDBC: `credentials/jdbc/infor-compass-jdbc-*.jar`

## Deploy (Azure Container Apps)

```bash
./apps/christian/AppDatalakeSync/deploy.sh
```
