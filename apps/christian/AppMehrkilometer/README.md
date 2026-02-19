# AppMehrkilometer

Eigenstaendige MFDApps-App im Monorepo.

## Lokal starten

```bash
./apps/christian/AppMehrkilometer/dev-server.sh --port 8000
```

Dann im Browser:

- http://127.0.0.1:8000/apps/christian/AppMehrkilometer/

## Fabric SQL Setup (Service Principal)

1. Python-Abhaengigkeiten lokal installieren:

```bash
python3 -m venv /Users/crupp/dev/MFDAPPS/.venv
/Users/crupp/dev/MFDAPPS/.venv/bin/pip install pyodbc python-dotenv
```

2. Datei `/Users/crupp/dev/MFDAPPS/.ENV` anlegen:

```env
FABRIC_CLIENT_ID=...
FABRIC_TENANT_ID=...
FABRIC_CLIENT_SECRET=...
FABRIC_SQL_SERVER=...database.fabric.microsoft.com
FABRIC_SQL_DATABASE=...
FABRIC_SQL_DRIVER=ODBC Driver 18 for SQL Server
FABRIC_SQL_PORT=1433
FABRIC_SQL_TIMEOUT=20
```

3. Verbindung pruefen:

```bash
curl -sS "http://127.0.0.1:8000/api/mehrkilometer/fabric/health"
```

4. Fabric-Daten fuer ein Jahr importieren (schreibt nach SQLite):

```bash
curl -sS -X POST "http://127.0.0.1:8000/api/mehrkilometer/fabric/import?year=2025"
```

5. Datenstand aus der SQLite-DB pruefen:

```bash
curl -sS "http://127.0.0.1:8000/api/mehrkilometer/fabric/status?year=2025"
```

## Deploy (Azure Container Apps)

```bash
./apps/christian/AppMehrkilometer/deploy.sh
```

Optionale Env-Variablen:

- `APP_NAME`
- `RESOURCE_GROUP`
- `ACR_NAME`
- `IMAGE_REPO`
- `TAG`
