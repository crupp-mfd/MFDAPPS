# AppTeilenummer

Eigenstaendige MFDApps-App im Monorepo.

## Lokal starten

```bash
./apps/christian/AppTeilenummer/dev-server.sh --port 8000
```

Voraussetzungen (lokal):

- Credentials nur unter `/Users/crupp/dev/MFDAPPS/credentials`
- Runtime nur unter `/Users/crupp/dev/MFDAPPS/data`
- Mindestens vorhanden: `/Users/crupp/dev/MFDAPPS/credentials/ionapi`

## Deploy (Azure Container Apps)

```bash
./apps/christian/AppTeilenummer/deploy.sh
```

Optionale Env-Variablen:

- `APP_NAME`
- `RESOURCE_GROUP`
- `ACR_NAME`
- `IMAGE_REPO`
- `TAG`
