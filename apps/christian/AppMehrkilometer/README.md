# AppMehrkilometer

Eigenstaendige MFDApps-App im Monorepo.

## Lokal starten

```bash
./apps/christian/AppMehrkilometer/dev-server.sh --port 8000
```

Dann im Browser:

- http://127.0.0.1:8000/apps/christian/AppMehrkilometer/

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
