# MFDAPPS

## Team Workflow (Commit / Push / Deploy)

Einheitliche Skripte:

- `/Users/crupp/dev/MFDAPPS/scripts/codex.sh`
- `/Users/crupp/dev/MFDAPPS/scripts/git-commit.sh`
- `/Users/crupp/dev/MFDAPPS/scripts/git-push.sh`
- `/Users/crupp/dev/MFDAPPS/scripts/deploy-main.sh`

Nutzung:

```bash
./scripts/codex.sh commit "feat: meine aenderung"
./scripts/codex.sh push
./scripts/codex.sh deploy
```

Direkt (ohne Wrapper):

```bash
./scripts/git-commit.sh "feat: meine aenderung"
./scripts/git-push.sh
./scripts/deploy-main.sh
```

Regel:

- `deploy-main.sh` deployt nur, wenn du auf `main` bist und der Working Tree sauber ist.
- Das Azure File Volume (`/runtime`) bleibt bei `az containerapp update --image` erhalten.

## CI/CD Auto-Deploy

Workflow:

- `/Users/crupp/dev/MFDAPPS/.github/workflows/deploy-main.yml`
- Trigger: Push auf `main` (und manuell via `workflow_dispatch`)
- `/Users/crupp/dev/MFDAPPS/.github/workflows/dev-pr-automerge.yml`
- `/Users/crupp/dev/MFDAPPS/.github/workflows/pr-checks.yml`

## Auto-Merge dev/* -> main

Ziel:

- Push auf `dev/ali`, `dev/timo`, `dev/christian` erstellt/aktualisiert automatisch eine PR nach `main`.
- Auto-Merge wird aktiviert; nach gruener PR-Checks wird gemerged.
- Nach Merge auf `main` startet automatisch Deploy nach `mfd-automation`.
- Bei direktem Push auf `main` startet der Deploy ebenfalls automatisch.
- Required status check fuer Merge nach `main`: `validate`.

Einmalige GitHub-Einstellungen (UI):

1. `Settings -> General -> Pull Requests`:
   - `Allow auto-merge` aktivieren.
2. `Settings -> Branches -> Branch protection rules -> main`:
   - `Require a pull request before merging` aktivieren.
   - `Require status checks to pass before merging` aktivieren.
   - Required check: `PR Checks / validate`.

Notwendige GitHub Secrets:

1. `AZURE_CREDENTIALS` (Service Principal JSON fuer `azure/login`)

Minimal empfohlene Azure Rollen fuer diesen Service Principal:

- Resource Group `rg-mfd-automation`: `Contributor`
- ACR `acrmfdauto10028`: `AcrPush`

Optional Team-Rechte (Ali/Timo/Christian lokal deployen):

- auf `rg-mfd-automation`: `Contributor` oder `Container App Contributor`
- auf `acrmfdauto10028`: `AcrPush`

## Name Flight WebApp

Lokaler Start:

```bash
python3 app.py
```

Dann im Browser aufrufen (Standard):

- http://127.0.0.1:8000
- http://127.0.0.1:8000/apps/christian/
- http://127.0.0.1:8000/apps/ali/
- http://127.0.0.1:8000/apps/timo/

Wenn `8000` belegt ist, wählt die App automatisch `8001`, `8002`, ...  
Nimm dann die URL aus der Terminal-Ausgabe.

Optional explizit mit anderem Port starten:

```bash
python3 app.py --port 8080
```

Wenn die Seite nicht lädt:

```bash
lsof -iTCP:8000 -sTCP:LISTEN
python3 app.py --port 8080
```

Benutzung:

1. Namen eingeben
2. `Start` klicken
3. Der Name fliegt als animierter Text durch den Browser

Portal:

- `/` zeigt die Uebersicht auf alle drei Seiten.

## Robust Start/Restart Script

Script:

- `/Users/crupp/dev/MFDAPPS/scripts/server.sh`

Beispiele:

```bash
./scripts/server.sh start
./scripts/server.sh stop
./scripts/server.sh restart
./scripts/server.sh status
./scripts/server.sh logs
```

Standard ohne Argument ist `restart`, also:

```bash
./scripts/server.sh
```
