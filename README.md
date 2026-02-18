# MFDAPPS

## Name Flight WebApp

Lokaler Start:

```bash
python3 app.py
```

Dann im Browser aufrufen (Standard):

- http://127.0.0.1:8000

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
