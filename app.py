#!/usr/bin/env python3
import argparse
import os
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Startet die Willkommen-Webapp.")
    parser.add_argument("--host", default="127.0.0.1", help="Host/IP zum Binden.")
    parser.add_argument("--port", type=int, default=8000, help="Port (z. B. 8000).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    web_dir = Path(__file__).parent / "web"
    os.chdir(web_dir)

    try:
        server = ThreadingHTTPServer((args.host, args.port), SimpleHTTPRequestHandler)
    except OSError as exc:
        print(f"Server konnte nicht starten: {exc}")
        print(
            "Tipp: Nutze einen anderen Port, z. B. "
            f"`python3 app.py --host 127.0.0.1 --port 8080`"
        )
        raise SystemExit(1) from exc

    print(f"Webapp running on http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
