#!/usr/bin/env python3
import argparse
import errno
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
import sys

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000
DEFAULT_PORT_TRIES = 20


class AppHandler(SimpleHTTPRequestHandler):
    """Serve the static app files from the repository root."""


class ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Name Flight web app.")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Host/IP to bind to.")
    parser.add_argument(
        "--port", type=int, default=DEFAULT_PORT, help="Preferred start port."
    )
    parser.add_argument(
        "--port-tries",
        type=int,
        default=DEFAULT_PORT_TRIES,
        help="How many consecutive ports to try if the preferred port is busy.",
    )
    return parser.parse_args()


def create_server(
    host: str, preferred_port: int, port_tries: int
) -> tuple[ReusableThreadingHTTPServer, int]:
    for offset in range(max(1, port_tries)):
        candidate_port = preferred_port + offset
        try:
            server = ReusableThreadingHTTPServer((host, candidate_port), AppHandler)
            return server, candidate_port
        except OSError as exc:
            if exc.errno == errno.EADDRINUSE:
                continue
            raise
    raise OSError(
        errno.EADDRINUSE,
        f"No free port found in range {preferred_port}-{preferred_port + port_tries - 1}.",
    )


def main() -> None:
    args = parse_args()
    try:
        server, bound_port = create_server(args.host, args.port, args.port_tries)
    except OSError as exc:
        print(f"Start fehlgeschlagen: {exc}", file=sys.stderr)
        print(
            "Tipp: Starte mit einem anderen Port, z.B. `python3 app.py --port 8080`.",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc

    if bound_port != args.port:
        print(f"Port {args.port} ist belegt, nutze stattdessen Port {bound_port}.", flush=True)
    print(f"WebApp running at http://{args.host}:{bound_port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
