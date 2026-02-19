#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def main() -> int:
    root = _repo_root()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    import app as mfd_app  # noqa: PLC0415

    parser = argparse.ArgumentParser(description="Run a read query against Fabric SQL.")
    parser.add_argument("--sql", required=True, help="SQL query to execute.")
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum rows to print (default: 200).",
    )
    args = parser.parse_args()

    limit = max(1, min(5000, args.limit))

    with mfd_app._connect_fabric_sql() as conn:
        cur = conn.cursor()
        cur.execute(args.sql)
        rows = cur.fetchmany(limit)

    for row in rows:
        print("\t".join("" if col is None else str(col) for col in row))

    print(f"rows={len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
