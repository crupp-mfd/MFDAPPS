"""Database helpers shared across MFDApps services."""

from __future__ import annotations

import sqlite3
from pathlib import Path


def create_sqlite_connection(path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn
