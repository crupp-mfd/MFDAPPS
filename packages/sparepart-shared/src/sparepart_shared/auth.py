"""Authentication helpers shared across MFDApps services."""

from __future__ import annotations

import base64


def is_basic_auth_valid(auth_header: str, expected_user: str, expected_pass: str) -> bool:
    if not expected_user or not expected_pass:
        return False
    if not auth_header.startswith("Basic "):
        return False

    encoded = auth_header.split(" ", 1)[1].strip()
    try:
        decoded = base64.b64decode(encoded).decode("utf-8")
    except Exception:
        return False

    if ":" not in decoded:
        return False

    user, password = decoded.split(":", 1)
    return user == expected_user and password == expected_pass
