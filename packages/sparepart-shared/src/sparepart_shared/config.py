"""Runtime configuration helpers for split frontend/backends."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _clean_url(value: str | None) -> str:
    return (value or "").strip().rstrip("/")


@dataclass(frozen=True)
class ApiBaseUrls:
    core_api_base_url: str
    rsrd2_api_base_url: str
    goldenview_api_base_url: str

    @classmethod
    def from_env(cls) -> "ApiBaseUrls":
        return cls(
            core_api_base_url=_clean_url(os.getenv("CORE_API_BASE_URL")),
            rsrd2_api_base_url=_clean_url(os.getenv("RSRD2_API_BASE_URL")),
            goldenview_api_base_url=_clean_url(os.getenv("GOLDENVIEW_API_BASE_URL")),
        )
