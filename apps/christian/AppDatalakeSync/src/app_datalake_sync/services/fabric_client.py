from __future__ import annotations

from app_datalake_sync.config import DatalakeSyncSettings


class FabricSqlClient:
    """Placeholder client for future Microsoft Fabric SQL access."""

    def __init__(self, settings: DatalakeSyncSettings) -> None:
        self.settings = settings

    def is_configured(self) -> bool:
        return self.settings.fabric_ready()
