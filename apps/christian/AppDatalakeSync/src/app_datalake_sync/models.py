from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field


class SyncRequest(BaseModel):
    source_table: str = Field(..., description="Infor Data Lake source table")
    target_schema: str = Field(default="dbo", description="Fabric SQL target schema")
    target_table: str | None = Field(default=None, description="Fabric SQL target table (default: source_table)")
    mode: Literal["full", "incremental"] = Field(default="incremental")
    primary_key: str | None = Field(default=None)
    watermark_column: str | None = Field(default=None)
    batch_size: int = Field(default=10_000, ge=1)
    dry_run: bool = Field(default=True)


class SyncJobResponse(BaseModel):
    job_id: str
    status: str
    message: str
    submitted_at_utc: str

    @classmethod
    def accepted(cls, message: str) -> "SyncJobResponse":
        return cls(
            job_id=f"job-{uuid4()}",
            status="accepted",
            message=message,
            submitted_at_utc=datetime.now(timezone.utc).isoformat(),
        )
