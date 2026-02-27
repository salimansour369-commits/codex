from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(slots=True)
class DataPacket:
    """Mutable payload with immutable metadata boundary."""

    id: str
    payload: dict[str, Any]
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    tags: set[str] = field(default_factory=set)


@dataclass(slots=True)
class StageResult:
    """Result envelope produced by each stage execution."""

    stage_name: str
    ok: bool
    duration_ms: float
    details: dict[str, Any] = field(default_factory=dict)
