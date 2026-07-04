from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class QueueJob:
    asset_id: int
    media_url: str
    media_type: str
    priority: int
