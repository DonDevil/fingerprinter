from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path


class ProcessingMetadataStore:
    """Persist local processing metadata and file lifecycle state."""

    def __init__(self, db_path: str):
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path))
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self) -> None:
        self.conn.execute(
            """CREATE TABLE IF NOT EXISTS processed_assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id INTEGER NOT NULL,
                media_url TEXT NOT NULL,
                local_path TEXT,
                file_size_bytes INTEGER,
                duration_seconds REAL,
                decision TEXT NOT NULL,
                note TEXT,
                created_at TEXT,
                updated_at TEXT,
                deleted_at TEXT,
                UNIQUE(asset_id, local_path)
            )"""
        )
        self.conn.commit()

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def record_asset(
        self,
        *,
        asset_id: int,
        media_url: str,
        local_path: str,
        file_size_bytes: int | None,
        duration_seconds: float | None,
        decision: str,
        note: str | None = None,
    ) -> None:
        now = self._now()
        self.conn.execute(
            """INSERT INTO processed_assets (
                   asset_id, media_url, local_path, file_size_bytes, duration_seconds,
                   decision, note, created_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(asset_id, local_path) DO UPDATE SET
                   file_size_bytes = excluded.file_size_bytes,
                   duration_seconds = excluded.duration_seconds,
                   decision = excluded.decision,
                   note = excluded.note,
                   updated_at = excluded.updated_at""",
            (
                asset_id,
                media_url,
                local_path,
                file_size_bytes,
                duration_seconds,
                decision,
                note,
                now,
                now,
            ),
        )
        self.conn.commit()

    def list_rejected_not_deleted(self) -> list[dict]:
        cur = self.conn.execute(
            """SELECT id, asset_id, local_path, file_size_bytes, created_at
               FROM processed_assets
               WHERE decision LIKE 'rejected_%' AND deleted_at IS NULL
               ORDER BY created_at ASC, id ASC"""
        )
        return [dict(row) for row in cur.fetchall()]

    def mark_deleted(self, record_id: int, note: str | None = None) -> None:
        now = self._now()
        self.conn.execute(
            "UPDATE processed_assets SET deleted_at = ?, note = COALESCE(?, note), updated_at = ? WHERE id = ?",
            (now, note, now, record_id),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()
