from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from work_queue.models import QueueJob


class CrawlerMediaQueue:
    """Consume sample jobs directly from crawler media evidence database."""

    def __init__(self, db_path: str, worker_name: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.worker_name = worker_name
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self._sample_jobs_columns = self._load_table_columns("sample_jobs")
        self._media_assets_columns = self._load_table_columns("media_assets")

    def _load_table_columns(self, table_name: str) -> set[str]:
        rows = self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {str(row[1]) for row in rows}

    def _has_sample_jobs_column(self, column_name: str) -> bool:
        return column_name in self._sample_jobs_columns

    def _has_media_assets_column(self, column_name: str) -> bool:
        return column_name in self._media_assets_columns

    def _update_media_asset(
        self,
        *,
        asset_id: int,
        status: str,
        now: str,
        match_confidence: float | None = None,
        matched_title: str | None = None,
    ) -> None:
        assignments: list[str] = []
        params: list[object] = []

        if self._has_media_assets_column("status"):
            assignments.append("status = ?")
            params.append(status)
        if self._has_media_assets_column("last_seen"):
            assignments.append("last_seen = ?")
            params.append(now)
        if self._has_media_assets_column("match_confidence") and match_confidence is not None:
            assignments.append("match_confidence = ?")
            params.append(match_confidence)
        if self._has_media_assets_column("matched_title") and matched_title is not None:
            assignments.append("matched_title = ?")
            params.append(matched_title)

        if not assignments:
            return

        params.append(asset_id)
        sql = f"UPDATE media_assets SET {', '.join(assignments)} WHERE id = ?"
        self.conn.execute(sql, tuple(params))

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _parse_timestamp(value: str | None) -> datetime | None:
        if not value:
            return None
        raw = value.strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def pending_count(self) -> int:
        cur = self.conn.execute("SELECT COUNT(*) FROM sample_jobs WHERE status = 'pending'")
        row = cur.fetchone()
        return int(row[0]) if row else 0

    def requeue_stale_claimed_jobs(self, *, stale_after_seconds: int) -> int:
        """Move stale claimed jobs back to pending so they can be retried."""

        threshold_seconds = max(1, int(stale_after_seconds))
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=threshold_seconds)

        rows = self.conn.execute(
            "SELECT asset_id, updated_at FROM sample_jobs WHERE status = 'claimed'"
        ).fetchall()
        if not rows:
            return 0

        reclaimed_asset_ids: list[int] = []
        for row in rows:
            updated_at = self._parse_timestamp(row["updated_at"])
            if updated_at is None or updated_at <= cutoff:
                reclaimed_asset_ids.append(int(row["asset_id"]))

        if not reclaimed_asset_ids:
            return 0

        now = self._now()
        for asset_id in reclaimed_asset_ids:
            if self._has_sample_jobs_column("claimed_by"):
                self.conn.execute(
                    """UPDATE sample_jobs
                       SET status = 'pending', claimed_by = NULL, updated_at = ?
                       WHERE asset_id = ?""",
                    (now, asset_id),
                )
            else:
                self.conn.execute(
                    "UPDATE sample_jobs SET status = 'pending', updated_at = ? WHERE asset_id = ?",
                    (now, asset_id),
                )
            self._update_media_asset(
                asset_id=asset_id,
                status="queued_for_sampling",
                now=now,
            )

        self.conn.commit()
        return len(reclaimed_asset_ids)

    def claim_job(self) -> Optional[QueueJob]:
        now = self._now()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            source_domain_select = "ma.source_domain" if self._has_media_assets_column("source_domain") else "NULL as source_domain"
            row = self.conn.execute(
                f"""SELECT sj.asset_id, sj.priority, ma.url, ma.media_type, {source_domain_select}
                   FROM sample_jobs sj
                   JOIN media_assets ma ON ma.id = sj.asset_id
                   WHERE sj.status = 'pending'
                   ORDER BY sj.priority ASC, sj.updated_at ASC, sj.created_at ASC
                   LIMIT 1"""
            ).fetchone()

            if row is None:
                self.conn.execute("COMMIT")
                return None

            asset_id = int(row["asset_id"])
            if self._has_sample_jobs_column("claimed_by"):
                self.conn.execute(
                    "UPDATE sample_jobs SET status = 'claimed', claimed_by = ?, updated_at = ? WHERE asset_id = ?",
                    (self.worker_name, now, asset_id),
                )
            else:
                self.conn.execute(
                    "UPDATE sample_jobs SET status = 'claimed', updated_at = ? WHERE asset_id = ?",
                    (now, asset_id),
                )
            self._update_media_asset(
                asset_id=asset_id,
                status="claimed",
                now=now,
            )
            self.conn.execute("COMMIT")

            return QueueJob(
                asset_id=asset_id,
                media_url=str(row["url"]),
                media_type=str(row["media_type"] or "unknown"),
                priority=int(row["priority"]),
                source_domain=str(row["source_domain"]).strip().lower() if row["source_domain"] else None,
            )
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def update_job_status(
        self,
        asset_id: int,
        status: str,
        *,
        last_error: str | None = None,
        match_confidence: float | None = None,
        matched_title: str | None = None,
    ) -> None:
        now = self._now()
        self.conn.execute(
            "UPDATE sample_jobs SET status = ?, last_error = ?, updated_at = ? WHERE asset_id = ?",
            (status, last_error, now, asset_id),
        )
        self._update_media_asset(
            asset_id=asset_id,
            status=status,
            now=now,
            match_confidence=match_confidence,
            matched_title=matched_title,
        )
        self.conn.commit()

    def mark_failed_or_retry(
        self,
        asset_id: int,
        *,
        error_message: str,
        max_retry_count: int,
    ) -> str:
        """Increment retry_count and requeue when below limit, else mark failed."""

        if max_retry_count < 0:
            max_retry_count = 0

        now = self._now()

        if not self._has_sample_jobs_column("retry_count"):
            self.conn.execute(
                "UPDATE sample_jobs SET status = 'failed', last_error = ?, updated_at = ? WHERE asset_id = ?",
                (error_message, now, asset_id),
            )
            self._update_media_asset(
                asset_id=asset_id,
                status="failed",
                now=now,
            )
            self.conn.commit()
            return "failed"

        current_row = self.conn.execute(
            "SELECT retry_count FROM sample_jobs WHERE asset_id = ?",
            (asset_id,),
        ).fetchone()
        current_retry_count = int(current_row[0]) if current_row and current_row[0] is not None else 0
        next_retry_count = current_retry_count + 1

        if next_retry_count <= max_retry_count:
            self.conn.execute(
                """UPDATE sample_jobs
                   SET status = 'pending', retry_count = ?, last_error = ?, updated_at = ?
                   WHERE asset_id = ?""",
                (next_retry_count, error_message, now, asset_id),
            )
            self._update_media_asset(
                asset_id=asset_id,
                status="queued_for_sampling",
                now=now,
            )
            self.conn.commit()
            return "pending"

        self.conn.execute(
            """UPDATE sample_jobs
               SET status = 'failed', retry_count = ?, last_error = ?, updated_at = ?
               WHERE asset_id = ?""",
            (next_retry_count, error_message, now, asset_id),
        )
        self._update_media_asset(
            asset_id=asset_id,
            status="failed",
            now=now,
        )
        self.conn.commit()
        return "failed"

    def mark_match(self, asset_id: int, *, matched_title: str, confidence: float) -> None:
        self.update_job_status(
            asset_id,
            "matched",
            match_confidence=confidence,
            matched_title=matched_title,
        )

    def boost_domain_priority(self, source_domain: str, *, boosted_priority: int = 1) -> int:
        """Raise crawler attention for domains that produced pirated matches.

        Returns number of pending sample jobs updated.
        """

        domain = (source_domain or "").strip().lower()
        if not domain:
            return 0

        priority_value = max(1, int(boosted_priority))
        now = self._now()
        cur = self.conn.execute(
            """UPDATE sample_jobs
               SET priority = ?, updated_at = ?
               WHERE status = 'pending'
                 AND asset_id IN (
                   SELECT id FROM media_assets WHERE lower(coalesce(source_domain, '')) = ?
                 )
                 AND priority > ?""",
            (priority_value, now, domain, priority_value),
        )
        self.conn.commit()
        return int(cur.rowcount or 0)

    def close(self) -> None:
        self.conn.close()
