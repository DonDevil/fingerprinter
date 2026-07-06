from __future__ import annotations

import json
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
        self.conn.execute(
            """CREATE TABLE IF NOT EXISTS processing_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id INTEGER NOT NULL,
                source_domain TEXT,
                target_title TEXT,
                target_path TEXT,
                candidate_path TEXT,
                final_status TEXT NOT NULL,
                piracy_score REAL NOT NULL,
                created_at TEXT
            )"""
        )
        self.conn.execute(
            """CREATE TABLE IF NOT EXISTS stage_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                asset_id INTEGER NOT NULL,
                stage_name TEXT NOT NULL,
                score REAL NOT NULL,
                decision TEXT,
                note TEXT,
                details_json TEXT,
                created_at TEXT,
                FOREIGN KEY(run_id) REFERENCES processing_runs(id)
            )"""
        )
        self.conn.execute(
            """CREATE TABLE IF NOT EXISTS piracy_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id INTEGER NOT NULL,
                media_url TEXT NOT NULL,
                source_domain TEXT,
                target_title TEXT,
                candidate_path TEXT,
                piracy_score REAL NOT NULL,
                status TEXT NOT NULL,
                evidence_json TEXT,
                created_at TEXT
            )"""
        )
        self.conn.execute(
            """CREATE TABLE IF NOT EXISTS crawler_feedback_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id INTEGER NOT NULL,
                source_domain TEXT,
                feedback_type TEXT NOT NULL,
                feedback_value TEXT,
                created_at TEXT
            )"""
        )
        self.conn.execute(
            """CREATE TABLE IF NOT EXISTS compare_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_key TEXT NOT NULL UNIQUE,
                mode TEXT NOT NULL,
                asset_id INTEGER,
                candidate_ref TEXT NOT NULL,
                target_path TEXT,
                techniques TEXT,
                status TEXT NOT NULL,
                outcome_status TEXT,
                last_error TEXT,
                last_run_id INTEGER,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                started_at TEXT,
                updated_at TEXT,
                completed_at TEXT
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

    def list_non_matching_not_deleted(self) -> list[dict]:
        cur = self.conn.execute(
            """SELECT id, asset_id, local_path, file_size_bytes, created_at
               FROM processed_assets
               WHERE decision IN ('rejected_non_video', 'rejected_too_short', 'no_match_pending_review', 'failed')
                 AND deleted_at IS NULL
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

    def create_processing_run(
        self,
        *,
        asset_id: int,
        source_domain: str | None,
        target_title: str,
        target_path: str,
        candidate_path: str,
        final_status: str,
        piracy_score: float,
    ) -> int:
        now = self._now()
        cur = self.conn.execute(
            """INSERT INTO processing_runs (
                   asset_id, source_domain, target_title, target_path, candidate_path,
                   final_status, piracy_score, created_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                asset_id,
                source_domain,
                target_title,
                target_path,
                candidate_path,
                final_status,
                float(piracy_score),
                now,
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def record_stage_result(
        self,
        *,
        run_id: int,
        asset_id: int,
        stage_name: str,
        score: float,
        decision: str,
        note: str,
        details: dict,
    ) -> None:
        now = self._now()
        self.conn.execute(
            """INSERT INTO stage_results (
                   run_id, asset_id, stage_name, score, decision, note, details_json, created_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                run_id,
                asset_id,
                stage_name,
                float(score),
                decision,
                note,
                json.dumps(details, sort_keys=True),
                now,
            ),
        )
        self.conn.commit()

    def record_piracy_match(
        self,
        *,
        asset_id: int,
        media_url: str,
        source_domain: str | None,
        target_title: str,
        candidate_path: str,
        piracy_score: float,
        status: str,
        evidence: dict,
    ) -> None:
        now = self._now()
        self.conn.execute(
            """INSERT INTO piracy_matches (
                   asset_id, media_url, source_domain, target_title, candidate_path,
                   piracy_score, status, evidence_json, created_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                asset_id,
                media_url,
                source_domain,
                target_title,
                candidate_path,
                float(piracy_score),
                status,
                json.dumps(evidence, sort_keys=True),
                now,
            ),
        )
        self.conn.commit()

    def record_crawler_feedback(
        self,
        *,
        asset_id: int,
        source_domain: str | None,
        feedback_type: str,
        feedback_value: str,
    ) -> None:
        now = self._now()
        self.conn.execute(
            """INSERT INTO crawler_feedback_events (
                   asset_id, source_domain, feedback_type, feedback_value, created_at
               ) VALUES (?, ?, ?, ?, ?)""",
            (asset_id, source_domain, feedback_type, feedback_value, now),
        )
        self.conn.commit()

    def start_compare_task(
        self,
        *,
        task_key: str,
        mode: str,
        candidate_ref: str,
        target_path: str,
        techniques: list[str],
        asset_id: int | None = None,
    ) -> None:
        now = self._now()
        techniques_json = json.dumps(sorted(techniques))
        self.conn.execute(
            """INSERT INTO compare_tasks (
                   task_key, mode, asset_id, candidate_ref, target_path, techniques,
                   status, outcome_status, last_error, last_run_id, attempt_count,
                   started_at, updated_at, completed_at
               ) VALUES (?, ?, ?, ?, ?, ?, 'in_progress', NULL, NULL, NULL, 1, ?, ?, NULL)
               ON CONFLICT(task_key) DO UPDATE SET
                   mode = excluded.mode,
                   asset_id = excluded.asset_id,
                   candidate_ref = excluded.candidate_ref,
                   target_path = excluded.target_path,
                   techniques = excluded.techniques,
                   status = 'in_progress',
                   outcome_status = NULL,
                   last_error = NULL,
                   last_run_id = NULL,
                   attempt_count = compare_tasks.attempt_count + 1,
                   updated_at = excluded.updated_at,
                   completed_at = NULL""",
            (
                task_key,
                mode,
                asset_id,
                candidate_ref,
                target_path,
                techniques_json,
                now,
                now,
            ),
        )
        self.conn.commit()

    def complete_compare_task(self, *, task_key: str, run_id: int | None, outcome_status: str) -> None:
        now = self._now()
        self.conn.execute(
            """UPDATE compare_tasks
               SET status = 'completed',
                   outcome_status = ?,
                   last_run_id = ?,
                   updated_at = ?,
                   completed_at = ?
               WHERE task_key = ?""",
            (outcome_status, run_id, now, now, task_key),
        )
        self.conn.commit()

    def fail_compare_task(self, *, task_key: str, error_message: str) -> None:
        now = self._now()
        self.conn.execute(
            """UPDATE compare_tasks
               SET status = 'failed',
                   last_error = ?,
                   updated_at = ?
               WHERE task_key = ?""",
            (error_message, now, task_key),
        )
        self.conn.commit()

    def is_compare_task_completed(self, *, task_key: str) -> bool:
        row = self.conn.execute(
            "SELECT status FROM compare_tasks WHERE task_key = ?",
            (task_key,),
        ).fetchone()
        return bool(row and str(row[0]) == "completed")

    def compare_task_status_counts(self) -> dict[str, int]:
        rows = self.conn.execute(
            "SELECT status, COUNT(*) AS count FROM compare_tasks GROUP BY status"
        ).fetchall()
        counts: dict[str, int] = {"completed": 0, "in_progress": 0, "failed": 0}
        for row in rows:
            status = str(row["status"])
            counts[status] = int(row["count"])
        return counts

    def mark_assets_deleted_by_local_paths(self, *, local_paths: list[str], note: str) -> int:
        if not local_paths:
            return 0
        now = self._now()
        updated = 0
        for local_path in local_paths:
            cur = self.conn.execute(
                """UPDATE processed_assets
                   SET deleted_at = COALESCE(deleted_at, ?),
                       note = COALESCE(?, note),
                       updated_at = ?
                   WHERE local_path = ?""",
                (now, note, now, local_path),
            )
            updated += int(cur.rowcount or 0)
        self.conn.commit()
        return updated

    def reset_all_tables(self) -> None:
        """Delete all local processing metadata for a clean start."""

        self.conn.execute("DELETE FROM stage_results")
        self.conn.execute("DELETE FROM piracy_matches")
        self.conn.execute("DELETE FROM crawler_feedback_events")
        self.conn.execute("DELETE FROM processing_runs")
        self.conn.execute("DELETE FROM compare_tasks")
        self.conn.execute("DELETE FROM processed_assets")
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()
