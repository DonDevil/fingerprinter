import sqlite3
from pathlib import Path
from typing import Optional, List, Dict

class JobQueue:
    """Queue for video fingerprinting jobs, backed by SQLite."""
    def __init__(self, db_path: str = "storage/jobs.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self._create_table()

    def _create_table(self):
        self.conn.execute(
            """CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                priority INTEGER DEFAULT 10,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        )
        self.conn.commit()

    def add_job(self, url: str, priority: int = 10):
        self.conn.execute(
            "INSERT INTO jobs (url, priority) VALUES (?, ?)", (url, priority)
        )
        self.conn.commit()

    def claim_job(self) -> Optional[Dict]:
        cur = self.conn.execute(
            "SELECT id, url FROM jobs WHERE status = 'pending' ORDER BY priority ASC, created_at ASC LIMIT 1"
        )
        row = cur.fetchone()
        if row:
            job_id, url = row
            self.conn.execute(
                "UPDATE jobs SET status = 'claimed', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (job_id,)
            )
            self.conn.commit()
            return {"id": job_id, "url": url}
        return None

    def update_job_status(self, job_id: int, status: str):
        self.conn.execute(
            "UPDATE jobs SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (status, job_id)
        )
        self.conn.commit()

    def pending_count(self) -> int:
        cur = self.conn.execute("SELECT COUNT(*) FROM jobs WHERE status = 'pending'")
        return cur.fetchone()[0]

    def close(self):
        self.conn.close()
