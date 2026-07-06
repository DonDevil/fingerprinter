from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from urllib.parse import quote


def to_file_url(path: Path) -> str:
    return f"file://{quote(str(path.resolve()))}"


def enqueue_local_videos(db_path: Path, folder: Path, limit: int | None) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS media_assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL UNIQUE,
                media_type TEXT NOT NULL DEFAULT 'unknown',
                source_domain TEXT,
                mime_type TEXT,
                status TEXT NOT NULL DEFAULT 'queued_for_sampling',
                first_seen TEXT,
                last_seen TEXT,
                last_source_page TEXT,
                last_referrer_url TEXT,
                last_discovered_by TEXT,
                last_discovery_method TEXT
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS sample_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id INTEGER NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'pending',
                priority INTEGER NOT NULL DEFAULT 10,
                retry_count INTEGER NOT NULL DEFAULT 0,
                byte_range_strategy TEXT NOT NULL DEFAULT 'head-window',
                last_error TEXT,
                created_at TEXT,
                updated_at TEXT
            )"""
        )

        videos = sorted(
            [p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in {".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v"}]
        )
        if limit is not None:
            videos = videos[: max(0, limit)]

        inserted = 0
        for video in videos:
            url = to_file_url(video)
            conn.execute(
                """INSERT OR IGNORE INTO media_assets (
                       url, media_type, source_domain, status, last_discovered_by, last_discovery_method
                   ) VALUES (?, 'video', ?, 'queued_for_sampling', 'fingerprinter-admin', 'local-test')""",
                (url, "local.storage"),
            )
            row = conn.execute("SELECT id FROM media_assets WHERE url = ?", (url,)).fetchone()
            if not row:
                continue
            asset_id = int(row[0])
            conn.execute(
                """INSERT OR IGNORE INTO sample_jobs (asset_id, status, priority)
                   VALUES (?, 'pending', 10)""",
                (asset_id,),
            )
            inserted += 1

        conn.commit()
        return inserted
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Enqueue local downloaded videos into crawler sample_jobs")
    parser.add_argument("--db", required=True, help="Path to crawler media_evidence.db")
    parser.add_argument("--folder", required=True, help="Folder containing local candidate videos")
    parser.add_argument("--limit", type=int, help="Max number of files to enqueue")
    args = parser.parse_args()

    db_path = Path(args.db)
    folder = Path(args.folder)
    count = enqueue_local_videos(db_path=db_path, folder=folder, limit=args.limit)
    print(f"enqueued={count}")


if __name__ == "__main__":
    main()
