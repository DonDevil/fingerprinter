import sqlite3

from old.work_queue.crawler_media_queue import CrawlerMediaQueue


def _create_media_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE media_assets (
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
            last_discovery_method TEXT,
            match_confidence REAL,
            matched_title TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE sample_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'pending',
            priority INTEGER NOT NULL DEFAULT 10,
            retry_count INTEGER NOT NULL DEFAULT 0,
            byte_range_strategy TEXT NOT NULL DEFAULT 'head-window',
            claimed_by TEXT,
            last_error TEXT,
            created_at TEXT,
            updated_at TEXT
        )"""
    )
    conn.commit()


def _create_legacy_media_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE media_assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL UNIQUE,
            media_type TEXT NOT NULL DEFAULT 'unknown',
            status TEXT NOT NULL DEFAULT 'queued_for_sampling',
            last_seen TEXT,
            match_confidence REAL,
            matched_title TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE sample_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'pending',
            priority INTEGER NOT NULL DEFAULT 10,
            last_error TEXT,
            created_at TEXT,
            updated_at TEXT
        )"""
    )
    conn.commit()


def test_crawler_media_queue_claims_pending_job(tmp_path):
    db_path = tmp_path / "media_evidence.db"
    conn = sqlite3.connect(str(db_path))
    _create_media_schema(conn)

    conn.execute(
        "INSERT INTO media_assets (id, url, media_type, status) VALUES (1, ?, 'video', 'queued_for_sampling')",
        ("https://cdn.example/movie.mp4",),
    )
    conn.execute(
        """INSERT INTO sample_jobs (asset_id, status, priority, created_at, updated_at)
           VALUES (1, 'pending', 4, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')"""
    )
    conn.commit()
    conn.close()

    queue = CrawlerMediaQueue(str(db_path), worker_name="worker-1")
    try:
        job = queue.claim_job()
        assert job is not None
        assert job.asset_id == 1
        assert job.media_url == "https://cdn.example/movie.mp4"

        pending_after = queue.pending_count()
        assert pending_after == 0
    finally:
        queue.close()


def test_crawler_media_queue_updates_status(tmp_path):
    db_path = tmp_path / "media_evidence.db"
    conn = sqlite3.connect(str(db_path))
    _create_media_schema(conn)

    conn.execute(
        "INSERT INTO media_assets (id, url, media_type, status) VALUES (2, ?, 'video', 'queued_for_sampling')",
        ("https://cdn.example/clip.mp4",),
    )
    conn.execute(
        """INSERT INTO sample_jobs (asset_id, status, priority, created_at, updated_at)
           VALUES (2, 'pending', 8, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')"""
    )
    conn.commit()
    conn.close()

    queue = CrawlerMediaQueue(str(db_path), worker_name="worker-2")
    try:
        queue.update_job_status(2, "rejected_too_short", last_error="below threshold")

        conn = sqlite3.connect(str(db_path))
        sample_status = conn.execute("SELECT status, last_error FROM sample_jobs WHERE asset_id = 2").fetchone()
        asset_status = conn.execute("SELECT status FROM media_assets WHERE id = 2").fetchone()
        conn.close()

        assert sample_status == ("rejected_too_short", "below threshold")
        assert asset_status == ("rejected_too_short",)
    finally:
        queue.close()


def test_crawler_media_queue_claims_with_legacy_schema_without_claimed_by(tmp_path):
    db_path = tmp_path / "media_evidence_legacy.db"
    conn = sqlite3.connect(str(db_path))
    _create_legacy_media_schema(conn)

    conn.execute(
        "INSERT INTO media_assets (id, url, media_type, status) VALUES (3, ?, 'video', 'queued_for_sampling')",
        ("https://cdn.example/legacy.mp4",),
    )
    conn.execute(
        """INSERT INTO sample_jobs (asset_id, status, priority, created_at, updated_at)
           VALUES (3, 'pending', 1, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')"""
    )
    conn.commit()
    conn.close()

    queue = CrawlerMediaQueue(str(db_path), worker_name="legacy-worker")
    try:
        job = queue.claim_job()
        assert job is not None
        assert job.asset_id == 3
    finally:
        queue.close()


def test_retry_policy_requeues_before_max_then_fails(tmp_path):
    db_path = tmp_path / "media_retry.db"
    conn = sqlite3.connect(str(db_path))
    _create_media_schema(conn)

    conn.execute(
        "INSERT INTO media_assets (id, url, media_type, status) VALUES (4, ?, 'video', 'claimed')",
        ("https://cdn.example/retry.mp4",),
    )
    conn.execute(
        """INSERT INTO sample_jobs (asset_id, status, priority, retry_count, created_at, updated_at)
           VALUES (4, 'claimed', 5, 0, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')"""
    )
    conn.commit()
    conn.close()

    queue = CrawlerMediaQueue(str(db_path), worker_name="worker-retry")
    try:
        status_1 = queue.mark_failed_or_retry(4, error_message="network", max_retry_count=2)
        assert status_1 == "pending"

        status_2 = queue.mark_failed_or_retry(4, error_message="network", max_retry_count=2)
        assert status_2 == "pending"

        status_3 = queue.mark_failed_or_retry(4, error_message="network", max_retry_count=2)
        assert status_3 == "failed"

        conn = sqlite3.connect(str(db_path))
        retry_count, status = conn.execute(
            "SELECT retry_count, status FROM sample_jobs WHERE asset_id = 4"
        ).fetchone()
        conn.close()

        assert retry_count == 3
        assert status == "failed"
    finally:
        queue.close()


def test_boost_domain_priority_updates_pending_jobs_for_same_source(tmp_path):
    db_path = tmp_path / "media_domain_boost.db"
    conn = sqlite3.connect(str(db_path))
    _create_media_schema(conn)

    conn.execute(
        """INSERT INTO media_assets (id, url, media_type, source_domain, status)
           VALUES (10, ?, 'video', 'pirate.example', 'queued_for_sampling')""",
        ("https://pirate.example/a.mp4",),
    )
    conn.execute(
        """INSERT INTO media_assets (id, url, media_type, source_domain, status)
           VALUES (11, ?, 'video', 'pirate.example', 'queued_for_sampling')""",
        ("https://pirate.example/b.mp4",),
    )
    conn.execute(
        """INSERT INTO media_assets (id, url, media_type, source_domain, status)
           VALUES (12, ?, 'video', 'other.example', 'queued_for_sampling')""",
        ("https://other.example/c.mp4",),
    )

    conn.execute(
        """INSERT INTO sample_jobs (asset_id, status, priority, created_at, updated_at)
           VALUES (10, 'pending', 9, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')"""
    )
    conn.execute(
        """INSERT INTO sample_jobs (asset_id, status, priority, created_at, updated_at)
           VALUES (11, 'pending', 8, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')"""
    )
    conn.execute(
        """INSERT INTO sample_jobs (asset_id, status, priority, created_at, updated_at)
           VALUES (12, 'pending', 7, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')"""
    )
    conn.commit()
    conn.close()

    queue = CrawlerMediaQueue(str(db_path), worker_name="worker-domain")
    try:
        updated = queue.boost_domain_priority("pirate.example", boosted_priority=1)
        assert updated == 2

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute(
            """SELECT ma.source_domain, sj.priority
               FROM sample_jobs sj
               JOIN media_assets ma ON ma.id = sj.asset_id
               ORDER BY sj.asset_id ASC"""
        ).fetchall()
        conn.close()

        assert rows[0] == ("pirate.example", 1)
        assert rows[1] == ("pirate.example", 1)
        assert rows[2] == ("other.example", 7)
    finally:
        queue.close()


def test_requeue_stale_claimed_jobs_moves_jobs_back_to_pending(tmp_path):
    db_path = tmp_path / "media_reclaim.db"
    conn = sqlite3.connect(str(db_path))
    _create_media_schema(conn)

    conn.execute(
        "INSERT INTO media_assets (id, url, media_type, status) VALUES (20, ?, 'video', 'claimed')",
        ("https://cdn.example/stale.mp4",),
    )
    conn.execute(
        """INSERT INTO sample_jobs (asset_id, status, priority, claimed_by, created_at, updated_at)
           VALUES (20, 'claimed', 5, 'worker-a', '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')"""
    )
    conn.commit()
    conn.close()

    queue = CrawlerMediaQueue(str(db_path), worker_name="worker-reclaim")
    try:
        reclaimed = queue.requeue_stale_claimed_jobs(stale_after_seconds=10)
        assert reclaimed == 1

        conn = sqlite3.connect(str(db_path))
        sample = conn.execute(
            "SELECT status, claimed_by FROM sample_jobs WHERE asset_id = 20"
        ).fetchone()
        asset = conn.execute(
            "SELECT status FROM media_assets WHERE id = 20"
        ).fetchone()
        conn.close()

        assert sample == ("pending", None)
        assert asset == ("queued_for_sampling",)
    finally:
        queue.close()
