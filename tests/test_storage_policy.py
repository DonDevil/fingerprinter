from pathlib import Path

from storage.file_retention import RejectedFileRetentionPolicy
from storage.processing_metadata_store import ProcessingMetadataStore


def test_metadata_store_records_and_lists_rejected(tmp_path):
    db_path = tmp_path / "processing.db"
    store = ProcessingMetadataStore(str(db_path))

    try:
        file_path = tmp_path / "rej.mp4"
        file_path.write_bytes(b"1234")

        store.record_asset(
            asset_id=1,
            media_url="https://cdn.example/rej.mp4",
            local_path=str(file_path),
            file_size_bytes=4,
            duration_seconds=5.0,
            decision="rejected_too_short",
            note="below threshold",
        )

        rows = store.list_rejected_not_deleted()
        assert len(rows) == 1
        assert rows[0]["asset_id"] == 1
    finally:
        store.close()


def test_retention_policy_deletes_oldest_rejected_files(tmp_path):
    db_path = tmp_path / "processing.db"
    store = ProcessingMetadataStore(str(db_path))

    try:
        first = tmp_path / "r1.mp4"
        second = tmp_path / "r2.mp4"
        first.write_bytes(b"1" * 10)
        second.write_bytes(b"2" * 10)

        store.record_asset(
            asset_id=11,
            media_url="https://cdn.example/r1.mp4",
            local_path=str(first),
            file_size_bytes=10,
            duration_seconds=3.0,
            decision="rejected_too_short",
            note="short",
        )
        store.record_asset(
            asset_id=12,
            media_url="https://cdn.example/r2.mp4",
            local_path=str(second),
            file_size_bytes=10,
            duration_seconds=4.0,
            decision="rejected_too_short",
            note="short",
        )

        policy = RejectedFileRetentionPolicy(
            store,
            max_rejected_files=1,
            max_rejected_bytes_mb=1024,
            delete_overflow=True,
        )
        deleted = policy.enforce()

        assert deleted == 1
        remaining_files = [p for p in [first, second] if Path(p).exists()]
        assert len(remaining_files) == 1
    finally:
        store.close()


def test_processing_run_stage_and_feedback_records(tmp_path):
    db_path = tmp_path / "processing.db"
    store = ProcessingMetadataStore(str(db_path))

    try:
        run_id = store.create_processing_run(
            asset_id=99,
            source_domain="pirate.example",
            target_title="Blast",
            target_path="target/Blast.mp4",
            candidate_path="storage/downloads/asset_99.mp4",
            final_status="matched",
            piracy_score=0.92,
        )

        store.record_stage_result(
            run_id=run_id,
            asset_id=99,
            stage_name="stage1_metadata",
            score=0.88,
            decision="pass",
            note="good duration alignment",
            details={"duration_score": 0.88},
        )
        store.record_piracy_match(
            asset_id=99,
            media_url="https://pirate.example/blast.mp4",
            source_domain="pirate.example",
            target_title="Blast",
            candidate_path="storage/downloads/asset_99.mp4",
            piracy_score=0.92,
            status="pirated",
            evidence={"run_id": run_id},
        )
        store.record_crawler_feedback(
            asset_id=99,
            source_domain="pirate.example",
            feedback_type="pirate_domain_priority_boost",
            feedback_value="pending_jobs_updated=3",
        )

        conn = store.conn
        run_count = conn.execute("SELECT COUNT(*) FROM processing_runs").fetchone()[0]
        stage_count = conn.execute("SELECT COUNT(*) FROM stage_results").fetchone()[0]
        match_count = conn.execute("SELECT COUNT(*) FROM piracy_matches").fetchone()[0]
        feedback_count = conn.execute("SELECT COUNT(*) FROM crawler_feedback_events").fetchone()[0]

        assert run_count == 1
        assert stage_count == 1
        assert match_count == 1
        assert feedback_count == 1
    finally:
        store.close()


def test_delete_non_matching_assets_removes_file(tmp_path):
    db_path = tmp_path / "processing.db"
    store = ProcessingMetadataStore(str(db_path))
    try:
        file_path = tmp_path / "candidate.mp4"
        file_path.write_bytes(b"x" * 32)
        store.record_asset(
            asset_id=201,
            media_url="https://cdn.example/candidate.mp4",
            local_path=str(file_path),
            file_size_bytes=32,
            duration_seconds=12.0,
            decision="no_match_pending_review",
            note="no match",
        )

        policy = RejectedFileRetentionPolicy(
            store,
            max_rejected_files=100,
            max_rejected_bytes_mb=1024,
            delete_overflow=True,
        )
        deleted = policy.delete_non_matching_assets()
        assert deleted == 1
        assert not file_path.exists()
    finally:
        store.close()


def test_compare_task_tracking_records_completed_and_failed(tmp_path):
    db_path = tmp_path / "processing.db"
    store = ProcessingMetadataStore(str(db_path))
    try:
        store.start_compare_task(
            task_key="file:/tmp/a.mp4|target:/tmp/target.mp4|techniques:audio,visual",
            mode="single_file",
            asset_id=123,
            candidate_ref="/tmp/a.mp4",
            target_path="/tmp/target.mp4",
            techniques=["audio", "visual"],
        )
        assert store.is_compare_task_completed(
            task_key="file:/tmp/a.mp4|target:/tmp/target.mp4|techniques:audio,visual"
        ) is False

        store.complete_compare_task(
            task_key="file:/tmp/a.mp4|target:/tmp/target.mp4|techniques:audio,visual",
            run_id=1,
            outcome_status="matched",
        )
        assert store.is_compare_task_completed(
            task_key="file:/tmp/a.mp4|target:/tmp/target.mp4|techniques:audio,visual"
        ) is True

        store.start_compare_task(
            task_key="queue_asset:99",
            mode="queue",
            asset_id=99,
            candidate_ref="https://cdn.example/v.mp4",
            target_path="target/Blast.mp4",
            techniques=["metadata", "visual", "audio", "temporal"],
        )
        store.fail_compare_task(task_key="queue_asset:99", error_message="boom")

        counts = store.compare_task_status_counts()
        assert counts["completed"] == 1
        assert counts["failed"] == 1
    finally:
        store.close()


def test_mark_assets_deleted_by_local_paths(tmp_path):
    db_path = tmp_path / "processing.db"
    store = ProcessingMetadataStore(str(db_path))
    try:
        file_path = tmp_path / "sample.mp4"
        file_path.write_bytes(b"x" * 4)
        store.record_asset(
            asset_id=300,
            media_url="https://cdn.example/sample.mp4",
            local_path=str(file_path),
            file_size_bytes=4,
            duration_seconds=1.0,
            decision="no_match_pending_review",
            note="before",
        )

        updated = store.mark_assets_deleted_by_local_paths(
            local_paths=[str(file_path)],
            note="deleted by test",
        )
        assert updated == 1

        row = store.conn.execute(
            "SELECT deleted_at, note FROM processed_assets WHERE asset_id = 300"
        ).fetchone()
        assert row[0] is not None
        assert row[1] == "deleted by test"
    finally:
        store.close()


def test_reset_all_tables_clears_processing_metadata(tmp_path):
    db_path = tmp_path / "processing.db"
    store = ProcessingMetadataStore(str(db_path))
    try:
        run_id = store.create_processing_run(
            asset_id=401,
            source_domain="example.com",
            target_title="Blast",
            target_path="target/Blast.mp4",
            candidate_path="storage/downloads/a.mp4",
            final_status="matched",
            piracy_score=0.99,
        )
        store.record_stage_result(
            run_id=run_id,
            asset_id=401,
            stage_name="stage1_metadata",
            score=0.5,
            decision="pass",
            note="ok",
            details={},
        )
        store.record_asset(
            asset_id=401,
            media_url="https://example.com/a.mp4",
            local_path="storage/downloads/a.mp4",
            file_size_bytes=10,
            duration_seconds=2.0,
            decision="matched",
            note="ok",
        )
        store.start_compare_task(
            task_key="queue_asset:401",
            mode="queue",
            asset_id=401,
            candidate_ref="https://example.com/a.mp4",
            target_path="target/Blast.mp4",
            techniques=["metadata"],
        )

        store.reset_all_tables()

        tables = [
            "processed_assets",
            "processing_runs",
            "stage_results",
            "piracy_matches",
            "crawler_feedback_events",
            "compare_tasks",
        ]
        for table in tables:
            count = store.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert count == 0
    finally:
        store.close()
