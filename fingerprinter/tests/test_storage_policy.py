from pathlib import Path

from fingerprinter.storage.file_retention import RejectedFileRetentionPolicy
from fingerprinter.storage.processing_metadata_store import ProcessingMetadataStore


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
