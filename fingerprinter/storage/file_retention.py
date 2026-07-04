from __future__ import annotations

from pathlib import Path

from loguru import logger

from fingerprinter.storage.processing_metadata_store import ProcessingMetadataStore


class RejectedFileRetentionPolicy:
    """Enforce maximum retained rejected files by count and total size."""

    def __init__(
        self,
        metadata_store: ProcessingMetadataStore,
        *,
        max_rejected_files: int,
        max_rejected_bytes_mb: int,
        delete_overflow: bool,
    ):
        self.metadata_store = metadata_store
        self.max_rejected_files = max(0, int(max_rejected_files))
        self.max_rejected_bytes = max(0, int(max_rejected_bytes_mb)) * 1024 * 1024
        self.delete_overflow = delete_overflow

    def enforce(self) -> int:
        if not self.delete_overflow:
            return 0

        records = self.metadata_store.list_rejected_not_deleted()
        total_count = len(records)
        total_size = sum(int(item.get("file_size_bytes") or 0) for item in records)
        deleted = 0

        for item in records:
            count_ok = total_count <= self.max_rejected_files if self.max_rejected_files > 0 else total_count == 0
            size_ok = total_size <= self.max_rejected_bytes if self.max_rejected_bytes > 0 else total_size == 0
            if count_ok and size_ok:
                break

            local_path = Path(str(item.get("local_path") or ""))
            file_size = int(item.get("file_size_bytes") or 0)
            record_id = int(item["id"])

            try:
                if local_path.exists():
                    local_path.unlink()
                self.metadata_store.mark_deleted(
                    record_id,
                    note="deleted by rejected-file retention policy",
                )
                deleted += 1
                total_count -= 1
                total_size = max(0, total_size - file_size)
                logger.info("Deleted rejected file due to retention policy: {}", local_path)
            except Exception as exc:
                logger.warning("Failed to delete rejected file {}: {}", local_path, exc)

        return deleted
