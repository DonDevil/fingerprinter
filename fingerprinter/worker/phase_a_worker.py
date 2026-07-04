from __future__ import annotations

import time
from pathlib import Path

from loguru import logger

from fingerprinter.config.settings import Settings
from fingerprinter.downloader.video_downloader import VideoDownloader
from fingerprinter.fingerprint.video_probe import get_video_duration_seconds
from fingerprinter.matcher.duration_gate import should_reject_for_short_duration
from fingerprinter.matcher.media_type_gate import should_reject_non_video
from fingerprinter.queue.crawler_media_queue import CrawlerMediaQueue
from fingerprinter.queue.models import QueueJob
from fingerprinter.storage.file_retention import RejectedFileRetentionPolicy
from fingerprinter.storage.processing_metadata_store import ProcessingMetadataStore


class PhaseAWorker:
    """Phase A worker: claim crawler jobs, sample media, and run duration gate."""

    def __init__(self, settings: Settings):
        self.settings = settings
        if settings.queue.backend != "crawler":
            raise ValueError(
                f"Unsupported queue backend: {settings.queue.backend}. Phase A currently supports crawler backend only."
            )

        self.downloader = VideoDownloader(
            download_dir=settings.downloader.download_dir,
            request_timeout_seconds=settings.downloader.request_timeout_seconds,
        )
        self.queue = CrawlerMediaQueue(
            db_path=settings.queue.crawler_media_db_path,
            worker_name=settings.queue.worker_name,
        )
        self.metadata_store = ProcessingMetadataStore(settings.storage_policy.metadata_db_path)
        self.retention_policy = RejectedFileRetentionPolicy(
            self.metadata_store,
            max_rejected_files=settings.storage_policy.max_rejected_files,
            max_rejected_bytes_mb=settings.storage_policy.max_rejected_bytes_mb,
            delete_overflow=settings.storage_policy.delete_rejected_overflow,
        )

    @staticmethod
    def _build_filename(job: QueueJob) -> str:
        suffix = Path(job.media_url).suffix or ".bin"
        return f"asset_{job.asset_id}{suffix}"

    def process_job(self, job: QueueJob) -> str:
        logger.info("Processing asset_id={} url={}", job.asset_id, job.media_url)

        reject_non_video, non_video_reason = should_reject_non_video(
            job.media_type,
            job.media_url,
            enable_tor=self.settings.downloader.enable_tor,
        )
        if reject_non_video:
            self.queue.update_job_status(job.asset_id, "rejected_non_video", last_error=non_video_reason)
            self.metadata_store.record_asset(
                asset_id=job.asset_id,
                media_url=job.media_url,
                local_path="",
                file_size_bytes=None,
                duration_seconds=None,
                decision="rejected_non_video",
                note=non_video_reason,
            )
            self.retention_policy.enforce()
            logger.info("Rejected asset_id={} reason={}", job.asset_id, non_video_reason)
            return "rejected_non_video"

        local_path = self.downloader.download(job.media_url, filename=self._build_filename(job))
        file_size_bytes = Path(local_path).stat().st_size if Path(local_path).exists() else None

        duration_seconds = get_video_duration_seconds(local_path)
        reject, reason = should_reject_for_short_duration(
            duration_seconds=duration_seconds,
            threshold_seconds=self.settings.pipeline.short_video_threshold_seconds,
        )

        if reject:
            self.queue.update_job_status(job.asset_id, "rejected_too_short", last_error=reason)
            self.metadata_store.record_asset(
                asset_id=job.asset_id,
                media_url=job.media_url,
                local_path=local_path,
                file_size_bytes=file_size_bytes,
                duration_seconds=duration_seconds,
                decision="rejected_too_short",
                note=reason,
            )
            self.retention_policy.enforce()
            logger.info("Rejected asset_id={} reason={}", job.asset_id, reason)
            return "rejected_too_short"

        self.queue.update_job_status(job.asset_id, "sampled")
        self.metadata_store.record_asset(
            asset_id=job.asset_id,
            media_url=job.media_url,
            local_path=local_path,
            file_size_bytes=file_size_bytes,
            duration_seconds=duration_seconds,
            decision="sampled",
            note="retained as approved evidence candidate",
        )
        logger.info(
            "Sampled asset_id={} duration_seconds={} threshold={}s",
            job.asset_id,
            duration_seconds,
            self.settings.pipeline.short_video_threshold_seconds,
        )
        return "sampled"

    def run_once(self) -> bool:
        job = self.queue.claim_job()
        if job is None:
            return False

        try:
            self.process_job(job)
        except Exception as exc:
            final_status = self.queue.mark_failed_or_retry(
                job.asset_id,
                error_message=str(exc),
                max_retry_count=self.settings.queue.max_retry_count,
            )
            self.metadata_store.record_asset(
                asset_id=job.asset_id,
                media_url=job.media_url,
                local_path="",
                file_size_bytes=None,
                duration_seconds=None,
                decision=final_status,
                note=str(exc),
            )
            logger.exception("Phase A processing failed for asset_id={}: {}", job.asset_id, exc)

        return True

    def run_loop(self, max_jobs: int | None = None) -> None:
        processed = 0
        poll_seconds = max(1, int(self.settings.queue.poll_interval_seconds))

        while True:
            if max_jobs is not None and processed >= max_jobs:
                logger.info("Reached max_jobs={} and stopping", max_jobs)
                break

            worked = self.run_once()
            if worked:
                processed += 1
                continue

            logger.info("No pending jobs. Sleeping {}s", poll_seconds)
            time.sleep(poll_seconds)

    def close(self) -> None:
        self.queue.close()
        self.metadata_store.close()
