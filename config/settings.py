from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


DEFAULT_SHORT_VIDEO_THRESHOLD_SECONDS = 19
DEFAULT_TARGET_TITLE = ""
DEFAULT_TARGET_FILE_PATH = "target/Blast.mp4"
DEFAULT_PHASE_B_LOW_MATCH_THRESHOLD = 0.2
DEFAULT_PHASE_B_HIGH_MATCH_THRESHOLD = 0.65
DEFAULT_PIRATE_DOMAIN_BOOST_PRIORITY = 1
DEFAULT_QUEUE_BACKEND = "crawler"
DEFAULT_CRAWLER_MEDIA_DB_PATH = "../crawler/storage/media_evidence.db"
DEFAULT_WORKER_NAME = "fingerprinter-worker"
DEFAULT_POLL_INTERVAL_SECONDS = 2
DEFAULT_MAX_RETRY_COUNT = 3
DEFAULT_RECLAIM_CLAIMED_AFTER_SECONDS = 600
DEFAULT_DOWNLOAD_DIR = "storage/downloads"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30
DEFAULT_ENABLE_TOR = False
DEFAULT_METADATA_DB_PATH = "storage/processing.db"
DEFAULT_MAX_REJECTED_FILES = 200
DEFAULT_MAX_REJECTED_BYTES_MB = 2048
DEFAULT_DELETE_REJECTED_OVERFLOW = True


@dataclass(slots=True)
class PipelineConfig:
    short_video_threshold_seconds: int = DEFAULT_SHORT_VIDEO_THRESHOLD_SECONDS
    target_title: str = DEFAULT_TARGET_TITLE
    target_file_path: str = DEFAULT_TARGET_FILE_PATH
    phase_b_low_match_threshold: float = DEFAULT_PHASE_B_LOW_MATCH_THRESHOLD
    phase_b_high_match_threshold: float = DEFAULT_PHASE_B_HIGH_MATCH_THRESHOLD
    pirate_domain_boost_priority: int = DEFAULT_PIRATE_DOMAIN_BOOST_PRIORITY


@dataclass(slots=True)
class QueueConfig:
    backend: str = DEFAULT_QUEUE_BACKEND
    crawler_media_db_path: str = DEFAULT_CRAWLER_MEDIA_DB_PATH
    worker_name: str = DEFAULT_WORKER_NAME
    poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS
    max_retry_count: int = DEFAULT_MAX_RETRY_COUNT
    reclaim_claimed_after_seconds: int = DEFAULT_RECLAIM_CLAIMED_AFTER_SECONDS


@dataclass(slots=True)
class DownloaderConfig:
    download_dir: str = DEFAULT_DOWNLOAD_DIR
    request_timeout_seconds: int = DEFAULT_REQUEST_TIMEOUT_SECONDS
    enable_tor: bool = DEFAULT_ENABLE_TOR


@dataclass(slots=True)
class StoragePolicyConfig:
    metadata_db_path: str = DEFAULT_METADATA_DB_PATH
    max_rejected_files: int = DEFAULT_MAX_REJECTED_FILES
    max_rejected_bytes_mb: int = DEFAULT_MAX_REJECTED_BYTES_MB
    delete_rejected_overflow: bool = DEFAULT_DELETE_REJECTED_OVERFLOW


@dataclass(slots=True)
class Settings:
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)
    queue: QueueConfig = field(default_factory=QueueConfig)
    downloader: DownloaderConfig = field(default_factory=DownloaderConfig)
    storage_policy: StoragePolicyConfig = field(default_factory=StoragePolicyConfig)


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    if not isinstance(raw, dict):
        return {}

    return raw


def load_settings(path: str = "config.yaml") -> Settings:
    raw = _load_yaml(Path(path))

    pipeline_raw = raw.get("pipeline", {}) if isinstance(raw.get("pipeline", {}), dict) else {}
    queue_raw = raw.get("queue", {}) if isinstance(raw.get("queue", {}), dict) else {}
    downloader_raw = raw.get("downloader", {}) if isinstance(raw.get("downloader", {}), dict) else {}
    storage_raw = raw.get("storage_policy", {}) if isinstance(raw.get("storage_policy", {}), dict) else {}

    return Settings(
        pipeline=PipelineConfig(
            short_video_threshold_seconds=int(
                pipeline_raw.get("short_video_threshold_seconds", DEFAULT_SHORT_VIDEO_THRESHOLD_SECONDS)
            ),
            target_title=str(
                pipeline_raw.get("target_title", DEFAULT_TARGET_TITLE)
            ).strip(),
            target_file_path=str(
                pipeline_raw.get("target_file_path", DEFAULT_TARGET_FILE_PATH)
            ).strip(),
            phase_b_low_match_threshold=float(
                pipeline_raw.get("phase_b_low_match_threshold", DEFAULT_PHASE_B_LOW_MATCH_THRESHOLD)
            ),
            phase_b_high_match_threshold=float(
                pipeline_raw.get("phase_b_high_match_threshold", DEFAULT_PHASE_B_HIGH_MATCH_THRESHOLD)
            ),
            pirate_domain_boost_priority=int(
                pipeline_raw.get("pirate_domain_boost_priority", DEFAULT_PIRATE_DOMAIN_BOOST_PRIORITY)
            ),
        ),
        queue=QueueConfig(
            backend=str(queue_raw.get("backend", DEFAULT_QUEUE_BACKEND)).strip().lower(),
            crawler_media_db_path=str(
                queue_raw.get("crawler_media_db_path", DEFAULT_CRAWLER_MEDIA_DB_PATH)
            ),
            worker_name=str(queue_raw.get("worker_name", DEFAULT_WORKER_NAME)),
            poll_interval_seconds=int(
                queue_raw.get("poll_interval_seconds", DEFAULT_POLL_INTERVAL_SECONDS)
            ),
            max_retry_count=int(
                queue_raw.get("max_retry_count", DEFAULT_MAX_RETRY_COUNT)
            ),
            reclaim_claimed_after_seconds=int(
                queue_raw.get("reclaim_claimed_after_seconds", DEFAULT_RECLAIM_CLAIMED_AFTER_SECONDS)
            ),
        ),
        downloader=DownloaderConfig(
            download_dir=str(downloader_raw.get("download_dir", DEFAULT_DOWNLOAD_DIR)),
            request_timeout_seconds=int(
                downloader_raw.get("request_timeout_seconds", DEFAULT_REQUEST_TIMEOUT_SECONDS)
            ),
            enable_tor=bool(
                downloader_raw.get("enable_tor", DEFAULT_ENABLE_TOR)
            ),
        ),
        storage_policy=StoragePolicyConfig(
            metadata_db_path=str(storage_raw.get("metadata_db_path", DEFAULT_METADATA_DB_PATH)),
            max_rejected_files=int(storage_raw.get("max_rejected_files", DEFAULT_MAX_REJECTED_FILES)),
            max_rejected_bytes_mb=int(storage_raw.get("max_rejected_bytes_mb", DEFAULT_MAX_REJECTED_BYTES_MB)),
            delete_rejected_overflow=bool(
                storage_raw.get("delete_rejected_overflow", DEFAULT_DELETE_REJECTED_OVERFLOW)
            ),
        ),
    )
