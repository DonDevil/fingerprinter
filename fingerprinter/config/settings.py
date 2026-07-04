from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


DEFAULT_SHORT_VIDEO_THRESHOLD_SECONDS = 19
DEFAULT_QUEUE_BACKEND = "crawler"
DEFAULT_CRAWLER_MEDIA_DB_PATH = "../crawler/storage/media_evidence.db"
DEFAULT_WORKER_NAME = "fingerprinter-worker"
DEFAULT_POLL_INTERVAL_SECONDS = 2
DEFAULT_MAX_RETRY_COUNT = 3
DEFAULT_DOWNLOAD_DIR = "fingerprinter/storage/downloads"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30
DEFAULT_ENABLE_TOR = False
DEFAULT_METADATA_DB_PATH = "fingerprinter/storage/processing.db"
DEFAULT_MAX_REJECTED_FILES = 200
DEFAULT_MAX_REJECTED_BYTES_MB = 2048
DEFAULT_DELETE_REJECTED_OVERFLOW = True


@dataclass(slots=True)
class PipelineConfig:
    short_video_threshold_seconds: int = DEFAULT_SHORT_VIDEO_THRESHOLD_SECONDS


@dataclass(slots=True)
class QueueConfig:
    backend: str = DEFAULT_QUEUE_BACKEND
    crawler_media_db_path: str = DEFAULT_CRAWLER_MEDIA_DB_PATH
    worker_name: str = DEFAULT_WORKER_NAME
    poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS
    max_retry_count: int = DEFAULT_MAX_RETRY_COUNT


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
