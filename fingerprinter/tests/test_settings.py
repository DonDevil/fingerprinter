from fingerprinter.config.settings import load_settings


def test_load_settings_uses_defaults_for_missing_file(tmp_path):
    settings = load_settings(str(tmp_path / "missing.yaml"))
    assert settings.pipeline.short_video_threshold_seconds == 19
    assert settings.queue.backend == "crawler"
    assert settings.queue.max_retry_count == 3
    assert settings.downloader.enable_tor is False
    assert settings.storage_policy.max_rejected_files == 200


def test_load_settings_reads_threshold_and_queue_fields(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
                """pipeline:
    short_video_threshold_seconds: -1
queue:
    backend: crawler
    worker_name: unit-worker
    poll_interval_seconds: 5
    max_retry_count: 6
storage_policy:
    max_rejected_files: 50
    max_rejected_bytes_mb: 512
downloader:
    enable_tor: false
""",
        encoding="utf-8",
    )

    settings = load_settings(str(config_path))
    assert settings.pipeline.short_video_threshold_seconds == -1
    assert settings.queue.worker_name == "unit-worker"
    assert settings.queue.poll_interval_seconds == 5
    assert settings.queue.max_retry_count == 6
    assert settings.downloader.enable_tor is False
    assert settings.storage_policy.max_rejected_files == 50
    assert settings.storage_policy.max_rejected_bytes_mb == 512
