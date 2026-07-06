from config.settings import load_settings


def test_load_settings_uses_defaults_for_missing_file(tmp_path):
    settings = load_settings(str(tmp_path / "missing.yaml"))
    assert settings.pipeline.short_video_threshold_seconds == 19
    assert settings.pipeline.target_title == ""
    assert settings.pipeline.target_file_path == "target/Blast.mp4"
    assert settings.pipeline.phase_b_low_match_threshold == 0.2
    assert settings.pipeline.phase_b_high_match_threshold == 0.65
    assert settings.pipeline.pirate_domain_boost_priority == 1
    assert settings.queue.backend == "crawler"
    assert settings.queue.max_retry_count == 3
    assert settings.queue.reclaim_claimed_after_seconds == 600
    assert settings.downloader.download_dir == "storage/downloads"
    assert settings.downloader.enable_tor is False
    assert settings.storage_policy.metadata_db_path == "storage/processing.db"
    assert settings.storage_policy.max_rejected_files == 200


def test_load_settings_reads_threshold_and_queue_fields(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
                """pipeline:
    short_video_threshold_seconds: -1
    target_title: The Matrix
    target_file_path: target/TheMatrix.mp4
    phase_b_low_match_threshold: 0.15
    phase_b_high_match_threshold: 0.70
    pirate_domain_boost_priority: 2
queue:
    backend: crawler
    worker_name: unit-worker
    poll_interval_seconds: 5
    max_retry_count: 6
    reclaim_claimed_after_seconds: 120
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
    assert settings.pipeline.target_title == "The Matrix"
    assert settings.pipeline.target_file_path == "target/TheMatrix.mp4"
    assert settings.pipeline.phase_b_low_match_threshold == 0.15
    assert settings.pipeline.phase_b_high_match_threshold == 0.70
    assert settings.pipeline.pirate_domain_boost_priority == 2
    assert settings.queue.worker_name == "unit-worker"
    assert settings.queue.poll_interval_seconds == 5
    assert settings.queue.max_retry_count == 6
    assert settings.queue.reclaim_claimed_after_seconds == 120
    assert settings.downloader.enable_tor is False
    assert settings.storage_policy.max_rejected_files == 50
    assert settings.storage_policy.max_rejected_bytes_mb == 512
