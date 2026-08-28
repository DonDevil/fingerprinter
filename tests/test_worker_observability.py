"""Phase 13C — tests for worker/observability.py and its wiring into
worker/fingerprint_worker.py, worker/matching_handler.py, worker/main.py.

Unit-level tests exercise `ObservingWorkerObserver` directly against the
shared test Redis (`tests/conftest.py`'s db 15) through a real `Worker`,
so counters/latency reflect the exact same claim/reclaim/finalize
boundaries production code goes through — no separate bookkeeping to
drift out of sync. Process-level tests at the bottom exercise
`python -m worker.main` as a real OS process for the run-record/crash
semantics that only make sense at process granularity (SIGTERM vs SIGKILL).
"""
from __future__ import annotations

import io
import json
import logging
import os
import signal as signal_module
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

from tests.conftest import TEST_REDIS_URL
from work_queue.jobs import Job
from work_queue.keys import CONSUMER_GROUP, stream_key
from work_queue.producer import JobProducer
from worker.fingerprint_worker import PermanentFailure, TransientFailure, Worker
from worker.observability import (
    JsonFormatter,
    ObservingWorkerObserver,
    ResourceSampler,
    WorkerIdentity,
    classify_error_type,
    configure_json_logging,
    log_event,
    redis_health_snapshot,
)

REPO_ROOT = Path(__file__).parent.parent
BLOCK_MS = 200
LEASE_MS = 5000


def _make_logger():
    """A private, non-propagating logger writing JSON lines to an in-memory
    buffer, so tests can assert on exactly the structured events this
    module emits without depending on root-logger/pytest log-capture
    configuration."""
    logger = logging.getLogger(f"test.observability.{uuid.uuid4().hex}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    buffer = io.StringIO()
    handler = logging.StreamHandler(buffer)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    return logger, buffer


def _events(buffer: io.StringIO) -> list[dict]:
    return [json.loads(line) for line in buffer.getvalue().splitlines() if line.strip()]


def _make_observer(redis_client, consumer_name="obs-w1", health_interval_s=9999.0):
    logger, buffer = _make_logger()
    identity = WorkerIdentity.build(consumer_name)
    observer = ObservingWorkerObserver(
        identity, redis_client, stream_key(), CONSUMER_GROUP, logger, health_interval_s=health_interval_s
    )
    return observer, buffer


# ---------------------------------------------------------------------------
# 1. Structured startup event
# ---------------------------------------------------------------------------


def test_structured_event_is_valid_json_with_identity_fields():
    logger, buffer = _make_logger()
    log_event(logger, "worker_started", message="starting up", worker_id="w1", hostname="h1", pid=123)

    events = _events(buffer)
    assert len(events) == 1
    event = events[0]
    assert event["event"] == "worker_started"
    assert event["message"] == "starting up"
    assert event["worker_id"] == "w1"
    assert event["hostname"] == "h1"
    assert event["pid"] == 123
    assert "timestamp" in event and "level" in event


def test_worker_process_emits_worker_started_and_worker_ready_as_valid_json(redis_client, tmp_path):
    proc = subprocess.Popen(
        [sys.executable, "-m", "worker.main"],
        cwd=str(REPO_ROOT),
        env=_subprocess_env(tmp_path),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        assert _wait_for_consumer_group(redis_client), "worker never created consumer group"
        proc.send_signal(signal_module.SIGTERM)
        returncode = proc.wait(timeout=15)
        output = proc.stdout.read()
        assert returncode == 0, output

        events = [json.loads(line) for line in output.splitlines() if line.strip().startswith("{")]
        event_names = {e["event"] for e in events}
        assert "worker_started" in event_names
        assert "worker_ready" in event_names
        assert "worker_shutdown_requested" in event_names
        assert "worker_stopped" in event_names

        started = next(e for e in events if e["event"] == "worker_started")
        assert "configuration" in started
        assert started["configuration"]["namespace"] == "fingerprint"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


# ---------------------------------------------------------------------------
# 2. Configuration snapshot never leaks Redis credentials
# ---------------------------------------------------------------------------


def test_config_snapshot_does_not_leak_redis_credentials():
    import worker.main as main_module

    config = main_module.WorkerConfig.from_env(
        {"REDIS_URL": "redis://myuser:supersecret@redis.internal:6379/2"}
    )
    snapshot = main_module.config_snapshot(config, "worker-1")

    dumped = json.dumps(snapshot)
    assert "supersecret" not in dumped
    assert "myuser" not in dumped
    assert snapshot["redis_endpoint"] == "redis://redis.internal:6379/2"
    assert snapshot["redis_db"] == "2"
    assert snapshot["log_level"] == "INFO"
    assert snapshot["log_format"] == "auto"


def test_configure_json_logging_pins_known_noisy_third_party_loggers_to_warning():
    """Same reasoning as target/cli.py's identical guard: redis-py's
    harmless "Failed to enable maintenance notifications" DEBUG line (and
    PIL's per-PNG-chunk decode logging) must not drown out this project's
    own DEBUG diagnostics when WORKER_LOG_LEVEL=DEBUG. Saves/restores the
    root logger's level and handlers -- `configure_json_logging` mutates
    the root logger by design (this is what makes it work in a real
    process), which would otherwise leak into other tests sharing this
    pytest process."""
    root = logging.getLogger()
    original_level = root.level
    original_handlers = list(root.handlers)
    try:
        configure_json_logging(level=logging.DEBUG)

        assert root.getEffectiveLevel() == logging.DEBUG
        assert logging.getLogger("redis").getEffectiveLevel() == logging.WARNING
        assert logging.getLogger("PIL").getEffectiveLevel() == logging.WARNING
        assert logging.getLogger("worker.matching_handler").getEffectiveLevel() == logging.DEBUG
    finally:
        root.setLevel(original_level)
        root.handlers[:] = original_handlers


# ---------------------------------------------------------------------------
# 3/6. Claim / reclaim increment the right counters
# ---------------------------------------------------------------------------


def test_job_claim_increments_counter(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    observer, _ = _make_observer(redis_client)
    worker = Worker(redis_client, consumer_name="w1", block_ms=BLOCK_MS, observer=observer)

    entry = worker.claim_one()

    assert entry.is_valid
    counters = observer.counters_snapshot()
    assert counters["jobs_claimed"] == 1
    assert counters["active_jobs"] == 1
    assert counters["total_job_attempts"] == 1


def test_reclaim_increments_reclaim_counter(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker_a = Worker(redis_client, consumer_name="worker-a", block_ms=BLOCK_MS, lease_ms=150)
    worker_a.claim_one()  # "crashes": never finalized

    time.sleep(0.25)

    observer, _ = _make_observer(redis_client, consumer_name="worker-b")
    worker_b = Worker(redis_client, consumer_name="worker-b", block_ms=BLOCK_MS, lease_ms=150, observer=observer)
    reclaimed = worker_b.reclaim_stale()

    assert len(reclaimed) == 1
    counters = observer.counters_snapshot()
    assert counters["jobs_reclaimed"] == 1
    assert counters["active_jobs"] == 1


def test_malformed_job_rejection_increments_rejected_not_claimed(redis_client):
    redis_client.xadd(
        stream_key(),
        {
            "job_id": "bad-job",
            "media_evidence_id": "evidence-1",
            "media_type": "video",
            "source_domain": "example.com",
            "target_id": "target-1",
            "target_version": "abc123",
            "techniques": "dinov2",
            "max_attempts": "3",
        },
    )
    observer, buffer = _make_observer(redis_client)
    worker = Worker(redis_client, consumer_name="w1", block_ms=BLOCK_MS, observer=observer)

    entry = worker.claim_one()

    assert not entry.is_valid
    counters = observer.counters_snapshot()
    assert counters["jobs_rejected"] == 1
    assert counters["jobs_claimed"] == 0
    assert counters["active_jobs"] == 0

    rejected_events = [e for e in _events(buffer) if e["event"] == "job_rejected"]
    assert len(rejected_events) == 1
    # Validation error text is safe to log in full (never a media URL).
    assert "media_url" in rejected_events[0]["error"]


# ---------------------------------------------------------------------------
# 4/7/8. Completion increments counter exactly once, active_jobs returns to
# zero, latency is recorded
# ---------------------------------------------------------------------------


def test_successful_completion_increments_completion_counter_exactly_once_and_records_latency(
    redis_client, sample_job
):
    JobProducer(redis_client).enqueue(sample_job)
    observer, buffer = _make_observer(redis_client)
    worker = Worker(redis_client, consumer_name="w1", block_ms=BLOCK_MS, observer=observer)
    entry = worker.claim_one()

    worker.process_claim(entry, lambda job: None)

    counters = observer.counters_snapshot()
    assert counters["jobs_completed"] == 1
    assert counters["active_jobs"] == 0

    completed_events = [e for e in _events(buffer) if e["event"] == "job_completed"]
    assert len(completed_events) == 1

    latency = observer.latency_snapshot()
    assert latency["claim_to_completion_ms"]["count"] == 1
    assert latency["claim_to_completion_ms"]["min"] is not None
    assert latency["claim_to_completion_ms"]["min"] >= 0


def test_stale_finalize_does_not_double_count_completion(redis_client, sample_job):
    """A stale worker's finalize attempt (attempt fencing, Phase 2) must not
    also increment jobs_completed — only the winning worker's finalize does."""
    JobProducer(redis_client).enqueue(sample_job)
    worker_a = Worker(redis_client, consumer_name="worker-a", block_ms=BLOCK_MS, lease_ms=150)
    worker_b = Worker(redis_client, consumer_name="worker-b", block_ms=BLOCK_MS, lease_ms=150)
    entry_a = worker_a.claim_one()
    time.sleep(0.25)
    reclaimed = worker_b.reclaim_stale()
    entry_b = reclaimed[0]

    observer, _ = _make_observer(redis_client, consumer_name="worker-a")
    worker_a2 = Worker(
        redis_client, consumer_name="worker-a", block_ms=BLOCK_MS, lease_ms=150, observer=observer
    )
    stale_completed = worker_a2.ack(entry_a)
    worker_b.ack(entry_b)

    assert stale_completed is False
    assert observer.counters_snapshot()["jobs_completed"] == 0


# ---------------------------------------------------------------------------
# 5. Failure increments the correct counter (transient/permanent/exhausted)
# ---------------------------------------------------------------------------


def test_permanent_failure_increments_jobs_failed(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    observer, buffer = _make_observer(redis_client)
    worker = Worker(redis_client, consumer_name="w1", block_ms=BLOCK_MS, observer=observer)
    entry = worker.claim_one()

    def _boom(job):
        raise PermanentFailure("no good", error_type="NotFoundError")

    worker.process_claim(entry, _boom)

    counters = observer.counters_snapshot()
    assert counters["jobs_failed"] == 1
    assert counters["active_jobs"] == 0
    failed_events = [e for e in _events(buffer) if e["event"] == "job_failed"]
    assert failed_events[0]["error_category"] == classify_error_type("NotFoundError")


def test_transient_failure_increments_jobs_retried(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    observer, _ = _make_observer(redis_client)
    worker = Worker(
        redis_client, consumer_name="w1", block_ms=BLOCK_MS, observer=observer, retry_base_delay_s=0.05
    )
    entry = worker.claim_one()

    def _blip(job):
        raise TransientFailure("network blip", error_type="ConnectionTimeoutError")

    worker.process_claim(entry, _blip)

    counters = observer.counters_snapshot()
    assert counters["jobs_retried"] == 1
    assert counters["active_jobs"] == 0


def test_exhausted_retries_increments_jobs_permanently_failed(redis_client, make_job):
    job = make_job(job_id="job-exhaust", max_attempts=1)
    JobProducer(redis_client).enqueue(job)
    observer, _ = _make_observer(redis_client)
    worker = Worker(redis_client, consumer_name="w1", block_ms=BLOCK_MS, observer=observer)
    entry = worker.claim_one()

    def _blip(job):
        raise TransientFailure("network blip", error_type="ConnectionTimeoutError")

    worker.process_claim(entry, _blip)  # attempt 1 == max_attempts 1: exhausted, not retried

    counters = observer.counters_snapshot()
    assert counters["jobs_permanently_failed"] == 1
    assert counters["jobs_retried"] == 0
    assert counters["active_jobs"] == 0


# ---------------------------------------------------------------------------
# 9. Health summary is emitted
# ---------------------------------------------------------------------------


def test_health_summary_is_emitted_with_expected_fields(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    observer, buffer = _make_observer(redis_client, health_interval_s=0.0)
    worker = Worker(redis_client, consumer_name="w1", block_ms=BLOCK_MS, observer=observer)
    entry = worker.claim_one()
    worker.process_claim(entry, lambda job: None)

    observer.on_loop_tick()  # interval is 0 -> due immediately

    health_events = [e for e in _events(buffer) if e["event"] == "worker_health"]
    assert len(health_events) == 1
    event = health_events[0]
    assert event["jobs_completed"] == 1
    assert "redis" in event and event["redis"] is not None
    assert "resources" in event and event["resources"] is not None
    assert event["resources"]["cpu_time_s"] is not None


def test_health_summary_not_emitted_before_interval_elapses(redis_client):
    observer, buffer = _make_observer(redis_client, health_interval_s=9999.0)
    observer.on_loop_tick()
    observer.on_loop_tick()
    assert [e for e in _events(buffer) if e["event"] == "worker_health"] == []


# ---------------------------------------------------------------------------
# 10. Redis pending/stream metrics are read correctly
# ---------------------------------------------------------------------------


def test_redis_health_snapshot_reads_stream_and_pel_state(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = Worker(redis_client, consumer_name="w1", block_ms=BLOCK_MS)
    worker.claim_one()  # leaves one entry pending (never acked)

    snapshot = redis_health_snapshot(redis_client, stream_key(), CONSUMER_GROUP, "w1")

    assert snapshot["stream_length"] == 1
    assert snapshot["group_pending"] == 1
    assert snapshot["consumer_pending"] == 1
    assert snapshot["oldest_pending_idle_ms"] is not None
    assert snapshot["oldest_pending_idle_ms"] >= 0


def test_redis_health_snapshot_empty_stream(redis_client):
    Worker(redis_client, consumer_name="w1", block_ms=BLOCK_MS)  # ensures group/stream exist

    snapshot = redis_health_snapshot(redis_client, stream_key(), CONSUMER_GROUP, "w1")

    assert snapshot["stream_length"] == 0
    assert snapshot["group_pending"] == 0
    assert snapshot["consumer_pending"] == 0
    assert snapshot["oldest_pending_idle_ms"] is None


# ---------------------------------------------------------------------------
# 11. Resource metrics are collected
# ---------------------------------------------------------------------------


def test_resource_sampler_collects_expected_fields():
    sampler = ResourceSampler()
    first = sampler.sample()
    assert first["cpu_time_s"] is not None and first["cpu_time_s"] >= 0
    assert first["rss_peak_bytes"] > 0
    assert first["thread_count"] >= 1
    # First sample has no prior point to diff against.
    assert first["cpu_percent"] is None

    time.sleep(0.05)
    second = sampler.sample()
    assert second["cpu_percent"] is not None
    assert second["rss_peak_bytes"] >= first["rss_peak_bytes"]


# ---------------------------------------------------------------------------
# 12. Shutdown summary is emitted
# ---------------------------------------------------------------------------


def test_shutdown_summary_is_emitted(redis_client):
    observer, buffer = _make_observer(redis_client)
    observer.emit_shutdown_summary(shutdown_reason="graceful_shutdown", clean=True)

    events = [e for e in _events(buffer) if e["event"] == "worker_stopped"]
    assert len(events) == 1
    assert events[0]["shutdown_reason"] == "graceful_shutdown"
    assert events[0]["clean"] is True


# ---------------------------------------------------------------------------
# 13. SIGTERM still calls Worker.stop() and does not ACK work (regression
# guard specific to this phase's changes to install_shutdown_handlers'
# call site / Worker's new observer plumbing)
# ---------------------------------------------------------------------------


def test_sigterm_handler_still_only_calls_stop():
    from unittest.mock import MagicMock

    import worker.main as main_module

    fake_worker = MagicMock()
    original = signal_module.getsignal(signal_module.SIGTERM)
    try:
        main_module.install_shutdown_handlers(fake_worker)
        handler = signal_module.getsignal(signal_module.SIGTERM)
        handler(signal_module.SIGTERM, None)
        assert fake_worker.stop.call_count == 1
        fake_worker.ack.assert_not_called()
        fake_worker.commit_result.assert_not_called()
    finally:
        signal_module.signal(signal_module.SIGTERM, original)


# ---------------------------------------------------------------------------
# 14/15. Process-level: run JSON on clean shutdown; crashed run is not
# falsely marked successful
# ---------------------------------------------------------------------------


def _wait_for_consumer_group(redis_client, timeout_s: float = 20.0) -> bool:
    from redis.exceptions import ResponseError

    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            groups = redis_client.xinfo_groups(stream_key())
        except ResponseError:
            groups = []
        if any(g["name"] == CONSUMER_GROUP for g in groups):
            return True
        time.sleep(0.1)
    return False


def _wait_for_file(path: Path, timeout_s: float = 20.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if path.exists():
            return True
        time.sleep(0.1)
    return False


def _subprocess_env(tmp_path: Path, **overrides) -> dict:
    env = dict(os.environ)
    env.update(
        {
            "REDIS_URL": TEST_REDIS_URL,
            "TARGET_CACHE_PATH": str(tmp_path / "target_cache"),
            "EMBEDDING_DEVICE": "cpu",
            "TORCH_NUM_THREADS": "1",
            "WORKER_BLOCK_MS": "200",
            "WORKER_LEASE_MS": "5000",
        }
    )
    env.update(overrides)
    return env


def test_run_json_is_generated_atomically_for_clean_shutdown(redis_client, tmp_path):
    run_output = tmp_path / "run.json"
    proc = subprocess.Popen(
        [sys.executable, "-m", "worker.main"],
        cwd=str(REPO_ROOT),
        env=_subprocess_env(tmp_path, WORKER_RUN_OUTPUT=str(run_output)),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        assert _wait_for_consumer_group(redis_client), "worker never created consumer group"
        assert _wait_for_file(run_output.with_suffix(run_output.suffix + ".marker")), "startup marker never written"

        proc.send_signal(signal_module.SIGTERM)
        returncode = proc.wait(timeout=15)
        output = proc.stdout.read()
        assert returncode == 0, output

        assert run_output.exists()
        record = json.loads(run_output.read_text())
        assert record["shutdown"]["clean"] is True
        assert record["shutdown"]["reason"] == "graceful_shutdown"
        assert record["metadata"]["hostname"]
        assert record["metadata"]["pid"] == proc.pid
        assert "counters" in record and "jobs_claimed" in record["counters"]
        assert "resources" in record
        assert "redis" in record

        # Clean shutdown removes the startup marker — its absence plus a
        # clean=true run record is the "this run finished normally" signal.
        assert not run_output.with_suffix(run_output.suffix + ".marker").exists()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


def test_incomplete_run_is_not_falsely_marked_successful(redis_client, tmp_path):
    run_output = tmp_path / "run.json"
    marker = run_output.with_suffix(run_output.suffix + ".marker")
    proc = subprocess.Popen(
        [sys.executable, "-m", "worker.main"],
        cwd=str(REPO_ROOT),
        env=_subprocess_env(tmp_path, WORKER_RUN_OUTPUT=str(run_output)),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        assert _wait_for_consumer_group(redis_client), "worker never created consumer group"
        assert _wait_for_file(marker), "startup marker never written"

        # Simulate a crash: SIGKILL cannot be caught, so no shutdown code
        # ever runs — no run.json is ever written for this run.
        proc.send_signal(signal_module.SIGKILL)
        proc.wait(timeout=10)

        assert not run_output.exists(), "a crashed run must never produce a run record claiming success"
        assert marker.exists(), "the startup marker must survive an uncaught crash as evidence a run began"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
