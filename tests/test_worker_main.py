"""Phase 13B — tests for worker/main.py, the production worker entrypoint.

Config-parsing/validation, torch_num_threads plumbing, and signal-wiring
tests are unit-level (fast, no Redis/model needed). The subprocess tests
at the bottom exercise `python -m worker.main` as a real OS process against
a real (test) Redis, for startup and graceful-shutdown behavior.

Full end-to-end job processing (claim -> acquire -> embed -> match -> ack)
through the real subprocess is deliberately NOT covered here: the
production `MediaAcquirer` this entrypoint constructs uses Phase 13A's
SSRF guard with its default (correct, unmodified) `allow_private_networks
=False`, which rejects every loopback test-server address this sandbox can
reach, and no external network access is available to this suite. That
full-stack path is already covered against a *directly constructed*
MediaAcquirer/Worker (not through this entrypoint) by
tests/test_matching_handler.py and benchmarks/bench_pipeline.py.
"""
from __future__ import annotations

import os
import signal as signal_module
import socket
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from redis.exceptions import ResponseError

from tests.conftest import TEST_REDIS_URL
from work_queue.keys import CONSUMER_GROUP, stream_key
from worker.main import (
    ConfigError,
    WorkerConfig,
    build_engine,
    build_worker,
    install_shutdown_handlers,
)

REPO_ROOT = Path(__file__).parent.parent


# ---------------------------------------------------------------------------
# 1. Configuration parsing / defaults
# ---------------------------------------------------------------------------


def test_config_defaults_when_env_is_empty():
    config = WorkerConfig.from_env({})

    assert config.redis_url == "redis://localhost:6379/0"
    assert config.consumer_name is None
    assert config.lease_ms == 30_000
    assert config.block_ms == 5_000
    assert config.reclaim_interval_ms is None
    assert config.max_attempts is None
    assert config.embedding_device == "auto"
    assert config.torch_num_threads == 1
    assert config.target_cache_path == "./target_cache"
    assert config.media_max_bytes == 100 * 1024 * 1024


def test_config_overrides_from_env():
    env = {
        "REDIS_URL": "redis://example.invalid:6379/2",
        "WORKER_CONSUMER_NAME": "worker-fixed-1",
        "WORKER_LEASE_MS": "15000",
        "WORKER_BLOCK_MS": "1000",
        "WORKER_RECLAIM_INTERVAL_MS": "20000",
        "WORKER_MAX_ATTEMPTS": "5",
        "EMBEDDING_DEVICE": "cpu",
        "TORCH_NUM_THREADS": "4",
        "TARGET_CACHE_PATH": "/var/lib/fingerprinter/targets",
        "MEDIA_MAX_BYTES": "1048576",
    }

    config = WorkerConfig.from_env(env)

    assert config.redis_url == "redis://example.invalid:6379/2"
    assert config.consumer_name == "worker-fixed-1"
    assert config.lease_ms == 15000
    assert config.block_ms == 1000
    assert config.reclaim_interval_ms == 20000
    assert config.max_attempts == 5
    assert config.embedding_device == "cpu"
    assert config.torch_num_threads == 4
    assert config.target_cache_path == "/var/lib/fingerprinter/targets"
    assert config.media_max_bytes == 1048576


# ---------------------------------------------------------------------------
# 2. Invalid configuration -> ConfigError (and main() exits non-zero)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "env",
    [
        {"WORKER_LEASE_MS": "not-an-int"},
        {"WORKER_BLOCK_MS": "0"},
        {"WORKER_LEASE_MS": "-1"},
        {"WORKER_RECLAIM_INTERVAL_MS": "0"},
        {"WORKER_MAX_ATTEMPTS": "0"},
        {"EMBEDDING_DEVICE": "tpu"},
        {"TORCH_NUM_THREADS": "0"},
        {"TORCH_NUM_THREADS": "-3"},
        {"MEDIA_MAX_BYTES": "0"},
        {"REDIS_URL": "not-a-url"},
        {"REDIS_URL": "http://localhost:6379"},
    ],
)
def test_invalid_config_raises_config_error(env):
    with pytest.raises(ConfigError):
        WorkerConfig.from_env(env)


def test_main_returns_nonzero_on_invalid_config(monkeypatch):
    import worker.main as main_module

    monkeypatch.setattr(main_module.os, "environ", {"WORKER_LEASE_MS": "not-an-int"})

    assert main_module.main() == 1


# ---------------------------------------------------------------------------
# 3. torch_num_threads is passed to DINOv2EmbeddingEngine
# ---------------------------------------------------------------------------


def test_torch_num_threads_is_passed_to_engine_constructor(monkeypatch):
    import worker.main as main_module

    captured = {}

    class _FakeEngine:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(main_module, "DINOv2EmbeddingEngine", _FakeEngine)

    config = WorkerConfig.from_env({"TORCH_NUM_THREADS": "7", "EMBEDDING_DEVICE": "cpu"})
    build_engine(config)

    assert captured["torch_num_threads"] == 7
    assert captured["device"] == "cpu"


def test_default_torch_num_threads_is_one_not_unbounded():
    """Guards against silently reverting to torch's own default (untouched
    global thread pool) for a value nobody explicitly configured — the
    exact footgun Phase 13 audit blocker #5 names."""
    config = WorkerConfig.from_env({})
    assert config.torch_num_threads == 1


# ---------------------------------------------------------------------------
# 4/5. Worker identity: explicit honored, default remains valid
# ---------------------------------------------------------------------------


def test_explicit_worker_identity_is_honored(redis_client):
    config = WorkerConfig.from_env({"WORKER_CONSUMER_NAME": "worker-explicit-1"})
    worker = build_worker(redis_client, config)
    assert worker.consumer_name == "worker-explicit-1"


def test_default_worker_identity_matches_existing_pattern(redis_client):
    config = WorkerConfig.from_env({})
    worker = build_worker(redis_client, config)
    assert worker.consumer_name.startswith(f"worker-{socket.gethostname()}-{os.getpid()}-")


# ---------------------------------------------------------------------------
# 6/9. SIGTERM/SIGINT call Worker.stop() and nothing else (does not ack)
# ---------------------------------------------------------------------------


def test_signal_handlers_call_worker_stop_and_only_stop():
    fake_worker = MagicMock()
    original_term = signal_module.getsignal(signal_module.SIGTERM)
    original_int = signal_module.getsignal(signal_module.SIGINT)
    try:
        install_shutdown_handlers(fake_worker)

        term_handler = signal_module.getsignal(signal_module.SIGTERM)
        int_handler = signal_module.getsignal(signal_module.SIGINT)
        term_handler(signal_module.SIGTERM, None)
        int_handler(signal_module.SIGINT, None)

        assert fake_worker.stop.call_count == 2
        fake_worker.ack.assert_not_called()
        fake_worker.commit_result.assert_not_called()
        fake_worker.run.assert_not_called()
    finally:
        signal_module.signal(signal_module.SIGTERM, original_term)
        signal_module.signal(signal_module.SIGINT, original_int)


# ---------------------------------------------------------------------------
# 7. Startup failure returns non-zero, does not hang
# ---------------------------------------------------------------------------


def test_main_returns_nonzero_when_redis_unreachable(monkeypatch):
    import worker.main as main_module

    # Nothing listens here -> connection refused immediately, not a timeout.
    monkeypatch.setattr(
        main_module.os, "environ", {"REDIS_URL": "redis://127.0.0.1:1/0"}
    )

    started = time.monotonic()
    result = main_module.main()
    elapsed = time.monotonic() - started

    assert result == 1
    assert elapsed < 5.0


def test_main_returns_nonzero_and_closes_redis_when_component_construction_fails(monkeypatch, redis_client):
    import worker.main as main_module

    monkeypatch.setattr(main_module.os, "environ", {"REDIS_URL": TEST_REDIS_URL})

    fake_redis = MagicMock()
    fake_redis.ping.return_value = True
    monkeypatch.setattr(main_module, "build_redis_client", lambda config: fake_redis)

    def _boom(config):
        raise RuntimeError("engine boom")

    monkeypatch.setattr(main_module, "build_engine", _boom)

    result = main_module.main()

    assert result == 1
    fake_redis.close.assert_called_once()


# ---------------------------------------------------------------------------
# 8/9. Process-level: a worker can start against real (test) Redis and
# shuts down gracefully on SIGTERM without hanging.
# ---------------------------------------------------------------------------


def _wait_for_consumer_group(redis_client, timeout_s: float = 20.0) -> bool:
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


def test_worker_process_starts_against_real_redis_and_shuts_down_on_sigterm(redis_client, tmp_path):
    proc = subprocess.Popen(
        [sys.executable, "-m", "worker.main"],
        cwd=str(REPO_ROOT),
        env=_subprocess_env(tmp_path),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        started = _wait_for_consumer_group(redis_client)
        assert started, proc.stdout.read() if proc.poll() is not None else "worker never created consumer group"

        proc.send_signal(signal_module.SIGTERM)
        returncode = proc.wait(timeout=15)
        output = proc.stdout.read()

        assert returncode == 0, output
        assert "startup success" in output
        assert "requesting graceful shutdown" in output
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


def test_worker_process_exits_nonzero_fast_when_redis_unreachable(tmp_path):
    proc = subprocess.Popen(
        [sys.executable, "-m", "worker.main"],
        cwd=str(REPO_ROOT),
        env=_subprocess_env(tmp_path, REDIS_URL="redis://127.0.0.1:1/0"),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        returncode = proc.wait(timeout=10)
        output = proc.stdout.read()
        assert returncode == 1, output
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


def test_worker_process_exits_nonzero_on_bad_config(tmp_path):
    proc = subprocess.Popen(
        [sys.executable, "-m", "worker.main"],
        cwd=str(REPO_ROOT),
        env=_subprocess_env(tmp_path, TORCH_NUM_THREADS="0"),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    returncode = proc.wait(timeout=10)
    output = proc.stdout.read()
    assert returncode == 1, output
    assert "invalid configuration" in output
