"""Production worker process entrypoint (Phase 13B, observability in Phase
13C).

Turns `worker.fingerprint_worker.Worker` — a well-tested library class with
no process wrapper (Phase 13 audit, §2, "PRODUCTION BLOCKER") — into an
actually runnable process: parse configuration from the environment,
connect to Redis, construct the production pipeline in the same dependency
order Phase 12 already established (Redis -> MediaAcquirer ->
TargetRegistry -> DINOv2EmbeddingEngine -> matching handler -> Worker),
wire SIGTERM/SIGINT to `Worker.stop()`, run until shutdown, and exit.

Phase 13C adds structured JSON logging, process-local counters/latency,
periodic health summaries, a startup configuration snapshot, and an
optional machine-readable run record — see `worker/observability.py` and
docs/architecture/phase-13-production-hardening.md, "Phase 13C" — without
changing any of the above dependency wiring or claim/lease/retry semantics.

Run with:

    python -m worker.main

See docs/architecture/phase-13-production-hardening.md, "Phase 13B"/"Phase
13C", for the full configuration reference, startup sequence, and
observability schema this module implements.
"""
from __future__ import annotations

import contextlib
import logging
import os
import signal
import socket
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional
from urllib.parse import urlparse, urlunparse

from redis import Redis
from redis.exceptions import RedisError

from acquisition import MediaAcquirer
from acquisition.acquirer import DEFAULT_MAX_BYTES
from embedding.dinov2_engine import DINOv2EmbeddingEngine
from target.cache import FilesystemEmbeddingCache
from target.registry import TargetRegistry
from target.segment_cache import FilesystemSegmentEmbeddingCache
from target.shared_cache import SharedFilesystemEmbeddingCache, SharedFilesystemSegmentEmbeddingCache
from target.shared_storage import SharedArtifactStore, SharedTargetMediaStore
from work_queue.keys import CONSUMER_GROUP, stream_key
from worker.fingerprint_worker import Worker, default_consumer_name
from worker.matching_handler import build_matching_handler
from worker.observability import (
    DEFAULT_OBSERVABILITY_INTERVAL_MS,
    NAMESPACE,
    ObservingWorkerObserver,
    WorkerIdentity,
    configure_json_logging,
    log_event,
)

logger = logging.getLogger("worker.main")

DEFAULT_REDIS_URL = "redis://localhost:6379/0"
DEFAULT_LEASE_MS = 30_000
DEFAULT_BLOCK_MS = 5_000
DEFAULT_TARGET_CACHE_PATH = "./target_cache"

# Safe deterministic default: pin this process to a single torch compute
# thread. Phase 11 measured (docs/architecture/phase-11-performance-
# benchmarks.md §19a/§19b) that running multiple worker *processes* on one
# host, each left at torch's own default (physical-core-count) thread
# pool, oversubscribes the host's cores combinatorially — a measured 15x
# per-job slowdown and net-negative scaling at just 4 processes on a
# 6-physical-core machine. Pinning each process to 1 thread instead
# ("isolated-1thread") is the configuration Phase 11 measured as actually
# scaling usefully across multiple processes (0.81 efficiency at 4
# workers). This entrypoint has no visibility into how many other worker
# processes will run on the same host, so it defaults to the safe choice
# rather than the fast-but-dangerous one. A lone worker on an otherwise-
# idle host pays for this safety (§12: measured 3.6x slower per job than
# 6 threads) — an operator who knows exactly one worker process is running
# on a host should override via TORCH_NUM_THREADS.
DEFAULT_TORCH_NUM_THREADS = 1

# Conservative, fixed connection settings — not exposed as env vars to keep
# the configuration surface minimal (per Phase 13B scope). See "Redis
# connection" in the Phase 13B doc section for why these four in
# particular were chosen.
REDIS_SOCKET_CONNECT_TIMEOUT_S = 5.0
REDIS_SOCKET_TIMEOUT_S = 10.0
REDIS_HEALTH_CHECK_INTERVAL_S = 30
REDIS_RETRY_ON_TIMEOUT = True

_VALID_EMBEDDING_DEVICES = ("auto", "cpu", "cuda")  # mirrors embedding.dinov2_engine._VALID_DEVICES


class ConfigError(ValueError):
    """Raised when environment-derived worker configuration is invalid."""


def _getenv_int(name: str, default: int, env: Mapping[str, str]) -> int:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


def _getenv_optional_int(name: str, env: Mapping[str, str]) -> Optional[int]:
    raw = env.get(name)
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


@dataclass(frozen=True)
class WorkerConfig:
    redis_url: str = DEFAULT_REDIS_URL
    consumer_name: Optional[str] = None
    lease_ms: int = DEFAULT_LEASE_MS
    block_ms: int = DEFAULT_BLOCK_MS
    reclaim_interval_ms: Optional[int] = None
    # Accepted and validated for configuration-surface parity only — see
    # "known limitations" in the Phase 13B doc: max_attempts is a per-Job
    # field set by the producer at submission time
    # (work_queue.jobs.Job.max_attempts), not a Worker constructor
    # parameter, so this value is never passed to anything. Logged as a
    # warning at startup if set, so an operator relying on it notices.
    max_attempts: Optional[int] = None
    embedding_device: str = "auto"
    torch_num_threads: int = DEFAULT_TORCH_NUM_THREADS
    target_cache_path: str = DEFAULT_TARGET_CACHE_PATH
    # Phase 13D: unset (the default) keeps the pre-Phase-13D, single-host
    # behavior (FilesystemEmbeddingCache/FilesystemSegmentEmbeddingCache
    # rooted at target_cache_path, host-local, NOT multi-host-safe — see
    # docs/architecture/phase-13d-distributed-target-artifacts.md). Set to
    # a directory on a genuinely shared mount (NFS, cluster filesystem) to
    # enable the shared, multi-host-safe backend. Deliberately independent
    # of target_cache_path/REDIS_URL — this is its own infrastructure
    # dependency, not implied by either.
    shared_artifact_store_path: Optional[str] = None
    media_max_bytes: int = DEFAULT_MAX_BYTES
    # Phase 13C
    observability_interval_ms: int = DEFAULT_OBSERVABILITY_INTERVAL_MS
    run_output: Optional[str] = None

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "WorkerConfig":
        env = os.environ if env is None else env
        config = cls(
            redis_url=env.get("REDIS_URL") or DEFAULT_REDIS_URL,
            consumer_name=env.get("WORKER_CONSUMER_NAME") or None,
            lease_ms=_getenv_int("WORKER_LEASE_MS", DEFAULT_LEASE_MS, env),
            block_ms=_getenv_int("WORKER_BLOCK_MS", DEFAULT_BLOCK_MS, env),
            reclaim_interval_ms=_getenv_optional_int("WORKER_RECLAIM_INTERVAL_MS", env),
            max_attempts=_getenv_optional_int("WORKER_MAX_ATTEMPTS", env),
            embedding_device=env.get("EMBEDDING_DEVICE") or "auto",
            torch_num_threads=_getenv_int("TORCH_NUM_THREADS", DEFAULT_TORCH_NUM_THREADS, env),
            target_cache_path=env.get("TARGET_CACHE_PATH") or DEFAULT_TARGET_CACHE_PATH,
            shared_artifact_store_path=env.get("SHARED_ARTIFACT_STORE_PATH") or None,
            media_max_bytes=_getenv_int("MEDIA_MAX_BYTES", DEFAULT_MAX_BYTES, env),
            observability_interval_ms=_getenv_int(
                "WORKER_OBSERVABILITY_INTERVAL_MS", DEFAULT_OBSERVABILITY_INTERVAL_MS, env
            ),
            run_output=env.get("WORKER_RUN_OUTPUT") or None,
        )
        config.validate()
        return config

    def validate(self) -> None:
        errors = []

        if not self.redis_url or urlparse(self.redis_url).scheme not in ("redis", "rediss", "unix"):
            errors.append(
                f"REDIS_URL must be a redis://, rediss://, or unix:// URL, got {self.redis_url!r}"
            )
        if self.lease_ms <= 0:
            errors.append(f"WORKER_LEASE_MS must be > 0, got {self.lease_ms}")
        if self.block_ms <= 0:
            errors.append(f"WORKER_BLOCK_MS must be > 0, got {self.block_ms}")
        if self.reclaim_interval_ms is not None and self.reclaim_interval_ms <= 0:
            errors.append(f"WORKER_RECLAIM_INTERVAL_MS must be > 0, got {self.reclaim_interval_ms}")
        if self.max_attempts is not None and self.max_attempts < 1:
            errors.append(f"WORKER_MAX_ATTEMPTS must be >= 1, got {self.max_attempts}")
        if self.embedding_device not in _VALID_EMBEDDING_DEVICES:
            errors.append(
                f"EMBEDDING_DEVICE must be one of {_VALID_EMBEDDING_DEVICES}, got {self.embedding_device!r}"
            )
        if self.torch_num_threads < 1:
            errors.append(f"TORCH_NUM_THREADS must be >= 1, got {self.torch_num_threads}")
        if self.media_max_bytes <= 0:
            errors.append(f"MEDIA_MAX_BYTES must be > 0, got {self.media_max_bytes}")
        if self.observability_interval_ms <= 0:
            errors.append(
                f"WORKER_OBSERVABILITY_INTERVAL_MS must be > 0, got {self.observability_interval_ms}"
            )

        if errors:
            raise ConfigError("; ".join(errors))


def _redact_redis_url(url: str) -> str:
    """Strip credentials before this URL ever reaches a log line."""
    parsed = urlparse(url)
    if parsed.username is None and parsed.password is None:
        return url
    netloc = parsed.hostname or ""
    if parsed.port:
        netloc += f":{parsed.port}"
    return urlunparse(parsed._replace(netloc=netloc))


def _redis_db(url: str) -> str:
    db = urlparse(url).path.lstrip("/")
    return db or "0"


def configure_logging() -> None:
    """Structured JSON logs (Phase 13C) — see worker/observability.py's
    JsonFormatter. Every pre-existing log call in this module keeps its
    original human-readable text under the JSON line's "message" key, so
    substring checks written against earlier plain-text output still see
    that text (see JsonFormatter's docstring)."""
    configure_json_logging(level=logging.INFO)


def config_snapshot(config: WorkerConfig, consumer_name: str) -> dict:
    """The effective configuration this worker is actually running with
    (Phase 13C §9) — safe to log and to embed in a run record: Redis
    credentials are stripped, nothing else in this dict can carry a
    secret."""
    return {
        "worker_id": consumer_name,
        "redis_endpoint": _redact_redis_url(config.redis_url),
        "redis_db": _redis_db(config.redis_url),
        "namespace": NAMESPACE,
        "stream": stream_key(),
        "consumer_group": CONSUMER_GROUP,
        "lease_ms": config.lease_ms,
        "block_ms": config.block_ms,
        "reclaim_interval_ms": config.reclaim_interval_ms,
        "embedding_device": config.embedding_device,
        "torch_num_threads": config.torch_num_threads,
        "target_cache_path": config.target_cache_path,
        "shared_artifact_store_path": config.shared_artifact_store_path,
        "media_max_bytes": config.media_max_bytes,
        "observability_interval_ms": config.observability_interval_ms,
        "run_output": config.run_output,
    }


def build_redis_client(config: WorkerConfig) -> Redis:
    """Construct and verify the single Redis client this process owns.

    Every production module downstream (Worker, TargetRegistry) takes an
    already-constructed client — see Phase 13 audit §2, "Redis
    connections". This is the one place a `Redis(...)`/`Redis.from_url(...)`
    call belongs in production code.
    """
    client = Redis.from_url(
        config.redis_url,
        decode_responses=True,
        socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT_S,
        socket_timeout=REDIS_SOCKET_TIMEOUT_S,
        health_check_interval=REDIS_HEALTH_CHECK_INTERVAL_S,
        retry_on_timeout=REDIS_RETRY_ON_TIMEOUT,
    )
    client.ping()
    return client


def build_acquirer(config: WorkerConfig) -> MediaAcquirer:
    return MediaAcquirer(max_bytes=config.media_max_bytes)


def build_media_store(config: WorkerConfig) -> Optional[SharedTargetMediaStore]:
    """Phase 13D: `None` unless `shared_artifact_store_path` is configured
    — see that field's docstring. Raises (does not silently degrade) if
    the configured path is unreachable, matching `build_registry`'s own
    fail-fast behavior — both are caught by `main()`'s existing startup
    error handling."""
    if not config.shared_artifact_store_path:
        return None
    store = SharedArtifactStore(Path(config.shared_artifact_store_path))
    return SharedTargetMediaStore(store)


def build_registry(redis_client: Redis, config: WorkerConfig) -> TargetRegistry:
    """Phase 13D: `shared_artifact_store_path` unset (default) keeps the
    original, single-host-only backend (`FilesystemEmbeddingCache`/
    `FilesystemSegmentEmbeddingCache`, host-local disk under
    `target_cache_path`) exactly as before Phase 13D. Set, it switches to
    the shared, multi-host-safe backend (`SharedFilesystemEmbeddingCache`/
    `SharedFilesystemSegmentEmbeddingCache`) rooted at that path instead —
    which MUST be a genuinely shared mount across every worker host, or
    this only looks fixed (see `target/shared_storage.py`'s module
    docstring and docs/architecture/phase-13d-distributed-target-
    artifacts.md). If that path can't be created/accessed,
    `SharedArtifactStore.__init__` raises `SharedArtifactStoreError`
    immediately — a worker that can't reach its shared storage fails to
    start rather than silently falling back to a host-local cache and
    claiming to be distributed (audit §11)."""
    if config.shared_artifact_store_path:
        store = SharedArtifactStore(Path(config.shared_artifact_store_path))
        media_store = SharedTargetMediaStore(store)
        pooled_cache = SharedFilesystemEmbeddingCache(store, prefix="pooled")
        segment_cache = SharedFilesystemSegmentEmbeddingCache(store, prefix="segments")
        return TargetRegistry(redis_client, pooled_cache, segment_cache, media_store=media_store)

    base = Path(config.target_cache_path)
    pooled_cache = FilesystemEmbeddingCache(base / "pooled")
    segment_cache = FilesystemSegmentEmbeddingCache(base / "segments")
    return TargetRegistry(redis_client, pooled_cache, segment_cache)


def build_engine(config: WorkerConfig) -> DINOv2EmbeddingEngine:
    return DINOv2EmbeddingEngine(
        device=config.embedding_device,
        torch_num_threads=config.torch_num_threads,
    )


def build_worker(
    redis_client: Redis, config: WorkerConfig, observer: Optional[ObservingWorkerObserver] = None
) -> Worker:
    return Worker(
        redis_client,
        consumer_name=config.consumer_name,
        block_ms=config.block_ms,
        lease_ms=config.lease_ms,
        reclaim_interval_ms=config.reclaim_interval_ms,
        observer=observer,
    )


def install_shutdown_handlers(worker: Worker) -> None:
    """SIGTERM/SIGINT both request Worker.stop() — same graceful-shutdown
    semantics either way, exactly as tested by
    tests/test_crash_recovery.py::test_graceful_shutdown_does_not_ack_unfinished_work.
    Never calls anything more forceful; the process exits when run()
    returns, not from inside the handler.
    """

    def _handle_signal(signum, _frame):
        log_event(
            logger, "worker_shutdown_requested",
            message=f"received {signal.Signals(signum).name}, requesting graceful shutdown",
            signal=signal.Signals(signum).name,
        )
        worker.stop()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)


def main() -> int:
    configure_logging()
    hostname_pid_fields = {"hostname": socket.gethostname(), "pid": os.getpid()}

    try:
        config = WorkerConfig.from_env()
    except ConfigError as exc:
        log_event(
            logger, "worker_fatal_error", message=f"invalid configuration: {exc}",
            level=logging.ERROR, reason="invalid_configuration", **hostname_pid_fields,
        )
        return 1

    if config.max_attempts is not None:
        logger.warning(
            "WORKER_MAX_ATTEMPTS=%d is set but has no effect: max_attempts is a per-job field "
            "set by the producer at job submission time (work_queue.jobs.Job.max_attempts), not "
            "a worker-level setting. Accepted here for configuration-surface parity only.",
            config.max_attempts,
        )

    consumer_name = config.consumer_name or default_consumer_name()
    snapshot = config_snapshot(config, consumer_name)
    log_event(
        logger, "worker_started",
        message=(
            f"starting fingerprinter worker: redis={snapshot['redis_endpoint']} "
            f"device={snapshot['embedding_device']} torch_num_threads={snapshot['torch_num_threads']} "
            f"lease_ms={snapshot['lease_ms']} block_ms={snapshot['block_ms']} "
            f"reclaim_interval_ms={snapshot['reclaim_interval_ms']} media_max_bytes={snapshot['media_max_bytes']} "
            f"target_cache_path={snapshot['target_cache_path']}"
        ),
        configuration=snapshot,
    )

    try:
        redis_client = build_redis_client(config)
    except RedisError as exc:
        log_event(
            logger, "worker_fatal_error",
            message=f"could not connect to Redis at {snapshot['redis_endpoint']}: {exc}",
            level=logging.ERROR, reason="redis_unreachable", error_type=type(exc).__name__,
            **hostname_pid_fields,
        )
        return 1

    identity = WorkerIdentity.build(consumer_name)
    observer = ObservingWorkerObserver(
        identity, redis_client, stream_key(), CONSUMER_GROUP, logger,
        health_interval_s=config.observability_interval_ms / 1000,
    )

    try:
        acquirer = build_acquirer(config)
        registry = build_registry(redis_client, config)
        media_store = build_media_store(config)
        engine = build_engine(config)
        handler = build_matching_handler(
            acquirer, engine, registry, stage_recorder=observer.record_stage_duration, media_store=media_store
        )
        worker = build_worker(redis_client, config, observer=observer)
    except Exception as exc:  # noqa: BLE001 - deliberately broad: any startup failure must exit non-zero, not hang
        log_event(
            logger, "worker_fatal_error",
            message=f"fatal startup error ({type(exc).__name__}): {exc}",
            level=logging.ERROR, reason="component_construction_failed", error_type=type(exc).__name__,
        )
        if config.run_output:
            with contextlib.suppress(Exception):
                observer.write_run_record(
                    config.run_output, configuration=snapshot,
                    shutdown_reason="fatal_startup_error", clean=False,
                )
        redis_client.close()
        return 1

    if config.run_output:
        with contextlib.suppress(Exception):
            observer.write_startup_marker(config.run_output, snapshot)

    install_shutdown_handlers(worker)
    log_event(
        logger, "worker_ready",
        message=(
            f"worker {worker.consumer_name} started (group={CONSUMER_GROUP} stream={stream_key()}) "
            "- startup success"
        ),
    )

    shutdown_reason = "graceful_shutdown"
    try:
        worker.run(handler)
    finally:
        observer.emit_shutdown_summary(shutdown_reason=shutdown_reason, clean=True)
        if config.run_output:
            with contextlib.suppress(Exception):
                observer.write_run_record(
                    config.run_output, configuration=snapshot,
                    shutdown_reason=shutdown_reason, clean=True,
                )
        redis_client.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
