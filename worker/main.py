"""Production worker process entrypoint (Phase 13B).

Turns `worker.fingerprint_worker.Worker` — a well-tested library class with
no process wrapper (Phase 13 audit, §2, "PRODUCTION BLOCKER") — into an
actually runnable process: parse configuration from the environment,
connect to Redis, construct the production pipeline in the same dependency
order Phase 12 already established (Redis -> MediaAcquirer ->
TargetRegistry -> DINOv2EmbeddingEngine -> matching handler -> Worker),
wire SIGTERM/SIGINT to `Worker.stop()`, run until shutdown, and exit.

Run with:

    python -m worker.main

See docs/architecture/phase-13-production-hardening.md, "Phase 13B", for
the full configuration reference, startup sequence, and CPU-sizing
guidance this module implements.
"""
from __future__ import annotations

import logging
import os
import signal
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
from work_queue.keys import CONSUMER_GROUP, stream_key
from worker.fingerprint_worker import Worker
from worker.matching_handler import build_matching_handler

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
    media_max_bytes: int = DEFAULT_MAX_BYTES

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
            media_max_bytes=_getenv_int("MEDIA_MAX_BYTES", DEFAULT_MAX_BYTES, env),
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


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


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


def build_registry(redis_client: Redis, config: WorkerConfig) -> TargetRegistry:
    base = Path(config.target_cache_path)
    pooled_cache = FilesystemEmbeddingCache(base / "pooled")
    segment_cache = FilesystemSegmentEmbeddingCache(base / "segments")
    return TargetRegistry(redis_client, pooled_cache, segment_cache)


def build_engine(config: WorkerConfig) -> DINOv2EmbeddingEngine:
    return DINOv2EmbeddingEngine(
        device=config.embedding_device,
        torch_num_threads=config.torch_num_threads,
    )


def build_worker(redis_client: Redis, config: WorkerConfig) -> Worker:
    return Worker(
        redis_client,
        consumer_name=config.consumer_name,
        block_ms=config.block_ms,
        lease_ms=config.lease_ms,
        reclaim_interval_ms=config.reclaim_interval_ms,
    )


def install_shutdown_handlers(worker: Worker) -> None:
    """SIGTERM/SIGINT both request Worker.stop() — same graceful-shutdown
    semantics either way, exactly as tested by
    tests/test_crash_recovery.py::test_graceful_shutdown_does_not_ack_unfinished_work.
    Never calls anything more forceful; the process exits when run()
    returns, not from inside the handler.
    """

    def _handle_signal(signum, _frame):
        logger.info("received %s, requesting graceful shutdown", signal.Signals(signum).name)
        worker.stop()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)


def main() -> int:
    configure_logging()

    try:
        config = WorkerConfig.from_env()
    except ConfigError as exc:
        logger.error("invalid configuration: %s", exc)
        return 1

    if config.max_attempts is not None:
        logger.warning(
            "WORKER_MAX_ATTEMPTS=%d is set but has no effect: max_attempts is a per-job field "
            "set by the producer at job submission time (work_queue.jobs.Job.max_attempts), not "
            "a worker-level setting. Accepted here for configuration-surface parity only.",
            config.max_attempts,
        )

    logger.info(
        "starting fingerprinter worker: redis=%s device=%s torch_num_threads=%d "
        "lease_ms=%d block_ms=%d reclaim_interval_ms=%s media_max_bytes=%d target_cache_path=%s",
        _redact_redis_url(config.redis_url),
        config.embedding_device,
        config.torch_num_threads,
        config.lease_ms,
        config.block_ms,
        config.reclaim_interval_ms,
        config.media_max_bytes,
        config.target_cache_path,
    )

    try:
        redis_client = build_redis_client(config)
    except RedisError as exc:
        logger.error("could not connect to Redis at %s: %s", _redact_redis_url(config.redis_url), exc)
        return 1

    try:
        acquirer = build_acquirer(config)
        registry = build_registry(redis_client, config)
        engine = build_engine(config)
        handler = build_matching_handler(acquirer, engine, registry)
        worker = build_worker(redis_client, config)
    except Exception as exc:  # noqa: BLE001 - deliberately broad: any startup failure must exit non-zero, not hang
        logger.error("fatal startup error (%s): %s", type(exc).__name__, exc)
        redis_client.close()
        return 1

    install_shutdown_handlers(worker)
    logger.info(
        "worker %s started (group=%s stream=%s) - startup success",
        worker.consumer_name,
        CONSUMER_GROUP,
        stream_key(),
    )

    try:
        worker.run(handler)
    finally:
        logger.info("worker %s exiting", worker.consumer_name)
        redis_client.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
