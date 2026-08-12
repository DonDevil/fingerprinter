"""Phase 13C — production observability for `worker/main.py`.

Turns `worker.fingerprint_worker.Worker`'s existing, unmodified lifecycle
boundaries (claim / reclaim / reject / complete / fail / retry-schedule /
permanently-fail, plus `worker/matching_handler.py`'s existing per-stage
call sequence) into structured logs, process-local counters, bounded
latency stats, a periodic health summary, and a machine-readable run
record — without changing any claim/lease/retry/commit semantics.

Design (see docs/architecture/phase-13-production-hardening.md, "Phase
13C", for the full reasoning):

- Standard library only. `psutil` is not a dependency of this project
  (`pip show psutil` -> not installed, checked before writing this module)
  and the stdlib (`resource`, `/proc/self/status`, `threading`) is
  sufficient for what's needed here, so no new dependency was added.
- Everything here is worker-local (this process only). Fleet-wide
  aggregation is explicitly out of scope (Phase 13C task brief §13) — the
  schema is shaped to make a later aggregation step straightforward
  (every record carries hostname/pid/consumer_name), not to perform one.
- `ObservingWorkerObserver` is the only stateful/mutating piece. It
  implements `worker.fingerprint_worker.WorkerObserver`'s no-op interface,
  so `Worker` never imports this module — the dependency points one way.
- Redis reads here are bounded: XLEN, XINFO GROUPS/CONSUMERS (data Redis
  already maintains), and a `count=1` XPENDING range read for the oldest
  pending entry's idle time. No SCAN, no per-job walk, no new Redis data
  structure (task brief §6).
"""
from __future__ import annotations

import collections
import contextlib
import json
import logging
import os
import resource
import socket
import sys
import tempfile
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, Optional

from redis import Redis
from redis.exceptions import RedisError

from work_queue.jobs import Job
from worker.fingerprint_worker import WorkerObserver

# Fixed key-prefix namespace this whole project uses (work_queue/keys.py,
# target/keys.py) — not independently configurable today, so this is a
# constant, not an env var, kept here purely for the identity fields §1/§13
# require on every structured event.
NAMESPACE = "fingerprint"

RUN_RECORD_SCHEMA_VERSION = 1

DEFAULT_OBSERVABILITY_INTERVAL_MS = 60_000

# Bounded per-metric sample retention (§3: "do not store every latency
# sample indefinitely"). 2000 samples is enough for stable p50/p95/p99 at
# the throughput this project's own benchmarks measured (~1 job/s/host,
# phase-11 doc) while staying a small, fixed amount of memory regardless
# of worker uptime.
_MAX_LATENCY_SAMPLES = 2000


# ---------------------------------------------------------------------------
# Structured JSON logging
# ---------------------------------------------------------------------------


class JsonFormatter(logging.Formatter):
    """One JSON object per log line. Extends stdlib `logging` (still the
    project's only logging stack — nothing here replaces it), rather than
    introducing a separate logging framework, per the task brief's "extend,
    not replace" instruction.

    `record.getMessage()` (the normal interpolated log text) is always
    preserved under the `message` key — existing log call sites that were
    already human-readable strings (e.g. `worker/main.py`'s startup/
    shutdown lines) keep their exact text there, so any code or operator
    grepping for that substring still finds it inside the JSON line.
    Structured context passed via `extra={"event": ..., "fields": {...}}`
    (see `log_event` below) is merged in under `event` and the top level.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            ),
            "level": record.levelname,
            "logger": record.name,
            "event": getattr(record, "event", record.name),
            "message": record.getMessage(),
        }
        fields = getattr(record, "fields", None)
        if fields:
            payload.update(fields)
        if record.exc_info and record.exc_info[0] is not None:
            # Exception type only — never the repeated full traceback (§1:
            # "Do NOT log ... full exception tracebacks repeatedly for the
            # same event").
            payload["exc_type"] = record.exc_info[0].__name__
        return json.dumps(payload, default=str)


def configure_json_logging(level: int = logging.INFO) -> None:
    """Install `JsonFormatter` on the root logger. Replaces whatever
    handlers were previously attached (mirrors `logging.basicConfig`'s own
    "only do this once, at process startup" contract) — safe to call
    exactly once, from `worker/main.py`'s `main()`.
    """
    root = logging.getLogger()
    root.setLevel(level)
    for handler in list(root.handlers):
        root.removeHandler(handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(JsonFormatter())
    root.addHandler(stream_handler)


def log_event(
    logger_: logging.Logger,
    event: str,
    message: Optional[str] = None,
    level: int = logging.INFO,
    **fields,
) -> None:
    """Emit one structured event. `message` defaults to `event` itself;
    pass an explicit human-readable `message` to preserve existing
    free-text log wording (see `worker/main.py`) while still tagging the
    line with a consistent machine-readable `event` name.
    """
    logger_.log(level, message if message is not None else event, extra={"event": event, "fields": fields})


# ---------------------------------------------------------------------------
# Worker identity (§13 — every record must be attributable to one process)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkerIdentity:
    hostname: str
    pid: int
    consumer_name: str
    namespace: str = NAMESPACE

    @classmethod
    def build(cls, consumer_name: str) -> "WorkerIdentity":
        return cls(hostname=socket.gethostname(), pid=os.getpid(), consumer_name=consumer_name)

    def as_fields(self) -> Dict[str, object]:
        return {
            "worker_id": self.consumer_name,
            "hostname": self.hostname,
            "pid": self.pid,
            "consumer_name": self.consumer_name,
            "namespace": self.namespace,
        }


# ---------------------------------------------------------------------------
# Counters (§2)
# ---------------------------------------------------------------------------


@dataclass
class WorkerCounters:
    jobs_claimed: int = 0
    jobs_reclaimed: int = 0
    jobs_completed: int = 0
    jobs_failed: int = 0
    jobs_rejected: int = 0
    jobs_retried: int = 0
    jobs_permanently_failed: int = 0
    active_jobs: int = 0
    total_job_attempts: int = 0


# ---------------------------------------------------------------------------
# Latency (§3/§4) — bounded sample retention, min/max/avg + percentiles
# ---------------------------------------------------------------------------


class BoundedLatencyStats:
    """count/min/max/avg exactly; p50/p95/p99 approximated from a bounded
    (`maxlen=_MAX_LATENCY_SAMPLES`), most-recent-N sample window — not
    every sample ever recorded (§3). Process-local, resets when the worker
    (re)starts; never claims to represent fleet-wide latency.
    """

    def __init__(self, maxlen: int = _MAX_LATENCY_SAMPLES):
        self._samples: "collections.deque[float]" = collections.deque(maxlen=maxlen)
        self.count = 0
        self.min: Optional[float] = None
        self.max: Optional[float] = None
        self._sum = 0.0

    def add(self, value: float) -> None:
        self.count += 1
        self._sum += value
        self.min = value if self.min is None else min(self.min, value)
        self.max = value if self.max is None else max(self.max, value)
        self._samples.append(value)

    def _percentile(self, p: float) -> Optional[float]:
        if not self._samples:
            return None
        ordered = sorted(self._samples)
        idx = min(len(ordered) - 1, int(round(p * (len(ordered) - 1))))
        return ordered[idx]

    def snapshot(self) -> Dict[str, Optional[float]]:
        return {
            "count": self.count,
            "min": self.min,
            "max": self.max,
            "avg": round(self._sum / self.count, 3) if self.count else None,
            "p50": self._percentile(0.50),
            "p95": self._percentile(0.95),
            "p99": self._percentile(0.99),
            # Sample window may be smaller than `count` once bounded — make
            # that explicit rather than implying every sample informs the
            # percentiles above.
            "sample_window": len(self._samples),
        }


# ---------------------------------------------------------------------------
# Error classification (§5)
# ---------------------------------------------------------------------------

# Maps a caught exception's *type name* (never its message — messages from
# acquisition.errors can embed the candidate media URL, see
# acquisition/acquirer.py, and must never reach logs/metrics) onto one of
# the meaningful categories this pipeline actually distinguishes. Anything
# absent from this table is genuinely unclassified, not silently guessed.
_ERROR_CATEGORY_MAP = {
    # acquisition/errors.py — permanent
    "UnsupportedSchemeError": "permanent_acquisition_failure",
    "RedirectLimitExceededError": "permanent_acquisition_failure",
    "UnsafeDestinationError": "permanent_acquisition_failure",
    "NotFoundError": "permanent_acquisition_failure",
    "GoneError": "permanent_acquisition_failure",
    "ClientError": "permanent_acquisition_failure",
    "UnexpectedStatusError": "permanent_acquisition_failure",
    "UnsupportedContentTypeError": "permanent_acquisition_failure",
    "SizeLimitExceededError": "permanent_acquisition_failure",
    "PermanentAcquisitionError": "permanent_acquisition_failure",
    # acquisition/errors.py — transient
    "ConnectionTimeoutError": "transient_acquisition_failure",
    "ReadTimeoutError": "transient_acquisition_failure",
    "NetworkError": "transient_acquisition_failure",
    "RateLimitedError": "transient_acquisition_failure",
    "ServerError": "transient_acquisition_failure",
    "TransientAcquisitionError": "transient_acquisition_failure",
    # media validation
    "InvalidMediaError": "media_validation_failure",
    "UnsupportedMediaError": "media_validation_failure",
    # embedding/errors.py
    "InferenceError": "embedding_failure",
    "ModelLoadError": "embedding_failure",
    "DeviceUnavailableError": "embedding_failure",
    # job/routing configuration problems
    "KeyError": "malformed_job",  # unknown target_id/version (matching_handler._resolve_target_segments)
    "UnsupportedTechnique": "malformed_job",  # job.techniques names nothing this worker implements
    "JobValidationError": "malformed_job",  # schema-invalid stream entry
    # target build-lock contention (Redis-backed, target/lock.py)
    "TimeoutError": "redis_coordination_failure",
}


def classify_error_type(error_type: str) -> str:
    return _ERROR_CATEGORY_MAP.get(error_type, "unclassified_error")


# ---------------------------------------------------------------------------
# Process resource metrics (§7) — stdlib only, see module docstring
# ---------------------------------------------------------------------------


def _read_current_rss_bytes() -> Optional[int]:
    """Current RSS from /proc/self/status (Linux only — this project's only
    deployment target per the current environment). Returns None rather
    than guessing on any other platform or read failure."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        return None
    return None


def _read_open_fd_count() -> Optional[int]:
    try:
        return len(os.listdir("/proc/self/fd"))
    except OSError:
        return None


class ResourceSampler:
    """Process CPU/RSS/thread/fd sampling. Deliberately keeps two distinct
    numbers that are easy to conflate (task brief §7, "this distinction was
    important in the crawler benchmark work and must remain correct here"):

    - `cpu_time_s`: cumulative process CPU time (user+sys) since process
      start, from `resource.getrusage(RUSAGE_SELF)`. Monotonically
      increasing. NOT a percentage.
    - `cpu_percent`: instantaneous utilization, the CPU-time delta over the
      wall-clock delta between two consecutive `sample()` calls. `None` on
      the first sample (no prior point to diff against) — never fabricated.
    """

    def __init__(self):
        self._peak_rss_bytes = 0
        self._prev_cpu_time_s: Optional[float] = None
        self._prev_wall_s: Optional[float] = None

    def sample(self) -> Dict[str, object]:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        cpu_time_s = usage.ru_utime + usage.ru_stime
        # ru_maxrss is KB on Linux, bytes on macOS. This project runs on
        # Linux (see environment); documented rather than silently assumed.
        peak_rss_bytes = usage.ru_maxrss if sys.platform == "darwin" else usage.ru_maxrss * 1024
        self._peak_rss_bytes = max(self._peak_rss_bytes, peak_rss_bytes)

        wall_s = time.monotonic()
        cpu_percent = None
        if self._prev_cpu_time_s is not None and self._prev_wall_s is not None:
            wall_delta = wall_s - self._prev_wall_s
            if wall_delta > 0:
                cpu_percent = round(((cpu_time_s - self._prev_cpu_time_s) / wall_delta) * 100, 1)
        self._prev_cpu_time_s = cpu_time_s
        self._prev_wall_s = wall_s

        return {
            "rss_current_bytes": _read_current_rss_bytes(),
            "rss_peak_bytes": self._peak_rss_bytes,
            "cpu_time_s": round(cpu_time_s, 3),
            "cpu_percent": cpu_percent,
            "thread_count": threading.active_count(),
            "open_fds": _read_open_fd_count(),
        }


# ---------------------------------------------------------------------------
# Redis queue/PEL health snapshot (§6)
# ---------------------------------------------------------------------------


def redis_health_snapshot(redis_client: Redis, stream: str, group: str, consumer_name: str) -> Dict[str, object]:
    """Cheap, bounded Redis Streams introspection: XLEN, XINFO GROUPS/
    CONSUMERS (data Redis already computes for `Worker`'s own
    XAUTOCLAIM/backpressure use, see `integration/backpressure.py`), and a
    `count=1` XPENDING range read for the oldest pending entry's idle time.
    No SCAN, no per-job walk, no new Redis data structure. Safe to call
    periodically during a long-running worker.
    """
    stream_length = redis_client.xlen(stream)

    group_lag: Optional[int] = None
    group_pending = 0
    for g in redis_client.xinfo_groups(stream):
        if g.get("name") != group:
            continue
        lag = g.get("lag")
        group_lag = int(lag) if lag is not None else None
        group_pending = int(g.get("pending") or 0)
        break

    consumer_pending = 0
    for c in redis_client.xinfo_consumers(stream, group):
        if c.get("name") == consumer_name:
            consumer_pending = int(c.get("pending") or 0)
            break

    oldest_pending_idle_ms: Optional[int] = None
    oldest = redis_client.xpending_range(stream, group, min="-", max="+", count=1)
    if oldest:
        idle = oldest[0].get("time_since_delivered")
        oldest_pending_idle_ms = int(idle) if idle is not None else None

    return {
        "stream_length": stream_length,
        "group_lag": group_lag,
        "group_pending": group_pending,
        "consumer_pending": consumer_pending,
        "oldest_pending_idle_ms": oldest_pending_idle_ms,
    }


# ---------------------------------------------------------------------------
# Atomic file writes (§11/§12)
# ---------------------------------------------------------------------------


def _atomic_write_json(path: str, data: dict) -> None:
    """Write `data` to `path` such that a crash mid-write can never leave a
    partial/corrupt file at `path` — write to a temp file in the same
    directory, then `os.replace` (atomic rename on POSIX)."""
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".worker-run-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2, sort_keys=True, default=str)
        os.replace(tmp_path, path)
    except Exception:
        with contextlib.suppress(OSError):
            os.remove(tmp_path)
        raise


def marker_path(run_output_path: str) -> str:
    return f"{run_output_path}.marker"


# ---------------------------------------------------------------------------
# The observer: Worker-facing hooks + counters + logging + health/run record
# ---------------------------------------------------------------------------


class ObservingWorkerObserver(WorkerObserver):
    def __init__(
        self,
        identity: WorkerIdentity,
        redis_client: Redis,
        stream: str,
        group: str,
        logger: logging.Logger,
        health_interval_s: float = DEFAULT_OBSERVABILITY_INTERVAL_MS / 1000,
    ):
        self.identity = identity
        self._redis = redis_client
        self._stream = stream
        self._group = group
        self._logger = logger
        self._health_interval_s = health_interval_s

        self._lock = threading.Lock()
        self._counters = WorkerCounters()
        self._error_categories: "collections.Counter[str]" = collections.Counter()
        self._latency = {
            "claim_to_completion_ms": BoundedLatencyStats(),
            "claim_to_failure_ms": BoundedLatencyStats(),
        }
        self._stage_latency: Dict[str, BoundedLatencyStats] = collections.defaultdict(BoundedLatencyStats)
        self._resources = ResourceSampler()

        self._started_monotonic = time.monotonic()
        self._started_at = time.time()
        self._last_health_emit = 0.0

    # -- internal helpers ----------------------------------------------

    def _log(self, event: str, level: int = logging.INFO, **fields) -> None:
        merged = {
            **self.identity.as_fields(),
            "stream": self._stream,
            "consumer_group": self._group,
            **fields,
        }
        log_event(self._logger, event, level=level, **merged)

    def _safe_redis_snapshot(self) -> Optional[dict]:
        try:
            return redis_health_snapshot(self._redis, self._stream, self._group, self.identity.consumer_name)
        except RedisError as exc:
            self._log("worker_health", level=logging.WARNING, redis_error=type(exc).__name__)
            return None

    # -- WorkerObserver interface ---------------------------------------

    def on_job_claimed(self, *, job: Job, attempt: int) -> None:
        with self._lock:
            self._counters.jobs_claimed += 1
            self._counters.active_jobs += 1
            self._counters.total_job_attempts += 1
        self._log("job_claimed", job_id=job.job_id, attempt=attempt)

    def on_job_reclaimed(self, *, job: Job, attempt: int) -> None:
        with self._lock:
            self._counters.jobs_reclaimed += 1
            self._counters.active_jobs += 1
            self._counters.total_job_attempts += 1
        self._log("job_reclaimed", job_id=job.job_id, attempt=attempt)

    def on_job_rejected(self, *, entry_id: str, error: str, source: str) -> None:
        with self._lock:
            self._counters.jobs_rejected += 1
            if source == "reclaim":
                # The stream entry genuinely was reclaimed via XAUTOCLAIM
                # before being found malformed — counted here, not via
                # on_job_reclaimed, since it never entered active
                # processing (see class docstring / task brief §2).
                self._counters.jobs_reclaimed += 1
        # Validation errors (Job.from_stream_fields) only ever describe
        # missing/malformed field names — never a media URL — so the
        # message itself is safe to log in full, unlike failure reasons
        # elsewhere in this module.
        self._log("job_rejected", entry_id=entry_id, source=source, error=error)

    def on_job_completed(
        self, *, job: Job, attempt: int, latency_ms: Optional[float], decision: Optional[str] = None
    ) -> None:
        with self._lock:
            self._counters.jobs_completed += 1
            self._counters.active_jobs = max(0, self._counters.active_jobs - 1)
            if latency_ms is not None:
                self._latency["claim_to_completion_ms"].add(latency_ms)
        self._log("job_completed", job_id=job.job_id, attempt=attempt, latency_ms=latency_ms, decision=decision)

    def on_job_failed(self, *, job: Job, attempt: int, error_type: str, latency_ms: Optional[float]) -> None:
        category = classify_error_type(error_type)
        with self._lock:
            self._counters.jobs_failed += 1
            self._counters.active_jobs = max(0, self._counters.active_jobs - 1)
            self._error_categories[category] += 1
            if latency_ms is not None:
                self._latency["claim_to_failure_ms"].add(latency_ms)
        self._log(
            "job_failed", job_id=job.job_id, attempt=attempt, error_type=error_type,
            error_category=category, latency_ms=latency_ms,
        )

    def on_job_retry_scheduled(
        self, *, job: Job, attempt: int, error_type: str, latency_ms: Optional[float]
    ) -> None:
        category = classify_error_type(error_type)
        with self._lock:
            self._counters.jobs_retried += 1
            self._counters.active_jobs = max(0, self._counters.active_jobs - 1)
            self._error_categories[category] += 1
            if latency_ms is not None:
                self._latency["claim_to_failure_ms"].add(latency_ms)
        self._log(
            "job_retry_scheduled", job_id=job.job_id, attempt=attempt, error_type=error_type,
            error_category=category, latency_ms=latency_ms,
        )

    def on_job_permanently_failed(
        self, *, job: Job, attempt: int, error_type: str, latency_ms: Optional[float]
    ) -> None:
        category = classify_error_type(error_type)
        with self._lock:
            self._counters.jobs_permanently_failed += 1
            self._counters.active_jobs = max(0, self._counters.active_jobs - 1)
            self._error_categories[category] += 1
            if latency_ms is not None:
                self._latency["claim_to_failure_ms"].add(latency_ms)
        self._log(
            "job_permanently_failed", job_id=job.job_id, attempt=attempt, error_type=error_type,
            error_category=category, latency_ms=latency_ms,
        )

    def on_loop_tick(self) -> None:
        now = time.monotonic()
        if now - self._last_health_emit < self._health_interval_s:
            return
        self._last_health_emit = now
        self.emit_health_summary()

    # -- read-only snapshots (used by health/run-record above, and by tests) --

    def counters_snapshot(self) -> dict:
        with self._lock:
            return asdict(self._counters)

    def latency_snapshot(self) -> Dict[str, dict]:
        with self._lock:
            return {name: stats.snapshot() for name, stats in self._latency.items()}

    # -- pipeline stage timing (called from worker/matching_handler.py) --

    def record_stage_duration(self, stage: str, duration_s: float) -> None:
        with self._lock:
            self._stage_latency[stage].add(duration_s * 1000)

    # -- periodic health summary (§8) ------------------------------------

    def emit_health_summary(self) -> None:
        redis_snapshot = self._safe_redis_snapshot()
        resources = self._resources.sample()
        with self._lock:
            counters = asdict(self._counters)
            completion_latency = self._latency["claim_to_completion_ms"].snapshot()
        uptime_s = time.monotonic() - self._started_monotonic
        self._log(
            "worker_health",
            uptime_s=round(uptime_s, 1),
            **counters,
            average_job_latency_ms=completion_latency["avg"],
            p95_job_latency_ms=completion_latency["p95"],
            redis=redis_snapshot,
            resources=resources,
        )

    # -- shutdown summary + machine-readable run record (§10/§11) -------

    def build_run_record(self, *, configuration: dict, shutdown_reason: str, clean: bool) -> dict:
        redis_snapshot = self._safe_redis_snapshot()
        resources = self._resources.sample()
        with self._lock:
            counters = asdict(self._counters)
            latency = {name: stats.snapshot() for name, stats in self._latency.items()}
            stage_metrics = {name: stats.snapshot() for name, stats in self._stage_latency.items()}
            error_categories = dict(self._error_categories)
        ended_at = time.time()
        uptime_s = time.monotonic() - self._started_monotonic
        return {
            "metadata": {
                **self.identity.as_fields(),
                "stream": self._stream,
                "consumer_group": self._group,
                "schema_version": RUN_RECORD_SCHEMA_VERSION,
            },
            "configuration": configuration,
            "timing": {
                "started_at": self._started_at,
                "ended_at": ended_at,
                "uptime_s": round(uptime_s, 3),
            },
            "counters": counters,
            "error_categories": error_categories,
            "latency": latency,
            "pipeline_stage_metrics": stage_metrics,
            "redis": redis_snapshot,
            "resources": resources,
            "shutdown": {"reason": shutdown_reason, "clean": clean},
        }

    def emit_shutdown_summary(self, *, shutdown_reason: str, clean: bool) -> None:
        with self._lock:
            counters = asdict(self._counters)
            completion_latency = self._latency["claim_to_completion_ms"].snapshot()
        resources = self._resources.sample()
        uptime_s = time.monotonic() - self._started_monotonic
        self._log(
            "worker_stopped",
            uptime_s=round(uptime_s, 1),
            **counters,
            average_job_latency_ms=completion_latency["avg"],
            p95_job_latency_ms=completion_latency["p95"],
            peak_rss_bytes=resources["rss_peak_bytes"],
            cpu_time_s=resources["cpu_time_s"],
            shutdown_reason=shutdown_reason,
            clean=clean,
        )

    def write_run_record(self, path: str, *, configuration: dict, shutdown_reason: str, clean: bool) -> None:
        """Atomically write the full run record to `path` and remove the
        startup marker (§12) — the marker's absence-after-clean-write and
        presence-after-crash is the whole crash-detection signal; no Redis
        state is involved."""
        record = self.build_run_record(configuration=configuration, shutdown_reason=shutdown_reason, clean=clean)
        _atomic_write_json(path, record)
        with contextlib.suppress(OSError):
            os.remove(marker_path(path))

    def write_startup_marker(self, path: str, configuration: dict) -> None:
        """Lightweight heartbeat (§12): if the process is killed with
        SIGKILL/power loss before it can call `write_run_record`, this
        marker is left behind at `<path>.marker` — an external tool can
        treat "marker exists, no matching clean run record with
        shutdown.clean=true and a newer timestamp" as "worker started but
        did not shut down cleanly". No distributed state, no Redis key —
        just a file, per the task brief's "keep this lightweight"."""
        marker = {
            "metadata": {**self.identity.as_fields(), "stream": self._stream, "consumer_group": self._group},
            "configuration": configuration,
            "started_at": self._started_at,
        }
        _atomic_write_json(marker_path(path), marker)
