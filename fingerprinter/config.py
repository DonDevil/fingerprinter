"""Configuration (design §12.6). Environment: ``FINGERPRINTER_<FIELD>`` and
``FINGERPRINTER_<SECTION>__<FIELD>``; list values are comma-separated.

All defaults are the design's proposed initial values; the ones that need
tuning from measurements are marked in docs (design §22, exit.md).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from fingerprinter.errors import ConfigError

GiB = 1024**3


class _Section(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ScyllaConfig(_Section):
    contact_points: list[str] = ["127.0.0.1"]
    port: int = 9042
    keyspace: str = "fingerprinter"
    replication_factor: int = 1
    local_dc: str | None = None
    request_timeout_s: float = 20.0


class RedisConfig(_Section):
    host: str = "127.0.0.1"
    port: int = 6379
    db: int = 0
    password: str | None = None
    prefix: str = "fp:"
    stream_prefix: str = "events:"
    socket_timeout_s: float = 10.0


class MinioConfig(_Section):
    endpoint: str = "127.0.0.1:9000"
    access_key: str = ""
    secret_key: str = ""
    secure: bool = False
    representations_bucket: str = "fingerprinter-representations"
    targets_bucket: str = "fingerprinter-targets"


class EncoderConfig(_Section):
    device: str = "auto"
    max_batch: int = 256
    cpu_batch: int = 8
    calibration_frames: int = Field(default=64, ge=1)
    """Frames per timed repetition of a GPU calibration candidate (``max(frames, B)``)."""
    calibration_reps: int = Field(default=3, ge=3)
    """Timed repetitions per candidate (and again for the two confirmed leaders)."""
    calibration_tolerance: float = Field(default=0.05, ge=0.0, lt=0.5)
    """Medians within this fraction of the best count as equal; the smallest B wins."""
    queue_items: int | None = None
    decode_threads: int = 3
    claim_threads: int = 2
    inference_threads: int | None = None
    lease_ms: int = 60_000
    gpu_lease_ms: int = 30_000
    gpu_lease_renew_ms: int = 10_000
    max_attempts: int = 4
    max_claims: int = 10
    attempt_timeout_s: float = 14_400.0
    attempt_lease_s: float = 600.0
    attempt_lease_renew_s: float = 120.0
    max_rss_bytes: int = 4 * GiB
    shutdown_grace_s: float = 60.0
    backoff_base_s: float = 600.0
    stable_after: int = 500
    poll_interval_s: float = 1.0
    local_files_only: bool = True


class AcquireConfig(_Section):
    max_bytes: int = 8 * GiB
    timeout_s: float = 3600.0
    connect_timeout_s: float = 10.0
    read_timeout_s: float = 60.0
    max_redirects: int = 5
    user_agent: str = "fingerprinter/2 (+anti-piracy research)"
    ssrf_allow_cidrs: list[str] = []
    """Dev/test only: networks exempt from the SSRF guard (e.g. a loopback fixture
    server). Empty in production; never settable from an event."""


class IntakeConfig(_Section):
    reader: str = "encode-requested"
    lease_ttl_s: float = 60.0
    lease_renew_s: float = 20.0
    batch: int = 200
    poll_interval_s: float = 1.0
    replay_margin_s: float = 86_400.0
    crawler2_outbox_retention_s: float = 1_209_600.0


class PublisherConfig(_Section):
    stream_max_bytes: int = 64 * 1024**2
    hold_alarm_s: float = 3600.0


class ReconcilerConfig(_Section):
    intent_grace_s: float = 120.0
    interval_s: float = 60.0
    full_pass_interval_s: float = 86_400.0


class BackfillConfig(_Section):
    priority: int = 10
    max_in_flight: int = 200
    max_per_hour: int | None = None
    queue_high_water: int = 1000
    buckets: int = 64


class Settings(_Section):
    host_id: str = "host-1"
    roles: list[str] = ["intake", "scheduler", "encoder", "reconciler"]
    scratch_dir: Path = Path("var/scratch")
    scratch_max_bytes: int = 20 * GiB
    orphan_grace_s: float = 86_400.0
    retire_grace_s: float = 604_800.0
    model_cache_dir: Path | None = None
    metrics_port: int | None = None
    """Serve Prometheus metrics on this port (``/metrics``) from long-running roles."""
    scylla: ScyllaConfig = ScyllaConfig()
    redis: RedisConfig = RedisConfig()
    minio: MinioConfig = MinioConfig()
    encoder: EncoderConfig = EncoderConfig()
    acquire: AcquireConfig = AcquireConfig()
    intake: IntakeConfig = IntakeConfig()
    publisher: PublisherConfig = PublisherConfig()
    reconciler: ReconcilerConfig = ReconcilerConfig()
    backfill: BackfillConfig = BackfillConfig()
    due_shards: int = Field(default=16, ge=1, le=256)
    intent_shards: int = Field(default=16, ge=1, le=256)

    @model_validator(mode="after")
    def _grace_exceeds_timeout(self) -> Settings:
        # design §7.2/§8.5: an object of a still-running attempt must never look orphaned.
        if self.orphan_grace_s <= self.encoder.attempt_timeout_s:
            raise ValueError(
                f"orphan_grace_s ({self.orphan_grace_s}) must exceed "
                f"encoder.attempt_timeout_s ({self.encoder.attempt_timeout_s})"
            )
        if self.encoder.attempt_lease_renew_s >= self.encoder.attempt_lease_s:
            raise ValueError("encoder.attempt_lease_renew_s must be below attempt_lease_s")
        return self

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None, **overrides: Any) -> Settings:
        env = os.environ if environ is None else environ
        data: dict[str, Any] = {}
        for key, value in env.items():
            if not key.startswith("FINGERPRINTER_"):
                continue
            path = key[len("FINGERPRINTER_") :].lower().split("__")
            if len(path) > 2:
                continue
            node = data
            for part in path[:-1]:
                node = node.setdefault(part, {})
            node[path[-1]] = _value(cls, path, value)
        for key, value in overrides.items():
            if isinstance(value, dict):
                data.setdefault(key, {}).update(value)
            else:
                data[key] = value
        try:
            return cls.model_validate(data)
        except ValueError as exc:
            raise ConfigError(str(exc)) from exc


def _value(cls: type[BaseModel], path: list[str], raw: str) -> Any:
    model: Any = cls
    for part in path[:-1]:
        field = model.model_fields.get(part)
        if field is None:
            return raw
        model = field.annotation
    field = model.model_fields.get(path[-1]) if hasattr(model, "model_fields") else None
    if field is not None and "list" in str(field.annotation):
        return [v.strip() for v in raw.split(",") if v.strip()]
    return raw
