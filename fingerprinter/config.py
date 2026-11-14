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


class IndexConfig(_Section):
    """P10 vector index (design §4, §12, §13.3). Values are the design's provisional ones."""

    url: str = "http://127.0.0.1:16333"
    api_key: str | None = None
    timeout_s: float = Field(default=10.0, gt=0)
    instance: str = "dev-1"
    """Key of the capacity ledger: one Qdrant instance (all specs, roles, generations)."""
    collection_prefix: str = Field(default="fp", pattern=r"^[a-z][a-z0-9]{0,15}$")
    """Namespace of this deployment's collections in the instance (N-14): unique per keyspace."""
    max_points: int = Field(default=250_000, gt=0)
    """Resident points per instance. Provisional 250,000 until E-R1 measures a value (P10-D14)."""
    max_points_evidence: str | None = None
    """E-R1 report id that measured ``max_points``; required for any other value (CAP-8)."""
    capacity_margin_points: int = Field(default=10_000, ge=0)
    rebuild_headroom: float = Field(default=0.05, ge=0, le=1)
    active_ttl_s: float = Field(default=5.0, gt=0)
    # minimum HNSW ef of a search (ef = max(k, search_ef)). With ef = k, queries inside the dense
    # near-duplicate clusters of a much-copied corpus miss their own neighbourhood (19x: clip-segment
    # recall@10 0.932); 384 is the measured knee (0.992, p95 2.2 ms) — validation §13.17
    search_ef: int | None = Field(default=384, ge=1)
    oversampling: float = Field(default=2.0, ge=1)
    upsert_batch: int = Field(default=256, ge=1, le=4096)
    mutation_lease_s: float = Field(default=120.0, gt=0)
    mutation_max_attempts: int = Field(default=8, ge=1)
    clock_margin_s: float = Field(default=5.0, ge=0)
    drain_grace_s: float = Field(default=900.0, gt=0)
    revalidate_after_s: float = Field(default=3600.0, gt=0)
    build_lease_s: float = Field(default=300.0, gt=0)
    verify_cycle_s: float = Field(default=3600.0, gt=0)
    probe_queries: int = Field(default=8, ge=0)


class MatchingConfig(_Section):
    """P10 candidate generation, verification and tasks (design §7.2, §9, §11)."""

    k0: int = Field(default=32, ge=1)
    k_max_a: int = Field(default=256, ge=1)
    # direction B (validation §13.18): a target segment of a much-copied film has more true neighbours
    # above floor_b than 512 (19x corpus: up to 3,208), so 512 ended every run INCOMPLETE; 4096 covers
    # the measured requirement. A floor must never exceed the technique's s_seg (checked at start-up
    # and before every search step).
    k_max_b: int = Field(default=4096, ge=1)
    k_max_eq: int = Field(default=512, ge=1)
    floor_a: float = Field(default=0.60, gt=-1, lt=1)
    floor_b: float = Field(default=0.70, gt=-1, lt=1)
    floor_eq: float = Field(default=0.70, gt=-1, lt=1)
    min_hits: int = Field(default=2, ge=1)
    cap_a: int = Field(default=50, ge=1)
    page_b: int = Field(default=200, ge=1)
    cap_b_total: int = Field(default=5000, ge=1)
    cap_eq_total: int = Field(default=2000, ge=1)
    search_timeout_s: float = Field(default=120.0, gt=0)
    verify_timeout_s: float = Field(default=30.0, gt=0)
    verify_retries: int = Field(default=3, ge=1)
    workers: int = Field(default=2, ge=1, le=64)
    task_lease_s: float = Field(default=300.0, gt=0)
    task_max_attempts: int = Field(default=20, ge=1)
    backoff_base_s: float = Field(default=30.0, gt=0)
    backoff_max_s: float = Field(default=900.0, gt=0)
    technique_version: str = "2.1"
    equivalence_enabled: bool = False
    equivalence_rule: str = "equiv/v1"
    publish_probable: bool = False
    lowinfo_enabled: bool = False
    lowinfo_limit: int = Field(default=256, ge=1)
    lowinfo_threshold: float = Field(default=0.95, gt=0, le=1)
    lowinfo_max_attempts: int = Field(default=5, ge=1)   # refresh failures before BLOCKED (design §7.3)
    artifact_cache_bytes: int = Field(default=256 * 1024**2, ge=0)
    poll_interval_s: float = Field(default=1.0, gt=0)


class MatcherReconcileConfig(_Section):
    """The P10 reconciler (design §6.1, §12.6, §12.10)."""

    interval_s: float = Field(default=60.0, gt=0)
    intent_grace_s: float = Field(default=120.0, ge=0)
    scan_page: int = Field(default=500, ge=1)
    targets_keys_page: int = Field(default=100, ge=2)
    scan_rows_per_s: float = Field(default=200.0, gt=0)
    reps_interval_s: float = Field(default=3600.0, ge=0)
    targets_interval_s: float = Field(default=60.0, ge=0)
    reader_batch: int = Field(default=200, ge=1)
    lease_ttl_s: float = Field(default=60.0, gt=0)
    capacity_audit_s: float = Field(default=60.0, gt=0)
    repair_backoff_base_s: float = Field(default=30.0, gt=0)
    repair_backoff_max_s: float = Field(default=900.0, gt=0)
    repair_max_attempts: int = Field(default=10, ge=1)
    rerun_per_s: float = Field(default=5.0, gt=0)
    verify_page: int = Field(default=512, ge=1)


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
    index: IndexConfig = IndexConfig()
    matching: MatchingConfig = MatchingConfig()
    matcher_reconcile: MatcherReconcileConfig = MatcherReconcileConfig()
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
        _check_p10(self)
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


PROVISIONAL_MAX_POINTS = 250_000


def _check_e_r1_evidence(max_points: int, evidence: str | None) -> None:
    """CAP-8: a limit other than the provisional one only from a **passing** E-R1 report that measured
    at least that many resident points (design §13.4: set to a measured passing total, never raised by
    a failure). ``evidence`` is the report's path; a label alone is not evidence."""
    import json

    if not evidence:
        raise ValueError("index.max_points other than 250000 needs index.max_points_evidence (E-R1 report)")
    try:
        report = json.loads(Path(evidence).expanduser().read_text())
    except (OSError, ValueError) as exc:
        raise ValueError(f"index.max_points_evidence {evidence!r} is not a readable E-R1 report: {exc}") from exc
    measured = report.get("measured_max_resident_total")
    if report.get("benchmark") != "E-R1" or report.get("pass") is not True or report.get("gate_eligible") is not True:
        raise ValueError(f"index.max_points_evidence {evidence!r} is not a passing, gate-eligible E-R1 report")
    if not isinstance(measured, int) or max_points > measured:
        raise ValueError(f"index.max_points {max_points} exceeds the E-R1 measured total {measured}")


def _check_p10(s: Settings) -> None:
    """P10 bounds that must hold together (design §7.2, §12.4, §13.3); fail closed at startup."""
    ix, m = s.index, s.matching
    if ix.capacity_margin_points >= ix.max_points:
        raise ValueError("index.capacity_margin_points must be below index.max_points")
    if ix.max_points != PROVISIONAL_MAX_POINTS:
        _check_e_r1_evidence(ix.max_points, ix.max_points_evidence)
    if ix.drain_grace_s < 2 * ix.active_ttl_s + m.task_lease_s:
        raise ValueError("index.drain_grace_s must be >= 2 * index.active_ttl_s + matching.task_lease_s")
    for k_max in (m.k_max_a, m.k_max_b, m.k_max_eq):
        if m.k0 > k_max:
            raise ValueError("matching.k0 must not exceed any k_max")
    if m.page_b > m.cap_b_total:
        raise ValueError("matching.page_b must not exceed matching.cap_b_total")
    from fingerprinter.verify import EQUIV_RULES

    if m.equivalence_rule not in EQUIV_RULES:
        raise ValueError(f"matching.equivalence_rule {m.equivalence_rule!r} is not one of {sorted(EQUIV_RULES)}")
    if m.backoff_base_s > m.backoff_max_s:
        raise ValueError("matching.backoff_base_s must not exceed matching.backoff_max_s")


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
