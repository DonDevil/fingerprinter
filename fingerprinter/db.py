"""Scylla access: connection, prepared statements, LWT results, migrations (design §4.1).

Keyspace ``fingerprinter`` (configurable), tablets disabled (LWT needs vnodes on
Scylla 6.2). **No table has a TTL** except ``encode_attempt_log`` (observability
only, never read for scheduling or recovery).

Consistency: authoritative reads that decide a transition are SERIAL; plain
writes and reads are LOCAL_QUORUM (identical meaning at RF=1 and RF=3).
"""

from __future__ import annotations

import threading
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any

from cassandra import (
    ConsistencyLevel,
    CoordinationFailure,
    OperationTimedOut,
    ReadTimeout,
    Unavailable,
    WriteFailure,
    WriteTimeout,
)
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, NoHostAvailable
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import PreparedStatement, dict_factory

from fingerprinter.config import ScyllaConfig
from fingerprinter.errors import StorageUnavailableError

# CoordinationFailure = ReadFailure/WriteFailure (a replica failed to execute, e.g. code 1500 under load)
UNAVAILABLE = (NoHostAvailable, OperationTimedOut, Unavailable, ReadTimeout, WriteTimeout, CoordinationFailure)

SERIAL = "serial"
QUORUM = "quorum"


class LwtUnknownError(StorageUnavailableError):
    """An LWT timed out: it may or may not have been applied (resolve by SERIAL read)."""


@dataclass(frozen=True)
class LwtResult:
    applied: bool
    row: dict[str, Any]


class Db:
    def __init__(self, cluster: Cluster, session: Any, keyspace: str) -> None:
        self._cluster = cluster
        self._session = session
        self.keyspace = keyspace
        self._prepared: dict[tuple[str, str], PreparedStatement] = {}
        self._lock = threading.Lock()

    @classmethod
    def connect(cls, cfg: ScyllaConfig) -> Db:
        profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=cfg.local_dc)),
            request_timeout=cfg.request_timeout_s,
            row_factory=dict_factory,
        )
        cluster = Cluster(
            contact_points=cfg.contact_points,
            port=cfg.port,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            protocol_version=4,
        )
        try:
            session = cluster.connect()
        except UNAVAILABLE as exc:
            cluster.shutdown()
            raise StorageUnavailableError(f"cannot connect to Scylla: {exc}") from exc
        return cls(cluster, session, cfg.keyspace)

    def close(self) -> None:
        self._cluster.shutdown()

    def _stmt(self, cql: str, level: str) -> PreparedStatement:
        key = (cql, level)
        with self._lock:
            stmt = self._prepared.get(key)
        if stmt is None:
            try:
                stmt = self._session.prepare(cql.format(ks=self.keyspace))
            except UNAVAILABLE as exc:
                raise StorageUnavailableError(str(exc)) from exc
            stmt.consistency_level = ConsistencyLevel.LOCAL_QUORUM
            stmt.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
            if level == SERIAL:
                stmt.consistency_level = ConsistencyLevel.LOCAL_SERIAL
            with self._lock:
                self._prepared[key] = stmt
        return stmt

    def rows(self, cql: str, *values: Any, level: str = QUORUM) -> list[dict[str, Any]]:
        try:
            return list(self._session.execute(self._stmt(cql, level), values))
        except UNAVAILABLE as exc:
            raise StorageUnavailableError(f"{type(exc).__name__}: {exc}") from exc

    def one(self, cql: str, *values: Any, level: str = QUORUM) -> dict[str, Any] | None:
        out = self.rows(cql, *values, level=level)
        return out[0] if out else None

    def run(self, cql: str, *values: Any) -> None:
        self.rows(cql, *values)

    def lwt(self, cql: str, *values: Any) -> LwtResult:
        """A conditional write. ``LwtUnknownError`` when the outcome is unknown."""
        try:
            result = list(self._session.execute(self._stmt(cql, QUORUM), values))
        except (WriteTimeout, WriteFailure, OperationTimedOut) as exc:
            # a failed Paxos write may still have been applied on some replica: outcome unknown
            raise LwtUnknownError(f"LWT outcome unknown: {exc}") from exc
        except UNAVAILABLE as exc:
            raise StorageUnavailableError(f"{type(exc).__name__}: {exc}") from exc
        row = dict(result[0]) if result else {}
        applied = bool(row.pop("[applied]", False))
        return LwtResult(applied, row)

    def stream(self, cql: str, *values: Any, fetch_size: int = 500) -> Iterator[dict[str, Any]]:
        stmt = self._stmt(cql, QUORUM).bind(values)
        stmt.fetch_size = fetch_size
        try:
            yield from self._session.execute(stmt)
        except UNAVAILABLE as exc:
            raise StorageUnavailableError(f"{type(exc).__name__}: {exc}") from exc

    def ddl(self, cql: str) -> None:
        try:
            self._session.execute(cql.format(ks=self.keyspace), timeout=120)
        except UNAVAILABLE as exc:
            raise StorageUnavailableError(str(exc)) from exc


COLUMNS: Sequence[tuple[str, str, str]] = (
    # added after the first P9 schema (OF-2): the calibration report (JSON)
    ("encoder_hosts", "calibration", "text"),
)

TABLES: Sequence[str] = (
    """CREATE TABLE IF NOT EXISTS {ks}.specs (
        spec_digest text PRIMARY KEY, name text, version text, document text,
        status text, compute_dtype text, origin_profile text, evidence text,
        created_at timestamp, updated_at timestamp, audit list<text>)""",
    """CREATE TABLE IF NOT EXISTS {ks}.spec_roles (
        role text PRIMARY KEY, spec_digest text, epoch int, updated_at timestamp,
        audit list<text>)""",
    """CREATE TABLE IF NOT EXISTS {ks}.runtime_profiles (
        profile text PRIMARY KEY, document text, admitted_variants set<text>,
        created_at timestamp, audit list<text>)""",
    """CREATE TABLE IF NOT EXISTS {ks}.spec_runtime_compat (
        spec_digest text, profile text, approved_by text, approved_at timestamp,
        evidence text, PRIMARY KEY (spec_digest, profile))""",
    """CREATE TABLE IF NOT EXISTS {ks}.encode_requests (
        content_id text, spec_key text, content_digest text, resolved_spec text,
        required_spec text, origin text, state text, priority int,
        sources map<text, text>, bad_sources map<text, text>, tried_sources set<text>,
        request_event_ids set<text>, first_event_id text,
        next_attempt_at timestamp, attempt_id text, lease_until timestamp,
        claims int, claims_base int, failures int, episode int,
        last_failure text, last_retryable boolean, last_detail text, last_failed_at timestamp,
        failed_envelope text, failed_event_id text, failed_event_published_at timestamp,
        representation_id text, created_at timestamp, updated_at timestamp,
        PRIMARY KEY (content_id, spec_key))""",
    """CREATE TABLE IF NOT EXISTS {ks}.encode_due (
        due_day text, shard int, due_at timestamp, content_id text, spec_key text,
        PRIMARY KEY ((due_day, shard), due_at, content_id, spec_key))""",
    """CREATE TABLE IF NOT EXISTS {ks}.encode_due_days (due_day text PRIMARY KEY)""",
    """CREATE TABLE IF NOT EXISTS {ks}.representations (
        content_id text, spec_digest text, representation_id text, state text,
        artifact_attempt_id text, artifact_uri text, artifact_sha256 text,
        artifact_size bigint, segment_count int, duration_s double,
        runtime_fingerprint text, device text, compute_dtype text, request_spec_key text,
        ready_envelope text, ready_event_id text, ready_event_published_at timestamp,
        f_index boolean, f_request boolean, f_targets boolean,
        created_at timestamp, retired_at timestamp,
        PRIMARY KEY (content_id, spec_digest))""",
    """CREATE TABLE IF NOT EXISTS {ks}.representations_by_spec (
        spec_digest text, bucket int, content_id text, representation_id text,
        PRIMARY KEY ((spec_digest, bucket), content_id))""",
    """CREATE TABLE IF NOT EXISTS {ks}.intents (
        day text, shard int, intent_id text, kind text, fact_key text,
        attempt_id text, event_id text, artifact_uri text, stream text, envelope text,
        payload text, created_at timestamp, holder text,
        PRIMARY KEY ((day, shard), intent_id))""",
    """CREATE TABLE IF NOT EXISTS {ks}.intent_days (day text PRIMARY KEY)""",
    """CREATE TABLE IF NOT EXISTS {ks}.targets (
        target_id text, version int, title text, kind text, aliases list<text>,
        release_year int, content_id text, ref_sha256 text, ref_size bigint, ref_key text,
        state text, spec_digest text, registered_at timestamp,
        registered_envelope text, registered_event_id text,
        registered_event_published_at timestamp,
        retired_envelope text, retired_event_id text, retired_event_published_at timestamp,
        retired_at timestamp, retire_reason text, failure text, audit list<text>,
        PRIMARY KEY (target_id, version))""",
    """CREATE TABLE IF NOT EXISTS {ks}.targets_by_content (
        content_id text, target_id text, version int, state text,
        PRIMARY KEY (content_id, target_id, version))""",
    """CREATE TABLE IF NOT EXISTS {ks}.target_representations (
        target_id text, version int, spec_digest text, state text,
        content_id text, representation_id text, target_representation_id text,
        bound_at timestamp, PRIMARY KEY (target_id, version, spec_digest))""",
    """CREATE TABLE IF NOT EXISTS {ks}.stream_registry (
        stream text PRIMARY KEY, required_groups set<text>, hold_until_registered boolean,
        audit list<text>)""",
    """CREATE TABLE IF NOT EXISTS {ks}.intake_state (
        reader text PRIMARY KEY, owner text, epoch int, lease_until timestamp,
        cursor_id text, consumed_ordinal bigint, mode text, halt_reason text,
        initialized_at timestamp, init_mode text, invalid_total bigint)""",
    """CREATE TABLE IF NOT EXISTS {ks}.intake_gaps (
        reader text, gap_id text, detected_at timestamp, kind text, after_cursor text,
        first_retained text, missing_count bigint, window_from timestamp,
        window_to timestamp, state text, replay_from timestamp, replay_to timestamp,
        replay_start_id text, replay_end_id text, audit list<text>,
        PRIMARY KEY (reader, gap_id))""",
    """CREATE TABLE IF NOT EXISTS {ks}.intake_invalid (
        reader text, entry_id text, reason text, seen_at timestamp,
        PRIMARY KEY (reader, entry_id))""",
    """CREATE TABLE IF NOT EXISTS {ks}.backfill_jobs (
        job_id text PRIMARY KEY, from_spec text, to_spec text, state text, epoch int,
        filters text, created_at timestamp, updated_at timestamp, buckets int,
        audit list<text>)""",
    """CREATE TABLE IF NOT EXISTS {ks}.backfill_progress (
        job_id text, bucket int, cursor text, done boolean, enqueued int,
        PRIMARY KEY (job_id, bucket))""",
    """CREATE TABLE IF NOT EXISTS {ks}.backfill_items (
        job_id text, content_id text, outcome text, detail text, updated_at timestamp,
        PRIMARY KEY (job_id, content_id))""",
    """CREATE TABLE IF NOT EXISTS {ks}.encoder_hosts (
        host_id text, device text, spec_digest text, compute_dtype text,
        b_max int, b_start int, b_stable int, oom_recovered bigint, oom_single bigint,
        calibrated_at timestamp, PRIMARY KEY (host_id, device, spec_digest, compute_dtype))""",
    """CREATE TABLE IF NOT EXISTS {ks}.retire_guard (
        spec_digest text, holder text, PRIMARY KEY (spec_digest, holder))""",
    """CREATE TABLE IF NOT EXISTS {ks}.audit_log (
        day text, at timeuuid, actor text, action text, detail text,
        PRIMARY KEY (day, at))""",
    """CREATE TABLE IF NOT EXISTS {ks}.encode_attempt_log (
        content_id text, spec_key text, attempt_id text, host_id text, device text,
        outcome text, detail text, started_at timestamp, ended_at timestamp,
        stage_seconds text, PRIMARY KEY ((content_id, spec_key), attempt_id))
        WITH default_time_to_live = 7776000""",
)


def migrate(db: Db, replication_factor: int = 1) -> None:
    """Create the keyspace (tablets off) and tables; idempotent."""
    db.ddl(
        "CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = "
        "{{'class': 'NetworkTopologyStrategy', 'replication_factor': "
        + str(int(replication_factor))
        + "}} AND tablets = {{'enabled': false}}"
    )
    for cql in TABLES:
        db.ddl(cql)
    for table, column, kind in COLUMNS:
        if db.one(
            "SELECT column_name FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ? "
            "AND column_name = ?", db.keyspace, table, column,
        ) is None:
            db.ddl(f"ALTER TABLE {{ks}}.{table} ADD {column} {kind}")
