"""P10 tables (design §5, §11.1, §12.10, §13.3) in the fingerprinter keyspace.

All tables are P10-owned and additive; no P9 table gains a column and no P9
code writes them (P9 rows used by P10: ``stream_registry`` gets P10's own
``match.found.v1`` row and ``retire_guard`` the holder ``p10-index``, both as
P9 designed them for consumers). No TTL anywhere.

Every cell used as a fence is written only by LWT (design §12.10: a plain
write and an LWT on the same cells are not ordered).
"""

from __future__ import annotations

from collections.abc import Sequence

from fingerprinter.db import Db

P10_TABLES: Sequence[str] = (
    # generations per (spec, role); static: the reader pointer and counters (§4.2, §12.4)
    """CREATE TABLE IF NOT EXISTS {ks}.index_generations (
        spec_digest text, role text, generation int,
        active_generation int static, active_epoch int static, routing_epoch int static,
        next_generation int static,
        state text, collection text, params text, params_digest text, build_epoch int,
        boundary_at timestamp, build_cursor map<int, text>, expected_count bigint, actual_count bigint,
        reserved_points bigint, manifest_digest text, detect_count int, validated_detect_count int,
        suspect_since timestamp, suspect_reason text, audit_state text, audit_cursor int,
        promoted_at timestamp, drain_horizon_at timestamp, last_clean_verify_at timestamp,
        verify_cursor text, verify_started_at timestamp, validated_at timestamp, ready_at timestamp,
        created_at timestamp, updated_at timestamp, failure text, audit list<text>,
        PRIMARY KEY ((spec_digest, role), generation))""",
    """CREATE TABLE IF NOT EXISTS {ks}.index_build_lease (
        spec_digest text, role text, owner text, epoch int, lease_until timestamp,
        PRIMARY KEY ((spec_digest, role)))""",
    # the durable mutation intent and its bookkeeping (§12.2)
    """CREATE TABLE IF NOT EXISTS {ks}.index_entries (
        spec_digest text, role text, bucket int, subject_id text,
        desired_op text, desired_ver text, seq bigint, applied map<int, bigint>,
        segment_count int, state text, blocked_reason text, attempts int, last_error text,
        next_attempt_at timestamp, subject text, updated_at timestamp,
        PRIMARY KEY ((spec_digest, role, bucket), subject_id))""",
    # the recovery ledger: one row per occurrence (§12.10, C-14, C-15)
    """CREATE TABLE IF NOT EXISTS {ks}.index_repairs (
        spec_digest text, role text, bucket int, subject_id text, generation int, occurrence int,
        occurrence_id timeuuid, state text, detections int, gen_marked boolean,
        detected_at timestamp, detected_by text, row_seq bigint, expected_count int,
        observed_count int, repaired_at timestamp, required_run map<text, int>,
        close_reason text, closed_at timestamp, resolution text, resolution_reason text,
        resolved_by map<text, int>, resolved_at timestamp, attempts int, next_attempt_at timestamp,
        last_error text,
        PRIMARY KEY ((spec_digest, role, bucket), subject_id, generation, occurrence))
        WITH CLUSTERING ORDER BY (subject_id ASC, generation ASC, occurrence DESC)""",
    # capacity ledger per Qdrant instance (§13.3)
    """CREATE TABLE IF NOT EXISTS {ks}.index_capacity (
        instance text, holder text,
        max_points bigint static, reserved bigint static, updated_at timestamp static,
        audit list<text> static, points bigint, created_at timestamp,
        PRIMARY KEY (instance, holder))""",
    """CREATE TABLE IF NOT EXISTS {ks}.p10_readers (
        reader text PRIMARY KEY, owner text, epoch int, lease_until timestamp,
        cursor_id text, last_gap_at timestamp, gaps bigint)""",
    """CREATE TABLE IF NOT EXISTS {ks}.p10_scans (
        scan text, spec_digest text, bucket int, cycle int, cycle_started_at timestamp,
        last_key text, owner_epoch int, done boolean, updated_at timestamp,
        PRIMARY KEY ((scan, spec_digest), bucket))""",
    """CREATE TABLE IF NOT EXISTS {ks}.target_set_rev (
        spec_digest text PRIMARY KEY, rev bigint, members_digest text, updated_at timestamp)""",
    """CREATE TABLE IF NOT EXISTS {ks}.lowinfo_class (
        spec_digest text, target_representation_id text, target_id text, version int,
        rev bigint, rule text, status text, flags set<int>,
        witnesses map<int, frozen<list<text>>>, computed_at timestamp,
        PRIMARY KEY (spec_digest, target_representation_id))""",
    """CREATE TABLE IF NOT EXISTS {ks}.lowinfo_refresh (
        spec_digest text PRIMARY KEY, requested_rev bigint, done_rev bigint, full_requested boolean,
        owner text, epoch int, lease_until timestamp, state text, attempts int, last_error text,
        requested_at timestamp, updated_at timestamp)""",
    # match tasks: run-numbered (§11.1, §12.10)
    """CREATE TABLE IF NOT EXISTS {ks}.match_tasks (
        subject_id text, spec_digest text, kind text,
        state text, epoch int, owner text, lease_until timestamp,
        run int, run_started_at timestamp, done_run int, done_state text, done_ok_run int,
        recovery_runs map<timeuuid, int>, rerun_reason text, attempt int, failures int,
        next_attempt_at timestamp, due_at timestamp, waiting_reason text, failure text,
        searched_generation int, searched_run int, target_set_rev bigint, ann_stats text,
        candidate_count int, verify_cursor int, incomplete_reason text, cap_override int, subject text,
        created_at timestamp, updated_at timestamp,
        PRIMARY KEY (subject_id, spec_digest, kind))""",
    """CREATE TABLE IF NOT EXISTS {ks}.match_due (
        due_day text, shard int, due_at timestamp, subject_id text, spec_digest text, kind text,
        PRIMARY KEY ((due_day, shard), due_at, subject_id, spec_digest, kind))""",
    """CREATE TABLE IF NOT EXISTS {ks}.match_due_days (due_day text PRIMARY KEY)""",
    """CREATE TABLE IF NOT EXISTS {ks}.match_candidates (
        subject_id text, spec_digest text, kind text, generation int, run int, rank int,
        other_id text, other text, hits int, best_score double, state text, attempts int,
        outcome text, match_id text, reason text, updated_at timestamp,
        PRIMARY KEY ((subject_id, spec_digest, kind, generation, run), rank))""",
    # matches (§9.3, §9.3a; key corrected C-6)
    """CREATE TABLE IF NOT EXISTS {ks}.matches (
        content_id text, target_id text, target_version int, media_spec_digest text,
        target_spec_digest text, technique_name text, technique_version text,
        match_id text, representation_id text, target_representation_id text, verdict text,
        score double, confidence double, calibration_id text, alignment text,
        verifier_params_digest text, exclusion_digest text, provenance text, decided_at timestamp,
        envelope text, event_id text, published_at timestamp,
        PRIMARY KEY (content_id, target_id, target_version, media_spec_digest, target_spec_digest,
                     technique_name, technique_version))""",
    """CREATE TABLE IF NOT EXISTS {ks}.matches_by_target (
        target_id text, target_version int, content_id text, media_spec_digest text,
        target_spec_digest text, technique_name text, technique_version text, match_id text,
        PRIMARY KEY ((target_id, target_version), content_id, media_spec_digest, target_spec_digest,
                     technique_name, technique_version))""",
    """CREATE TABLE IF NOT EXISTS {ks}.matches_by_id (
        match_id text PRIMARY KEY, content_id text, target_id text, target_version int,
        media_spec_digest text, target_spec_digest text, technique_name text,
        technique_version text)""",
    """CREATE TABLE IF NOT EXISTS {ks}.equivalence_links (
        content_id text, other_content_id text, spec_digest text, technique_name text,
        technique_version text, state text, confidence double, evidence text, provenance text,
        decided_at timestamp, revoked_at timestamp, audit list<text>,
        PRIMARY KEY (content_id, other_content_id, spec_digest, technique_name, technique_version))""",
    """CREATE TABLE IF NOT EXISTS {ks}.match_intents (
        day text, shard int, intent_id text, kind text, fact_key text, payload text,
        created_at timestamp, holder text,
        PRIMARY KEY ((day, shard), intent_id))""",
    """CREATE TABLE IF NOT EXISTS {ks}.match_intent_days (day text PRIMARY KEY)""",
    """CREATE TABLE IF NOT EXISTS {ks}.verifier_params (
        technique_name text, technique_version text, params text, params_digest text,
        calibration text, calibration_status text, calibration_id text,
        created_at timestamp, audit list<text>,
        PRIMARY KEY (technique_name, technique_version))""",
    """CREATE TABLE IF NOT EXISTS {ks}.p10_leases (
        name text PRIMARY KEY, owner text, epoch int, lease_until timestamp)""",
)


P10_COLUMNS: Sequence[tuple[str, str, str]] = (
    # added after the first P10 schema (tables created by an earlier build of this module)
    ("match_tasks", "searched_run", "int"),
    ("match_tasks", "cap_override", "int"),
)


def migrate_p10(db: Db) -> None:
    """Create the P10 tables and additive columns; idempotent. The keyspace must exist (P9 ``migrate``)."""
    for cql in P10_TABLES:
        db.ddl(cql)
    for table, column, kind in P10_COLUMNS:
        if db.one("SELECT column_name FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ? "
                  "AND column_name = ?", db.keyspace, table, column) is None:
            db.ddl(f"ALTER TABLE {{ks}}.{table} ADD {column} {kind}")
