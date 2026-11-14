"""Prometheus metrics (design §12.7), ``fingerprinter_`` prefix.

Process-local counters/gauges; ``fingerprinter-* status --json`` and the soak
report read the authoritative counts from Scylla, not from these.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

P = "fingerprinter_"

REQUESTS = Counter(P + "requests_total", "Request upserts and transitions", ["outcome"])
CLAIMS = Counter(P + "claims_total", "Successful request claims")
RELEASES = Counter(P + "releases_total", "Attempts released without a failure", ["reason"])
ATTEMPT_FAILURES = Counter(P + "attempt_failures_total", "Attributed failures", ["failure"])
ENCODES = Counter(P + "encodes_total", "Encode attempts by outcome", ["outcome"])
COMMITTED = Counter(P + "representations_committed_total", "Authoritative commits won")
STAGE = Histogram(
    P + "encode_stage_seconds",
    "Per-stage time",
    ["stage"],
    buckets=(0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 900, 3600),
)
FRAMES = Counter(P + "encoder_frames_total", "Frames embedded", ["device"])
OOM_RECOVERED = Counter(P + "encoder_oom_recovered_total", "Recovered batch OOMs")
OOM_SINGLE = Counter(P + "encoder_oom_single_total", "Single-item OOMs")
CONTROLLED_EXITS = Counter(P + "encoder_controlled_exits_total", "Controlled exits", ["reason"])
BATCH = Gauge(P + "encoder_batch_size", "Current batch size")
BATCH_MAX = Gauge(P + "encoder_batch_calibrated_max", "Largest batch size that fit at calibration")
QUEUE_DEPTH = Gauge(P + "encoder_queue_depth", "Items in the bounded inference queue")
RSS = Gauge(P + "encoder_host_rss_bytes", "Encoder process RSS")
VRAM = Gauge(P + "encoder_vram_bytes", "VRAM", ["kind"])
RUNTIME_INCOMPATIBLE = Counter(P + "runtime_incompatible_total", "Specs refused by runtime check")
GOLDEN = Counter(P + "golden_check_total", "Startup golden checks", ["result"])
INTAKE_APPLIED = Counter(P + "intake_entries_total", "Intake entries", ["outcome"])
INTAKE_OPEN_GAPS = Gauge(P + "intake_open_gaps", "Intake gap records not CLOSED_*")
INTAKE_LAG = Gauge(P + "intake_lag_entries", "Stream entries not yet consumed")
REPAIRS = Counter(P + "reconciler_repairs_total", "Reconciler repairs", ["kind"])
INTENTS_OPEN = Gauge(P + "intents_open", "Open intents")
ORPHANS = Counter(P + "orphans_deleted_total", "Orphan objects deleted", ["kind"])
DUE_REPAIRED = Counter(P + "due_index_repaired_total", "Due entries recreated by the full pass")
EVENTS = Counter(P + "events_total", "Publication outcomes", ["stream", "outcome"])

# --- P10 vector index and matcher (P10 design §14) --------------------------------------
P10_TASKS = Gauge(P + "match_tasks", "Match tasks by kind and state (status scan)", ["kind", "state"])
P10_QUERIES = Counter(P + "match_queries_total", "Index queries", ["kind", "outcome"])
P10_QUERY_SECONDS = Histogram(P + "match_query_seconds", "One search step", ["kind"],
                              buckets=(0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 30, 120))
P10_ANN_HITS = Counter(P + "match_ann_hits_total", "ANN hits", ["kind"])
P10_HITS_ABOVE_FLOOR = Counter(P + "match_hits_above_floor_total", "ANN hits at or above the floor", ["kind"])
P10_CANDIDATES = Counter(P + "match_candidates_total", "Candidates by stage", ["kind", "stage"])
P10_SATURATED = Counter(P + "match_saturated_segments_total", "Query segments that ended at k_max", ["kind"])
P10_VERIFY_SECONDS = Histogram(P + "match_verify_seconds", "One verification", ["kind"],
                               buckets=(0.001, 0.01, 0.05, 0.1, 0.5, 1, 5, 30))
P10_VERIFY_FAILURES = Counter(P + "match_verify_failures_total", "Candidate verification failures", ["kind", "reason"])
P10_DECISIONS = Counter(P + "match_decisions_total", "Verified decisions", ["kind", "verdict"])
P10_INCOMPLETE = Counter(P + "match_incomplete_total", "Runs that ended INCOMPLETE", ["kind", "reason"])
P10_WAITING = Counter(P + "match_waiting_index_total", "Task steps that went WAITING_INDEX", ["kind", "reason"])
P10_LOWINFO_EXCLUDED = Counter(P + "match_lowinfo_excluded_total", "Segments/hits excluded by usable flags", ["kind"])
P10_DIVERGENCE = Counter(P + "match_decision_divergence_total", "A later verification differed from the first decision")
P10_UNPUBLISHED = Gauge(P + "match_unpublished_uncalibrated", "Stored decisions held for calibration", ["version"])
P10_INDEX_UNAVAILABLE = Counter(P + "index_unavailable_total", "Index calls that failed as unavailable", ["role"])
P10_INDEX_MISS = Counter(P + "index_miss_total", "Subjects found missing from a generation", ["role"])
P10_INDEX_STALE = Counter(P + "index_stale_total", "Stale or dead-subject points dropped as candidates", ["role"])
P10_UPSERT_POINTS = Counter(P + "index_upsert_points_total", "Points upserted", ["role"])
P10_UPSERT_SECONDS = Histogram(P + "index_upsert_seconds", "One subject apply", ["role"],
                               buckets=(0.01, 0.05, 0.1, 0.5, 1, 5, 30))
P10_ENTRIES_BLOCKED = Counter(P + "index_entries_blocked_total",
                              "Mutation rows that became BLOCKED", ["role", "reason"])
P10_VERIFY_STALE = Counter(P + "index_verify_stale_total", "Stale points removed by verification", ["role"])
P10_VERIFY_ORPHANS = Counter(P + "index_verify_orphans_total", "Orphan points found by verification", ["role"])
P10_STALE_CANDIDATES = Counter(P + "index_verify_stale_candidates_total",
                               "Stale points that proposed a candidate", ["role"])
P10_CAPACITY_RESERVED = Gauge(P + "index_capacity_reserved_points", "Reserved points (ledger)", ["instance"])
P10_CAPACITY_LIMIT = Gauge(P + "index_capacity_limit_points", "Configured max_points", ["instance"])
P10_CAPACITY_BLOCKED = Counter(P + "index_capacity_blocked_total", "Reservations refused")
P10_REBUILD_CAPACITY_BLOCKED = Gauge(P + "index_rebuild_capacity_blocked",
                                     "Refused rebuild admission (points)", ["holder"])
P10_BUILD_PROGRESS = Gauge(P + "index_build_progress", "Build buckets done", ["spec", "role", "generation"])
P10_GENERATION_SUSPECT = Gauge(P + "index_generation_suspect",
                               "Generation marked suspect", ["spec", "role", "generation"])
P10_REPAIRS = Gauge(P + "index_repairs", "Recovery occurrences by state (last scan)", ["role", "state"])
P10_REPAIRS_BLOCKED = Gauge(P + "index_repairs_blocked", "Occurrences past repair max_attempts (last scan)", ["role"])
P10_REPAIRS_CLOSED = Counter(P + "index_repairs_closed_total", "Occurrences closed", ["close_reason"])
P10_RERUNS = Counter(P + "match_task_reruns_total", "Task runs bumped", ["reason"])
P10_RECOVERY_OPEN = Gauge(P + "match_recovery_open", "recovery_open (last full scan)", ["spec"])
P10_RECOVERY_UNRESOLVED = Gauge(P + "match_recovery_unresolved", "recovery_unresolved (last full scan)", ["spec"])
P10_HINT_GAPS = Counter(P + "p10_hint_gaps_total", "Hint stream ranges trimmed before reading")
P10_SCAN_ROWS = Counter(P + "p10_scan_rows_total", "Backstop scan rows", ["scan"])
P10_SCAN_CYCLE = Gauge(P + "p10_scan_cycle", "Completed backstop cycles", ["scan"])
P10_SCAN_STALLED = Counter(P + "p10_scan_stalled_total", "Backstop scans that refused to skip", ["scan"])
P10_LOWINFO_WITNESS = Counter(P + "lowinfo_witness_checks_total", "Witness liveness checks", ["result"])
P10_LOWINFO_TRUNCATED = Counter(P + "lowinfo_enumeration_truncated_total", "Truncated classification queries")
P10_LOWINFO_REFRESH_FAILURES = Counter(P + "lowinfo_refresh_failures_total", "Failed low-information refreshes")
P10_LOWINFO_DIFF = Counter(P + "lowinfo_backstop_diff_total", "Full recomputation differences", ["kind"])
P10_EQUIVALENCE = Counter(P + "equivalence_links_total", "Equivalence decisions", ["state"])
