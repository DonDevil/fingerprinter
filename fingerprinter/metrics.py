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
