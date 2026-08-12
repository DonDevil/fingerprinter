"""`FingerprintCandidate` — the crawler-facing input to the Phase 12
integration boundary.

Per the phase brief ("keep the integration contract intentionally small",
"do NOT blindly copy the entire crawler URL metadata structure"), this is
deliberately not a copy of the crawler's own `evidence:asset:{aid}` hash
(observation history, variants, last_referrer_url, discovery_method, ...) —
see `docs/architecture/phase-12-crawler-fingerprinter-integration.md` for
the full field-by-field mapping. It carries exactly what
`work_queue.jobs.Job` needs plus the one thing `Job` doesn't (and shouldn't)
carry: `priority`, which selects *which* job stream to enqueue onto
(`work_queue.keys.stream_key(priority)`) rather than being a field inside
the job payload itself.

`media_evidence_id` is the crawler's own opaque reference back to its
storage (already part of `Job` since Phase 1 — `work_queue/jobs.py`'s
`REQUIRED_FIELDS` docstring calls it "crawler's reference to the sample,
opaque to us"). The fingerprinter never interprets it; it exists solely so
a result can be correlated back to whatever the crawler wants to update
(an `evidence:asset:{aid}` hash, in the sibling crawler repo — see the
phase-12 doc's "Current crawler architecture" section for why the
fingerprinter does not read or write that hash directly).
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Tuple

from matching.aggregation import DINOV2_TEMPORAL_TECHNIQUE
from work_queue.keys import DEFAULT_PRIORITY

DEFAULT_MAX_ATTEMPTS = 3

# URL schemes accepted at candidate-admission time. Intentionally the same
# set `acquisition.acquirer.MediaAcquirer` enforces
# (`DEFAULT_ALLOWED_SCHEMES`) — checked again here so an obviously-invalid
# candidate is rejected before it ever reaches Redis, not just at
# acquisition time inside a worker process. This is *not* a second,
# divergent policy: if the acquirer's allowed schemes ever change, this
# constant should change with it (see phase-12 doc, "Security").
_ALLOWED_URL_SCHEMES = ("http://", "https://")


class CandidateValidationError(ValueError):
    """A `FingerprintCandidate` does not satisfy the integration contract."""


class FingerprintPriority(str, Enum):
    """What fingerprint priority means (phase brief, "Priority"): three
    coarse bands, each mapped to a distinct job stream
    (`PRIORITY_STREAM_NAMES` below) that ops can staff with a different
    number of worker processes — not a numeric score, since nothing in this
    project's architecture (Redis Streams + consumer groups, one stream per
    priority) supports finer-grained priority than "which stream." A
    caller with a numeric confidence/urgency signal maps it onto one of
    these three bands itself; seeing the crawler's own frontier/evidence
    priority convention (plain int, lower = more urgent, default 10 — see
    phase-12 doc) is not itself a reason to import that scale here, since
    the two systems' priority mechanisms are structurally different
    (numeric ZSET score vs. named stream selection).
    """

    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


# NORMAL intentionally reuses `work_queue.keys.DEFAULT_PRIORITY` ("default")
# rather than a new "normal" stream name — every existing worker/test that
# never specifies a priority already reads `stream_key(DEFAULT_PRIORITY)`;
# introducing a same-meaning-different-name stream here would silently
# split that traffic across two streams no consumer group is unified over.
PRIORITY_STREAM_NAMES = {
    FingerprintPriority.HIGH: "high",
    FingerprintPriority.NORMAL: DEFAULT_PRIORITY,
    FingerprintPriority.LOW: "low",
}


@dataclass(frozen=True)
class FingerprintCandidate:
    """One crawler-discovered candidate the fingerprinter should evaluate
    against one target version.

    Field -> `work_queue.jobs.Job` field mapping (see phase-12 doc, "Job
    schema" for the full table):

        candidate_url     -> media_url
        media_evidence_id -> media_evidence_id
        media_type        -> media_type
        source_domain     -> source_domain
        target_id         -> target_id
        target_version    -> target_version
        techniques        -> techniques
        max_attempts      -> max_attempts
        priority           (selects the stream; not a Job field at all)
    """

    candidate_url: str
    media_evidence_id: str
    media_type: str
    source_domain: str
    target_id: str
    target_version: str
    techniques: Tuple[str, ...] = (DINOV2_TEMPORAL_TECHNIQUE,)
    priority: FingerprintPriority = FingerprintPriority.NORMAL
    max_attempts: int = DEFAULT_MAX_ATTEMPTS

    def validate(self) -> None:
        """Cheap, local, pre-Redis validation. Deliberately not a full
        re-implementation of `acquisition.validation.probe_media` (that
        requires the actual bytes and only runs inside a worker) — this
        only catches candidates malformed enough that enqueuing them would
        be pointless (empty required fields, disallowed URL scheme,
        non-positive max_attempts), the same class of check
        `work_queue.jobs.Job.from_stream_fields` already performs on the
        far side of the queue. Raises `CandidateValidationError`."""
        if not self.candidate_url or not self.candidate_url.lower().startswith(_ALLOWED_URL_SCHEMES):
            raise CandidateValidationError(
                f"candidate_url must start with one of {_ALLOWED_URL_SCHEMES!r}: {self.candidate_url!r}"
            )
        for name in ("media_evidence_id", "media_type", "source_domain", "target_id", "target_version"):
            if not getattr(self, name):
                raise CandidateValidationError(f"{name} must not be empty")
        if not self.techniques:
            raise CandidateValidationError("techniques must contain at least one entry")
        if self.max_attempts < 1:
            raise CandidateValidationError("max_attempts must be >= 1")
        if not isinstance(self.priority, FingerprintPriority):
            raise CandidateValidationError(f"priority must be a FingerprintPriority, got {self.priority!r}")
