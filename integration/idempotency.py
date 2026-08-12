"""Deterministic job identity — the core of the Phase 12 idempotency
strategy (phase brief, "Idempotency" — mandatory).

Per the brief: "Prefer deterministic IDs / atomic Redis operations ... over
a distributed transaction." A `FingerprintCandidate` maps to a `job_id` by
hashing exactly the fields that define "the same unit of work":

    (candidate_url, target_id, target_version, techniques)

Not included: `media_evidence_id`, `source_domain`, `priority`,
`max_attempts` — none of these change *what* is being checked (the same
URL against the same target version, using the same technique set), only
bookkeeping/scheduling detail. Two crawler observations of the same URL
with different `media_evidence_id`s (e.g. rediscovered on a different page)
must collapse onto the same job — that is exactly the duplicate-delivery
case the brief requires designing for.

`target_version` is included deliberately (phase brief, "Target
versioning": "target_id alone is not sufficient") — a target version bump
must produce a fresh job_id, not silently reuse a stale result recorded
against the old version. `techniques` is included (sorted, for a
stable key independent of input ordering) because a different technique
set is a materially different unit of work, not a duplicate of the same
one.

This function alone does not guarantee at-most-once enqueue — Redis
Streams' `XADD` has no built-in dedup, so two calls with the same `job_id`
would still produce two stream entries. `integration.submission` closes
that gap with an atomic `SET NX` marker keyed by this same `job_id` — see
that module's docstring.
"""
from __future__ import annotations

import hashlib
import json

from integration.candidate import FingerprintCandidate

JOB_ID_LENGTH = 32  # half a sha256 hex digest; collision risk is astronomically negligible at this job volume


def derive_job_id(candidate: FingerprintCandidate) -> str:
    payload = json.dumps(
        {
            "candidate_url": candidate.candidate_url,
            "target_id": candidate.target_id,
            "target_version": candidate.target_version,
            "techniques": sorted(candidate.techniques),
        },
        sort_keys=True,
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return digest[:JOB_ID_LENGTH]
