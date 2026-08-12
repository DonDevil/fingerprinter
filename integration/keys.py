"""Redis key conventions for the Phase 12 crawler-integration layer.

One new key pattern, in its own sub-namespace under the existing
`fingerprint:*` prefix (`work_queue/keys.py`, `target/keys.py`) — never
`crawler:*`/`evidence:*`, the crawler's own namespaces (see phase-12 doc,
"Redis namespaces" for the full survey of what already exists there).

`fingerprint:submission:{job_id}` is deliberately distinct from
`fingerprint:job:{job_id}:state` (`work_queue/keys.py::state_key`): the state
hash is only written when a worker *claims* the job (Phase 1's own design —
"state is written on claim, not on creation"), so it cannot be used to
detect a duplicate submission that hasn't been claimed yet. The submission
marker exists purely to close that window — see `integration/submission.py`.
"""
from __future__ import annotations


def submission_marker_key(job_id: str) -> str:
    return f"fingerprint:submission:{job_id}"
