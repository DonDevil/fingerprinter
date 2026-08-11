"""Wires media acquisition into the worker/handler contract (Phase 5).

Bridges acquisition-specific exceptions onto Phase 3's
TransientFailure/PermanentFailure — no separate retry framework.

The handler built here is a stand-in for the real fingerprint pipeline: it
proves claim -> acquire -> handler receives a MediaArtifact -> Result ->
Phase 4 commit, using a synthetic Result. A later phase's handler receives
the exact same MediaArtifact and runs DINOv2/pHash/audio against
`artifact.local_path` instead of returning a synthetic decision.
"""
from __future__ import annotations

import time
from typing import Callable

from acquisition import MediaAcquirer, PermanentAcquisitionError, TransientAcquisitionError
from work_queue.jobs import Job
from work_queue.results import Result, ResultDecision
from worker.fingerprint_worker import PermanentFailure, TransientFailure


def build_acquisition_handler(acquirer: MediaAcquirer) -> Callable[[Job], Result]:
    """Returns a handler: acquire the job's media, then report a synthetic Result.

    No fingerprinting happens here (out of scope for this phase) — this
    only proves the artifact is usable and reaches the result contract.
    The artifact is always cleaned up before the handler returns, whether
    or not building the Result succeeds.
    """

    def handler(job: Job) -> Result:
        started = time.time()
        try:
            artifact = acquirer.acquire(job.media_url)
        except TransientAcquisitionError as exc:
            raise TransientFailure(str(exc)) from exc
        except PermanentAcquisitionError as exc:
            raise PermanentFailure(str(exc)) from exc

        try:
            return Result(
                decision=ResultDecision.NO_MATCH,
                algorithm="acquisition-only-v0",
                processing_started_at=started,
                processing_completed_at=time.time(),
                summary=(
                    f"acquired {artifact.byte_size} bytes from {artifact.final_url} "
                    f"(sha256={artifact.checksum_sha256[:12]}...)"
                ),
            )
        finally:
            artifact.cleanup()

    return handler
