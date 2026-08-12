"""Phase 12 — end-to-end local integration test (phase brief, "End-to-end
local test"): a *synthetic* crawler candidate flows through the real
integration boundary, real Redis, a real worker, real (CPU) DINOv2
inference against a local HTTP media server, and back out through
`resolve_outcome` — no internet, no external search engine/piracy site, no
GPU, matching the brief's explicit constraints.

Reuses the exact fixtures/conventions `tests/test_matching_handler.py`
(Phase 10) already established: `tests/fixtures/tiny_video.mp4`, the
`media_server` fixture, `device="cpu"`, a short `segment_duration_s=0.5` so
the 2s fixture yields 4 segments (enough for `MatcherConfig`'s default
`min_matched_segments=3`).

This is the one place the "synthetic crawler" stands in for a real crawler
process (see `docs/architecture/phase-12-crawler-fingerprinter-integration.md`,
"Local end-to-end flow" for why: the crawler repo is out of scope to modify
or run here, per the phase brief's own "do not modify unrelated crawler
... behavior").
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from acquisition import MediaAcquirer
from embedding.config import SegmentSamplingConfig
from embedding.dinov2_engine import DINOv2EmbeddingEngine
from integration.candidate import FingerprintCandidate, FingerprintPriority
from integration.outcome import FingerprintOutcome, resolve_outcome
from integration.submission import FingerprintJobSubmitter, SubmissionOutcome
from target.cache import FilesystemEmbeddingCache
from target.registry import TargetRegistry
from target.segment_cache import FilesystemSegmentEmbeddingCache
from work_queue.keys import CONSUMER_GROUP, stream_key
from worker.fingerprint_worker import Worker
from worker.matching_handler import build_matching_handler

FIXTURES_DIR = Path(__file__).parent / "fixtures"
TINY_VIDEO = FIXTURES_DIR / "tiny_video.mp4"

BLOCK_MS = 200
LEASE_MS = 5000


@pytest.fixture(scope="module")
def engine() -> DINOv2EmbeddingEngine:
    return DINOv2EmbeddingEngine(device="cpu", segment_sampling_config=SegmentSamplingConfig(segment_duration_s=0.5))


@pytest.fixture
def registry(redis_client, tmp_path) -> TargetRegistry:
    pooled = FilesystemEmbeddingCache(tmp_path / "embedding-cache")
    segments = FilesystemSegmentEmbeddingCache(tmp_path / "segment-cache")
    return TargetRegistry(redis_client, pooled, segments)


def _worker(redis_client, **overrides) -> Worker:
    kwargs = dict(block_ms=BLOCK_MS, lease_ms=LEASE_MS)
    kwargs.update(overrides)
    return Worker(redis_client, **kwargs)


def _handler(engine, registry):
    acquirer = MediaAcquirer(connect_timeout_s=5.0, read_timeout_s=5.0)
    return build_matching_handler(acquirer, engine, registry)


def _synthetic_candidate(media_server, **overrides) -> FingerprintCandidate:
    """Stands in for a real crawler's discovered-URL record — see module
    docstring. `candidate_url` points at the local media server's `/video`
    route, the same fixture-serving route `test_matching_handler.py`
    already uses."""
    defaults = dict(
        candidate_url=media_server.url("/video"),
        media_evidence_id="synthetic-evidence-1",
        media_type="video",
        source_domain="pirate.example",
        target_id="target-1",
        target_version="v1",
    )
    defaults.update(overrides)
    return FingerprintCandidate(**defaults)


def test_synthetic_candidate_self_match_end_to_end(redis_client, media_server, engine, registry):
    """synthetic crawler candidate -> fingerprint job -> Redis -> fingerprint
    worker -> local HTTP media server -> DINOv2 -> matcher -> result ->
    result lookup, exactly the flow the phase brief specifies."""
    registry.register_target("target-1", "v1", str(TINY_VIDEO))

    submission = FingerprintJobSubmitter(redis_client).submit(_synthetic_candidate(media_server))
    assert submission.outcome == SubmissionOutcome.ENQUEUED

    worker = _worker(redis_client)
    entry = worker.claim_one()
    assert entry is not None and entry.is_valid
    assert entry.job.job_id == submission.job_id
    worker.process_claim(entry, _handler(engine, registry))

    view = resolve_outcome(redis_client, submission.job_id)

    assert view.outcome == FingerprintOutcome.MATCH
    assert view.media_evidence_id == "synthetic-evidence-1"
    assert view.target_id == "target-1"
    assert view.target_version == "v1"
    assert view.confidence > 0.99
    evidence = json.loads(view.evidence)
    assert evidence[0]["technique"] == "dinov2"
    assert evidence[0]["matched"] is True


@pytest.fixture(scope="module")
def unrelated_video(tmp_path_factory) -> Path:
    """A synthetic video with visually distinct content from
    `tiny_video.mp4` (solid blue vs. whatever pattern the fixture uses) so
    real DINOv2 embeddings for the two are genuinely dissimilar — not just
    "a different file," a different *embedding*, which is what
    `matching.matcher.match_segments`'s thresholds actually key off."""
    out_dir = tmp_path_factory.mktemp("unrelated-video")
    out_path = out_dir / "unrelated.mp4"
    subprocess.run(
        [
            "ffmpeg", "-y", "-v", "error",
            "-f", "lavfi", "-i", "color=c=blue:s=32x32:rate=1:duration=2",
            "-pix_fmt", "yuv420p", "-c:v", "libx264", "-preset", "ultrafast",
            str(out_path),
        ],
        check=True, capture_output=True, timeout=60,
    )
    return out_path


def test_synthetic_candidate_no_match_end_to_end(redis_client, media_server, engine, registry, unrelated_video):
    """A target registered from content unrelated to the candidate -> the
    real matcher concludes NO_MATCH, not merely "not MATCH"."""
    registry.register_target("target-unrelated", "v1", str(unrelated_video))

    submission = FingerprintJobSubmitter(redis_client).submit(
        _synthetic_candidate(media_server, target_id="target-unrelated", target_version="v1")
    )
    assert submission.outcome == SubmissionOutcome.ENQUEUED

    worker = _worker(redis_client)
    entry = worker.claim_one()
    worker.process_claim(entry, _handler(engine, registry))

    view = resolve_outcome(redis_client, submission.job_id)

    assert view.outcome == FingerprintOutcome.NO_MATCH


def test_target_version_mismatch_is_a_permanent_error(redis_client, media_server, engine, registry):
    """A candidate submitted against a target_version the registry never
    registered (phase brief, "Target versioning": target_id alone is not
    sufficient) -> PermanentFailure -> PERMANENT_ERROR, not a silent
    fallback to some other version."""
    registry.register_target("target-1", "v1", str(TINY_VIDEO))

    submission = FingerprintJobSubmitter(redis_client).submit(
        _synthetic_candidate(media_server, target_id="target-1", target_version="v2-never-registered")
    )
    assert submission.outcome == SubmissionOutcome.ENQUEUED

    worker = _worker(redis_client)
    entry = worker.claim_one()
    worker.process_claim(entry, _handler(engine, registry))

    view = resolve_outcome(redis_client, submission.job_id)

    assert view.outcome == FingerprintOutcome.PERMANENT_ERROR
    assert "v2-never-registered" in view.reason


def test_retryable_acquisition_error_end_to_end(redis_client, engine, registry):
    """A candidate URL that times out / is unreachable maps to
    TransientAcquisitionError -> TransientFailure -> RETRYABLE_ERROR."""
    registry.register_target("target-1", "v1", str(TINY_VIDEO))
    unreachable_url = "http://127.0.0.1:1/never-listens.mp4"

    submission = FingerprintJobSubmitter(redis_client).submit(
        FingerprintCandidate(
            candidate_url=unreachable_url,
            media_evidence_id="evidence-unreachable",
            media_type="video",
            source_domain="pirate.example",
            target_id="target-1",
            target_version="v1",
        )
    )
    assert submission.outcome == SubmissionOutcome.ENQUEUED

    worker = _worker(redis_client)
    entry = worker.claim_one()
    acquirer = MediaAcquirer(connect_timeout_s=1.0, read_timeout_s=1.0)
    worker.process_claim(entry, build_matching_handler(acquirer, engine, registry))

    view = resolve_outcome(redis_client, submission.job_id)

    assert view.outcome == FingerprintOutcome.RETRYABLE_ERROR


def test_permanent_acquisition_error_end_to_end(redis_client, media_server, engine, registry):
    """A 404 from the media server maps to PermanentAcquisitionError ->
    PermanentFailure -> PERMANENT_ERROR."""
    registry.register_target("target-1", "v1", str(TINY_VIDEO))

    submission = FingerprintJobSubmitter(redis_client).submit(
        _synthetic_candidate(media_server, candidate_url=media_server.url("/does-not-exist"))
    )
    assert submission.outcome == SubmissionOutcome.ENQUEUED

    worker = _worker(redis_client)
    entry = worker.claim_one()
    worker.process_claim(entry, _handler(engine, registry))

    view = resolve_outcome(redis_client, submission.job_id)

    assert view.outcome == FingerprintOutcome.PERMANENT_ERROR


def test_worker_crash_lease_recovery_end_to_end(redis_client, media_server, engine, registry):
    """A worker that claims a crawler-submitted job and then "crashes"
    (never acks) -> a second worker reclaims via XAUTOCLAIM and finishes
    it, exactly Phase 2's existing lease-recovery guarantee, exercised
    through the Phase 12 submission/outcome API instead of raw
    Job/JobProducer calls."""
    registry.register_target("target-1", "v1", str(TINY_VIDEO))
    submission = FingerprintJobSubmitter(redis_client).submit(_synthetic_candidate(media_server))

    crashed_worker = Worker(redis_client, consumer_name="worker-crashed", block_ms=BLOCK_MS, lease_ms=150)
    stale_entry = crashed_worker.claim_one()
    assert stale_entry.is_valid  # claimed, then "crashes": no ack, no further contact

    import time

    time.sleep(0.3)  # past lease_ms

    recovering_worker = Worker(redis_client, consumer_name="worker-recovering", block_ms=BLOCK_MS, lease_ms=150)
    reclaimed = recovering_worker.reclaim_stale()
    assert len(reclaimed) == 1
    assert reclaimed[0].job.job_id == submission.job_id
    assert reclaimed[0].attempt == 2

    recovering_worker.process_claim(reclaimed[0], _handler(engine, registry))

    view = resolve_outcome(redis_client, submission.job_id)
    assert view.outcome == FingerprintOutcome.MATCH
    assert view.attempt == 2
    assert view.worker_id == "worker-recovering"


def test_multiple_workers_process_distinct_jobs_without_duplication(redis_client, media_server, engine, registry):
    """Phase brief, "multiple fingerprint workers processing jobs": two
    distinct crawler-submitted candidates against the same target, claimed
    by two independent worker instances, each produces exactly one
    result, with no cross-talk."""
    registry.register_target("target-1", "v1", str(TINY_VIDEO))
    registry.register_target("target-2", "v1", str(TINY_VIDEO))
    submitter = FingerprintJobSubmitter(redis_client)
    # Two distinct target_ids (job identity is derived from candidate_url +
    # target_id + target_version + techniques, deliberately not
    # media_evidence_id — see integration/idempotency.py) so this is
    # genuinely two separate jobs, not the dedup case covered elsewhere.
    first = submitter.submit(_synthetic_candidate(media_server, media_evidence_id="evidence-a", target_id="target-1"))
    second = submitter.submit(
        _synthetic_candidate(
            media_server,
            media_evidence_id="evidence-b",
            target_id="target-2",
            priority=FingerprintPriority.NORMAL,
        )
    )
    assert first.outcome == second.outcome == SubmissionOutcome.ENQUEUED
    assert first.job_id != second.job_id

    worker_a = Worker(redis_client, consumer_name="worker-a", block_ms=BLOCK_MS, lease_ms=LEASE_MS)
    worker_b = Worker(redis_client, consumer_name="worker-b", block_ms=BLOCK_MS, lease_ms=LEASE_MS)

    entry_a = worker_a.claim_one()
    entry_b = worker_b.claim_one()
    assert entry_a.job.job_id != entry_b.job.job_id

    worker_a.process_claim(entry_a, _handler(engine, registry))
    worker_b.process_claim(entry_b, _handler(engine, registry))

    view_a = resolve_outcome(redis_client, first.job_id)
    view_b = resolve_outcome(redis_client, second.job_id)
    assert view_a.outcome == FingerprintOutcome.MATCH
    assert view_b.outcome == FingerprintOutcome.MATCH
    assert view_a.worker_id != view_b.worker_id

    pending = redis_client.xpending(stream_key(), CONSUMER_GROUP)
    assert pending["pending"] == 0
