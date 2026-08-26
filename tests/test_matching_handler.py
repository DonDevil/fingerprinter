"""Phase 10 — worker/matching_handler.py, exercised end-to-end.

The happy-path/target-caching tests use real DINOv2 inference
(device="cpu") against the existing tiny_video.mp4 fixture (2s), the same
"real inference, tiny input" convention tests/test_embedding.py's Phase 9
segment tests already established, plus real HTTP acquisition via
media_server's "/video" route and a real TargetRegistry backed by
filesystem caches against the shared Redis test db — this is the first
test that drives claim -> acquire -> embed -> match -> commit_result
through Worker.process_claim end to end.

A short segment_duration_s (0.5s) keeps the 2s fixture at 4 segments —
enough for MatcherConfig's default min_matched_segments=3 to be
meaningful without needing a longer/real video.

The error-path tests (candidate embedding failure, unknown target,
technique gating) call the handler directly against a fake acquirer
rather than going through real HTTP acquisition + Worker — Phase 5's
MediaAcquirer already validates media via ffprobe before a handler ever
sees it (tests/test_acquisition.py's own corrupt-media coverage), so
reaching `embed_video_segments`'s own UnsupportedMediaError path needs
bytes that pass ffprobe's stream check but still aren't decodable as the
declared content-type — easiest to construct directly, not through HTTP.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from acquisition import MediaAcquirer
from acquisition.artifact import MediaArtifact
from embedding.config import SegmentSamplingConfig
from embedding.dinov2_engine import DINOv2EmbeddingEngine
from target.cache import FilesystemEmbeddingCache
from target.registry import TargetRegistry
from target.segment_cache import FilesystemSegmentEmbeddingCache
from work_queue.keys import state_key
from work_queue.producer import JobProducer
from work_queue.results import ResultDecision, ResultStore
from work_queue.state import JobStatus
from worker.fingerprint_worker import Worker
from worker.matching_handler import build_matching_handler

FIXTURES_DIR = Path(__file__).parent / "fixtures"
TINY_VIDEO = FIXTURES_DIR / "tiny_video.mp4"

BLOCK_MS = 200
LEASE_MS = 5000


class _FakeAcquirer:
    """Duck-types `MediaAcquirer.acquire` without going through real HTTP
    download or ffprobe validation — see module docstring."""

    def __init__(self, artifact: MediaArtifact):
        self._artifact = artifact

    def acquire(self, url: str) -> MediaArtifact:
        return self._artifact


def _video_artifact(path: Path, content_type: str = "video/mp4") -> MediaArtifact:
    return MediaArtifact(
        local_path=path,
        original_url="local://fixture",
        final_url="local://fixture",
        content_type=content_type,
        byte_size=path.stat().st_size,
        checksum_sha256="unused-in-tests",
        acquisition_duration_s=0.0,
    )


def _candidate_artifact(tmp_path: Path, content_type: str = "video/mp4") -> MediaArtifact:
    """A disposable copy of the fixture as the artifact's `local_path` —
    `worker/matching_handler.py`'s handler always `cleanup()`s (deletes)
    the candidate artifact when it's done, so the shared fixture file
    itself must never be handed to it directly (see git history: it was
    deleted mid-suite the first time this test used TINY_VIDEO directly)."""
    copy_path = tmp_path / "candidate.mp4"
    copy_path.write_bytes(TINY_VIDEO.read_bytes())
    return _video_artifact(copy_path, content_type)


def _spec_for(engine: DINOv2EmbeddingEngine):
    result = engine.embed_video_segments(_video_artifact(TINY_VIDEO))
    return result.to_embedding_spec()


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


def _real_handler(engine, registry):
    # allow_private_networks=True: this suite runs against the loopback
    # media_server fixture, which Phase 13A's SSRF guard would otherwise
    # reject by default.
    acquirer = MediaAcquirer(connect_timeout_s=5.0, read_timeout_s=5.0, allow_private_networks=True)
    return build_matching_handler(acquirer, engine, registry)


# -- end-to-end, real inference ---------------------------------------------


def test_self_match_is_detected_end_to_end(redis_client, make_job, media_server, engine, registry):
    """The candidate is byte-identical to the target -> every segment
    matches with similarity ~1.0, well past every default threshold."""
    registry.register_target("target-1", "v1", str(TINY_VIDEO))

    job = make_job(media_url=media_server.url("/video"), target_id="target-1", target_version="v1")
    JobProducer(redis_client).enqueue(job)
    worker = _worker(redis_client)

    entry = worker.claim_one()
    worker.process_claim(entry, _real_handler(engine, registry))

    state = redis_client.hgetall(state_key(job.job_id))
    assert state["status"] == JobStatus.COMPLETED

    record = ResultStore(redis_client).get(job.job_id)
    assert record is not None
    assert record["decision"] == ResultDecision.MATCH
    assert record["algorithm"] == "dinov2"
    assert float(record["confidence"]) > 0.99

    evidence = json.loads(record["evidence"])
    assert len(evidence) == 1
    assert evidence[0]["technique"] == "dinov2"
    assert evidence[0]["matcher_version"] == "temporal_v1"
    assert evidence[0]["matched"] is True
    assert evidence[0]["detail"]["matched_segment_count"] == 4
    assert evidence[0]["detail"]["temporal_offset_s"] == pytest.approx(0.0, abs=1e-6)


def test_target_segment_embedding_is_cached_after_first_job(redis_client, make_job, media_server, engine, registry):
    registry.register_target("target-1", "v1", str(TINY_VIDEO))
    spec = _spec_for(engine)
    assert registry.has_compatible_segment_embedding("target-1", "v1", spec) is False

    job = make_job(job_id="job-cache", media_url=media_server.url("/video"), target_id="target-1", target_version="v1")
    JobProducer(redis_client).enqueue(job)
    worker = _worker(redis_client)

    entry = worker.claim_one()
    worker.process_claim(entry, _real_handler(engine, registry))

    assert registry.has_compatible_segment_embedding("target-1", "v1", spec) is True


# -- error paths (fake acquirer, no HTTP/ffprobe) ---------------------------


def test_candidate_embedding_failure_yields_processing_failure_result(engine, registry, make_job, tmp_path):
    registry.register_target("target-1", "v1", str(TINY_VIDEO))

    garbage = tmp_path / "not-really-a-video.mp4"
    garbage.write_bytes(b"not a real video, just garbage bytes" * 10)
    artifact = _video_artifact(garbage)

    handler = build_matching_handler(_FakeAcquirer(artifact), engine, registry)
    job = make_job(target_id="target-1", target_version="v1")

    result = handler(job)

    assert result.decision == ResultDecision.PROCESSING_FAILURE
    assert result.algorithm == "dinov2"
    assert "candidate embedding failed" in result.summary
    assert not garbage.exists()  # artifact.cleanup() still ran despite the failure


def test_unknown_target_raises_permanent_failure(engine, registry, make_job, tmp_path):
    from worker.fingerprint_worker import PermanentFailure

    artifact = _candidate_artifact(tmp_path)
    handler = build_matching_handler(_FakeAcquirer(artifact), engine, registry)
    job = make_job(target_id="never-registered", target_version="v1")

    with pytest.raises(PermanentFailure):
        handler(job)


def test_deleted_target_raises_permanent_failure_same_as_unknown_target(engine, registry, make_job, tmp_path):
    """Target-management design doc, S13's active-job deletion policy:
    delete is allowed immediately, and a job that reaches a since-deleted
    target must fail exactly the same way a job against a target that was
    never registered fails -- no new code path, no special-cased 'deleted'
    state, just the existing unknown-target -> PermanentFailure behavior
    `test_unknown_target_raises_permanent_failure` already proves."""
    from worker.fingerprint_worker import PermanentFailure

    registry.register_target("target-1", "v1", str(TINY_VIDEO))
    registry.delete_target("target-1", "v1")

    artifact = _candidate_artifact(tmp_path)
    handler = build_matching_handler(_FakeAcquirer(artifact), engine, registry)
    job = make_job(target_id="target-1", target_version="v1")

    with pytest.raises(PermanentFailure):
        handler(job)


def test_job_without_supported_technique_raises_permanent_failure(engine, registry, make_job, tmp_path):
    from worker.fingerprint_worker import PermanentFailure

    artifact = _candidate_artifact(tmp_path)
    handler = build_matching_handler(_FakeAcquirer(artifact), engine, registry)
    job = make_job(techniques=("phash",))

    with pytest.raises(PermanentFailure):
        handler(job)
