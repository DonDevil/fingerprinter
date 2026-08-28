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
import logging
from pathlib import Path

import pytest

import embedding.dinov2_engine as dinov2_engine_module
from acquisition import MediaAcquirer
from acquisition.artifact import MediaArtifact
from embedding.config import SegmentSamplingConfig
from embedding.dinov2_engine import DINOv2EmbeddingEngine
from embedding.frames import DEFAULT_SEGMENT_FFMPEG_TIMEOUT_S
from target.cache import FilesystemEmbeddingCache
from target.registry import TargetRegistry
from target.segment_cache import FilesystemSegmentEmbeddingCache
from work_queue.keys import state_key
from work_queue.producer import JobProducer
from work_queue.results import ResultDecision, ResultStore
from work_queue.state import JobStatus
from worker.fingerprint_worker import Worker
from worker.matching_handler import _redact_url, build_matching_handler

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


def test_worker_uses_bounded_ffmpeg_timeout_for_candidate_and_target_build(
    redis_client, make_job, media_server, engine, registry, monkeypatch
):
    """Runtime worker processing must keep the existing bounded
    `DEFAULT_SEGMENT_FFMPEG_TIMEOUT_S` for ffmpeg segment extraction --
    both for the candidate (always) and for a target build-on-miss (this
    job is the target's first) -- unlike `target.cli build`'s explicit
    `timeout=None` override (see tests/test_target_build.py /
    tests/test_target_cli.py's equivalent unbounded-timeout tests)."""
    registry.register_target("target-1", "v1", str(TINY_VIDEO))
    captured_timeouts = []
    real_extract_segment_frames = dinov2_engine_module.extract_segment_frames

    def _spy(media_path, sampling, frames_dir, timeout=DEFAULT_SEGMENT_FFMPEG_TIMEOUT_S):
        captured_timeouts.append(timeout)
        return real_extract_segment_frames(media_path, sampling, frames_dir, timeout=timeout)

    monkeypatch.setattr(dinov2_engine_module, "extract_segment_frames", _spy)

    job = make_job(media_url=media_server.url("/video"), target_id="target-1", target_version="v1")
    JobProducer(redis_client).enqueue(job)
    worker = _worker(redis_client)

    entry = worker.claim_one()
    worker.process_claim(entry, _real_handler(engine, registry))

    # one call for the candidate, one for the target build-on-miss
    assert len(captured_timeouts) == 2
    assert captured_timeouts == [DEFAULT_SEGMENT_FFMPEG_TIMEOUT_S, DEFAULT_SEGMENT_FFMPEG_TIMEOUT_S]


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


# -- DEBUG-mode diagnostics (observability audit) ---------------------------
#
# These assert on structured event names/fields (LogRecord.event /
# .fields, set by worker/observability.py:log_event), not on rendered
# message text or timestamps -- stable across any future formatting change.


def _debug_events(records) -> dict:
    return {r.event: r for r in records if r.name == "worker.matching_handler" and hasattr(r, "event")}


def test_debug_mode_emits_stage_events_with_expected_fields(caplog, engine, registry, make_job, tmp_path):
    caplog.set_level(logging.DEBUG, logger="worker.matching_handler")
    registry.register_target("target-1", "v1", str(TINY_VIDEO))
    artifact = _candidate_artifact(tmp_path)
    handler = build_matching_handler(_FakeAcquirer(artifact), engine, registry)
    job = make_job(target_id="target-1", target_version="v1")

    handler(job)

    events = _debug_events(caplog.records)
    assert events["job_processing_started"].fields["target_id"] == "target-1"
    assert events["job_processing_started"].fields["target_version"] == "v1"
    assert events["job_processing_started"].fields["candidate_url"] == "https://example.com/video.mp4"

    assert events["candidate_acquired"].fields["content_type"] == "video/mp4"
    assert events["candidate_embedded"].fields["candidate_segment_count"] == 4
    assert events["target_resolved"].fields["target_segment_count"] == 4
    assert events["target_resolution_started"].fields["cache_status"] == "miss"

    matching = events["matching_completed"].fields
    assert matching["decision"] == "MATCH"  # byte-identical candidate, same as test_self_match_is_detected
    assert matching["matched_segment_count"] == 4
    assert matching["target_segment_count"] == 4
    assert matching["candidate_segment_count"] == 4
    assert matching["target_coverage_hits"] == pytest.approx(1.0)
    assert matching["target_coverage_span"] == pytest.approx(1.0)
    assert matching["candidate_coverage"] == pytest.approx(1.0)
    assert matching["similarity_threshold"] == pytest.approx(0.90)
    assert matching["min_matched_segments"] == 3


def test_debug_events_are_suppressed_without_debug_logging_enabled(caplog, engine, registry, make_job, tmp_path):
    """Default (INFO-effective) level: none of the new DEBUG diagnostics
    are emitted at all -- normal-mode output stays exactly as concise as
    before this change."""
    registry.register_target("target-1", "v1", str(TINY_VIDEO))
    artifact = _candidate_artifact(tmp_path)
    handler = build_matching_handler(_FakeAcquirer(artifact), engine, registry)
    job = make_job(target_id="target-1", target_version="v1")

    handler(job)

    assert [r for r in caplog.records if r.name == "worker.matching_handler"] == []


def test_debug_mode_reports_cache_miss_then_hit(caplog, engine, registry, make_job, tmp_path):
    registry.register_target("target-1", "v1", str(TINY_VIDEO))
    caplog.set_level(logging.DEBUG, logger="worker.matching_handler")

    handler_a = build_matching_handler(_FakeAcquirer(_candidate_artifact(tmp_path)), engine, registry)
    handler_a(make_job(job_id="job-a", target_id="target-1", target_version="v1"))
    first_status = _debug_events(caplog.records)["target_resolution_started"].fields["cache_status"]
    caplog.clear()

    handler_b = build_matching_handler(_FakeAcquirer(_candidate_artifact(tmp_path)), engine, registry)
    handler_b(make_job(job_id="job-b", target_id="target-1", target_version="v1"))
    second_status = _debug_events(caplog.records)["target_resolution_started"].fields["cache_status"]

    assert first_status == "miss"
    assert second_status == "hit"


def test_debug_mode_logs_stage_failed_on_candidate_embedding_failure(caplog, engine, registry, make_job, tmp_path):
    caplog.set_level(logging.DEBUG, logger="worker.matching_handler")
    registry.register_target("target-1", "v1", str(TINY_VIDEO))
    garbage = tmp_path / "not-really-a-video.mp4"
    garbage.write_bytes(b"not a real video, just garbage bytes" * 10)
    artifact = _video_artifact(garbage)
    handler = build_matching_handler(_FakeAcquirer(artifact), engine, registry)
    job = make_job(target_id="target-1", target_version="v1")

    result = handler(job)

    assert result.decision == ResultDecision.PROCESSING_FAILURE
    failure_event = _debug_events(caplog.records)["stage_failed"]
    assert failure_event.fields["stage"] == "candidate_embedding"
    assert failure_event.fields["error_type"] == "UnsupportedMediaError"


def test_redact_url_strips_credentials_and_query_string():
    assert (
        _redact_url("https://user:pass@example.com:8443/path/to/video.mp4?token=secret123")
        == "https://example.com/path/to/video.mp4"
    )


def test_redact_url_truncates_very_long_urls():
    long_path = "/" + ("a" * 500)
    redacted = _redact_url(f"https://example.com{long_path}")
    assert len(redacted) <= 200
    assert redacted.endswith("...")
