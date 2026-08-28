"""target/build.py:build_target -- the explicit, operator-triggered
eager-build orchestration (docs/architecture/target-eager-build-audit.md,
Part B).

No DINOv2, no real media, no ffmpeg -- a synthetic engine stands in for
`DINOv2EmbeddingEngine`, mirroring tests/test_target_build_on_miss.py's
established style for testing `get_or_build_segment_embedding` callers.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from embedding.config import PreprocessingConfig, SegmentSamplingConfig
from embedding.errors import InferenceError, UnsupportedMediaError
from embedding.result import SegmentEmbedding
from target.build import build_target
from target.cache import FilesystemEmbeddingCache
from target.errors import TargetNotFoundError
from target.registry import TargetRegistry
from target.segment_cache import FilesystemSegmentEmbeddingCache

SEGMENTS = (
    SegmentEmbedding(segment_index=0, start_time=0.0, end_time=5.0, vector=(0.1, 0.2, 0.3)),
    SegmentEmbedding(segment_index=1, start_time=5.0, end_time=10.0, vector=(0.4, 0.5, 0.6)),
)
COARSE_VECTOR = (0.4, 0.5, 0.6)


class FakeEngine:
    """Stands in for `DINOv2EmbeddingEngine`: exposes exactly the attributes
    `target.build._segment_spec_for_engine` reads, plus `embed_video_segments`.
    """

    def __init__(self, raises: Exception = None):
        self.model_id = "dinov2-synthetic"
        self.model_version = "v1"
        self.preprocessing_config = PreprocessingConfig()
        self.segment_sampling_config = SegmentSamplingConfig()
        self._raises = raises
        self.calls = 0
        self.timeouts_seen = []

    def embed_video_segments(self, artifact, on_frame=None, timeout="unset"):
        self.calls += 1
        self.timeouts_seen.append(timeout)
        if self._raises is not None:
            raise self._raises
        if on_frame is not None:
            for index in range(1, len(SEGMENTS) + 1):
                on_frame(index, len(SEGMENTS))
        return SimpleNamespace(segments=SEGMENTS, coarse_vector=COARSE_VECTOR)


def _write(tmp_path, name="movie.mp4", content: bytes = b"target bytes"):
    path = tmp_path / name
    path.write_bytes(content)
    return path


@pytest.fixture
def registry(redis_client, tmp_path):
    pooled = FilesystemEmbeddingCache(tmp_path / "embedding-cache")
    segments = FilesystemSegmentEmbeddingCache(tmp_path / "segment-cache")
    return TargetRegistry(redis_client, pooled, segments)


def test_build_target_success_builds_and_registers(registry, tmp_path):
    registry.register_target("blast", "v1", str(_write(tmp_path)))
    engine = FakeEngine()

    result = build_target(registry, engine, "blast", "v1")

    assert result.already_built is False
    assert result.target_id == "blast"
    assert result.target_version == "v1"
    assert len(result.entry.segments) == 2
    assert engine.calls == 1


def test_build_target_uses_unbounded_timeout(registry, tmp_path):
    """The explicit, operator-triggered build path must never inherit
    `embed_video_segments`'s own bounded default (`DEFAULT_SEGMENT_FFMPEG_TIMEOUT_S`)
    -- it must explicitly request `timeout=None` (no ffmpeg subprocess
    timeout at all), unlike the worker's lazy build-on-miss path (see
    `tests/test_matching_handler.py`'s equivalent bounded-timeout test)."""
    registry.register_target("blast", "v1", str(_write(tmp_path)))
    engine = FakeEngine()

    build_target(registry, engine, "blast", "v1")

    assert engine.timeouts_seen == [None]


def test_build_target_forwards_on_frame_callback_when_a_build_actually_runs(registry, tmp_path):
    """Observability audit, "Progress Display": `on_frame` is forwarded to
    the engine only when a build actually runs, and not at all otherwise
    (default None keeps every other call site's exact prior behavior)."""
    registry.register_target("blast", "v1", str(_write(tmp_path)))
    engine = FakeEngine()
    calls = []

    result = build_target(registry, engine, "blast", "v1", on_frame=lambda index, total: calls.append((index, total)))

    assert result.already_built is False
    assert calls == [(1, 2), (2, 2)]


def test_build_target_on_frame_not_invoked_on_cache_hit(registry, tmp_path):
    registry.register_target("blast", "v1", str(_write(tmp_path)))
    build_target(registry, FakeEngine(), "blast", "v1")  # warm the cache

    calls = []
    result = build_target(
        registry, FakeEngine(), "blast", "v1", on_frame=lambda index, total: calls.append((index, total))
    )

    assert result.already_built is True
    assert calls == []  # no build ran, so on_frame was never called


def test_build_target_not_found_raises_without_calling_engine(registry):
    engine = FakeEngine()

    with pytest.raises(TargetNotFoundError):
        build_target(registry, engine, "nope", "v1")

    assert engine.calls == 0


def test_build_target_is_idempotent_already_built_skips_engine(registry, tmp_path):
    registry.register_target("blast", "v1", str(_write(tmp_path)))
    engine = FakeEngine()
    first = build_target(registry, engine, "blast", "v1")
    assert first.already_built is False
    assert engine.calls == 1

    second = build_target(registry, engine, "blast", "v1")

    assert second.already_built is True
    assert engine.calls == 1  # engine not invoked again
    assert second.entry.segments == first.entry.segments


def test_build_target_media_failure_propagates_and_leaves_cache_empty(registry, tmp_path):
    registry.register_target("blast", "v1", str(_write(tmp_path)))
    engine = FakeEngine(raises=UnsupportedMediaError("ffmpeg timed out extracting segment frames"))

    with pytest.raises(UnsupportedMediaError):
        build_target(registry, engine, "blast", "v1")

    from target.build import _segment_spec_for_engine

    assert registry.has_compatible_segment_embedding("blast", "v1", _segment_spec_for_engine(engine)) is False


def test_build_target_inference_failure_propagates(registry, tmp_path):
    registry.register_target("blast", "v1", str(_write(tmp_path)))
    engine = FakeEngine(raises=InferenceError("DINOv2 forward pass failed"))

    with pytest.raises(InferenceError):
        build_target(registry, engine, "blast", "v1")


def test_build_target_retry_after_failure_performs_a_full_build(registry, tmp_path):
    """A failed build must not leave the target half-built or unretryable --
    the lock is released on exception (target/registry.py's existing
    guarantee) and the next attempt performs a complete build from scratch,
    not a resume."""
    registry.register_target("blast", "v1", str(_write(tmp_path)))
    failing_engine = FakeEngine(raises=UnsupportedMediaError("boom"))

    with pytest.raises(UnsupportedMediaError):
        build_target(registry, failing_engine, "blast", "v1")

    working_engine = FakeEngine()
    result = build_target(registry, working_engine, "blast", "v1")

    assert result.already_built is False
    assert working_engine.calls == 1
    assert len(result.entry.segments) == 2


def test_build_target_result_is_reused_by_lazy_resolution_without_rebuilding(registry, tmp_path):
    """After an explicit build, the exact call the fingerprint worker makes
    lazily (`TargetRegistry.get_or_build_segment_embedding`) must resolve
    from cache and never invoke its own build callback -- this is the
    operational property the eager `build` command exists to deliver."""
    registry.register_target("blast", "v1", str(_write(tmp_path)))
    engine = FakeEngine()
    build_target(registry, engine, "blast", "v1")

    from target.build import _segment_spec_for_engine

    spec = _segment_spec_for_engine(engine)

    def must_not_be_called(record):
        raise AssertionError("lazy resolution must not rebuild an already-built target")

    entry = registry.get_or_build_segment_embedding("blast", "v1", spec, must_not_be_called)
    assert len(entry.segments) == 2
