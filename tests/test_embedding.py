"""Phase 7 — DINOv2 embedding engine tests.

Runs entirely offline against the two tiny local fixtures generated for
this phase (`tests/fixtures/tiny_image.png`, `tests/fixtures/tiny_video.mp4`
— both created via `ffmpeg -f lavfi testsrc`, no external downloads) and
the `facebook/dinov2-base` weights already cached locally. Every engine in
this module is constructed with `device="cpu"` for determinism/CI-safety,
except the one dedicated CUDA test, which self-skips if no GPU is present.

No test here touches Redis directly — `TargetEmbeddingCache`/
`FilesystemEmbeddingCache` (Phase 6) have no Redis dependency either, only
`TargetRegistry` does, and this phase doesn't use `TargetRegistry`.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest
import torch

from acquisition.artifact import MediaArtifact
from embedding.config import IMAGE_SAMPLING_CONFIG, PreprocessingConfig, SamplingConfig, SegmentSamplingConfig
from embedding.dinov2_engine import DINOv2EmbeddingEngine
from embedding.errors import DeviceUnavailableError, UnsupportedMediaError
from embedding.frames import extract_frames, extract_segment_frames, make_frames_dir
from target.cache import FilesystemEmbeddingCache
from target.segment_cache import FilesystemSegmentEmbeddingCache
from target.versioning import EmbeddingSpec

FIXTURES_DIR = Path(__file__).parent / "fixtures"
TINY_IMAGE = FIXTURES_DIR / "tiny_image.png"
TINY_VIDEO = FIXTURES_DIR / "tiny_video.mp4"


def _image_artifact(path: Path = TINY_IMAGE) -> MediaArtifact:
    return MediaArtifact(
        local_path=path,
        original_url="local://fixture",
        final_url="local://fixture",
        content_type="image/png",
        byte_size=path.stat().st_size if path.exists() else 0,
        checksum_sha256="unused-in-tests",
        acquisition_duration_s=0.0,
    )


def _video_artifact(path: Path = TINY_VIDEO) -> MediaArtifact:
    return MediaArtifact(
        local_path=path,
        original_url="local://fixture",
        final_url="local://fixture",
        content_type="video/mp4",
        byte_size=path.stat().st_size if path.exists() else 0,
        checksum_sha256="unused-in-tests",
        acquisition_duration_s=0.0,
    )


@pytest.fixture(scope="module")
def cpu_engine() -> DINOv2EmbeddingEngine:
    """One engine instance shared across tests in this module — exercises
    "model loads once per engine instance" implicitly (every test below
    reuses the same loaded model) alongside the dedicated counting test."""
    return DINOv2EmbeddingEngine(device="cpu", sampling_config=SamplingConfig(fps=2.0, max_frames=4))


# 1. engine initializes correctly
def test_engine_initializes_correctly(cpu_engine):
    assert cpu_engine.embedding_dim == 768
    assert cpu_engine.device == torch.device("cpu")
    assert cpu_engine.model_id == "facebook/dinov2-base"
    assert cpu_engine.model_load_duration_s >= 0.0


# 2. model loads only once per engine instance
def test_model_loads_only_once_per_engine_instance(monkeypatch):
    from transformers import AutoImageProcessor, AutoModel

    processor_calls = {"n": 0}
    model_calls = {"n": 0}
    real_processor_from_pretrained = AutoImageProcessor.from_pretrained
    real_model_from_pretrained = AutoModel.from_pretrained

    def counting_processor(*args, **kwargs):
        processor_calls["n"] += 1
        return real_processor_from_pretrained(*args, **kwargs)

    def counting_model(*args, **kwargs):
        model_calls["n"] += 1
        return real_model_from_pretrained(*args, **kwargs)

    monkeypatch.setattr(AutoImageProcessor, "from_pretrained", counting_processor)
    monkeypatch.setattr(AutoModel, "from_pretrained", counting_model)

    engine = DINOv2EmbeddingEngine(device="cpu")
    assert processor_calls["n"] == 1
    assert model_calls["n"] == 1

    engine.embed(_image_artifact())
    engine.embed(_image_artifact())

    assert processor_calls["n"] == 1
    assert model_calls["n"] == 1


# 3. image embedding succeeds
def test_image_embedding_succeeds(cpu_engine):
    result = cpu_engine.embed(_image_artifact())
    assert result.media_type == "image"
    assert result.frame_count == 1
    assert len(result.vector) == 768


# 4. video sampling is deterministic
def test_video_sampling_is_deterministic():
    sampling = SamplingConfig(fps=2.0, max_frames=4)

    dir_a = make_frames_dir()
    dir_b = make_frames_dir()
    try:
        frames_a = extract_frames(TINY_VIDEO, sampling, dir_a)
        frames_b = extract_frames(TINY_VIDEO, sampling, dir_b)

        assert len(frames_a) == len(frames_b) > 0
        for fa, fb in zip(frames_a, frames_b):
            assert fa.read_bytes() == fb.read_bytes()
    finally:
        for f in dir_a.glob("*"):
            f.unlink(missing_ok=True)
        for f in dir_b.glob("*"):
            f.unlink(missing_ok=True)
        dir_a.rmdir()
        dir_b.rmdir()


# 5. video embedding succeeds
def test_video_embedding_succeeds(cpu_engine):
    result = cpu_engine.embed(_video_artifact())
    assert result.media_type == "video"
    assert result.frame_count > 0
    assert len(result.vector) == 768


# 6. embedding dimensionality is correct
def test_embedding_dimensionality_is_correct(cpu_engine):
    image_result = cpu_engine.embed(_image_artifact())
    video_result = cpu_engine.embed(_video_artifact())
    assert image_result.dimensionality == cpu_engine.embedding_dim == 768
    assert video_result.dimensionality == cpu_engine.embedding_dim == 768
    assert len(image_result.vector) == image_result.dimensionality
    assert len(video_result.vector) == video_result.dimensionality


# 7. normalization behavior is correct
def test_normalization_behavior_is_correct():
    normalized_engine = DINOv2EmbeddingEngine(device="cpu", normalize=True)
    raw_engine = DINOv2EmbeddingEngine(device="cpu", normalize=False)

    normalized_result = normalized_engine.embed(_image_artifact())
    raw_result = raw_engine.embed(_image_artifact())

    normalized_norm = float(np.linalg.norm(normalized_result.vector))
    raw_norm = float(np.linalg.norm(raw_result.vector))

    assert normalized_result.normalized is True
    assert raw_result.normalized is False
    assert normalized_norm == pytest.approx(1.0, abs=1e-5)
    assert raw_norm != pytest.approx(1.0, abs=1e-2)


# 8. preprocessing configuration is represented correctly
def test_preprocessing_configuration_is_represented_correctly(cpu_engine):
    result = cpu_engine.embed(_image_artifact())
    assert result.preprocessing_config == cpu_engine.preprocessing_config
    as_dict = result.preprocessing_config.to_dict()
    assert as_dict["crop_size"] == 224
    assert as_dict["resize_shortest_edge"] == 256
    assert as_dict["normalize_mean"] == [0.485, 0.456, 0.406]
    assert as_dict["normalize_std"] == [0.229, 0.224, 0.225]


# 9. sampling configuration is represented correctly
def test_sampling_configuration_is_represented_correctly(cpu_engine):
    image_result = cpu_engine.embed(_image_artifact())
    video_result = cpu_engine.embed(_video_artifact())

    assert image_result.sampling_config == IMAGE_SAMPLING_CONFIG
    assert image_result.sampling_config.to_dict()["frame_selection"] == "single_image"

    assert video_result.sampling_config == cpu_engine.sampling_config
    as_dict = video_result.sampling_config.to_dict()
    assert as_dict["fps"] == 2.0
    assert as_dict["max_frames"] == 4
    assert as_dict["frame_selection"] == "uniform_time_from_start"


# 10. CPU execution works
def test_cpu_execution_works():
    engine = DINOv2EmbeddingEngine(device="cpu")
    assert engine.device == torch.device("cpu")
    result = engine.embed(_image_artifact())
    assert len(result.vector) == 768


# 11. unavailable requested device fails clearly
def test_unavailable_requested_device_fails_clearly(monkeypatch):
    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
    with pytest.raises(DeviceUnavailableError):
        DINOv2EmbeddingEngine(device="cuda")


# Phase 11: torch_num_threads is additive-only — default (None) must not
# touch the global torch thread pool at all, so a caller who never passes it
# sees identical behavior to every prior phase.
def test_torch_num_threads_defaults_to_untouched():
    before = torch.get_num_threads()
    DINOv2EmbeddingEngine(device="cpu", sampling_config=SamplingConfig(fps=2.0, max_frames=1))
    assert torch.get_num_threads() == before


def test_torch_num_threads_explicit_value_is_applied():
    original = torch.get_num_threads()
    try:
        engine = DINOv2EmbeddingEngine(
            device="cpu", sampling_config=SamplingConfig(fps=2.0, max_frames=1), torch_num_threads=1
        )
        assert engine.torch_num_threads == 1
        assert torch.get_num_threads() == 1
    finally:
        torch.set_num_threads(original)


def test_torch_num_threads_rejects_non_positive_value():
    with pytest.raises(ValueError):
        DINOv2EmbeddingEngine(device="cpu", torch_num_threads=0)


# 12. repeated inference gives numerically consistent output
def test_repeated_inference_gives_numerically_consistent_output(cpu_engine):
    first = cpu_engine.embed(_image_artifact())
    second = cpu_engine.embed(_image_artifact())
    np.testing.assert_allclose(np.array(first.vector), np.array(second.vector), rtol=0, atol=1e-6)


# 13. malformed/unsupported input fails clearly
def test_malformed_unsupported_input_fails_clearly(cpu_engine, tmp_path):
    garbage = tmp_path / "garbage.png"
    garbage.write_bytes(b"not a real image, just garbage bytes" * 10)
    with pytest.raises(UnsupportedMediaError):
        cpu_engine.embed(_image_artifact(garbage))

    with pytest.raises(UnsupportedMediaError):
        cpu_engine.embed(_video_artifact(garbage))

    unsupported_type = MediaArtifact(
        local_path=TINY_IMAGE,
        original_url="local://fixture",
        final_url="local://fixture",
        content_type="application/pdf",
        byte_size=TINY_IMAGE.stat().st_size,
        checksum_sha256="unused-in-tests",
        acquisition_duration_s=0.0,
    )
    with pytest.raises(UnsupportedMediaError):
        cpu_engine.embed(unsupported_type)


# 14. engine does not depend on Redis
def test_engine_does_not_depend_on_redis():
    script = (
        "import sys\n"
        "import embedding\n"
        "from embedding.dinov2_engine import DINOv2EmbeddingEngine\n"
        "assert 'redis' not in sys.modules, sorted(m for m in sys.modules if 'redis' in m)\n"
        "print('OK')\n"
    )
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, proc.stderr
    assert "OK" in proc.stdout


# 15. embedding result can be converted into Phase 6's EmbeddingSpec
def test_embedding_result_can_be_converted_to_embedding_spec(cpu_engine):
    result = cpu_engine.embed(_image_artifact())
    spec = result.to_embedding_spec()

    assert isinstance(spec, EmbeddingSpec)
    assert spec.model_id == result.model_id
    assert spec.model_version == result.model_version
    assert spec.embedding_schema_version == result.embedding_schema_version
    assert spec.preprocessing_config == result.preprocessing_config.to_dict()
    assert spec.sampling_config == result.sampling_config.to_dict()


# 16. synthetic/local embedding can be passed to the Phase 6 cache
def test_embedding_can_be_stored_and_retrieved_from_phase6_cache(cpu_engine, tmp_path):
    result = cpu_engine.embed(_image_artifact())
    spec = result.to_embedding_spec()
    cache = FilesystemEmbeddingCache(tmp_path / "embedding-cache")

    cache.put("target-1", "v1", "content-hash-abc", spec, result.vector)
    entry = cache.get("target-1", "v1", "content-hash-abc", spec)

    assert entry is not None
    np.testing.assert_allclose(np.array(entry.vector), np.array(result.vector), rtol=0, atol=1e-9)
    assert cache.exists("target-1", "v1", "content-hash-abc", spec)


# -- Phase 9: segment-level video embedding ---------------------------------
#
# Small, real-DINO tests against the existing `tiny_video.mp4` fixture (2s
# long) — per the phase brief, "the primary Phase 9 correctness tests
# should operate on deterministic embedding arrays, not expensive DINO
# inference" (see tests/test_matching.py for those). This module only
# checks that `embed_video_segments` produces the right *shape* of output
# against real inference, not matching correctness.


# 17. segment frame extraction spans the whole video, not a capped window
def test_segment_frame_extraction_spans_full_duration():
    sampling = SegmentSamplingConfig(segment_duration_s=0.5)  # 2s video -> 4 segments
    frames_dir = make_frames_dir()
    try:
        frame_paths = extract_segment_frames(TINY_VIDEO, sampling, frames_dir)
        assert len(frame_paths) == 4
    finally:
        for f in frames_dir.glob("*"):
            f.unlink(missing_ok=True)
        frames_dir.rmdir()


# 18. video segment embedding succeeds and covers full duration
def test_video_segment_embedding_succeeds():
    engine = DINOv2EmbeddingEngine(device="cpu", segment_sampling_config=SegmentSamplingConfig(segment_duration_s=0.5))
    result = engine.embed_video_segments(_video_artifact())

    assert result.segment_count == 4
    assert result.total_duration_s == pytest.approx(2.0)
    for i, segment in enumerate(result.segments):
        assert segment.segment_index == i
        assert segment.start_time == pytest.approx(i * 0.5)
        assert segment.end_time == pytest.approx((i + 1) * 0.5)
        assert len(segment.vector) == 768
    assert len(result.coarse_vector) == 768


# 19. coarse vector is a re-normalized mean of the segment vectors, not a
# separate inference pass
def test_coarse_vector_is_pooled_from_segments():
    engine = DINOv2EmbeddingEngine(device="cpu", segment_sampling_config=SegmentSamplingConfig(segment_duration_s=0.5))
    result = engine.embed_video_segments(_video_artifact())

    stacked = np.array([s.vector for s in result.segments])
    expected = stacked.mean(axis=0)
    expected = expected / np.linalg.norm(expected)
    np.testing.assert_allclose(np.array(result.coarse_vector), expected, rtol=0, atol=1e-5)


# 20. segment embedding result converts to a Phase 6-compatible EmbeddingSpec
def test_segment_embedding_result_can_be_converted_to_embedding_spec():
    engine = DINOv2EmbeddingEngine(device="cpu", segment_sampling_config=SegmentSamplingConfig(segment_duration_s=0.5))
    result = engine.embed_video_segments(_video_artifact())
    spec = result.to_embedding_spec()

    assert isinstance(spec, EmbeddingSpec)
    assert spec.sampling_config == result.segment_sampling_config.to_dict()


# 21. segment embedding can be stored/retrieved from the Phase 9 segment cache
def test_segment_embedding_can_be_stored_and_retrieved_from_segment_cache(tmp_path):
    engine = DINOv2EmbeddingEngine(device="cpu", segment_sampling_config=SegmentSamplingConfig(segment_duration_s=0.5))
    result = engine.embed_video_segments(_video_artifact())
    spec = result.to_embedding_spec()
    cache = FilesystemSegmentEmbeddingCache(tmp_path / "segment-cache")

    cache.put("target-1", "v1", "content-hash-abc", spec, result.segments, result.coarse_vector)
    entry = cache.get("target-1", "v1", "content-hash-abc", spec)

    assert entry is not None
    assert len(entry.segments) == result.segment_count
    np.testing.assert_allclose(np.array(entry.coarse_vector), np.array(result.coarse_vector), rtol=0, atol=1e-9)


# 22. embed_video_segments rejects non-video input
def test_embed_video_segments_rejects_non_video_artifact():
    engine = DINOv2EmbeddingEngine(device="cpu", segment_sampling_config=SegmentSamplingConfig(segment_duration_s=0.5))
    with pytest.raises(UnsupportedMediaError):
        engine.embed_video_segments(_image_artifact())
