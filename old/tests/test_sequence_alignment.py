from old.fingerprint.segment_fingerprint import AudioSegmentFingerprint, VideoSegmentFingerprint
from old.matcher.sequence_alignment import align_audio_segments_dtw
from old.matcher.sequence_alignment import align_audio_segments_offset_xcorr
from old.matcher.sequence_alignment import align_video_segments_constrained
from old.matcher.sequence_alignment import align_video_segments_dtw


def _v(digest: str) -> VideoSegmentFingerprint:
    return VideoSegmentFingerprint(start_second=0.0, end_second=1.0, digest=digest, frame_count=2)


def _a(rms: float, zcr: float) -> AudioSegmentFingerprint:
    return AudioSegmentFingerprint(
        start_second=0.0,
        end_second=1.0,
        digest="x",
        rms=rms,
        zero_crossing_rate=zcr,
    )


def test_align_video_segments_constrained_finds_best_offset():
    target = [_v("a" * 40), _v("b" * 40), _v("c" * 40), _v("d" * 40)]
    candidate = [_v("b" * 40), _v("c" * 40)]

    result = align_video_segments_constrained(target, candidate, candidate_segment_seconds=2.0)

    assert result.target_start_index == 1
    assert result.target_end_index == 2
    assert result.offset_seconds == 2.0
    assert result.similarity > 0.9


def test_align_video_segments_dtw_returns_similarity_and_range():
    target = [_v("a" * 40), _v("b" * 40), _v("c" * 40)]
    candidate = [_v("a" * 40), _v("c" * 40)]

    result = align_video_segments_dtw(
        target,
        candidate,
        candidate_segment_seconds=2.0,
        band_ratio=0.5,
    )

    assert 0.0 <= result.similarity <= 1.0
    assert result.target_start_index <= result.target_end_index


def test_align_audio_segments_offset_xcorr_finds_best_offset():
    target = [_a(0.2, 0.1), _a(0.4, 0.2), _a(0.6, 0.3)]
    candidate = [_a(0.4, 0.2), _a(0.6, 0.3)]

    result = align_audio_segments_offset_xcorr(target, candidate, audio_segment_seconds=1.5)

    assert result.target_start_index == 1
    assert result.target_end_index == 2
    assert result.offset_seconds == 1.5
    assert result.similarity > 0.9


def test_align_audio_segments_dtw_returns_similarity_and_range():
    target = [_a(0.2, 0.1), _a(0.3, 0.12), _a(0.4, 0.14)]
    candidate = [_a(0.2, 0.1), _a(0.4, 0.14)]

    result = align_audio_segments_dtw(
        target,
        candidate,
        audio_segment_seconds=1.5,
        band_ratio=0.5,
    )

    assert 0.0 <= result.similarity <= 1.0
    assert result.target_start_index <= result.target_end_index
