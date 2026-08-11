from old.matcher.multi_stage_fingerprint import run_staged_fingerprint


class _VideoSeg:
    def __init__(self, digest: str):
        self.digest = digest


class _AudioSeg:
    def __init__(self, rms: float, zcr: float):
        self.rms = rms
        self.zero_crossing_rate = zcr


class _Align:
    def __init__(self, *, method: str, similarity: float, offset_seconds: float):
        self.method = method
        self.similarity = similarity
        self.target_start_index = 0
        self.target_end_index = 1
        self.offset_seconds = offset_seconds


def test_run_staged_fingerprint_emits_all_stages(tmp_path, monkeypatch):
    target = tmp_path / "target.mp4"
    candidate = tmp_path / "candidate.mp4"

    payload = b"A" * 2048
    target.write_bytes(payload)
    candidate.write_bytes(payload)

    monkeypatch.setattr(
        "matcher.multi_stage_fingerprint.extract_video_segment_fingerprints",
        lambda path, **_: [_VideoSeg("a" * 40), _VideoSeg("b" * 40), _VideoSeg("c" * 40)]
        if "target" in path
        else [_VideoSeg("b" * 40), _VideoSeg("c" * 40)],
    )
    monkeypatch.setattr(
        "matcher.multi_stage_fingerprint.extract_audio_segment_fingerprints",
        lambda path, **_: [_AudioSeg(0.3, 0.2), _AudioSeg(0.35, 0.22), _AudioSeg(0.4, 0.25)]
        if "target" in path
        else [_AudioSeg(0.35, 0.22), _AudioSeg(0.4, 0.25)],
    )

    result = run_staged_fingerprint(
        target_title="Blast",
        candidate_url="https://cdn.example/blast-copy.mp4",
        candidate_path=str(candidate),
        target_path=str(target),
        low_threshold=0.2,
        high_threshold=0.65,
    )

    stage_names = [item.stage_name for item in result.outcomes]
    assert stage_names == [
        "stage0_sanitization",
        "stage1_metadata",
        "stage2_visual_quick",
        "stage3_audio_fingerprint",
        "stage4_temporal_alignment",
    ]
    assert 0.0 <= result.piracy_score <= 1.0
    assert result.final_status in {"matched", "uncertain_manual_review", "no_match_pending_review", "failed"}


def test_run_staged_fingerprint_supports_selected_techniques(tmp_path):
    target = tmp_path / "target.mp4"
    candidate = tmp_path / "candidate.mp4"

    payload = b"A" * 2048
    target.write_bytes(payload)
    candidate.write_bytes(payload)

    result = run_staged_fingerprint(
        target_title="Blast",
        candidate_url="https://cdn.example/blast-copy.mp4",
        candidate_path=str(candidate),
        target_path=str(target),
        low_threshold=0.2,
        high_threshold=0.65,
        enabled_techniques={"visual"},
    )

    by_stage = {item.stage_name: item for item in result.outcomes}
    assert by_stage["stage2_visual_quick"].decision in {"pass", "weak"}
    assert by_stage["stage1_metadata"].decision == "skipped"
    assert by_stage["stage3_audio_fingerprint"].decision == "skipped"
    assert by_stage["stage4_temporal_alignment"].decision == "skipped"


def test_run_staged_fingerprint_rejects_unknown_techniques(tmp_path):
    target = tmp_path / "target.mp4"
    candidate = tmp_path / "candidate.mp4"
    target.write_bytes(b"A" * 256)
    candidate.write_bytes(b"A" * 256)

    try:
        run_staged_fingerprint(
            target_title="Blast",
            candidate_url="https://cdn.example/blast-copy.mp4",
            candidate_path=str(candidate),
            target_path=str(target),
            low_threshold=0.2,
            high_threshold=0.65,
            enabled_techniques={"made_up"},
        )
    except ValueError as exc:
        assert "Unknown techniques" in str(exc)
    else:
        raise AssertionError("Expected ValueError for unknown technique")


def test_run_staged_fingerprint_rejects_unknown_alignment_method(tmp_path):
    target = tmp_path / "target.mp4"
    candidate = tmp_path / "candidate.mp4"
    target.write_bytes(b"A" * 256)
    candidate.write_bytes(b"A" * 256)

    try:
        run_staged_fingerprint(
            target_title="Blast",
            candidate_url="https://cdn.example/blast-copy.mp4",
            candidate_path=str(candidate),
            target_path=str(target),
            low_threshold=0.2,
            high_threshold=0.65,
            sequence_alignment_method="invalid",
        )
    except ValueError as exc:
        assert "Unknown alignment method" in str(exc)
    else:
        raise AssertionError("Expected ValueError for unknown alignment method")


def test_run_staged_fingerprint_rejects_unknown_audio_alignment_method(tmp_path):
    target = tmp_path / "target.mp4"
    candidate = tmp_path / "candidate.mp4"
    target.write_bytes(b"A" * 256)
    candidate.write_bytes(b"A" * 256)

    try:
        run_staged_fingerprint(
            target_title="Blast",
            candidate_url="https://cdn.example/blast-copy.mp4",
            candidate_path=str(candidate),
            target_path=str(target),
            low_threshold=0.2,
            high_threshold=0.65,
            audio_alignment_method="invalid",
        )
    except ValueError as exc:
        assert "Unknown audio alignment method" in str(exc)
    else:
        raise AssertionError("Expected ValueError for unknown audio alignment method")


def test_run_staged_fingerprint_uses_dtw_methods(tmp_path, monkeypatch):
    target = tmp_path / "target.mp4"
    candidate = tmp_path / "candidate.mp4"

    payload = b"A" * 2048
    target.write_bytes(payload)
    candidate.write_bytes(payload)

    monkeypatch.setattr(
        "matcher.multi_stage_fingerprint.extract_video_segment_fingerprints",
        lambda _path, **_: [_VideoSeg("a" * 40), _VideoSeg("b" * 40), _VideoSeg("c" * 40)],
    )
    monkeypatch.setattr(
        "matcher.multi_stage_fingerprint.extract_audio_segment_fingerprints",
        lambda _path, **_: [_AudioSeg(0.3, 0.1), _AudioSeg(0.31, 0.11), _AudioSeg(0.32, 0.12)],
    )

    result = run_staged_fingerprint(
        target_title="Blast",
        candidate_url="https://cdn.example/blast-copy.mp4",
        candidate_path=str(candidate),
        target_path=str(target),
        low_threshold=0.2,
        high_threshold=0.65,
        sequence_alignment_method="dtw",
        audio_alignment_method="dtw",
    )

    by_stage = {item.stage_name: item for item in result.outcomes}
    assert by_stage["stage2_visual_quick"].details["alignment_method"] == "dtw"
    assert by_stage["stage3_audio_fingerprint"].details["alignment_method"] == "dtw"


def test_run_staged_fingerprint_promotes_strong_multimodal_clip_match(tmp_path, monkeypatch):
    target = tmp_path / "target.mp4"
    candidate = tmp_path / "candidate.mp4"

    payload = b"A" * 2048
    target.write_bytes(payload)
    candidate.write_bytes(payload)

    monkeypatch.setattr(
        "matcher.multi_stage_fingerprint.extract_video_segment_fingerprints",
        lambda _path, **_: [_VideoSeg("a" * 40), _VideoSeg("b" * 40)],
    )
    monkeypatch.setattr(
        "matcher.multi_stage_fingerprint.extract_audio_segment_fingerprints",
        lambda _path, **_: [_AudioSeg(0.2, 0.1), _AudioSeg(0.3, 0.15)],
    )
    monkeypatch.setattr(
        "matcher.multi_stage_fingerprint.align_video_segments_constrained",
        lambda *_args, **_kwargs: _Align(method="constrained", similarity=0.58, offset_seconds=2100.0),
    )
    monkeypatch.setattr(
        "matcher.multi_stage_fingerprint.align_audio_segments_offset_xcorr",
        lambda *_args, **_kwargs: _Align(method="offset_xcorr", similarity=0.95, offset_seconds=680.0),
    )
    monkeypatch.setattr(
        "matcher.multi_stage_fingerprint.get_media_probe",
        lambda path: {"duration_seconds": 60.0, "width": 1280, "height": 720}
        if "candidate" in path
        else {"duration_seconds": 120.0, "width": 1280, "height": 720},
    )

    result = run_staged_fingerprint(
        target_title="Blast",
        candidate_url="https://cdn.example/notitle.mp4",
        candidate_path=str(candidate),
        target_path=str(target),
        low_threshold=0.2,
        high_threshold=0.9,
        candidate_segment_seconds=150.0,
    )

    assert result.piracy_score < 0.9
    assert result.final_status == "matched"


def test_run_staged_fingerprint_uses_precomputed_target_segments(tmp_path, monkeypatch):
    target = tmp_path / "target.mp4"
    candidate = tmp_path / "candidate.mp4"

    payload = b"A" * 2048
    target.write_bytes(payload)
    candidate.write_bytes(payload)

    precomputed_video = [_VideoSeg("a" * 40), _VideoSeg("b" * 40), _VideoSeg("c" * 40)]
    precomputed_audio = [_AudioSeg(0.2, 0.1), _AudioSeg(0.3, 0.2), _AudioSeg(0.35, 0.25)]

    def _extract_video(path, **_):
        if "target" in path:
            raise AssertionError("target video segments should come from precomputed cache")
        return [_VideoSeg("b" * 40), _VideoSeg("c" * 40)]

    def _extract_audio(path, **_):
        if "target" in path:
            raise AssertionError("target audio segments should come from precomputed cache")
        return [_AudioSeg(0.3, 0.2), _AudioSeg(0.35, 0.25)]

    monkeypatch.setattr("matcher.multi_stage_fingerprint.extract_video_segment_fingerprints", _extract_video)
    monkeypatch.setattr("matcher.multi_stage_fingerprint.extract_audio_segment_fingerprints", _extract_audio)

    result = run_staged_fingerprint(
        target_title="Blast",
        candidate_url="https://cdn.example/blast-copy.mp4",
        candidate_path=str(candidate),
        target_path=str(target),
        low_threshold=0.2,
        high_threshold=0.65,
        precomputed_target_video_segments=precomputed_video,
        precomputed_target_audio_segments=precomputed_audio,
    )

    assert result.final_status in {"matched", "uncertain_manual_review", "no_match_pending_review", "failed"}
