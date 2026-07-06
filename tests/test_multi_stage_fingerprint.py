from matcher.multi_stage_fingerprint import run_staged_fingerprint


class _VideoSeg:
    def __init__(self, digest: str):
        self.digest = digest


class _AudioSeg:
    def __init__(self, rms: float, zcr: float):
        self.rms = rms
        self.zero_crossing_rate = zcr


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
