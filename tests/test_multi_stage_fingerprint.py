from matcher.multi_stage_fingerprint import run_staged_fingerprint


def test_run_staged_fingerprint_emits_all_stages(tmp_path):
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
