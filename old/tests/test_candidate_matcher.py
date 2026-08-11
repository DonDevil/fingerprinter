from old.matcher.candidate_matcher import (
    build_container_signature,
    classify_match_score,
    score_title_token_overlap,
)


def test_build_container_signature_extracts_extensions():
    result = build_container_signature(
        media_url="https://cdn.example/library/The.Matrix.1999.1080p.mp4",
        local_path="storage/downloads/asset_11.mp4",
        duration_seconds=8100.5,
    )
    assert result["url_extension"] == ".mp4"
    assert result["file_extension"] == ".mp4"
    assert result["duration_seconds"] == 8100.5


def test_score_title_token_overlap_scores_overlap():
    result = score_title_token_overlap(
        target_title="The Matrix",
        candidate_text="https://cdn.example/the-matrix-1999-1080p.mkv",
    )
    assert result["score"] > 0.5
    assert "matrix" in result["matched_tokens"]


def test_classify_match_score_ranges():
    status, _ = classify_match_score(0.8, low_threshold=0.2, high_threshold=0.65)
    assert status == "sampled"

    status, _ = classify_match_score(0.1, low_threshold=0.2, high_threshold=0.65)
    assert status == "no_match_pending_review"

    status, _ = classify_match_score(0.4, low_threshold=0.2, high_threshold=0.65)
    assert status == "uncertain_manual_review"
