from fingerprinter.matcher.media_type_gate import should_reject_non_video


def test_media_type_gate_rejects_image_extension():
    rejected, reason = should_reject_non_video("unknown", "https://cdn.example/poster.png")
    assert rejected is True
    assert "image asset" in (reason or "")


def test_media_type_gate_accepts_video_extensions():
    rejected, reason = should_reject_non_video("unknown", "https://cdn.example/movie.mp4")
    assert rejected is False
    assert reason is None


def test_media_type_gate_rejects_unsupported_type():
    rejected, reason = should_reject_non_video("document", "https://cdn.example/readme.pdf")
    assert rejected is True
    assert "unsupported media_type" in (reason or "")


def test_media_type_gate_rejects_onion_when_tor_disabled():
    rejected, reason = should_reject_non_video(
        "video",
        "http://examplehiddenserviceabcdef.onion/movie.mp4",
        enable_tor=False,
    )
    assert rejected is True
    assert "requires Tor support" in (reason or "")


def test_media_type_gate_allows_onion_when_tor_enabled():
    rejected, reason = should_reject_non_video(
        "video",
        "http://examplehiddenserviceabcdef.onion/movie.mp4",
        enable_tor=True,
    )
    assert rejected is False
    assert reason is None
