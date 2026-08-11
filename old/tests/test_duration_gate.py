from old.matcher.duration_gate import should_reject_for_short_duration


def test_duration_gate_rejects_below_threshold():
    rejected, reason = should_reject_for_short_duration(12.4, 19)
    assert rejected is True
    assert "below threshold" in (reason or "")


def test_duration_gate_accepts_equal_or_above_threshold():
    rejected, _ = should_reject_for_short_duration(19.0, 19)
    assert rejected is False

    rejected, _ = should_reject_for_short_duration(45.2, 19)
    assert rejected is False


def test_duration_gate_minus_one_disables_rejection():
    rejected, reason = should_reject_for_short_duration(5.0, -1)
    assert rejected is False
    assert reason is None
