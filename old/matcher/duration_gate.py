from __future__ import annotations


def should_reject_for_short_duration(
    duration_seconds: float | None,
    threshold_seconds: int,
) -> tuple[bool, str | None]:
    """Return whether a sample should be rejected for short duration.

    Rules:
    - threshold == -1 disables duration rejection.
    - Unknown duration never triggers automatic rejection.
    - duration < threshold triggers rejection for threshold >= 0.
    """

    if threshold_seconds == -1:
        return False, None

    if threshold_seconds < -1:
        return False, f"Invalid threshold {threshold_seconds}; expected -1 or >=0"

    if duration_seconds is None:
        return False, None

    if duration_seconds < float(threshold_seconds):
        return (
            True,
            f"Rejected: duration {duration_seconds:.2f}s is below threshold {threshold_seconds}s",
        )

    return False, None
