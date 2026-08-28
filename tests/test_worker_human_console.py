"""Tests for `worker/observability.py`'s human-readable console output
(`HumanConsoleHandler`, `configure_console_logging`, `shorten_url`) — the
presentation-only layer added alongside the existing `JsonFormatter`/
`configure_json_logging` machine-readable path, which those two already
have dedicated coverage for in tests/test_worker_observability.py.

These tests render structured events the same way `log_event` produces
them in production (see worker/matching_handler.py, worker/observability.py,
worker/main.py) and assert on the *rendered text*, not on any internal
formatting constant, so they stay meaningful if the exact column widths or
separators are tuned later.
"""
from __future__ import annotations

import io
import logging

from worker.observability import (
    HumanConsoleHandler,
    JsonFormatter,
    configure_console_logging,
    log_event,
    shorten_url,
)


def _make_handler_logger():
    buffer = io.StringIO()
    logger = logging.getLogger(f"test.human_console.{id(buffer)}")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    logger.handlers[:] = [HumanConsoleHandler(buffer)]
    return logger, buffer


# ---------------------------------------------------------------------------
# 1. Job lifecycle renders as readable, correlated lines -- not raw JSON
# ---------------------------------------------------------------------------


def test_job_lifecycle_renders_human_readable_lines_not_json():
    logger, buffer = _make_handler_logger()

    log_event(logger, "job_claimed", job_id="job-1", attempt=1)
    log_event(
        logger, "job_processing_started", level=logging.DEBUG, job_id="job-1",
        target_id="blast", target_version="v1", candidate_url="https://cdn.example.com/x.mp4",
    )
    log_event(logger, "job_completed", job_id="job-1", attempt=1, latency_ms=4830.0, decision="match")

    output = buffer.getvalue()
    assert "{" not in output  # not JSON
    assert "job-1" in output
    assert "blast / v1" in output
    assert "MATCH" in output
    assert "4.83s" in output  # 4830ms rendered as seconds


def test_job_result_uses_matching_decision_when_job_completed_has_none():
    """`job_completed` (INFO) doesn't always carry a `decision` (only set
    when the outcome was a `Result`, see worker/fingerprint_worker.py's two
    `on_job_completed` call sites) -- when it's absent, the DEBUG-only
    `matching_completed` event's decision (already logged a moment earlier
    for the same job_id) fills in the closing [Result] line instead of
    falling back to a meaningless generic label."""
    logger, buffer = _make_handler_logger()

    log_event(logger, "job_claimed", job_id="job-2", attempt=1)
    log_event(
        logger, "matching_completed", level=logging.DEBUG, job_id="job-2",
        target_id="blast", target_version="v1", target_segment_count=100, candidate_segment_count=10,
        matched_segment_count=0, target_coverage_hits=0.0, candidate_coverage=0.0,
        mean_similarity=0.1, coarse_similarity=0.1, temporal_offset_s=0.0,
        similarity_threshold=0.9, min_matched_segments=3, decision="no_match", duration_s=0.01,
    )
    log_event(logger, "job_completed", job_id="job-2", attempt=1, latency_ms=100.0, decision=None)

    output = buffer.getvalue()
    assert "NO_MATCH" in output


def test_job_state_is_cleared_after_terminal_event():
    """Per-job bookkeeping (`_jobs`) must not grow unboundedly across a
    long-running worker -- it's popped as soon as each job's terminal event
    (job_completed/job_failed/job_retry_scheduled/job_permanently_failed) is
    rendered."""
    logger, buffer = _make_handler_logger()
    handler = logger.handlers[0]

    log_event(logger, "job_claimed", job_id="job-3", attempt=1)
    assert "job-3" in handler._jobs

    log_event(logger, "job_completed", job_id="job-3", attempt=1, latency_ms=50.0, decision="match")
    assert "job-3" not in handler._jobs


def test_job_failed_renders_error_type_and_category():
    logger, buffer = _make_handler_logger()

    log_event(logger, "job_claimed", job_id="job-4", attempt=1)
    log_event(
        logger, "job_failed", job_id="job-4", attempt=1,
        error_type="NetworkError", error_category="transient_acquisition_failure", latency_ms=200.0,
    )

    output = buffer.getvalue()
    assert "FAILED" in output
    assert "NetworkError" in output
    assert "transient_acquisition_failure" in output


# ---------------------------------------------------------------------------
# 2. Matching statistics are all present in the rendered line
# ---------------------------------------------------------------------------


def test_matching_completed_renders_all_key_statistics():
    logger, buffer = _make_handler_logger()

    log_event(
        logger, "matching_completed", level=logging.DEBUG, job_id="job-5",
        target_id="blast", target_version="v1", target_segment_count=1699, candidate_segment_count=12,
        matched_segment_count=4, target_coverage_hits=0.0024, target_coverage_span=0.003,
        candidate_coverage=0.3333, mean_similarity=0.9366, coarse_similarity=0.7816,
        temporal_offset_s=650.0, similarity_threshold=0.9, min_matched_segments=3,
        decision="MATCH", duration_s=0.05,
    )

    line = buffer.getvalue()
    assert "4/12 matched" in line
    assert "33.33%" in line  # candidate coverage
    assert "0.24%" in line  # target coverage
    assert "0.9366" in line  # mean similarity
    assert "0.7816" in line  # coarse similarity
    assert "650.0s" in line  # temporal offset
    assert "0.9000" in line  # threshold
    assert "MATCH" in line


# ---------------------------------------------------------------------------
# 3. embedding_progress collapses into one updating line, not N lines
# ---------------------------------------------------------------------------


def test_embedding_progress_checkpoints_collapse_into_one_line():
    logger, buffer = _make_handler_logger()

    total = 12
    for frame in (0, 1, 2, 3, 12):
        log_event(
            logger, "embedding_progress", level=logging.DEBUG, job_id="job-6",
            stage="candidate_embedding", frame=frame, total=total,
            percent=round(100.0 * frame / total, 1),
        )

    raw = buffer.getvalue()
    # Every checkpoint after the first is a carriage-return update, not a
    # new line -- exactly one "\n" total (the terminator on the final,
    # frame==total checkpoint), never one newline per checkpoint.
    assert raw.count("\n") == 1
    assert raw.count("\r") == 5
    assert raw.endswith("12/12 (100.0%)\n")


def test_embedding_progress_final_checkpoint_terminates_the_line():
    logger, buffer = _make_handler_logger()

    log_event(
        logger, "embedding_progress", level=logging.DEBUG, job_id="job-7",
        stage="target_build", frame=10, total=10, percent=100.0,
    )
    # A subsequent, unrelated line must start cleanly on its own line, not
    # get appended after the progress bar.
    log_event(logger, "job_claimed", job_id="job-8", attempt=1)

    lines = buffer.getvalue().splitlines()
    assert any("Target build" in line for line in lines)
    assert any("job-8" in line for line in lines)


def test_non_progress_event_flushes_an_open_progress_line_first():
    """If a progress bar is left "open" (no trailing newline yet, e.g. the
    worker crashed mid-stage) and a different event needs to be rendered,
    the handler must not mangle the two together on one terminal line."""
    logger, buffer = _make_handler_logger()

    log_event(
        logger, "embedding_progress", level=logging.DEBUG, job_id="job-9",
        stage="candidate_embedding", frame=3, total=12, percent=25.0,
    )
    log_event(logger, "stage_failed", level=logging.DEBUG, job_id="job-9", stage="candidate_embedding", error_type="InferenceError")

    output = buffer.getvalue()
    assert "3/12" in output
    assert "InferenceError" in output
    # The failure line is not glued onto the same terminal line as the bar.
    bar_line, _, rest = output.partition("\n")
    assert "InferenceError" not in bar_line


# ---------------------------------------------------------------------------
# 4. Worker lifecycle boundaries (startup/ready/health/shutdown)
# ---------------------------------------------------------------------------


def test_worker_started_renders_config_summary_block():
    logger, buffer = _make_handler_logger()

    log_event(
        logger, "worker_started", message="starting", configuration={
            "worker_id": "worker-1", "embedding_device": "cuda", "redis_endpoint": "redis://localhost:6379",
            "redis_db": "0", "namespace": "fingerprint", "log_level": "DEBUG",
        },
    )

    output = buffer.getvalue()
    assert "FINGERPRINTER WORKER" in output
    assert "worker-1" in output
    assert "CUDA" in output
    assert "fingerprint" in output


def test_worker_health_renders_as_single_concise_line():
    logger, buffer = _make_handler_logger()

    log_event(
        logger, "worker_health", uptime_s=120.4, jobs_claimed=5, jobs_completed=4, jobs_failed=0,
        active_jobs=1, average_job_latency_ms=1200.0, redis={"group_lag": 0},
    )

    lines = [line for line in buffer.getvalue().splitlines() if line.strip()]
    assert len(lines) == 1
    assert "[HEALTH]" in lines[0]


def test_worker_stopped_renders_summary_block():
    logger, buffer = _make_handler_logger()

    log_event(
        logger, "worker_stopped", uptime_s=125.0, jobs_claimed=5, jobs_completed=4, jobs_failed=0,
        jobs_rejected=0, jobs_retried=0, jobs_permanently_failed=0, active_jobs=0, total_job_attempts=5,
        average_job_latency_ms=1200.0, shutdown_reason="graceful_shutdown", clean=True,
    )

    output = buffer.getvalue()
    assert "WORKER STOPPED" in output
    assert "graceful_shutdown" in output


# ---------------------------------------------------------------------------
# 5. Unmapped / plain stdlib log calls still render sensibly
# ---------------------------------------------------------------------------


def test_plain_logger_warning_call_without_log_event_still_renders():
    """Not every log call in this codebase goes through `log_event` (e.g.
    worker/main.py's WORKER_MAX_ATTEMPTS warning uses a plain
    `logger.warning(...)` with %-args) -- the handler must not crash or
    silently drop these."""
    logger, buffer = _make_handler_logger()

    logger.warning("WORKER_MAX_ATTEMPTS=%d is set but has no effect", 5)

    output = buffer.getvalue()
    assert "WORKER_MAX_ATTEMPTS=5 is set but has no effect" in output
    assert "[WARNING]" in output


def test_info_level_generic_message_has_no_level_prefix():
    logger, buffer = _make_handler_logger()

    logger.info("plain info message")

    assert buffer.getvalue().strip() == "plain info message"


# ---------------------------------------------------------------------------
# 6. shorten_url
# ---------------------------------------------------------------------------


def test_shorten_url_leaves_short_urls_untouched():
    url = "https://cdn.example.com/x.mp4"
    assert shorten_url(url) == url


def test_shorten_url_shortens_long_urls_but_keeps_host_and_filename():
    url = "https://cdn.example.com/" + "a" * 40 + "/really-long-filename-1234567890.mp4"
    short = shorten_url(url)
    assert len(short) <= 72
    assert "cdn.example.com" in short
    assert short.endswith(".mp4")


def test_shorten_url_handles_none():
    assert shorten_url(None) == "n/a"


# ---------------------------------------------------------------------------
# 7. configure_console_logging format selection (auto/json/human)
# ---------------------------------------------------------------------------


class _FakeTtyStream(io.StringIO):
    def isatty(self) -> bool:
        return True


def test_configure_console_logging_auto_picks_json_for_non_tty_stream():
    root = logging.getLogger()
    original_level, original_handlers = root.level, list(root.handlers)
    try:
        stream = io.StringIO()  # StringIO.isatty() is False
        configure_console_logging(level=logging.INFO, format_="auto", stream=stream)
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0].formatter, JsonFormatter)
    finally:
        root.setLevel(original_level)
        root.handlers[:] = original_handlers


def test_configure_console_logging_auto_picks_human_for_tty_stream():
    root = logging.getLogger()
    original_level, original_handlers = root.level, list(root.handlers)
    try:
        stream = _FakeTtyStream()
        configure_console_logging(level=logging.INFO, format_="auto", stream=stream)
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0], HumanConsoleHandler)
    finally:
        root.setLevel(original_level)
        root.handlers[:] = original_handlers


def test_configure_console_logging_explicit_format_overrides_tty_detection():
    root = logging.getLogger()
    original_level, original_handlers = root.level, list(root.handlers)
    try:
        stream = _FakeTtyStream()  # would auto-select "human"
        configure_console_logging(level=logging.INFO, format_="json", stream=stream)
        assert isinstance(root.handlers[0].formatter, JsonFormatter)
    finally:
        root.setLevel(original_level)
        root.handlers[:] = original_handlers
