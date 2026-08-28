# Observability Implementation — DEBUG Mode for Target Build & the Fingerprinter Worker

## 1. Status

**IMPLEMENTED.** This document describes the code actually merged for an
explicit DEBUG/diagnostic mode, following
`docs/architecture/observability-audit.md`'s findings and its final answer:
mostly logging-only changes, plus one small, localized instrumentation
addition (a DINOv2 embedding progress callback). No architecture was
changed: no new logging framework, no new Redis keys, no queue-contract
change, no change to matching thresholds/semantics, no change to embedding
math, no reintroduction of lazy target building. See §6 for the explicit
list of what this deliberately did **not** touch.

## 2. What was delivered

| Capability | Entry point |
|---|---|
| `--debug` flag: verbose diagnostics for `target.cli` (every subcommand, most useful on `build`) | `target/cli.py` |
| `WORKER_LOG_LEVEL` env var: DEBUG/INFO/WARNING/ERROR/CRITICAL for the worker daemon | `worker/main.py` (`WorkerConfig.log_level`) |
| Per-frame embedding progress callback | `embedding/dinov2_engine.py:embed_video_segments(..., on_frame=...)` |
| ffmpeg extraction timing (DEBUG) | `embedding/frames.py` |
| Target cache hit/miss/build/resolve diagnostics (DEBUG) | `target/build.py`, `worker/matching_handler.py` |
| Per-job pipeline stage diagnostics: acquisition, candidate embedding, target resolution, matching (DEBUG) | `worker/matching_handler.py` |
| Full existing matching-metric exposure (segment counts, both coverage variants, similarities, threshold, decision) | `worker/matching_handler.py:_log_matching_debug` |
| Stage-tagged failure diagnostics (DEBUG) | `worker/matching_handler.py:_log_stage_failure` |

Normal (INFO) output is unchanged in every component: two `print()` lines
for `target.cli build`, and the same JSON lifecycle events
`worker/observability.py` already emitted.

## 3. Why this exists

The audit (§1, §3, §4, §5) found the domain code (`embedding/`, `target/`,
`matching/`, `acquisition/`) had zero logging calls, and that the DINOv2
progress bar the old prototype had (`old/matcher/dinov2_matcher.py`, gated
by a `debug_mode` flag) was dropped, not merely hidden, in the current
engine. Every matching metric requested (segment counts, coverage,
similarity, threshold, decision) was already computed by
`matching/matcher.py` but never logged. This implementation closes exactly
those gaps, at the smallest points that already existed.

## 4. Architecture (as built)

### 4.1 `embedding/dinov2_engine.py`

`embed_video_segments(artifact, segment_sampling_config=None, on_frame=None)`
gained one optional parameter: `on_frame: Optional[Callable[[int, int], None]]`,
called as `on_frame(index, total)` (1-based `index`) once per embedded
frame. `None` (the default) is a no-op branch (`if on_frame is not None:`),
so every existing call site — benchmarks, tests, any caller that doesn't
know this parameter exists — is unaffected. The engine still knows nothing
about logging, TTY detection, or progress-bar rendering; it only reports.

### 4.2 `embedding/frames.py`

`extract_frames` and `extract_segment_frames` each gained a
`logger = logging.getLogger(__name__)` and `logger.debug(...)` calls before
starting ffmpeg, and on success/timeout/non-zero-exit, each including
elapsed wall time. No behavior changed — these are pure additions around
the existing `subprocess.run` calls.

### 4.3 `target/build.py`

`build_target(..., on_frame=None)` forwards `on_frame` to
`engine.embed_video_segments()` only when a build actually runs (the
existing `already_built` cache-hit branch never touches the engine at all,
so `on_frame` is correctly never invoked on a cache hit). Two
`logger.debug()` calls report cache hit/miss (reusing the `already_built`
value the function already computed for its return value — no new cache
lookup) and the resolved segment count + wall time once
`get_or_build_segment_embedding` returns.

### 4.4 `target/cli.py`

- `_configure_logging(debug: bool)`: installs one plain-text
  `StreamHandler` (`"%(levelname)s: %(message)s"`) on the **root** logger
  at `WARNING` (default) or `DEBUG` (`--debug`), clearing any prior handler
  first. Deliberately plain text, not `worker/observability.py`'s
  `JsonFormatter` — this CLI's normal output is human `print()` lines, and
  the audit (§8, §11) explicitly recommended against JSON here.
- `--debug` is declared on a shared `argparse.ArgumentParser(add_help=False)`
  and attached to every subcommand via `parents=[...]`, so it appears after
  the subcommand name — the same position `--json` already uses on every
  subcommand — rather than as a global pre-subcommand flag.
- `_cmd_build` calls `_log_engine_ready(engine)` (model id/revision/device/
  thread count/`model_load_duration_s`, all already computed by the engine)
  and builds a progress callback (`_build_progress_callback`) that logs at
  roughly 10 evenly-spaced checkpoints regardless of segment count — both
  only when `args.debug` is set, so a stand-in engine used elsewhere in the
  test suite that doesn't expose those attributes is never touched on the
  normal path.

### 4.5 `worker/main.py`

`WorkerConfig` gained `log_level: str = "INFO"`, read from `WORKER_LOG_LEVEL`
(uppercased) in `.from_env()` and validated against
`{"DEBUG","INFO","WARNING","ERROR","CRITICAL"}` in `.validate()`, following
the exact pattern `embedding_device`/`EMBEDDING_DEVICE` already established.
`configure_logging()` now takes a `level` parameter (default `logging.INFO`,
preserving its previous hardcoded behavior when called with no argument).
`main()` reads `WORKER_LOG_LEVEL` directly (falling back to INFO if unset or
not a real level name) to configure logging *before* `WorkerConfig.from_env()`
runs, so a config-parse error for some other variable is still logged
correctly even if the log-level value itself also turns out to be invalid;
`WorkerConfig.validate()` still rejects a genuinely bad `WORKER_LOG_LEVEL`
with `ConfigError`, exactly like every other field. `config_snapshot()`
gained a `log_level` key.

### 4.6 `worker/matching_handler.py`

One `debug = logger.isEnabledFor(logging.DEBUG)` check at the top of
`handler()` gates everything below; in normal (INFO) operation this is the
only added cost per job. When true:

- `job_processing_started` — job/target identity, techniques, and a
  redacted candidate URL (`_redact_url`: scheme + hostname + path only,
  strips userinfo/query/fragment/credentials, capped at 200 chars —
  preserving the existing policy documented in
  `worker/observability.py`'s `_ERROR_CATEGORY_MAP` comment that a raw
  media URL must never reach logs).
- `candidate_acquired` — content type, byte size, acquisition duration
  (reusing the duration already computed for `record_stage`, not a new
  timer).
- `candidate_embedded` — candidate segment count, duration. The candidate
  embedding call also gets a `_make_progress_logger("candidate_embedding", job_id)`
  callback (checkpoint-style, same cadence as `target.cli`'s).
- `target_resolution_started` / `target_resolved` — `_resolve_target_segments`
  gained `job_id`/`debug` keyword parameters; when debug, it pre-checks
  `registry.has_compatible_segment_embedding(...)` (the same method
  `target/build.py` already calls) purely to report `cache_status` before
  calling the real, unchanged `get_or_build_segment_embedding`. This extra
  read only happens when debug logging is enabled — zero added Redis/disk
  traffic in normal mode. A build-on-miss inside this path gets the same
  `on_frame` progress callback, labeled `"target_build"`.
- `matching_completed` — `_log_matching_debug` reads fields already on
  `TemporalMatchResult` (matching/result.py) and reports, all in one
  structured event: `target_segment_count`, `candidate_segment_count`,
  `matched_segment_count`, `target_coverage_hits` (matched_segment_count /
  total_target_segments), `target_coverage_span` ((target_end - target_start)
  / segment_duration_s / total_target_segments — the audit's §7 second
  formula, since a single "coverage" number can be misleading),
  `candidate_coverage`, `mean_similarity`, `coarse_similarity`,
  `temporal_offset_s`, `similarity_threshold`, `min_matched_segments`, and
  `decision` (`"MATCH"`/`"NO_MATCH"`). No new scoring, no threshold change —
  every value already existed on `MatcherConfig`/`TemporalMatchResult`.
- `stage_failed` — logged immediately before each existing
  `TransientFailure`/`PermanentFailure` raise (media_acquisition,
  candidate_embedding, target_resolution), tagging the exact stage and
  `error_type` (never the exception message, which can embed a media URL).
  No exception is caught-and-swallowed anywhere that wasn't already caught;
  this only adds a log line ahead of the existing `raise`.

All new logging in this module goes through `worker/observability.py`'s
existing `log_event()` helper (same structured `event`/`fields` shape the
JSON daemon output already uses), rather than a second logging convention.

## 5. Example output

```
$ python -m target.cli build blast --version v1
building blast/v1 ...
built: blast/v1 (1699 segments, 8495.5s)
```

```
$ python -m target.cli build blast --version v1 --debug
building blast/v1 ...
DEBUG: engine ready: model=facebook/dinov2-base revision=f9e44... device=cpu torch_num_threads=None model_load_duration_s=2.140
DEBUG: target blast/v1: compatible segment embedding not cached, build required
DEBUG: ffmpeg extract_segment_frames starting: /home/dhanush/Videos/Blast.mp4 (segment_duration_s=5.0, timeout=300.0s)
DEBUG: ffmpeg extract_segment_frames done: 1699 segment frame(s) in 41.8s
DEBUG: embedding blast/v1: frame 170/1699 (10.0%)
DEBUG: embedding blast/v1: frame 340/1699 (20.0%)
...
DEBUG: target blast/v1: resolved 1699 segment(s) in 372.4s (built)
built: blast/v1 (1699 segments, 8495.5s)
```

```
$ WORKER_LOG_LEVEL=DEBUG python -m worker.main
{"timestamp": "...", "level": "INFO", "event": "worker_started", ...}
{"timestamp": "...", "level": "DEBUG", "event": "job_processing_started", "job_id": "...", "target_id": "blast", "target_version": "v1", "candidate_url": "https://example.com/clip.mp4", ...}
{"timestamp": "...", "level": "DEBUG", "event": "target_resolution_started", "cache_status": "hit", ...}
{"timestamp": "...", "level": "DEBUG", "event": "matching_completed", "matched_segment_count": 6, "target_segment_count": 1699, "target_coverage_hits": 0.0035, "target_coverage_span": 0.0047, "candidate_coverage": 1.0, "mean_similarity": 0.94, "similarity_threshold": 0.9, "decision": "MATCH", ...}
{"timestamp": "...", "level": "INFO", "event": "job_completed", "decision": "match", ...}
```

## 6. Explicitly not changed

- No matching algorithm, threshold, or scoring formula — `matching/matcher.py`
  and `matching/aggregation.py` are untouched.
- No embedding math, model, sampling config, or cache format — only an
  additive, default-`None` callback parameter.
- No Redis schema, queue contract, or job/result shape.
- No reintroduction of lazy target building — `_resolve_target_segments`'s
  cache-first, build-on-miss call to `get_or_build_segment_embedding` is
  identical to before; the debug pre-check only *reads* cache state to log
  it.
- No new logging framework — stdlib `logging` throughout, using the two
  formatters the project already established (JSON for the worker daemon,
  plain text for the operator CLI).
- DEBUG is never the default anywhere (`WorkerConfig.log_level` defaults to
  `"INFO"`; `target.cli`'s `--debug` defaults to `False`).

## 7. Known characteristics / limitations

- Enabling DEBUG configures the **root** logger (matching
  `worker/observability.py:configure_json_logging`'s own existing
  approach), so third-party libraries' own DEBUG-level output would
  otherwise also be surfaced. Two were concretely observed in manual
  testing and are now explicitly pinned to WARNING regardless of the
  requested root level, in both `target/cli.py:_QUIET_THIRD_PARTY_LOGGERS`
  and `worker/observability.py:_QUIET_THIRD_PARTY_LOGGERS` (kept in sync,
  one list per module since the two logging setups are intentionally
  independent — see §4.4):
  - `redis` — redis-py logs `"Failed to enable maintenance notifications:
    unknown subcommand 'MAINT_NOTIFICATIONS'"` at DEBUG on every connection
    to a Redis server that predates that optional feature. This is a
    harmless, automatic fallback inside redis-py, not an error and not
    something this project's code raised or can act on — but unfiltered,
    it read as if this tool had failed at something.
  - `PIL` — logs one line per PNG chunk (`STREAM b'IHDR' ...`) while
    decoding each extracted frame.
  Any *other* third-party library's DEBUG output is not filtered — only
  these two were actually observed; add to the tuple if another one shows
  up in practice rather than pre-emptively guessing at more.
- `target/build.py`'s cache-hit/miss check and `worker/matching_handler.py`'s
  equivalent pre-check call `TargetRegistry.has_compatible_segment_embedding`
  an extra time purely for logging. In `target/build.py` this was already
  unconditional (existing `already_built` return value); in
  `worker/matching_handler.py` it is gated behind `debug` and therefore adds
  zero overhead in normal operation.
- `target.cli`'s `--debug` flag must appear after the subcommand name (e.g.
  `build ID --version V --debug`), matching `--json`'s existing position —
  not before it.

## 8. Tests added

- `tests/test_target_build.py` — `on_frame` is forwarded only when a build
  actually runs, and never on a cache hit.
- `tests/test_target_cli.py` — `--debug` produces the expected diagnostic
  lines on stderr (engine info, cache status, progress checkpoints,
  resolution summary) while stdout's normal `--json` output is unaffected;
  a sibling build with no `--debug` produces no stderr output at all.
- `tests/test_worker_main.py` — `WORKER_LOG_LEVEL` parses/validates/
  normalizes correctly; `main()` passes the resolved level into
  `configure_logging()`; an invalid level falls back to INFO for that one
  early call while still failing config validation correctly.
- `tests/test_worker_observability.py` — `config_snapshot()` includes
  `log_level`; `configure_json_logging(level=DEBUG)` pins `redis`/`PIL` to
  WARNING while this project's own loggers get DEBUG.
- `tests/test_target_cli.py` — same third-party-quieting behavior for
  `_configure_logging`, and that redis/PIL noise doesn't leak into a real
  `--debug` build's stderr.
- `tests/test_matching_handler.py` — DEBUG mode emits `job_processing_started`,
  `candidate_acquired`, `candidate_embedded`, `target_resolution_started`/
  `target_resolved`, and `matching_completed` with the expected fields
  (including both coverage variants); none of these fire without DEBUG
  enabled; `target_resolution_started.cache_status` correctly reports
  `"miss"` then `"hit"` across two jobs against the same target;
  `stage_failed` correctly tags `candidate_embedding` on a real candidate
  embedding failure; `_redact_url` strips credentials/query strings and
  truncates long URLs.

Full suite: 393 passed (see conversation for the exact run).
