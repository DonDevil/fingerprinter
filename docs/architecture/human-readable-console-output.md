# Human-Readable Console Output for the Fingerprinter Worker

## 1. Status

**IMPLEMENTED.** This document describes a presentation-only addition on top
of `docs/architecture/observability-implementation.md`'s DEBUG-mode work: a
second console renderer for `worker/main.py`'s structured log events that
displays them as short, aligned terminal lines instead of raw JSON. No
architecture changed: no new logging framework, no change to the structured
event schema (`event`/`fields`, `log_event()`), no change to Redis, streams,
consumer groups, job semantics, matching/embedding logic, target management,
or worker behavior. `JsonFormatter`/`configure_json_logging` are untouched
and remain the machine-readable path this project's run records and any
future log ingestion/dashboards use.

## 2. Problem

Every worker log line — from `worker_started` down to a DEBUG-level
`embedding_progress` checkpoint — was one JSON object per line
(`worker/observability.py`'s `JsonFormatter`). Technically complete, but
hard to follow by eye during development: a job's `job_claimed` →
`candidate_acquired` → `matching_completed` → `job_completed` sequence reads
as N unrelated JSON blobs, and a target/candidate embedding stage's ~10
progress checkpoints (already rate-limited by
`worker/matching_handler.py:_make_progress_logger`, see
observability-implementation.md §4.6) each printed on their own line.

## 3. What was delivered

| Capability | Entry point |
|---|---|
| `HumanConsoleHandler` — renders the same structured events as short, aligned terminal lines | `worker/observability.py` |
| Per-job correlation: DEBUG-only fields (target id/version, cache status, matching stats) are remembered by `job_id` so the closing `[Result]` line stays informative even when the terminal event itself (`job_completed`) doesn't carry them | `worker/observability.py:HumanConsoleHandler._jobs` |
| In-place-updating embedding progress bar (carriage return, one line per stage, not one line per checkpoint) | `worker/observability.py:HumanConsoleHandler._render_embedding_progress` |
| URL shortening for terminal width while keeping host + filename | `worker/observability.py:shorten_url` |
| `WORKER_LOG_FORMAT` env var (`auto` / `json` / `human`) — `auto` (default) picks human output for an interactive terminal and JSON otherwise | `worker/main.py` (`WorkerConfig.log_format`), `worker/observability.py:configure_console_logging` |

Structured JSON logging (`JsonFormatter`, `configure_json_logging`,
`log_event`) is unchanged and still what `WORKER_LOG_FORMAT=json` (and the
non-interactive `auto` default) produces. The separate machine-readable run
record (`WorkerConfig.run_output` / `ObservingWorkerObserver.write_run_record`)
is untouched — it was already a distinct file-output path from console
logging and this work never touched it.

## 4. Why `auto` + TTY detection, not a flipped default

The goal ("make terminal output human-readable") and the constraint
("preserve machine-readable structured logs for dashboards/log ingestion")
both had to hold at once. Three options were available: always JSON
(the old behavior — doesn't solve the stated problem), always human (solves
it, but silently breaks any deployment piping worker stdout/stderr into a
log aggregator that expects JSON), or auto-detect. TTY detection
(`stream.isatty()`) resolves this without an operator having to configure
anything in either environment:

- An operator running `python -m worker.main` directly in a terminal gets
  human-readable output immediately — this is the actual dev/debugging
  scenario the problem statement described.
- A process manager or container that captures stdout/stderr through a pipe
  (the normal production shape, and also exactly how
  `subprocess.Popen(..., stdout=subprocess.PIPE)` invokes the process in
  `tests/test_worker_observability.py`) is not a TTY, so `auto` resolves to
  `json` — identical to the worker's behavior before this change, with zero
  existing test or production log consumer needing to change.

`WORKER_LOG_FORMAT=json` / `=human` remain available to force either mode
regardless of TTY state (e.g. human output piped through `less`, or JSON
forced on an interactive terminal for a manual schema check).

## 5. Architecture (as built)

### 5.1 `worker/observability.py`

A new section ("Human-readable console output") added after
`configure_json_logging`, reusing exactly the same structured payload
`JsonFormatter` reads (`record.event`, `record.fields`, both set by the
existing `log_event()` helper) — this section only reads that payload, it
never produces it.

- **`HumanConsoleHandler(logging.StreamHandler)`** — `emit()` looks up
  `record.event` in a `_RENDERERS` dict mapping event name → renderer
  function (`(handler, record, fields) -> Optional[str]`); an event with no
  specific renderer (including plain `logger.warning(...)` calls that don't
  go through `log_event`, e.g. `worker/main.py`'s `WORKER_MAX_ATTEMPTS`
  warning) falls through to `_render_generic`, which uses
  `record.getMessage()` and prefixes non-INFO levels with `[LEVEL]`.
- **Renderer functions**, one per event actually emitted by
  `worker/observability.py`/`worker/main.py`/`worker/matching_handler.py`:
  `worker_started` (config summary block), `worker_ready`,
  `worker_shutdown_requested`, `worker_fatal_error`, `worker_stopped`
  (summary block), `worker_health` (one concise line), `job_claimed` /
  `job_reclaimed` (opens a job block + a `_jobs[job_id]` state entry),
  `job_rejected`, `job_processing_started`, `candidate_acquired`,
  `candidate_embedded`, `target_resolution_started` (suppressed on a cache
  HIT — `target_resolved` a moment later already reports it, and printing
  both would just double the common-case line for no new information),
  `target_resolved`, `matching_completed`, `stage_failed`, `job_completed`
  (closes the job block; if the event's own `decision` field is `None` —
  true for one of `worker/fingerprint_worker.py`'s two `on_job_completed`
  call sites — falls back to the `decision` `matching_completed` already
  recorded into `_jobs[job_id]` for the same job, rather than showing a
  meaningless generic label), `job_failed` / `job_retry_scheduled` /
  `job_permanently_failed` (closes the job block with error type/category).
- **`_jobs: Dict[str, dict]`** — the only new mutable state. Populated
  incrementally as a job's DEBUG-level events arrive (target id/version,
  cache status, matching stats) and popped as soon as that job's terminal
  event is rendered, so it cannot grow unbounded across a long-running
  worker process. Purely a presentation convenience — it duplicates nothing
  that the structured events themselves don't already carry; it only
  lets the closing line reference an earlier event's fields.
- **Progress bar** — `embedding_progress` is special-cased in `emit()`
  (checked before the `_RENDERERS` lookup) and routed to
  `_render_embedding_progress`, which writes `"\r" + text` with **no**
  trailing newline for every checkpoint except the final one
  (`frame == total`), which appends `"\n"` and clears the "line is open"
  flag (`_progress_open`). Any other event arriving while a progress line
  is open calls `_end_progress()` first (writes a bare `"\n"`) so it can
  never be concatenated onto the same terminal line as the bar — verified
  by `tests/test_worker_human_console.py::test_non_progress_event_flushes_an_open_progress_line_first`.
- **`shorten_url(url, max_len=72)`** — operates on the URL as it already
  arrives at this layer, i.e. *after*
  `worker/matching_handler.py:_redact_url` has already stripped
  userinfo/query/fragment (see observability-implementation.md §4.6); this
  function does no additional redaction, only width-shortening (keeps
  `scheme://host/…/last-path-segment`, hard-truncating the tail if even
  that is still too long).
- **`configure_human_logging(level, stream=None)`** — installs
  `HumanConsoleHandler` on the root logger, mirroring
  `configure_json_logging`'s exact contract (replace prior handlers, pin
  `_QUIET_THIRD_PARTY_LOGGERS` to WARNING).
- **`configure_console_logging(level, format_="auto", stream=None)`** — the
  single entry point `worker/main.py` calls. Resolves `"auto"` via
  `stream.isatty()` (defaulting to `sys.stderr`, matching
  `logging.StreamHandler()`'s own default) and dispatches to
  `configure_human_logging` or `configure_json_logging`.
- `configure_json_logging` gained an optional `stream` parameter (default
  `None`, preserving its exact previous behavior when omitted) purely so
  `configure_console_logging` and tests can inject a stream — no behavior
  change for any existing caller.

### 5.2 `worker/main.py`

- `WorkerConfig` gained `log_format: str = "auto"`, parsed from
  `WORKER_LOG_FORMAT` (lowercased) in `.from_env()` and validated against
  `_VALID_LOG_FORMATS = ("auto", "json", "human")` in `.validate()` —
  the exact pattern `log_level`/`WORKER_LOG_LEVEL` already established.
  `config_snapshot()` gained a `log_format` key.
- `configure_logging(level)` (the wrapper `main()` calls) kept its existing
  signature — no caller needed to change — but now reads
  `WORKER_LOG_FORMAT` directly from `os.environ` (falling back to `"auto"`
  if unset or not one of the three valid values) and calls
  `configure_console_logging(level=level, format_=requested_format)`
  instead of calling `configure_json_logging` directly. This mirrors
  `main()`'s existing "read the raw env var directly, before
  `WorkerConfig.from_env()` runs" pattern for `WORKER_LOG_LEVEL` — logging
  has to exist before a config-parse error on some *other* variable can be
  logged, and `WorkerConfig.validate()` still separately validates and
  records `log_format` for the snapshot a few lines later.

## 6. Example output

```
$ python -m worker.main               # interactive terminal -> human (auto)
────────────────────────────────────────────────────────────────
FINGERPRINTER WORKER
────────────────────────────────────────────────────────────────
Worker      : worker-a1b2c3
Device      : CUDA
Redis       : redis://localhost:6379/0
Namespace   : fingerprint
Log level   : DEBUG
────────────────────────────────────────────────────────────────
[READY] worker worker-a1b2c3 started (group=fingerprint-workers stream=fingerprint:jobs) - startup success
────────────────────────────────────────────────────────────────
[JOB CLAIMED] 3f9a1c2e-...  (attempt 1)
  Target      : blast / v1
  Candidate   : https://cdn.example.com/…/xyz123-abcdef.mp4
  Acquired    : video/mp4, 4.2MB in 0.41s
  Embedding   : [########################] 12/12 (100.0%)
  Embedded    : candidate — 12 segments in 1.20s
  Target      : blast / v1 — cache HIT — 1699 segments
  Matching    : 4/12 matched · candidate 33.33% · target 0.24% · mean 0.9366 · coarse 0.7816 · offset 650.0s · threshold 0.9000 → MATCH
  Result      : MATCH   latency=4.83s
────────────────────────────────────────────────────────────────
```

```
$ python -m worker.main 2>worker.log  # redirected -> json (auto)
$ head -1 worker.log
{"timestamp": "...", "level": "INFO", "event": "worker_started", "configuration": {...}}
```

```
$ WORKER_LOG_FORMAT=human python -m worker.main > worker.log   # forced human, even though redirected
$ WORKER_LOG_FORMAT=json  python -m worker.main                # forced JSON, even on a real terminal
```

## 7. Explicitly not changed

- The structured event schema (`event` name, `fields` keys) — every
  renderer only *reads* fields the existing DEBUG-mode implementation
  (observability-implementation.md) already produces. No event gained a
  field, no field was renamed, purely for this formatter's benefit.
- `JsonFormatter`, `configure_json_logging`, `log_event` — byte-for-byte
  unchanged behavior when `WORKER_LOG_FORMAT=json` or `auto` resolves to
  JSON (non-TTY stream).
- The machine-readable run record (`WorkerConfig.run_output`,
  `ObservingWorkerObserver.build_run_record`/`write_run_record`) — a
  separate file-output path, untouched.
- Any claim/lease/retry/commit semantics, Redis Streams usage, matching or
  embedding logic, or target management/acquisition/build behavior —
  this is presentation-only, layered entirely on top of existing log call
  sites.
- `target/cli.py`'s own plain-text `_configure_logging` — a distinct,
  already-human-readable logging setup (see
  observability-implementation.md §4.4) for a different entry point
  (operator CLI, not the worker daemon); out of scope for this change and
  left as-is.

## 8. Known characteristics / limitations

- `HumanConsoleHandler` assumes one job is processed at a time per worker
  process (true today — `Worker.run`/`process_claim` is single-threaded per
  process). If that ever changed, interleaved DEBUG events from two jobs
  sharing one terminal stream could interleave their lines; `_jobs` is
  keyed by `job_id` so state itself would stay correct, but the visual
  block-per-job grouping assumes non-interleaved output.
- `_render_target_resolution_started` deliberately suppresses the common
  cache-HIT case (only prints on a MISS, i.e. "about to build") — a
  presentation judgment call to avoid printing two near-identical lines
  back to back, not a change to what's logged (the JSON path still emits
  both `target_resolution_started` and `target_resolved` in every case).
- TTY detection uses `stream.isatty()`, which is `False` for
  `subprocess.PIPE`, most CI runners, and typical container log capture —
  `auto` resolves to `json` in all of those, matching pre-existing
  behavior. An operator who wants human output in one of those contexts
  must set `WORKER_LOG_FORMAT=human` explicitly.
- The progress bar's carriage-return updates are inert (but harmless) when
  captured to a non-terminal sink (a file, `subprocess.PIPE`, a StringIO in
  tests) — the `\r` characters are written literally rather than visually
  overwriting a line, since only a real terminal interprets them. This
  matches how any other CLI progress bar (e.g. `pip`, `wget`) behaves when
  redirected, and is why `auto` picks JSON for non-TTY streams in the first
  place.

## 9. Tests added

- `tests/test_worker_human_console.py` (new, 19 tests):
  - Job lifecycle renders as correlated human-readable lines, not JSON.
  - `job_completed` with no `decision` falls back to the `matching_completed`
    decision recorded earlier for the same `job_id`.
  - `_jobs` state is cleared after each job's terminal event.
  - `job_failed` renders `error_type`/`error_category`.
  - `matching_completed` renders every key statistic (matched count, both
    coverage percentages, mean/coarse similarity, offset, threshold,
    decision).
  - `embedding_progress` checkpoints collapse into one line (exactly one
    trailing `\n`, one `\r` per checkpoint before the last) rather than one
    line per checkpoint; the final checkpoint terminates the line; a
    non-progress event arriving while a progress line is open flushes it
    first instead of concatenating.
  - Worker startup/health/shutdown render as expected blocks/lines.
  - A plain `logger.warning(...)` call with no `log_event`/`fields` extras
    still renders (and gets a `[WARNING]` prefix); a plain INFO message
    gets no level prefix.
  - `shorten_url`: short URLs pass through unchanged; long ones keep host +
    filename within the length budget; `None` is handled.
  - `configure_console_logging("auto", ...)` picks `JsonFormatter` for a
    non-TTY stream and `HumanConsoleHandler` for a TTY-like stream; an
    explicit `format_` overrides TTY detection either way.
- `tests/test_worker_main.py` — `WorkerConfig.log_format` parses/validates/
  normalizes correctly (default `"auto"`, invalid value raises
  `ConfigError`); `configure_logging()` reads `WORKER_LOG_FORMAT` and passes
  the resolved format through to `configure_console_logging` (or falls back
  to `"auto"` when the env value is invalid) — checked by intercepting
  `configure_console_logging` itself, the same approach the existing
  `WORKER_LOG_LEVEL` tests use, so it doesn't depend on either formatter's
  exact rendering.
- `tests/test_worker_observability.py` — `config_snapshot()` includes
  `log_format`.

Full suite: 428 passed (up from 393 in observability-implementation.md,
reflecting this change's 19 new tests plus growth from other work in
between).
