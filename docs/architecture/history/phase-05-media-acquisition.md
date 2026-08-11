# Phase 5 — Media Acquisition Contract

## Objective

Build the first real pipeline component: `fingerprint job -> media
acquisition -> validated local media artifact`. HTTP download only —
no DINOv2, no frame extraction, no pHash/audio fingerprinting. The
acquisition layer must be independent of every fingerprinting concern so a
later technique-specific handler can consume its output without knowing
where the bytes came from.

## Package layout

New top-level package `acquisition/`, alongside the existing `work_queue/`
and `worker/` packages, with **no** dependency on either — it only depends
on `requests` (added to `requirements.txt`) and, for validation, an
external `ffprobe` binary on `PATH`:

- `acquisition/artifact.py` — `MediaArtifact`, the typed result.
- `acquisition/errors.py` — the failure taxonomy (see below).
- `acquisition/validation.py` — `probe_media()`, the ffprobe wrapper.
- `acquisition/acquirer.py` — `MediaAcquirer`, the downloader.

The queue-side wiring lives in `worker/acquisition_handler.py`
(`build_acquisition_handler()`), which is the *only* place that imports
both `acquisition` and `worker`/`work_queue` — this keeps the dependency
arrow one-directional (worker depends on acquisition, never the reverse),
matching the objective that a future DINOv2/pHash/audio handler is just
another consumer of `MediaArtifact`, structurally identical to this
phase's synthetic one.

## Acquisition API

```python
from acquisition import MediaAcquirer

acquirer = MediaAcquirer(
    connect_timeout_s=5.0,
    read_timeout_s=30.0,
    max_bytes=100 * 1024 * 1024,
    max_redirects=5,
    allowed_schemes=("http", "https"),
    allowed_content_type_prefixes=("video/", "image/", "audio/"),
)
artifact = acquirer.acquire(url)   # raises on failure, see below
...                                # pipeline reads artifact.local_path
artifact.cleanup()                 # caller-owned, explicit
```

`acquire()` is the single entry point: URL in, `MediaArtifact` out or an
`AcquisitionError` subclass raised. Every parameter above has a default;
only `session` (inject a `requests.Session`-alike, used by the connect-
timeout test), `validator` (inject a fake validator), and `temp_dir` are
otherwise-notable constructor options.

## Artifact structure

```python
@dataclass
class MediaArtifact:
    local_path: Path
    original_url: str
    final_url: str
    content_type: str
    byte_size: int
    checksum_sha256: str
    acquisition_duration_s: float
    media_metadata: Optional[Mapping[str, object]]   # cheap ffprobe fields, or None
```

`media_metadata` holds only what ffprobe returns for free while validating
(`format_name`, `duration_s`, `codec_type`, `codec_name`, `width`,
`height` — keys omitted when ffprobe doesn't report them). Nothing here is
fingerprint-specific; a DINOv2 handler would still need to open the file
itself to extract frames.

## Download limits & policy

- **Timeouts**: `requests`' `timeout=(connect_timeout_s, read_timeout_s)`
  tuple gives independent connect vs. read timeouts natively — both apply
  per-socket-operation, so a read timeout fires on a stalled *body*, not
  just stalled *headers*.
- **Max size, enforced while streaming**: `_stream_to_disk()` writes in
  `chunk_size`-sized chunks (default 64 KiB) via `response.iter_content()`,
  checking the running total against `max_bytes` **after every chunk**,
  before it's written — the file is never fully buffered in memory, and a
  server lying about `Content-Length` (or omitting it) cannot bypass the
  limit, since the check is against bytes actually received.
- **Redirects**: followed manually (`allow_redirects=False` on every
  request, loop in `acquire()`), one hop at a time, so each hop can be
  re-validated (scheme check re-runs on every redirect target) and counted
  against `max_redirects`. A missing `Location` header on a 3xx is treated
  as a permanent failure, not a silent stop.
- **Scheme allow-list**: checked before every request, including after
  each redirect — a `file://` or `ftp://` target can't be reached by
  redirecting through an initially-valid `http://` URL.
- **Content-Type**: the response header is checked against
  `allowed_content_type_prefixes` **before** downloading the body, purely
  as a fast-reject optimization (an obviously-wrong `text/html` error page
  shouldn't be streamed to disk at all). It is explicitly *not* trusted as
  proof of validity — a missing/generic header does not skip validation,
  and `probe_media()` (below) is the actual authority on whether the bytes
  are usable media.

## Media validation

`acquisition/validation.py:probe_media()` shells out to `ffprobe -v error
-print_format json -show_format -show_streams <path>`, bounded by a
5-second subprocess timeout (`DEFAULT_FFPROBE_TIMEOUT_S`). A non-zero
return code, unparseable JSON, or zero streams in the output all raise
`InvalidMediaError`. This is explicitly *not* fingerprinting — it never
decodes frames/audio samples, only reads the container's own metadata,
which is what makes it cheap enough to run as part of acquisition rather
than a separate pipeline stage.

`ffprobe` is an **explicit external dependency** (present in this dev
environment at `/usr/bin/ffprobe`). If it's missing from `PATH`,
`probe_media()` raises a plain `RuntimeError` — deliberately *not*
classified as Transient/Permanent, since a missing binary is an
environment/config problem, not a fact about the job. Left uncaught, it
propagates like an unhandled handler exception already does per
[Phase 3](phase-03-retry-backoff.md): the entry stays in the PEL,
indistinguishable from a worker crash, until whoever's running the worker
fixes the environment.

## Failure classification

`acquisition/errors.py` defines two roots, mirroring Phase 3's
`TransientFailure`/`PermanentFailure` one-for-one so the worker-side
mapping (below) needs no translation table:

| Condition | Exception | Class |
|---|---|---|
| Connect timeout | `ConnectionTimeoutError` | Transient |
| Read timeout (headers or body) | `ReadTimeoutError` | Transient |
| Other network failure (DNS, reset, interrupted transfer) | `NetworkError` | Transient |
| HTTP 429 | `RateLimitedError` | Transient |
| HTTP 5xx | `ServerError` | Transient |
| Unsupported URL scheme | `UnsupportedSchemeError` | Permanent |
| HTTP 404 | `NotFoundError` | Permanent |
| HTTP 410 | `GoneError` | Permanent |
| Other 4xx | `ClientError` | Permanent |
| Unexpected/unclassified status code | `UnexpectedStatusError` | Permanent |
| Unsupported Content-Type | `UnsupportedContentTypeError` | Permanent |
| Download exceeds `max_bytes` | `SizeLimitExceededError` | Permanent |
| Redirect chain exceeds `max_redirects` | `RedirectLimitExceededError` | Permanent |
| Corrupt/empty/unprobeable media | `InvalidMediaError` | Permanent |

`UnexpectedStatusError` defaults unrecognized status codes to *permanent*
rather than transient — an unclassified condition is safer treated as
non-retryable than risking an infinite retry loop against a URL that will
never succeed. HTTP status errors carry `.status_code` via a small
`_HTTPStatusMixin` shared across the five status-derived exception types,
instead of duplicating an `__init__` five times.

## Temporary-file lifecycle

Ownership is explicit and matches the brief's preferred shape exactly:

```
acquire()  ->  MediaArtifact  ->  caller reads artifact.local_path  ->  artifact.cleanup()
```

- The artifact's temp file is created via `tempfile.mkstemp()` (prefix
  `fingerprinter-acq-`, suffix `.media`) and is **not** deleted on a
  successful `acquire()` return — the caller owns it from that point.
- On any failure *during* acquisition (size exceeded, read timeout mid-
  stream, corrupt media caught by `probe_media()`), the partial file is
  deleted before the exception propagates — nothing is ever left behind on
  a failed `acquire()` call. Verified directly by
  `test_temp_file_cleaned_up_on_size_limit_failure` and
  `test_temp_file_cleaned_up_on_corrupt_media_failure`.
- `MediaArtifact.cleanup()` is idempotent (`_cleaned_up` flag,
  `Path.unlink(missing_ok=True)`) — safe to call more than once, and safe
  even if the file was already removed out-of-band.
- `worker/acquisition_handler.py`'s handler calls `artifact.cleanup()` in a
  `finally` block after building its (synthetic) `Result`, so the temp file
  never outlives one job's processing — a real fingerprint handler would do
  the same after it finishes reading `local_path`.

## Worker integration

`worker/acquisition_handler.py:build_acquisition_handler(acquirer)` returns
a handler with the `Callable[[Job], Result]` shape `Worker.process_claim()`
already expects (Phase 4) — no changes to `Worker` itself were needed.
It:

1. Calls `acquirer.acquire(job.media_url)`.
2. Catches `TransientAcquisitionError` → re-raises as `worker.TransientFailure`.
3. Catches `PermanentAcquisitionError` → re-raises as `worker.PermanentFailure`.
4. On success, builds a synthetic `Result` (`algorithm="acquisition-only-v0"`,
   `decision=ResultDecision.NO_MATCH`, a `summary` noting byte size and
   checksum prefix) and returns it — which `process_claim()` commits via
   Phase 4's `commit_result()`, exactly like any other handler's `Result`.
5. Always calls `artifact.cleanup()` before returning, success or not.

This is deliberately a two-line `try/except` re-raise, not a new retry
framework — Phase 3's `process_claim()` dispatch (success /
`TransientFailure` / `PermanentFailure` / uncaught-looks-like-a-crash)
handles acquisition failures exactly like it already handles any other
handler failure.

## Tests

**`tests/media_test_server.py`** — an in-process `ThreadingHTTPServer` on
`127.0.0.1` (random free port), wired into a module-scoped `media_server`
fixture in `conftest.py`. No external network access anywhere in this
phase's tests. Routes: `/ok` (tiny valid 67-byte PNG), `/redirect/<n>`
(chains `n` redirects to `/ok`), `/redirect-loop` (redirects to itself, for
the limit test), `/notfound` (404), `/gone` (410), `/error` (500),
`/toomany` (429), `/badtype` (200, `text/html`), `/corrupt` (200,
`video/mp4`, random garbage bytes), `/large` (200, 4 KiB body — tiny, but
exceeds the small `max_bytes` the size-limit test configures), `/slow`
(sends headers immediately, then sleeps 1.5s before the body — trips a
short `read_timeout_s`).

**`tests/test_acquisition.py`**, 18 tests:

1. `test_successful_http_download`
2. `test_redirect_handling`
3. `test_final_redirected_url_is_recorded`
4. `test_redirect_limit_exceeded`
5. `test_connection_timeout_maps_to_transient` — via a fake `session`
   injected into `MediaAcquirer`, raising `requests.exceptions.ConnectTimeout`.
   A real OS-level connect-timeout isn't reliably reproducible against a
   local-only test target (loopback connects essentially never hang), so
   this exercises the *classification*, not socket-level timing.
6. `test_read_timeout` — real, against `/slow`.
7. `test_maximum_size_enforced_while_streaming`
8. `test_unsupported_content_type`
9. `test_http_404_classified_permanent`
10. `test_http_410_classified_permanent`
11. `test_http_5xx_classified_transient`
12. `test_http_429_classified_transient`
13. `test_corrupt_media_rejected`
14. `test_checksum_correctness`
15. `test_temp_file_cleaned_up_on_size_limit_failure`
16. `test_temp_file_cleaned_up_on_corrupt_media_failure`
17. `test_artifact_remains_available_until_explicit_cleanup`
18. `test_unsupported_scheme_rejected`

**`tests/test_worker_acquisition.py`**, 3 tests (item 15 from the brief):

1. `test_worker_acquires_media_and_commits_synthetic_result` — full
   claim → acquire → handler → `Result` → Phase 4 commit chain.
2. `test_permanent_acquisition_failure_maps_to_permanent_worker_failure` —
   404 → job state `failed` immediately, retry ZSET stays empty.
3. `test_transient_acquisition_failure_schedules_retry` — 500 → job state
   `retry_scheduled`, one entry lands in the retry ZSET.

Run: `.venv/bin/python -m pytest tests/` — all 56 tests pass (35 Phase 1-4
+ 18 + 3 Phase 5).

## Limitations

- **No SSRF protection.** Per the task brief, explicitly deferred as a
  production-hardening item: nothing here resolves DNS or rejects
  redirects/targets pointing at private/link-local/metadata-service
  addresses. A production crawler integration must add this before
  `media_url` values can come from untrusted input.
- **Content-Type validation is prefix-based and coarse**
  (`video/`, `image/`, `audio/`) — no per-codec/container allow-list, and
  `application/octet-stream` is accepted as a "type unknown, let
  `probe_media()` decide" fallback rather than rejected outright.
- **`ffprobe` missing is an uncaught `RuntimeError`**, not a job-level
  classification — acceptable per the reasoning above, but means a
  misconfigured worker host will look like it's stuck crash-looping a job
  rather than surfacing a clear "ffprobe not installed" signal to an
  operator. A dedicated startup check (fail fast on `Worker.__init__`
  rather than per-job) would be a reasonable follow-up.
- **No retry-aware connection pooling tuning** — `MediaAcquirer` uses one
  `requests.Session()` per instance with library defaults; no explicit
  connection-pool sizing, no `Retry`/`HTTPAdapter` configuration layered
  under `requests` itself (all retry logic is at the Phase 3 worker level,
  intentionally, so there's exactly one retry mechanism, not two).
- **No partial-download resume.** A read timeout or size-limit failure
  discards the partial file entirely; the next attempt (a Phase 3 retry)
  re-downloads from scratch.
- **No streaming validation.** `probe_media()` only runs after the full
  file has landed on disk — a very large permanently-invalid file still
  costs a full download before being rejected. Bounded in practice by
  `max_bytes`, not by early-exit validation.
- **No dedicated test for `ffprobe`-missing behavior** — would require
  manipulating `PATH` in-process; considered out of scope for this phase's
  focused test list.
- **Redirect targets are not content-type/size pre-checked** — only the
  final (non-redirect) response's headers are inspected; an intermediate
  hop's headers are ignored beyond the `Location` field.

## Deferred work

Same bucket as Phases 1-4, still unchanged: DINOv2, ViT, embeddings,
FAISS, frame extraction, temporal voting, pHash, audio fingerprinting, GPU
processing, target management, crawler integration, object storage,
monitoring, production deployment, Redis HA. Also still deferred: SSRF
protection (see Limitations above), a real downstream result consumer, and
rewriting the top-level architecture document.
