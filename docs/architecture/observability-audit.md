# CLI / Runtime Observability Audit — Logging, DINOv2 Progress, and Matching Metrics

## Status

**AUDIT ONLY. No production code, tests, configuration, Redis state, target
cache, or generated files were modified to produce this document.** Every
finding below comes from direct, read-only source inspection (`grep`, file
reads), one read-only `ffprobe` duration check against the registered
`blast/v1` target media, and dependency checks against the project's own
virtualenv (`./env/bin/python -c "import ..."`). No fingerprint job was run,
no embedding job was triggered, and no Redis command was issued.

Labeling convention (matching `docs/architecture/target-management-audit.md`
and `docs/architecture/target-eager-build-audit.md`):

- **VERIFIED FROM SOURCE** — read directly in the file/line cited, this session.
- **VERIFIED FROM RUNTIME STATE** — read directly from a live filesystem/`ffprobe` check, this session.
- **INFERRED** — a reasonable conclusion from source that isn't a single directly-asserted line.
- **RECOMMENDATION** — a proposed direction, explicitly not implemented anywhere in this document.

Scope: `target/{cli,build,service,registry,cache,segment_cache,errors}.py`,
`embedding/{dinov2_engine,frames,config,result}.py`,
`worker/{main,observability,matching_handler,fingerprint_worker,acquisition_handler}.py`,
`acquisition/{acquirer,artifact,validation}.py`,
`matching/{matcher,aggregation,config,result,similarity}.py`,
`work_queue/{results,jobs}.py`, `pyproject.toml`, `requirements*.txt`.
`old/matcher/dinov2_matcher.py` and `old/dinov2/*` were read for historical
precedent only (see §4). Crawler and bridge are separate repositories not
present in this working directory — their logging setup is out of scope and
unverified here. No architecture change, Redis contract change, queue-schema
change, or matching-algorithm change is proposed anywhere in this document.

---

## 1. Executive Summary

**VERIFIED FROM SOURCE.** The observability gap has one root cause, repeated
in three places: the domain code (`embedding/`, `target/`, `matching/`,
`acquisition/`) contains **zero `logging` calls**. `grep -rn "logger\."`
across those packages (excluding `old/` and tests) returns nothing. The only
logging in the non-`old/` codebase lives in `worker/observability.py` +
`worker/main.py`, and it is a bolt-on wrapper (`ObservingWorkerObserver`)
around the *Worker's* lifecycle events (claim/complete/fail) — it never sees
inside a stage, so it can log "candidate_embedding took 4.2s" but not
"candidate has 42 segments" or "similarity=0.94".

The DINOv2 progress bar is not hidden by any config — it does not exist in
the current implementation. `embedding/dinov2_engine.py` is a clean-room
rewrite that dropped the `tqdm`-based progress bar the *old* prototype had
(`old/matcher/dinov2_matcher.py:305-333`, gated by a `self.debug_mode` flag:
`disable=not self.debug_mode`). That precedent is exactly the normal/debug
split requested here — it simply was not carried into the rewrite.

Every metric on the requested list — segment counts, similarity scores,
matched/target/candidate coverage, temporal offset, threshold decisions — is
**already computed** by `matching/matcher.py:match_segments()` and packaged
into `TechniqueEvidence.detail` (`matching/aggregation.py`). It is serialized
into `Result.evidence` as a JSON blob and stored in Redis, but never printed
or logged anywhere. This is the largest single opportunity: a debug mode
needs no new math, only new log statements reading fields that already
exist.

Target coverage has one correct, already-computable definition (§7) with two
internally-consistent variants (hit-density vs. span) that must not be
conflated into a single misleading percentage.

`target.cli build` (`target/build.py`, `target/cli.py`) calls the DINOv2
engine with no additional instrumentation. For the registered test target
(`blast/v1`, `/home/dhanush/Videos/Blast.mp4`, **VERIFIED FROM RUNTIME
STATE** via `ffprobe`: duration `8495.552000` s ⇒ ≈1,699 segments at the
5.0s default `segment_duration_s`), the command currently prints
`building blast/v1 ...` and then produces no further output until the entire
multi-minute build finishes.

## 2. Current Logging Architecture

**VERIFIED FROM SOURCE.**

- **Framework:** Python stdlib `logging` only. Neither `structlog` nor
  `loguru` is used in the current tree — the old prototype used `loguru`
  (`old/matcher/dinov2_matcher.py`), dropped in the rewrite. `rich`, `click`,
  and `tqdm` are not project dependencies: `requirements.txt` lists only
  `redis`, `requests`, `torch`, `torchvision`, `transformers`, `Pillow`,
  `numpy`, and none of `rich`/`click`/`tqdm` import from the project's own
  virtualenv (`./env/bin/python -c "import tqdm"` fails).
- **Where configured:** exactly one place — `worker/main.py:configure_logging()`
  calls `worker/observability.py:configure_json_logging(level=logging.INFO)`,
  which replaces all root handlers with a single `StreamHandler` +
  `JsonFormatter` (one JSON object per line, to stdout).
- **Default level:** `INFO`, hardcoded as a `logging.INFO` literal in
  `configure_logging()` — not read from env, CLI, or a config file.
- **Supported levels:** all five stdlib levels are usable (`Logger.log()`
  takes any of them); `worker/main.py` already calls `logger.warning()` once
  (the unused-`WORKER_MAX_ATTEMPTS` notice) and `log_event(..., level=logging.ERROR)`
  for fatal startup errors. Nothing currently emits at `DEBUG`.
  `grep -rn "logger.debug\|logging.DEBUG"` outside `old/` and tests matches
  nothing.
- **Component separation:** `worker/main.py` is the only component that
  configures logging. `target/cli.py` has no `import logging` at all and
  communicates purely via `print()`/`sys.stderr`, with `--json` as an
  explicit per-subcommand opt-in flag. Crawler and bridge live in separate
  repositories and were not inspected.
- **Format consistency:** two disjoint styles exist in this repo —
  `worker.main` emits structured JSON lines; `target.cli` emits plain
  human `key: value` / sentence lines via `print()`. Neither is aware of the
  other's format.

## 3. Current Target-Build Observability

**VERIFIED FROM SOURCE.** Tracing `python -m target.cli build ID --version V`
end to end (`target/cli.py:_cmd_build` → `target/build.py:build_target` →
`TargetRegistry.get_or_build_segment_embedding` → `embedding/frames.py` +
`embedding/dinov2_engine.py`):

| Step | What happens | What's printed/logged |
|---|---|---|
| CLI entry | `_cmd_build` prints `building {id}/{version} ...` | 1 line, only if `--json` is not set |
| Engine construction | `DINOv2EmbeddingEngine.__init__` loads processor+model via `transformers`, measures `model_load_duration_s` | Nothing — no `import logging` in this module at all |
| FFmpeg extraction | `embedding/frames.py:extract_segment_frames` runs one blocking `subprocess.run(..., timeout=300.0)` for the whole target duration | Nothing, for up to 300s |
| Per-frame embedding | `embed_video_segments` loops over every sampled frame (≈1,699 for `blast/v1`), calling `_embed_pil_image` each time | Nothing — no loop counter, no progress bar, no periodic log |
| Cache write | `TargetRegistry.register_segment_embedding` writes segment vectors to the filesystem cache + Redis metadata | Nothing |
| Timings | `model_load_duration_s` and `inference_duration_s` (whole-call duration, not per-frame) are computed and stored on the result object | Computed but never printed/logged — the CLI's success line only reports `segment_count`/`total_duration_s` |
| Final CLI line | `_cmd_build` prints `built: id/version (N segments, D.Ns)` | 1 line, at the very end |

The entire multi-minute-to-multi-hour build is, today, exactly **two print
statements** with nothing between them.

**INFERRED — adjacent risk, not in scope to fix here:**
`extract_segment_frames`'s `DEFAULT_SEGMENT_FFMPEG_TIMEOUT_S = 300.0` is a
single fixed ffmpeg timeout applied to the entire file regardless of
duration, not scaled to it. `docs/architecture/target-eager-build-audit.md`
already documents a real timeout failure against this exact target's earlier
lazy-build path; ffmpeg decode is normally much faster than realtime, but
this remains a flat constant worth knowing about.

## 4. Why the DINOv2 Progress Bar Is Not Currently Visible

**VERIFIED FROM SOURCE.** It is not disabled by configuration — it was never
re-implemented in the current engine.

- `old/matcher/dinov2_matcher.py:305-333` had a direct answer to this
  exact question: `from tqdm import tqdm`, then
  `pbar = tqdm(total=total_frames, desc=f"Extracting {video_type} frames", unit="frames", disable=not self.debug_mode)`,
  plus a `loguru`-based `logger.info(...)` also gated on `self.debug_mode`.
  That is the exact normal/debug split requested, and it already existed
  once in this codebase.
- `embedding/dinov2_engine.py` (the current, in-use implementation) has no
  `tqdm` import, no `logging` import, no progress-callback parameter, and no
  debug flag — `grep -n "tqdm\|progress" embedding/dinov2_engine.py` matches
  nothing.
- `tqdm` is not installed in the project's own virtualenv — it is not merely
  unused, it is unavailable without adding it to `requirements.txt`.
- Nothing downstream (`target/build.py`, `target/cli.py`,
  `worker/matching_handler.py`) wraps the frame loop with any progress
  reporting either — there is no interception point that a bar could have
  been suppressed at.

There is no "wrong config" to fix here — the feature simply does not exist
in the rewritten engine.

## 5. Current Fingerprinter Observability (stage-by-stage)

**VERIFIED FROM SOURCE**, tracing a real job through
`worker/matching_handler.py:build_matching_handler().handler()`.

| Stage | Existing information | Log level | Timing | Missing |
|---|---|---|---|---|
| Job claimed | `job_claimed` event: job_id, attempt, worker identity | INFO (JSON, via `ObservingWorkerObserver`) | claim timestamp only | target_id/version, candidate URL/evidence_id (present on `Job`, not logged here) |
| Candidate acquisition | `acquisition/acquirer.py` raises typed errors (`NotFoundError`, `RateLimitedError`, etc.) carrying HTTP status + URL in the exception message; `MediaArtifact` carries `content_type`, `byte_size` | None on the happy path (no logger in `acquirer.py`); failures surface only via `on_job_failed`'s `error_type`/`error_category` — the raw message (which may embed the URL) is deliberately never logged | `stage_recorder("media_acquisition", …)` — aggregated into latency stats only, not logged per job | success-path status, byte size, content type — all exist on the object, none logged |
| Candidate validation | `acquisition/validation.py` maps to `InvalidMediaError`/`UnsupportedMediaError` | None | None | pass/fail + reason per job |
| Candidate frame extraction | `embedding/frames.py:extract_segment_frames` — blocking ffmpeg subprocess | None | folded into the `candidate_embedding` stage, not measured separately | frame/segment count, ffmpeg duration, ffmpeg stderr on non-timeout failure |
| Candidate embedding | `DINOv2EmbeddingEngine.embed_video_segments` → `VideoSegmentEmbeddingResult` (segments, coarse_vector, `inference_duration_s`) | None | `inference_duration_s` computed; also aggregated via `stage_recorder("candidate_embedding", …)` into periodic health-summary latency stats (not per-job) | per-frame progress, segment count logged per job |
| Target resolution (cache lookup or build-on-miss) | `TargetRegistry.get_or_build_segment_embedding` — cache-first, lock-based build-on-miss | None | `stage_recorder("target_resolution", …)` — aggregated only; `matching_handler.py`'s own docstring notes cache-hit vs. build-time isn't separable without modifying `registry.py` | cache hit/miss/build outcome per job, target segment count per job |
| Matching | `matching/matcher.py:match_segments` — full metric set (§6) | None | `stage_recorder("matching", …)` — aggregated only | every metric in §6, logged per job |
| Result aggregation | `matching/aggregation.py:combine()` builds `Result` (`decision`, `confidence`, `summary`, `evidence` JSON) | None | `stage_recorder("aggregation", …)` | the `evidence` JSON is computed but never echoed to logs |
| Job completion/failure | `on_job_completed`/`on_job_failed`/etc. — job_id, attempt, latency_ms, `decision` (completion only), error_type/error_category (failure only) | INFO | claim-to-completion / claim-to-failure latency, bucketed | confidence score, match/no-match detail, target/candidate coverage |

**Key structural finding:** per-job stage timings (`record_stage_duration`)
feed `BoundedLatencyStats`, which is only ever emitted via the periodic
health summary (default interval 60s) or the final shutdown run record —
**never as a per-job log line.** A single manually-submitted test job may
complete and the process may exit before any health summary ever fires.

## 6. Current Matching Metrics

**VERIFIED FROM SOURCE.** All computed inside
`matching/matcher.py:match_segments()`, held on `TemporalMatchResult`
(`matching/result.py`), copied into `TechniqueEvidence.detail`
(`matching/aggregation.py:temporal_match_to_evidence`), and from there
reaching `Result.evidence` as a JSON string stored in Redis — but never
printed or logged anywhere in the current code.

| Metric | Status | Where |
|---|---|---|
| Target segment count | ALREADY AVAILABLE | `TemporalMatchResult.total_target_segments` |
| Candidate segment count | ALREADY AVAILABLE | `TemporalMatchResult.total_candidate_segments` |
| Matched segment count | ALREADY AVAILABLE | `TemporalMatchResult.matched_segment_count` |
| Target coverage | ALREADY AVAILABLE (exact formula in §7) | derived from `matched_segment_count / total_target_segments` |
| Candidate coverage | ALREADY AVAILABLE | derived from `matched_segment_count / total_candidate_segments` |
| Similarity scores (per pair) | ALREADY AVAILABLE | `TemporalMatchResult.matched_pairs[i].similarity` |
| Maximum similarity | AVAILABLE WITH MINOR LOGGING ONLY | `max(p.similarity for p in matched_pairs)` — one-line derivation, not currently computed |
| Average/mean similarity | ALREADY AVAILABLE | `TemporalMatchResult.mean_similarity` |
| Threshold(s) used | ALREADY AVAILABLE | `MatcherConfig.segment_similarity_threshold`, `.coarse_similarity_threshold`, `.min_matched_segments` — in scope at the call site, not logged |
| Number of matching pairs | ALREADY AVAILABLE | `len(TemporalMatchResult.matched_pairs)` (equals `matched_segment_count`) |
| Temporal alignment / offset | ALREADY AVAILABLE | `TemporalMatchResult.temporal_offset_s` |
| Final match confidence/score | ALREADY AVAILABLE | `TemporalMatchResult.score` (= winning run's `mean_similarity`), and `Result.confidence` downstream |
| Coarse (whole-video) similarity | ALREADY AVAILABLE | `TemporalMatchResult.coarse_similarity` (`None` if coarse screening was skipped) |

Nothing on the requested list requires new matching-algorithm
instrumentation. Every gap here is in the reporting layer, not the algorithm.

## 7. Target-Coverage Metric

**INFERRED, derived directly from VERIFIED-FROM-SOURCE fields.** The current
algorithm (`matching/matcher.py:match_segments`) supports two different,
both mathematically valid, coverage definitions. They must not be conflated
into one percentage, per the audit's explicit warning against inventing a
potentially misleading number.

**A. Hit-density coverage** (what `matched_segment_count` directly measures):

```
target_coverage_hits = matched_segment_count / total_target_segments
```

Counts only target segments that were actually part of a
temporally-consistent hit in the winning run (`best_run`, built from
`best_per_candidate`, filtered from `consistent` pairs at/above
`segment_similarity_threshold`). Because `max_index_gap` (default 2)
tolerates gaps inside a run without penalty, a run can span more target
segments than it has hits for — so this number can under-represent the
run's actual timeline reach.

**B. Span coverage** (not currently computed, but trivially derivable from
existing fields — no new instrumentation):

```
target_span_segments = (target_end - target_start) / segment_duration_s
                       # equivalently: max(matched target_index) - min(matched target_index) + 1
target_coverage_span = target_span_segments / total_target_segments
```

Answers "what fraction of the target's timeline does the matched run
cover", including tolerated gaps. Always `>= target_coverage_hits`.

**Recommendation:** log both, labeled distinctly
(`target_coverage_hits`, `target_coverage_span`) rather than picking one and
calling it "target coverage" — either alone can mislead about what a given
percentage means. Both derive from fields `TemporalMatchResult` already
carries (`matched_segment_count`, `total_target_segments`,
`target_start`/`target_end`, plus `SegmentSamplingConfig.segment_duration_s`
for the span variant) — no instrumentation is required, only the two-line
arithmetic above at the log site.

**Interpretation note specific to `blast/v1`:** with ≈1,699 target segments,
a legitimately-matched short pirated clip (e.g. 30s ≈ 6 segments) will show
`target_coverage_hits ≈ 0.35%`. That is not a bug or an undersized match —
it is the correct answer to "how much of the full movie appears in this
candidate," which is inherently small for short-clip piracy. Candidate
coverage (`matched_segment_count / total_candidate_segments`) is the more
informative number for "how much of *this candidate* is matched target
content" and will typically be much higher for a genuine full-clip case.

## 8. Debug/Verbose Mode — Recommendation

**RECOMMENDATION**, sized to the smallest change consistent with existing
conventions:

- The project already has one logging entry point
  (`worker/observability.py:configure_json_logging(level=...)`) that accepts
  a level today but is only ever called with a hardcoded `logging.INFO`. The
  smallest change is making that level configurable, not adding a parallel
  mechanism.
- `worker/main.py:WorkerConfig` already has a complete, tested pattern for
  env-driven configuration (`_getenv_int`, `.from_env()`, `.validate()`). A
  `log_level: str = "INFO"` field via a `WORKER_LOG_LEVEL` env var
  (validated against `{"DEBUG","INFO","WARNING","ERROR","CRITICAL"}`) fits
  that shape exactly, mirroring `EMBEDDING_DEVICE`'s validation pattern.
- `target/cli.py` has no logging today and communicates via `print()` +
  `--json`. Its own convention (argparse, per-subcommand flags, `_EPILOG`)
  suggests a top-level `--debug`/`--verbose` argparse flag applying to every
  subcommand (most usefully `build`) — a per-invocation operator choice, not
  fleet wiring, so it belongs with `--json` as a flag rather than with
  `REDIS_URL`/`TARGET_CACHE_PATH` as env vars (those exist specifically
  because they must match the worker fleet's own wiring; debug-ness has no
  such constraint).
- Recommended shape: one conceptual on/off switch, expressed idiomatically
  per entrypoint — `--debug` for `target.cli` (interactive, per-invocation),
  `WORKER_LOG_LEVEL=DEBUG` for `worker.main` (long-running daemon, already
  configured via environment). Not process-wide or centrally shared — there
  is no shared config plane today across crawler/bridge/fingerprinter, and
  proposing one would be architecture beyond what this gap requires.
- Debug mode's actual content: new `logger.debug(...)` calls at the stage
  boundaries already present in `worker/matching_handler.py` (identifiers,
  per-stage entry/exit, the §6 metrics) and inside
  `embedding/dinov2_engine.py`/`embedding/frames.py` (frame-by-frame or
  every-N-frames progress). None of this exists today — it is new code, but
  logging calls added to existing call sites, not new instrumentation logic.

## 9. Progress Display — Recommendation

**RECOMMENDATION**, smallest change that avoids duplicating any existing
mechanism (there is currently no other progress mechanism in the live code
path to collide with — the old `tqdm` usage is dead code under `old/`):

- `embedding/dinov2_engine.py` has one natural seam per method: the
  `for frame_path in frame_paths:` loop in `_embed_video` and the
  `for index, frame_path in enumerate(frame_paths):` loop in
  `embed_video_segments`. A single optional callback parameter — e.g.
  `on_frame: Optional[Callable[[int, int], None]] = None`, invoked once per
  frame as `on_frame(index, total)` — is the minimum surface. Default
  `None` preserves every existing call site unchanged, matching the same
  additive pattern already used for `torch_num_threads` in this file.
- The caller decides what to do with the callback:
  `target/cli.py`'s `_cmd_build` can print a `tqdm` bar (if installed and
  stdout is a TTY) or a plain periodic `logger.debug(...)` line otherwise;
  `worker/matching_handler.py` can log every-N-frames in debug mode. This
  keeps `embedding/` free of presentation concerns, consistent with its own
  docstring ("this module knows nothing about Redis, crawler jobs, or
  URLs").
- `tqdm` is not currently a dependency. Adding it is optional — a plain
  periodic log line satisfies the stated requirement with no new
  dependency. If an interactive progress *bar* is wanted specifically for
  `target.cli build`, it should be gated on `sys.stdout.isatty()` so a
  non-interactive/redirected run (cron, CI, piped to a log file) gets plain
  log lines instead of raw carriage-return escape sequences.

## 10. Terminal Output — Illustrative Examples (not implemented)

**NORMAL mode**, `target.cli build blast --version v1`:
```
building blast/v1 ...
extracted 1699 target segments (8495.5s) in 42.3s
embedding 1699 segments ...
built: blast/v1 (1699 segments, 8495.5s) in 6m12.4s
```

**DEBUG mode**, `target.cli build blast --version v1 --debug`:
```
building blast/v1 ...
[DEBUG] ffmpeg extract_segment_frames: cmd=['ffmpeg', ...] timeout=300.0s
extracted 1699 target segments (8495.5s) in 42.3s
[DEBUG] model_load_duration_s=2.14
embedding 1699 segments ...
[DEBUG] frame 100/1699 (5.9%) elapsed=8.2s
[DEBUG] frame 200/1699 (11.8%) elapsed=16.1s
...
built: blast/v1 (1699 segments, 8495.5s) in 6m12.4s
```

**Worker, NORMAL mode**, one job (human-readable projection of what already exists):
```
job abc123 claimed (target=blast/v1)
job abc123 completed: MATCH (confidence=0.94) in 3.2s
```

**Worker, DEBUG mode**, same job:
```
job abc123 claimed (target=blast/v1, candidate=evidence-456)
[DEBUG] media_acquisition: 812ms, content_type=video/mp4, size=4.2MB
[DEBUG] candidate_embedding: 41 segments, 2.9s
[DEBUG] target_resolution: cache hit, 1699 target segments, 0.02s
[DEBUG] matching: coarse_similarity=0.81, matched_segments=6/41 (candidate),
        target_coverage_hits=6/1699, target_coverage_span=8/1699,
        mean_similarity=0.93, threshold=0.90, offset=+120.0s
job abc123 completed: MATCH (confidence=0.94) in 3.2s
```

## 11. Required Changes

| Change | Category |
|---|---|
| Make worker's log level configurable (env var, mirroring `WorkerConfig.from_env`) | REQUIRED |
| Add `--debug`/`--verbose` flag to `target.cli` (top-level parser) | REQUIRED |
| Add `logger.debug(...)` calls in `worker/matching_handler.py` at each existing stage boundary, logging fields already computed (job/target/candidate identity, segment counts, §6 matching metrics) | REQUIRED |
| Log `target_coverage_hits` and `target_coverage_span` (§7) at match time | REQUIRED |
| Add an optional frame-progress callback to `DINOv2EmbeddingEngine` (`embed_video_segments`/`_embed_video`) | REQUIRED — the one true instrumentation gap; no existing seam covers it |
| Wire that callback in `target/cli.py`'s `build` command to a periodic log line (every N frames normally, every frame in debug mode) | REQUIRED |
| Log per-job stage timings individually (not only aggregated into `BoundedLatencyStats`) in debug mode | OPTIONAL — addresses "timing of each stage" per job rather than only via periodic health summaries |
| Add a real TTY-aware `tqdm` progress bar (new dependency) for interactive `target.cli build` | OPTIONAL — a plain counted log line already satisfies the stated requirement without a new dependency |
| Explicitly log cache hit/miss/build outcome at the `target_resolution` stage | OPTIONAL — currently inferable indirectly (near-zero stage duration ⇒ cache hit) but not stated explicitly |
| Centralized/shared logging infrastructure across crawler/bridge/fingerprinter | NOT RECOMMENDED — no evidence the current architecture needs it; each process already configures logging independently, and a shared plane would be premature relative to the stated future dashboard/UI direction |
| Replacing `argparse`+`print()` in `target.cli` with `click`/`rich` | NOT RECOMMENDED — not a current dependency, closes no gap a flag + stdlib `logging` doesn't already close |
| Making JSON logging the only format for `worker.main`, or JSON for `target.cli` | NOT RECOMMENDED as a wholesale change — JSON already serves machine consumption of the daemon well; the missing piece is a concise *human* line for interactive/debug runs, not a format swap |

## 12. Implementation Scope (if approved)

- `embedding/dinov2_engine.py` — `DINOv2EmbeddingEngine.__init__`,
  `.embed_video_segments`, `._embed_video`: add optional progress callback
  parameter(s); no change to model/math logic.
- `embedding/frames.py` — optionally log timing around the `subprocess.run`
  calls in `extract_frames`/`extract_segment_frames` (currently silent even
  on success).
- `target/build.py` — `build_target()`: thread a progress/debug flag through
  to the engine call.
- `target/cli.py` — top-level parser: add `--debug`/`--verbose`; `_cmd_build`:
  wire the progress callback to console output; `main()`: call
  `logging.basicConfig` conditionally (currently absent entirely).
- `worker/main.py` — `WorkerConfig`: add `log_level` field + env var +
  validation; `configure_logging()`: accept/pass the level instead of the
  hardcoded `logging.INFO`.
- `worker/matching_handler.py` — `handler()` closure: add
  `logger.debug(...)` calls at each existing stage boundary (the stage
  boundaries and `stage_recorder` calls already mark exactly where these
  belong).
- `worker/observability.py` — no change required to `JsonFormatter`/
  `ObservingWorkerObserver` internals; only the level passed into
  `configure_json_logging` changes.
- `matching/matcher.py` / `matching/aggregation.py` — no changes needed;
  every field a debug log would want already exists on
  `TemporalMatchResult`/`TechniqueEvidence`.

## 13. Test Plan

- `target/cli.py`: a test invoking `build` with `--debug` (subprocess or
  `main(argv=...)`, capturing stdout) asserting debug-only lines appear only
  when the flag is set and are absent by default — mirrors the existing
  subprocess-capture pattern in `tests/test_embedding_lazy_import.py`.
- `worker/main.py`: extend `tests/test_worker_observability.py`'s existing
  `logging.StreamHandler` + buffer pattern (lines 52-56) to assert
  `WorkerConfig.from_env({"WORKER_LOG_LEVEL": "DEBUG"})` actually changes the
  effective level, and that an invalid value raises `ConfigError`, matching
  the existing `EMBEDDING_DEVICE` validation test shape.
- `embedding/dinov2_engine.py`: a test that a supplied progress callback is
  invoked exactly `len(frame_paths)` times with correctly increasing
  `(index, total)`, using a short synthetic video fixture
  (`benchmarks/gen_test_video.py` already generates tiny test videos cheaply
  — no real/long file needed).
- `worker/matching_handler.py`: a test asserting new debug log records (via
  `caplog` or a capturing handler) contain the expected fields
  (`target_coverage_hits`, `mean_similarity`, etc.) for a known synthetic
  match, and that none of these appear at INFO level.
- No changes needed to `matching/*` tests — the underlying computations are
  unchanged; only add assertions that the *logged* representation matches
  the already-tested `TemporalMatchResult` fields.

## 14. Scope / Safety Check

- No production files were modified during the audit — every tool call was
  read-only (`grep`, `ls`, `find`, `ffprobe`, `pip show`,
  `python -c "import ..."`); `git status --porcelain` showed a clean tree at
  the end of the audit session.
- No tests were modified.
- No Redis state was modified — no Redis command was issued.
- No target cache was modified or rebuilt — `target_cache/pooled` and
  `target_cache/segments` were only listed, never written to; no embedding
  job was run.
- No architecture redesign was performed or proposed — every recommendation
  in §11 extends an existing seam (`WorkerConfig.from_env`,
  `configure_json_logging`'s level parameter, `target.cli`'s argparse
  pattern, `stage_recorder`'s existing hook shape) rather than introducing a
  new subsystem.

## Final Answer

**Can the existing DINOv2 progress and existing matching information be
exposed through a proper debug mode with a small, localized change, or does
the current architecture require new instrumentation?**

Mostly the former, with one narrow exception:

- **Matching information (§6, §7):** zero new instrumentation. Every
  requested metric is already computed by `matching/matcher.py` and
  available on `TemporalMatchResult`/`TechniqueEvidence`. Exposing it is
  purely adding `logger.debug(...)` calls at existing points in
  `worker/matching_handler.py` that read fields that already exist.
- **Stage visibility, job/target/candidate identity, cache status (§5):**
  zero new instrumentation. All identifiers and most metadata are already
  in scope at the exact points where each stage already runs; logging them
  is call-site-only work.
- **DINOv2 embedding progress (§4, §9):** minimum new instrumentation
  required — a single optional per-frame progress callback parameter added
  to `DINOv2EmbeddingEngine.embed_video_segments`/`_embed_video`. This is
  genuinely new code (the old prototype's equivalent was deleted, not
  merely hidden), but it is the smallest possible addition: one callback
  parameter, invoked once per already-existing loop iteration, with no
  change to the embedding math, sampling, or caching logic.

One small, localized instrumentation addition (the frame-progress callback)
plus logging-only changes everywhere else — no redesign, no new subsystem,
no changes to the matching algorithm.
