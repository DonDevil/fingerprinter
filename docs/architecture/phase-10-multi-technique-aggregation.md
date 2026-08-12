# Phase 10 — Multi-Technique Aggregation

## 1. Objective

Phase 9 built the matching *algorithm* (segment embeddings, temporal
consistency, `TemporalMatchResult`) but explicitly left it unwired: no
fingerprint-worker handler existed at all, the build-on-miss lock
`docs/design/design-proposal-1.md` §8 specifies was never implemented, and
`TemporalMatchResult` had no path into `work_queue.results.Result` (see
phase-09 doc §16–§18, "Interface expected by Phase 10"). Phase 10's job is
exactly that wiring: a real `worker/matching_handler.py` that claims a job,
acquires the candidate, embeds it, resolves the target's segment
embedding (cache-first, build-on-miss under a lock), runs Phase 9's
matcher, and folds the result into a `Result` Phase 4's `Worker.
commit_result` already knows how to persist — plus the folding mechanism
itself (`matching/aggregation.py`), built to stay correct once a second
technique (audio/OCR/watermark) exists, per `TemporalMatchResult`'s own
docstring naming this "Phase 10 (Multi-technique aggregation)."

Explicitly not this phase: any second technique (still none exist),
real-data threshold calibration, GPU serving infrastructure, crawler-side
result consumption, Redis HA, or object storage for target media (all
still deferred per `docs/design/design-proposal-1.md`'s own deferred
list).

## 2. Phase 9 inputs / constraints

From `docs/architecture/history/phase-09-temporal-video-matching.md`:

- §16: no fingerprint-worker handler exists yet; `embed_video_segments`
  recomputes segments on every call with nothing wiring it into
  `TargetRegistry`'s cache-check-then-build pattern.
- §17 (deferred): build-on-miss lock, multi-technique aggregation of
  `TemporalMatchResult` into `Result`.
- §18 (interface Phase 10 should consume): `match_segments(...)` as the
  single matching entry point; candidate segments from
  `DINOv2EmbeddingEngine.embed_video_segments`; target segments from
  `TargetRegistry.get_compatible_segment_embedding` (a plain cache read,
  no build-on-miss yet at that point). `TemporalMatchResult.matched`/
  `.score` are "this technique's own decision/confidence, not a final
  `Result.decision`"; `matcher_version` should be preserved into whatever
  combined evidence record Phase 10 produces.

From `docs/design/design-proposal-1.md` §8 ("Build-on-miss race"):
multiple workers can miss the same target embedding cache key
simultaneously; guard with `SET fingerprint:lock:target:{cache_key} NX
PX <ttl>`, losers poll/wait rather than duplicating the embedding pass.
This was written in the original architecture proposal but never
implemented — Phase 9 shipped the cache, not the lock.

## 3. Design investigation summary

Investigated against the existing code before writing any:

- `target/registry.py` (Phase 6/9): already composes `TargetRegistry`
  with pooled + segment caches, and already exposes
  `get_compatible_segment_embedding` (plain read) /
  `register_segment_embedding` (plain write). No lock, no orchestration of
  "check cache, else build" existed — that's exactly the gap. Extended
  (not replaced) with `get_or_build_segment_embedding`, keeping the class
  free of any embedding-engine import (it takes a `build` callback) — see
  §4.
- `worker/acquisition_handler.py` (Phase 5): the shape every later
  handler should follow — acquire, map acquisition errors onto
  `TransientFailure`/`PermanentFailure`, clean up the artifact in a
  `finally`, return a `Result`. Its own docstring calls itself "a
  stand-in for the real fingerprint pipeline" — `worker/matching_handler.py`
  is that real pipeline, same shape, extended.
- `work_queue/results.py` (Phase 4): `Result`'s own docstring already
  anticipated this phase — "the schema stays usable if a later phase's
  handler combines multiple techniques into one `Result`" — but had no
  field to carry structured per-technique evidence, only `confidence`/
  `summary`. `docs/design/design-proposal-1.md` §4's original Redis result
  schema sketch already listed an `evidence json` field; Phase 4 never
  populated it because nothing existed yet to put there. Phase 10 adds it
  (§6).
- `embedding/errors.py` (Phase 7): documents a mapping from embedding
  exceptions onto `TransientFailure`/`PermanentFailure`, written before
  any handler existed and before `Result.PROCESSING_FAILURE` had anything
  wired to it. Phase 10 is the first phase to actually decide how
  candidate-side embedding failures surface, and picks differently from
  that table for the candidate (not target) path — see §7 for why.
- `matching/matcher.py` / `matching/result.py` (Phase 9): unchanged.
  `match_segments`'s signature already matches exactly what phase-09 §18
  said Phase 10 should call.

## 4. Build-on-miss lock

**IMPLEMENTED.** `target/lock.py`: `RedisLock`, a minimal `SET NX PX` /
Lua-compare-and-delete primitive — not Redlock, no multi-instance quorum,
no auto-renewal. `acquire(ttl_ms)` returns whether this call won the lock
(unique owner token via `uuid4`); `release()` only deletes the key if the
stored token still matches this instance's token, the same CAS spirit
`worker/fingerprint_worker.py`'s existing Lua scripts already use for
attempt-fencing — a lock whose TTL already expired and was re-acquired by
someone else can never be deleted by the original, now-stale holder.

`target/registry.py` gains `get_or_build_segment_embedding(target_id,
target_version, spec, build, lock_ttl_ms=600_000,
poll_interval_s=1.0, poll_timeout_s=600.0)`:

1. Cache check — hit returns immediately, no lock touched at all.
2. Miss: resolve the `TargetRecord` (raises `KeyError` for an unknown
   target, same as the existing `register_embedding`/
   `register_segment_embedding` convention), derive the lock key from
   `target.versioning.cache_entry_key(...)` (full target+content+spec
   identity, so the lock is scoped to one exact representation, not the
   whole target — a target with two different `EmbeddingSpec`s in flight
   doesn't serialize against itself unnecessarily), and try to acquire.
3. Winner: double-checks the cache (another worker may have finished
   between step 1 and winning the lock), builds on a second miss via the
   caller-supplied `build(record)` callback, registers the result, and
   releases the lock in a `finally` — a `build` exception still frees the
   lock immediately rather than leaving it held for its full TTL
   (`tests/test_target_build_on_miss.py::test_build_exception_releases_lock_for_a_later_retry`).
4. Loser: polls the cache every `poll_interval_s` until it appears or
   `poll_timeout_s` elapses, then raises `TimeoutError` rather than
   duplicating the winner's build or blocking forever.

`build` takes the already-fetched `TargetRecord` (not zero-args) so a
caller never has to re-fetch what this method already looked up — this
module still never imports an embedding engine (no torch/transformers
dependency), matching Phase 9's existing "registry owns cache, caller
owns how to embed" boundary; `worker/matching_handler.py`'s `build`
closure is what actually calls
`DINOv2EmbeddingEngine.embed_video_segments`.

**Known limitation, not solved by this phase** (see §14): the lock does
not auto-extend. A build that runs longer than `lock_ttl_ms` loses the
lock mid-build, and a second worker can then acquire it and start a
redundant build — both would eventually register successfully (the loser
just does unnecessary work, not incorrect work, since `register_segment_
embedding` is a plain overwrite), but the duplicated compute is real.
`lock_ttl_ms=600_000` (10 minutes) is a **PROVISIONAL HEURISTIC**, chosen
to be generous relative to expected single-video embedding time on this
dev machine, not measured against a real target-library workload.

## 5. Handler wiring

**IMPLEMENTED.** `worker/matching_handler.py::build_matching_handler(
acquirer, engine, registry, matcher_config=None) -> Callable[[Job],
Result]`, matching `Worker.process_claim`'s handler contract:

```text
claim -> acquire candidate (Phase 5) -> embed candidate segments (Phase 9 engine)
       -> resolve target segments (cache-first, build-on-miss, §4)
       -> match_segments (Phase 9)
       -> combine([evidence]) -> Result (§6)
```

The same `engine` instance embeds both the candidate and (inside the
`build` closure) the target, so both sides always share one
`SegmentSamplingConfig` (`segment_duration_s`) automatically — required
for the temporal offset model to mean anything at all (phase-09 §16's
"offset model assumes equal segment duration on both sides" limitation);
a caller cannot accidentally pass mismatched configs for the two sides
because there is only one config to pass, set once at engine construction.

`job.techniques` (a Phase 1 field that had never been read by any
handler until now) gates whether this handler runs at all:
`matching.aggregation.DINOV2_TEMPORAL_TECHNIQUE` ("dinov2") must be one of
the requested techniques, or the handler raises `PermanentFailure`
immediately, before acquiring anything — a job asking for techniques this
worker doesn't implement is a routing/config problem, not a retryable one.

Target media is wrapped as a `MediaArtifact` via a small
`_target_artifact()` helper (content-type guessed from the registered
file extension, defaulting to `video/mp4`) purely so the *same*
`embed_video_segments` call path handles both candidate and target —
one embedding code path, not two.

## 6. Result schema extension

**IMPLEMENTED.** `work_queue/results.py::Result` gains `evidence:
Optional[str] = None` — the `evidence json` field
`docs/design/design-proposal-1.md` §4 specified in the original schema
sketch but Phase 4 never populated (nothing existed yet to put there).
Included in `to_hash_fields()` only when non-empty (same pattern as the
existing `summary`/`confidence` handling); deliberately **not** added to
`to_event_fields()` — that stream event stays "deliberately minimal...
not a copy of the full record," unchanged from Phase 4's own reasoning.

## 7. Aggregation

**IMPLEMENTED, technique-agnostic.** `matching/aggregation.py`:

```python
@dataclass(frozen=True)
class TechniqueEvidence:
    technique: str
    matcher_version: str
    matched: bool
    score: Optional[float]
    detail: Mapping[str, object]

def temporal_match_to_evidence(result: TemporalMatchResult) -> TechniqueEvidence: ...
def combine(evidence: Sequence[TechniqueEvidence], processing_started_at, processing_completed_at) -> Result: ...
```

`combine()` doesn't know DINOv2 exists — it only knows the
`TechniqueEvidence` shape. Adding a second technique later means writing
one more `<technique>_to_evidence()` converter, not rewriting `combine()`.
Combination rule (**PROVISIONAL, no cross-technique calibration data
exists**): `matched` if *any* technique matched (a job's `techniques`
names which run, not that all must agree — one confirmed technique is
enough to flag a candidate); `algorithm` is every technique name joined
with `+` (e.g. future `"dinov2+phash"`); `confidence` is the highest score
across techniques that reported one; `Result.evidence` is the full JSON
list of every technique's `{technique, matcher_version, matched, score,
detail}`, not just the summary numbers, so a human or a later phase can
see exactly which technique(s) contributed and why.

`work_queue.results` is imported lazily inside `combine()`, not at module
level — same reasoning as `embedding/result.py`'s lazy
`target.versioning` import (Phase 7): it pulls in `redis`, and the rest of
`matching` (`matcher.py`, `result.py`) is deliberately redis-independent
pure similarity math. `import matching` alone still never pulls in
`redis`; only a caller that actually calls `combine()` does (verified
manually — see §12; no dedicated test was added, unlike Phase 7's explicit
`test_engine_does_not_depend_on_redis`, since this module's own docstring
states the property is maintained by construction, not by a claim that
needed a subprocess-isolated test to trust).

## 8. Error mapping

None of these were resolved by an earlier phase; Phase 10 is the first to
actually need an answer, since it's the first phase with a real handler
that can fail mid-pipeline.

| Failure | Mapping | Why |
| --- | --- | --- |
| Acquisition transient/permanent | `TransientFailure`/`PermanentFailure` | Unchanged from Phase 5. |
| Candidate embedding fails (`UnsupportedMediaError`/`InferenceError` on the *candidate*) | `Result(decision=PROCESSING_FAILURE)` | `work_queue.results.Result`'s own docstring names exactly this ("corrupt media, algorithm error") as what `PROCESSING_FAILURE` exists for — a completed job with unusable evidence is more useful downstream (crawler sees a definitive record) than a silent terminal failure with none at all. **Diverges from `embedding/errors.py`'s Phase-7 table**, which was written before `PROCESSING_FAILURE` had anywhere to be written to. |
| Target embedding fails while building the segment cache (`UnsupportedMediaError` -> `PermanentFailure`, `InferenceError` -> `TransientFailure`) | per `embedding/errors.py`'s table, applied literally | Not the candidate's fault and not evidence about it — an operational/config problem (broken target registration) that fails identically for every job against this target until ops fixes it. Belongs at the job/worker level, not folded into a per-candidate `Result`. |
| Unknown `target_id`/`target_version` (`KeyError`) | `PermanentFailure` | Routing/config problem, not retryable. |
| Build-on-miss lock wait timeout (`TimeoutError`) | `TransientFailure` | Another worker is still building; a retry may simply land after it finishes. |
| `job.techniques` doesn't include `"dinov2"` | `PermanentFailure` | This worker has nothing to run for the job as specified. |

## 9. Threshold status

No new numeric thresholds were introduced by this phase beyond what
Phase 9 already flagged as provisional (`matching/config.py`). New
non-numeric/architectural decisions, all **PROVISIONAL**:

| Decision | Status | Note |
| --- | --- | --- |
| `lock_ttl_ms=600_000` | PROVISIONAL HEURISTIC | Not measured against a real embedding workload; see §4's limitation. |
| `poll_interval_s=1.0` / `poll_timeout_s=600.0` | PROVISIONAL HEURISTIC | Symmetric with `lock_ttl_ms` by construction, not independently justified. |
| "matched if any technique matched" (§7) | ARCHITECTURAL, PROVISIONAL | The disjunction rule itself is a product decision (recall over precision when combining independent techniques); no cross-technique calibration data exists to validate it against, and only one technique exists to test it with today. |
| "confidence = max score across techniques" | PROVISIONAL HEURISTIC | The simplest aggregation that's still meaningful for N=1 technique; not evaluated against alternatives (e.g. weighted combination) because there's nothing yet to weight against. |
| Candidate-side embedding failure -> `PROCESSING_FAILURE` result (§8) | ARCHITECTURAL | Resolves an ambiguity Phase 4/7 left open; not a numeric threshold, a categorization decision. |

## 10. Complexity

No new algorithmic complexity — `combine()` is O(number of techniques) (1
today), the lock is O(1) Redis ops on the hit/build path and
O(poll_timeout_s / poll_interval_s) Redis reads in the worst-case loser
wait. The only new cost relative to Phase 9 is what Phase 9 already
measured (segment extraction + inference + the O(N×M) matrix) now
actually running per job instead of only in tests.

## 11. Tests

New/changed test files:

- `tests/test_target_lock.py` (7 tests) — `RedisLock` mutual exclusion,
  owner-scoped release (a stale lock holder can't delete a newer owner's
  lock), idempotent release, TTL expiry.
- `tests/test_target_build_on_miss.py` (7 tests) — synthetic segments/
  `build` callback (no DINOv2), mirroring `test_segment_cache.py`'s
  style: cache-hit skips `build` entirely, cache-miss builds exactly once
  and registers, unknown target raises `KeyError` without calling
  `build`, missing `segment_cache` raises `RuntimeError`, a `build`
  exception releases the lock for a later retry, concurrent miss (two
  threads) builds exactly once, and a lock-wait timeout raises
  `TimeoutError` without ever calling `build`.
- `tests/test_aggregation.py` (7 tests) — synthetic `TemporalMatchResult`/
  `TechniqueEvidence`, no DINOv2/Redis: conversion correctness, single
  matched/unmatched technique decisions, "matches if any technique
  matched" with two synthetic techniques, evidence JSON round-trips full
  detail, empty evidence raises, no-scores leaves confidence `None`.
- `tests/test_results.py` — 2 new tests: `evidence` persists through
  `commit_result` when set, is absent from the hash when not.
- `tests/test_matching_handler.py` (5 tests) — real DINOv2 inference
  (`device="cpu"`), the existing `tiny_video.mp4` fixture (2s,
  `segment_duration_s=0.5` -> 4 segments), real HTTP acquisition via a new
  `/video` route on the shared `MediaTestServer`, a real `TargetRegistry`
  on filesystem caches, driven through `Worker.process_claim` end to end:
  self-match (candidate byte-identical to target) is detected with
  `confidence > 0.99` and the expected evidence detail; the target's
  segment embedding is cached after the first job (second call is a pure
  `has_compatible_segment_embedding` hit). Error paths (candidate
  embedding failure, unknown target, unsupported technique) call the
  handler directly against a fake acquirer rather than through real HTTP
  — Phase 5's `MediaAcquirer` already rejects corrupt media via `ffprobe`
  before a handler ever sees it, so reaching `embed_video_segments`'s own
  `UnsupportedMediaError` needs bytes that aren't real acquisition output.

`tests/media_test_server.py` gained a `/video` route serving the real
`tiny_video.mp4` fixture bytes with `content_type="video/mp4"` — the first
test server route to serve real, decodable video rather than the 1x1 PNG
every other route reuses.

**Caught during development, not a shipped bug**: an early version of
`tests/test_matching_handler.py` passed the shared `tests/fixtures/
tiny_video.mp4` path directly as a fake artifact's `local_path`; the
handler's `artifact.cleanup()` then deleted the real fixture file
mid-suite. Fixed by giving every fake-acquirer test a disposable
`tmp_path` copy (`_candidate_artifact()`) — the shared fixture is never
handed to code that owns/deletes it. Restored via `git checkout --`
before landing; flagged here because it's exactly the kind of shared-state
mistake future test authors in this file should not repeat.

Full repository suite (`pytest -q`, all phases): **149 passed** (121
Phase 1-9 + 28 new), 0 failures, 0 skipped — run at the end to confirm no
cross-phase regression.

## 12. Manual verification

- `import matching` does not import `redis` (checked via
  `sys.modules` in an interactive run, per §7 — no dedicated subprocess
  test was added; see that section for why).
- `import worker.matching_handler` succeeds standalone (catches import
  cycles / typos not otherwise exercised until a test actually
  instantiates `build_matching_handler`).

## 13. Deferred decisions

Carried forward from Phase 9, still not resolved:

- Real-data calibration of every threshold (Phase 9's, and this phase's
  combination rule in §9) — still no labeled dataset.
- Continuous-time (rather than segment-index) offset modeling.
- Binary/columnar segment storage format.
- FAISS/ANN indexing.
- Mean-pool-within-segment vs. single-frame sampling.

New to this phase:

- **Lock auto-renewal / a real distributed-lock library** (§4) — a build
  exceeding `lock_ttl_ms` currently just risks a redundant (not
  incorrect) rebuild by a second worker; acceptable at today's scale, not
  solved.
- **A second technique** — `matching/aggregation.py` is built to support
  one, but none exists yet to prove the abstraction against; the "any
  technique matched" / "max score" rules in §7/§9 are unvalidated
  precisely because there is nothing yet to combine with.
- **`Result.evidence` size/shape at scale** — fine for one technique's
  JSON blob; not evaluated for N techniques each with their own detail
  payload, or for a `Job`'s `techniques` field naming techniques whose
  evidence-conversion doesn't yet exist (currently: if `job.techniques`
  includes something other than `"dinov2"`, e.g. `["dinov2", "phash"]`,
  this handler still only ever runs the dinov2 technique and produces
  `algorithm="dinov2"` — it does not fail or warn about the unimplemented
  technique it silently ignored. Only a job requesting *exclusively*
  unimplemented techniques hits the `PermanentFailure` gate in §5/§8).
- **Crawler-side result consumption** of the new `evidence` field —
  still entirely out of scope (per `docs/design/design-proposal-1.md`'s
  own deferred list).

## 14. Interface expected by Phase 11

Phase 11 ("benchmarks" per phase-09 §9's own forward reference, and/or a
second technique per this phase's §13) should be able to:

```python
from worker.matching_handler import build_matching_handler
from matching.aggregation import TechniqueEvidence, combine, temporal_match_to_evidence
```

`build_matching_handler(acquirer, engine, registry, matcher_config=None)`
is the single entry point a worker process wires into `Worker.run()`.
Adding a second technique means: (1) a new engine/technique-specific
match function producing some technique-specific result type, (2) a new
`<technique>_to_evidence()` converter into `TechniqueEvidence`, (3)
extending `worker/matching_handler.py`'s handler body to run both and pass
`combine([evidence_a, evidence_b], ...)` — `combine()` itself needs no
change (§7). The `job.techniques` gate (§5) should be generalized from a
single hardcoded name check into "run whichever of the implemented
techniques were requested" once a second one actually exists — noted, not
built, since building it for N=1 technique would be exactly the kind of
speculative generality this project's phases have consistently avoided.
