# Phase 9 — Temporal / Video Matching

## 1. Objective

Phase 8 concluded (investigation only, no code) that a single mean-pooled
video vector cannot support piracy-relevant matching questions — "does
candidate C contain content from target T" and "which part of C matches
which part of T" — because pooling collapses away exactly the information
those questions need. Phase 9's job is to make those questions answerable:
implement segment-level embeddings, similarity search between them, a
temporal-consistency model that separates a real copied clip from
scattered coincidental similarity, and a structured match result Phase 10
can build on.

Explicitly not this phase: multi-technique aggregation, audio/OCR/
watermark matching, crawler integration, distributed/GPU tuning, FAISS
(unless justified — see §9), or empirically calibrated thresholds (no
labeled dataset exists yet in this project — see §13).

## 2. Phase 8 inputs / constraints

Phase 8 (`docs/architecture/history/phase-08-video-representation-investigation.md`)
was investigation-only: it changed no code. Its conclusions were:

1. Per-segment embeddings must be retained across the full video duration
   — never collapsed into one vector before matching.
2. A mean-pooled vector is retained only as a coarse screening layer,
   derived from the segment embeddings (not a separate inference pass).
3. Matching must support partial-content detection and localization.
4. Segment duration, intra-segment sampling density, storage format,
   FAISS/threshold/temporal-voting decisions were all explicitly deferred
   to this phase.

Because Phase 8 shipped no code, Phase 9 had to implement the segment
*representation* (sampling, engine, cache) in addition to the matcher
itself — there was no existing segment-level embedding pipeline to build
matching on top of.

## 3. Design investigation summary

Investigated against the existing code before writing any:

- `embedding/config.py` / `embedding/frames.py` / `embedding/dinov2_engine.py`
  (Phase 7): `SamplingConfig(fps=2.0, max_frames=32)` + `extract_frames()`'s
  `ffmpeg -vf fps=<fps> -frames:v <max_frames>` — capped, always a 16s
  opening window, exactly the problem Phase 8 identified. Left entirely
  unchanged (see §5).
- `target/cache.py` (Phase 6): `EmbeddingCacheEntry.vector: tuple` — one
  vector per cache entry, one JSON file per entry. Structurally cannot
  hold a segment sequence without either overloading `vector`'s meaning or
  reintroducing Phase 8's "54,000 separate files" problem. Conclusion: a
  new, parallel cache type is required (§7), not an extension.
- `target/versioning.py` (`EmbeddingSpec`, `cache_entry_key`): already
  generic over `sampling_config: Mapping[str, object]` — reusable as-is
  for segment specs, no versioning-machinery changes needed.
- `target/registry.py`: composes `TargetRegistry` with an injected
  `TargetEmbeddingCache`. Extended (not replaced) with an optional,
  separately-injected `SegmentEmbeddingCache` — default `None`, so every
  existing `TargetRegistry(redis_client, cache)` call site is unaffected.
- `docs/design/design-proposal-1.md` §5/§6: job lifecycle already scopes
  one worker job to one `target_id`/`target_version` — matching is
  1:1 (candidate vs. the one named target), not a many-target search. This
  directly resolves the FAISS question (§9) and the coarse-to-fine
  question (§8): coarse-to-fine here means "skip fine matching against
  this one target," not "route among many targets."
- `work_queue/results.py` (Phase 4): `Result`/`ResultRecord` is the final,
  possibly multi-technique record a worker commits. Phase 9's
  `TemporalMatchResult` is evidence for *one* technique, not a replacement
  for `Result` — no changes made to `work_queue/results.py`.

## 4. Segment representation decision

**IMPLEMENTED.** `embedding.result.SegmentEmbedding`:

```python
@dataclass(frozen=True)
class SegmentEmbedding:
    segment_index: int      # 0-based, presentation order
    start_time: float       # seconds from video start
    end_time: float         # seconds from video start
    vector: Tuple[float, ...]
```

Validated at construction (`segment_index >= 0`, `end_time >= start_time`,
non-empty vector). A video's full representation is
`VideoSegmentEmbeddingResult`: an ordered tuple of `SegmentEmbedding` (the
primary, fine-layer representation — never collapsed before matching) plus
`coarse_vector` (mean-pool of the segments, re-normalized — the Phase 8
"coarse layer", derived from the fine layer, not a second inference pass).

This is additive: `EmbeddingResult`/`SamplingConfig` (Phase 7's pooled,
16s-window path) are untouched. `embed()` still exists and still produces
the old representation — kept for Phase 6/7 contract compatibility (their
tests still pass unmodified, see §15). Production video matching should
use `embed_video_segments()`/`VideoSegmentEmbeddingResult` instead; the old
path is not deleted because deleting Phase 7 code un-asked is outside this
phase's brief, but it should be considered superseded for the matching use
case per Phase 8's own conclusion.

## 5. Segment duration decision

**PROVISIONAL HEURISTIC — NOT VALIDATED.** `SegmentSamplingConfig.segment_duration_s`
defaults to **5.0 seconds** (`embedding.config.DEFAULT_SEGMENT_DURATION_S`).

Phase 8 was explicit that this number should be "driven by the shortest
pirated clip length worth detecting — a product/threat-model question, not
a compute question" — and that question has no answer yet (no labeled
data, no product decision on record). 5s is a middle point between Phase
8's two illustrative options (10s coarse / 2s dense): dense enough that a
`min_matched_segments=3` run (§10) corresponds to a 15s+ matched clip,
which feels like a reasonable lower bound for "a clip worth flagging"
without being able to justify it further; coarse enough to keep segment
counts and storage bounded (see §14). The dataclass and matcher take
`segment_duration_s` as a parameter specifically so this number can change
without a structural rewrite once real data exists to calibrate it.

## 6. Intra-segment sampling decision

**PROVISIONAL / ARCHITECTURAL SIMPLICITY CHOICE.** One representative
frame per segment, sampled at the segment's start
(`SegmentSamplingConfig.frame_selection = "segment_start"`), via
`ffmpeg -vf fps=1/segment_duration_s` — the same mechanism
`extract_frames` already used for Phase 7, just without the `-frames:v`
cap so it runs to end-of-input instead of stopping after `max_frames`
(`embedding/frames.py:extract_segment_frames`). Phase 8 left open "one
representative frame vs. mean-pooling a few frames within the segment";
this phase chose the single-frame option because it reuses the exact
extraction/embedding mechanism Phase 7 already validated, at the cost of
being more sensitive to a single unlucky frame (e.g. a mid-transition
frame) than a within-segment mean-pool would be. Not measured against the
alternative — a candidate for revisiting once real match/non-match data
exists.

## 7. Storage/cache decision

**IMPLEMENTED — new cache type, not an extension.** `target/segment_cache.py`:
`SegmentEmbeddingCache` (ABC) + `FilesystemSegmentEmbeddingCache`, storing
**one JSON file per `(target_id, target_version, content_sha256, spec)`**
containing the *entire* segment sequence + coarse vector — not one file
per segment. This directly resolves the "54,000 separate files" problem
Phase 8 measured for a 100-target library at segment granularity: file
count scales with target count, not target count × segments-per-target.

Reuses `target.versioning.EmbeddingSpec`/`cache_entry_key` unchanged
(confirmed generic enough in §3). Still JSON, not the `.npy`/columnar
format Phase 8 floated — at this phase's scale (§14) JSON parse/serialize
cost is not the bottleneck; revisit if Phase 11 benchmarks say otherwise.

`TargetRegistry` gained `register_segment_embedding` /
`get_compatible_segment_embedding` / `has_compatible_segment_embedding`,
mirroring the Phase 6 pooled-vector methods, via an optional
`segment_cache` constructor argument (default `None`, backward compatible
— see §3). The pooled cache and segment cache are verified independent in
`tests/test_segment_cache.py::test_pooled_and_segment_caches_are_independent`.

## 8. Similarity metric

**IMPLEMENTED.** Cosine similarity, defensively normalized (does not
assume inputs are already unit-norm even though the DINOv2 engine
normalizes by default) — `matching/similarity.py`. Two forms: a single-pair
`cosine_similarity` (used for the coarse screen) and a vectorized
`cosine_similarity_matrix` (candidate segments × target segments, one
matmul via numpy/BLAS — used for the fine pass).

## 9. Coarse-to-fine behavior and the FAISS question

**IMPLEMENTED (coarse-to-fine); FAISS NOT ADOPTED — justified
quantitatively below.**

Per §3, one worker job compares one candidate against the *one* target its
job names (`docs/design/design-proposal-1.md` §5) — this is a 1:1
comparison, not a many-target search. `matching.matcher.coarse_screen`
compares the two `coarse_vector`s first (cheap, one cosine op); only a
candidate that passes proceeds to the O(N×M) segment-level pass
(`match_segments`). A candidate that fails never pays for the dense
matrix — this is the "coarse-to-fine" behavior the phase brief asked for,
implemented without any indexing infrastructure.

FAISS would only earn its keep for a many-target search (Phase 12
crawler-integration territory, explicitly out of scope here) or if the 1:1
dense comparison itself were too slow. Measured on this dev machine
(`matching/similarity.py`, numpy/BLAS, single CPU core, no GPU):

| Scenario | Shape | Wall time |
|---|---|---|
| 2h target vs. 2h candidate, 5s segments (~1,440 × 1,440 × 768-dim) | dense cosine matrix | **0.019s** |
| 2h candidate vs. Phase 8's full 100-target-library estimate (~1,440 × 54,000 × 768-dim) | dense cosine matrix | **0.80s** |

Even the many-target worst case (which Phase 9's job model doesn't
actually need — §3) is sub-second with plain numpy. There is no
quantitative case for FAISS at this phase's scale; it is explicitly
deferred to whichever later phase (Phase 11 benchmarks, Phase 12 crawler
integration) first demonstrates a workload where brute force is no longer
sufficient.

## 10. Temporal consistency algorithm

**IMPLEMENTED — dominant-offset + longest-run, not DTW.** Per the phase
brief's explicit "do not over-engineer" instruction, `matching.matcher.match_segments`:

1. Build the full candidate×target cosine matrix (§8), keep pairs
   `(candidate_segment_index, target_segment_index, similarity)` above
   `MatcherConfig.segment_similarity_threshold` ("hits").
2. Histogram hits by integer offset `target_segment_index - candidate_segment_index`;
   take the mode as `dominant_offset` — the constant offset a genuinely
   copied clip should exhibit (`candidate_time ≈ target_time + offset`,
   per the phase brief).
3. Keep only hits within `MatcherConfig.offset_tolerance_segments` of the
   dominant offset (tolerates small boundary/off-by-one differences).
4. Collapse to one best-similarity target match per candidate segment
   (handles repeated content — see §11's worked case — without branching
   into an exhaustive path search).
5. Walk the remaining pairs in candidate order, extracting the longest run
   where the target index is non-decreasing and the gap between
   consecutive candidate indices is within `MatcherConfig.max_index_gap`
   (tolerates a dropped/low-similarity segment without breaking an
   otherwise continuous run — the "missing/extra frames" tolerance the
   phase brief asked for).
6. `matched = True` iff the best run's length ≥ `MatcherConfig.min_matched_segments`
   **and** its mean similarity ≥ `segment_similarity_threshold`.

This is exactly the mechanism that prevents one accidental high-similarity
segment from producing `matched=True` (verified in
`tests/test_matching.py::test_isolated_high_similarity_segment_is_not_a_match`,
`test_non_monotonic_accidental_matches_do_not_form_a_run`,
`test_reordered_segments_do_not_match`) — those all produce a
best-run length of 1, below `min_matched_segments`.

Not implemented, by design: dynamic time warping, weighted/EM offset
estimation, frame-rate-aware continuous-time offset clustering (current
model assumes matching `segment_duration_s` on both sides — a genuine
limitation, see §16).

## 11. Partial-match behavior

**IMPLEMENTED and tested.** Because the run-extraction in §10 operates
over whatever subset of segments actually pass threshold, it naturally
handles:

- Full match (`test_exact_full_match`).
- A short clip matching the *middle* of a long target
  (`test_partial_middle_match`, and the phase brief's own worked example
  in `test_brief_example_shifted_start_and_end`: target
  `A B C D E F G H I J`, candidate `X Y C D E F Z` → detects target C-F ↔
  candidate C-F, offset 0 once both sides' unrelated prefix/suffix
  segments are excluded from the run).
- Shifted start/end (extra unrelated segments before/after the matched
  region — `test_shifted_start`, `test_shifted_end`).
- Repeated content resolving to the *correct* continuous run rather than
  an ambiguous single-segment match
  (`test_repeated_content_resolves_correct_run`: target
  `A B C D E C F G` has `C` twice; candidate `C D E` correctly anchors to
  the first `C` because only that branch extends into a 3-segment run).

`target_start`/`target_end`/`candidate_start`/`candidate_end` on
`TemporalMatchResult` give exactly the localization the phase brief asked
for ("which temporal region of C matches which temporal region of T").

## 12. Result schema

**IMPLEMENTED.** `matching/result.py`:

```python
@dataclass(frozen=True)
class MatchedSegmentPair:
    target_segment_index: int
    candidate_segment_index: int
    similarity: float

@dataclass(frozen=True)
class TemporalMatchResult:
    matched: bool
    score: float                        # mean similarity of the winning run
    target_id: str
    target_version: str
    candidate_id: str
    matcher_version: str                # "temporal_v1"
    matched_duration_s: float
    target_start: Optional[float]
    target_end: Optional[float]
    candidate_start: Optional[float]
    candidate_end: Optional[float]
    matched_segment_count: int
    total_target_segments: int
    total_candidate_segments: int
    temporal_offset_s: Optional[float]
    mean_similarity: Optional[float]
    coarse_similarity: Optional[float] = None
    matched_pairs: Tuple[MatchedSegmentPair, ...] = ()
```

This is technique-specific evidence, scoped to one (target, candidate)
pair — not `work_queue.results.Result` (Phase 4's final, possibly
multi-technique record). Phase 10 is expected to consume one or more
`TemporalMatchResult`s (this technique, later audio/OCR/watermark) and
fold them into a `Result`. No speculative fields were added beyond what
the phase brief listed; `matcher_version` lets Phase 10 tell which
algorithm version produced a given piece of evidence if the algorithm
changes later.

## 13. Threshold status

None of the following have been calibrated against a real labeled
dataset — no such dataset exists yet in this project. See
`matching/config.py`'s module docstring for the same accounting in code.

| Field | Status | Value | Note |
|---|---|---|---|
| `segment_similarity_threshold` | PROVISIONAL HEURISTIC | 0.90 | Directionally consistent with the prototype's `cosine_threshold=0.93` (`docs/design/design-proposal-1.md` §6), not re-derived. |
| `coarse_similarity_threshold` | PROVISIONAL HEURISTIC | 0.60 | Deliberately looser than the segment threshold — a real partial-clip match can have mediocre coarse similarity while still containing a strong segment-level match; see `matching/config.py` docstring. |
| `min_matched_segments` (existence) | ARCHITECTURAL | — | The gate itself — "some minimum run length must exist" — is a design decision, not a fitted number. |
| `min_matched_segments` (value) | PROVISIONAL HEURISTIC | 3 | At the 5s provisional segment duration (§5), 3 segments ≈ 15s of matched content. |
| `offset_tolerance_segments` (existence) | ARCHITECTURAL | — | Some drift tolerance must exist for real-world encoding differences. |
| `offset_tolerance_segments` (value) | PROVISIONAL HEURISTIC | 1 | |
| `max_index_gap` (existence) | ARCHITECTURAL | — | Some gap tolerance must exist to survive a single dropped/low-similarity segment. |
| `max_index_gap` (value) | PROVISIONAL HEURISTIC | 2 | |
| `segment_duration_s` | PROVISIONAL HEURISTIC | 5.0 | See §5. |

**None of these are described as validated anywhere in code or docs.**
They exist as `MatcherConfig`/`SegmentSamplingConfig` fields specifically
so they can be swapped without a structural change once real
match/non-match data is available to calibrate against — that calibration
work is out of scope for this phase.

## 14. Complexity

- **Segment extraction**: one `ffmpeg` pass over the full file (no frame
  cap), linear in video duration — see Phase 8's own per-video timing
  table (still applicable; this phase didn't change the per-frame
  inference cost, only removed the 16s cap).
- **Storage**: one JSON file per target representation, not per segment
  (§7) — file count scales with target count. Per-file size scales with
  `segment_count × dimensionality` (Phase 8 measured ~9MB for a 90-minute
  film at 10s granularity in this JSON format; 5s granularity roughly
  doubles that).
- **Matching**: O(N×M×D) for the dense cosine matrix (N, M = segment
  counts, D = 768). Measured (§9): sub-second even at a 54,000-vector
  library-wide scale, which this phase's 1:1 job model doesn't actually
  require. Memory: the similarity matrix itself is the dominant cost,
  `N×M×8 bytes` (float64) — ~622MB for the 1,440×54,000 case in §9, ~17MB
  for a realistic 1:1 job.
- **Temporal consistency pass**: O(hits log hits) for the offset
  histogram/sort, where `hits ≤ N×M` — negligible next to the matrix
  build.

No multiprocessing, async, or caching layers were added for this phase,
per the brief's "avoid unnecessary X" list — the numbers above show none
are needed yet.

## 15. Tests

New/changed test files:

- `tests/test_matching.py` (20 tests) — deterministic one-hot-vector
  synthetic embeddings (exact cosine similarity by construction), covering
  every case in the phase brief's test-data list: exact full match,
  partial middle match, shifted start, shifted end, the brief's own
  worked example, isolated high-similarity segment, non-monotonic
  accidental matches, reordered segments, repeated content, no match,
  empty target/candidate/both, malformed segment metadata (out-of-order
  index), inconsistent within-side dimensionality, mismatched
  cross-side dimensionality, coarse screening (pass/reject/short-circuit),
  matcher_version recording.
- `tests/test_segment_cache.py` (8 tests) — `FilesystemSegmentEmbeddingCache`
  round-trip, one-file-per-target verification, sampling-config
  incompatibility, corruption handling, `TargetRegistry` segment-cache
  integration (including the no-segment-cache-configured case), and
  pooled/segment cache independence.
- `tests/test_embedding.py` — 16 existing Phase 7 tests unchanged/still
  passing; 6 new tests (17-22) against real DINOv2 inference on the
  existing `tiny_video.mp4` fixture (2s, no new/downloaded media, per the
  phase brief): full-duration segment coverage, segment shape/timing,
  coarse-vector-is-pooled-not-separately-inferred, `EmbeddingSpec`
  conversion, segment cache round-trip, non-video rejection.

Results actually run for this phase:

```
tests/test_matching.py ........................  20 passed
tests/test_segment_cache.py ....................   8 passed
tests/test_embedding.py (16 Phase 7 + 6 new) ...  22 passed
tests/test_target.py (Phase 6, unaffected) .....  20 passed
tests/test_results.py (Phase 4, unaffected) ....   6 passed
```
(`test_target.py`/`test_results.py` counts are pre-existing, run to
confirm no regression from the `target/registry.py` change — see §7.)

Full repository suite (`pytest -q`, all phases): **121 passed**, 0
failures, 0 skipped — run once at the end to confirm no cross-phase
regression, not repeatedly during development, per the phase brief's cost
constraint.

## 16. Limitations

- **Offset model assumes equal segment duration on both sides.** The
  temporal-consistency algorithm (§10) works in segment-index space, not
  continuous time, so it only produces a correct offset when the target's
  and candidate's `SegmentSamplingConfig.segment_duration_s` match. A
  genuinely different frame-rate/segment-duration comparison would need a
  continuous-time offset (seconds, not index count) — noted as a
  deferred improvement (§18), not implemented.
- **Single frame per segment** (§6) is more sensitive to an unlucky
  mid-segment frame (scene transition, motion blur) than a
  within-segment mean-pool would be — not measured against the
  alternative.
- **No real labeled evaluation.** Every threshold in §13 is provisional.
  This phase cannot report precision/recall, false-positive rate, or any
  accuracy metric — none of that is knowable without labeled data.
- **JSON storage**, while adequate at this phase's scale (§14), is not
  the eventual production format Phase 8 anticipated (`.npy`/columnar) —
  acceptable now, flagged for revisit under real load.
- **`embed_video_segments` recomputes segments** on every call; nothing
  in this phase wires it into `TargetRegistry`'s "check cache, build on
  miss" pattern the way Phase 6's pooled path does for a real pipeline —
  the storage/retrieval primitives exist (§7) but the worker-side
  build-on-miss orchestration is deferred (see §18; no fingerprint-worker
  handler exists yet at all, per Phase 7's own deferred-decisions note).

## 17. Deferred decisions

Carried forward from Phase 8, still not resolved (explicitly out of scope
here too, or resolved provisionally as noted):

- Real-data calibration of every threshold in §13 — needs a labeled
  dataset that does not exist yet.
- Continuous-time (rather than segment-index) offset modeling for
  target/candidate pairs embedded at different segment durations.
- Binary/columnar segment storage format (`.npy`-equivalent) — deferred
  until real load demonstrates JSON is the bottleneck.
- FAISS/ANN indexing — deferred until a many-target search workload
  actually exists (Phase 11 benchmarks / Phase 12 crawler integration).
- Wiring `embed_video_segments` + the segment cache into an actual
  fingerprint-worker handler and `TargetRegistry`'s build-on-miss lock
  pattern (Phase 8's "Implications for distributed GPU workers" — lease
  timeout scaling with duration — also still unresolved).
- Multi-technique aggregation of `TemporalMatchResult` into `Result`
  (Phase 10, by design).
- Mean-pool-within-segment as an alternative to single-frame sampling
  (§6) — not evaluated against the chosen approach.

## 18. Interface expected by Phase 10

Phase 10 ("Multi-technique aggregation") should consume:

```python
from matching import MatcherConfig, match_segments, coarse_screen
from matching.result import TemporalMatchResult, MatchedSegmentPair
```

`match_segments(target_segments, candidate_segments, target_id,
target_version, candidate_id, config=None, target_coarse_vector=None,
candidate_coarse_vector=None) -> TemporalMatchResult` is the single entry
point — it internally runs the coarse screen (§9) if coarse vectors are
supplied, then the segment/temporal pass (§10). Segments come from
`DINOv2EmbeddingEngine.embed_video_segments(artifact) -> VideoSegmentEmbeddingResult`
(candidate side, computed fresh per job) and
`TargetRegistry.get_compatible_segment_embedding(target_id, target_version, spec) -> Optional[SegmentEmbeddingCacheEntry]`
(target side, cached).

Phase 10 should treat `TemporalMatchResult` as one technique's evidence
input to whatever combination logic it builds — `matched`/`score` are this
technique's own decision/confidence, not a final `Result.decision`;
`matcher_version` should be preserved into whatever combined evidence
record Phase 10 produces, so a later reader can tell which matcher
algorithm version contributed which score.
