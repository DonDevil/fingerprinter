# Phase 8 — Video Representation / Sampling Investigation

## Objective

Phase 7 proved the embedding *contract* (`MediaArtifact -> EmbeddingResult ->
EmbeddingSpec`) using a deliberately naive video representation: sample at
`fps=2` up to `max_frames=32`, mean-pool. Before building similarity
matching (FAISS, thresholds, temporal voting), this phase investigates
whether that representation is *architecturally* adequate for real
piracy-video detection, or whether Phase 8/9 would be building a matching
pipeline on top of a representation that cannot support the task.

This is an investigation only. No FAISS, no thresholds, no temporal
voting, no production code changes. Conclusions are derived from
calculations against the existing `SamplingConfig`/`extract_frames`
implementation (`embedding/config.py`, `embedding/frames.py`) and Phase 7's
own diagnostic timings, plus one tiny synthetic JSON-size measurement (an
inline calculation, not a committed script — see "Storage estimates").

## Current Phase 7 limitation

`SamplingConfig(fps=2.0, max_frames=32, frame_selection="uniform_time_from_start")`
combined with `extract_frames()`'s ffmpeg invocation
(`-vf fps=<fps> -frames:v <max_frames>`, `embedding/frames.py:37-89`) means:

- Sampling always starts at t=0 (`ffmpeg -vf fps=` samples from the first
  frame in presentation order).
- Extraction stops as soon as either `max_frames` frames have been
  produced **or** the input ends — whichever comes first.
- At `fps=2`, `max_frames=32` is reached after exactly **16 seconds** of
  source video, for *any* video longer than 16 seconds.

The config's own name, `frame_selection="uniform_time_from_start"`, is
accurate about the sampling *within* the captured window (frames are
evenly spaced at 2 fps) but misleading about coverage: for any video
longer than 16s, this is not uniform sampling *of the video* — it is
**first-16-seconds sampling**, full stop. The `mean_pool_l2_normalized`
aggregation then collapses those 32 frames into a single 768-d vector
that represents, at best, a 16-second opening clip — never the movie.

This was correct scope for Phase 7 (prove the contract on `tiny_video.mp4`,
a 4s synthetic clip, where 16s of coverage is more than the whole input).
It is not correct scope for real target movies or real candidate crawl
video, which is exactly what this phase was asked to check before Phase
8/9 build matching on top of it.

## Coverage analysis

Fixed capture window = `max_frames / fps` = `32 / 2` = **16 seconds**,
independent of video length:

| Video duration | Captured window | Coverage |
|---|---|---|
| 1 minute (60s) | 16s | 26.7% |
| 10 minutes (600s) | 16s | 2.7% |
| 30 minutes (1,800s) | 16s | 0.89% |
| 90 minutes (5,400s) | 16s | 0.30% |
| 120 minutes (7,200s) | 16s | 0.22% |

For any content in the "movie" range (60–150 min is typical for the
piracy-detection target set this project cares about), coverage is
**under 1%**, and that 1% is always the same fixed opening segment. Any
pirated upload that trims, re-encodes, or overlays the first 16 seconds
(intro cards, uploader watermarks, a different logo bumper, a shifted
start point from re-encoding) invalidates the entire representation —
there is no fallback signal from the rest of the film. Conversely, a
30-second pirated *clip* excerpted from minute 47 of a target movie has
zero chance of matching today, regardless of any future similarity
threshold, because minute 47 was never embedded.

This is not a threshold-tuning problem (Phase 9's job) — it is a coverage
problem, and no similarity threshold can fix a representation that never
looked at the relevant frames.

## Candidate strategies

| # | Strategy | What it captures | Verdict |
|---|---|---|---|
| A | First-N-seconds sampling | A fixed opening window | This is what Phase 7 does today, effectively. Inadequate alone — see coverage analysis. |
| B | Uniform sampling across the entire video | Representative frames spread over full duration | Necessary baseline — coverage must scale with duration, not be a fixed constant. |
| C | Fixed-duration temporal segments | Video divided into e.g. 10s chunks, each independently embedded | Building block for localization: turns "is this video similar" into "which part of the video is similar." |
| D | Coarse-to-fine sampling | Cheap sparse pass first (reject/accept fast), expensive dense pass only on survivors | A compute-scaling strategy, not a representation by itself — layers on top of B/C/E. |
| E | Frame/segment embeddings retained individually | Many vectors per video instead of one | Required for any temporal matching (partial clips, reordering, edits) — a single pooled vector structurally cannot support this. |
| F | Single mean-pooled video embedding | One vector, whole video (or whole sampled window) averaged together | Good as a coarse/global signature (dedup, fast pre-filter). Structurally unable to localize or detect partial overlap once pooled over long, diverse content. |

These aren't mutually exclusive — the recommendation below is a
combination (B+C+E as the primary representation, F retained as a derived
coarse layer, D as the compute-scaling strategy around both).

Why F alone breaks down as duration grows: mean pooling is an unweighted
centroid. A 16-second, single-scene window pools coherently. A 2-hour
film pooled into one 768-d centroid averages together dozens of
visually unrelated scenes — the resulting vector represents none of them
well, and two unrelated 2-hour films can plausibly pool to similar
centroids (visually diverse "average frame") that a short, focused clip's
embedding wouldn't cosine-match well against even if the clip *is* lifted
from that film. Pooling error grows with how much distinct visual content
is being averaged together, not just with frame count.

## Computational estimates

From Phase 7's diagnostic table (`phase-07-dinov2-embedding.md`,
"Diagnostic measurements"): a 4-frame video took 0.52s CPU / 0.16s CUDA
total, and single-image warm inference was 0.12s CPU / 0.03s CUDA. The
4-frame figure includes fixed per-call overhead amortized over only 4
frames; the steady-state per-frame cost at larger frame counts is closer
to the single-image number. Estimates below use **0.12s/frame (CPU)** and
**0.035s/frame (CUDA)**, consistent with both measurements, on the same
dev hardware (RTX 2050, 4GB VRAM) — diagnostic-grade, not a benchmark.

**Today's config (fps=2, capped at 32 frames):** cost is *constant*
regardless of duration — always ≤32 frames ⇒ ~3.8s CPU / ~1.1s CUDA per
video, whether the source is 1 minute or 2 hours. This constant cost is
precisely the symptom of the coverage problem above, not a virtue: it's
cheap because it's not looking at 99%+ of the video.

**Proposed: one embedded frame per 10-second segment, spanning the full
duration** (segment length chosen for illustration — see "Deferred
decisions"):

| Duration | Segments (frames) | CPU estimate | CUDA estimate |
|---|---|---|---|
| 1 min | 6 | 0.7s | 0.2s |
| 10 min | 60 | 7.2s | 2.1s |
| 30 min | 180 | 21.6s | 6.3s |
| 90 min | 540 | 64.8s (~1.1 min) | 18.9s |
| 120 min | 720 | 86.4s (~1.4 min) | 25.2s |

Even CPU-only, a full 2-hour film costs under 1.5 minutes to embed at 10s
granularity — this is a tractable per-video cost, not a blocker. A denser
alternative (one embedded frame per 2-second segment, i.e. keep the
current 2 fps rate but apply it across the whole file instead of capping
at 16s):

| Duration | Segments (frames) | CPU estimate | CUDA estimate |
|---|---|---|---|
| 90 min | 2,700 | 324s (~5.4 min) | 94.5s (~1.6 min) |
| 120 min | 3,600 | 432s (~7.2 min) | 126s (~2.1 min) |

Still tractable per-video on a single consumer GPU, but the difference
between ~19s and ~95s per 90-minute film (CUDA) matters once multiplied
across a target library and, especially, across crawl volume — this is
where strategy D (coarse-to-fine) earns its keep (see below). Phase 7
also explicitly deferred batched frame inference; batching becomes more
valuable, not less, once per-video frame counts grow from ~32 to the
hundreds/thousands implied here.

## Storage estimates

Raw vector size: 768 × float32 = **3,072 bytes (3 KiB)** per embedding.

The existing `FilesystemEmbeddingCache` (`target/cache.py`) stores one
JSON file per `(target_id, target_version, content_sha256, spec)`
representation, containing the vector as a JSON float list plus
compatibility metadata (`payload` in `FilesystemEmbeddingCache.put`,
`target/cache.py:108-136`). A tiny inline measurement (not committed —
`json.dumps` on a realistic 768-float payload plus the actual metadata
fields Phase 6/7 store) gives:

| Encoding | Bytes | Notes |
|---|---|---|
| Raw float32 vector | 3,072 | 4 bytes × 768 |
| JSON vector list only | 15,964 | ~20.8 bytes/float — Python's `repr(float)` plus JSON punctuation |
| Full cache entry (current format: vector + metadata) | 16,701 | what one `.json` file under the cache dir actually costs today |

JSON storage is **~5.4x** the raw binary size for a single vector. That
overhead is irrelevant at "one vector per movie" scale and becomes the
dominant cost once segment-level storage (strategy E) is adopted:

**One target movie:**
- Current (1 pooled vector): ~16.3 KB (one JSON file).
- Segment-level, 10s granularity, 90 min film (540 segments): raw
  540 × 3,072B ≈ **1.66 MB**; as 540 separate JSON files in today's format,
  ≈ **9 MB** *and* 540 individual small files on disk.

**100 target movies** (segment-level, 10s granularity, ~90 min avg):
raw ≈ 100 × 1.66 MB ≈ **166 MB** — trivial for disk space. But as
individual JSON files in today's one-file-per-vector cache format, that's
**54,000 separate files**, which is a filesystem-inode/directory-listing
concern and a cache-API mismatch (`TargetEmbeddingCache.get`/`put` model
exactly one vector per representation today, not an array — see "Phase 6
cache design implications" below).

**A candidate video** (crawl-side, worst case ~2 hours): same order of
magnitude as a target movie of equal length — ~2.2 MB raw at 10s
segment-level granularity, or ~16 KB for a single coarse/global vector if
only the coarse pass runs (the common case for crawled non-matches, see
coarse-to-fine below).

Conclusion: raw binary vector storage is cheap at every scale considered
here (hundreds of MB, not a capacity problem). The one-JSON-file-per-
vector format Phase 6 shipped is fine for "one vector per target" but does
not scale to segment-level storage — this is a cache *format* problem, not
a storage *capacity* problem.

## Advantages / disadvantages summary

| Approach | Advantage | Disadvantage |
|---|---|---|
| A. First-N-seconds (today) | Cheapest, constant-time | Coverage collapses to ~0% for real movie lengths; trivially defeated by any change to the opening seconds |
| B. Uniform full-duration sampling | Coverage scales with content, not a fixed window | Alone (with pooling into one vector) still loses localization |
| C. Fixed-duration segments | Enables "where in the video" answers, not just "does it match" | More vectors to store/index/compare than a single pooled vector |
| D. Coarse-to-fine | Large compute savings at crawl scale (most candidates are non-matches) | Two-pass complexity; coarse pass can miss true positives if too aggressive (threshold-tuning risk, deferred to Phase 9) |
| E. Retain per-segment embeddings | Only way to detect partial/edited/reordered clips | Storage/index size scales with duration × segment density; today's cache format doesn't support it (see below) |
| F. Single mean-pooled vector | Cheap to store/compare; good coarse signature | Cannot localize; pooling quality degrades as more distinct content gets averaged together |

## Recommended representation

A **two-layer representation**, not a single choice from the list above:

1. **Fine layer (primary): per-segment embeddings, retained
   individually** (strategy C + E). Video divided into fixed-duration
   segments spanning the *entire* duration (strategy B), each segment
   embedded independently (one representative frame or a small mean-pool
   within the segment — segment length and intra-segment sampling density
   are deferred, see below). This is the representation that can actually
   answer the piracy-detection question that matters: "does any part of
   this candidate video match any part of this target movie."
2. **Coarse layer (screening only): a single mean-pooled vector**
   (strategy F), computed by pooling the fine-layer segment embeddings
   (not a separate embedding pass) — retained as a fast global signature
   for cheap first-pass filtering and whole-video deduplication, not as
   the basis for a match/no-match decision on its own.

A single mean-pooled vector alone (today's Phase 7 output, even if fixed
to cover the full duration) cannot support clip-level or partial-match
detection, which is core to the piracy-detection goal — pirated uploads
are frequently trimmed, re-cut, or partial. Per-segment embeddings alone
(no coarse layer) work but forfeit the cheap-rejection speed a global
signature gives for the common "totally unrelated video" case at crawl
scale. Keeping both, derived from one embedding pass, gets both
properties without double the inference cost.

## Recommended sampling strategy

Uniform sampling across the **entire** video (strategy B), organized into
**fixed-duration segments** (strategy C) spanning full duration, each
independently embedded and retained (strategy E). Concretely, replace
"sample 2 fps up to a 32-frame/16s cap" with "sample across the whole
file, chunked into fixed-length segments, one embedding per segment" —
removing the hard 16-second ceiling entirely. Layer strategy D
(coarse-to-fine) on top for crawl-scale compute control: a cheap coarse
pass (the pooled global vector, or a very sparse frame sample) run first
against a coarse index to reject obvious non-matches before paying for
dense per-segment embedding on survivors.

Exact segment duration (5s / 10s / 30s) is explicitly **not** decided here
— see "Deferred decisions." The computational estimates above show even
the denser 2s-segment option is affordable per-video; the real driver for
segment length should be the shortest pirated clip length worth detecting,
which is a product/threat-model question, not a compute question.

## Should mean pooling remain?

**Demoted to a coarse screening representation — not removed, not
primary.** It remains useful as: a cheap whole-video signature for
dedup/near-duplicate detection, and as the first stage of a coarse-to-fine
pipeline. It should not remain the *only* stored representation, and
should not be the basis of a final match decision for anything longer
than a single short segment, because it structurally cannot localize or
survive partial-content matches (see "Recommended representation" above).

## Should frame/segment-level embeddings be retained?

**Yes.** This is the main architectural conclusion of this investigation.
Without retaining per-segment embeddings, no future phase can build
temporal matching, partial-clip detection, or localization, regardless of
how sophisticated the similarity/threshold logic gets — the information
would already be gone, averaged away at embedding time. This is a
decision that has to be made now, before Phase 6's cache format and
Phase 9's indexing are built around one shape or the other.

## Implications for Phase 6 cache design

Phase 6's `TargetEmbeddingCache` (`target/cache.py`) and `EmbeddingSpec`
(`target/versioning.py`) were built around **exactly one vector per
`(target_id, target_version, content_sha256, spec)`** —
`EmbeddingCacheEntry.vector: tuple`, one JSON file per representation.
Retaining segment-level embeddings does not fit this shape without a
change:

- Either `EmbeddingCacheEntry` grows to hold a *sequence* of vectors plus
  per-segment metadata (segment index, start/end time), stored as one
  entry (one file/blob per target-content-spec, containing N vectors) —
  closer to a `.npy`/columnar array than N separate JSON files (the
  measured 54,000-small-files case above is the argument against
  continuing one-file-per-vector at segment granularity).
- Or the cache key gains a segment dimension (`spec` or a new parameter
  identifies which segment), and `get`/`put`/`exists` are called once per
  segment — keeps the existing per-vector shape but multiplies calls and
  files by segment count, reintroducing the small-files problem.

Either way, `EmbeddingSpec`'s compatibility fields
(`preprocessing_config`, `sampling_config`) still apply per representation
— `sampling_config` would need to represent "segment length + intra-
segment sampling," not "fps + max_frames capped globally," which is
itself a breaking change to `SamplingConfig` and therefore to every
existing cache entry's compatibility key (expected — Phase 6 already
treats `sampling_config` changes as the correct invalidation trigger for
exactly this situation, per `versioning.py`'s design). This is a schema
decision for whichever phase actually implements segment-level storage,
not resolved here.

## Implications for FAISS / indexing

Retaining segment embeddings is the difference between FAISS being able
to answer "is this candidate video similar to this whole target" (all
that a mean-pooled-only representation supports) versus "does this
candidate contain a segment matching any segment of any target" (the
actual piracy-detection question). At the scale sized above — 100 target
movies × ~540 segments each ≈ 54,000 vectors, ~166 MB raw — a flat FAISS
index is comfortably sufficient; no quantization/IVF is needed yet at
target-library scale. Crawl-scale candidate volume (potentially far more
segments per day than the target library holds) is where an IVF/PQ index
would eventually matter, and where the coarse-to-fine pass matters even
more (avoid dense-embedding and indexing every crawled video's segments
when a coarse pass could reject most of them first). Both are Phase 9
decisions; this phase only establishes that segment-level storage is a
prerequisite for FAISS to be asked the right question at all.

## Implications for distributed GPU workers

Today's fixed 32-frame cap makes every video's embedding job cost roughly
the same regardless of duration — convenient for worker/lease-timeout
sizing (Phase 1-3's retry/backoff and lease-timeout logic implicitly
assumed short, roughly-uniform job durations), but only because the
representation was throwing away almost all of the video. Once coverage
scales with duration (as recommended), job duration becomes
duration-dependent: per the estimates above, a 120-minute film costs
roughly 7x a 15-minute video at the same segment density. Distributed
worker/queue sizing and lease timeouts (Phase 2's crash-recovery leases)
should account for this variance rather than assuming uniform job cost —
concretely, lease timeouts may need to scale with source media duration
rather than being a fixed constant. This is a tuning concern for whichever
phase wires a real fingerprint-worker handler (still deferred per Phase
7), not something to resolve here.

## Deferred decisions

Explicitly not decided by this investigation — left for the phase that
implements the change:

- Exact segment duration (5s / 10s / 30s / other) — should be driven by
  the shortest pirated clip length worth detecting (a product/threat-model
  question), not purely by the compute estimates above.
- Intra-segment sampling density (one representative frame per segment vs.
  mean-pooling a few frames per segment).
- Whether the coarse global vector is computed as a separate pass or
  derived by pooling the fine-layer segment embeddings (this doc assumes
  the latter, to avoid double inference cost, but doesn't implement it).
- Binary/columnar storage format for segment-level cache entries (e.g. one
  `.npy`-style array per target instead of N JSON files) — a Phase 6
  cache-format change.
- `SamplingConfig`/`EmbeddingSpec` schema changes needed to represent
  "segment length + intra-segment density" instead of "fps + global
  max_frames cap" — breaking change to the existing compatibility key,
  by design (see "Phase 6 cache design implications").
- FAISS index type/parameters (flat vs. IVF/PQ) and the coarse/fine index
  split — Phase 9.
- Similarity thresholds and temporal voting/alignment across matched
  segments — Phase 9, explicitly out of scope per this phase's brief.
- Worker lease-timeout tuning for duration-variable embedding jobs.
- Batched frame inference (already deferred from Phase 7; grows more
  valuable as per-video frame counts increase from ~32 to the hundreds).
- Real crawl-data assumptions (actual candidate video length/quality
  distribution) — all estimates here are calculations against Phase 7's
  diagnostic timings on synthetic/dev hardware, not measurements against
  real target or candidate media.

No source code was modified. No FAISS, thresholds, or temporal voting were
implemented, per the phase brief.
