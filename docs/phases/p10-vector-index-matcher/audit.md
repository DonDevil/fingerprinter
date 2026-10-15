# P10 Audit — vector index and matcher

Status: **AUDIT COMPLETE — P10 in design review.** Nothing in this document
is implemented. P9 is complete for its approved scope (accepted 2026-10-15);
P10 has not been approved.
Date: 2026-10-15. Plan section: crawler2 `docs/v2-phase-plan.md` §P10.

> **Correction note (2026-10-15).** After the first design review, design
> §19 (C-1…C-7) corrected the resource budget, low-information freshness,
> the Qdrant/Scylla mutation and rebuild protocol, reconciliation ownership,
> confidence semantics, the `matches` key and the V1 baseline's role. This
> audit is unchanged except for the rows marked as superseded; its
> measurements stand.

Companion documents: [design.md](design.md), [decisions.md](decisions.md),
[validation.md](validation.md), [ADR-028 (proposed)](ADR-028-vector-index.md).

> **Where these documents live.** Every earlier phase keeps its phase
> documents and ADRs in crawler2 (`docs/phases/`, `docs/adr/`). This session
> was instructed to treat crawler2 as read-only, so the P10 documents are in
> the fingerprinter repository, where P10 is implemented. Moving them (or
> linking them) into crawler2's phase tree, ADR index and phase plan is an
> operator decision (decisions.md P10-D16); until then this directory is the
> authoritative P10 record and crawler2 does not mention P10's status.

Evidence produced for this audit (all reproducible, none in a gate):

| Artifact | What |
|---|---|
| `benchmarks/p10-index/encode_corpus.py` | encodes a P9 corpus with the production `dinov2-seg/2` FP32 decode/preprocess/model into a local vector cache (outside git) |
| `benchmarks/p10-index/fetch_soak_vectors.py` | copies the SOAK-1000 run-5 artifacts (`fp-seg-npz/v1`) from the dev MinIO into the cache (read-only) |
| `benchmarks/p10-index/lpairs_eval.py` → `results/lpairs-eval.json` | V1-matcher reproduction and matcher diagnostics on L-PAIRS |
| `benchmarks/p10-index/ann_bench.py` → `results/ann-bench.json`, `results/ann-growth.json`, `results/qdrant-int8-*.json` | exact oracle, FAISS and Qdrant variants, candidate policies, corpus growth |

Vector caches (not in git): `~/anti_piracy/datasets/p10/vectors/l-pairs/`
(187 items, 6,220 vectors, encoded on the RTX 2050 in CUDA FP32) and
`…/soak-1000/` (1,000 items, 98,983 vectors, the SOAK-1000 run-5 production
artifacts). The benchmark venv `~/anti_piracy/datasets/p10/venv` (faiss-cpu
1.12.0, qdrant-client 1.15.1) is separate from `env/`, whose package set is
part of runtime profile `rt-1` and was not touched.

---

## 1. Scope and inspected sources

| Area | Sources read |
|---|---|
| Plan and decisions | crawler2 `docs/v2-phase-plan.md` (decisions log, B.5 invariants, §P8–§P10), `docs/adr/README.md` (ADR-001…027; next free number **028**) |
| P9 (authoritative design and evidence) | crawler2 `docs/phases/p09-fingerprinter-encoder/design.md` §3, §4, §9–§11, §14; `exit.md` §5–§7; `validation.md` §7.4, §8.1; `data-preparation.md` §2, §6, §7 |
| P8 | crawler2 `docs/phases/p08-media-registry/design.md` (identity levels, P10 handoff), `exit.md`, `validation.md` X-1 |
| P7 | crawler2 `docs/phases/p07-crawl-intelligence/audit.md` §2.4 (observed media references) |
| Resource budget | crawler2 `docs/phases/p00-foundations/design.md` §4; `docker-compose.yml` |
| Contracts | crawler2 `contracts/src/antipiracy_contracts/` `models/matching.py`, `events/fingerprinting.py`, `events/catalog.py`, `ids.py`, `base.py`, compat fixture `match.found.v1.json` |
| Fingerprinter local docs | `README.md`, `docs/v2-encoder.md`, `docs/architecture/history/phase-09-temporal-video-matching.md` §13, `docs/architecture/phase-10-multi-technique-aggregation.md` (V1 numbering) |
| V1 matcher code | `matching/{config,similarity,matcher,result,aggregation}.py`, `worker/matching_handler.py`, `embedding/config.py`, `embedding/frames.py`; tests `tests/test_matching.py` (20), `tests/test_aggregation.py` (7) |
| P9 package code | `fingerprinter/{db,artifact,spec,store,commit,reconcile,publish,coord,targets,specs,backfill}.py`; `benchmarks/p9-encoder/{precision,make_real_corpora,soak}.py` |
| Live dev stack (read-only) | Scylla 6.2.3 keyspaces, MinIO buckets (`fp-soak-*`), container limits and usage |

Not inspected (not needed for P10): crawler2 runtime code beyond the
contracts, V1 crawler, P9 encoder internals (`encoder.py`, `media.py`) beyond
the functions reused by the benchmark.

## 2. Repository state at the start

fingerprinter `main` at `74ac9eb` (docs(p9): local guide…). One pre-existing
untracked file, `benchmarks/results/bench_embedding_20261010T174709_79bb9b73.json`
(a V1 embedding benchmark result from 2026-10-10), preserved and not staged.
crawler2 `main` at `08ac92c`, clean, not modified by this work.

## 3. P9 representation model as implemented (verified against code)

| Fact | Where verified | Consequence for P10 |
|---|---|---|
| `RepresentationId = RepresentationId.for_content(content_id, spec.name, spec.version, spec.config_digest)`; precision is inside the canonical spec, so FP32 and FP16 have different digests and ids | `spec.py`, contracts `ids.py:236`, design §3.1 | an index entry must carry the spec digest; vectors of different specs must never share an index |
| Default spec `dinov2-seg/2` FP32, digest `sha256:a4df3d19252891dc…`; `dinov2-seg/1` (`ddfc71c1…`) still valid; FP16 specs `gated` | design §3.2a, `spec.py` | P10 indexes `dinov2-seg/2` first (P9 exit §7 handoff) |
| `model.dim = 768`, CLS output, `postprocess.normalize = l2`, `vector_dtype = float32` | spec document; **measured**: all 104,443 benchmark vectors have norm 1.0 (min = max = 1.0 to 5 decimals) | inner product = cosine; no normalization step needed at query time |
| One vector per 5 s window: the **first decoded frame** whose `floor((t − t0)/5)` exceeds the previous one; absent windows stay absent; `segment_index`, exact `pts`, `pts_s` stored | design §3.3, `artifact.py` | segment boundaries of two copies of the same film differ by the clip's offset modulo 5 s — measured to be the main cause of lost similarity (§7) |
| Artifact `fp-seg-npz/v1`: `segment_index` int32, `pts` int64, `time_base`, `pts_s`/`window_start_s`/`window_end_s` float64, `vectors` float32 [N,768], `coarse` float32 [768], `manifest.json`; deterministic bytes; SHA-256 and size on the row, verified on read | `artifact.py`, design §3.8 | the authoritative source for index (re)builds and for verification inputs |
| Scylla `representations` PK `content_id` CK `spec_digest` (state, artifact URI/sha256/size, `segment_count`, `duration_s`, `created_at`, `retired_at`, follow-up flags `f_index`/`f_request`/`f_targets`) | `db.py` | per-representation metadata needed for consistency checks is present |
| `representations_by_spec` PK `(spec_digest, bucket)` CK `content_id`, 64 buckets, written by follow-up `f_index`; the reconciler completes missing follow-ups | `commit.py:175`, `reconcile.py:178`, `config.py` (`backfill.buckets = 64`) | the enumeration source for a full index build; a complete scan is 64 partition reads per spec |
| Targets: `targets` (state REGISTERING/READY/…/RETIRED), `targets_by_content`, `target_representations` PK `target_id` CK `version, spec_digest` (state PENDING/READY, `content_id`, `representation_id`, `target_representation_id`) | `db.py`, `targets.py`, design §10 | a target's vectors are the vectors of its reference content's representation; the binding gives `RepresentationId.for_target(...)` used by `MatchId` |
| `retire_guard (spec_digest, holder)` exists and is consulted by `spec retire`; nothing registers a holder yet | `db.py:238`, `specs.py:80` | P10 registers its indexed specs here (P9 exit §7 handoff) |
| Stream registry with held publication: `representation.ready` requires group `media-registry`; target events require nothing yet but are held | `publish.py` `SEED` | P9's handoff asks P10 to register a `matcher` group; see design §6.1 for why the design does not depend on it |
| Deletion: representations are kept on target retirement; spec retirement marks rows RETIRED and deletes objects after `retire_grace_s` (7 d) | design §11.4 | index cleanup is driven by spec retirement (guarded) and by target retirement |
| Redis only for coordination (`fp:` keys, leases, queue rebuilt from Scylla); `RedisClock` for all lease time | `coord.py`, design §4.3 | the matcher reuses the same lease/clock primitives |
| Typed errors: `StorageUnavailableError`, `LwtUnknownError`; reconciler pass errors are retried (X-10) | `db.py`, `errors.py`, `reconcile.py` | the matcher follows the same failure taxonomy |

No representation in this repository is stored anywhere except Scylla and
MinIO; there is no vector index of any kind today.

## 4. The existing matcher (V1 `matching/`)

### 4.1 Behaviour

`matching.matcher.match_segments(target_segments, candidate_segments, …)`
(`MATCHER_VERSION = "temporal_v1"`):

1. Optional coarse screen: cosine of the mean-pooled vectors ≥
   `coarse_similarity_threshold` (0.60), else not matched. The V1 worker
   (`worker/matching_handler.py:294`) always passes coarse vectors.
2. Dense cosine matrix candidate × target (float64, brute force, one pair of
   videos).
3. Hits = cells ≥ `segment_similarity_threshold` (0.90).
4. Dominant offset = the most frequent `target_index − candidate_index`
   among hits (segment-index units); keep hits within
   `offset_tolerance_segments` (1).
5. One best target segment per candidate segment; split into runs where the
   candidate index jumps by more than `max_index_gap` (2) or the target index
   goes backwards; keep the longest run.
6. Matched iff run length ≥ `min_matched_segments` (3) and mean similarity
   ≥ 0.90. `score` = mean similarity of the run ("not a calibrated
   probability").

It returns alignment (start/end on both sides, offset in seconds, matched
pairs). It is deterministic.

### 4.2 Threshold provenance

All five values are documented in `matching/config.py` and V1's
`phase-09-temporal-video-matching.md` §13 as **PROVISIONAL HEURISTIC / NOT
VALIDATED** (carried from a frame-level prototype's 0.93 and scaled down).
No labelled data existed when they were set.

### 4.3 Limitations found (verified)

| # | Limitation | Evidence |
|---|---|---|
| M-1 | Offset is an integer segment-index difference. Two copies of one film whose 5 s grids are out of phase compare frames up to 2.5 s apart; the matcher has no notion of time phase | §7.1: median phase distance 0.96 s; cosine falls from median 0.845 (< 0.5 s) to 0.504 (1.5–2.5 s) |
| M-2 | 0.90 segment threshold rejects most true correspondences of real re-encodes | §7.1: median per positive pair, 7 % of segments reach 0.90 |
| M-3 | The mean-similarity condition re-applies the same 0.90 threshold to the run | code, step 6 |
| M-4 | Coarse screen rejects 6 of 150 positives outright (coarse cosine < 0.60; min 0.392) | §7.4 |
| M-5 | No handling of low-information segments (black, title, logo, credits): identical frames occur in unrelated films | §7.2: negative pairs reach cosine 1.0 |
| M-6 | No scoring of evidence density or coverage; `score` is not a confidence | code |
| M-7 | One pair per call, O(N·M) per pair, no candidate generation: V1 compares a job's candidate against the one target the job names | `similarity.py` docstring, design-proposal job model |
| M-8 | VFR or speed-changed copies drift; a constant-offset model does not follow drift | code; SOAK-1000 has 24 VFR items (stretched timelines) |

`matching/aggregation.py` (V1 "Phase 10", multi-technique folding into the V1
`Result`) is unrelated to V2 P10 and is not reused; the name collision is
noted to avoid confusion.

### 4.4 Results and emission today

V1 writes a `work_queue` `Result` per job (Redis Streams, V1 contract). The
V2 package emits no match events. The shared contract already defines
`match.found` (below). crawler2 has **no consumer** of `match.found` yet
(P11/P12 own them, P9-D19).

## 5. `match.found` contract (crawler2 `antipiracy-contracts` 1.1.0, frozen package)

- Producer `Component.MATCHER` (service fingerprinter); consumers
  `FEEDBACK`, `EVIDENCE_COLLECTOR`; idempotency key `match.match_id`;
  ordering none; schema minor 0.
- `MatchResult` fields: `match_id`, `content_id`, `representation_id`,
  `target: TargetRef(target_id, version)`, `target_representation_id`,
  `technique: Technique(name, version)`, `verdict: confirmed|probable`,
  `confidence ∈ [0,1]`, `segments: tuple[AlignedSegment(media_offset_seconds,
  target_offset_seconds, duration_seconds)]`, `decided_at`.
- `match_id = MatchId.of(representation_id, target_representation_id,
  technique.name, technique.version)` (validated). **Any change of the
  verification rule or its parameters that can change a decision must change
  `technique.version`**, otherwise one `match_id` would name decisions made
  under different rules.
- "Non-matches are not events" (contract docstring).
- Models are `frozen`, `extra="ignore"`: additive optional fields are
  compatible for consumers.
- **Not in the contract:** ANN provenance (index, k, floor, candidate
  counts), spec digests (derivable through `representation.ready`, not from
  the id), completeness, and any media-equivalence event. P8's level-5
  relation has no contract representation.

## 6. Labelled data and baseline evidence

### 6.1 Corpora (all built in P9 from openly licensed films; outside git)

| Set | Provenance and licence | Content | Use in P10 |
|---|---|---|---|
| S-SRC | 7 Blender films; CC BY 3.0/2.5/4.0 (HERO weakest evidence) — P9 data-preparation §7 | 3,795.7 s | target references |
| L-PAIRS (`ab3d7711…`, csv `ff43168c…`) | derived from the normalized reel; labels = provenance only | 7 targets, 150 film excerpts (31–562 s; re-encode, 360p, 10 % crop, letterbox, pillarbox; H.264/HEVC/VP9), 30 synthetic clips; 480 pairs (150 +, 330 −) | precision/recall, thresholds |
| C-PREC (`af5826bc…`) | same | 63 clips, 14.26 h | not used here (precision corpus) |
| SOAK-1000 (`ed3bfd8d…`) | same; every item records its reel interval and films | 1,000 decodable items (136.8 h), 240p–1080p, 4 codecs, 3 containers, 24 VFR | historical-media corpus for ANN, growth, direction B |
| P8 different-encoding set | crawler2 `benchmarks/p8-media/`, ffmpeg `testsrc` + noise, 6 s, 320×240: H.264 200k/900k, MKV remux, fragmented MP4, VP8 WebM, HLS 1 s/2 s | synthetic | level-5 test — **too short** (see R-6) |
| `Blast.mp4` | local-only | — | not used |

Coverage gaps (carried from P9 exit §6 item 6): animated films only; no
live-action, no screen recordings or cam captures, no speed change other than
the 24 VFR items, no picture-in-picture, no mirroring, no overlays. All
derived material comes from one 3,796 s reel, so the corpus contains many
copies of few moments.

### 6.2 V1 baseline on L-PAIRS

| Baseline | Status | TP | FN | FP | TN | Precision | Recall |
|---|---|---|---|---|---|---|---|
| V1 matcher on V2 `dinov2-seg/2` FP32 vectors, no coarse screen (P9 oracle) | **measured** (P9 validation §7.4/§8.1), **reproduced** here from the cache | 37 | 113 | 0 | 330 | 1.000 | 0.247 |
| V1 matcher on V2 vectors, worker path (coarse 0.60 first) | **measured** here | 33 | 117 | 0 | 330 | 1.000 | 0.220 |
| V1 end to end (V1 `embedding/` sampling `fps=1/5`, HF image processor, V1 worker) | **NOT MEASURED** | — | — | — | — | — | — |

The reproduction matching P9 bit-for-bit (same counts, 6,220 segments)
validates that the cache holds the vectors P9 evaluated. Recall by transform
(oracle): re-encode 11/35, 360p 7/31, crop 7/28, letterbox 4/28, pillarbox
8/28. The M3 gate's comparison and its fragility (precision 1.0 leaves no
room for a single false positive on 330 negatives) are discussed in
validation.md §2.

## 7. Matcher diagnostics on L-PAIRS (measured, `results/lpairs-eval.json`)

### 7.1 True correspondences (labels by construction)

For each positive, clip time t maps to film time `t + clip.reel_start_s −
film.reel_start_s`; the nearest sampled target frame is compared (4,986
segment pairs):

| Quantity | p05 | p25 | p50 | p75 | p95 |
|---|---|---|---|---|---|
| Cosine to the true target segment | 0.151 | 0.432 | 0.714 | 0.851 | 0.942 |
| Time between the two sampled frames (s) | 0.04 | 0.43 | 0.96 | 1.80 | 2.29 |
| Best cosine to any target segment | 0.432 | 0.621 | 0.773 | 0.866 | 0.945 |

| Phase distance | n | p25 | p50 | p75 |
|---|---|---|---|---|
| < 0.5 s | 1,359 | 0.725 | 0.845 | 0.909 |
| 0.5–1.5 s | 2,098 | 0.438 | 0.707 | 0.833 |
| 1.5–2.5 s | 1,529 | 0.286 | 0.504 | 0.746 |

By transform, median cosine: re-encode 0.759, pillarbox 0.751, letterbox
0.709, 360p 0.706, crop 0.657. **Finding F-1:** the sampling phase costs more
similarity than any transform. It is a property of the P9 spec (one frame
per 5 s window); P10 cannot change it without a new spec and a P9 backfill
(P9 design §14), so the verifier must tolerate it (design §8).

### 7.2 Hard negatives

Per negative pair, the best cosine of any clip segment against the wrong
film: median 0.539, p90 **0.997**, max **1.000**. Per segment: median 0.307,
p99 1.000. **Finding F-2:** unrelated films share identical frames (black,
fades, title cards, studio logos). A similarity floor cannot provide
precision; a temporal run of several consecutive, offset-consistent,
*informative* segments must.

### 7.3 Threshold sensitivity of the V1 algorithm (exploratory, in-sample)

| Segment threshold | min run | TP | FP | Precision | Recall |
|---|---|---|---|---|---|
| 0.90 | 3 | 37 | 0 | 1.000 | 0.247 |
| 0.85 | 3 | 70 | 0 | 1.000 | 0.467 |
| 0.80 | 3 | 104 | 0 | 1.000 | 0.693 |
| 0.75 | 4 | 107 | 0 | 1.000 | 0.713 |
| 0.75 | 3 | 122 | 2 | 0.984 | 0.813 |
| 0.70 | 3 | 136 | 4 | 0.971 | 0.907 |

Computed on all 480 pairs, so this is **not** a tuned setting; it shows that
recall far above the V1 baseline is reachable without false positives by
re-fitting thresholds, and that precision degrades quickly below 0.75 — the
regime where the low-information handling of §4.3 M-5 matters. P10 tunes on
a held-out split (validation.md §3).

### 7.4 Coarse vectors

Coarse cosine, positives: min 0.392, p05 0.658, median 0.878; negatives:
median 0.480, p99 0.704, max 0.763. The distributions overlap and 6 positives
fall below 0.60. **Finding F-3:** the coarse vector is not usable as a
retrieval or rejection key for partial clips.

## 8. Vector volume (estimates; ingestion rate is not known)

### 8.1 Inputs

| Input | Value | Basis |
|---|---|---|
| Segments per minute | 12 | spec (5 s windows) |
| Segments per media item | mean 99 | **measured** on SOAK-1000 run 5 (98,983 / 1,000); the SOAK composition is P9's assumed crawl mix (60 % ≤ 3 min …), not observed crawl data |
| Media references observed | 2,462 `video_file` + 17 HLS in ≈ 46 h of M1 crawling (≈ 1,290/day) | **observed**, P7 audit §2.4; references, not distinct `ContentId`s; durations unknown |
| Encoder capacity | 1.117 s per minute of video (GPU end to end) ⇒ ≈ 77,000 min/day ⇒ ≈ 0.93 M segments/day per RTX 2050 host | **measured**, P9 G-6 |
| Targets | 7 in the labelled corpus; the future library size is unknown | — |

Distinct new contents per day, their durations and the target-library size
are **unknown**. The scenarios below are assumptions, labelled as such.

### 8.2 Media (historical) vectors, one spec

| Scenario | Assumption | Segments/day | 1 year | 3 years | Raw FP32, 1 y | Raw FP32, 3 y | Encoder load |
|---|---|---|---|---|---|---|---|
| Low | 200 new contents/day × 99 seg | 19,800 | 7.2 M | 21.7 M | 22 GB | 67 GB | 2 % of one GPU host |
| Base | 650/day (half of the observed reference rate distinct) × 99 seg | 64,350 | 23.5 M | 70.5 M | 72 GB | 217 GB | 7 % |
| High | 1,300/day × 600 seg (50 min average, feature-length piracy) | 780,000 | 284.7 M | 854 M | 875 GB | 2.6 TB | 84 % |

Raw FP32 = vectors × 768 × 4 B = vectors × **3,072 B**, before metadata,
alignment, index structures or replicas. The authoritative copy (MinIO
artifacts) grows by the same amount (≈ 3.1 KB per segment incl. timestamps)
whatever index is chosen. A second spec during a backfill doubles the
affected vectors until the old spec is retired.

### 8.3 Target vectors

| Scenario | Target versions | Vectors (100-min feature × 12/min) | Raw FP32 |
|---|---|---|---|
| Low | 50 | 60,000 | 0.18 GB |
| Base | 500 | 600,000 | 1.8 GB |
| High | 5,000 | 6,000,000 | 18 GB |

Index overhead and resident memory per technology are in §9.4; the budget
consequences are in design.md §13.

## 9. Index candidates (measured on real vectors, `results/ann-bench.json`)

Corpus M = SOAK-1000 (98,983) + L-PAIRS clips (5,460) = **104,443 vectors**,
1,180 items, 321 MB raw. Queries QB = the 760 segments of the 7 target films
(not in M). Exact oracle = FAISS `IndexFlatIP` (brute force, offline only).
Host: 12 cores, laptop, dev stack running; batch timings with 4 threads.

### 9.1 Recall and latency (k = 100 per query segment)

| Variant | Params | Build | Bytes/vector | Recall@100 vs exact | Single-query p50 / p99 | Media recall, policy k100/f0.6/h2 |
|---|---|---|---|---|---|---|
| exact (oracle) | — | 17 s (load) | 3,072 | 1.000 | 14.7 / 17.2 ms | 0.9665 |
| FAISS HNSW flat | M=32, efSearch 128 | 14.0 s | 3,344 | 0.987 | 0.16 / 0.38 ms | 0.9665 |
| FAISS HNSW-SQ8 | M=32, efSearch 128 | 10.3 s | 1,040 | 0.982 | 0.15 / 0.31 ms | 0.9669 |
| FAISS IVF-Flat | nlist 1024, nprobe 16 | 66.5 s | 3,110 | 0.980 | 0.33 / 0.58 ms | 0.9670 |
| FAISS IVF-PQ96 + exact re-rank of 400 | nprobe 32 | 83.4 s | 142 | 0.983 | 0.26 / 0.34 ms (+ re-rank) | 0.9675 |
| Qdrant HNSW f32 in RAM | m=32, ef 128 | 339 s (REST upload + index) | — | 0.965 | 1.91 / 2.51 ms (HTTP) | 0.9653 |
| Qdrant HNSW, int8 in RAM, originals on disk, rescore ×2 | m=32, ef 128 | 258 s | — | **0.992** | 1.98 / 2.68 ms | 0.9661 |
| Qdrant HNSW, PQ ×16 in RAM, originals on disk, rescore | m=32, ef 128 | 208 s | — | 0.982 | 2.35 / 3.22 ms | 0.9676 |

Recall@10 is ≈ 0.95–0.98 for every variant; with many near-identical copies
the exact top-10 contains ties, so set overlap at small k understates
quality. QA (2,000 clip segments vs SOAK) gives the same picture for FAISS
HNSW M=32: recall@100 0.909 / 0.963 / 0.969 at efSearch 64 / 128 / 256.

**Finding F-4:** at media level the ANN loss is negligible for every
candidate (0.965–0.968 vs exact 0.9665 for the same policy). The **candidate
policy**, not the ANN algorithm, decides recall (§10). Technology choice can
therefore be made on architecture, memory and operations.

### 9.2 Memory

| Variant | Measured | Note |
|---|---|---|
| FAISS HNSW flat | 3.3 KB/vector (serialized), RSS +359 MB for 104 k | in process |
| FAISS HNSW-SQ8 | 1.04 KB/vector | |
| FAISS IVF-PQ96 | 142 B/vector | needs the float32 vectors elsewhere for re-ranking |
| Qdrant f32 in RAM | container 497 MiB after indexing 104 k (idle 25 MiB) | ≈ 4.5 KB/vector all-in at this size |
| Qdrant int8 + on-disk originals | container 526 MiB under a 3 GiB limit (includes reclaimable mapped pages); see §9.3 for capped runs | |

### 9.3 Qdrant under a memory cap (measured)

| Cap | Result |
|---|---|
| 256 MiB | **OOM-killed during upload/indexing** of 104 k vectors (int8 config) |
| 512 MiB | completed; cgroup `memory.peak` = 536,875,008 B (**the cap itself**) during build; upload 148 s, upload + optimization 369 s (258 s uncapped); steady state 152–156 MiB (anon 44 MiB) ⇒ ≈ **1.3 KB resident per vector** after subtracting the 25 MiB idle footprint; recall@100 0.988, media recall 0.967; first 760 queries after the build p50 **558 ms** / p95 1.7 s (originals and graph faulted in from disk under memory pressure), then p50 2.0 ms; one p99 outlier 28.7 ms at ef 256 |

`results/qdrant-int8-256mib.json` was not written (the run died);
`results/qdrant-int8-512mib.json` holds the 512 MiB run.

**Finding F-5:** ingestion stages unoptimised segments (float32 + payload)
in memory before the optimizer builds HNSW and quantizes them; the peak
during a bulk (re)build is well above the steady state. A Qdrant memory limit
must be sized for build peaks, not for the quantized steady state (design
§13).

### 9.4 Not measured, and why

| Candidate | Status |
|---|---|
| Scylla vector search | **Not available**: the deployed Scylla is 6.2.3 (open source); it has no vector type or ANN index. Not testable here. |
| Milvus | Rejected without measurement: requires etcd + object storage + message queue components — far beyond the dev-host budget for no recall advantage (F-4). |
| pgvector | Rejected without measurement: introduces PostgreSQL, a second durable database the architecture does not have (plan decision 2026-09-28: one Scylla). |
| hnswlib / usearch | Not measured: same algorithm class as FAISS HNSW (F-4); same multi-host problem. |
| Qdrant gRPC upload, snapshots, distributed mode | Not measured (REST only). Upload throughput is therefore a lower bound. |

## 10. Candidate generation (measured with the exact oracle)

Direction B (new target → historical media) on M: per target segment top-k
with score ≥ floor, grouped by item, candidate if ≥ h distinct query segments
hit it. Relevant items per film (provenance, overlap ≥ 30 s): 90–376 (the
corpus has many copies of each moment).

| k | floor | h | Media recall mean / min | Candidates per target max | Irrelevant candidates (7 targets) |
|---|---|---|---|---|---|
| 10 | 0.6 | 2 | 0.307 / 0.172 | 115 | 25 |
| 32 | 0.6 | 2 | 0.703 / 0.559 | 271 | 69 |
| 100 | 0.6 | 2 | 0.967 / 0.934 | 406 | 249 |
| 100 | 0.7 | 3 | 0.906 / 0.787 | 352 | 28 |
| 256 | 0.7 | 2 | 0.993 / 0.985 | 460 | 496 |
| 1000 | 0.6 | 2 | 1.000 / 1.000 | 651 | 1,931 |

**Finding F-6:** with many near-identical copies, a fixed top-k saturates:
the k nearest neighbours of a target segment are all copies of the same
moment, so further copies are invisible to that segment. Exact similarity at
rank 100 has median 0.739; at rank 1,000 0.445.

### 10.1 Corpus growth (`results/ann-growth.json`)

The `growth` section inside `results/ann-bench.json` is **superseded**: that
run scored small-corpus recall against relevant items that were not in the
small index (a script defect, fixed before the `ann-growth.json` run, which
is the one reported here).

Small corpus = the 180 L-PAIRS clips (5,460 vectors; 21–22 relevant per
film); full = M (104,443; **19.1×**). Same queries.

| Policy | Small: recall / max candidates | Full: recall / max candidates | Full: truncated targets |
|---|---|---|---|
| k=100, f=0.6, h=2, cap 200 | 1.000 / 87 | 0.745 / 200 | 5 of 7 |
| k=100, f=0.6, h=2, cap 500 | 1.000 / 87 | 0.967 / 406 | 0 |
| adaptive k 32→512 (double while the k-th score ≥ floor), f=0.7, h=2 | 1.000 / 66 (mean k 37.6) | 0.9965 / 501 (mean k 242; 228 segments reached 512) | 0 |
| adaptive k 32→1000, f=0.7, h=2 | 1.000 / 66 | 0.9965 / 617 (135 segments reached 1,000) | 0 |

**Finding F-7:** a hard cap keeps per-query work bounded as the corpus grows,
but in direction B the number of *true* candidates grows with the number of
copies; a cap below that number silently loses recall (0.745). The bound must
be paired with saturation detection (k-th score still above the floor) and
an explicit, recorded incomplete outcome or continuation — design §7.

### 10.2 Within-item sparsification

Dropping a segment whose cosine to the previously kept segment of the same
item is ≥ τ keeps 95.9 % (τ 0.90), 98.5 % (0.95), 99.6 % (0.98) of vectors.
**Finding F-8:** at 5 s sampling consecutive segments are rarely
near-duplicates; within-item deduplication is not a useful memory lever.

## 11. Resource constraints (dev host)

| Item | Value | Source |
|---|---|---|
| Host | 12 cores, 15 GiB RAM (≈ 7.8 GiB available during this audit with the stack, desktop and browser running), 3 GiB swap, RTX 2050 4 GiB | `free`, plan decisions log |
| Stack limits | Scylla 2 GiB (`--memory 1400M --reserve-memory 512M`), Redis 1 GiB (`maxmemory 768mb`, noeviction), MinIO 512 MiB, app 512 MiB ⇒ ≈ 4 GiB | P0 design §4, compose |
| Stack usage observed | Scylla 446 MiB, Redis 398 MiB, MinIO 353 MiB, app 2 MiB | `docker stats` |
| GPU encoder | ≈ 1.3 GiB RSS (python + model) + ffmpeg 0.5 GiB at 1080p; a 4K source needed > 5 GiB in one P10 encode run (OOM-killed at a 5 GiB cap; succeeded at 8 GiB) | this audit |
| Disk | 457 GB, 292 GB free; MinIO and Scylla data on the dev disk; P9 recorded USB-HDD contention (exit §6 item 14) | `df`, P9 |
| Authoritative read rate | 1,000 artifacts (98,983 vectors, 306 MB) in 314 s single-stream from MinIO during GPU encoding ≈ **315 vectors/s** (lower bound) | `fetch_soak_vectors.py` |
| Qdrant ingest rate (REST, batches of 2,000, `wait=true`) | 104,443 points: upload 102–166 s, upload + optimization 208–339 s ⇒ **≈ 310–500 points/s** to "ready" (lower bound) | `ann_bench.py` |

## 12. Inventory: reuse, modify, new

| Component | Decision |
|---|---|
| `fp-seg-npz/v1` reader (`artifact.load`), SHA-256 verification, `Objects` | **reuse** as the index/verification source |
| `representations`, `representations_by_spec`, `target_representations`, `targets` | **reuse** read-only, unchanged |
| `retire_guard` | **reuse**: P10 registers holders |
| Lease/queue/clock primitives (`coord.py`), due-index and request-state patterns (`requests.py`), intents + reconciler + held publication (`commit.py`, `reconcile.py`, `publish.py`; ADR-026) | **reuse the patterns**; new P10 tables follow them |
| CLI framework, audit log, metrics registry | **reuse** |
| V1 `match_segments` | **keep frozen** as the baseline oracle `temporal_v1`; **not** the P10 verifier |
| V1 thresholds | **replace** by values fitted on a training split (decision P10-D7) |
| P9 commit follow-ups, P9 reconciler | **unchanged** — P10 discovers work with its own hint reader and backstop scans and runs its own reconciler (design §6.1, §12.6; correction C-4). A P9 follow-up flag (P10-D5 option (a)) would need separate approval |
| V1 `aggregation.py`, `worker/` | not used |
| Vector index, match tasks, match records, equivalence relation, index generations, verifier `temporal/v2`, `match.found` publication | **new** |

## 13. Defects, gaps and risks

| # | Item | Severity |
|---|---|---|
| R-1 | V1 thresholds give recall 0.25 on real re-encodes (F-1, M-1…M-3) | high — M3 can be met only by beating 0.247 *with zero false positives* |
| R-2 | Identical low-information frames across unrelated media (F-2) | high for precision and for candidate fan-out |
| R-3 | Fixed top-k saturation and silent truncation in direction B (F-6, F-7) | high for recall of "new target vs history" |
| R-4 | The index cannot hold production volumes on the 15 GB host; even the base target scenario (600 k vectors, 1.717 GiB raw FP32) is unproven under the 1.5 GiB limit (design §13, E-R1; this row originally said "≈ 1–3 M vectors", superseded by correction C-1) | high for production; dev capacity unresolved |
| R-5 | Rebuild throughput measured only as lower bounds (§11); at base 1-year volume a rebuild at the measured rates takes > 20 h (design §12.8) | medium |
| R-6 | P8's different-encoding set is 6 s long (2 segments) — no temporal verifier with a ≥ 3-segment run can link it; HLS items have no `ContentId` (P8-D6) and are never encoded | medium: the level-5 test needs a longer set (validation §8) |
| R-7 | Corpus homogeneity: one reel of animated films; hard negatives for live action, cam captures, mirrored or speed-changed copies are absent | medium: thresholds may not transfer |
| R-8 | Statistical power: 330 negatives — "precision ≥ 1.0" is a zero-false-positive requirement on a small set | medium (validation §2) |
| R-9 | A consumer group on `representation.ready` for the matcher would make the encoder's publication depend on matcher health (held publication / stream bytes) | medium (design §6.1) |
| R-10 | `match.found` cannot carry ANN provenance or completeness; no equivalence event exists | low for M3 (no consumer yet), needs a contract decision before P11/P12 |
| R-11 | New external service (if Qdrant): version pinning, upgrade and data-format compatibility, a new failure domain | medium |
| R-12 | VFR / speed-changed copies drift under a constant-offset model (M-8) | low–medium |

## 14. Classification

| Class | Items |
|---|---|
| Existing, verified | P9 identity, spec digests, artifact format and integrity, by-spec index, target binding, retire guard, leases/intents/held publication; V1 matcher behaviour; `match.found` contract; L-PAIRS labels; V1-oracle baseline (reproduced) |
| Existing with limitations | V1 matcher (M-1…M-8); P9 corpora (R-7); P8 equivalence set (R-6); dev-host resources (§11) |
| Missing | any vector index; candidate generation; match task lifecycle; match persistence; `match.found` publication; equivalence relation; index rebuild/check tooling; P10 metrics; V1 end-to-end baseline measurement |
| Proposed (design.md) | Qdrant-backed derived index with generations (active generation recorded in Scylla, correction C-3); two collections per spec; adaptive-k candidate generation with recorded saturation; verifier `temporal/v2`; durable match tasks; equivalence links; additive contract fields as a later decision |
| Unknown, needs measurement or decision | real ingestion rate and durations; target-library size; thresholds on a held-out split; low-information-segment rule; Qdrant gRPC ingest rate and snapshot behaviour; live-action transfer; P14 multi-host behaviour |

## 15. Questions resolved by measurement

1. *Does ANN lose matches?* Not measurably at media level (F-4).
2. *Which ANN technology recalls best?* All within ≈ 2 % at segment level;
   none materially better at media level.
3. *Is fixed top-k enough?* No (F-6, F-7); adaptive k with saturation
   reporting is.
4. *Why is V1 recall low?* Sampling phase and a 0.90 threshold (F-1, §7.3).
5. *Can the coarse vector shortlist?* No (F-3).
6. *Does within-item dedup shrink the index?* No (F-8).
7. *Does Qdrant fit a tiny memory cap during a bulk build?* No at 256 MiB (F-5).

## 16. Open questions (affect the design; listed in decisions.md)

- Q-1 Expected distinct contents/day, durations and target-library size
  (sizing of the index host; R-4).
- Q-2 Whether P10 docs and ADR-028 move into crawler2 (P10-D16).
- Q-3 Whether the M3 baseline is the V1 matcher on V2 vectors (measured) or V1
  end to end (not measured) — validation §2.
- Q-4 Whether an additive `match.found` 1.1 (provenance) and a
  `media.equivalence` event are wanted before P11/P12 (P10-D12, P10-D13).
- Q-5 Whether the level-5 test may use a new, longer P10 equivalence set built
  from S-SRC with P8's transforms (validation §8).
