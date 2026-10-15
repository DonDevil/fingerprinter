# P10 Design — vector index and matcher (proposal)

Status: **PROPOSED — awaiting operator design approval. Nothing is
implemented. No decision in this document is approved.** Decisions are
numbered P10-D1…P10-D18 and tracked in [decisions.md](decisions.md); the
evidence is in [audit.md](audit.md) (§ references below are to the audit
unless stated otherwise); tests and acceptance criteria are in
[validation.md](validation.md); the index architecture is recorded as
[ADR-028 (proposed)](ADR-028-vector-index.md).

Plan: crawler2 `docs/v2-phase-plan.md` §P10 (binding), invariants B.5 #3/#4.
P9 is complete and is not reopened: no P9 identity, spec, table, artifact or
event changes here (P10-D5 keeps P9 code unchanged).

---

## 1. Goals and non-goals

**Goals**

1. A derived, rebuildable vector index over P9 representations, per spec,
   reachable from every fingerprinter host.
2. Matching in both directions with ANN candidate reduction before temporal
   verification:
   A. new media → registered targets; B. new target → historical media
   (without re-encoding the media).
3. A temporal verifier that beats the V1 baseline on the labelled set with
   no false positives, with calibrated confidence and full provenance.
4. Bounded, observable verification work; explicit incomplete outcomes; no
   unbounded fallback.
5. Level-5 media equivalence (P8) as a reversible, provenance-carrying
   relation between different `ContentId`s.
6. `match.found` produced under the existing contract, idempotently.
7. A supported, tested index drop-and-rebuild procedure.

**Non-goals**

- No new representation spec, no change to segment duration, sampling,
  precision or identity (a denser sampling would address audit F-1 but is a
  P9-style spec change, out of scope; see §17).
- No FP16 (stays gated), no cross-spec matching.
- No feedback (P11), evidence (P12), analytics (P13).
- No real multi-machine validation (P14); P10 is designed for multi-host
  correctness and tested as simulated two-host only.
- No crawler2 or contract-package change in P10 unless P10-D12/D13 are
  approved separately.

## 2. Invariants (checked for every section)

| # | Invariant | How the design keeps it |
|---|---|---|
| I-1 | Authoritative data is durable; the index is derived | the index holds copies of vectors and ids only; every fact (match, task, equivalence, generation) is in Scylla; §5, §12 |
| I-2 | No all-to-all matching | every comparison is preceded by an ANN query with hard bounds; brute force exists only in offline tests; §7 |
| I-3 | Both directions | §6.2, §6.3 and the ordering argument of §6.4 |
| I-4 | Multi-host | one network-reachable index service; all coordination via Scylla LWT and Redis leases; no host-local authority; §4, §11 |
| I-5 | Idempotency | deterministic point ids, `match_id`, LWT-guarded task and record writes; §5, §11 |
| I-6 | Explicit failures | typed errors, states `INCOMPLETE`/`WAITING_INDEX`, metrics; no fallback; §11, §12 |
| I-7 | P9 identity stable | one index per spec digest; FP32/FP16 never mixed; §4.2 |
| I-8 | Contracts additive | `match.found` 1.0 used as is; extensions only by separate decision; §9 |
| I-9 | Level 3 ≠ level 5 | equivalence is a separate relation; ContentIds never merged; §10 |
| I-10 | Rebuild supported | generations + aliases + resumable builder + check; §12 |
| I-11 | Resource limits real | budgets in §13; Qdrant capped; build peaks measured (audit F-5) |
| I-12 | P14 owns real multi-machine | §16 |

## 3. Architecture

```
            Scylla (fingerprinter keyspace)                     MinIO
   P9 tables (read only) ─────────────┐                fingerprinter-representations
   representations, representations_by_spec,                (fp-seg-npz/v1, authoritative)
   targets, target_representations     │                         │
                                        ▼                         ▼
 representation.ready ──hint──►  matcher process (any host, N instances)  ──► Qdrant (index service)
 (group-less XREAD, §6.1)        ├─ scheduler: tasks from hints + backstop scan    collections per
                                 ├─ indexer: upsert/delete points                  (spec, role, generation)
                                 ├─ searcher: adaptive-k ANN, candidate rules      + alias per (spec, role)
                                 ├─ verifier: temporal/v2 (deterministic)
                                 └─ committer: matches, equivalence, intents
                                        │
            P10 tables: match_tasks, match_due, match_candidates, matches,
            matches_by_target, equivalence_links, index_generations,
            index_active, index_entries, match_intents, verifier_params
                                        │
                                        ▼ held publication (ADR-026 pattern)
                                 match.found (stream; held until a consumer registers)
```

Components (new modules in the `fingerprinter` package; the V1 top-level
`matching/` package is untouched):

| Module | Responsibility |
|---|---|
| `fingerprinter/vindex.py` | `VectorIndex` protocol + `QdrantIndex` adapter (collections, aliases, upsert/delete/search/count/scroll, health); typed errors |
| `fingerprinter/indexing.py` | generation model, incremental indexer, builder (full rebuild), checker |
| `fingerprinter/candidates.py` | adaptive-k retrieval, low-information filtering, grouping, ranking, caps |
| `fingerprinter/verify.py` | verifier `temporal/v2`, pure function of two `fp-seg-npz/v1` payloads and a registered parameter set |
| `fingerprinter/matching.py` | match tasks (state machine, leases, retries), commit of matches/equivalence, publication |
| `fingerprinter/cli.py` (extended) | `index …`, `match …`, `equivalence …`, `matcher run` |
| `benchmarks/p10-index/` | offline oracle, tuning and validation harnesses (not imported by the service) |

A **matcher** process is stateless apart from leases and an in-process LRU of
decoded artifacts (a cache: bounded, never authoritative). Any number may run
on any host; the GPU is not used.

## 4. Index technology and layout

### 4.1 Choice (P10-D1, ADR-028 proposed)

**Qdrant 1.15.1** (pinned image digest at implementation), run as a
fingerprinter-owned network service, is proposed as the index. FAISS
`IndexFlatIP` is kept only as the offline oracle.

Why (evidence in audit §9–§10):

- Retrieval quality is not the differentiator: every measured variant is
  within ≈ 2 % of exact at segment level and within 0.3 pp at media level
  (audit F-4). The candidate policy dominates recall.
- **Multi-host accessibility is a requirement**: an in-process FAISS index
  would be either one host's private structure (not shared) or a replica on
  every matcher host. Replicas need a global ordered change log, per-host
  watermarks and a read-your-writes protocol to keep the ordering guarantee
  of §6.4, and multiply memory by the host count. Qdrant gives one shared,
  immediately visible (`wait=true`) store reachable over the network, with
  collections, atomic alias switching, payload filters and deletes by
  filter, persistence (WAL), quantization and on-disk storage options, and a
  documented path to sharding/replication for P14.
- Measured fit: int8 quantization with on-disk originals and rescoring
  reached segment recall@100 of 0.992 at 2.0 ms p50 per HTTP query on 104 k
  real vectors (audit §9.1).
- Scylla vector search is unavailable on the deployed Scylla 6.2.3 (audit
  §9.4). Milvus and pgvector add infrastructure the architecture rejects.

Costs accepted (ADR-028 consequences): a new service and failure domain,
version pinning, a build-time memory peak (audit F-5), a REST ingest rate
measured only as a lower bound (≈ 310–500 points/s), and slow first
queries after a build when memory is tight (p50 558 ms for the first 760
queries under a 512 MiB cap, then 2 ms; audit §9.3) — the dev limit of §13
leaves headroom above the build peak for that reason.

### 4.2 Collections, aliases, compatibility (P10-D2)

- One **collection per (spec digest, role, generation)**:
  `fp_<spec12>_<role>_g<n>` where `spec12` = first 12 hex characters of the
  spec digest and `role ∈ {media, target}`. The full digest is stored on the
  generation row and on every point; the builder refuses a 12-hex collision
  with a different full digest.
- One **alias per (spec, role)**, `fp_<spec12>_<role>`, pointing at the active
  generation. Searches always go through the alias; promotion is an atomic
  alias switch (§12.3).
- **Specification compatibility is enforced by construction**: a
  representation is indexed only into its own spec's collections, and a
  query uses only the collections of the query representation's spec. No
  compatibility rule between specs exists; in particular `dinov2-seg/1` and
  `/2` (identical vectors for complete media, P9 §3.2a) and any FP16 spec are
  **not** interchangeable. Adding such a rule would need its own decision.
- **Roles.** `media`: every READY content representation of the spec,
  including target reference contents (they are contents too). `target`: the
  segments of every READY target binding (`target_representations`) whose
  target version is not RETIRED, keyed by the target representation.
- Distance `Dot` (vectors are L2-normalized by the spec; the indexer rejects
  a vector whose norm deviates from 1 by more than 1e-4 with
  `InvalidVectorError`, measured norms are 1.0).
- Media role: HNSW `m=32, ef_construct=200`, int8 scalar quantization in RAM,
  float32 originals on disk, search with `rescore=true, oversampling=2`.
  Target role: float32 in RAM (small, latency-critical). Values are the
  measured configuration; tuning is allowed within the generation's
  recorded parameters (§12.1).

### 4.3 Points (P10-D4)

| Role | Point id (UUID v5, namespace `antipiracy/fingerprinter/index/v1`) | Payload |
|---|---|---|
| media | `uuid5(representation_id + ":" + segment_index)` | `rep`, `cid` (content id), `spec` (full digest), `k` (segment index), `t` (pts_s), `lowinfo` (bool, §7.3) |
| target | `uuid5(target_representation_id + ":" + segment_index)` | `trep`, `tid`, `tver`, `rep` (content representation), `cid`, `spec`, `k`, `t`, `lowinfo` |

Deterministic ids make every upsert idempotent across hosts and retries
(duplicate events, concurrent indexers, rebuild + live writes all converge).
Index ids never appear in Scylla facts or events; they are recomputed from
domain ids. Payload indexes: `rep`, `cid`, `tid` (keyword).

## 5. Data model (new tables, fingerprinter keyspace, no TTL)

| Table | Key | Content |
|---|---|---|
| `index_generations` | PK `(spec_digest, role)`, CK `generation` | state `BUILDING`/`ACTIVE`/`DRAINING`/`DROPPED`/`FAILED`; collection; params JSON + digest (Qdrant version, HNSW/quantization params); `snapshot_at`; per-bucket progress `map<int,text>`; expected/actual point counts; timestamps; audit |
| `index_active` | PK `(spec_digest, role)` | active generation, `epoch` (LWT) |
| `index_entries` | PK `(spec_digest, role, bucket)`, CK `subject_id` | which subjects (representation or target representation) are indexed, `segment_count`, `generations set<int>`, `indexed_at`; used by check and repair |
| `match_tasks` | PK `subject_id`, CK `(spec_digest, kind)` | `kind ∈ {media_vs_targets, target_vs_media, media_vs_media}`; state machine §11.1; lease fields, attempt, failures, `next_attempt_at`; index generation used; ANN statistics JSON; candidate counts; `incomplete_reason`; timestamps |
| `match_due` | PK `(due_day, shard)`, CK `(due_at, subject_id, spec_digest, kind)` | discoverability index of non-terminal tasks (P9 `encode_due` pattern) |
| `match_candidates` | PK `(subject_id, spec_digest, kind, generation)`, CK `rank` | durable candidate list of a task (bounded by its cap), with per-candidate state for paginated verification |
| `matches` | PK `content_id`, CK `(target_id, target_version, technique_version)` | `match_id`, representation ids, verdict, confidence, alignment JSON, verifier param digest, provenance JSON (§9.2), `decided_at`, stored envelope, `event_id`, `published_at` |
| `matches_by_target` | PK `(target_id, target_version)`, CK `(content_id, technique_version)` | reverse lookup |
| `equivalence_links` | PK `content_id`, CK `(other_content_id, technique_version)` | state `LINKED`/`REVOKED`, confidence, evidence JSON, provenance, `decided_at`, `revoked_at`, audit; written in both orientations |
| `match_intents` | PK `(day, shard)`, CK `intent_id` | durable intents for match/equivalence commits and publication (ADR-026 pattern), resolved by the P10 reconciler pass |
| `verifier_params` | PK `technique_version` | immutable parameter document + digest + calibration evidence reference |

Bucketing uses the P9 `shard_of(content_id, 64)` function. Partition sizes
are bounded: `match_candidates` by the task cap (§7.5), `matches_by_target`
by the true matches of one target version (bucketed by month if a target
exceeds 10⁵ matches — not expected before P14).

Authoritative sources for every derived thing:

| Derived | Rebuilt from |
|---|---|
| media collection | `representations_by_spec(spec, *)` → `representations` (READY) → artifact (SHA-256 verified) |
| target collection | `target_representations` (READY, target not RETIRED) → bound content representation → artifact |
| `lowinfo` payload flags | recomputed by the rule of §7.3 from the target collection |
| `index_entries` | rewritten by the builder |
| Redis keys (`fp:` leases, `fp:q:match`) | `match_due` |

## 6. Work model and both matching directions

### 6.1 Triggers (P10-D5)

Authoritative trigger = **Scylla state**; event streams are only hints.

- **Hint:** the matcher reads `representation.ready` (and
  `target.registered` once published) **without a consumer group**
  (`XREAD` from its own cursor, as P9's intake does, ADR-025). A hint upserts
  a `match_tasks` row (`IF NOT EXISTS`). A group-less reader cannot hold
  stream entries, so a stopped matcher never blocks P9's publication or
  stream trimming (audit R-9); a missed hint is only a delay.
- **Backstop:** the reconciler's P10 pass enumerates
  `representations_by_spec` (indexed specs only) and `target_representations`
  and creates any missing task; it also covers representations that existed
  before P10 was deployed (initial catch-up, rate-limited). Interval: daily
  by default plus on demand (`match reconcile`).
- P9 code is not changed. *Alternative (a), not recommended:* an additive P9
  commit follow-up `f_match` that writes the task in the commit path (lower
  latency, touches P9 modules). *Alternative (b), not recommended:* the
  `matcher` consumer group suggested in P9's handoff (couples encoder
  publication to matcher health).

Every READY representation of an indexed spec gets `media_vs_targets` and
(if equivalence is enabled) `media_vs_media` tasks; every READY target
binding gets one `target_vs_media` task. Tasks are claimed with the P9 lease
pattern (Redis ZSET rebuilt from `match_due`, per-task Redis lease, Scylla
LWT on `epoch` for state transitions, Redis clock).

### 6.2 Direction A — new media against registered targets

1. **Index own vectors**: upsert the representation's segments into the media
   collection(s) of its spec (`wait=true`), record `index_entries`.
2. **Search targets**: every informative segment (§7.3) queries the target
   alias of the same spec with adaptive k (§7.1).
3. **Candidates**: hits ≥ floor grouped by target version; candidate iff ≥
   `min_hits` distinct query segments; ranked by (distinct hits, best score);
   capped at `cap_A` (§7.5).
4. **Verify** each candidate: load the target representation's artifact
   (verified SHA-256), run `temporal/v2` (§8).
5. **Commit** matches (§9) and finish the task (DONE or INCOMPLETE).

### 6.3 Direction B — new target against historical media

1. **Index the target**: upsert target segments into the target collection;
   recompute `lowinfo` flags (§7.3).
2. **Search media**: every informative target segment queries the media alias
   with adaptive k (k₀ → k_max, §7.1); saturation is recorded per segment.
3. **Candidates**: grouped by `content_id`, ≥ `min_hits`, ranked, written to
   `match_candidates` (≤ `cap_B_total`); the target's own reference content is
   excluded.
4. **Verify in pages** of `page_B` candidates; the task's cursor survives
   restarts; each page is one lease-bounded unit of work.
5. **Commit** each page's matches; the task ends DONE, or INCOMPLETE with
   reason `candidate_cap` / `saturated_segments` when bounds were hit (§7.5).

No media is re-encoded: direction B reads stored representations only (the
validation test asserts zero new encode attempts, validation §7).

### 6.4 Why no pair is missed by ordering

Each task indexes its own subject **before** it searches the opposite role,
and Qdrant applies an acknowledged `wait=true` upsert before answering later
searches. For a media M and a target T of the same spec: if M's search ran
before T was indexed, then T's indexing — and therefore T's later search —
happened after M was indexed, so T's search sees M. Either way at least one
of the two searches can retrieve the pair. The guarantee is about
*visibility*; whether ANN retrieves the pair is measured (validation §4).
Across index generations, live writes go to every non-dropped generation
(§12.3), so a promotion cannot lose a subject indexed during a build.

### 6.5 Spec coverage

A media representation and a target binding are comparable only under the
same spec. Pairs whose subjects have no common indexed spec are not compared;
the count of such subjects is exposed (`match status`,
`fingerprinter_match_spec_uncovered`) — they are resolved by a P9 backfill,
never by cross-spec comparison. P10 indexes `dinov2-seg/2` first and
registers `retire_guard(spec, "p10-index")` for each indexed spec.

## 7. Candidate generation and bounds (P10-D6)

### 7.1 Adaptive k

For each informative query segment: search with `k₀`; while the k-th score is
still ≥ `floor` and `k < k_max`, double k and search again. A segment that
ends at `k_max` with the k-th score ≥ floor is **saturated** — more
neighbours above the floor exist than were retrieved. Saturation is counted
per task and, if any segment saturates, the task's outcome records
`saturated_segments = n` (audit F-6, F-7: fixed k loses recall silently as
copies accumulate; adaptive k reached media recall 0.9965 at 19× growth vs
0.745 for k=100 with a 200 cap).

### 7.2 Initial values (provisional; final values from the tuning split, validation §3)

| Parameter | A (media → targets) | B (target → media) | Media ↔ media | Basis |
|---|---|---|---|---|
| `k₀` | 32 | 32 | 32 | audit §10.1 |
| `k_max` | 256 | 512 | 512 | B: 228 of 760 target segments saturated at 512 on the 19× corpus, recall 0.9965 |
| `floor` (cosine) | 0.60 | 0.70 | 0.70 | true-correspondence cosine p25 0.43, p50 0.71 (audit §7.1): the floor bounds work, precision comes from verification |
| `min_hits` (distinct query segments) | 2 | 2 | 2 | audit §10 |
| `cap_A` (target versions per media) | 50 | — | — | targets are distinct works; true matches per media are few |
| `page_B` | — | 200 | 200 | one lease-bounded verification step |
| `cap_B_total` (candidates per task) | — | 5,000 | 2,000 | beyond: INCOMPLETE `candidate_cap` |

### 7.3 Low-information segments (P10-D8)

Identical black, fade, logo and credit frames occur in unrelated works (audit
F-2). Rule `lowinfo/v1` (proposed, to be measured in increment I-1):
a **target** segment is low-information if it has cosine ≥ 0.95 with a
segment of a *different* target work (targets are distinct works by
definition, so such a vector does not identify a work). A **media** segment
is low-information if its nearest target-collection neighbour is a
low-information segment with cosine ≥ 0.95. Low-information segments do not
search, do not vote, and do not count toward verification runs (they may sit
inside a run without breaking it). The flag is derived (payload), recomputed
on target registration and by the builder. If measurement shows the rule
insufficient, an explicit stop-vector set curated by the operator is the
fallback design (recorded, audited, versioned).

### 7.4 Deduplication

Hits are deduplicated per (query segment, subject); candidates per subject
(target version or content). Media with the same `ContentId` are one subject
by construction (P8 level 3). Different `ContentId`s already linked as
level-5 equivalent are **still verified separately** (equivalence is not used
to skip verification in P10; it may become an optimization later only with
its own decision).

### 7.5 Bounds and what happens at a bound

| Bound | Effect when reached | Recorded as |
|---|---|---|
| `k_max` per segment | further neighbours not retrieved | `saturated_segments` on the task, metric |
| `cap_A` | lowest-ranked targets not verified | task INCOMPLETE `candidate_cap`, the truncated count, the rank threshold |
| `page_B` | next page in a new lease | progress cursor (not incomplete) |
| `cap_B_total` | candidates beyond rank `cap_B_total` not stored | task INCOMPLETE `candidate_cap`; `match retry --raise-cap` re-runs with a higher cap (audited) |
| verification timeout per candidate | candidate marked FAILED (retryable ×3) | per-candidate state; task INCOMPLETE `verification_failed` if retries exhausted |
| matcher concurrency | at most `matcher.workers` verifications per process | — |

Nothing is discarded without a recorded count and reason. An INCOMPLETE task
is not a "no match"; it is visible in `match status`, metrics and the
validation reports. There is **no** fallback to a full scan.

Verification cost is bounded per task: ≤ `cap × C_v(max duration)`, where
`C_v` is one dense similarity of two representations (≤ 1,440 × 1,440 × 768
FLOP ≈ 1.6 GFLOP for two 2-hour items, < 1 s on one core); measured in
validation §9.

## 8. Temporal verification `temporal/v2` (P10-D7)

ANN only proposes candidates; `temporal/v2` decides. It keeps V1's proven
structure (dense similarity of the two representations, one dominant
temporal offset, ordered runs, a minimum run length — the mechanism that
makes the zero-false-positive baseline possible) and changes what the audit
shows to be wrong:

| V1 `temporal_v1` | `temporal/v2` | Why |
|---|---|---|
| offset in segment-index units, tolerance 1 index | offset in **seconds** from `pts_s`; a media segment at time t is compared with the target segments whose sampled frames lie within ±`d` of `t + Δ` (the two neighbours in phase) and the max is used | sampling phase is the main loss (audit F-1) |
| single dominant offset | up to `n_offsets` (3) offset modes from the ANN hit votes, each verified, best kept | repeated scenes, intros |
| 0.90 segment and mean thresholds | `s_seg`, `s_mean`, `L` fitted on the training split | audit §7.3 |
| all segments count | low-information segments neither count nor break a run | audit F-2 |
| `score` = mean similarity | calibrated `confidence` (§9.1) | contract semantics |
| one run reported | every run ≥ `L` reported as an `AlignedSegment` (max 32) | evidence |

Deterministic: same inputs, same parameter set ⇒ same output, on any host
(float32 matrix products are computed in float64 as in V1 to avoid
order-dependent rounding; ties broken by indices). Drift (VFR/speed change,
audit M-8) remains a documented limitation; it is measured on the 24 VFR
SOAK items, not designed for.

V1 `match_segments` stays frozen as `temporal_v1` for baseline runs only.

## 9. Match decision, confidence, provenance, `match.found`

### 9.1 Verdict and confidence (P10-D9)

- `confidence` = **calibrated estimated precision**: an isotonic map from the
  verifier's score (run length, mean similarity, coverage) to the precision
  observed at or above that score on the calibration fold of the labelled
  data. It is therefore "the fraction of decisions at least this strong that
  were correct on the calibration set", not a probability for one decision
  in the wild; the calibration set, its size and the corpus limitations are
  recorded on the `verifier_params` row.
- `verdict = confirmed` at or above the confirmation operating point (zero
  false positives on the tuning data); `probable` between that and a looser
  floor. **Only `confirmed` is published** by default; `probable` decisions
  are stored (`matches`, verdict probable) and published only if
  `matching.publish_probable` is enabled (contract: "needs review before
  evidence").

### 9.2 Technique and provenance (P10-D10)

- `technique.name = "dinov2-seg-temporal"`, `technique.version = "2.<n>"`,
  where `<n>` names an immutable row in `verifier_params`. Any change that can
  change a decision (thresholds, rules, `lowinfo` version, calibration
  affecting the verdict) creates a new version, hence a new `match_id` — old
  decisions stay valid under their own version (contract rule, audit §5).
- Stored provenance per match (`matches.provenance`): spec digest(s), both
  representation ids, target representation id, verifier param digest,
  index generation and its params digest, Qdrant version, ANN parameters
  (k₀, k_max, floor, min_hits, caps, ef, quantization/rescore), similarity
  metric, query and candidate counts, rank of this candidate, saturation and
  truncation counts of the task, `complete` flag, verification duration,
  matcher instance, timestamps.

### 9.3 Persistence and publication (P10-D11)

1. Write intent `match_commit(match_id, …)` (`match_intents`).
2. `INSERT … IF NOT EXISTS` into `matches` (LWT); a duplicate decision with
   the same `match_id` is a no-op (same inputs ⇒ same decision; a different
   stored decision for the same id is an invariant violation and is
   reported, never overwritten). Then `matches_by_target`.
3. Build the `match.found` 1.0 envelope (producer `fingerprinter/matcher`),
   store it on the row, publish through the P9 publisher with **held
   publication**: stream `match.found` is registered with
   `hold_until_registered = true` and no required group until P11/P12
   register theirs. Held events stay in Scylla; `publisher republish`
   releases them.
4. Delete the intent. The P10 reconciler pass resolves stale intents.

`match.found` carries: `content_id`, `representation_id`, `target`,
`target_representation_id`, `technique`, `verdict`, `confidence`, `segments`
(aligned runs), `decided_at`. ANN provenance and completeness are **not** in
the 1.0 schema; they live in Scylla and `match show`. An additive
`match.found` 1.1 with an optional `provenance` object is proposed as a
separate crawler2 contract change (P10-D12, deferred). Non-matches are never
events. An INCOMPLETE search is never expressed as an event in 1.0.

### 9.4 Re-evaluation

A new technique version or index generation does not re-run old tasks
automatically. `match rerun --technique 2.<n> [--since …]` creates new tasks
explicitly (rate-limited, audited); old matches remain under their version.

## 10. Level-5 media equivalence (P10-D13)

- **Path:** `media_vs_media` tasks query the media alias of the same spec
  with adaptive k (§7), exclude the subject's own content, group by content,
  verify with `temporal/v2` and then apply the **equivalence rule**
  `equiv/v1`: one run covering ≥ 90 % of the informative segments of
  *both* items, absolute duration difference ≤ max(5 s, 2 %), and
  |offset| ≤ 10 s (container/packaging start differences). Partial overlap
  (a clip of a film) is a match-like relation, not equivalence, and is not
  recorded in P10.
- **Persistence:** `equivalence_links` rows in both orientations with state
  `LINKED`, confidence (calibrated on the equivalence set), technique version,
  evidence (alignment, coverage), provenance, `decided_at`, via an intent.
- **Reversible:** `equivalence revoke <a> <b> --reason …` sets `REVOKED` with
  an audit entry; nothing is deleted; a later technique version creates its
  own rows. **No `ContentId`, representation, media or observation is ever
  merged** (level 3 stays byte identity; P8 invariant).
- **Visibility to crawler2:** no contract event exists. P10 exposes the
  relation through the CLI and the table; a `media.equivalence_linked` event
  is a separate contract decision (P10-D13 part b, deferred).
- Equivalence can be disabled (`matching.equivalence_enabled`) without
  affecting A/B matching.

## 11. Concurrency, leases, idempotency, failures

### 11.1 Task state machine

`PENDING → INDEXED → SEARCHED → VERIFYING(page) → DONE | INCOMPLETE`;
`→ WAITING_INDEX` when the needed alias is missing or unhealthy (re-tried
with backoff, not failed); `→ FAILED` only for non-retryable causes
(artifact SHA-256 mismatch, invalid vector, missing representation row),
each with a typed reason. Every transition is an LWT on `epoch`; a lost lease
returns the task to the due index (P9 §7 pattern). States are resumable at
each boundary: re-indexing is idempotent (deterministic ids), the candidate
list is durable, verification pages are idempotent (`match_id`).

### 11.2 Failure semantics

| Failure | Detection | Behaviour |
|---|---|---|
| Qdrant unreachable / timeout | `IndexUnavailableError` | task → `WAITING_INDEX`, backoff 30 s → 15 min; metric `fingerprinter_index_unavailable_total`; health endpoint reports degraded; no fallback |
| alias missing (index lost) | `IndexMissingError` | as above; operator runs `index rebuild` (§12) |
| Qdrant collection RED / point-count mismatch | `index check` | generation marked suspect; rebuild recommended; queries continue on the alias unless the operator deactivates it |
| stale entry (point whose representation is retired/absent) | candidate validation against Scylla before verification | dropped from the candidate list, counted `index_stale`, repaired by deleting the subject's points |
| missing entry (READY representation without points) | `index check` / `index_entries` | re-indexed; counted `index_miss` |
| Scylla unavailable | `StorageUnavailableError`, `LwtUnknownError` | retry with backoff; LWT unknown resolved by SERIAL read (P9 pattern) |
| MinIO unavailable / artifact missing | typed error from `Objects` | candidate retryable (×3) → `verification_failed`; artifact SHA-256 mismatch → FAILED + operator alert (authoritative data corrupt) |
| verification timeout | per-candidate wall-clock limit (default 30 s) | candidate retry ×3 → `verification_failed` (task INCOMPLETE) |
| matcher crash | lease expiry | task resumes at its last durable state |
| duplicate hints / events / tasks | `IF NOT EXISTS`, deterministic ids | no-ops |
| concurrent indexers | idempotent upserts | converge |
| overload | bounded `matcher.workers`, due-index backpressure | latency rises; nothing dropped |

## 12. Index lifecycle: update, rebuild, check (P10-D2, I-10)

### 12.1 Generation parameters

A generation's params (Qdrant version, distance, HNSW `m`/`ef_construct`,
quantization, on-disk flags, payload schema version, `lowinfo` version) are
written to `index_generations` before the collection is created and are
immutable; their digest appears in match provenance.

### 12.2 Incremental updates

- New representation / binding → upsert (task step 1).
- Target retired → delete points by `trep` filter in every non-dropped target
  generation; tasks for the target stop; matches stay.
- Representation retired (spec retirement) → `spec retire` is refused while
  `retire_guard(spec, "p10-index")` exists; the operator first runs
  `index drop --spec …` (all generations), which deletes the collections,
  then removes the guard. Single-representation deletions follow P9 (none
  today); the checker removes points of non-READY representations.

### 12.3 Rebuild while serving

1. `index rebuild --spec S --role R`: LWT-create generation g+1 `BUILDING`,
   create its collection, record `snapshot_at` (Redis time).
2. From now on every live upsert/delete for (S, R) is applied to **all
   generations not DROPPED** (active and building).
3. The builder enumerates `representations_by_spec(S, b)` for b = 0…63 (or
   `target_representations`), skips non-READY rows, reads artifacts in
   parallel (`index.build_readers`, default 4), verifies SHA-256, upserts in
   batches, and records the per-bucket cursor after each bucket — an
   interrupted build resumes from the last committed bucket.
4. Catch-up: re-enumerate the bucket keys and index any subject not present
   in `index_entries` for g+1 (covers commits racing the scan).
5. Check (§12.5) on g+1. On failure: `FAILED`, collection kept for
   inspection, active generation untouched.
6. Promote: LWT on `index_active` (epoch) **then** alias switch in one Qdrant
   `update_aliases` call; old generation → `DRAINING` (still dual-written)
   for `index.drain_grace_s` (1 h), then `DROPPED` and deleted.

Queries during a rebuild use the active alias. If there is no active
generation (total loss), tasks wait in `WAITING_INDEX`; the target role is
rebuilt first (minutes at base volume) so direction A resumes before B.

### 12.4 Loss of the complete index

Delete the Qdrant volume (or lose the host) ⇒ aliases and collections gone ⇒
tasks wait ⇒ `index rebuild` for each (spec, role) from Scylla + MinIO ⇒
promotion ⇒ waiting tasks resume. No match, task, equivalence or
representation fact is lost, because none is stored only in Qdrant.

### 12.5 Consistency check (`index check`)

Per (spec, role, generation): expected points = Σ `segment_count` of READY
subjects; actual = Qdrant count; per-bucket diff of `index_entries` vs
Scylla; random sample of `index.check_sample` subjects: retrieve points by id
and compare vectors bit-for-bit with the artifact (int8 storage keeps the
originals, so exact comparison is possible); stale and missing lists;
result written to the generation row and `--json`.

### 12.6 Rebuild duration (extrapolated from measured lower bounds)

Measured: 315 vectors/s single-stream artifact reads (contended, HDD-backed
MinIO) and 310–500 points/s Qdrant REST ingest to "ready" (audit §11). With
r ≈ 400 vectors/s:

| Volume (media role) | Vectors | Duration at 400/s | At 3,000/s (target after gRPC + parallel readers; to be measured) |
|---|---|---|---|
| SOAK-scale (measured corpus) | 0.1 M | 4.4 min (measured 3.5–5.6 min) | — |
| Low 1 y | 7.2 M | 5.0 h | 40 min |
| Base 1 y | 23.5 M | 16.3 h | 2.2 h |
| Base 3 y | 70.5 M | 49 h | 6.5 h |
| Targets, base | 0.6 M | 25 min | 3.3 min |

Confidence: low — the two rates were measured once each, under contention,
with REST. Increment I-2 measures gRPC ingest and parallel reads (validation
§9). Matching continues during a rebuild, so its duration is an availability
risk only after total loss.

## 13. Resource budgets (P10-D14)

### 13.1 Development host (15 GiB, shared)

| Consumer | Budget |
|---|---|
| crawler2 stack (Scylla 2 GiB, Redis 1 GiB, MinIO 512 MiB, app 512 MiB) | ≈ 4 GiB (P0) |
| P9 GPU encoder (+ ffmpeg) | ≈ 2–3 GiB typical; > 5 GiB observed for a 4K source |
| **Qdrant** | `mem_limit` **1.5 GiB**, 2 CPUs (build peak sized; audit F-5: OOM at 256 MiB, peak = cap at 512 MiB for 104 k) |
| **matcher** (each) | RSS ≤ 1 GiB (`matcher.max_rss_bytes`), artifact LRU 256 MiB, 2 verification workers |
| Desktop, browsers, tools | the rest (≈ 5 GiB) |

Index capacity inside 1.5 GiB (media role, int8 + on-disk originals; estimate
≈ 1.3 KB resident per vector, measured at 104 k vectors under a 512 MiB cap, audit §9.3):
≈ 1 M media vectors, i.e. ≈ 3–6 weeks of the base scenario. The dev host is
a development environment, not the index host for production volume.

### 13.2 Production direction (input to P14, not validated in P10)

| Media vectors | int8 + on-disk originals (≈ 1.3 KB resident, measured at 104 k) | PQ ×16 (≈ 0.5 KB) | on-disk HNSW + PQ ×32 (≈ 0.2 KB) | Disk (originals + graph) |
|---|---|---|---|---|
| 7.2 M (low 1 y) | 9.4 GB | 3.6 GB | 1.4 GB | ≈ 25 GB |
| 23.5 M (base 1 y) | 31 GB | 12 GB | 4.7 GB | ≈ 80 GB |
| 70.5 M (base 3 y) | 92 GB | 35 GB | 14 GB | ≈ 240 GB |
| 285 M (high 1 y) | 370 GB | 143 GB | 57 GB | ≈ 1 TB |

The int8 figure is measured at 104 k vectors and extrapolated linearly; the
other per-vector figures are estimates
from the data structure sizes (int8 768 B, PQ ×16 192 B, graph links ≈ 128–256
B, ids/payload index ≈ 100 B). Consequences: beyond ≈ 1 M media vectors the
index moves to a dedicated host (or Qdrant distributed mode) with SSD; PQ
and on-disk HNSW trade latency for memory and must be re-measured for recall
before use; the target role stays in RAM. Moving the index to its own host
changes only the `index.url` configuration: the protocol is network-based
from the start.

## 14. Metrics and operational controls

Metrics (`fingerprinter_` prefix, P9 registry):

| Metric | Labels |
|---|---|
| `match_tasks` (gauge) | kind, state |
| `match_queries_total` | kind, outcome (`ok`, `index_unavailable`, `index_missing`) |
| `match_query_seconds` (histogram) | kind |
| `match_ann_hits_total`, `match_hits_above_floor_total` | kind |
| `match_lowinfo_segments_total` | kind |
| `match_candidates_total` | kind, stage (`grouped`, `dedup_removed`, `below_min_hits`, `stored`, `verified`, `truncated`) |
| `match_saturated_segments_total` | kind |
| `match_verify_seconds` (histogram), `match_verify_failures_total` | kind, reason |
| `match_decisions_total` | kind, verdict (`confirmed`, `probable`, `no_match`) |
| `match_incomplete_total` | kind, reason |
| `match_spec_uncovered` (gauge) | role |
| `index_points` (gauge) | spec, role, generation |
| `index_upsert_seconds`, `index_upsert_points_total` | role |
| `index_stale_total`, `index_miss_total`, `index_unavailable_total` | role |
| `index_build_progress` (gauge, buckets done) | spec, role, generation |
| `equivalence_links_total` | state |

CLI (all state changes audited):
`index status|build|rebuild|resume|check [--sample N]|activate|drop`;
`match status|show <content|target>|retry [--raise-cap]|rerun|reconcile`;
`equivalence show|revoke`; service role `matcher run`; the reconciler gains a
P10 pass (intents, backstop scan, periodic `index check` at sample size).

Structured logs: one JSON line per task transition and per incomplete or
failed outcome, with task id, kind, spec, generation and counts.

## 15. Security and data handling

- Qdrant listens on the backend network/loopback only, with an API key
  (`FINGERPRINTER_INDEX__API_KEY`); no public port; no snapshots exported off
  the host.
- Vectors are derived from possibly copyrighted media; they are internal
  data. Points carry ids and timestamps only — no URLs, titles or page data.
- `Blast.mp4` and other local-only material are never indexed into shared or
  exported stores.
- The matcher does no network fetches of media (only MinIO/Scylla/Redis/
  Qdrant).

## 16. Multi-host behaviour

Any matcher on any host: shared Scylla state, Redis leases, one Qdrant
endpoint. Correctness does not depend on host-local state (the LRU cache is
validated by artifact SHA-256). Simulated two-host tests in P10 (two matcher
processes, one Qdrant, one stack), including concurrent indexing of the same
subject and lease takeover. Qdrant replication/sharding, network partitions
between hosts and the index, and real machines are **P14**.

## 17. Known limitations and deferred work

- Sampling phase (audit F-1) is reduced by the verifier, not removed; a
  denser-sampling spec is a future P9-style decision.
- VFR/speed drift not modelled (audit M-8).
- Thresholds are fitted on animated films from one reel (audit R-7); transfer
  to live action is unmeasured.
- Equivalence is not visible to crawler2 until a contract event exists.
- ANN provenance not in `match.found` 1.0.
- Rebuild rates are lower bounds; production-scale memory figures beyond
  104 k vectors are estimates.
- Real multi-machine, Qdrant replication: P14.

## 18. Rollout and increments (after approval)

| Increment | Content | Depends on | Exit |
|---|---|---|---|
| I-1 | verifier `temporal/v2`, `lowinfo/v1`, tuning/calibration harness on the vector cache; L-PAIRS + N-SOAK grouped split; V1 end-to-end baseline run | D7, D8, D9, D10, D17 | held-out P/R vs both V1 baselines, param set `2.1` registered (validation §2–§3) |
| I-2 | `vindex` + Qdrant deployment (fingerprinter-owned compose for dev), generations, builder/check/rebuild CLI, gRPC ingest measurement | D1–D4, D14 | rebuild + check tests, memory/latency/ingest measurements |
| I-3 | match tasks, triggers, direction A and B, candidates, commits, held `match.found` | D5, D6, D11 | validation §4–§7, §10 |
| I-4 | equivalence | D13, D18 | validation §8 |
| I-5 | failure injection, growth test, simulated two-host, M3 run, docs | all | M3 gate (validation §11) |

Each increment is committed separately with tests and documentation.
Migrations are additive (`fingerprinter migrate` creates the new tables);
P10 can be disabled by not running `matcher run` — P9 behaviour is
unaffected.
