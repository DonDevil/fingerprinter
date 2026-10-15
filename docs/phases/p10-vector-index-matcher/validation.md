# P10 Validation plan — test matrix, methodology, acceptance criteria, evidence

Status: **PLAN.** No P10 test has been run, because nothing is implemented.
§12 lists the only evidence that exists today: the audit measurements, which
are inputs to the design, not results of P10 tests. Design:
[design.md](design.md); decisions: [decisions.md](decisions.md); audit:
[audit.md](audit.md).

Corrected 2026-10-15 (design §19, C-1…C-7): V1 end-to-end baseline as an I-1
prerequisite (§2.1), nested film-grouped tuning/calibration (§3), lowinfo
oracle comparison (§3.4), crash/reader/lowinfo/identity test groups
(§9a–§9d), E-R1 (§10), issue→test matrix (§11a).

Rules carried from P9: a result counts for a gate only on the gate's approved
input set; every report records environment, corpus manifest SHA-256, spec
digest, index generation params and the verifier parameter set; measured and
extrapolated numbers are labelled; benchmarks run in memory-capped
`systemd-run --user` units; heavy runs never overlap.

---

## 1. Data

| Set | Status | Labels | Use |
|---|---|---|---|
| L-PAIRS (P9; 7 targets, 180 clips, 480 pairs) | exists | provenance | primary labelled set (§2, §3) |
| **N-SOAK** (new derivation, no new media) | to derive | provenance: SOAK-1000 item vs film, relevant if reel overlap ≥ 30 s, irrelevant if ≥ 15 s apart, otherwise excluded (rule of audit §9) | thousands of extra positive and negative (item, target) pairs; precision power (audit R-8) |
| SOAK-1000 vectors (run 5) | cached | provenance | historical corpus, growth, direction B |
| **E-SET** (new, P10-D18) | to build from S-SRC | provenance: same excerpt, different encoding/packaging = equivalent; different excerpt, same duration = not | level 5 (§8) |
| P8 different-encoding set (6 s testsrc) | exists in crawler2 benchmarks | provenance | run and reported only (below minimum duration) |
| Synthetic | exists | — | harness checks, never a gate |

Licensing and handling as in P9 data-preparation §6: openly licensed films
only, nothing downloaded for P10, `Blast.mp4` never used, media and vector
caches outside git, manifests and result JSONs in
`benchmarks/p10-index/results/`.

**Leakage control.** All derived clips come from one reel. Splits are
**grouped by source film**: every clip, SOAK item and E-SET item is assigned
to the fold of its film (items spanning two films go to the fold of the film
with the larger overlap and are excluded from the other film's pairs);
synthetic clips are assigned round-robin. Targets (the films themselves) are
shared by all folds; they carry no labels.

## 2. M3 gate and V1 baseline (P10-D17, corrected C-7)

### 2.1 V1 end-to-end baseline — first task of I-1, a prerequisite

The V1 end-to-end baseline must exist and be reproducible **before** any
`temporal/v2` threshold is selected, before any parity claim with V1 and
before M3 is evaluated. The P9 oracle number (V1 `match_segments` on V2
vectors, no coarse screen) is a component measurement, not the V1 baseline.

| Item | Specification |
|---|---|
| code path | `worker.matching_handler.build_matching_handler(acquirer, engine, registry, MatcherConfig())` — the V1 worker handler: acquisition → V1 `DINOv2EmbeddingEngine` segment embedding (V1 `SegmentSamplingConfig`, 5 s, `fps=1/5`) → target resolution from the V1 `TargetRegistry` (Redis, build-on-miss) → coarse screen 0.60 → `match_segments` (0.90 / 3 / 1 / 2) → `matching.aggregation` → `Result` |
| invocation | an I-1 harness `benchmarks/p10-index/v1_e2e_baseline.py` calls the handler in-process with one `Job` per (clip, target) pair; targets registered once through the V1 registry from the S-SRC files; clips served by the local fixture HTTP server used by V1's own integration tests (`tests/media_test_server.py`), through V1's SSRF guard as configured for those tests; Redis db 15 |
| recorded | fingerprinter commit, V1 config values, V1 model id/revision as loaded from the HF cache, torch/transformers versions, device, L-PAIRS and N-SOAK manifest + CSV SHA-256s, per-pair `Result` (decision, confidence, evidence JSON) |
| metrics | TP/FP/FN/TN, precision, recall, Wilson 95 % intervals, per transform, per film |
| preserved | `benchmarks/p10-index/results/v1-e2e-<corpus>.json` (committed), per-pair outputs |
| populations | L-PAIRS (480 pairs) and N-SOAK provenance pairs, evaluated per outer fold (§3) so the comparison with P10 uses the identical held-out pairs |
| blockers known | none found in code; if the SSRF guard or registry cannot run against the fixture server, the exact blocker is recorded and the smallest change is a test-only configuration (no V1 production change) |

### 2.2 Gate

- Baselines on the **same held-out pairs**: (1) V1 end to end (§2.1);
  (2) V1 matcher on V2 vectors (P 1.000 / R 0.247 on all L-PAIRS; worker path
  with coarse screen: R 0.220 — audit §6.2).
- P10 decision = `confirmed` verdicts of `temporal/v2` through the real
  candidate path (ANN, adaptive k, caps, lowinfo at a current revision).
- **M3:** precision ≥ V1 and recall ≥ V1 on the outer-fold held-out pairs
  against the **stronger** baseline. With V1 precision 1.000 this means
  **zero false positives**. Reported with counts, Wilson intervals,
  per-transform and per-film recall, and the VFR items separately.
- Candidate generation, verifier thresholds, lowinfo rule and calibration
  are never tuned on the outer held-out film.
- Disclosure: animated films only, one reel, no live action, no cam/screen
  captures; the M3 statement is bounded to that population.

## 3. Tuning, calibration and evaluation protocol (I-1; corrected C-5)

### 3.1 Grouping and splits

- Grouping unit: **source film** (7). Every L-PAIRS clip, N-SOAK item and
  E-SET item belongs to the film it was cut from; items spanning two films
  are assigned to the film with the larger overlap and excluded from pairs
  with the other film; synthetic clips are distributed round-robin. Pairs
  are assigned to the fold of their clip/item, never split by row.
- **Outer loop (evaluation):** leave-one-film-out, 7 folds. The held-out
  film's pairs are used only for the final metrics of that fold.
- **Inner loop (tuning and calibration), inside the 6 training films:**
  leave-one-film-out over the 6 films produces out-of-fold verifier scores
  for every training pair. Thresholds (`s_seg`, `s_mean`, `L`, `n_offsets`,
  candidate floor/k, `τ_c`, `τ_p`) are selected on the inner folds; the
  calibration map is fitted on the inner out-of-fold scores of the 6 films.
  Nothing from the outer held-out film is used.
- Final deployed parameter set and calibration = the same inner procedure run
  on all 7 films; its expected performance is the outer-loop estimate
  (nested cross-validation). The outer-loop estimate is what is reported.

### 3.2 Threshold selection

Objective on training groups: maximum recall subject to zero false positives
on training negatives (L-PAIRS + N-SOAK). `τ_c` is the lowest score with zero
training false positives; `τ_p` the lowest with training precision ≥ 0.9.

### 3.3 Calibration

- Outcome: "this proposed (candidate-generated, verified) pair is a true
  match" (provenance label).
- Minimum data to fit isotonic regression: ≥ 50 positive and ≥ 50 negative
  out-of-fold examples in the calibration set, from ≥ 5 films, with ≥ 10
  negatives scoring above `τ_p` (otherwise the upper range is unconstrained).
  Below that: Platt (two parameters) if it passes the reliability check;
  otherwise `status = UNAVAILABLE` and no publication (design §9.1).
- Reliability on the outer held-out folds: Brier score and expected
  calibration error (≤ 5 equal-mass bins), with film-level bootstrap 95 %
  intervals. **Acceptance:** calibrated Brier ≤ Brier of the constant
  prevalence predictor and ≤ Brier of the raw score min-max-scaled to [0,1];
  in every bin with ≥ 10 examples, the observed frequency lies within the
  bootstrap interval of the mean predicted probability. No universal ECE
  threshold is claimed.
- Reported with every calibration: positives/negatives, films, effective
  sample size caveat (correlated clips), the identity of all data used.
- Stale calibration (design §9.1) ⇒ re-run the reliability check on the new
  pipeline before publication resumes.

### 3.4 Low-information filter evaluation (`lowinfo/v1`, provisional)

- Oracle: the same verifier and candidate path **without** the filter
  (unfiltered), on the same folds.
- Measured: true matches confirmed by the unfiltered path but lost with the
  filter (`lost_tp`); false positives removed; candidates and verification
  time saved; segments excluded.
- **Acceptance for adoption:** `lost_tp = 0` on the outer held-out pairs of
  L-PAIRS and N-SOAK, and FP(filtered) ≤ FP(unfiltered). Otherwise the
  filter is not enabled (`lowinfo` off is a valid configuration).
- Adequacy: the rule needs segments shared by different target works; with 7
  films these are mainly logos, black and credits. If fewer than 20 target
  segments are flagged at all, the filter's benefit is reported as **not
  established** and the smallest additional experiment is a set of ≥ 5
  further openly licensed works with shared studio idents/credits (not
  created unless the operator asks).

## 4. ANN recall@k (B)

- **Oracle:** FAISS `IndexFlatIP` over the same vectors (offline only).
- **Segment recall@k** = |ANN top-k ∩ exact top-k| / k averaged over queries,
  k ∈ {10, 100}; ties reported.
- **Media-level candidate recall** = fraction of provenance-relevant
  subjects that the full candidate policy (adaptive k, floor, min hits, caps)
  returns with ANN, divided by the same with exact search; plus absolute
  recall vs provenance.
- **Relevant** = provenance overlap ≥ 30 s; multiple segments map to one
  subject (content or target version) by grouping.
- Measured: latency p50/p95/p99 per query through the real `QdrantIndex`,
  batch throughput, container memory (`memory.peak`, steady), build time.
- **Acceptance:** segment recall@100 ≥ 0.97; media-level candidate recall with
  ANN ≥ exact − 0.005 for directions A and B; per-segment query p95 ≤ 10 ms
  on the dev host with the stack running; Qdrant within its 1.5 GiB limit
  during build and serving.

## 5. Index drop and rebuild (C)

Fixed corpus: L-PAIRS (targets + clips) + 200 SOAK items, ingested through the
real P9 path into a test keyspace; matching run to completion.

1. Record all decisions (`match_id`, verdict, confidence, aligned segments) and
   equivalence links.
2. Record the generation manifest digests (design §12.5); delete the Qdrant
   collections and volume.
3. Verify tasks go to `WAITING_INDEX`, nothing is marked done or failed, no
   fallback runs (metrics + task states).
4. `index rebuild` both roles; validation (design §12.5) must pass and the
   rebuilt manifest digests must **equal** the recorded ones (identities,
   vectors, roles, metadata, retirement state, classification revision,
   params).
5. `match rerun` for the corpus with the same technique version.

**Acceptance:** (a) equal manifest digests — the index contents are
identical, which is the deterministic invariant; (b) the set of decisions is **identical** (same `match_id`s,
verdicts, aligned segments; confidence equal to 1e-9), in two
configurations: exact search (`exact=true`) and the production ANN
parameters. Allowed nondeterminism: HNSW graph layout, ANN scores, result
order and candidate lists (differences are recorded) — never decisions. A decision difference fails the test and is investigated (raise
`ef`, inspect candidates) — the criterion is not relaxed. Also: interrupt the
build after a bucket (kill -9) and resume — same result; live upserts during
the build reach the new generation (design §12.4) — checked by indexing new
representations mid-build; crash points in §9a.

## 6. Verification workload and growth (D)

Corpora: base = L-PAIRS clips (5,460 vectors); 10× = base + SOAK subset
(≈ 55 k); 19× = base + all SOAK (104 k) — the same scales as audit §10.1.
Queries: the 7 targets (direction B), all L-PAIRS clips (direction A),
E-SET items (equivalence).

Per task, record: query segments, ANN hits, hits ≥ floor, low-information
segments, candidates grouped, removed by dedup/min-hits, stored, verified,
truncated, saturated segments, verification seconds.

**Acceptance (bounds):** at every scale, per task: ANN hits ≤ segments ×
`k_max`; candidates verified per page ≤ `page_B`; stored candidates ≤
`cap_B_total`; direction A candidates ≤ `cap_A`; every bound hit is recorded
on the task. **Acceptance (effectiveness):** direction B media recall vs
provenance at 19× ≥ 0.99 (audit measured 0.9965 for the provisional policy
with exact search), direction A recall at every scale ≥ the held-out recall
of §2; precision unchanged (zero FP) at every scale.

## 7. New target against historical media (E)

Real stack, real registry: ingest SOAK subset + L-PAIRS clips through
`encode.requested` (fixture server) so representations exist; run the matcher
(no targets yet); then `target register` the 7 films one by one.

**Acceptance:** for each target, matches against historical contents appear
(recall vs provenance ≥ the §2 held-out recall); `encode_requests` and
`encode_attempt_log` show **no new encode attempt** for any historical
content (only the target reference itself is encoded or reused); the target
task records counts; `match.found` envelopes are held and released by
`publisher republish` after a test group registers.

## 8. Level-5 equivalence (F)

E-SET: ≥ 20 S-SRC excerpts (60–300 s) × {H.264 low/high bitrate, MKV remux,
fragmented MP4 with 1 s and with 2 s fragments, VP9 WebM}, plus negatives
(other excerpts of equal duration; the same excerpt shifted by 30 s; a 50 %
sub-clip).

**Acceptance:** all variants of one excerpt have pairwise distinct
`ContentId`s (P8 level 3); every pair of variants is `LINKED` (recall 1.0 is
the target; misses reported); zero links between negatives; a sub-clip is not
linked as equivalent; `equivalence revoke` sets REVOKED and keeps evidence;
no record merged. P8's 6 s set: run, expected not linked (2 segments < L),
reported as a known minimum-duration limit. HLS variants have no `ContentId`
in P8 (P8-D6) and are out of scope.

## 9. Failure and recovery (G)

| Test | Injection | Expected |
|---|---|---|
| G-1 index unavailable | stop Qdrant during A and B tasks | tasks `WAITING_INDEX`, `index_unavailable_total` rises, health degraded, no decision, resume after restart, no duplicates |
| G-2 index loss | delete volume | §5 |
| G-3 corruption | delete a subset of points; alter a payload | `index check` reports missing/stale; `repair` fixes; queries skip stale |
| G-4 interrupted rebuild | kill -9 the builder mid-bucket | resume from the cursor; identical final counts |
| G-5 duplicate hints/events | replay `representation.ready` entries; duplicate `target.registered` | one task, one decision per `match_id` |
| G-6 duplicate match requests | `match retry` twice concurrently | one decision; the second is a no-op |
| G-7 concurrent indexers | two matchers index the same subject | identical points (deterministic ids) |
| G-8 stale entries | retire a target; retire a representation (test spec) | points removed; stale candidates dropped and counted |
| G-9 Scylla failure | pause Scylla during commits | typed retries; intents resolved by the reconciler; no lost or duplicated match |
| G-10 MinIO failure | stop MinIO during verification; corrupt one artifact | retryable then `verification_failed`; SHA-256 mismatch → FAILED + alert |
| G-11 worker restart | kill -9 matchers in each task state | lease expiry, resume, no lost work |
| G-12 rebuild + new representations | ingest during a build | new subjects in both generations before promotion |
| G-13 overload / timeout | tiny verification timeout; many candidates | per-candidate retries, INCOMPLETE `verification_failed`, bounded queue, nothing silently dropped |
| G-14 simulated two-host | two matcher processes, one Qdrant | no duplicates, lease takeover works (real machines: P14) |

### 9a. Index mutation and rebuild crash tests (design §12; corrected C-3)

Each test runs on the real stack with a fault-injection hook at the named
boundary (`kill -9` of the worker or builder, or a Qdrant/Scylla stop) and
ends with `matcher reconcile run --once` plus `index check`. Common
verification: generation manifest digest equals the digest computed from the
artifacts; Qdrant verification reports 0 stale, 0 orphans, 0 missing;
`index_entries` all APPLIED for routed generations; no task lost.

| # | Crash / fault point | Expected recovery |
|---|---|---|
| R-1 | before the intent LWT | nothing changed; the backstop scan writes the intent; converges |
| R-2 | after intent, before Qdrant | row PENDING; reconciler applies |
| R-3 | after Qdrant upsert, before bookkeeping | row PENDING; re-apply is idempotent; points identical |
| R-4 | after bookkeeping, before completion of all generations | missing `applied[g]` found; applied |
| R-5 | during DELETE (partial: some points deleted) | row PENDING; delete by filter repeated; 0 points left |
| R-6 | during bulk load of a rebuild | new builder (after lease expiry) resumes from the bucket cursor; same manifest |
| R-7 | during catch-up | resumes; completes a full pass; same manifest |
| R-8 | during validation | generation stays non-active; re-validated |
| R-9 | immediately before the promotion LWT | READY generation keeps receiving writes; re-validated if stale; promoted once |
| R-10 | immediately after promotion | old generation DRAINING → RETIRED after grace; readers never see an empty pointer |
| R-11 | during old-generation retirement (collection delete) | RETIRED state reached; stale writers' "collection not found" treated as no-op only because Scylla says RETIRED |
| R-12 | live writes (≥ 1 subject/s, mixed UPSERT/DELETE) throughout a rebuild | every subject present with the newest `seq` in the new generation at promotion |
| R-13 | two simultaneous `index rebuild` | one acquires the build lease; the other refuses; a stale holder's transitions fail on epoch |
| R-14 | stale worker: hold an old UPSERT, let a newer DELETE complete, then release the old write and kill the worker before its LWT | Qdrant verification finds the stale point (`seq` lower / row DELETE) within one cycle and removes it; queries meanwhile drop it as stale |
| R-15 | uncertain Qdrant outcome (proxy drops the response after the server applied it) | retry applies again; no duplicate points |

### 9b. Hint reader and backstop scans (design §6.1; corrected C-4)

| # | Test | Expected |
|---|---|---|
| H-1 | crash after reading a batch, before recording work | batch re-read; work recorded once |
| H-2 | crash after recording work, before cursor LWT | batch re-read; duplicates are no-ops |
| H-3 | duplicate and out-of-order entries | one task per subject; desired state from authoritative rows |
| H-4 | two readers (simulated two hosts) | one owner; the other cannot advance the cursor (epoch); takeover after lease expiry |
| H-5 | entries trimmed before reading (group acked, `XTRIM MINID`) | gap metric; backstop creates the work within one cycle |
| H-6 | backstop pagination across > 1 page per bucket, crash mid-bucket | resumes at the persisted `(bucket, last content_id)`; every row visited once per cycle (row-visit log in the test) |
| H-7 | targets registered/retired with no stream event (held) | targets scan discovers them; target-set revision bumps once per change |
| H-8 | eventual catch-up | after stopping the hint reader entirely, every READY representation of a 200-item corpus gets an index entry and tasks within one backstop cycle |

### 9c. Low-information lifecycle (design §7.3; corrected C-2)

| # | Test | Expected |
|---|---|---|
| L-1 | register a target sharing frames with existing targets | revision bumps; every target owning a segment ≥ 0.95 to the new target is reclassified (checked against an exhaustive offline computation) |
| L-2 | retire a target | revision bumps; every affected target reclassified (same check) |
| L-3 | crash after the revision bump, before recomputation | `done_rev < requested_rev`; refresh resumes; converges |
| L-4 | stale flag present (refresh paused) | the flagged segment still votes/queries/counts; matches identical to the unfiltered path for that segment |
| L-5 | duplicate registration/retirement observation | revision bumps once (members digest unchanged) |
| L-6 | stale worker writes classification for an older revision | LWT `IF rev <` rejects it |
| L-7 | backstop weekly recomputation | finds and fixes an injected wrong flag |
| L-8 | registration during a running refresh | refresh loops to the latest revision |
| L-9 | refresh failing repeatedly | BLOCKED, visible; flags treated as stale ⇒ eligible; no lost match |
| L-10 | offline comparison filtered vs unfiltered (§3.4) | `lost_tp = 0` before adoption |

### 9d. Match identity and persistence (design §9.3a; corrected C-6)

| # | Test | Expected |
|---|---|---|
| K-1 | same MatchId written twice (and concurrently) | one row; second write a no-op |
| K-2 | same content + target version, two specs (`dinov2-seg/1` and `/2` in a test keyspace), same technique version | two rows, two MatchIds |
| K-3 | same content, two target versions | two rows |
| K-4 | two bindings of one target version (two specs) | not collapsed |
| K-5 | new technique version | new rows, old rows unchanged |
| K-6 | every row: `match_id == MatchId.of(...)` of its key; `matches_by_id` resolves back to the row | holds for all rows |
| K-7 | retry after a crash between `matches` and `matches_by_target` | no duplicates; both rows present after reconcile |
| K-8 | publication | one held envelope per MatchId; `event_id` stable across retries; uncalibrated technique ⇒ no envelope |

## 10. Resource and performance (H)

**Before I-2: E-R1** (design §13.4) — the capacity-boundary experiment
(626,658 vectors, 1.5 GiB cap, two generations resident, live writes),
with its pass criterion and the decision each outcome supports. Until it
passes, `index.max_points` stays 250,000 and the base target scenario is not
claimed for the dev host.


Measured and reported (each run alone on the host): index build time and
peak memory per role; query latency distribution; recall@k; Qdrant memory
(steady, peak) and disk; incremental upsert latency (one representation);
rebuild duration on the 104 k corpus with gRPC and 4 parallel readers, and the
extrapolation to the §8 scenarios (labelled extrapolated); verification time
per candidate by duration; contention: matcher + Qdrant + encoder + stack
together (host `MemAvailable` floor, encoder throughput change).

## 11. M3 exit checklist (to be filled after implementation)

| Item | Criterion | Status |
|---|---|---|
| M3-1 | P/R ≥ V1 (stronger baseline), held-out, zero FP (§2) | not run |
| M3-2 | new target matched against historical media without re-encoding (§7) | not run |
| P10-T1 | ANN recall@k (§4) | not run |
| P10-T2 | drop-and-rebuild identical decisions (§5) | not run |
| P10-T3 | growth bounds + effectiveness (§6) | not run |
| P10-T4 | level-5 linking (§8) | not run |
| P10-T5 | failures (§9) | not run |
| P10-T6 | resources (§10) | not run |
| P10-T7 | V1 end-to-end baseline reproducible (§2.1) — prerequisite of M3-1 | not run |
| P10-T8 | calibration status and reliability (§3.3) — prerequisite of publication | not run |
| P10-T9 | lowinfo vs unfiltered oracle, `lost_tp = 0` (§3.4) — prerequisite of enabling lowinfo | not run |
| P10-T10 | E-R1 capacity (design §13.4) — prerequisite of I-2 | not run |
| P10-T11 | mutation/rebuild crashes R-1…R-15, reader/scan H-1…H-8, lowinfo L-1…L-10, identity K-1…K-8 (§9a–§9d) | not run |

### 11a. Issue → test matrix (corrections C-1…C-7, design §19)

| Issue | Tests / measurements | Pass criterion | Blocks |
|---|---|---|---|
| C-1 resources | E-R1; §10 | no OOM, anon peak ≤ 1.35 GiB, p95 ≤ 10 ms | I-2; base target scenario on dev |
| C-2 lowinfo freshness | L-1…L-10; §3.4 | exhaustive-check equality; `lost_tp = 0` | enabling lowinfo (matching works without it) |
| C-3 mutation/rebuild | R-1…R-15; §5 | 0 stale/orphan/missing; equal manifests; identical decisions | I-2 exit |
| C-4 reconciliation ownership | H-1…H-8 | every item discovered within one backstop cycle; no cursor-before-work loss | I-3 exit |
| C-5 confidence | §3.3 | reliability acceptance or explicit UNAVAILABLE | publication of `match.found` |
| C-6 match key | K-1…K-8 | all pass | I-3 exit |
| C-7 V1 baseline | §2.1 | reproducible report exists | threshold selection, M3-1 |

## 12. Evidence that exists today (audit inputs, not P10 tests)

| Measurement | Result | File |
|---|---|---|
| Vector cache reproduces P9's V1-oracle result | TP 37 / FN 113 / FP 0 / TN 330 (identical) | `results/lpairs-eval.json` |
| V1 worker path (coarse 0.60) | TP 33 / FP 0 | same |
| Phase/transform/negative/coarse diagnostics | audit §7 | same |
| Exploratory V1 threshold sweep (in-sample) | e.g. 0.80/3: R 0.693, 0 FP | same |
| Exact, FAISS and Qdrant variants on 104 k real vectors | audit §9.1 | `results/ann-bench.json` |
| Candidate policies, growth 19×, adaptive k | audit §10 | `results/ann-bench.json`, `results/ann-growth.json` |
| Qdrant under 256 MiB / 512 MiB caps | OOM / peak = cap, steady 1.3 KB/vector | `results/qdrant-int8-512mib.json` |
| Artifact read rate, Qdrant ingest rate | ≈ 315 vectors/s; ≈ 310–500 points/s (lower bounds) | audit §11 |

Not measured: V1 end to end; any P10 component; gRPC ingest; snapshots;
latency under concurrent load; live-action material.
