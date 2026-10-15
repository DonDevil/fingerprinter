# P10 Validation plan — test matrix, methodology, acceptance criteria, evidence

Status: **PLAN.** No P10 test has been run, because nothing is implemented.
§12 lists the only evidence that exists today: the audit measurements, which
are inputs to the design, not results of P10 tests. Design:
[design.md](design.md); decisions: [decisions.md](decisions.md); audit:
[audit.md](audit.md).

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

## 2. M3 gate and V1 baseline (P10-D17)

- Baselines on the **same held-out folds**:
  1. V1 matcher on V2 FP32 vectors (`temporal_v1`, today's `MatcherConfig`,
     no coarse screen) — P 1.000 / R 0.247 on all of L-PAIRS (measured,
     reproduced; audit §6.2);
  2. V1 end to end (V1 `embedding/` + V1 worker path) — **NOT MEASURED**;
     measured in I-1 with the V1 engine on L-PAIRS and N-SOAK.
- P10 decision = `confirmed` verdicts of `temporal/v2` reached through the
  real candidate path (ANN, adaptive k, caps), not the verifier alone.
- **Gate (M3, plan):** precision ≥ V1 and recall ≥ V1 on the held-out
  evaluation, against the stronger baseline. Because V1 precision is 1.000,
  the gate requires **zero false positives** on the held-out pairs. The
  report gives counts, Wilson 95 % intervals and per-transform recall.
- Additional acceptance (proposed): zero false positives on all held-out
  N-SOAK negatives; recall reported per transform and for VFR items.

## 3. Threshold tuning and calibration (I-1)

1. Grouped k-fold by film (k = 7, leave-one-film-out): tune `s_seg`,
   `s_mean`, `L`, `n_offsets`, `lowinfo` threshold on the training films,
   objective = recall subject to zero FP on training negatives.
2. Evaluate on the held-out film; aggregate over folds (each pair evaluated
   once, by the model that did not see its film).
3. Choose one final parameter set by the same rule on all films; register it
   as `2.1`; calibrate `confidence` by isotonic regression on pooled
   out-of-fold scores.
4. Candidate floors and k are tuned on the same folds with the oracle-free
   rule "media-level candidate recall ≥ 0.99 of what exact search gives".

Acceptance: the chosen setting is reported with its out-of-fold P/R; no
threshold is reported as validated from in-sample numbers (audit §7.3 is
exploratory).

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
2. Delete the Qdrant collections, aliases and volume.
3. Verify tasks go to `WAITING_INDEX`, nothing is marked done or failed, no
   fallback runs (metrics + task states).
4. `index rebuild` both roles; `index check` must pass (counts, bucket diffs,
   sampled bit-exact vectors).
5. `match rerun` for the corpus with the same technique version.

**Acceptance:** the set of decisions is **identical** (same `match_id`s,
verdicts, aligned segments; confidence equal to 1e-9), in two
configurations: exact search (`exact=true`) and the production ANN
parameters. Allowed nondeterminism: ANN scores and result order, never
decisions. A decision difference fails the test and is investigated (raise
`ef`, inspect candidates) — the criterion is not relaxed. Also: interrupt the
build after a bucket (kill -9) and resume — same result; live upserts during
the build reach the new generation (§design 12.3) — checked by indexing new
representations mid-build.

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

## 10. Resource and performance (H)

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
