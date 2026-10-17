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

Second correction 2026-10-15 (design §19.1, C-8…C-11): targets-scan
pagination cases H-6/H-9; low-information freshness L-1…L-16 with expected
fail-closed behaviour; R-1…R-21 (R-22…R-25 added by C-12) with initial state, fault, state before
repair, repair and pass condition; capacity guard CAP-1…CAP-8; E-R1 aligned
with the resident-point accounting (§10). All are **planned**; none has run.

Third correction 2026-10-17 (design §19.1, C-12): stale DELETE after a newer
UPSERT — R-22 (the race, in ACTIVE, VALIDATING, READY and right after
promotion, with crash and uncertain-response variants), R-23 (stale UPSERT
then stale DELETE after a newer UPSERT), R-24 (Qdrant 1.15.1 filter
semantics), R-25 (non-protocol point loss during matching: suspect
generation, no DONE, re-run); common pass condition extended; E-R1 adds a
resident media collection and re-UPSERTs. All **planned and unrun**.

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
  candidate path (ANN, adaptive k, caps, only *usable* lowinfo flags —
  design §7.3).
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

### 9a. Index mutation and rebuild crash tests (design §12, §12.9; corrected C-3, C-11)

Each test runs on the real stack (test keyspace, dev Qdrant) with a
fault-injection hook at the named boundary (`kill -9` of the worker,
builder or reconciler; a held/paused worker; a proxy that drops or delays
Qdrant responses; a Qdrant/Scylla stop) and ends with `matcher reconcile run
--once` plus `index check`. **Common pass condition** (in addition to each
row): generation manifest digest equals the digest computed from the
artifacts; Qdrant verification reports 0 stale, 0 orphans, 0 missing;
`index_entries` all APPLIED with `applied[g] = seq` for every generation in
`CREATED…DRAINING`; no task lost; no match committed whose subject was not
authoritatively live at decision time; **the current-`seq` points of every
live subject were present in every routed generation at every sample taken
after their UPSERT was applied** (a 100 ms poller counts `subject = S AND seq
= row.seq` during the test — S2); no task reached DONE while the generation it
searched had `suspect_since` set. **Evidence recorded:** the
`index_entries` rows and generation rows before and after repair, Qdrant
point ids + payload `seq` of the affected subjects (scroll), reconciler
metrics, the injected-fault log, matches written during the test.

| # | Initial state | Injected fault / race | Expected state before repair | Expected repair | Pass condition |
|---|---|---|---|---|---|
| R-1 | subject S READY in P9, no row | crash before the intent LWT (also: LWT returns unknown) | no row; Qdrant unchanged (unknown ⇒ SERIAL read before any Qdrant call) | backstop scan writes the intent; apply | Qdrant never written before the row existed (hook asserts); converges |
| R-2 | as R-1 | crash after intent, before Qdrant | row PENDING, no points | intents pass applies | points = artifact; row APPLIED |
| R-3 | as R-1; also a DELETE variant | crash after Qdrant applied, before bookkeeping | Qdrant done, row PENDING | re-apply (idempotent), record | identical point set (ids, vectors); no duplicates |
| R-4 | two routed generations (ACTIVE + BUILDING) | crash after applying to one generation | `applied` has one generation only | intents/catch-up pass applies to the other | both generations hold S at `seq` |
| R-5 | S indexed, then desired DELETE | Qdrant stopped mid delete-by-filter | some points left; row PENDING | delete by filter repeated | 0 points of S; no candidate from S accepted meanwhile |
| R-6 | rebuild in BUILDING, half the buckets done | kill -9 builder; new builder after lease expiry | generation BUILDING, cursor at bucket b | new holder adopts g, resumes from the cursor | same manifest digest as an uninterrupted build |
| R-7 | g in CATCHING_UP | (a) kill -9 the pass; (b) while the pass holds S at `seq = s`, a mutator writes s+1 (DELETE) and applies; then the pass resumes and lands its stale UPSERT and is killed before its LWT | (a) partial pass; (b) g contains a stale point of S | (a) pass restarts, completes a full pass; (b) the pass's LWT fails or VALIDATING's no-extras check finds S ⇒ back to CATCHING_UP; or verification removes it | g promoted only with S absent; no match from S |
| R-8 | g in VALIDATING | kill -9 during validation | g non-active | re-validated | never promoted on a partial validation |
| R-9 | g READY | crash immediately before the promotion LWT; live writes continue | g READY, still routed | revalidation if older than `revalidate_after_s`; promote once | exactly one ACTIVE; `active_epoch` +1 once |
| R-10 | promotion done | crash immediately after the LWT; a reader caches the old pointer | old generation DRAINING | reconciler RETIRED after grace | readers never see an empty pointer; old pointer searches succeed during grace |
| R-11 | old generation DRAINING → RETIRED | a worker holding a pre-retirement routing set writes to it after deletion | collection gone | none needed | write fails "not found", treated as no-op only because Scylla says RETIRED; no collection re-created; generation numbers not reused |
| R-12 | g just CREATED | live writes ≥ 1 subject/s mixed UPSERT/DELETE through the whole rebuild, plus a mutator held between its intent LWT and its routing read across CREATED, and one whose routing read follows CREATED | in-flight rows missing from g | catch-up (E1) / own mutator (E2) | every subject present in g at its newest `seq` at promotion; no extras |
| R-13 | build lease held by builder 1 | second `index rebuild`; then builder 1 pauses past its lease, builder 2 takes over, builder 1 resumes | — | builder 2 adopts or FAILs g | builder 1's transitions fail on `build_epoch`; one ACTIVE; its stale Qdrant writes found by no-extras/verification |
| R-14 | S indexed at seq s (UPSERT) | worker W holds an old UPSERT; a newer DELETE (s+1) completes; W lands its write and is killed before its LWT | row DELETE APPLIED; stale points of S in Qdrant | verification finds `seq` < row / present for DELETE; delete | while stale: queries that hit S drop it (authoritative check), `index_verify_stale_candidates_total` > 0, no match with S; after one cycle 0 points |
| R-15 | S PENDING | proxy drops the response after the server applied (also: times out before applying) | outcome unknown | retry applies the current desired state | no duplicate points; `attempts` incremented; permanent 4xx ⇒ BLOCKED with error, not retried |
| R-16 | g ACTIVE | insert a point with a valid-looking payload for a subject without a row (and one for a retired subject) | orphan point | verification: READY ⇒ intent + apply; else delete by id | orphan removed or adopted; never accepted as a candidate for a retired subject |
| R-17 | S row APPLIED | delete S's points directly in Qdrant | row APPLIED, points missing | verification count ≠ `segment_count` ⇒ re-apply; S's own task re-enqueued | points restored; `index_miss_total` +1; S's pairs re-covered (task rerun recorded) |
| R-18 | S at seq s | (a) newer UPSERT s+1 (payload change) supersedes s while the s worker is paused; (b) the s worker's Redis lease expires and a second worker applies s+1, then the s worker resumes and writes | Qdrant may hold `seq = s` payload | the s worker's LWT fails ⇒ it re-applies s+1; if killed instead, verification re-applies | the stale `seq = s` write lands as **separate** points (ids contain `seq`) and never overwrites `seq = s+1` points (poller: their count stays `segment_count`); after cleanup only `seq = s+1` points remain; Redis lease shown not to fence (the write lands) |
| R-19 | S UPSERT APPLIED | DELETE s+1 written while an UPSERT retry of s is in flight | either | as R-14 | 0 points of S; DELETE row kept with `applied[g] = s+1` for all routed generations |
| R-20 | ACTIVE generations of both roles, matches recorded | delete the Qdrant volume | tasks `WAITING_INDEX` | `index rebuild` (target role first), validation, promotion | §5 acceptance: equal manifest digests, identical decisions; no fact lost |
| R-21 | a verification pass has repaired part of a corrupted generation (R-16/R-17 injections) | kill -9 the reconciler mid-pass; restart | partially repaired | pass resumes from its persisted cursor | every injected divergence repaired exactly once in effect; repeating the pass changes nothing |
| R-22 | S live, row UPSERT `seq = 10`, Qdrant holds S's points with `seq = 10` (variants: g ACTIVE; g VALIDATING; g READY then promoted; promotion LWT executed while A is paused) | worker A reads DELETE 11 and is paused before its Qdrant call (hook); S becomes READY again ⇒ UPSERT 12 written; worker B applies 12 ((a) + cleanup) and records `applied[g] = 12`; A resumes and sends delete `subject = S AND seq ≤ 11`, its LWT fails. Sub-variants: (e) A is killed right after its Qdrant call; (f) the proxy drops A's response and A retries | Qdrant: all `segment_count` points of S with `seq = 12` present; no `seq ≤ 11` points; row UPSERT 12 APPLIED; A's LWT rejected (logged) | none needed; (f) A's retry re-reads the row and applies UPSERT 12 (idempotent) | scroll shows S's point ids = the 12-ids and payload `seq = 12` before and after A's DELETE; poller never saw fewer than `segment_count` `seq = 12` points; `index_miss_total` unchanged; validation of g in the VALIDATING/READY variants passes and promotion proceeds without re-validation being triggered by A; a Direction-A task for a clip of S run immediately after A's DELETE retrieves S as a candidate |
| R-23 | as R-22, UPSERT 12 applied | worker C holds UPSERT 10, worker A holds DELETE 11; after 12 is applied, C lands its UPSERT 10, then A lands DELETE 11; both killed before their LWTs | `seq = 12` points present throughout; `seq = 10` points created by C then removed by A's `seq ≤ 11` filter | none needed for the 12 points; any remaining older-`seq` points removed by verification | poller: `seq = 12` count = `segment_count` at every sample; final scroll: only `seq = 12` points; contrasts with the pre-C-12 protocol, where this sequence deletes S |
| R-24 | Qdrant 1.15.1 (pinned image), a test collection with the §4.2 parameters and the §4.3 payload indexes (`rep`/`trep` keyword, `seq` integer) | points of subject S with `seq = 12`, and of S with `seq = 10`, and of another subject with `seq = 5`; issue delete with filter `must: [rep = S, seq range lte 11]` (and the `trep` variant), `wait=true` | — | — | only S's `seq = 10` points are deleted; S's `seq = 12` points and the other subject remain (exact `count` per condition before and after); repeat without the integer index to show whether the result depends on it (recorded). **Planned, unrun — the semantics are an assumption until this runs** |
| R-25 | g ACTIVE, target T indexed; a media task M is searching g | delete T's current points directly in Qdrant (non-protocol loss) before M searches; M finishes; then verification runs | M found no candidate T; g gets `suspect_since` when verification counts T short; tasks searching g after that go `WAITING_INDEX(suspect)` | T re-applied; T's `target_vs_media` re-run enqueued after the repair is confirmed; `suspect_since` cleared | the re-run of T confirms the (M, T) match; no `no_match` verdict row exists for (M, T) from the gap; no task reached DONE on g between `suspect_since` and its clearance; `match_task_reruns_total{reason=index_repair}` = 1 |

### 9b. Hint reader and backstop scans (design §6.1; corrected C-4, C-8)

| # | Test | Expected |
|---|---|---|
| H-1 | crash after reading a batch, before recording work | batch re-read; work recorded once |
| H-2 | crash after recording work, before cursor LWT | batch re-read; duplicates are no-ops |
| H-3 | duplicate and out-of-order entries | one task per subject; desired state from authoritative rows |
| H-4 | two readers (simulated two hosts) | one owner; the other cannot advance the cursor (epoch); takeover after lease expiry |
| H-5 | entries trimmed before reading (group acked, `XTRIM MINID`) | gap metric; backstop creates the work by the end of the next cycle that starts after the trim |
| H-6 | `reps` pagination across > 1 page per bucket, crash mid-bucket | resumes at the persisted `(bucket, last content_id)`; every row visited at least once per cycle (row-visit log in the test) |
| H-7 | targets registered/retired with no stream event (held) | targets scan discovers them; target-set revision bumps once per change |
| H-8 | eventual catch-up | after stopping the hint reader entirely, every READY representation of a 200-item corpus gets an index entry and tasks by the end of the first backstop cycle that starts after ingestion |
| H-9 | targets scan pagination (below) | sub-cases H-9a…H-9h all pass |

H-9 setup: Scylla 6.2.3 test keyspace (tablets off, as `db.py` creates it);
≥ 300 target partitions, of which ≥ 20 hold ≥ 4 clustering rows (several
versions × 2 specs in a test spec registry); keys-page `LIMIT` lowered to 3
and the driver `fetch_size` inside a partition lowered to 1 so that pages and
fetches end between clustering rows; the scanner's row-visit log and every
desired-state write recorded. **Failure** in any sub-case = an authoritative
row present at a cycle's start that the cycle did not visit, a cursor
pointing into a partially processed partition, or duplicate *work* (a second
task/intent rather than an idempotent no-op).

| # | Injected condition | Pass condition |
|---|---|---|
| H-9a | fetch inside a partition ends between two clustering rows of one `target_id`; the scanner continues | all rows of the partition visited before the cursor moves past it |
| H-9b | kill -9 after processing a partition, before the cursor LWT | restart re-processes that partition from `>= last_token`; no row missing; duplicates are no-ops |
| H-9c | kill -9 right after the cursor LWT | restart continues at the next partition; nothing re-done except the `>=` partition |
| H-9d | kill -9 in the middle of a partition (half its rows done) | cursor still names the previous complete partition; the partition is processed again in full |
| H-9e | duplicate and out-of-order discovery (scan + task start observe the same binding; replay a page) | one `index_entries` row and one task per binding; `target_set_rev` bumped once |
| H-9f | insert a new version (PENDING→READY) of a target behind the cursor during a cycle | discovered by the next cycle (visit log) — within two cycles of the write |
| H-9g | retire a target version (`targets.state`) behind and ahead of the cursor during a cycle | DELETE intent written, membership updated, by the end of the next cycle; tasks of that version stop |
| H-9h | full cycle, then restart from the persisted cursor only (fresh process, empty caches) | the union of visit logs covers every row present at cycle start; the cycle-completion record (`cycle += 1`) only after a short keys page; a stub token source with two keys sharing one token yields both keys (no skip) and no stall |

### 9c. Low-information lifecycle (design §7.3; corrected C-2, C-9)

Reference for every case: an **exhaustive offline classification** (numpy,
float32, all target segments of the test corpus at the authoritative target
set of that moment). "Fail-closed" = the segment stays **eligible** (votes,
queries, counts); "filter OK" = exclusion of the segment is permitted
because the flag is usable. Every case records: the authoritative target
set, `target_set_rev`, the `lowinfo_class` rows, witness-check log, payloads,
and the segments excluded by each task.

| # | Initial state | Injection / race | Expected | Class |
|---|---|---|---|---|
| L-1 | complete classification at rev r | register (and scan observes) a target X sharing frames with existing targets | rev r+1; X COMPLETE; other targets gain flags with X as witness; flags = exhaustive reference (after `done_rev = r+1`) | filter OK after verification |
| L-2 | T flagged only by witness X | retire X (observed) | records listing X found from witness lists; T's segment cleared (no other witness) or keeps another live witness; `target_vs_media` of T re-enqueued when a flag clears | fail-closed until cleared, then filter OK |
| L-3 | as L-1 | crash after the revision bump, before all records written | `done_rev < requested_rev`; resumed; converges to the reference | records not yet written ⇒ fail-closed (missing flags) |
| L-4 | T flagged by witness X | retire X in P9, refresh paused | witness check finds X dead ⇒ T's segment eligible; matches identical to the unfiltered path for it | fail-closed |
| L-5 | — | duplicate registration/retirement observation | revision bumps once (members digest unchanged) | — |
| L-6 | record at rev r+1 | a stale worker writes T's record for rev r | LWT `IF rev <` / `IF rev = :read` rejects it | — |
| L-7 | complete classification | corrupt one record (a flag with a live but non-similar witness), delete another record | full recomputation reports `extra_flag` and `missing_record` and repairs both; detection within one recomputation | the missing record is fail-closed; the corrupted flag is outside the safety model (records are written only after an exact cosine check) and is an **alarm case** — the test measures that it is detected and repaired, and reports the window |
| L-8 | refresh running for r+1 | register Y (r+2) during it | worker loops to r+2; `done_rev = r+2` only after both steps complete | filter OK per usable flag throughout |
| L-9 | — | refresh fails repeatedly (Qdrant stopped for the refresh worker) | BLOCKED, visible, `last_error`; exclusions still only through usable flags | fail-closed where witnesses died |
| L-10 | — | offline comparison filtered vs unfiltered (§3.4) | `lost_tp = 0` before adoption | acceptance gate (separate) |
| L-11 | complete classification | register X in P9; the scan has **not** observed it; run media tasks | no new exclusions; no flag references X; results equal to the run before X except the X candidates themselves | fail-closed (missing flags) |
| L-12 | T flagged only by X | retire X in P9; scan not yet observed; `target_set_rev` unchanged; run media tasks hitting T | witness check: X dead ⇒ T's segment eligible; decisions equal the unfiltered path for T | fail-closed |
| L-13 | T's record `PARTIAL` (refresh killed mid-T) | run tasks hitting T while the refresh is incomplete | no segment of T excluded | fail-closed |
| L-14 | a test segment duplicated in > `L` targets (synthetic copies within the test spec) | affected-target search returns exactly `L` | step `enumeration = TRUNCATED`; `done_rev` not advanced; `full_requested` set; full recomputation completes the set; also: on Qdrant 1.15.1, `exact = true` + `quantization.ignore = true` scores equal the numpy float32 cosine to 1e-5 and `score_threshold` excludes 0.9499 | partial ⇒ never complete; the X flag itself (≥ 1 witness) is filter OK |
| L-15 | T payload `lowinfo = true` but record cleared; U payload `false` but record flagged | run tasks | T eligible (record decides); U eligible (payload false ⇒ no check) | fail-closed |
| L-16 | T flagged, witness live | Scylla unavailable for the witness read | task retries (`WAITING`), no exclusion and no DONE on an unknown | fail-closed |

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

### 9e. Capacity guard (design §13.3; corrected C-10)

Run on a small dev-scale instance with `index.max_points` set to a small test
value (e.g. 20,000) in a test keyspace — a test configuration of the same
code path, never the dev instance's value. Evidence: ledger rows,
`reserved_points` per generation, Qdrant exact counts per collection at every
step, blocked rows, metrics.

| # | Test | Pass condition |
|---|---|---|
| CAP-1 | UPSERT of a subject that would exceed `max_points − capacity_margin_points` | refused before any Qdrant call (proxy log empty); row `BLOCKED: capacity`; task `WAITING_INDEX(capacity)` |
| CAP-2 | counted states | points of CREATED…DRAINING and FAILED-with-collection generations are counted; RETIRED (deleted) are not; ledger ≥ Σ exact counts at every step |
| CAP-3 | rebuild admission with active + projected > limit | `CAPACITY_BLOCKED`; no collection or generation row created; active generation untouched |
| CAP-4 | second generation's bulk load and live writes | all draw on its reservation / the ledger; a write beyond them is refused (no bypass path) |
| CAP-5 | rebuild fails mid-build (kill + mark FAILED) | active generation serves; FAILED collection deleted ⇒ reservation released; ledger returns to the pre-rebuild value ± live growth |
| CAP-6 | promotion and retirement | ledger unchanged at promotion; decreases only after the old collection's deletion is confirmed; crash between reservation and write leaves the ledger high, the capacity audit never lowers it below the bound |
| CAP-7 | blocked subject recovery | after a release (retire a target), the reconciler applies the blocked row; task resumes; nothing lost |
| CAP-8 | configured value provenance | startup refuses a `max_points` above the last value recorded from an E-R1 report (report id in the ledger audit), except the provisional 250,000 |

## 10. Resource and performance (H)

**Before I-2: E-R1** (design §13.4) — the capacity-boundary experiment,
run by a harness on a dedicated `qdrant-e-r1` container (same image,
1.5 GiB, same collection configuration), never on the dev instance and
without any P10 guard override (the guard does not exist before I-2).
Resident points are counted exactly as the guard counts them — all
collections in the instance, both target generations during the rebuild
plus a resident 10,000-point media-role collection (C-12: per-collection
overhead of the two-role dev instance): 636,658 in phases 1–2,
**1,263,316 + 2 × live writes (≈ 1.30 M)** in phase 3, 636,658 + live after
deletion of g1; every 10th live write a re-UPSERT at a new `seq` with
cleanup; test ceiling `P_test` = 1,330,000.
Required assertions per phase: resident count = Σ exact Qdrant counts;
generation count; `oom_kill` = 0; `anon` peak ≤ 1.35 GiB; query p95 ≤ 10 ms
after warm-up (phases 2, 3, 5); `du` recorded; live-write latency recorded.
Outcome rules (design §13.4): `max_points` is set only from a measured full
pass (phase-3 resident total minus margin); a phase-3-only failure means one
generation fits but in-place rebuild does not, and the limit is set from a
smaller full pass; no failure can raise the limit. Until then
`index.max_points` stays **250,000 resident points** and the base target
scenario is not claimed for the dev host. Not run.


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
| P10-T11 | mutation/rebuild crashes R-1…R-25, reader/scan H-1…H-9, lowinfo L-1…L-16, identity K-1…K-8, capacity CAP-1…CAP-8 (§9a–§9e) | not run |

### 11a. Issue → test matrix (corrections C-1…C-12, design §19)

| Issue | Tests / measurements | Pass criterion | Blocks |
|---|---|---|---|
| C-1 resources | E-R1; §10 | no OOM, anon peak ≤ 1.35 GiB, p95 ≤ 10 ms | I-2; base target scenario on dev |
| C-2 lowinfo freshness | L-1…L-10; §3.4 | exhaustive-check equality; `lost_tp = 0` | enabling lowinfo (matching works without it) |
| C-3 mutation/rebuild | R-1…R-15; §5 | 0 stale/orphan/missing; equal manifests; identical decisions | I-2 exit |
| C-4 reconciliation ownership | H-1…H-8 | every item discovered within one backstop cycle; no cursor-before-work loss | I-3 exit |
| C-5 confidence | §3.3 | reliability acceptance or explicit UNAVAILABLE | publication of `match.found` |
| C-6 match key | K-1…K-8 | all pass | I-3 exit |
| C-7 V1 baseline | §2.1 | reproducible report exists | threshold selection, M3-1 |
| C-8 targets scan pagination | H-6, H-9a…h | no row present at cycle start left unvisited; cursor never inside a partition | D20 / I-2 exit |
| C-9 lowinfo freshness | L-1…L-16 (L-11…L-16 new); §3.4 unchanged | every fail-closed case eligible; partial never complete; `lost_tp = 0` | enabling lowinfo |
| C-10 capacity / E-R1 | E-R1 (§10); CAP-1…CAP-8 | E-R1 pass rules; guard never bypassed; limit only from a measured pass | I-2 |
| C-11 mutation guarantees | R-1…R-21 | common pass condition of §9a; no stale point accepted | D19 / I-2 exit |
| C-12 stale DELETE after newer UPSERT; task completion under index loss | R-22…R-25 (R-14, R-18 keep the stale-UPSERT direction) | current-`seq` points never absent after apply (poller); R-24 filter semantics on 1.15.1; no DONE on a suspect generation; repaired subject re-run finds the hidden match | D4/D19 / I-2 exit |

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
