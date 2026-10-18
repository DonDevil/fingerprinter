# P10 Validation plan — test matrix, methodology, acceptance criteria, evidence

Status: **IN EXECUTION (2026-10-18).** The plan below is unchanged; what has
actually been executed, with commands and measured outcomes, is recorded in
**§13** (execution record) and nowhere else — a test not listed there has not
run. §12 lists the audit measurements that preceded implementation (inputs
to the design, not P10 tests). Design:
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
semantics), R-25 (non-protocol point loss during matching; its "no DONE"
expectation is withdrawn by C-13); common pass condition extended; E-R1 adds a
resident media collection and re-UPSERTs. All **planned and unrun**.

Fourth correction 2026-10-17 (design §12.10, §19.1 C-13): the C-12 "no DONE
on a suspect generation" rule is replaced by the durable recovery protocol;
R-25 revised (loss before the search, apply-heal, DRAINING horizon,
unauditable fallback), **R-26 new** (the deterministic task-completion versus
detection race, §9a.1), R-24 expanded to every filter boundary the protocol
uses, E-R1 phase 3 sized to the admission requirement (§10). All **planned
and unrun**.

Fifth correction 2026-10-17 (design §19.2, C-14): **R-27 new** (repeated
loss of one subject — occurrence identity, §9a.2), **R-28 new** (detection
versus repair confirmation), R-26 d1 expectation corrected, R-4/R-10/R-20/
R-22/R-24/R-25 sharpened, V-1 (verifier independent of ANN, §5), G-1/G-10
expanded, G-15 (saturation), L-17 (per-hit exclusion), K-9…K-11, CAP-5/6
revised, CAP-9 (ledger only lowered by the audit); gates require
`recovery_open = 0` **and** `recovery_unresolved = 0`. All **planned and
unrun**.

Sixth correction 2026-10-17 (design §19.3, C-15): one `index_repairs` row
per occurrence; **R-29 new** (§9a.3: unresolved occurrences survive later
occurrences, v1–v13 incl. retired subject); R-27 revised to the
per-occurrence rows. Planned and unrun.

**Test classes and run status** (updated 2026-10-18; details and commands in §13).

| Group | Class | Environment | Run |
|---|---|---|---|
| V-1 (unit part), verifier determinism | unit | CPU, fixtures | **yes** — pass (§13.3) |
| §2, §3 (I-1 baseline, tuning, calibration, lowinfo oracle) | offline evaluation | vector cache, V1 handler | **yes** — §13.3, §13.4 |
| §4 ANN recall | benchmark | dev Qdrant | **yes** — §13.14 (partly fails) |
| §5–§8 | live-stack integration | test keyspaces, dev Qdrant, MinIO, Redis | **yes** — §13.6, §13.9–§13.15 |
| G-*, R-1…R-29, H-1…H-9, L-1…L-17, K-1…K-11, CAP-1…CAP-9 | live-stack fault injection / integration | as above; H-9 on populated Scylla 6.2.3 | **partly** — 75 tests pass (§13.6–§13.8, §13.12); unrun items listed in §11 |
| R-24 | version-specific behaviour test | Qdrant 1.15.1 pinned image, test collection | **yes** — pass (§13.1) |
| E-R1 | capacity benchmark | dedicated `qdrant-e-r1` container | **yes** — **fail** (§13.5) |
| G-14 | simulated two-host (two processes, one host) | dev host | **yes** — pass (two matcher instances in one process, §13.8) |
| real multi-host, network partitions between hosts | external | P14 | not in P10 |

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
- Decision sets for M3 (and for §5–§7) are taken only at `recovery_open = 0`
  **and** `recovery_unresolved = 0` (no open index-integrity occurrence or
  audit, and no `RERUN_INCOMPLETE`/`TASK_FAILED` close left unresolved,
  design §12.10); the obligation counts of the run are recorded in the
  report.

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

**V-1 — verifier independence from ANN (C-14e).** *Unit:* `verify()` takes
two artifacts, a parameter set and an exclusion set, and nothing else (no
hit lists, scores, ranks or generation); two processes on two devices'
CPUs produce bit-identical output for 50 L-PAIRS pairs. *Integration:* for
every labelled pair proposed by both directions, by exact search and by
the production ANN parameters, before and after a rebuild, the decision
(verdict, runs, score, confidence) is identical when the exclusion sets are
equal; any difference fails the test. Precondition: lowinfo disabled, or a
fixed exclusion set injected. Proves: decisions are reproducible from
stored provenance and ANN only proposes.

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
| G-1 index unavailable | stop Qdrant during A and B tasks; also: the proxy drops or delays the response of **one** segment query of a search step | tasks `WAITING_INDEX`, `index_unavailable_total` rises, health degraded, no decision, resume after restart, no duplicates; a single failed segment query fails the step (no `SEARCHED`, retried) and is never counted as zero hits |
| G-2 index loss | delete volume | §5 |
| G-3 corruption | delete a subset of points; alter a payload | `index check` reports missing/stale; `repair` fixes; queries skip stale |
| G-4 interrupted rebuild | kill -9 the builder mid-bucket | resume from the cursor; identical final counts |
| G-5 duplicate hints/events | replay `representation.ready` entries; duplicate `target.registered` | one task, one decision per `match_id` |
| G-6 duplicate match requests | `match retry` twice concurrently | one decision; the second is a no-op |
| G-7 concurrent indexers | two matchers index the same subject | identical points (deterministic ids) |
| G-8 stale entries | retire a target; retire a representation (test spec) | points removed; stale candidates dropped and counted |
| G-9 Scylla failure | pause Scylla during commits | typed retries; intents resolved by the reconciler; no lost or duplicated match |
| G-10 MinIO failure | stop MinIO during verification; corrupt one **candidate's** artifact; separately corrupt the **subject's own** artifact | retryable then `verification_failed`; a candidate mismatch → candidate FAILED `artifact_corrupt`, run INCOMPLETE `verification_failed`, alert, the subject's task not FAILED (an open obligation of the subject would close `RERUN_INCOMPLETE`, not `TASK_FAILED`); the subject's own mismatch → task FAILED + alert |
| G-11 worker restart | kill -9 matchers in each task state | lease expiry, resume, no lost work |
| G-12 rebuild + new representations | ingest during a build | new subjects in both generations before promotion |
| G-13 overload / timeout | tiny verification timeout; many candidates | per-candidate retries, INCOMPLETE `verification_failed`, bounded queue, nothing silently dropped |
| G-14 simulated two-host | two matcher processes, one Qdrant | no duplicates, lease takeover works (real machines: P14) |
| G-15 saturation (C-14g) | `k_max` lowered so that ≥ 1 query segment saturates, in directions A, B and media↔media | run INCOMPLETE `saturated_segments = n`; `match status` reports the subject's "no match" `provisional (incomplete: saturated_segments)`; never DONE |

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
= row.seq` during the test — S2); every injected **non-protocol** loss of a
live subject's current points in a searched generation has an
`index_repairs` occurrence (own `occurrence_id`) that was durable before the
first repairing write, and no occurrence reached CLOSED before a run of each
of the subject's task kinds numbered after **its own** repair had finished
(design §12.10, invariant R); at `recovery_open = 0` and
`recovery_unresolved = 0` every labelled pair the loss hid is matched. (The C-12
condition "no task reached DONE while the generation it searched was
suspect" is withdrawn: it is not a property of the design, see R-26.)
**Evidence recorded:** the
`index_entries` rows and generation rows before and after repair, Qdrant
point ids + payload `seq` of the affected subjects (scroll), reconciler
metrics, the injected-fault log, matches written during the test.

| # | Initial state | Injected fault / race | Expected state before repair | Expected repair | Pass condition |
|---|---|---|---|---|---|
| R-1 | subject S READY in P9, no row | crash before the intent LWT (also: LWT returns unknown) | no row; Qdrant unchanged (unknown ⇒ SERIAL read before any Qdrant call) | backstop scan writes the intent; apply | Qdrant never written before the row existed (hook asserts); converges |
| R-2 | as R-1 | crash after intent, before Qdrant | row PENDING, no points | intents pass applies | points = artifact; row APPLIED |
| R-3 | as R-1; also a DELETE variant | crash after Qdrant applied, before bookkeeping | Qdrant done, row PENDING | re-apply (idempotent), record | identical point set (ids, vectors); no duplicates |
| R-4 | two routed generations (ACTIVE + BUILDING) | crash after applying to one generation and its bookkeeping LWT | `applied` has one generation only; `state` still PENDING (C-14k) | intents/catch-up pass applies to the other | both generations hold S at `seq`; `state = APPLIED` only after both |
| R-5 | S indexed, then desired DELETE | Qdrant stopped mid delete-by-filter | some points left; row PENDING | delete by filter repeated | 0 points of S; no candidate from S accepted meanwhile |
| R-6 | rebuild in BUILDING, half the buckets done | kill -9 builder; new builder after lease expiry | generation BUILDING, cursor at bucket b | new holder adopts g, resumes from the cursor | same manifest digest as an uninterrupted build |
| R-7 | g in CATCHING_UP | (a) kill -9 the pass; (b) while the pass holds S at `seq = s`, a mutator writes s+1 (DELETE) and applies; then the pass resumes and lands its stale UPSERT and is killed before its LWT | (a) partial pass; (b) g contains a stale point of S | (a) pass restarts, completes a full pass; (b) the pass's LWT fails or VALIDATING's no-extras check finds S ⇒ back to CATCHING_UP; or verification removes it | g promoted only with S absent; no match from S |
| R-8 | g in VALIDATING | kill -9 during validation | g non-active | re-validated | never promoted on a partial validation |
| R-9 | g READY | crash immediately before the promotion LWT; live writes continue | g READY, still routed | revalidation if older than `revalidate_after_s`; promote once | exactly one ACTIVE; `active_epoch` +1 once |
| R-10 | promotion done | crash immediately after the LWT; a reader caches the old pointer; a search step that started on the old pointer is slowed (proxy delay) past `search_timeout_s` | old generation DRAINING | reconciler RETIRED after grace and the post-horizon pass | readers never see an empty pointer; old pointer searches succeed during grace; every query of a step goes to its pinned generation; no query is sent after the step deadline (proxy log) — the slow step is abandoned and repeated on the new pointer; no query reaches the old generation after `drain_horizon_at` (C-14i) |
| R-11 | old generation DRAINING → RETIRED | a worker holding a pre-retirement routing set writes to it after deletion | collection gone | none needed | write fails "not found", treated as no-op only because Scylla says RETIRED; no collection re-created; generation numbers not reused |
| R-12 | g just CREATED | live writes ≥ 1 subject/s mixed UPSERT/DELETE through the whole rebuild, plus a mutator held between its intent LWT and its routing read across CREATED, and one whose routing read follows CREATED | in-flight rows missing from g | catch-up (E1) / own mutator (E2) | every subject present in g at its newest `seq` at promotion; no extras |
| R-13 | build lease held by builder 1 | second `index rebuild`; then builder 1 pauses past its lease, builder 2 takes over, builder 1 resumes | — | builder 2 adopts or FAILs g | builder 1's transitions fail on `build_epoch`; one ACTIVE; its stale Qdrant writes found by no-extras/verification |
| R-14 | S indexed at seq s (UPSERT) | worker W holds an old UPSERT; a newer DELETE (s+1) completes; W lands its write and is killed before its LWT | row DELETE APPLIED; stale points of S in Qdrant | verification finds `seq` < row / present for DELETE; delete | while stale: queries that hit S drop it (authoritative check), `index_verify_stale_candidates_total` > 0, no match with S; after one cycle 0 points |
| R-15 | S PENDING | proxy drops the response after the server applied (also: times out before applying) | outcome unknown | retry applies the current desired state | no duplicate points; `attempts` incremented; permanent 4xx ⇒ BLOCKED with error, not retried |
| R-16 | g ACTIVE | insert a point with a valid-looking payload for a subject without a row (and one for a retired subject) | orphan point | verification: READY ⇒ intent + apply; else delete by id | orphan removed or adopted; never accepted as a candidate for a retired subject |
| R-17 | S row APPLIED | delete S's points directly in Qdrant | row APPLIED, points missing | verification count ≠ `segment_count` ⇒ obligation OPEN ⇒ re-apply ⇒ REPAIRED ⇒ run bump of S's own task(s) ⇒ CLOSED after the bumped run | points restored; `index_miss_total` +1; one obligation (occurrence 1) closed `RERUN_DONE`; S's pairs re-covered (task run number +1, `rerun_reason = index_repair`) |
| R-18 | S at seq s | (a) newer UPSERT s+1 (payload change) supersedes s while the s worker is paused; (b) the s worker's Redis lease expires and a second worker applies s+1, then the s worker resumes and writes | Qdrant may hold `seq = s` payload | the s worker's LWT fails ⇒ it re-applies s+1; if killed instead, verification re-applies | the stale `seq = s` write lands as **separate** points (ids contain `seq`) and never overwrites `seq = s+1` points (poller: their count stays `segment_count`); after cleanup only `seq = s+1` points remain; Redis lease shown not to fence (the write lands) |
| R-19 | S UPSERT APPLIED | DELETE s+1 written while an UPSERT retry of s is in flight | either | as R-14 | 0 points of S; DELETE row kept with `applied[g] = s+1` for all routed generations |
| R-20 | ACTIVE generations of both roles, matches recorded | delete the Qdrant volume | tasks `WAITING_INDEX`; the lost generations stay ACTIVE (no ACTIVE→FAILED) and every search fails loudly | `index rebuild` (target role first), validation, promotion; the old generations become DRAINING; unauditable-fallback occurrences written for every applied UPSERT row; then RETIRED | §5 acceptance: equal manifest digests, identical decisions; no fact lost; RETIRED only after the fallback occurrences were durable (C-14m); reruns rate-limited |
| R-21 | a verification pass has repaired part of a corrupted generation (R-16/R-17 injections) | kill -9 the reconciler mid-pass; restart | partially repaired | pass resumes from its persisted cursor | every injected divergence repaired exactly once in effect; repeating the pass changes nothing |
| R-22 | S live, row UPSERT `seq = 10`, Qdrant holds S's points with `seq = 10` (variants: g ACTIVE; g VALIDATING; g READY then promoted; promotion LWT executed while A is paused) | worker A reads DELETE 11 and is paused before its Qdrant call (hook); S becomes READY again ⇒ UPSERT 12 written; worker B applies 12 ((a) + cleanup) and records `applied[g] = 12`; A resumes and sends delete `subject = S AND seq ≤ 11`, its LWT fails. Sub-variants: (e) A is killed right after its Qdrant call; (f) the proxy drops A's response and A retries | Qdrant: all `segment_count` points of S with `seq = 12` present; no `seq ≤ 11` points; row UPSERT 12 APPLIED; A's LWT rejected (logged) | none needed; (f) A's retry re-reads the row and applies UPSERT 12 (idempotent) | scroll shows S's point ids = the 12-ids and payload `seq = 12` before and after A's DELETE; poller never saw fewer than `segment_count` `seq = 12` points; `index_miss_total` unchanged; validation of g in the VALIDATING/READY variants passes and promotion proceeds without re-validation being triggered by A; a Direction-A task for a clip of S run immediately after A's DELETE retrieves S as a candidate |
| R-23 | as R-22, UPSERT 12 applied | worker C holds UPSERT 10, worker A holds DELETE 11; after 12 is applied, C lands its UPSERT 10, then A lands DELETE 11; both killed before their LWTs | `seq = 12` points present throughout; `seq = 10` points created by C then removed by A's `seq ≤ 11` filter | none needed for the 12 points; any remaining older-`seq` points removed by verification | poller: `seq = 12` count = `segment_count` at every sample; final scroll: only `seq = 12` points; contrasts with the pre-C-12 protocol, where this sequence deletes S |
| R-24 | Qdrant 1.15.1 (pinned image), a test collection with **the production §4.2 parameters** (int8 `always_ram`, on-disk vectors, on-disk payload) and the §4.3 payload indexes (`rep`/`trep` keyword, `seq` integer) | points of subject S at `seq` ∈ {10, 11, 12}, of subject S′ at `seq` ∈ {5, 11, 12} (same keyword field, other value), and a `trep` variant collection; operations, each `wait=true`, each followed immediately by exact `count` per (subject, seq): (i) DELETE boundary — delete `must: [rep = S, seq range lte 11]`; (ii) cleanup boundary — on a fresh copy, delete `must: [rep = S, seq range lt 12]`; (iii) manifest / pre-count — `count must: [rep = S, seq match 12]`; (iv) the same on `seq` values near 2⁴⁰ (bigint `seq`); (v) (i)–(iii) before and after the segment is optimized/indexed (low `indexing_threshold` or enough points) — plain and indexed segments execute filters differently; (vi) (i)–(iii) with the `seq` integer index absent (recorded only, not the production configuration); (vii) retrieve the points of S with vectors and compare with the uploaded float32 bytes (C-14: the §12.5 content digest assumes a `Dot` collection returns stored originals bit-identically) | — | — | (i) exactly S's `seq` 10 and 11 deleted; S 12 and every S′ point remain; (ii) exactly S's 10 and 11 deleted, S 12 kept (strict `<`); (iii) count = 12-points only; (iv)–(v) identical results; (vi) recorded; (vii) bit-identical; every count exact immediately after the acknowledged write. **Any deviation ⇒ the §12.2 cleanup/DELETE protocol is revised before any other I-2 work.** Planned, unrun — the semantics are an assumption until this runs |
| R-25 | g ACTIVE (both roles), target T indexed at `seq = s`; media clip M of T (provenance true match) | (a) delete T's current points directly in Qdrant **before** M's search; M runs to DONE (no race — M finishes before verification starts); then a verification pass; (b) as (a), but before the verification pass T gets a new intent (re-UPSERT at `s+1`, e.g. `lowinfo_rev` change) whose apply would re-create T's points; (c) promote g′, then delete T's points from the now DRAINING g during its search horizon (a reader with a cached pointer searches g); (d) as (a), then delete g's collection before an audit/verification can count T | (a) M DONE, no (M, T) match; obligation OPEN (T, g); `suspect_since` set; (b) the apply's pre-count writes the obligation **before** its upsert (hook log order); (c) the post-horizon pass of g detects T; (d) fallback obligations for every `index_entries` row with `applied[g]` | obligation driven to CLOSED: repair (or g RETIRED), bump of T's `target_vs_media`, rerun | T's qualifying rerun confirms (M, T); no `no_match` row for (M, T); M's DONE untouched (run 1); obligation closed `RERUN_DONE` only after T's bumped run finished; `match_task_reruns_total{reason=index_repair}` = 1 per obligation; (c) g not RETIRED before the post-horizon pass completed; (d) obligations = applied subjects of g, all durable before g became RETIRED, reruns rate-limited and audited |
| R-26 | see §9a.1 | the deterministic race between a task's completion and loss detection | §9a.1 | §9a.1 | §9a.1 |
| R-27 | see §9a.2 | repeated loss of one subject in one generation; stale task-map entry retained; delayed worker of the first occurrence; unknown outcomes; restart | §9a.2 | §9a.2 | §9a.2 |
| R-29 | see §9a.3 | an occurrence closed unresolved, then later occurrences of the same (subject, generation); crashes, unknown outcomes, duplicate and delayed reconcilers, restart, settlement probes | §9a.3 | §9a.3 | §9a.3 |
| R-28 | occurrence OPEN for (T, g); repairer has written T's points, read `detections = d` and taken its confirming count (hook `P7 reconciler.after_confirm_before_repaired`) | delete T's points again at P7, then: (a) a verification pass merges (`detections = d + 1`) before the repairer's `repaired` LWT; (b) the pass counts T short but its merge is held until after `repaired` lands; (c) as (b) with the merge's response dropped | (a) OPEN, `detections = d + 1`; (b) REPAIRED, merge rejected; (c) unknown | (a) `repaired` fails on `detections`, repair repeated, REPAIRED with a new `:d`; (b) the detector opens the next occurrence (a new row); (c) SERIAL read shows the row past OPEN ⇒ open the next occurrence | no occurrence is REPAIRED while a loss recorded after its `:d` read is unrepaired; in every variant a run of T's `target_vs_media` numbered after the **final** REPAIRED finishes before the last occurrence closes; a media clip ingested and searched during the second loss is matched by that run (C-14b) |

R-22's DELETE-then-UPSERT sequence of a live subject is **injected** by the
test: P9 liveness is monotone (design §4.2), so P9 paths do not produce it
today; S2 must hold without relying on that (C-14q).
R-22/R-23 (stale **mutation** races: an older operation landing after a
newer one, in ACTIVE/VALIDATING/READY/promotion interleavings) and R-24
(filter semantics) test the S2 property — that the protocol never removes
current points. R-25 and R-26 test what happens when points are removed by
**something outside** the protocol: R-25 the non-racing cases, R-26 the
completion/detection interleaving. They are separate tests with separate
fault hooks.

### 9a.1 R-26 — deterministic task-completion versus detection race (design §12.10)

**Purpose.** Exercise exactly the interleaving that broke the C-12 rule
(design §12.10, T1–T5) and show that, under the selected protocol, no task
escapes revalidation and no obligation is lost or closed early.

**Fixture.** Test keyspace, dev Qdrant, generations g (target role) and h
(media role) ACTIVE; target T indexed and APPLIED at `seq = s` (its
`target_vs_media` task DONE at run 1); media clip M whose provenance says it
matches T; M's `media_vs_targets` task. Fault hooks (test build only, no-ops
in production): `P1 matcher.before_completion_lwt(task)` — **after** the
run's last read of anything (verification of the last page done; this is
also where C-12 read `suspect_since`) and **before** the completion LWT;
`P2 verifier.after_count_before_obligation(S, g)`; `P3
reconciler.after_repair_write`; `P4 reconciler.after_bump_before_record`;
`P5 matcher.before_completion_lwt` for T's own run (R-26b). Every hook logs a
monotonic timestamp; Scylla rows and Qdrant counts are sampled every 100 ms.

**Procedure (R-26a — another subject's task races detection).**
1. Delete T's current points from g directly (non-protocol loss).
2. Start M's run 1; it searches g (T absent), verifies its other candidates
   and **blocks at P1**.
3. Run one verification pass of g. It counts T short and writes the
   obligation `index_repairs(T, g)` (OPEN, occurrence 1) and the generation
   mark (`detect_count` + 1, `suspect_since`). The hook log must show the
   obligation LWT acknowledged before the pass cursor moves past T.
4. Release P1. M's completion LWT executes.
5. Observe and record: M's task row (state, `run`, `done_run`), the
   `matches` rows for M, the `index_repairs` row, g's generation row,
   `match status` for M and the spec.
6. Block the reconciler's repair write (Qdrant proxy returns 503): the
   obligation stays OPEN with `attempts` > 0 and `last_error`; it is not
   closed or deleted.
7. Unblock; the reconciler re-applies T in g, confirms the exact count, sets
   REPAIRED; then bumps T's `target_vs_media` (run 1 → 2,
   `recovery_runs[oid₁] = 2`, keyed by the occurrence id) and records
   `required_run`, RERUN_PENDING.
8. Hold T's run 2 before its search; run the reconciler twice: the
   obligation stays RERUN_PENDING (`done_run` 1 < 2).
9. Release T's run 2: it searches h, retrieves M, verifies on the artifacts,
   commits (M, T), completes (`done_run = 2`); the reconciler closes the
   obligation `RERUN_DONE`; after a clean verification pass that started
   after the detection, `suspect_since` is cleared.

**Crash and uncertainty variants** (each a separate run of steps 1–9):
c1 kill the verifier at P2 → restart re-counts T and writes the obligation
(T was not healed meanwhile); c2 the proxy drops the obligation LWT's
response after Scylla applied it → SERIAL read, one row, occurrence 1; c3
kill the reconciler at P3 → OPEN, repair re-applied idempotently; c4 kill it
at P4 → the next pass reads `recovery_runs[oid₁] = 2` from T's row and
records it — `run` is 2, never 3; c5 the bump LWT times out → SERIAL read
resolves it; c6 kill T's run-2 worker mid-VERIFYING → lease expiry, run 2
resumes and closes the obligation; c7 restart every P10 process between
steps 7 and 8 → the `repairs` pass resumes from Scylla alone.

**Duplicate variants.** d1 two verifier passes run concurrently at step 3 and
a third afterwards → one obligation row, occurrence 1 (the losers merged,
`detections` ≥ 1), `gen_marked = true`, `detect_count` increased by ≥ 1 (a
crash between the mark and `gen_marked` may raise it twice; the promotion
fence only needs an increase — C-14p); d2 two reconcilers at steps 7–9 → one bump (T's
`run` = 2), one (M, T) match row, one held envelope.

**R-26b — the lost subject's own run races the bump (the real fence, S5).**
Re-create the obligation as in steps 1–3, but with T's own run 1 re-opened
(audited `match rerun`) and blocked at P5 before its completion LWT; then let
the reconciler repair and bump. Order (i) bump first: T's run-1 completion
LWT fails on `run`; the worker re-reads and stops; run 2 executes. Order (ii)
release P5 first: run 1 completes, the bump re-opens the row as run 2. Order
(iii) T's row PENDING and unclaimed (`run_started_at = null`) at bump time:
coalesced, no extra run. In every order the obligation requires a run
numbered after REPAIRED and is closed only by it.

**Expected outcomes.**

| Check | Strict generation-health fence (Option A — rejected, reference only) | Selected protocol: durable revalidation (design §12.10) |
|---|---|---|
| M's completion at step 4 | rejected by a fence condition; M → WAITING, later re-run | **succeeds**: M run 1 DONE (expected, not a failure) |
| M's result | no result until the rerun | no (M, T) match yet; M's "no match" reported `provisional (recovery pending)`; nothing published or stored as a negative |
| obligation | — (not part of Option A; tasks completed before marking remain uncovered) | `index_repairs(T, g)` OPEN at step 3, durable before cursor advance and before any repair write |
| generation | suspect, completions blocked | `detect_count` + 1, `suspect_since` set; **no task gated** |
| after the repair | M re-runs | T's `target_vs_media` bumped to run 2; M's row untouched |
| closure | when M's rerun finishes | only after T's run 2 finishes (`done_run ≥ required_run`); never at step 6 or 8 |
| pair (M, T) | found by M's rerun | found by T's run 2; at `recovery_open = 0` and `recovery_unresolved = 0` the decision set equals a no-loss run |

**Pass condition (selected protocol).** The hook log proves M was blocked at
P1 **before** the verifier's count and completed **after** the obligation
was durable (a run where M completes before the verifier starts is invalid
and fails the test); M's completion succeeded; at no sample was the
obligation absent after step 3 or CLOSED before T's run 2 finished; T's
`run` increased exactly once per obligation occurrence; (M, T) is confirmed;
every crash and duplicate variant ends in the same final state (one
obligation CLOSED `RERUN_DONE`, one (M, T) row, T at run 2, M at run 1);
`index_repairs_blocked` raised in step 6 and cleared after. Planned, unrun.

### 9a.2 R-27 — repeated loss of one subject: occurrence identity (design §12.10, C-14a; revised C-15)

**Purpose.** Prove that a second loss of the same (subject, generation)
requires a genuinely new rerun even when the first occurrence's task-map
entry is still on the task row, and that no worker of the first occurrence
can change or close the second. R-26 (another subject's completion races
detection) and R-26b (the subject's own completion races the bump) test
different properties and stay separate.

**Fixture.** As R-26: generations g (target role) and h (media role)
ACTIVE; target T APPLIED in g at `seq = s`; T's `target_vs_media` at run 1
DONE; media clips M₁ and M₂ whose provenance says each matches T (M₂ not yet
ingested). Additional hooks: `P6 reconciler.before_close(occurrence)`; `P8
reconciler.before_bump(occurrence, kind)`; the test build disables any
optional removal of `recovery_runs` entries (step C). Rows sampled every
100 ms; every LWT, its condition and its `[applied]` result logged.

**Procedure.**
- **A.** Delete T's current points from g. Run M₁'s `media_vs_targets` to
  DONE (no (M₁, T) match). A verification pass opens occurrence 1:
  `occurrence = 1`, `occurrence_id = oid₁`.
- **B.** Reconciler R₁ repairs, REPAIRED, bumps T run 1 → 2
  (`recovery_runs[oid₁] = 2`), RERUN_PENDING (`required_run = {target_vs_media:
  2}`); run 2 finds (M₁, T) and finishes DONE; R₁ closes `RERUN_DONE`.
  Meanwhile a second reconciler instance W₁ (lease taken over by test
  control) read occurrence 1 in RERUN_PENDING and is **held at P6**; a third
  worker W₂ read occurrence 1 in REPAIRED and is **held at P8** (before its
  bump).
- **C.** Assert T's task row still holds `recovery_runs[oid₁] = 2` (retained
  deliberately for the rest of the test).
- **D.** Delete T's current points from g again. Ingest M₂ through the P9
  path; its `media_vs_targets` indexes M₂ in h, searches g (T absent) and
  finishes DONE with no (M₂, T) match.
- **E.** A verification pass reads the latest occurrence of (T, g)
  (occurrence 1, CLOSED) and inserts **a new row** `occurrence = 2`,
  `occurrence_id = oid₂ ≠ oid₁`, with every column at its initial value
  (`state = OPEN`, `detections = 0`, `gen_marked = false`, `required_run =
  {}`, `resolution = PENDING`, `resolved_by = {}`, nulls, `attempts = 0`);
  assert row 1 is byte-for-byte unchanged (CLOSED, `RERUN_DONE`, RESOLVED).
- **F.** R₁ marks, repairs and sets REPAIRED for occurrence 2.
- **G.** R₁'s bump step reads T's row: `recovery_runs` has `oid₁` but not
  `oid₂` ⇒ bump run 2 → 3 with `recovery_runs[oid₂] = 3`; `required_run =
  {target_vs_media: 3}`; RERUN_PENDING. Hold run 3 before its search; run the
  `repairs` pass twice: the occurrence stays RERUN_PENDING (`done_run = 2 <
  3`).
- **H.** Release W₁ (close of occurrence 1) and W₂ (bump for occurrence 1,
  `IF run = 1`).
- **I.** Expected: W₁'s `close … IF occurrence_id = oid₁ AND state =
  'RERUN_PENDING'` on row 1 is rejected (row 1 is already CLOSED); it can
  address only row 1, never row 2; W₂'s bump is rejected (`run` is 3); both re-read and
  abandon (W₂'s re-read of T's row finds `recovery_runs[oid₁]` present, so
  it makes no bump; its `rerun pending` for occurrence 1 is rejected on the
  occurrence condition); no column of the row or of T's task changes
  because of them. (Even a bump by a stale worker could only move the run
  forward — one extra run, never an early close.)
- **J.** Uncertainty and restart, each a separate run of A–K: j1 the
  occurrence-2 INSERT's response is dropped after Scylla applied it ⇒ the
  SERIAL read of row 2 shows `occurrence_id = oid₂` (the detector's own) ⇒
  no row 3; j2 the bump LWT for `oid₂` times out ⇒ SERIAL read
  shows `recovery_runs[oid₂] = 3` ⇒ recorded, `run` never 4; j3 kill R₁
  between REPAIRED and the bump and restart every P10 process ⇒ the
  `repairs` pass resumes from Scylla and bumps exactly once; j4 the `close`
  LWT for occurrence 2 times out ⇒ SERIAL read of row 2 decides; rows 1
  and 2 only.
- **K.** Release run 3: it searches h, finds M₂ (and M₁, skipped as
  `already_decided`), commits (M₂, T), `done_run = 3`, `done_state = DONE`;
  the reconciler closes occurrence 2 `RERUN_DONE`.

**Expected final state (every variant).**
`index_repairs` for (T, g): exactly two rows — occurrence 1 (`oid₁`,
CLOSED `RERUN_DONE`, RESOLVED, `resolved_by = {target_vs_media: 2}`) and
occurrence 2 (`oid₂`, CLOSED `RERUN_DONE`, RESOLVED, `resolved_by =
{target_vs_media: 3}`); T's task: `run = 3`, `done_run = done_ok_run = 3`,
`recovery_runs = {oid₁: 2, oid₂: 3}`; one `matches` row each for (M₁, T) and (M₂, T); M₁ and M₂ at
run 1, untouched; `recovery_open = 0`, `recovery_unresolved = 0`.

**Unresolved-close variants.** u1: force T's run 3 INCOMPLETE (`cap_B_total =
1`) ⇒ CLOSED `RERUN_INCOMPLETE`, `recovery_unresolved = 1`; `match status`
reports an unrelated media subject's "no match" `provisional (recovery
unresolved)`; then `match retry --raise-cap` ⇒ run 4 DONE ⇒ `resolve`
(`resolution_reason = LATER_RUN`, `resolved_by = {target_vs_media: 4}`) ⇒
`recovery_unresolved = 0` and the same status reads
`settled`. u2: corrupt T's own artifact before run 3 ⇒ T's task FAILED ⇒
`TASK_FAILED`, unresolved, alert.

**Pass condition.** Occurrence 2 never reaches CLOSED while `done_run < 3`;
its `required_run` is never 2; no LWT conditioned on `oid₁` is applied after
step E; row 1 is never modified after step B; every variant ends in the
stated final state; with the C-13 protocol
(`obligation_id` reused) the same script closes occurrence 2 at step G
without run 3 — the test must be able to show that difference (run once
against a protocol stub of C-13 as a negative control). Class: live-stack
fault injection. Planned, unrun.

### 9a.3 R-29 — unresolved occurrences survive later occurrences (design §12.10, C-15)

**Purpose.** Prove that an occurrence closed unresolved stays counted, keeps
its own requirements and is resolved only by runs that satisfy them,
whatever happens to later occurrences of the same (subject, generation);
and that `settled` is never reported while any occurrence row is
unresolved. R-26 (another subject's completion vs detection), R-26b (own
completion vs bump), R-27 (occurrence identity) and R-28 (detection vs
repair confirmation) test different properties and stay separate.

**Fixture.** As R-27, with a media subject S (kinds `media_vs_targets` and,
where stated, `media_vs_media` with equivalence enabled) in generation h, and
the target T of R-27 in g; an unrelated media subject X whose last run is
DONE (its "no match" is the settlement probe). Hooks additionally:
`P9 reconciler.after_close_before_resolve(oid)`, `P10
reconciler.before_resolve(oid)`. Every LWT with its condition and result is
logged; `index_repairs` rows, task rows and the two counters (recomputed by
a full scan, not read from gauges) are sampled every 100 ms.

**Common steps.** Loss 1 of the subject ⇒ occurrence 1 (`oid₁`) repaired,
bumped (`required_run` recorded), its rerun forced to end as the variant
says, occurrence 1 CLOSED. Loss 2 ⇒ occurrence 2 (`oid₂`, new row) repaired,
bumped, rerun, closed.

| # | Variant | Expected durable state | Pass / fail |
|---|---|---|---|
| v1 | T: occ 1 bumps run 1 → 2 (`required_run = 2`); run 2 fails non-retryably (T's own artifact made unreadable) ⇒ T's task FAILED ⇒ occ 1 CLOSED `TASK_FAILED`, `resolution = PENDING`. Loss 2 opens occ 2 (new row) while T is FAILED; its repair completes; the artifact is restored and an audited `match retry` creates run 3 **before** occ 2's bump, so occ 2 coalesces on run 3 (`required_run = 3`); run 3 DONE; the `P10` hook holds occ 1's resolve while occ 2 closes | occ 2 CLOSED `RERUN_DONE`, RESOLVED by run 3; occ 1 still CLOSED `TASK_FAILED`, PENDING while held; after release occ 1 RESOLVED (`LATER_RUN`, `resolved_by = {target_vs_media: 3}`, 3 ≥ 2) | `recovery_unresolved` = 1 from occ 1's close until occ 1's **own** resolve LWT — including the whole interval in which occ 2 is open and after it is resolved; X never `settled` in that interval |
| v1′ | as v1 without the retry: occ 2's bump finds T FAILED (required 3) and closes `TASK_FAILED` | both rows CLOSED `TASK_FAILED`, PENDING | `recovery_unresolved` = 2 indefinitely; alert; X `provisional (recovery unresolved)` until a retry produces a DONE run ≥ 3 |
| v2 | T: occ 1's run 2 INCOMPLETE (`cap_B_total = 1`); occ 2 bumps run 3 (cap restored) DONE | occ 1 CLOSED `RERUN_INCOMPLETE`, then resolved by run 3 (`LATER_RUN`); occ 2 CLOSED `RERUN_DONE` | occ 1's row is present and PENDING from its close until the resolve; never removed or rewritten by occ 2's open or close |
| v3 | as v2: one later DONE run (3) satisfies both occurrences | each row resolved by its **own** resolve/close LWT, exactly once (`IF resolution = 'PENDING'`), both recording run 3 | exactly one applied resolve per row in the LWT log; duplicate attempts rejected |
| v4 | S (media, equivalence enabled): occ 1 requires `media_vs_targets` and `media_vs_media`; its `media_vs_media` rerun INCOMPLETE ⇒ occ 1 `RERUN_INCOMPLETE`; equivalence then disabled; occ 2 requires only `media_vs_targets` and closes `RERUN_DONE` | occ 2 RESOLVED; occ 1 stays PENDING (its `media_vs_media` requirement unsatisfied) | `recovery_unresolved` = 1; resolved only after an audited `match rerun --kind media_vs_media` of S finishes DONE at a run ≥ the requirement |
| v5 | crash of the detector right after occurrence 2's INSERT applied, before its cursor advance and before the mark | row 2 OPEN, `gen_marked = false` | restart: the detector re-counts, reads the latest occurrence (row 2, OPEN) and merges — no row 3; the reconciler marks and proceeds. (The head/record split of a two-row design does not exist here: the INSERT is the only write that creates an occurrence.) |
| v6 | crash of the reconciler after occ 1's close LWT (unresolved) and before anything else; crash after a resolve LWT applied, before its metric/audit write | row 1 CLOSED PENDING; resp. RESOLVED | restart re-derives everything from rows: the pending row is re-checked, the resolved row is not touched again; counters recomputed from rows are unchanged by the restart |
| v7 | the resolve LWT's response dropped after Scylla applied it; the open INSERT for occ 2 times out | SERIAL read shows RESOLVED (resp. row 2 with `oid₂`) ⇒ treated as applied; no second resolve, no row 3 | one applied LWT each |
| v8 | two reconcilers resolve occ 1 concurrently (leases split by test control) | one resolve applied, the other rejected on `resolution = 'PENDING'`, re-reads, no-op | identical final rows |
| v9 | a reconciler that read occ 1 as RERUN_PENDING before its close is held; occ 2 opens, closes; the held worker resumes and issues occ 1's close | its LWT acts on row 1 only: applied if row 1 is still RERUN_PENDING (valid — occ 1 is not superseded), rejected otherwise; row 2 unaffected | no write to row 2 by any actor holding `oid₁` |
| v10 | three subjects, five occurrences, two of them CLOSED PENDING in different buckets; kill and restart every P10 process | after restart the `repairs` pass finds both pending rows from the table alone and continues | counters equal before and after restart; no row lost or duplicated |
| v11 | settlement probe for X while one historical occurrence (occ 1 of T) is PENDING and T's latest occurrence is RESOLVED | — | X reported `provisional (recovery unresolved)`; never `settled` |
| v12 | then occ 1 is resolved (v2's run 3); X's own last run DONE; no open occurrence | `recovery_open = 0`, `recovery_unresolved = 0` | X reported `settled` only from the first scan after the resolve LWT; and still `provisional` in a sub-variant where X has a newer run pending (`run > done_run`) |
| v13 | T has occ 1 CLOSED `TASK_FAILED`; T is retired (all versions RETIRED) | resolve with `resolution_reason = SUBJECT_RETIRED` (C-15b) | `recovery_unresolved` returns to 0 only through that LWT; T's tasks stop; the retirement is visible in the row |

**Negative control.** The same script against a stub of the C-14 protocol
(one row per (S, g), reset on re-open, history text) must report
`recovery_unresolved = 0` in v1/v2 after occurrence 2 closes — the test must
show that difference. Class: live-stack fault injection. Planned, unrun.

### 9a.4 Open requirement — serial slice read of the latest occurrence (design §12.10)

Not yet a numbered test; no evidence exists. Design §12.10 reads the latest
occurrence of (S, g) with `SELECT … WHERE spec_digest = ? AND role = ? AND
bucket = ? AND subject_id = ? AND generation = ? LIMIT 1` at LOCAL_SERIAL on
`index_repairs` (CK `subject_id, generation, occurrence`, clustering order
`occurrence DESC`). Before the implementation relies on that read, a test on
Scylla 6.2.3 (tablets off, as `db.py` configures the keyspace) must show:
1. the statement is accepted **and executed** at LOCAL_SERIAL on that schema;
2. it returns the row with the highest `occurrence` of (S, g), including
   immediately after an acknowledged `INSERT … IF NOT EXISTS` of a new
   occurrence and after an LWT whose outcome was unknown to its client;
3. its result is sufficient for §12.10: a detector acting on it never
   creates a second row for one occurrence number, never skips a number and
   never merges into an occurrence past OPEN (every such step is still
   guarded by its own single-row LWT condition).
If any of 1–3 fails or cannot be shown, the implementation uses the
documented walk (single-row `INSERT … IF NOT EXISTS` upward from occurrence
1) instead. The walk itself is also untested.

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
| L-17 | T₁'s segment *a* flagged (usable); T₂'s segment *b* unflagged, cos(*a*, *b*) < 0.95; a media clip of T₂ whose segment *m* has cos(*m*, *a*) > cos(*m*, *b*) ≥ floor (synthetic test-spec vectors) | direction A task | the hit on *a* does not vote; the hit on *b* votes; T₂ becomes a candidate exactly as in the unfiltered path (C-14h) | filter OK, no recall loss |

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
| K-9 | the same pair verified twice under one technique version with different exclusion sets (a witness retired between the runs), plus an A/B race on one pair | one `matches` row = the first committed decision; `match_decision_divergence_total` + 1 and an audit entry with both exclusion digests when the results differ; the second candidate otherwise skipped as `already_decided` (C-14f) |
| K-10 | `match publish-held` run twice concurrently; crash between the envelope LWT and the stream write; `publisher republish` afterwards | `confidence`/`calibration_id` written once (`IF calibration_id = null`); one `event_id` and one stored envelope per MatchId; re-publication reuses it |
| K-11 | liveness predicate (design §4.2) over every P9 state combination: representation READY/RETIRED; binding PENDING/READY × target REGISTERING/READY/BUILD_FAILED/RETIRED/PURGED | desired state, candidate acceptance, witness check, backstop and repair return the same verdict for each combination (one function); a READY binding with REGISTERING or BUILD_FAILED counts as live (C-14j) |

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
| CAP-5 | rebuild fails mid-build (kill + mark FAILED) | active generation serves; after the FAILED collection's deletion is confirmed, the next capacity audit removes its holder row and lowers the ledger to the pre-rebuild value ± live growth; nothing else lowers it |
| CAP-6 | promotion and retirement | ledger unchanged at promotion; decreases only by an audit after the old collection's deletion is confirmed; crash between reservation and write leaves the ledger high; the audit never lowers it below the bound (recomputed at every 100 ms sample from exact counts + holder rows + pending rows) |
| CAP-9 | release idempotency and audit races (C-14l): kill a worker after its cleanup and let it re-run; drop the response of a reservation CAS; run audits continuously while live writes, cleanups and a rebuild admission proceed; crash after the admission batch, before the CREATED row | ledger ≥ Σ exact counts of all collections at every sample; no path other than the audit decrements `reserved` (statement log); a duplicate reservation is removed by the next audit; the orphan holder row is kept until the build lease expired, then removed; the admission batch's static condition and clustering insert apply together or not at all on Scylla 6.2.3 |
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
1,263,316 + 2 × live writes in phase 3, which **continues with live writes
after the build until resident ≥ `R_req` = 1,304,649** (the rebuild-admission
requirement: resident before the rebuild + ⌈1.05 × 626,658⌉ projected +
10,000 margin — design §13.4, corrected C-13), 636,658 + live after
deletion of g1; every 10th live write a re-UPSERT at a new `seq` with
cleanup; test ceiling `P_test` = 1,330,000.
Required assertions per phase: resident count = Σ exact Qdrant counts;
generation count; `oom_kill` = 0; `anon` peak ≤ 1.35 GiB; query p95 ≤ 10 ms
after warm-up (phases 2, 3, 5); `du` recorded; live-write latency recorded.
Outcome rules (design §13.4): `max_points` is set only from a measured full
pass — the phase-3 resident total, which is ≥ `R_req`; the margin is
subtracted once, by the guard at every check, not from the measurement, so
the guard admits exactly the measured rebuild; a phase-3-only failure means
one generation fits but in-place rebuild does not, and the limit is set from
a smaller full pass (`R_req` 662,325 at 3 copies, 448,217 at 2, 234,109 at 1);
no failure can raise the limit. Until then
`index.max_points` stays **250,000 resident points** and the base target
scenario is not claimed for the dev host. Not run.


Measured and reported (each run alone on the host): index build time and
peak memory per role; query latency distribution; recall@k; Qdrant memory
(steady, peak) and disk; incremental upsert latency (one representation);
rebuild duration on the 104 k corpus with gRPC and 4 parallel readers, and the
extrapolation to the §8 scenarios (labelled extrapolated); verification time
per candidate by duration; contention: matcher + Qdrant + encoder + stack
together (host `MemAvailable` floor, encoder throughput change).

## 11. M3 exit checklist (filled 2026-10-18 from §13)

| Item | Criterion | Status | Evidence |
|---|---|---|---|
| M3-1 | P/R ≥ V1 (stronger baseline), held-out, zero FP (§2) | **PASS** on the measured populations | live candidate path, nested held-out: L-PAIRS P 1.000 / R 0.733, **0 FP** vs V1 R 0.247 (§13.10); pooled with N-SOAK P 0.9989 / R 0.812 vs V1-on-V2 P 0.9857 / R 0.449 (§13.13); the V1 *end-to-end* baseline exists for L-PAIRS only (§13.4) |
| M3-2 | new target matched against historical media without re-encoding (§7) | **PASS** (with a caveat) | `test_flow` (0 new encode claims); live 19x direction-B candidate recall vs provenance 0.997; caveat: direction-B runs end INCOMPLETE (saturated) at 10x/19x, and release of envelopes is shown only with a test-only calibrated version (2.1 is held) |
| P10-T1 | ANN recall@k (§4) | **FAIL** | segment recall@100 0.981–0.999 (pass); single-query p95 latency size-dependent, 40 ms plateau (fail); direction-B media-level candidate recall vs exact 0.74–0.99 while saturated (fail) — §13.14 |
| P10-T2 | drop-and-rebuild identical decisions (§5) | **PASS for the executed scope** | equal manifest digests, identical decisions, all re-proposed (§13.6, §13.10/§13.16); **not run:** the exact-search decision configuration and the +200 SOAK corpus of §5 |
| P10-T3 | growth bounds + effectiveness (§6) | **PASS (bounds, recall), with direction B INCOMPLETE** | §13.15 |
| P10-T4 | level-5 linking (§8) | **FAIL** (recall target) | 228/300 linked (target 1.0), 0 false links, revocation durable — §13.9, §13.11 |
| P10-T5 | failures (§9) | **PARTIAL** | G-1, G-1b, G-4, G-9, G-10, G-11, G-12, G-14, G-15 pass; G-2 via §5; **not run:** G-3, G-5…G-8, G-13 |
| P10-T6 | resources (§10) | **FAIL** | E-R1 failed (latency) — §13.5 |
| P10-T7 | V1 end-to-end baseline reproducible (§2.1) | **PASS (L-PAIRS)**; N-SOAK not run (cost) | §13.4 |
| P10-T8 | calibration status and reliability (§3.3) | **run → UNAVAILABLE** (reliability check fails) | §13.3; publication held |
| P10-T9 | lowinfo vs unfiltered oracle, `lost_tp = 0` (§3.4) | **FAIL** (`lost_tp` = 1; 12 flagged) → not enabled | §13.3 |
| P10-T10 | E-R1 capacity (design §13.4) | **FAIL** | §13.5 |
| P10-T11 | mutation/rebuild crashes R-1…R-29, reader/scan H-1…H-9, lowinfo L-1…L-17, identity K-1…K-11, capacity CAP-1…CAP-9, V-1, G-15 | **PARTIAL** | passed: R-2…R-6, R-11…R-13, R-16, R-17, R-20, R-22, R-23, R-25 a, R-26 a/b, R-27, R-28, R-29 u1/v2, H-1…H-5, H-9, L-1, L-4, L-6, L-12, L-13, L-16, L-17, K-1, K-2, K-6, K-8…K-11, CAP-1, CAP-3, CAP-5 (live), V-1 unit, G-15, §9a.4; **not run:** R-1, R-7…R-10, R-14, R-15, R-18, R-19, R-21, the c*/d*/j*/v* variants, H-6…H-8, L-2, L-3, L-5, L-7…L-11, L-14 (lifecycle part), L-15, K-3…K-5, K-7, CAP-2, CAP-4, CAP-6…CAP-9 scenarios, V-1 integration across two hosts |

**M3: NOT PASSED.** Gates that failed on measurement: P10-T1 (latency and
saturated direction-B candidate recall), P10-T4 (equivalence recall),
P10-T6/T10 (E-R1), P10-T9 (lowinfo not adoptable); P10-T8 leaves
publication held; P10-T5/T11 are partly unrun. M3-1 and M3-2 hold on the
measured populations. Real multi-machine validation remains P14.

### 11a. Issue → test matrix (corrections C-1…C-15, design §19)

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
| C-12 stale DELETE after newer UPSERT | R-22…R-24 (R-14, R-18 keep the stale-UPSERT direction) | current-`seq` points never absent after apply (poller); R-24 filter semantics on 1.15.1 (all boundaries) | D4/D19 / I-2 exit (R-24 first) |
| C-13 task completion versus loss detection; DRAINING horizon; witness cosine; E-R1 sizing | R-25, R-26 (§9a.1); L-1, L-7 (witness confirmed on artifacts); E-R1 phase 3 ≥ `R_req` | obligation durable before repair/cursor advance; never closed before a post-repair run of the lost subject; M's DONE allowed and the hidden pair found by the lost subject's rerun; all crash/duplicate variants converge; guard admits the measured rebuild | D19/D20 / I-3 exit (R-26); I-2 (E-R1) |
| C-14 occurrence identity; detection vs repair; unresolved closes; verifier independence; decision immutability; saturation; per-hit exclusion; search pinning; liveness; capacity release; lost generations | R-27, R-28, R-26 (d1), R-4, R-10, R-20, R-25(d); V-1; G-1, G-10, G-15; L-17; K-9…K-11; CAP-5, CAP-6, CAP-9; R-24 (vii) | a second occurrence never closes on a run older than its repair; no stale-occurrence LWT applies; settled negatives only at `recovery_open = recovery_unresolved = 0`; identical decisions across directions/generations/exact vs ANN; ledger never below resident points | D19/D20 / I-3 exit (R-27, R-28); I-2 exit (CAP-9, R-24 vii); M3 (V-1) |
| C-15 unresolved occurrence history; retired subject with an unresolved occurrence | R-29 v1–v13; R-27 (revised) | an unresolved occurrence is never removed, reset or resolved by another occurrence; `recovery_unresolved` falls only by an occurrence's own resolve; `settled` never while any occurrence row is PENDING | D19/D20 / I-3 exit |

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

## 13. Execution record (implementation phase, from 2026-10-18)

Operator authorization (2026-10-18): implement the current design and its
P10-D1…D20 proposals as documented, subject to every gate in this plan; no
change to crawler2, frozen contracts, P8/P9 behaviour or V1 (decisions.md
§0). Results below are **executed** and labelled by class; anything not
listed here is still planned and unrun. Commands assume
`cd ~/anti_piracy/fingerprinter`; integration commands first `. scripts/p10-env.sh`
(keyspace `fingerprinter_it_p10`, dev Qdrant `deploy/qdrant/compose.yaml`).
Small result JSONs are in `benchmarks/p10-index/results/` (tests write theirs
to `results/evidence/` when `P10_EVIDENCE_DIR` is set); per-pair logs and
samples are outside git in `~/anti_piracy/datasets/p10/runs/`.

Environment: Scylla 6.2.3 single node (crawler2 compose, tablets off),
Qdrant 1.15.1 (image `sha256:d122138f…a4c6`, 1.5 GiB hard limit, 2 CPUs),
Redis 7.4.2, MinIO 2026-08-04, Python 3.12, numpy 2.5.2, host 15 GiB RAM,
12 CPUs, RTX 2050 4 GiB.

### 13.1 Version-specific behaviour (first I-2 tests)

| ID | What | Command | Result | Measured |
|---|---|---|---|---|
| R-24 (i)–(v) | `seq` `lte`/`lt`/`match` filter boundaries, subject scoping, bigint `seq` ≈ 2⁴⁰, plain and indexed segments, media (`rep`) and target (`trep`) roles, production collection params | `P10_EVIDENCE_DIR=benchmarks/p10-index/results/evidence env/bin/python -m pytest tests/p10/test_r24_qdrant_semantics.py` | **PASS** (integration, Qdrant 1.15.1) | (i) S@10, S@11 deleted, S@12 and every S′ point kept; (ii) strict `<`; (iii) count = 4 = the S@12 points; identical at 2⁴⁰ and after indexing (`indexed_vectors_count` = 24); counts exact immediately after each `wait=true` write — `r24-qdrant-filters-{media,target}.json` |
| R-24 (vi) | the same without the `seq` integer index (recorded only) | same file | recorded | identical semantics — `r24-vi-no-seq-index.json` |
| R-24 (vii) | stored float32 vectors returned bit-identically | same file | **PASS** | 64/64 by retrieve, by scroll and after indexing — the §12.5 content digest can be computed from Qdrant points — `r24-vii-bit-identical.json` |
| L-14 (Qdrant part) | `exact=true` + `quantization.ignore=true` + `score_threshold` | same file | **PASS** | max |score − numpy float32 cosine| = 7.1e-8; 0.9501 and 0.97 returned, 0.9499 excluded — `l14-exact-ignore-quantization.json`. The truncation/`done_rev` part of L-14 is a lowinfo-lifecycle test (§13.4) |

Consequence: the §12.2 cleanup/DELETE protocol is used **as designed**; no
revision needed.

### 13.2 Scylla 6.2.3 LWT execution semantics (executed, not prepared)

`env/bin/python -m pytest tests/p10/test_scylla_semantics.py` — integration,
Scylla 6.2.3, keyspace `fingerprinter_it_p10` (tablets off). **7/7 PASS.**

| Form (design ref) | Executed check | Result |
|---|---|---|
| run fence (§11.1, R-26 forms) | claim `IF run = ? AND run_started_at = null` (first applies, second rejected); bump `IF run = ? AND state IN ?` with `recovery_runs[timeuuid] = ?` (applies; map holds the entry; `run_started_at` cleared); stale completion `IF epoch = ? AND run = ?` after the bump rejected and returns the new `run`; coalesce on an unclaimed run; FAILED excluded by the `IN` list | PASS — `scylla-r26-task-forms.json` |
| list append in an LWT; `= null` | `audit = audit + ?` under `IF state =`; `IF suspect_since = null` on a never-written column, then on a written one | PASS — `scylla-list-append-null.json` |
| CAP-9 admission batch (§13.3) | static-only `INSERT … IF NOT EXISTS`; batch `reserved = r + P IF reserved = r` + holder-row insert applies together; a stale `r` applies neither | PASS — `scylla-cap9-batch.json` (the CAP-9 *audit-race* part is §13.5) |
| promotion batch (§12.4) | static `IF active_epoch = e` + row `IF state = 'READY' AND detect_count = v` + old row `IF state = 'ACTIVE'`: fenced by a changed `detect_count` (nothing applied), then applied atomically, replay rejected | PASS — `scylla-promotion-batch.json` |
| **§9a.4** latest occurrence by `LOCAL_SERIAL … LIMIT 1` | (1) prepared and executed at `LOCAL_SERIAL` on `index_repairs` (`occurrence DESC`); (2) returns the highest occurrence right after acknowledged `INSERT … IF NOT EXISTS` (3 inserts) and after **8 LWTs whose outcome was unknown to the client** (client timeout 0.5 ms; each decided by the slice read and confirmed by a SERIAL point read); (3) 5 rounds × 6 concurrent detectors acting on it: occurrences exactly 1…5, one opener each, 25 merges, none gave up, no merge into a row past OPEN | PASS — `scylla-9a4-slice-read.json`, `scylla-9a4-concurrent.json` |
| §12.10 walk (fallback) | single-row `INSERT … IF NOT EXISTS` upward from occurrence 1, failed inserts return the existing row | PASS |

Consequence: §9a.4 requirements 1–3 are established **on the deployed
single-node Scylla 6.2.3**; the implementation uses the slice read
(`recovery.Recovery.latest`) and keeps the walk documented and tested as the
fallback. Multi-node Paxos behaviour is P14.

### 13.3 I-1: verifier, tuning, calibration, lowinfo (offline evaluation)

`temporal/v2` unit tests: `env/bin/python -m pytest tests/p10/test_verify.py`
— **14/14 PASS** (unit): partial clip with offset and 2.3 s phase; unrelated
content; identical frames at scattered offsets do not form a run; two
identical frames < L; usable-flag exclusion neutral (neither counts nor
breaks a run) and a forged 5-frame logo run removed by it; reordering breaks
runs into two verified modes; repeated scenes give several runs; sparse
matches vs `max_gap`; phase tolerance; offsets in seconds independent of
segment numbering; empty inputs; **V-1 unit**: `verify()`'s parameters are
exactly (media, target, params, excluded), and two processes with 1 and 8
BLAS threads give bit-identical output on 20 noisy pairs; `equiv/v1` links a
re-encode at another phase and rejects a 50 % sub-clip, a 30 s shift and
other content.

Nested film-grouped tuning, calibration and the lowinfo oracle comparison:
`OMP_NUM_THREADS=1 env/bin/python benchmarks/p10-index/tune_eval.py --data
~/anti_piracy/datasets/p9 --vectors ~/anti_piracy/datasets/p10/vectors --out
benchmarks/p10-index/results/tune-eval.json` (203 s). Offline evaluation on
the cached `dinov2-seg/2` vectors; candidates generated with **exact**
search under the provisional direction-A policy (§13.6 measures the live ANN
path). Populations: L-PAIRS (480 pairs, 150 +) and **N-SOAK** derived here
from SOAK-1000 provenance (6,092 pairs, 929 +; relevant ≥ 30 s overlap,
irrelevant ≥ 15 s apart, multi-film items paired only with their largest
film; `benchmarks/p10-index/corpus.py`). 40 configurations × 2 filter
settings (grid in the result file); selection = maximum training recall at
zero training FP; outer 7 × inner 6 leave-one-film-out.

| Held-out (outer folds pooled) | TP | FN | FP | TN | Precision | Recall |
|---|---|---|---|---|---|---|
| **P10 `temporal/v2` confirmed, all** | 876 | 203 | 1 | 5,492 | 0.9989 | 0.812 |
| V1 matcher on V2 vectors, no coarse, all | 484 | 595 | 7 | 5,486 | 0.9857 | 0.449 |
| V1 worker path (coarse 0.60), all | 443 | 636 | 3 | 5,490 | 0.9933 | 0.411 |
| P10, L-PAIRS | 110 | 40 | **0** | 330 | 1.000 | 0.733 |
| V1 on V2 vectors, L-PAIRS (reproduces P9: 37/113/0/330) | 37 | 113 | 0 | 330 | 1.000 | 0.247 |
| P10, N-SOAK | 766 | 163 | 1 | 5,162 | 0.9987 | 0.825 |
| V1 on V2 vectors, N-SOAK | 447 | 482 | 7 | 5,156 | 0.9846 | 0.481 |
| P10, the 24 VFR SOAK items | 20 | 2 | 0 | 139 | 1.000 | 0.909 |

Candidate generation (exact): 3,182 pairs proposed, 1,078 of 1,079
positives (candidate recall 0.9991), no saturated query segment, 0
INCOMPLETE items. The one held-out FP (fold *sintel*, whose training
selected the looser `s_seg 0.70 / L 4 / τ_c 4.85`) is a 240p HEVC Sintel
item matched to Elephants Dream end credits (5 segments, mean 0.81): the
credits/look-alike risk of audit F-2, not caught by `lowinfo/v1`.
Six of seven outer folds selected the same setting as the final one.

**Final parameter set (all 7 films, the same inner procedure) =
`dinov2-seg-temporal` 2.1:** `s_seg` 0.80, `s_mean` 0.85, `L` 2,
`phase_tolerance_s` 2.5, `max_gap` 1, `vote_floor` 0.70, `n_offsets` 3,
`τ_c` = `τ_p` = 1.0 (no training negative produces a qualifying run, so
every qualifying run is `confirmed` and there is no `probable` band); params
digest `377167ec…af`. Its expected performance is the outer-loop estimate
above.

**Calibration (§3.3): status UNAVAILABLE.** The isotonic data minimum is met
(1,078 +, 2,104 − proposed out-of-fold pairs, 7 films, 20 negatives ≥ τ_p),
and Brier 0.080 beats the constant predictor (0.224) and the scaled raw
score (0.297), but **4 of 5 equal-mass bins fail the film-level bootstrap
check** (e.g. mean predicted 0.222 vs observed 0.050). Per design §9.1 no
probability is published: decisions of 2.1 are stored and held
(`match publish-held` refuses). Platt is the design's fallback only when the
isotonic minimum is *not* met, so it was not substituted.

**`lowinfo/v1` (§3.4): not adopted.** 12 of 760 target segments meet the
rule (< 20 ⇒ benefit **not established**); filtered vs unfiltered on the
outer held-out pairs: `lost_tp` = 1, FP 1 vs 1 ⇒ the adoption rule fails.
`matching.lowinfo_enabled` stays `false` (a valid configuration); the
machinery is implemented and tested (§13.4) but excludes nothing.

Limits: seven animated films from one reel; correlated clips; no live
action; candidate generation exact here.

### 13.4 I-1 (a): V1 end-to-end baseline on L-PAIRS (executed)

`env/bin/python benchmarks/p10-index/v1_e2e_baseline.py --data ~/anti_piracy/datasets/p9 --device cuda --scratch ~/anti_piracy/datasets/p10/runs/v1-e2e --out benchmarks/p10-index/results/v1-e2e-l-pairs.json`
— the unchanged V1 worker handler (V1 DINOv2 engine at the pinned revision,
`fps=1/5` sampling, V1 `TargetRegistry` with build-on-miss, coarse screen
0.60, `match_segments` 0.90/3/1/2, aggregation), one `Job` per pair, clips
over HTTP from a loopback fixture server, `MediaAcquirer(allow_private_networks=True)`
as in V1's own integration tests, Redis db 15; RTX 2050, torch 2.13.0+cu130;
480 pairs in 45 min. **Gate-eligible.**

| V1 end to end | TP | FN | FP | TN | Precision | Recall (Wilson 95 %) |
|---|---|---|---|---|---|---|
| L-PAIRS, all 480 pairs | 34 | 116 | 0 | 330 | 1.000 | 0.227 (0.167–0.299) |

No pair needed the environmental probe-timeout retry; strict V1 worker
semantics give the same counts. Per transform: re-encode 14/35, pillarbox
10/28, crop 6/28, letterbox 2/28, 360p 2/31. Per outer fold 2–7 TP, 0 FP
everywhere.

**Stronger baseline for M3-1 on L-PAIRS** (validation §2.2): V1 matcher on V2
vectors, P 1.000 / R 0.247 (37 TP) ≥ V1 end to end (34 TP). N-SOAK: the V1
end-to-end path was **not run** — the per-pair V1 handler re-decodes each
SOAK item per target (≈ 137 h of video × up to 7 targets on this host); the
N-SOAK comparison therefore uses V1 on V2 vectors only (P 0.9846 / R 0.481,
§13.3). This is a recorded limitation, not a pass.

### 13.5 E-R1 — capacity boundary (design §13.4), dedicated `qdrant-e-r1` container

Harness: `benchmarks/p10-index/capacity_e_r1.py` in the isolated venv
(qdrant-client 1.15.1, gRPC batches of 1,000), container `qdrant-e-r1` (same
digest, `--memory 1.5g --memory-swap 1.5g --cpus 2`), fresh volume per run,
run alone on the host (the only other activity: idle crawler2 services).
`FINGERPRINTER_INDEX__API_KEY=… ~/anti_piracy/datasets/p10/venv/bin/python -I benchmarks/p10-index/capacity_e_r1.py --vectors ~/anti_piracy/datasets/p10/vectors --copies <c> --container qdrant-e-r1 --cgroup /sys/fs/cgroup/system.slice/docker-<id>.scope --out benchmarks/p10-index/results/e-r1-<c>.json --samples ~/anti_piracy/datasets/p10/runs/e-r1/<c>.samples.jsonl`.
(A first 6-copy attempt died because the *harness* held the corpus twice and
systemd-oomd killed it; fixed by building the copies in place — no Qdrant
measurement was affected.)

**6 copies (N = 626,658, R_req = 1,304,649): FAIL — stopped in phase 2.**

| Measured | Value |
|---|---|
| phase 1 (626,658 target + 10,000 media points) | ingested and indexed in ≈ 40 min (≈ 300 points/s while the optimizer indexed on 2 CPUs); green; exact counts = harness total (636,658) |
| memory | `oom_kill` 0; `anon` 0.70 GiB at the end of phase 1 (≤ 1.35 GiB); `memory.peak` = the 1.5 GiB cap (page cache) |
| volume | 3.34 GB after phase 1 |
| query latency, phase 2 (single search, limit 100, int8 + rescore, oversampling 2) | probe of 20 queries: **p50 16.6 s, p95 23.2 s** (criterion p95 ≤ 10 ms) |
| cause | rescoring reads the float32 originals from disk; with 1.5 GiB shared by ≈ 0.7 GiB anon and the page cache, the ≈ 1.9 GB of originals are not cached and every query does ~200 random reads on the host's HDD (`sda` 98 % busy, ≈ 108 reads/s at ≈ 18 ms) |

Phase 2 alone (2,280 sequential queries) would have taken hours; phases 3–5
were not run. The result cannot pass the latency rule, so per §13.4 the run
is repeated at fewer copies. Record: `results/e-r1-6copies-aborted.json`.

**1 copy (N = 104,443, R_req = 234,109): FAIL (latency); memory rules pass.**
`results/e-r1-1copy.json` (per-second samples outside git).

| Rule / measurement | Result |
|---|---|
| no `oom_kill` | pass (0) |
| `anon` peak ≤ 1.35 GiB | pass — 0.394 GiB (phase 3: 0.42 GiB peak by phase table; `memory.peak` = cap via page cache) |
| exact counts = harness total at every boundary | pass (114,443 → 234,222 → 122,111) |
| phase 3 resident ≥ R_req | pass — 234,222 |
| query p95 ≤ 10 ms after 60 s warm-up, phases 2/3/5 | **FAIL** — phase 3 (rebuild while serving): p50 27.7 ms, **p95 5.1 s**; phase 5 (steady): p50 8.7 ms, **p95 154 ms**; phase 2 lasted 14 s (2,280 queries) so it had no post-warm-up samples |
| live writes (12 points into every routed generation) | p50 132 ms, p95 2.4 s; 0 errors |
| build g2 while serving | ingest 153 s, green after 168 s |
| volume | 0.89 GB (phase 1), 2.36 GB (phase 3), 1.07 GB (phase 4) |

**E-R1 outcome: FAILED at every scale tried (6 copies, 1 copy) on the query
latency rule; the memory and counting rules pass where measured.** Per
design §13.4 ("if 1 copy fails too, P10 indexing on the dev host is blocked
pending a separate decision"): no `max_points` value is derived from E-R1,
the configured value stays the provisional 250,000 (never raised), the
capacity gate (P10-T10) and the resource criterion (P10-T6) are **not met**,
and the base target scenario is not supported by evidence. The measured
cause is disk-bound rescoring of on-disk float32 originals on this HDD host
under a 1.5 GiB cap (page cache cannot hold the originals; during a rebuild
the optimizer competes for the same disk). Options, each a **separate
operator decision** (none applied): an SSD-backed index host (P14), a
larger memory limit (D14 "changed only by a separate decision"), originals
in RAM for small roles (a D2 change), or a different latency criterion. The
2- and 3-copy repeats were not run: they hold more originals per byte of
page cache than the 1-copy run, so they cannot pass a rule the 1-copy run
fails. Implementation and functional validation continue on small
test-keyspace indexes (validation §13.6 onwards); none of them is evidence
for this gate.

### 13.6 Implementation tests on the real stack (I-2, I-3, part of I-5)

`. scripts/p10-env.sh && env/bin/python -m pytest -p no:cacheprovider tests/p10 -q`
— **49 passed** (10 min 19 s) against Scylla 6.2.3, Qdrant 1.15.1 (dev
container), MinIO, Redis; P9 unit suite unchanged (`tests/v2`, 100 passed).
Data enter through P9's own request → claim → commit protocol and
`TargetManager.register` (fixture `tests/p10/conftest.py::p10`), with
artifacts from cached real `dinov2-seg/2` vectors (L-PAIRS) or seeded
synthetic vectors. Faults are injected at named hook points or by direct
Qdrant calls (stale/paused workers, non-protocol losses). These are
functional tests on small indexes — **not** evidence for E-R1 or for scale.

| Group | Tests (file) | Result | What was shown |
|---|---|---|---|
| H integration, both directions | `test_flow.py` | PASS | A: clip → Sintel confirmed, no cross-film match; B: a target registered after the media matched it with **zero new encode claims**; candidates come from ANN (`queries ≥ query segments`, bounded); decisions stored with full provenance, **held** (no confidence, no envelope, nothing XADDed: 2.1 UNAVAILABLE); an unrelated subject reports `settled` |
| R-2, R-3 | `test_mutation.py` | PASS | crash after the intent (no Qdrant write; reconciler applies) and after Qdrant before bookkeeping (no duplicate points) |
| R-4 (C-14k) | | PASS after fix | **found a defect**: an apply restricted to some generations marked the row APPLIED; fixed — APPLIED is judged against the full routing set |
| R-5, R-11, R-16 | | PASS | interrupted DELETE repeated; write to a RETIRED generation is a no-op and re-creates nothing; orphans of an unknown subject deleted by verification |
| R-22 / R-23 (S2) | | PASS | stale UPSERT of an older seq + stale DELETE `seq ≤ u−1` after UPSERT u: the seq-u points survive at every step; verification removes the extras |
| CAP-1, CAP-3, audit | | PASS | refusal before any Qdrant write (`BLOCKED:capacity`, `WaitingIndexError(capacity)`); rebuild `CAPACITY_BLOCKED` creates no generation row; no reservation for an already-applied seq; audit bound ≥ resident points |
| R-17 / R-25 (a) | `test_recovery.py` | PASS | loss before the search: M DONE without (M, T) and `settled` while unknown; detection ⇒ occurrence OPEN ⇒ M `provisional`; repair, bump of T (run 2, `recovery_runs = {oid: 2}`), (M, T) found by T's own rerun; CLOSED `RERUN_DONE`, RESOLVED; M untouched (R4) |
| R-26 a | | PASS | M blocked at P1 while the verifier writes the occurrence; order M@P1 → occurrence durable → M's DONE (not gated); the pair is recovered by T's run 2 |
| R-26 b (S5) | | PASS | T's own run 2 blocked at P5; bump first ⇒ run 2's completion rejected (`superseded`), never recorded; `required_run = 3`, resolved by run 3 |
| R-27 | | PASS | second loss ⇒ a **new row** (occurrence 2, new `occurrence_id`, initial values), row 1 byte-for-byte unchanged; run 3 required and used; counters 0/0 at the end |
| R-29 (u1/v2) | | PASS | rerun INCOMPLETE (cap 1) ⇒ CLOSED `RERUN_INCOMPLETE`, PENDING; an unrelated subject `provisional (recovery unresolved)`; `retry --raise-cap` ⇒ DONE ⇒ resolved `LATER_RUN` by its own LWT; probe `settled` afterwards |
| G-10 (C-14d) | | PASS | a corrupt **candidate** artifact fails only that candidate (`artifact_corrupt`), run INCOMPLETE `verification_failed`; the subject's own corrupt artifact ⇒ its task FAILED |
| G-15 (C-14g) | | PASS | saturation ⇒ INCOMPLETE `saturated_segments`, never DONE; `match status` provisional with that reason |
| K-1, K-6, K-9 | | PASS | `match_id = MatchId.of(key)`; `matches_by_id` resolves back; three concurrent re-commits with a different later score are no-ops (first decision final). **Found a gap**: a target's reference content matched its own target in direction A — fixed (N-13) |
| §5 drop/rebuild, R-20 | `test_rebuild_discovery.py` | PASS | collections deleted ⇒ a probe task `WAITING_INDEX` (no decision, no fallback); rebuild target role first, **manifest digests equal** the recorded ones and the artifact-only digest; lost generations stay ACTIVE until superseded, become DRAINING, get the 7 unauditable-fallback occurrences, then RETIRED; reruns re-propose every earlier decision; **decision set identical**; counters 0/0 |
| H-1, H-2, H-3, H-4, H-5 | | PASS | crash before the cursor LWT ⇒ batch re-read, work recorded once; a second reader cannot advance the cursor; trimmed hints ⇒ gap recorded, backstop scan creates the missed task |
| H-9 a–h | | PASS | 300 target partitions (20 with 4 rows: 2 versions × 2 specs), keys page 3, fetch size 1, kill right before a cursor LWT: resume from `>=` the last completed partition; a full cycle visits every row present; cursor never inside a partition |

Defects found and fixed by this round: R-4 bookkeeping (above); stale
`match_due` rows re-enqueued finished tasks every round (completion now
clears every due row of the task); a DRAINING generation's grace counted
from its *own* old `promoted_at` (now from its recorded search horizon);
build-lease renew/release raised on an unknown LWT outcome (now advisory —
the `build_epoch` condition remains the fence). Not yet run: the remaining
R-* crash variants (R-6…R-10, R-12…R-15, R-18, R-19, R-21, R-28, the c*/d*
variants of R-26 and j*/v* of R-27/R-29), G-1…G-9, G-11…G-14, L-1…L-17
(lowinfo not enabled), K-2…K-5, K-7, K-8, K-10, K-11, CAP-2, CAP-4…CAP-9 as
full scenarios, §8 E-SET.

### 13.7 Publication, liveness and low-information tests

`env/bin/python -m pytest -p no:cacheprovider tests/p10/test_publication.py tests/p10/test_lowinfo.py`
— **16 passed** (integration, real stack).

| ID | Result | What was shown |
|---|---|---|
| K-8, K-10 | PASS | with a **test-only** CALIBRATED technique version (labelled, never in a real keyspace): one envelope per `match_id`, built through the frozen contract and decoded back as `match.found` 1.0; `event_id` stored once; held while no consumer group exists (nothing XADDed); two concurrent `publish-held` reuse the stored envelope; after a consumer registers, exactly one stream entry with the same `event_id`; `published_at` set |
| publication rule | PASS | `publish-held` for 2.1 (UNAVAILABLE) is refused |
| K-11 | PASS | the one liveness predicate over binding × target states (READY binding of a REGISTERING / BUILD_FAILED target is live; RETIRED / PURGED / PENDING are not) |
| L-1 | PASS | classification flags equal an exhaustive numpy reference over the authoritative set; witnesses confirmed on artifacts |
| L-17 (C-14h) | PASS | per-hit exclusion: hits on usable-flag segments do not vote, the lower hits on an unflagged target still make it a candidate |
| L-4 / L-12 | PASS | witness retired in P9 with no refresh ⇒ the flag is not usable (fail-closed) |
| L-13, L-6 | PASS | PARTIAL record excludes nothing; a stale writer of an older revision is rejected |
| L-16 | PASS | witness read failure propagates (the task retries); nothing is excluded on an unknown |

The lowinfo tests enable `matching.lowinfo_enabled` in their test
configuration only; production keeps it disabled (§13.3).

Isolation defect found while running these concurrently with the M3 run:
see design §20 N-14 (collection names collided across keyspaces); fixed
with `index.collection_prefix`; the contaminated first M3 attempt was
discarded and repeated from a dropped keyspace.

### 13.8 Matcher failure injection (§9 G-*)

`env/bin/python -m pytest -p no:cacheprovider tests/p10/test_failures.py` — **5 passed** (integration).

| ID | Result | What was shown |
|---|---|---|
| G-1 | PASS | index unreachable from the matcher (connection refused): task `WAITING_INDEX(unavailable)`, no decision, no fallback; after the index returns the run resumes to DONE with exactly one decision |
| G-1 (one query) | PASS | exactly one failed segment query of a search step fails the whole step: no `SEARCHED`, no candidate list from partial results, never "zero hits"; retried to DONE |
| G-9 | PASS | Scylla failure right after the commit intent: the intent survives, the task waits/retries, the reconciler's intents pass and the rerun converge to exactly one `matches` row |
| G-11 | PASS | a worker killed after a verification page (uncatchable exception): task left VERIFYING under its lease; after lease expiry (shifted clock) another claim resumes the same run (run 1) to DONE, one decision |
| G-14 (simulated two-host) | PASS | two matcher instances (separate instance ids, one Qdrant, one keyspace) drain the same queue concurrently: every task DONE once, one row per `match_id` |

### 13.9 Level-5 equivalence (§8, I-4) — E-SET and P8's set

**E-SET** (P10-D18), `benchmarks/p10-index/make_eset.py --excerpts 20`
(outside git: `~/anti_piracy/datasets/p10/e-set/`, 160 items): 20 S-SRC
excerpts of 60–120 s (seeded, spread over the 7 films), each as six
variants — H.264 400 kb/s, H.264 3 Mb/s, MKV remux, fragmented MP4 (1 s and
2 s fragments), VP9 WebM — from a 480p mezzanine cut, plus a 30 s-shifted
copy and a 50 % sub-clip. Vectors with P9's production pipeline
(`encode_corpus.py`, `dinov2-seg/2`, CUDA FP32). Offline `equiv/v1` over
all 12,720 item pairs: `eset_eval.py` → `results/e-set-offline.json`.

| Measured (offline) | Value |
|---|---|
| equivalent pairs linked | **228 / 300 — recall 0.760** (Wilson 0.709–0.805); target 1.0 **not met** |
| false links | **0 / 12,420** (precision 1.000) |
| 30 s-shifted copies / 50 % sub-clips linked to their excerpt | 0 / 0 |
| recall by variant (pairs containing it) | H.264 400k 0.52, VP9 0.74, fMP4 1 s 0.82, fMP4 2 s 0.82, H.264 3M 0.83, MKV remux 0.83 |
| cause of misses | heavy compression (400 kb/s at 480p) and VP9 lower segment cosines below the 2.1 `s_seg` 0.80, so the best run covers < 90 % of the segments of one side; the rule and parameters were not changed on this test set |

**P8's 6 s set** (P8's exact ffmpeg commands rebuilt by `p8_set.py`,
crawler2 untouched; `results/p8-set.json`): 5 items × 2 segments; **all 10
pairs link** under 2.1 (`L` = 2). The plan expected "not linked (2 segments
< L)" because it assumed `L` = 3; with the tuned `L` = 2 a 2-segment item can
satisfy the 90 % coverage rule. All pairs are truly equivalent, so no false
link, but two 5 s segments are thin evidence — recorded as a limitation (the
minimum verifiable duration under 2.1 is 10 s).

Live equivalence (media↔media tasks writing `equivalence_links`): §13.11.

### 13.10 M3 through the live system: L-PAIRS (I-5) — superseded by §13.16 (pre-fix run)

`benchmarks/p10-index/m3_live.py --drop-rebuild` in keyspace
`fingerprinter_m3` (collection prefix `m3`): 7 films registered through
`TargetManager.register` on the real S-SRC files (production content ids),
reference representations and 180 clips committed through P9's commit
protocol from the cached P9-encoded vectors (ingest 1,153 s under load),
index built and validated (760 target / 6,220 media points, manifests
recorded), production matcher and reconciler. `results/m3-live-lpairs.json`.

| Result | Value |
|---|---|
| tasks | 187 `media_vs_targets` + 7 `target_vs_media`, all DONE (32 s); recovery 0/0 |
| live ANN candidate recall vs provenance (A ∪ B) | **1.000** (150/150) |
| **nested held-out (per-fold 2.1 parameters on the live candidate sets) — the M3-1 figure** | TP 110 / FN 40 / **FP 0** / TN 330 — **P 1.000, R 0.733** (Wilson 0.656–0.798); equal to the exact-search emulation (§13.3) |
| deployed 2.1 (in-sample parameters) | TP 100 / FN 50 / FP 0 / TN 330 |
| V1 baselines on the same 480 pairs | V1 on V2 vectors P 1.000 / R 0.247; V1 end to end P 1.000 / R 0.227 (§13.4) |
| decisions | 100 stored, all held (0 published, 0 XADDed) |
| §5 drop/rebuild | tasks `WAITING_INDEX` while lost; rebuild 31 s; **manifest digests equal** (both roles); decisions **identical** (100/100); every decision re-proposed; recovery 0/0 |

On L-PAIRS, M3-1's comparison (precision ≥ V1 and recall ≥ V1 against the
stronger baseline on identical held-out pairs, zero FP) **holds** through
the live candidate path.

### 13.12 Rebuild, lease and occurrence-fence scenarios

`env/bin/python -m pytest -p no:cacheprovider tests/p10/test_generations.py` — **5 passed** (integration).

| ID | Result | What was shown |
|---|---|---|
| R-6 / G-4 | PASS | builder killed after 20 of 64 buckets (uncatchable): generation left BUILDING with 20 bucket cursors; after the build lease expires a new build **adopts** the same generation (no new number), resumes from the cursors and validates READY with the artifact-only manifest digest |
| R-12 / G-12 | PASS after a correction | a subject written during the build (after CREATED) is present at its newest seq in both the new and the old generation at promotion. **Found a design defect**: validation's "exact count of g ≤ `reserved_points`" rejected this correctly accounted generation (34 > 31, the live write having reserved separately); corrected to the ledger condition (design §20 N-16) |
| R-13 | PASS | a second builder cannot take a valid build lease; after expiry it gets epoch + 1; the stale builder's transition conditioned on its old `build_epoch` is rejected |
| R-28 (C-14b) | PASS | T's points deleted again after the repairer's confirming count (hook P7) with a detector merging before `repaired`: `detections` raised, the REPAIRED LWT does not hide the second loss; the repair is repeated, every occurrence of T closes and T's qualifying run is at or after every requirement |
| K-2 | PASS | the same content and target version under `dinov2-seg/1` and `/2` give two distinct MatchIds (two `matches` keys, C-6) |

### 13.13 M3 through the live system: L-PAIRS + SOAK-1000 (19x growth)

`m3_live.py --soak --rerun-targets` then `--soak --rerun-all` (after the fix
below) in keyspace `fingerprinter_soak_m3` (prefix `m3s`):
`results/m3-live-soak.json`. Corpus: 7 films, 180 L-PAIRS clips, 1,000
SOAK-1000 items — **105,203 media points** (19.1x the L-PAIRS media), 760
target points. Ingest 53 min, media build + validation 12 min (first attempt
failed validation: N-17, fixed), matching 6.5 min per full pass.

| Result (pooled L-PAIRS + N-SOAK, 6,572 pairs, 1,079 +) | TP | FN | FP | TN | P | R |
|---|---|---|---|---|---|---|
| **nested held-out (M3-1 figure)** | 876 | 203 | 1 | 5,492 | 0.9989 | 0.812 |
| deployed 2.1 (in-sample) | 867 | 212 | 0 | 5,493 | 1.000 | 0.804 |
| L-PAIRS only, nested | 110 | 40 | 0 | 330 | 1.000 | 0.733 |
| N-SOAK only, nested | 766 | 163 | 1 | 5,162 | 0.9987 | 0.825 |
| V1 on V2 vectors, same pairs (§13.3) | 484 | 595 | 7 | 5,486 | 0.9857 | 0.449 |

| Live path | Value |
|---|---|
| live ANN candidate recall vs provenance (A ∪ B) | **0.9991** (= the exact-search emulation, §13.3) |
| direction B (targets → historical media) candidate recall vs provenance | **0.9972** (criterion §6: ≥ 0.99 at 19x — met) |
| direction A tasks | 1,187 DONE, no saturation; ≤ 50 candidates each (cap never hit) |
| **direction B tasks at 19x** | **all 7 INCOMPLETE `saturated_segments`** (2–38 query segments per target ended at `k_max` 512 with the k-th score ≥ floor 0.70); stored candidates 301–504 per target (≤ `cap_B_total` 5,000); 83–577 queries per target |
| bounds | hits ≤ segments × `k_max`; candidates ≤ caps; every bound hit recorded on the task (`ann_stats`) |
| decisions | 1,574 stored, all held (calibration UNAVAILABLE), 0 events; recovery 0/0; ledger 111,262 reserved for 105,963 resident points |

Defect found by this run and fixed (commit `2d11056`): candidates whose
sample hit was segment 0 were dropped as `foreign_point` (`int(k or -1)`),
≈ 30 % of direction-A candidates in the first pass; deployed recall rose
from 0.797 to 0.804 after re-running every task; the nested figure was not
affected (it scores every proposed candidate). Regression assertion added
to `test_flow.py` (shown to fail on the old code).

Consequences for M3: precision ≥ V1 and recall ≥ V1 on identical held-out
pairs hold on both populations through the live candidate path (the one
held-out FP is the credits case of §13.3; V1 itself has 7 FPs on N-SOAK). On
the strict L-PAIRS reading (V1 precision 1.000 ⇒ zero FP) P10 has 0 FP.
Direction B's saturation at 19x makes every target run INCOMPLETE, so
direction-B negatives at that scale are provisional; `k_max`/floor tuning
for direction B is open (design §17).

### 13.14 ANN recall against the exact oracle, live index (§4)

`ann_recall.py` against the 19x ACTIVE media generation (105,203 points,
int8 HNSW m=32, rescoring, oversampling 2, `hnsw_ef = max(k, search_ef)`),
exact = the same collection with `exact=true` + `quantization.ignore=true`
(agrees with numpy on stored vectors to float precision):
`results/ann-recall-live.json`. Run alone on the host.

| Criterion (§4) | Measured | Result |
|---|---|---|
| segment recall@100 ≥ 0.97 | **0.9989** (760 film segments), **0.9808** (180 clip segments); @10: 0.998 / 0.983 | **PASS** |
| per-segment query p95 ≤ 10 ms (single-query REST requests) | limit 100: **p95 3.1 ms**; limit 10 in the harness (with payload): p95 43.9 ms; probe: limit 10 0.9 ms, **limit 32 42.6 ms**, limit 100 1.5 ms | **FAIL (size-dependent)**: a ≈ 40 ms plateau for some response sizes — the TCP delayed-ACK/Nagle interaction with Qdrant's HTTP server (same with `bytes` bodies); the matcher batches up to 64 queries per request, which amortises it; not resolved (gRPC or transport tuning is follow-up work) |
| media-level candidate recall with ANN ≥ exact − 0.005 (direction B policy) | per film 0.740–0.991 (mean 0.876) | **FAIL**: at 19x every target has saturated segments (§13.13), so the two top-512 neighbour sets — and the grouped candidates derived from them — differ between ANN and exact; against provenance the live direction-B candidate recall is 0.997 and the direction-A live candidate recall equals the exact emulation (0.9991). The comparison is not meaningful while saturated; it needs the direction-B `k_max`/floor retuning first |
| memory within the 1.5 GiB limit | dev Qdrant at 105 k + 760 points: no OOM during the M3 runs | observed (not a gate measurement; E-R1 §13.5 governs) |

### 13.15 Verification workload and growth (§6): base, 10x, 19x

Same harness, three keyspaces: base = L-PAIRS only (6,220 media points,
§13.10), 10x = L-PAIRS + the first 500 SOAK items (`--soak-limit 500`,
**55,945** points, keyspace `fingerprinter_g10_m3`, `results/m3-live-g10.json`),
19x = L-PAIRS + all SOAK-1000 (**105,203** points, §13.13).

| Scale | Pairs | Nested held-out TP / FN / FP | L-PAIRS recall (nested) | Live candidate recall | Dir. B recall vs provenance | Dir. B runs | Max stored candidates per B run |
|---|---|---|---|---|---|---|---|
| base | 480 | 110 / 40 / 0 | 0.733 | 1.000 | 1.000 | 7 DONE | — (all candidates verified) |
| 10x | 3,519 | 504 / 114 / 0 | 0.733 | 1.000 | 0.998 | 1 DONE, **6 INCOMPLETE** (1–19 saturated segments) | 313 |
| 19x | 6,572 | 876 / 203 / 1 | 0.733 | 0.9991 | 0.997 | **7 INCOMPLETE** (2–38 saturated segments) | 504 |

**Bounds (acceptance, all scales): met** — hits per segment ≤ `k_max`;
direction-A candidates ≤ `cap_A` (50, never truncated); stored direction-B
candidates ≤ `cap_B_total` (5,000; max 504); verification in pages of
`page_B` (200); every saturation recorded on the task. **Effectiveness:**
direction B recall vs provenance at 19x 0.997 ≥ 0.99 — met; direction A
recall at every scale (0.733 on L-PAIRS) ≥ the held-out recall of §2 —
met; precision: 0 FP on L-PAIRS at every scale; the single pooled FP at 19x
is an N-SOAK pair that exists only in the 19x corpus (the credits case,
§13.3), not a growth effect. **Not met as a workload property:** direction
B saturates from 10x on (INCOMPLETE), so its negatives are provisional;
`k_max_b`/`floor_b` retuning is required before direction B can finish DONE
on duplicated corpora (design §17 anticipated this).

### 13.11 Live level-5 equivalence (§8, I-4)

`FINGERPRINTER_MATCHING__EQUIVALENCE_ENABLED=true eset_eval.py --live`,
keyspace `fingerprinter_eq_m3` (prefix `m3e`): the 160 E-SET items ingested
through P9's commit protocol, media role built and promoted, 160
`media_vs_media` tasks run by the production matcher (adaptive k, floor
0.70, cap 2,000; verification `equiv/v1` on artifacts), links committed via
intents. `results/e-set.json`.

| Check (§8 acceptance) | Result |
|---|---|
| variants have pairwise distinct `ContentId`s (P8 level 3) | **yes** (160 distinct) |
| every pair of variants `LINKED` (target recall 1.0) | **228 / 300 = 0.760** — target **not met** (identical to offline; misses as in §13.9) |
| zero links between negatives (other excerpts, 30 s shift, 50 % sub-clip) | **0 / 12,420** — met |
| links written in both orientations | yes, every link |
| `equivalence revoke` | both orientations REVOKED, evidence kept; after re-running both subjects' tasks the pair stays REVOKED (`IF NOT EXISTS` keeps it) — met |
| no record merged | met (links are a separate relation; no ContentId, representation or observation changed) |
| tasks | 160 DONE (no saturation, no cap) |

### 13.16 L-PAIRS live run repeated with the corrected code

The §13.10 run preceded the `foreign_point` fix (§13.13); it was repeated
from a dropped keyspace (`m3_live.py --drop-rebuild`,
`results/m3-live-lpairs.json`, superseding the earlier file): 194 tasks
DONE; live candidate recall 1.000; **deployed 2.1 = nested held-out = TP
110 / FN 40 / FP 0 / TN 330** (the earlier deployed 100 was the defect);
110 decisions stored and held. §5 procedure: tasks `WAITING_INDEX` while the
index was deleted; rebuild 28 s; manifest digests equal for both roles;
**decisions identical (110 / 110)**, every one re-proposed; recovery 0/0.
