# P10 Decisions — vector index and matcher

Status: **all decisions PROPOSED; none approved.** Date 2026-10-15;
corrected the same day after the first review (C-1…C-7, §5) and after the
second review (C-8…C-11, §5), and on 2026-10-17 for the stale-DELETE race
(C-12, §5), the task-completion/detection race (C-13, §5) and the
consistency and edge-case audit (C-14, §5; design §19.2) and unresolved occurrence history (C-15, §5;
design §19.3).
Design: [design.md](design.md). Evidence: [audit.md](audit.md).
Validation plan: [validation.md](validation.md).

Legend for "Basis": **M** = supported by a measurement in this audit;
**A** = architectural reasoning / existing approved decisions;
**E** = estimate. "Needs" names what the operator must decide.

## 1. Decision register

| ID | Decision (proposed) | Alternatives considered | Basis | Status |
|---|---|---|---|---|
| P10-D1 | **Index technology: Qdrant 1.15.1**, a fingerprinter-owned network service; FAISS `IndexFlatIP` only as the offline oracle (ADR-028) | in-process FAISS (HNSW, IVF-PQ) as per-host replicas; a self-built FAISS index service; Scylla vector search; Milvus; pgvector | M: all variants within ≈ 2 % segment recall and 0.3 pp media recall of exact (audit §9.1, F-4); Qdrant int8 recall@100 0.992 at 2 ms p50 HTTP; Scylla 6.2.3 has no vector search. A: multi-host shared visibility and read-your-writes (design §4.1, §6.4) | PROPOSED — **needs operator approval** (new external service) |
| P10-D2 | Collections per (spec digest, role ∈ {media, target}, generation); **the active generation is a static column in Scylla `index_generations`, promoted by one LWT** (a Qdrant alias is cosmetic only); no cross-spec search; **both roles int8 in RAM + float32 originals on disk + rescoring** (corrected C-1, C-3) | one collection with a spec payload filter; alias as the reader's source of truth; target role float32 in RAM | A: P9 identity; single-LWT atomic promotion. M: int8 recall@100 0.992. D: base target raw FP32 = 1.717 GiB > 1.5 GiB | PROPOSED (changed by C-1/C-3) |
| P10-D3 | Deployment: Qdrant run by the fingerprinter (own dev compose file in this repository, loopback/backend network, API key, `mem_limit` 1.5 GiB, 2 CPUs); production placement on a dedicated index host decided in P14 | add Qdrant to crawler2's compose (crawler2 change); run it inside the matcher process (embedded) | M: OOM at 256 MiB, peak = cap at 512 MiB during a 104 k build (F-5). A: service-owned infrastructure | PROPOSED — **needs approval** (where it runs) |
| P10-D4 | Point id = UUIDv5(`representation_id:seq:segment_index`) (target role: `target_representation_id:seq:segment_index`), `seq` = the applied intent's `index_entries` sequence; payload of ids, timestamps and `seq`; payload indexes include `seq` (integer) (changed C-12) | seq-independent ids (first proposal — a stale UPSERT overwrites and a stale subject-only DELETE removes the current points); sequential ids kept in Scylla; Qdrant-generated ids | A: idempotent upserts per intent across hosts and rebuilds; operations of an older intent cannot overwrite or (with `seq ≤` deletes) remove a newer intent's points; index ids never leave the index | PROPOSED (changed by C-12) |
| P10-D5 | Work discovery: **P10-owned** group-less hint reader of `representation.ready` (cursor advanced by LWT only after durable work, epoch-fenced lease) + **P10-owned backstop scans** of `representations_by_spec` (per-bucket cursor) and `target_representations` (**partition-granular** token walk, cursor = last fully processed partition, `>=` resume — C-8); target events are held by P9 and never read; **P9 code, stream registrations and the P9 reconciler unchanged** (corrected C-4) | (a) P9 commit follow-up `f_match`; (b) `matcher` consumer group (P9 handoff); (c) a P10 pass inside P9's reconciler | M/A: `publish.py` trims with `XTRIM MINID` for registered groups only (hints may vanish) and holds target events; `intake.py` lease pattern. **Deviates from the P9 exit §7 handoff** | PROPOSED — **needs approval** (clarified by C-4, C-8) |
| P10-D6 | Candidate generation: adaptive k (k₀ 32, double while the k-th score ≥ floor, up to k_max 256/512/512 for A/B/equivalence), floors 0.60/0.70/0.70, ≥ 2 distinct query-segment hits, caps (A 50 targets; B pages of 200, ≤ 5,000 per task; equivalence ≤ 2,000), saturation and truncation recorded, **a saturated segment or a cap ⇒ run INCOMPLETE in every direction** (C-14), one pinned generation per search step with a step deadline (C-14), no fallback | fixed top-k + hard cap; similarity-range search without k; coarse-vector shortlist | M: fixed k=100/cap 200 → recall 0.745 at 19× growth; adaptive → 0.9965 (audit §10.1); coarse vectors overlap (F-3). Values provisional until tuned (validation §3) | PROPOSED (values provisional) |
| P10-D7 | Verifier `temporal/v2`: seconds-based offsets with ±d phase tolerance, up to 3 offset modes **taken from the dense similarity of the two artifacts, never from ANN hits** (C-14: inputs = two artifacts + params + exclusion set), segments with a *usable* low-information flag (design §7.3) excluded from runs (not usable ⇒ counted), all runs ≥ L reported, thresholds fitted on a grouped training split; V1 kept frozen as `temporal_v1` | keep V1 with new thresholds only; learned verifier (model); DTW | M: phase is the dominant loss (F-1); V1 at 0.80/3 reaches R 0.693 with 0 FP in-sample (§7.3, exploratory). A: keeps V1's proven run structure | PROPOSED — **needs approval** (semantics change vs V1) |
| P10-D8 | Low-information rule `lowinfo/v1` (**provisional**): only target segments are classified (≥ 0.95 to a different target work); `target_set_rev` orders refresh work but does **not** certify freshness; a segment is excluded only through a **usable** flag — COMPLETE record **and** ≥ 1 recorded witness authoritatively live at check time (P9 point reads) (C-9); removals found from witness lists, additions by exact (`exact`, `quantization.ignore`, `score_threshold`) searches, a result of size `L` = truncated ⇒ not complete; LWT `IF rev <`; media never flagged; **anything unproven ⇒ eligible**; witness cosine confirmed on SHA-verified artifact vectors, never on the Qdrant score alone (C-13); in direction A a hit on a flagged segment does not vote, the query segment's other hits do (per-hit, C-14); enabled only if the offline oracle comparison shows `lost_tp = 0` (corrected C-2) | stored media flags (first proposal); IDF over media; no filter | M: negatives reach cosine 1.0 (F-2). Effect **not measured** | PROPOSED — provisional, measured in I-1 (changed by C-2, C-9) |
| P10-D9 | `confidence` = **calibrated probability that a proposed, verified match is correct** (isotonic if the data minimum is met, else Platt if reliable, else UNAVAILABLE); operating-point precision/recall reported separately; nested film-grouped protocol; **publish only with a CALIBRATED technique version**, otherwise store and hold; `probable` stored, not published by default (corrected C-5); held release and envelope written once by LWT; STALE stops new publications, nothing published is retracted (C-14) | "precision among decisions at least this strong" (first proposal); mean similarity as confidence; publish uncalibrated | A: statistics; contract field meaning; 7 correlated film groups | PROPOSED — **needs approval** (changed by C-5) |
| P10-D10 | `technique = dinov2-seg-temporal / 2.<n>`, `<n>` bound to an immutable `verifier_params` row; any decision-affecting change ⇒ new version ⇒ new `match_id` | version only on algorithm change (V1 practice) | A: `MatchId` derivation (audit §5) | PROPOSED |
| P10-D11 | Matches authoritative in Scylla: `matches` PK `content_id`, CK `(target_id, target_version, media_spec_digest, target_spec_digest, technique_name, technique_version)` (contains every MatchId input; verified injective), plus `matches_by_target`, `matches_by_id`; commit via intents; **the first committed decision per `match_id` is final**, later divergent results recorded, not applied (C-14); `match.found` 1.0 via held publication until P11/P12 register (corrected C-6) | key `(content_id; target_id, target_version, technique_version)` — **collides** across specs and technique names | A/verified: `ids.py` (`MatchId.of`, `_derive`), `spec.py` (digest determines name/version), `target_representations` CK has `spec_digest` | PROPOSED (changed by C-6) |
| P10-D12 | No contract change for M3. Additive `match.found` 1.1 with optional `provenance` (ANN params, completeness) proposed for later | change field meanings; second event type | A: contracts additive; consumers absent | **DEFERRED** (crawler2 contract change, separate approval) |
| P10-D13 | Level-5 equivalence: `media_vs_media` tasks + rule `equiv/v1` (≥ 90 % mutual coverage of all segments — C-14, duration Δ ≤ max(5 s, 2 %), \|offset\| ≤ 10 s); reversible `equivalence_links`; no ContentId merge. (b) a `media.equivalence_linked` event | do it in crawler2; level-5 by cosine of coarse vectors | A: P8 identity hierarchy; F-3 rules out coarse vectors | (a) PROPOSED — **needs approval**; (b) **DEFERRED** |
| P10-D14 | Resources: dev Qdrant **hard 1.5 GiB** (changed only by a separate decision), both roles int8 + on-disk originals, capacity guard `index.max_points` = **250,000 resident points per Qdrant instance**, ledger lowered **only by the capacity audit** (C-14) (all specs, roles and existing generations, both generations during a rebuild; reservation ledger, rebuild admission) **provisional** until E-R1; E-R1 on a dedicated container counting the same way, its rebuild phase held at ≥ the admission requirement `R_req` = 1,304,649 resident (incl. a resident media-role collection — C-12, C-13); the limit is set only to a measured passing total, the margin applied once by the guard (C-13), never raised by a failure (C-10); matcher ≤ 1 GiB; production on a dedicated index host (P14); raw / steady / build-peak / disk separated (corrected C-1) | 1.5 GiB with float32 targets and "≈ 1 M vectors" (first proposal); raise the limit | M: steady 1,315 B/vector; peak > 256 MiB and ≤ 512 MiB for 104 k. D: base target scenario peak 1.32–2.76 GiB by linear extrapolation | PROPOSED — **unresolved pending E-R1** (changed by C-1, C-10) |
| P10-D15 | Brute force only as the offline oracle; never a production fallback | exact fallback when the index is down | plan B.5 #4 | PROPOSED (restates the invariant) |
| P10-D16 | P10 documents live in `fingerprinter/docs/phases/p10-vector-index-matcher/` until the operator decides whether to move or link them into crawler2's phase tree, ADR index (ADR-028) and phase plan | write them in crawler2 (forbidden this session) | session constraint | **needs operator decision** |
| P10-D17 | M3 baseline: the **reproducible V1 end-to-end baseline** (V1 worker handler, coarse screen, registry, aggregation) is the first task of I-1 and a **prerequisite** of verifier threshold selection, any V1-parity claim and M3; gate against the stronger of it and the V1-matcher-on-V2-vectors number, on identical outer-fold held-out pairs (corrected C-7) | only the P9 oracle number; measuring V1 after Qdrant integration | M (oracle); entry point verified (`worker/matching_handler.py`) | PROPOSED — **needs approval** (clarified by C-7) |
| P10-D18 | Level-5 test set: new **E-SET** built from S-SRC excerpts (≥ 60 s) with P8's transforms (bitrate, container remux, fragmented MP4, WebM); P8's original 6 s set is run and reported as below the minimum verifiable duration | use only P8's set | M/A: P8 set has 2 segments per item (R-6) | PROPOSED — **needs approval** |
| P10-D19 | Index mutation and rebuild protocol: `index_entries` row per subject is the durable intent (desired op/version, `seq`) written before any Qdrant call; idempotent apply; bookkeeping `IF seq = s`; Qdrant-side verification for unfenceable stale writes; generation state machine CREATED→BUILDING→CATCHING_UP→VALIDATING→READY→ACTIVE→DRAINING→RETIRED (+FAILED); build lease; completeness proof; manifest digests; single-LWT promotion (new, C-3); guarantees A–G and routing races stated in design §12.9: no Qdrant write without a read-back intent, stale points never accepted as evidence (candidates checked against live P9 rows and verified on artifacts), DELETE rows kept until every routed generation applied them, promotion only after catch-up + authoritative coverage + no-extras + digests, generation numbers never reused (clarified C-11); **UPSERT u writes ids of u then deletes `seq < u`, DELETE d deletes `seq ≤ d`**, so a stale operation cannot remove a newer intent's points (strict S2); **index-integrity recovery (design §12.10, corrected C-13, C-14, C-15):** a non-protocol point loss opens a durable `index_repairs` **occurrence — its own row (CK `subject_id, generation, occurrence`), its own `occurrence_id`, never reset or deleted** — before any repair (verification, apply pre-count, audit, DRAINING post-horizon pass); every step a single-row LWT on that row; REPAIRED fenced against concurrent detections (`detections`); the task map `recovery_runs` is keyed by `occurrence_id`, so an earlier occurrence's run can never satisfy a later one; closed only after a run of each of the lost subject's own tasks numbered after **that occurrence's** repair has finished (run bump fenced by single-row LWT on the task row); `RERUN_INCOMPLETE`/`TASK_FAILED` closes stay unresolved on their own rows until a later DONE run of every required kind (or the subject's retirement) resolves them, and no later occurrence can hide them; other tasks are not gated — DONE asserts no index integrity, negatives are provisional until `recovery_open = 0` and `recovery_unresolved = 0` (exact `settled` rule, design §12.10); promotion fenced on `detect_count`. *The C-12 text "no task DONE on it" is withdrawn* (it was a cross-partition check-then-act) | upsert-then-record without intents; alias switch as promotion; per-mutation log replay; a generation-health fence on task completion (rejected C-13: cross-partition, hotspot, cannot cover tasks completed before detection) | A: P9 LWT/lease/intent patterns; Qdrant has no conditional writes; Scylla LWT is single-partition | PROPOSED — **needs approval** before I-2 (changed by C-13) |
| P10-D20 | P10-owned reconciler (`matcher reconcile run`): intents, backstop scans, Qdrant verification, generation watchdog, generation audit, **`repairs` pass driving every `index_repairs` occurrence to CLOSED and re-checking unresolved closes** (C-13, C-14), capacity audit as the only path that lowers the ledger (C-14), lowinfo refresh, capacity audit; repair table with detection, idempotency, metric and test per divergence (design §12.6, §12.10); separate from P9's `reconcile run` (new, C-4; clarified C-11, C-13) | a P10 pass inside P9's reconciler | A: P9/P10 ownership boundary | PROPOSED — **needs approval** before I-2 |

## 2. Decisions requiring operator approval before implementation

1. **P10-D1 + ADR-028** — Qdrant as the index (new service).
2. **P10-D3** — where Qdrant runs (fingerprinter-owned dev compose).
3. **P10-D5** — triggers without a consumer group (deviation from P9 handoff).
4. **P10-D7, D8, D9, D10** — verifier semantics, low-information rule,
   confidence meaning, technique versioning.
5. **P10-D13 (a)** — equivalence rule and persistence.
6. **P10-D14** — resource budgets; the dev capacity stays **unresolved**
   until E-R1 (design §13.4).
7. **P10-D19, P10-D20** — mutation/rebuild protocol (with the guarantees
   of design §12.9), the index-integrity recovery protocol and invariant R
   (design §12.10, incl. the run-numbered task layout of §11.1 and the
   `index_repairs` ledger), and the P10-owned reconciler (required before
   I-2). Approving D19 means accepting that a task may finish DONE during
   an undetected loss, that every "no match" is provisional until
   `recovery_open = 0` and `recovery_unresolved = 0` (counted over every
   occurrence row, older occurrences included), that an occurrence whose
   rerun FAILED and is never retried keeps the spec's negatives provisional
   indefinitely, and that even
   `settled` means "no known uncertainty", not proof of absence (no strict
   prevention is claimed).
8. **P10-D16** — documentation location.
9. **P10-D17** — M3 baseline definition.
10. **P10-D18** — the E-SET for the level-5 test.

D2, D4, D6, D11, D15 follow from these and are listed for review.

Measurements still required before the related decision can be settled:
E-R1 (D14, before I-2); V1 end-to-end baseline (D17, first in I-1);
lowinfo oracle comparison (D8); calibration adequacy (D9). Runtime checks
that can revise D19 after approval: R-24 (Qdrant 1.15.1 filter semantics,
first I-2 test — a failure revises the §12.2 protocol before further I-2
work) and R-26/R-27/R-28/R-29 (LWT execution semantics of the run fence and of
occurrence identity, I-3 exit); CAP-9 (the capacity admission batch, I-2
exit).

## 3. Deferred

- P10-D12 `match.found` 1.1 provenance; P10-D13 (b) equivalence event —
  crawler2 contract changes, before P11/P12 consume them.
- Denser sampling spec (audit F-1) — a P9-style spec + backfill decision.
- Qdrant replication/sharding, index host sizing — P14.

## 4. Risks (from audit §13) and mitigations

| Risk | Mitigation in the design |
|---|---|
| R-1 recall vs V1 | `temporal/v2` + tuning (I-1 first) |
| R-2 identical frames | `lowinfo/v1`, run length, held-out FP checks on N-SOAK |
| R-3 saturation | adaptive k + saturation reporting |
| R-4 index size | dev cap; P14 host; PQ/on-disk options re-measured before use |
| R-5 rebuild time | rebuild while serving; target role first; I-2 measures gRPC/parallel rates |
| R-6 P8 set too short | E-SET (D18) |
| R-7 corpus homogeneity | limitation recorded; thresholds re-evaluated when live-action labelled data exist |
| R-8 small negative set | N-SOAK provenance negatives added (validation §2) |
| R-9 stream coupling | D5 |
| R-10 contract gaps | D12/D13(b) deferred, provenance in Scylla |
| R-11 new service | pinned image, health checks, rebuild procedure, no authoritative state in it |
| R-12 drift | measured on VFR items, documented |

## 5. Correction log (2026-10-15)

The full record (original assumption, why unsafe, correction, evidence,
outstanding work, gate) is design §19. Decision impact:

| Correction | Decisions | Impact |
|---|---|---|
| C-1 resource budget | D2, D14 | changed; dev capacity unresolved pending E-R1 |
| C-2 low-information freshness | D8 | changed; still provisional |
| C-3 Qdrant/Scylla mutation and rebuild | D19 (new), D2 | new decision; D2 clarified |
| C-4 P9/P10 ownership and reconciliation | D20 (new), D5 | new decision; D5 clarified (no P9 change) |
| C-5 confidence semantics | D9 | changed |
| C-6 `matches` key vs MatchId | D11 | changed (verified against code) |
| C-7 V1 end-to-end baseline | D17 | clarified as an I-1 prerequisite |
| C-8 targets backstop pagination | D5, D20 | clarified (no P9 change) |
| C-9 low-information freshness and enumeration | D8 | changed; still provisional |
| C-10 capacity accounting and E-R1 | D14 | changed; dev capacity still unresolved pending E-R1 |
| C-11 mutation/rebuild guarantees | D19, D20 | clarified |
| C-12 stale DELETE after a newer UPSERT (2026-10-17) | D4, D19; D14 (E-R1 composition) | D4 changed (point id includes `seq`); D19 and D14 clarified |
| C-13 task completion vs loss detection; DRAINING horizon; witness cosine; E-R1 sizing (2026-10-17) | D19, D20; D8; D14 | D19 changed (C-12 "no DONE while suspect" withdrawn; recovery protocol §12.10); D20 clarified (`repairs` pass, audit); D8 clarified (witness cosine on artifacts); D14 clarified (E-R1 phase 3 ≥ `R_req`, margin applied once) |
| C-14 consistency and edge-case audit (2026-10-17; design §19.2): occurrence identity (C-13 reused one id), detection vs repair fence, unresolved closes and the `settled` rule, verifier independent of ANN, first decision final, saturation ⇒ INCOMPLETE, per-hit exclusion, pinned search step, liveness predicate, audit-only capacity release, lost ACTIVE/DRAINING collections, `equiv/v1` coverage | D19, D20; D7; D6; D8; D9, D11; D13; D14 | D19 changed (occurrence-scoped protocol, unresolved closes); D20 clarified; D7 changed (offset source); D6 changed (saturation outcome); D8 changed (per-hit); D9/D11 clarified (publication idempotency, first decision final); D13 clarified (coverage); D14 changed (release only by audit) |
| C-15 unresolved occurrence history (2026-10-17; design §19.3): C-14 reset one row per (subject, generation), so an unresolved occurrence vanished from `recovery_unresolved` when a later one opened; retired subjects could leave an occurrence unresolved forever | D19, D20 | D19 changed (one row per occurrence, per-occurrence resolution with evidence); D20 clarified (`repairs` pass scans every occurrence row) |

No decision was approved by any correction; no new decision was needed
for C-8…C-15 (each changes or clarifies an existing one).
