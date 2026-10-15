# P10 Decisions — vector index and matcher

Status: **all decisions PROPOSED; none approved.** Date 2026-10-15;
corrected the same day after the first review (C-1…C-7, §5).
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
| P10-D4 | Point id = UUIDv5(`representation_id:segment_index`) (target role: `target_representation_id:segment_index`); payload of ids and timestamps only | sequential ids kept in Scylla; Qdrant-generated ids | A: idempotent upserts across hosts and rebuilds; index ids never leave the index | PROPOSED |
| P10-D5 | Work discovery: **P10-owned** group-less hint reader of `representation.ready` (cursor advanced by LWT only after durable work, epoch-fenced lease) + **P10-owned backstop scans** of `representations_by_spec` (per-bucket cursor) and `target_representations` (token-range cursor); target events are held by P9 and never read; **P9 code, stream registrations and the P9 reconciler unchanged** (corrected C-4) | (a) P9 commit follow-up `f_match`; (b) `matcher` consumer group (P9 handoff); (c) a P10 pass inside P9's reconciler | M/A: `publish.py` trims with `XTRIM MINID` for registered groups only (hints may vanish) and holds target events; `intake.py` lease pattern. **Deviates from the P9 exit §7 handoff** | PROPOSED — **needs approval** (clarified by C-4) |
| P10-D6 | Candidate generation: adaptive k (k₀ 32, double while the k-th score ≥ floor, up to k_max 256/512/512 for A/B/equivalence), floors 0.60/0.70/0.70, ≥ 2 distinct query-segment hits, caps (A 50 targets; B pages of 200, ≤ 5,000 per task; equivalence ≤ 2,000), saturation and truncation recorded, INCOMPLETE outcome, no fallback | fixed top-k + hard cap; similarity-range search without k; coarse-vector shortlist | M: fixed k=100/cap 200 → recall 0.745 at 19× growth; adaptive → 0.9965 (audit §10.1); coarse vectors overlap (F-3). Values provisional until tuned (validation §3) | PROPOSED (values provisional) |
| P10-D7 | Verifier `temporal/v2`: seconds-based offsets with ±d phase tolerance, up to 3 offset modes, segments with a *current* low-information flag excluded from runs (stale ⇒ counted), all runs ≥ L reported, thresholds fitted on a grouped training split; V1 kept frozen as `temporal_v1` | keep V1 with new thresholds only; learned verifier (model); DTW | M: phase is the dominant loss (F-1); V1 at 0.80/3 reaches R 0.693 with 0 FP in-sample (§7.3, exploratory). A: keeps V1's proven run structure | PROPOSED — **needs approval** (semantics change vs V1) |
| P10-D8 | Low-information rule `lowinfo/v1` (**provisional**): only target segments are classified (≥ 0.95 to a different target work) at a durable `target_set_rev`; exact affected-set reclassification over the target role; LWT `IF rev <`; media never flagged; **stale or unknown flags ⇒ eligible**; enabled only if the offline oracle comparison shows `lost_tp = 0` (corrected C-2) | stored media flags (first proposal); IDF over media; no filter | M: negatives reach cosine 1.0 (F-2). Effect **not measured** | PROPOSED — provisional, measured in I-1 (changed by C-2) |
| P10-D9 | `confidence` = **calibrated probability that a proposed, verified match is correct** (isotonic if the data minimum is met, else Platt if reliable, else UNAVAILABLE); operating-point precision/recall reported separately; nested film-grouped protocol; **publish only with a CALIBRATED technique version**, otherwise store and hold; `probable` stored, not published by default (corrected C-5) | "precision among decisions at least this strong" (first proposal); mean similarity as confidence; publish uncalibrated | A: statistics; contract field meaning; 7 correlated film groups | PROPOSED — **needs approval** (changed by C-5) |
| P10-D10 | `technique = dinov2-seg-temporal / 2.<n>`, `<n>` bound to an immutable `verifier_params` row; any decision-affecting change ⇒ new version ⇒ new `match_id` | version only on algorithm change (V1 practice) | A: `MatchId` derivation (audit §5) | PROPOSED |
| P10-D11 | Matches authoritative in Scylla: `matches` PK `content_id`, CK `(target_id, target_version, media_spec_digest, target_spec_digest, technique_name, technique_version)` (contains every MatchId input; verified injective), plus `matches_by_target`, `matches_by_id`; commit via intents; `match.found` 1.0 via held publication until P11/P12 register (corrected C-6) | key `(content_id; target_id, target_version, technique_version)` — **collides** across specs and technique names | A/verified: `ids.py` (`MatchId.of`, `_derive`), `spec.py` (digest determines name/version), `target_representations` CK has `spec_digest` | PROPOSED (changed by C-6) |
| P10-D12 | No contract change for M3. Additive `match.found` 1.1 with optional `provenance` (ANN params, completeness) proposed for later | change field meanings; second event type | A: contracts additive; consumers absent | **DEFERRED** (crawler2 contract change, separate approval) |
| P10-D13 | Level-5 equivalence: `media_vs_media` tasks + rule `equiv/v1` (≥ 90 % mutual coverage, duration Δ ≤ max(5 s, 2 %), \|offset\| ≤ 10 s); reversible `equivalence_links`; no ContentId merge. (b) a `media.equivalence_linked` event | do it in crawler2; level-5 by cosine of coarse vectors | A: P8 identity hierarchy; F-3 rules out coarse vectors | (a) PROPOSED — **needs approval**; (b) **DEFERRED** |
| P10-D14 | Resources: dev Qdrant **hard 1.5 GiB** (changed only by a separate decision), both roles int8 + on-disk originals, capacity guard `index.max_points` = **250,000 provisional** until E-R1; matcher ≤ 1 GiB; production on a dedicated index host (P14); raw / steady / build-peak / disk separated (corrected C-1) | 1.5 GiB with float32 targets and "≈ 1 M vectors" (first proposal); raise the limit | M: steady 1,315 B/vector; peak > 256 MiB and ≤ 512 MiB for 104 k. D: base target scenario peak 1.32–2.76 GiB by linear extrapolation | PROPOSED — **unresolved pending E-R1** (changed by C-1) |
| P10-D15 | Brute force only as the offline oracle; never a production fallback | exact fallback when the index is down | plan B.5 #4 | PROPOSED (restates the invariant) |
| P10-D16 | P10 documents live in `fingerprinter/docs/phases/p10-vector-index-matcher/` until the operator decides whether to move or link them into crawler2's phase tree, ADR index (ADR-028) and phase plan | write them in crawler2 (forbidden this session) | session constraint | **needs operator decision** |
| P10-D17 | M3 baseline: the **reproducible V1 end-to-end baseline** (V1 worker handler, coarse screen, registry, aggregation) is the first task of I-1 and a **prerequisite** of verifier threshold selection, any V1-parity claim and M3; gate against the stronger of it and the V1-matcher-on-V2-vectors number, on identical outer-fold held-out pairs (corrected C-7) | only the P9 oracle number; measuring V1 after Qdrant integration | M (oracle); entry point verified (`worker/matching_handler.py`) | PROPOSED — **needs approval** (clarified by C-7) |
| P10-D18 | Level-5 test set: new **E-SET** built from S-SRC excerpts (≥ 60 s) with P8's transforms (bitrate, container remux, fragmented MP4, WebM); P8's original 6 s set is run and reported as below the minimum verifiable duration | use only P8's set | M/A: P8 set has 2 segments per item (R-6) | PROPOSED — **needs approval** |
| P10-D19 | Index mutation and rebuild protocol: `index_entries` row per subject is the durable intent (desired op/version, `seq`) written before any Qdrant call; idempotent apply; bookkeeping `IF seq = s`; Qdrant-side verification for unfenceable stale writes; generation state machine CREATED→BUILDING→CATCHING_UP→VALIDATING→READY→ACTIVE→DRAINING→RETIRED (+FAILED); build lease; completeness proof; manifest digests; single-LWT promotion (new, C-3) | upsert-then-record without intents; alias switch as promotion; per-mutation log replay | A: P9 LWT/lease/intent patterns; Qdrant has no conditional writes | PROPOSED — **needs approval** before I-2 |
| P10-D20 | P10-owned reconciler (`matcher reconcile run`): intents, backstop scans, Qdrant verification, generation watchdog, lowinfo refresh; separate from P9's `reconcile run` (new, C-4) | a P10 pass inside P9's reconciler | A: P9/P10 ownership boundary | PROPOSED — **needs approval** before I-2 |

## 2. Decisions requiring operator approval before implementation

1. **P10-D1 + ADR-028** — Qdrant as the index (new service).
2. **P10-D3** — where Qdrant runs (fingerprinter-owned dev compose).
3. **P10-D5** — triggers without a consumer group (deviation from P9 handoff).
4. **P10-D7, D8, D9, D10** — verifier semantics, low-information rule,
   confidence meaning, technique versioning.
5. **P10-D13 (a)** — equivalence rule and persistence.
6. **P10-D14** — resource budgets; the dev capacity stays **unresolved**
   until E-R1 (design §13.4).
7. **P10-D19, P10-D20** — mutation/rebuild protocol and the P10-owned
   reconciler (required before I-2).
8. **P10-D16** — documentation location.
9. **P10-D17** — M3 baseline definition.
10. **P10-D18** — the E-SET for the level-5 test.

D2, D4, D6, D11, D15 follow from these and are listed for review.

Measurements still required before the related decision can be settled:
E-R1 (D14, before I-2); V1 end-to-end baseline (D17, first in I-1);
lowinfo oracle comparison (D8); calibration adequacy (D9).

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

No decision was approved by this correction.
