# P10 Decisions — vector index and matcher

Status: **all decisions PROPOSED; none approved.** Date 2026-10-15.
Design: [design.md](design.md). Evidence: [audit.md](audit.md).
Validation plan: [validation.md](validation.md).

Legend for "Basis": **M** = supported by a measurement in this audit;
**A** = architectural reasoning / existing approved decisions;
**E** = estimate. "Needs" names what the operator must decide.

## 1. Decision register

| ID | Decision (proposed) | Alternatives considered | Basis | Status |
|---|---|---|---|---|
| P10-D1 | **Index technology: Qdrant 1.15.1**, a fingerprinter-owned network service; FAISS `IndexFlatIP` only as the offline oracle (ADR-028) | in-process FAISS (HNSW, IVF-PQ) as per-host replicas; a self-built FAISS index service; Scylla vector search; Milvus; pgvector | M: all variants within ≈ 2 % segment recall and 0.3 pp media recall of exact (audit §9.1, F-4); Qdrant int8 recall@100 0.992 at 2 ms p50 HTTP; Scylla 6.2.3 has no vector search. A: multi-host shared visibility and read-your-writes (design §4.1, §6.4) | PROPOSED — **needs operator approval** (new external service) |
| P10-D2 | Collections per (spec digest, role ∈ {media, target}, generation), an alias per (spec, role); no cross-spec search; int8 + on-disk originals for media, float32 in RAM for targets; immutable generation params | one collection with a spec payload filter; mixed-spec collections | A: P9 identity (precision in the digest); atomic promotion via aliases. M: int8 config measured | PROPOSED |
| P10-D3 | Deployment: Qdrant run by the fingerprinter (own dev compose file in this repository, loopback/backend network, API key, `mem_limit` 1.5 GiB, 2 CPUs); production placement on a dedicated index host decided in P14 | add Qdrant to crawler2's compose (crawler2 change); run it inside the matcher process (embedded) | M: OOM at 256 MiB, peak = cap at 512 MiB during a 104 k build (F-5). A: service-owned infrastructure | PROPOSED — **needs approval** (where it runs) |
| P10-D4 | Point id = UUIDv5(`representation_id:segment_index`) (target role: `target_representation_id:segment_index`); payload of ids and timestamps only | sequential ids kept in Scylla; Qdrant-generated ids | A: idempotent upserts across hosts and rebuilds; index ids never leave the index | PROPOSED |
| P10-D5 | Work trigger: **group-less hint reader** of `representation.ready`/`target.registered` + Scylla **backstop scan** of `representations_by_spec` and `target_representations`; **P9 code unchanged**; no `matcher` consumer group | (a) additive P9 commit follow-up `f_match`; (b) `matcher` consumer group as P9's handoff suggested | A: a group would let a stopped matcher hold `representation.ready` entries and couple P9's publication to P10 health (audit R-9); ADR-025 precedent for group-less reading. **Deviates from the P9 exit §7 handoff** ("register the matcher group") | PROPOSED — **needs approval** (deviation from the handoff) |
| P10-D6 | Candidate generation: adaptive k (k₀ 32, double while the k-th score ≥ floor, up to k_max 256/512/512 for A/B/equivalence), floors 0.60/0.70/0.70, ≥ 2 distinct query-segment hits, caps (A 50 targets; B pages of 200, ≤ 5,000 per task; equivalence ≤ 2,000), saturation and truncation recorded, INCOMPLETE outcome, no fallback | fixed top-k + hard cap; similarity-range search without k; coarse-vector shortlist | M: fixed k=100/cap 200 → recall 0.745 at 19× growth; adaptive → 0.9965 (audit §10.1); coarse vectors overlap (F-3). Values provisional until tuned (validation §3) | PROPOSED (values provisional) |
| P10-D7 | Verifier `temporal/v2`: seconds-based offsets with ±d phase tolerance, up to 3 offset modes, low-information segments excluded from runs, all runs ≥ L reported, thresholds fitted on a grouped training split; V1 kept frozen as `temporal_v1` | keep V1 with new thresholds only; learned verifier (model); DTW | M: phase is the dominant loss (F-1); V1 at 0.80/3 reaches R 0.693 with 0 FP in-sample (§7.3, exploratory). A: keeps V1's proven run structure | PROPOSED — **needs approval** (semantics change vs V1) |
| P10-D8 | Low-information rule `lowinfo/v1`: segments near-identical (≥ 0.95) across *different* target works are non-informative; media segments nearest to them too; operator-curated stop list as fallback | ignore the problem (rely on run length); IDF-weighting over the media corpus | M: negatives reach cosine 1.0 (F-2). E: the rule's effect is **not yet measured** | PROPOSED — measured in I-1 before adoption |
| P10-D9 | `confidence` = calibrated estimated precision (isotonic on the calibration fold); `confirmed` at the zero-FP operating point; `probable` stored, **not published** by default | publish mean similarity as confidence (V1 `score`); publish probable | A: contract semantics ("confidence" ∈ [0,1], `probable` needs review) | PROPOSED — **needs approval** |
| P10-D10 | `technique = dinov2-seg-temporal / 2.<n>`, `<n>` bound to an immutable `verifier_params` row; any decision-affecting change ⇒ new version ⇒ new `match_id` | version only on algorithm change (V1 practice) | A: `MatchId` derivation (audit §5) | PROPOSED |
| P10-D11 | Matches authoritative in Scylla (`matches`, `matches_by_target`); commit via intents; `match.found` 1.0 via the P9 publisher with **held publication** until P11/P12 register | publish immediately without a required group | A: ADR-026 pattern; no consumer exists yet | PROPOSED |
| P10-D12 | No contract change for M3. Additive `match.found` 1.1 with optional `provenance` (ANN params, completeness) proposed for later | change field meanings; second event type | A: contracts additive; consumers absent | **DEFERRED** (crawler2 contract change, separate approval) |
| P10-D13 | Level-5 equivalence: `media_vs_media` tasks + rule `equiv/v1` (≥ 90 % mutual coverage, duration Δ ≤ max(5 s, 2 %), \|offset\| ≤ 10 s); reversible `equivalence_links`; no ContentId merge. (b) a `media.equivalence_linked` event | do it in crawler2; level-5 by cosine of coarse vectors | A: P8 identity hierarchy; F-3 rules out coarse vectors | (a) PROPOSED — **needs approval**; (b) **DEFERRED** |
| P10-D14 | Resource budgets: dev Qdrant 1.5 GiB, matcher ≤ 1 GiB each; dev capacity ≈ 1 M media vectors; beyond that a dedicated index host (P14) | put the index on the 15 GB host at all volumes | M: 1.3 KB/vector resident (int8, 104 k). E: production table design §13.2 | PROPOSED — **needs approval** |
| P10-D15 | Brute force only as the offline oracle; never a production fallback | exact fallback when the index is down | plan B.5 #4 | PROPOSED (restates the invariant) |
| P10-D16 | P10 documents live in `fingerprinter/docs/phases/p10-vector-index-matcher/` until the operator decides whether to move or link them into crawler2's phase tree, ADR index (ADR-028) and phase plan | write them in crawler2 (forbidden this session) | session constraint | **needs operator decision** |
| P10-D17 | M3 baseline: report both V1 baselines — V1 matcher on V2 vectors (measured: P 1.000 / R 0.247) and V1 end to end (to be measured in I-1); the gate compares against the **stronger** of the two on the held-out evaluation | only the P9 oracle number | M (oracle); gap (end-to-end not measured) | PROPOSED — **needs approval** |
| P10-D18 | Level-5 test set: new **E-SET** built from S-SRC excerpts (≥ 60 s) with P8's transforms (bitrate, container remux, fragmented MP4, WebM); P8's original 6 s set is run and reported as below the minimum verifiable duration | use only P8's set | M/A: P8 set has 2 segments per item (R-6) | PROPOSED — **needs approval** |

## 2. Decisions requiring operator approval before implementation

1. **P10-D1 + ADR-028** — Qdrant as the index (new service).
2. **P10-D3** — where Qdrant runs (fingerprinter-owned dev compose).
3. **P10-D5** — triggers without a consumer group (deviation from P9 handoff).
4. **P10-D7, D8, D9, D10** — verifier semantics, low-information rule,
   confidence meaning, technique versioning.
5. **P10-D13 (a)** — equivalence rule and persistence.
6. **P10-D14** — resource budgets and the dev-capacity limit.
7. **P10-D16** — documentation location.
8. **P10-D17** — M3 baseline definition.
9. **P10-D18** — the E-SET for the level-5 test.

D2, D4, D6, D11, D15 follow from these and are listed for review.

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
