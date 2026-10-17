# P10 — vector index and matcher (milestone M3)

Status (2026-10-18): **IMPLEMENTATION IN PROGRESS** under the operator's
authorization to implement P10-D1…D20 as documented (decisions.md §0).
**M3 not passed.** Executed evidence is recorded only in
[validation.md §13](validation.md#13-execution-record-implementation-phase-from-2026-10-18);
the local operator guide is [`docs/p10-matcher.md`](../../p10-matcher.md).

| Status | P10 today (2026-10-18) |
|---|---|
| decisions | D1–D20 approved for implementation; D12, D13 (b) deferred; D8 implemented but not enabled; D14 value provisional (250,000) pending E-R1 |
| implemented and tested | `temporal/v2` verifier and `equiv/v1` (unit); `VectorIndex`/`QdrantIndex` adapter; fingerprinter-owned dev Qdrant (`deploy/qdrant/`) |
| executed gates | R-24 (Qdrant 1.15.1 filter boundaries, bit-identical vectors) **pass**; L-14 Qdrant part **pass**; Scylla 6.2.3 LWT forms incl. CAP-9 batch, promotion batch and the §9a.4 slice read **pass**; I-1 tuning/calibration **run** (validation §13.3) |
| I-1 outcome | held-out (pooled L-PAIRS + N-SOAK, exact candidates) P 0.9989 / R 0.812 vs V1-on-V2 P 0.9857 / R 0.449; L-PAIRS 110/150, 0 FP; calibration **UNAVAILABLE** (publication held); lowinfo **not adopted** |
| running / next | V1 end-to-end baseline (I-1 a); then E-R1 on a dedicated container; I-2…I-5 implementation |
| blocks M3 | V1 E2E baseline, live-path M3 run, E-R1, calibration (publication only), the I-2…I-5 exits of validation §11 |

**Implementation progress (updated as increments land).**

| Step | Status | Evidence | Remaining |
|---|---|---|---|
| I-1 (a) V1 end-to-end baseline | running (L-PAIRS) | — | result; N-SOAK cost decision |
| I-1 (b–d) verifier, tuning, calibration, lowinfo offline | **done** | validation §13.3, `results/tune-eval.json` | — (calibration UNAVAILABLE is the measured outcome) |
| R-24 / L-14 / Scylla LWT semantics / §9a.4 | **pass** | validation §13.1–§13.2, `results/evidence/` | — |
| E-R1 capacity experiment | harness written, smoke-checked | — | the gate run (alone on the host) |
| I-2 index, generations, mutations, reconciler, capacity | in progress | — | code, R-*/CAP-*/H-* tests |
| I-3 tasks, both directions, persistence, held publication, recovery | not started | — | |
| I-4 equivalence / I-5 failure injection, growth, M3 | not started | — | |

**History of the design review.** Status (2026-10-17): **IN DESIGN REVIEW.** Audit complete; design proposed;
no decision approved; nothing implemented; M3 not passed. P9 is complete for
its approved scope and unchanged.
Design corrected after the first review (design §19, C-1…C-7); ready for a
second review; second correction pass (design §19.1, C-8…C-11: targets
scan pagination, low-information freshness, capacity accounting and E-R1,
mutation/rebuild guarantees) done the same day; third pass 2026-10-17
(C-12: stale DELETE after a newer UPSERT, task completion under index loss);
fourth pass 2026-10-17 (C-13: task completion versus loss detection — the
C-12 "no DONE while suspect" rule was a cross-partition check-then-act and
is replaced by the durable recovery protocol of design §12.10; DRAINING
search horizon; witness cosine on artifacts; E-R1 sized to the admission
requirement); fifth pass 2026-10-17 (C-14, design §19.2: consistency and
edge-case audit — recovery occurrences get their own `occurrence_id`
because C-13 let an earlier occurrence's run satisfy a later loss; plus
detection-vs-repair fence, unresolved closes and the `settled` rule,
verifier independent of ANN, first decision per `match_id` final,
saturation ⇒ INCOMPLETE, per-hit lowinfo exclusion, pinned search step,
one liveness predicate, audit-only capacity release, lost ACTIVE/DRAINING
collections, `equiv/v1` coverage); sixth pass 2026-10-17 (C-15, design
§19.3: one `index_repairs` row per occurrence, so an unresolved occurrence
can no longer be hidden by a later one; retired subjects resolve theirs).

| Document | Content |
|---|---|
| [audit.md](audit.md) | existing matcher and P9 representation model, labelled data and V1 baseline, measurements on real vectors, volume estimates, risks |
| [design.md](design.md) | proposed architecture, both matching directions, candidate bounds, verifier, provenance, equivalence, rebuild, budgets |
| [decisions.md](decisions.md) | P10-D1…D20 (all PROPOSED; correction log §5), what needs operator approval |
| [design.md §12.10](design.md#1210-index-integrity-recovery-obligations-and-task-reruns-p10-d19-p10-d20-corrected-c-13-c-14) | authoritative index-integrity recovery protocol (C-13, occurrence identity C-14) |
| [validation.md](validation.md) | test matrix, methodology, acceptance criteria; evidence that exists today |
| [ADR-028-vector-index.md](ADR-028-vector-index.md) | proposed index architecture ADR |

Measurement scripts and result JSONs: `benchmarks/p10-index/`. Vector caches
and the benchmark venv live outside git in `~/anti_piracy/datasets/p10/`.

These documents are in the fingerprinter repository because crawler2 was
read-only during the P10 audit; whether to move or link them into crawler2's
phase tree, ADR index and phase plan is decision P10-D16.
