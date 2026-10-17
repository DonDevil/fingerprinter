# P10 — vector index and matcher (milestone M3)

Status (2026-10-17): **IN DESIGN REVIEW.** Audit complete; design proposed;
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
collections, `equiv/v1` coverage).

| Status | P10 today |
|---|---|
| design corrected | C-1…C-14 ([design.md §19](design.md#19-correction-record-2026-10-15-after-the-first-design-review), [§19.2](design.md#192-fifth-correction-pass-2026-10-17-c-14-consistency-and-edge-case-audit)) |
| design reviewed | C-1…C-12 answered earlier reviews; **C-13 and C-14 await review** |
| decisions approved | **none** (P10-D1…D20 PROPOSED; [decisions.md §2](decisions.md#2-decisions-requiring-operator-approval-before-implementation)) |
| implemented | nothing |
| validated | nothing — every test in [validation.md](validation.md) (incl. R-22…R-28, E-R1, I-1 baseline) is planned and unrun; M3 not passed |
| permitted next | review of C-13/C-14; operator approval decisions; then I-1 (V1 end-to-end baseline first, no Qdrant) and E-R1 (harness on a dedicated container) |
| blocks I-2 | approval of D1–D4, D14, D19, D20; a passed E-R1. R-24 is the first I-2 test and can force a protocol revision |
| blocks M3 | I-1 baseline and held-out P/R, calibration, I-2…I-5 exits, all of validation §11 |

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
