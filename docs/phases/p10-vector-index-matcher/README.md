# P10 — vector index and matcher (milestone M3)

Status (2026-10-18): **IMPLEMENTATION COMPLETE FOR THE APPROVED DESIGN; M3 NOT
PASSED.** Implemented under the operator's authorization (decisions.md §0).
Executed evidence is recorded only in
[validation.md §13](validation.md#13-execution-record-implementation-phase-from-2026-10-18);
the M3 checklist is [validation.md §11](validation.md#11-m3-exit-checklist-filled-2026-10-18-from-13);
the local operator guide is [`docs/p10-matcher.md`](../../p10-matcher.md);
implementation notes and design corrections found by testing are design §20.

| Status | P10 (2026-10-18) |
|---|---|
| decisions | D1–D20 approved for implementation; D12, D13 (b) deferred; D8 implemented, not enabled (gate failed); D9: technique 2.1 calibration UNAVAILABLE (held); D14 provisional 250,000 — E-R1 **failed** |
| implemented | vector index adapter + dev service, generations, mutation protocol, builder/validation/promotion, capacity ledger, recovery protocol, reconciler, discovery, run-fenced tasks in both directions + media↔media, verifier `temporal/v2`, `equiv/v1`, technique registry, held `match.found` 1.0, CLI |
| tests | 75 integration/unit tests pass on the real Scylla 6.2.3, Qdrant 1.15.1, MinIO, Redis; P9 unit suite unchanged (100 pass) |
| passes | R-24; Scylla LWT forms incl. §9a.4; M3-1 (live, held-out: L-PAIRS P 1.000 / R 0.733, 0 FP vs V1 R 0.247; pooled P 0.9989 / R 0.812 vs V1 P 0.9857 / R 0.449); M3-2; drop/rebuild (executed scope); growth bounds; segment recall@100 ≥ 0.97 |
| fails | E-R1 (query latency on the HDD dev host under 1.5 GiB); single-query latency criterion; direction-B ANN-vs-exact candidate recall while saturated; E-SET equivalence recall 0.76 (target 1.0); calibration reliability; lowinfo adoption |
| known limits | direction B saturates at `k_max` 512 from 10x (runs INCOMPLETE); 7 animated films from one reel; V1 end-to-end not run on N-SOAK; single-node Scylla; real multi-host is P14 |

**Implementation and validation status per step.**

| Step | Status | Evidence | Remaining |
|---|---|---|---|
| I-1 (a) V1 end-to-end baseline | **done** (L-PAIRS): TP 34, FP 0, R 0.227 | validation §13.4 | N-SOAK not run (cost; limitation recorded) |
| I-1 (b–d) verifier, tuning, calibration, lowinfo offline | **done** | §13.3, `results/tune-eval.json` | calibration UNAVAILABLE, lowinfo not adopted (measured outcomes) |
| R-24 / L-14 / Scylla LWT semantics / §9a.4 | **pass** | §13.1–§13.2 | — |
| E-R1 capacity experiment | **FAILED** (latency) at 6 and 1 copies | §13.5 | operator decision (index host, memory limit or latency criterion) |
| I-2 index, generations, mutations, reconciler, capacity | **implemented, tested** | §13.6, §13.12 | unrun R-/CAP- variants (§11) |
| I-3 tasks, both directions, persistence, held publication, recovery | **implemented, tested** | §13.6–§13.8 | calibration for publication |
| I-4 equivalence | **implemented, tested**; recall target not met | §13.9, §13.11 | equivalence-specific parameters (a new technique version) |
| I-5 failure injection, growth, M3 | **run**; M3 not passed | §11, §13.10, §13.13–§13.15 | the failed and unrun items of §11 |

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
