# P10 — vector index and matcher (milestone M3)

Status (2026-10-21, after the exit-gate resolution pass): **IMPLEMENTATION
COMPLETE; M3 NOT PASSED** — level-5 equivalence recall fails on measurement;
E-R1 (fails on the HDD dev host at 1.5 GiB and at 4 GiB), calibration and
the N-SOAK V1 end-to-end comparison are blocked on hardware, labelled data
or owner decisions. Implemented under the operator's authorization
(decisions.md §0). Executed evidence only in
[validation.md §13](validation.md#13-execution-record-implementation-phase-from-2026-10-18)
(this pass: §13.26–§13.33; previous: §13.17–§13.25); the gate table is
[validation.md §11](validation.md#11-m3-exit-checklist-updated-2026-10-21-after-the-exit-gate-resolution-pass-1326-1333);
owner decisions: [decisions.md §6, §6.1](decisions.md#6-owner-decisions-required-after-the-exit-gate-pass-2026-10-18);
local operator guide [`docs/p10-matcher.md`](../../p10-matcher.md);
implementation notes design §20 (N-1…N-28).

| Gate (validation §11) | Status | Key measurement |
|---|---|---|
| M3-1 P/R ≥ V1, zero FP | **PASS** (measured populations) | L-PAIRS 110/40/0/330 (R 0.733) vs V1 R 0.247 / 0.227; pooled P 0.9989 / R 0.812 vs V1-on-V2 P 0.9857 / R 0.449 |
| M3-2 new target vs history, no re-encode | **PASS** | direction B DONE at base/10x/19x; B candidate recall 0.9972 |
| P10-T1 ANN recall + latency | **PASS** | segment recall@100 0.9999 (film) / 0.9949 (clip); media-level ANN = exact in both directions; p95 ≤ 3.4 ms |
| P10-T2 drop/rebuild | **PASS** | manifests equal; decisions identical with ANN and exact search (403/403, 110/110) |
| P10-T3 growth bounds | **PASS** | bounds hold; no saturation up to 19x (`k_max_b` 4096) |
| P10-T4 equivalence | **FAIL** | `equiv/v1` (default) 228/300, `equiv/v2` 280/300 (0.933), 1 s-sampling test spec 286/300 held-out (not registered); 0/12,420 false links in all |
| P10-T5 failures G-* | **PASS** (single host) | all G-1…G-15; real multi-host is P14 |
| P10-T6 / T10 E-R1 | **BLOCKED** (O-1; FAIL on this host) | disk-bound rescoring on a USB HDD, reproduced (5.5 → 3,083 ms p95); 6 copies at 4 GiB (decision run, not gate-eligible) p95 2.8–44.5 s; `max_points` stays 250,000 |
| P10-T7 V1 end to end | PASS (L-PAIRS); N-SOAK **BLOCKED** (O-4, owner: not now) | TP 34, FP 0; N-SOAK ≈ 22 h estimated |
| P10-T8 calibration | **BLOCKED** (data, O-3); publication held | UNAVAILABLE; no hard negatives reach confirmation (1 of 877) |
| P10-T9 lowinfo adoption | not adopted — filter disabled (valid; not an M3 gate) | 12 flagged, `lost_tp` 1 |
| P10-T11 crash/scan/identity/capacity | **PASS** (single host) | every listed case run; 5 defects found and fixed (CAP-4, L-2, L-9, L-14, repairs gauge) |

| Status | P10 (2026-10-21) |
|---|---|
| decisions | D1–D20 approved for implementation; D12, D13 (b) deferred; D6 values re-tuned by measurement (`k_max_b` 4096, `search_ef` 384); D8 implemented, not enabled; D9: technique 2.1 UNAVAILABLE (held); D13 (a): `equiv/v1` default, `equiv/v2` registered; D14 provisional 250,000 — E-R1 **failed**; open owner decisions O-1…O-6 (decisions §6.1) |
| implemented | vector index adapter + dev service, generations, mutation protocol, builder/validation/promotion (with revalidation), capacity ledger (admission-bounded bulk load, CAP-8 evidence check), recovery protocol, reconciler, discovery, run-fenced tasks in both directions + media↔media, verifier `temporal/v2`, versioned `equiv/v1`/`equiv/v2`, technique registry, held `match.found` 1.0, CLI |
| tests | 148 integration/unit tests pass on the real Scylla 6.2.3, Qdrant 1.15.1, MinIO, Redis (validation §13.33); P9 unit suite unchanged (100 pass) |
| known limits | 7 animated films from one reel (equivalence tuning: 5); V1 end-to-end not run on N-SOAK; single-node Scylla; USB-HDD dev host; real multi-host is P14; P9 `publisher republish --stream <other>` defect (O-6) tracked for P9 |

Exit-gate completion pass commits (2026-10-18): `3e52357`, `1142b04`,
`8792515`, `2c62405`, `cfe5137`, `bbe9284`. Exit-gate resolution pass
(2026-10-21, local branch `p10-impl`, not pushed): `939d59e` defects found
by the remaining T11 matrix cases, with their tests; `bf10cab` R-26/R-27/R-29
variants; `63d395f` equivalence density experiment; `b334497` R-25 (b)–(d) tests,
E-R1 reproduction and the 4 GiB decision measurement; followed by the
documentation commit that adds this paragraph.

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
| [decisions.md](decisions.md) | P10-D1…D20 (approved for implementation, §0; correction log §5), owner decisions O-1…O-6 (§6, §6.1) |
| [design.md §12.10](design.md#1210-index-integrity-recovery-obligations-and-task-reruns-p10-d19-p10-d20-corrected-c-13-c-14) | authoritative index-integrity recovery protocol (C-13, occurrence identity C-14) |
| [validation.md](validation.md) | test matrix, methodology, acceptance criteria; evidence that exists today |
| [ADR-028-vector-index.md](ADR-028-vector-index.md) | proposed index architecture ADR |

Measurement scripts and result JSONs: `benchmarks/p10-index/`. Vector caches
and the benchmark venv live outside git in `~/anti_piracy/datasets/p10/`.

These documents are in the fingerprinter repository because crawler2 was
read-only during the P10 audit; whether to move or link them into crawler2's
phase tree, ADR index and phase plan is decision P10-D16.
