# P10 — vector index and matcher (milestone M3)

Status (2026-10-15): **IN DESIGN REVIEW.** Audit complete; design proposed;
no decision approved; nothing implemented; M3 not passed. P9 is complete for
its approved scope and unchanged.
Design corrected after the first review (design §19, C-1…C-7); ready for a
second review; second correction pass (design §19.1, C-8…C-11: targets
scan pagination, low-information freshness, capacity accounting and E-R1,
mutation/rebuild guarantees) done the same day. Open before implementation:
E-R1 (capacity), approval of P10-D19/D20 (mutation/rebuild protocol, P10-owned
reconciler); I-1 starts with the V1 end-to-end baseline.

| Document | Content |
|---|---|
| [audit.md](audit.md) | existing matcher and P9 representation model, labelled data and V1 baseline, measurements on real vectors, volume estimates, risks |
| [design.md](design.md) | proposed architecture, both matching directions, candidate bounds, verifier, provenance, equivalence, rebuild, budgets |
| [decisions.md](decisions.md) | P10-D1…D20 (all PROPOSED; correction log §5), what needs operator approval |
| [validation.md](validation.md) | test matrix, methodology, acceptance criteria; evidence that exists today |
| [ADR-028-vector-index.md](ADR-028-vector-index.md) | proposed index architecture ADR |

Measurement scripts and result JSONs: `benchmarks/p10-index/`. Vector caches
and the benchmark venv live outside git in `~/anti_piracy/datasets/p10/`.

These documents are in the fingerprinter repository because crawler2 was
read-only during the P10 audit; whether to move or link them into crawler2's
phase tree, ADR index and phase plan is decision P10-D16.
