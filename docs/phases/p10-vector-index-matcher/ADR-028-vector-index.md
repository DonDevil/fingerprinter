# ADR-028 — The vector index is a derived, network-reachable Qdrant service with generations per spec and role; Scylla and MinIO stay authoritative

Status: **Proposed** (2026-10-15, P10 design review; decisions P10-D1, D2,
D3, D4, D14, D19, D20; corrected the same day — design §19 C-1, C-3, C-4,
and §19.1 C-10, C-11, C-12).
Not accepted. Number taken as the next free number after
crawler2's ADR-027; this file lives in the fingerprinter repository until the
operator decides where P10 records belong (P10-D16) — crawler2's
`docs/adr/README.md` does not list it yet.

## Context

P10 must match new media against registered targets and new targets against
historical media through ANN candidate reduction (plan B.5 #4), with an index
that is derived and rebuildable from the fingerprinter's Scylla keyspace and
MinIO artifacts (B.5 #3), reachable from several fingerprinter hosts, and
inside the 15 GB development host's budget.

Measured on 104,443 real `dinov2-seg/2` FP32 vectors (P10 audit §9–§10):
FAISS HNSW, HNSW-SQ8, IVF-Flat, IVF-PQ + re-rank and Qdrant (float32, int8,
PQ) all reach segment recall@100 of 0.965–0.992 against an exact oracle, and
their media-level candidate recall differs from exact search by ≤ 0.3 pp —
the candidate policy, not the ANN algorithm, decides recall. Scylla 6.2.3
(deployed) has no vector search. An in-process index would have to be
replicated to every matcher host with an ordered change log and per-host
watermarks to give the read-your-writes visibility the two-direction
ordering argument needs (design §6.4).

## Decision

1. The index is **Qdrant** (1.15.1, pinned by image digest at
   implementation), operated by the fingerprinter as its own service, reached
   over the network with an API key; never embedded in a matcher process.
2. The index is **derived state**. Every point is recomputable from
   `representations_by_spec` / `target_representations` → `representations`
   → `fp-seg-npz/v1` artifact (SHA-256 verified). No match, task, equivalence,
   target or representation fact is stored only in the index. Losing it
   means rebuild, never data loss.
3. One collection per **(spec digest, role ∈ {media, target}, generation)**.
   The generation readers use is recorded **in Scylla** (static
   `active_generation` of `index_generations`) and changed by one LWT; a
   Qdrant alias, if kept, is cosmetic. Vectors of different specs —
   including FP32 and FP16 variants of one family — never share a collection
   or a query. Generation parameters are immutable and recorded in Scylla.
   Both roles store int8 vectors in RAM and float32 originals on disk with
   exact rescoring.
4. Point ids are UUIDv5 of (representation or target-representation id,
   intent `seq`, segment index): upserts of one intent are idempotent across
   hosts, retries and rebuilds, and an older intent's operations can neither
   overwrite nor (deletes are bounded by `seq ≤ d`) remove a newer intent's
   points (design §4.3, §12.2; corrected C-12).
5. Every index mutation is preceded by a durable intent in Scylla
   (`index_entries`: desired op/version, `seq`), applied idempotently, and
   recorded only `IF seq` is unchanged; deletes are bounded by `seq ≤ d` and
   point ids contain `seq`; writes Qdrant cannot fence are found
   by a Qdrant-side verification pass of the P10-owned reconciler. A rebuild
   is a new generation under a build lease: CREATED → BUILDING →
   CATCHING_UP → VALIDATING → READY → ACTIVE (one LWT) → DRAINING →
   RETIRED, promoted only after a full catch-up pass and manifest-digest
   validation against the artifacts (design §12). Guarantees and routing
   races: design §12.9.
6. FAISS exact search is used **only** as the offline oracle for recall
   tests; there is no production fallback to brute force.

## Consequences

- One additional service and failure domain; matching pauses
  (`WAITING_INDEX`) while it is unavailable — explicit, never silent.
- Build-time memory peaks exceed the quantized steady state (OOM at 256 MiB,
  peak = 512 MiB cap for 104 k vectors; steady ≈ 1.3 KB/vector). The dev
  limit is a hard 1.5 GiB; dev capacity is **unresolved** (capacity guard
  `max_points` = 250,000 resident points per instance, all generations,
  provisional) until experiment E-R1 measures a 626 k-point generation and
  its rebuild (≈ 1.26–1.30 M resident, with a resident media collection) on a dedicated test container; the base target
  scenario (600 k vectors, 1.717 GiB raw FP32) is not claimed for the dev
  host; production volumes need a dedicated index host (P14).
- Points lost by non-protocol causes are repaired eventually: the
  generation is marked suspect (no task finishes DONE on it) and the
  repaired subject's task is re-run (design §12.9 H).
- REST ingest measured at ≈ 310–500 points/s (lower bound): a full rebuild at
  base 1-year volume is hours to a day until faster ingest is measured.
- Qdrant upgrades are handled like runtime changes: a new generation built
  and checked before promotion.
- Qdrant writes cannot be made conditional and a Scylla LWT cannot fence a
  write already sent: stale operations are not prevented. A stale DELETE
  cannot remove a newer intent's points (sequence-versioned ids,
  `seq`-bounded deletes; relies on Qdrant's integer `range` filter, to be
  verified on 1.15.1); a stale UPSERT adds older points that cannot be
  accepted as match evidence (candidates
  are checked against live P9 rows and verified on artifact vectors) and are
  removed within one verification cycle after they land (design §12.9).
- Moving to another index technology later means a new adapter behind the
  `VectorIndex` protocol and a rebuild; no authoritative data migrates.

## Alternatives rejected

| Alternative | Reason |
|---|---|
| In-process FAISS replicas per host | multi-host visibility requires a change log + watermarks; memory multiplied per host; no persistence (measured recall equal, not better) |
| A self-built FAISS index service | re-implements persistence, deletes, aliases and replication |
| Scylla vector search | not available in the deployed Scylla 6.2.3 |
| Milvus | several additional infrastructure components; no recall benefit |
| pgvector | a second durable database, contrary to the one-Scylla decision |
