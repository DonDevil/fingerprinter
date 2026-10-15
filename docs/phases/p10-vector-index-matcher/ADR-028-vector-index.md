# ADR-028 — The vector index is a derived, network-reachable Qdrant service with generations per spec and role; Scylla and MinIO stay authoritative

Status: **Proposed** (2026-10-15, P10 design review; decisions P10-D1, D2,
D3, D4, D14). Not accepted. Number taken as the next free number after
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
3. One collection per **(spec digest, role ∈ {media, target}, generation)**,
   one alias per (spec, role) for searches. Vectors of different specs —
   including FP32 and FP16 variants of one family — never share a collection
   or a query. Generation parameters are immutable and recorded in Scylla.
4. Point ids are UUIDv5 of (representation or target-representation id,
   segment index): upserts are idempotent across hosts, retries and rebuilds.
5. Rebuild = new generation built from authoritative data while the active
   one serves, live writes applied to all non-dropped generations, check,
   then promotion by LWT in Scylla followed by an atomic alias switch.
6. FAISS exact search is used **only** as the offline oracle for recall
   tests; there is no production fallback to brute force.

## Consequences

- One additional service and failure domain; matching pauses
  (`WAITING_INDEX`) while it is unavailable — explicit, never silent.
- Build-time memory peaks exceed the quantized steady state (OOM at 256 MiB,
  peak = 512 MiB cap for 104 k vectors; steady ≈ 1.3 KB/vector): the dev
  limit is 1.5 GiB and dev capacity is ≈ 1 M media vectors; production
  volumes need a dedicated index host (P14).
- REST ingest measured at ≈ 310–500 points/s (lower bound): a full rebuild at
  base 1-year volume is hours to a day until faster ingest is measured.
- Qdrant upgrades are handled like runtime changes: a new generation built
  and checked before promotion.
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
