# P10 Design — vector index and matcher (proposal)

Status: **PROPOSED — awaiting operator design approval. Nothing is
implemented. No decision in this document is approved.** Decisions are
numbered P10-D1…P10-D18 and tracked in [decisions.md](decisions.md); the
evidence is in [audit.md](audit.md) (§ references below are to the audit
unless stated otherwise); tests and acceptance criteria are in
[validation.md](validation.md); the index architecture is recorded as
[ADR-028 (proposed)](ADR-028-vector-index.md).

**Corrected 2026-10-15** after the first review (C-1…C-7, §19): resource
budget, low-information freshness, Qdrant/Scylla mutation and rebuild
protocol, P10-owned reconciliation, confidence semantics, `matches` key,
V1 end-to-end baseline as an I-1 prerequisite. Still a proposal.

Plan: crawler2 `docs/v2-phase-plan.md` §P10 (binding), invariants B.5 #3/#4.
P9 is complete and is not reopened: no P9 identity, spec, table, artifact or
event changes here (P10-D5 keeps P9 code unchanged).

---

## 1. Goals and non-goals

**Goals**

1. A derived, rebuildable vector index over P9 representations, per spec,
   reachable from every fingerprinter host.
2. Matching in both directions with ANN candidate reduction before temporal
   verification:
   A. new media → registered targets; B. new target → historical media
   (without re-encoding the media).
3. A temporal verifier that beats the V1 baseline on the labelled set with
   no false positives, with calibrated confidence and full provenance.
4. Bounded, observable verification work; explicit incomplete outcomes; no
   unbounded fallback.
5. Level-5 media equivalence (P8) as a reversible, provenance-carrying
   relation between different `ContentId`s.
6. `match.found` produced under the existing contract, idempotently.
7. A supported, tested index drop-and-rebuild procedure.

**Non-goals**

- No new representation spec, no change to segment duration, sampling,
  precision or identity (a denser sampling would address audit F-1 but is a
  P9-style spec change, out of scope; see §17).
- No FP16 (stays gated), no cross-spec matching.
- No feedback (P11), evidence (P12), analytics (P13).
- No real multi-machine validation (P14); P10 is designed for multi-host
  correctness and tested as simulated two-host only.
- No crawler2 or contract-package change in P10 unless P10-D12/D13 are
  approved separately.

## 2. Invariants (checked for every section)

| # | Invariant | How the design keeps it |
|---|---|---|
| I-1 | Authoritative data is durable; the index is derived | the index holds copies of vectors and ids only; every fact (match, task, equivalence, generation) is in Scylla; §5, §12 |
| I-2 | No all-to-all matching | every comparison is preceded by an ANN query with hard bounds; brute-force candidate matching exists only in offline tests; the only exact search in production is the bounded target-role search of `lowinfo/v1` classification, which never produces match candidates; §7 |
| I-3 | Both directions | §6.2, §6.3 and the ordering argument of §6.4 |
| I-4 | Multi-host | one network-reachable index service; all coordination via Scylla LWT and Redis leases; no host-local authority; §4, §11 |
| I-5 | Idempotency | deterministic point ids, `match_id`, LWT-guarded task and record writes; §5, §11 |
| I-6 | Explicit failures | typed errors, states `INCOMPLETE`/`WAITING_INDEX`, metrics; no fallback; §11, §12 |
| I-7 | P9 identity stable | one index per spec digest; FP32/FP16 never mixed; §4.2 |
| I-8 | Contracts additive | `match.found` 1.0 used as is; extensions only by separate decision; §9 |
| I-9 | Level 3 ≠ level 5 | equivalence is a separate relation; ContentIds never merged; §10 |
| I-10 | Rebuild supported | generations, intent-first mutations, resumable builder, completeness proof, single-LWT promotion, P10 reconciler; §12 |
| I-11 | Resource limits real | §13: raw/steady/peak/disk separated; hard 1.5 GiB dev limit; capacity guard; E-R1 before claiming the base target scenario |
| I-12 | P14 owns real multi-machine | §16 |

## 3. Architecture

```
            Scylla (fingerprinter keyspace)                     MinIO
   P9 tables (read only) ─────────────┐                fingerprinter-representations
   representations, representations_by_spec,                (fp-seg-npz/v1, authoritative)
   targets, target_representations     │                         │
                                        ▼                         ▼
 representation.ready ──hint──►  matcher process (any host, N instances)  ──► Qdrant (index service)
 (group-less XREAD, §6.1)        ├─ scheduler: tasks from hints + backstop scan    collections per
                                 ├─ indexer: upsert/delete points                  (spec, role, generation)
                                 ├─ searcher: adaptive-k ANN, candidate rules      active generation per
                                 ├─ P10 reconciler (separate role, §12.6)          (spec, role) in Scylla
                                 ├─ verifier: temporal/v2 (deterministic)
                                 └─ committer: matches, equivalence, intents
                                        │
            P10 tables: match_tasks, match_due, match_candidates, matches,
            matches_by_target, equivalence_links, index_generations,
            index_active, index_entries, match_intents, verifier_params
                                        │
                                        ▼ held publication (ADR-026 pattern)
                                 match.found (stream; held until a consumer registers)
```

Components (new modules in the `fingerprinter` package; the V1 top-level
`matching/` package is untouched):

| Module | Responsibility |
|---|---|
| `fingerprinter/vindex.py` | `VectorIndex` protocol + `QdrantIndex` adapter (collections, upsert/delete/search/count/scroll, health; no conditional writes); typed errors |
| `fingerprinter/indexing.py` | generation model, incremental indexer, builder (full rebuild), checker |
| `fingerprinter/candidates.py` | adaptive-k retrieval, low-information filtering, grouping, ranking, caps |
| `fingerprinter/verify.py` | verifier `temporal/v2`, pure function of two `fp-seg-npz/v1` payloads and a registered parameter set |
| `fingerprinter/matching.py` | match tasks (state machine, leases, retries), commit of matches/equivalence, publication |
| `fingerprinter/cli.py` (extended) | `index …`, `match …`, `equivalence …`, `matcher run` |
| `benchmarks/p10-index/` | offline oracle, tuning and validation harnesses (not imported by the service) |

A **matcher** process is stateless apart from leases and an in-process LRU of
decoded artifacts (a cache: bounded, never authoritative). Any number may run
on any host; the GPU is not used.

## 4. Index technology and layout

### 4.1 Choice (P10-D1, ADR-028 proposed)

**Qdrant 1.15.1** (pinned image digest at implementation), run as a
fingerprinter-owned network service, is proposed as the index. FAISS
`IndexFlatIP` is kept only as the offline oracle.

Why (evidence in audit §9–§10):

- Retrieval quality is not the differentiator: every measured variant is
  within ≈ 2 % of exact at segment level and within 0.3 pp at media level
  (audit F-4). The candidate policy dominates recall.
- **Multi-host accessibility is a requirement**: an in-process FAISS index
  would be either one host's private structure (not shared) or a replica on
  every matcher host. Replicas need a global ordered change log, per-host
  watermarks and a read-your-writes protocol to keep the ordering guarantee
  of §6.4, and multiply memory by the host count. Qdrant gives one shared,
  immediately visible (`wait=true`) store reachable over the network, with
  collections, payload filters and deletes by
  filter, persistence (WAL), quantization and on-disk storage options, and a
  documented path to sharding/replication for P14.
- Measured fit: int8 quantization with on-disk originals and rescoring
  reached segment recall@100 of 0.992 at 2.0 ms p50 per HTTP query on 104 k
  real vectors (audit §9.1).
- Scylla vector search is unavailable on the deployed Scylla 6.2.3 (audit
  §9.4). Milvus and pgvector add infrastructure the architecture rejects.

Costs accepted (ADR-028 consequences): a new service and failure domain,
version pinning, a build-time memory peak (audit F-5), a REST ingest rate
measured only as a lower bound (≈ 310–500 points/s), and slow first
queries after a build when memory is tight (p50 558 ms for the first 760
queries under a 512 MiB cap, then 2 ms; audit §9.3) — the dev limit of §13
leaves headroom above the build peak for that reason.

### 4.2 Collections, active generation, compatibility (P10-D2, corrected C-3)

- One **collection per (spec digest, role, generation)**:
  `fp_<spec12>_<role>_g<n>` where `spec12` = first 12 hex characters of the
  spec digest and `role ∈ {media, target}`. The full digest is stored on the
  generation row and on every point; the builder refuses a 12-hex collision
  with a different full digest.
- **Which generation readers use is decided in Scylla, not in Qdrant**: the
  static column `active_generation` of the `index_generations` partition of
  (spec, role) (§5, §12.4). Readers resolve it (cached ≤ `index.active_ttl_s`,
  5 s) and search that collection by name. Promotion is a single-partition
  LWT and therefore atomic for every reader. A Qdrant alias with the same
  name is maintained only as an operator convenience and is never read by
  the matcher. *(Corrected: the first proposal made the alias the reader's
  source of truth; a crash between the Scylla LWT and the alias switch could
  then leave the two disagreeing.)*
- **Specification compatibility is enforced by construction**: a
  representation is indexed only into its own spec's collections, and a
  query uses only the collections of the query representation's spec. No
  compatibility rule between specs exists; in particular `dinov2-seg/1` and
  `/2` (identical vectors for complete media, P9 §3.2a) and any FP16 spec are
  **not** interchangeable. Adding such a rule would need its own decision.
- **Roles.** `media`: every READY content representation of the spec,
  including target reference contents (they are contents too). `target`: the
  segments of every READY target binding (`target_representations`) whose
  target version is not RETIRED, keyed by the target representation.
  The two roles have different cardinalities and budgets (§13).
- Distance `Dot` (vectors are L2-normalized by the spec; the indexer rejects
  a vector whose norm deviates from 1 by more than 1e-4 with
  `InvalidVectorError`, measured norms are 1.0).
- **Both roles**: HNSW `m=32, ef_construct=200`, int8 scalar quantization in
  RAM (`always_ram`), float32 originals on disk (`on_disk` vectors), payload
  on disk, search with `rescore=true, oversampling=2` — rescoring reads the
  float32 originals, so the final scores are exact cosines of the stored
  vectors. *(Corrected C-1: the first proposal kept the target role in
  float32 RAM; at the base scenario that alone is 1.717 GiB of raw vectors,
  above the 1.5 GiB dev limit.)* Values are the measured configuration
  (audit §9.1: recall@100 0.992 with rescoring); tuning is allowed only
  through a new generation with recorded parameters (§12.1).

### 4.3 Points (P10-D4)

| Role | Point id (UUID v5, namespace `antipiracy/fingerprinter/index/v1`) | Payload |
|---|---|---|
| media | `uuid5(representation_id + ":" + segment_index)` | `rep`, `cid` (content id), `spec` (full digest), `k` (segment index), `t` (pts_s), `seq` (mutation sequence, §12.2) |
| target | `uuid5(target_representation_id + ":" + segment_index)` | `trep`, `tid`, `tver`, `rep` (content representation), `cid`, `spec`, `k`, `t`, `seq`, `lowinfo` (bool), `lowinfo_rev` (§7.3) |

Deterministic ids make every upsert idempotent across hosts and retries
(duplicate events, concurrent indexers, rebuild + live writes all converge).
Index ids never appear in Scylla facts or events; they are recomputed from
domain ids. Payload indexes: `rep`, `cid`, `tid` (keyword). Media points carry
**no** low-information flag (§7.3).

## 5. Data model (new tables, fingerprinter keyspace, no TTL)

All tables are P10-owned; no P9 table gains a column and no P9 code writes
them.

| Table | Key | Content |
|---|---|---|
| `index_generations` | PK `(spec_digest, role)`, CK `generation`; **static** `active_generation`, `active_epoch`, `routing_epoch` | per generation: state (§12.4), collection, params JSON + digest, `boundary_at`, per-bucket build cursor, expected/actual counts, manifest digest, build owner epoch, timestamps, audit |
| `index_build_lease` | PK `(spec_digest, role)` | `owner`, `epoch`, `lease_until` — one builder per (spec, role) (P9 intake lease pattern) |
| `index_entries` | PK `(spec_digest, role, bucket)`, CK `subject_id` | **the durable mutation intent and its bookkeeping** (§12.2): `desired_op` UPSERT/DELETE, `desired_ver` (artifact SHA-256, plus `lowinfo_rev` for targets), `seq` bigint, `applied map<int, bigint>` (generation → applied seq), `segment_count`, `state` PENDING/APPLIED/BLOCKED, `attempts`, `last_error`, `updated_at` |
| `p10_readers` | PK `reader` | hint-reader lease and cursor: `owner`, `epoch`, `lease_until`, `cursor_id`, `last_gap_at` (§6.1) |
| `p10_scans` | PK `(scan, spec_digest)`, CK `bucket` | backstop scan cursor per bucket or token range, `cycle`, `last_key`, `updated_at` (§6.1) |
| `target_set_rev` | PK `spec_digest` | `rev` bigint, `members_digest`, `updated_at` — target-set revision for low-information classification (§7.3) |
| `lowinfo_class` | PK `spec_digest`, CK `target_representation_id` | `rev`, `rule` (`lowinfo/v1`), per-segment flag bitmap, `computed_at` (§7.3) |
| `lowinfo_refresh` | PK `spec_digest` | `requested_rev`, `done_rev`, owner/epoch/lease (§7.3) |
| `match_tasks` | PK `subject_id`, CK `(spec_digest, kind)` | `kind ∈ {media_vs_targets, target_vs_media, media_vs_media}`; state machine §11.1; lease fields, attempt, failures, `next_attempt_at`; generation used; target-set rev used; ANN statistics JSON; candidate counts; `incomplete_reason`; timestamps |
| `match_due` | PK `(due_day, shard)`, CK `(due_at, subject_id, spec_digest, kind)` | discoverability index of non-terminal tasks (P9 `encode_due` pattern) |
| `match_candidates` | PK `(subject_id, spec_digest, kind, generation)`, CK `rank` | durable candidate list of a task (bounded by its cap), per-candidate state |
| `matches` | PK `content_id`, CK `(target_id, target_version, media_spec_digest, target_spec_digest, technique_name, technique_version)` | `match_id` (checked equal to `MatchId.of(...)` of the key components on write), `representation_id`, `target_representation_id`, verdict, confidence, `calibration_id`, alignment JSON, verifier param digest, provenance JSON (§9.2), `decided_at`, stored envelope, `event_id`, `published_at` — *corrected C-6, §9.3a* |
| `matches_by_target` | PK `(target_id, target_version)`, CK `(content_id, media_spec_digest, target_spec_digest, technique_name, technique_version)` | `match_id`; reverse lookup |
| `matches_by_id` | PK `match_id` | the `matches` key components; idempotency lookup by id |
| `equivalence_links` | PK `content_id`, CK `(other_content_id, spec_digest, technique_name, technique_version)` | state `LINKED`/`REVOKED`, confidence, evidence JSON, provenance, `decided_at`, `revoked_at`, audit; written in both orientations |
| `match_intents` | PK `(day, shard)`, CK `intent_id` | durable intents for match/equivalence commits and publication (ADR-026 pattern), resolved by the **P10 reconciler** (§12.6) |
| `verifier_params` | PK `(technique_name, technique_version)` | immutable parameter document + digest; calibration record and status (§9.1) |

Bucketing uses the P9 `shard_of(content_id, 64)` function (target subjects:
`shard_of(target_representation_id, 64)`). Partition sizes are bounded:
`index_entries` by subjects per bucket (≈ 1/64 of representations, not
segments), `match_candidates` by the task cap (§7.5), `matches_by_target` by
the true matches of one target version (bucketed by month if a target exceeds
10⁵ matches — not expected before P14).

Authoritative sources for every derived thing:

| Derived | Rebuilt from |
|---|---|
| media collection | `representations_by_spec(spec, *)` → `representations` (READY) → artifact (SHA-256 verified) |
| target collection | `target_representations` (READY, target not RETIRED) → bound content representation → artifact |
| `index_entries` | recomputable from the same authoritative rows (desired state); applied state is re-verified against Qdrant (§12.6) |
| `lowinfo_class`, target `lowinfo` payload | recomputed by `lowinfo/v1` from the target collection at the current `target_set_rev` (§7.3) |
| Redis keys (`fp:` leases, `fp:q:match`) | `match_due` |

## 6. Work model and both matching directions

### 6.1 Discovering work: hint reader and backstop scan (P10-D5, corrected C-4)

Authoritative trigger = **Scylla state**. Event streams are only hints. All
of this is done by P10-owned components (`matcher` hint reader and the P10
reconciler, §12.6); **no P9 code, table, stream registration or reconciler
pass is changed** (corrected: the first proposal also said "the reconciler
gains a P10 pass", which contradicted that).

**Facts checked in P9 code** (`publish.py`, `intake.py`):

- `representation.ready.v1` is XADDed by the P9 publisher to
  `<stream_prefix>representation.ready.v1` and trimmed with `XTRIM MINID` to
  the oldest entry any *registered group* still needs. A group-less reader is
  invisible to trimming, so entries can disappear as soon as `media-registry`
  has acknowledged them. Hints can therefore be lost; the backstop must not
  depend on them.
- `target.registered.v1` / `target.retired.v1` are seeded with no required
  group and `hold_until_registered = true`: they are **not XADDed at all**
  until a consumer group is registered. P10 does not register one, so target
  hints do not exist; targets are discovered by the backstop only.

**A. Hint reader (`representation.ready`, best effort)**

- One active reader per (keyspace, stream): row `p10_readers(reader =
  "rep-ready")` with `owner`, `epoch`, `lease_until`, `cursor_id` — the P9
  intake pattern (`intake.py`: acquire by `LWT … IF epoch = ?`, renew by
  `IF owner = ? AND epoch = ?`). The keyspace scopes the cursor to the
  environment; the stream key includes the configured prefix.
- Start position at initialisation: the oldest retained entry (`0-0`);
  re-reading is harmless (idempotent task creation).
- Cycle: `XREAD` a batch after `cursor_id` → for each entry, upsert the
  `match_tasks` rows `IF NOT EXISTS` (and the `index_entries` desired state,
  §12.2) → only then advance the cursor with
  `UPDATE … SET cursor_id = :last IF epoch = :mine AND cursor_id = :prev`.
  A crash before the cursor update re-reads the batch (duplicates are
  no-ops); the cursor can never pass an entry whose work is not durable.
- A stale owner (lease expired, epoch advanced) fails the cursor LWT and
  stops; work it already recorded is valid and idempotent.
- Trimmed range (first retained id > cursor, or cursor id no longer
  present): no gap protocol is needed for hints — the reader jumps to the
  oldest retained entry, records `last_gap_at`, increments
  `fingerprinter_p10_hint_gaps_total` and requests an immediate backstop pass.
- Out-of-order or duplicate entries: irrelevant, because each entry only
  causes an idempotent upsert of desired state derived from the current
  authoritative rows, not from the event payload.

**B. Backstop scan (the guarantee)**

| Scan | Source (schema-checked) | Paging and cursor |
|---|---|---|
| `reps` | `representations_by_spec` PK `(spec_digest, bucket)` CK `content_id` (64 buckets) for every indexed spec; per row a point read of `representations(content_id, spec_digest)` for state and artifact SHA-256 | per bucket: `WHERE spec_digest = ? AND bucket = ? AND content_id > ? LIMIT 500`; cursor `(bucket, last content_id)` persisted in `p10_scans` after each page; a bucket ends when a page returns < 500 rows; a cycle ends when all 64 buckets end, then `cycle += 1` and cursors reset |
| `targets` | `target_representations` PK `target_id` CK `(version, spec_digest)` (small table, no bucket) joined with `targets(target_id, version).state` | token ranges: `WHERE token(target_id) > ? LIMIT 200`; cursor = last token, persisted per page |

Per row the scan computes the desired state (index entry, match tasks,
target-set membership) and upserts it idempotently. Rows inserted behind a
cursor are reached in the next cycle; rows are never deleted by P9
(representations become RETIRED; targets RETIRED), so retirements are seen
as state changes. Rate limits: `reconcile.scan_rows_per_s` (default 200 on
the dev host) and an interval between cycles (`reps`: 1 h; `targets`: 60 s).
Progress metrics: `p10_scan_cycle`, `p10_scan_rows_total`,
`p10_scan_cycle_seconds`. One scanner per scan (same lease pattern as the
reader, rows `p10_readers("scan:reps")`, `("scan:targets")`).

**C. Eventual catch-up guarantee.** Assumptions: (1) the authoritative rows
remain readable; (2) the backstop scanner runs and completes cycles; (3)
Scylla, MinIO and Qdrant are reachable often enough for retries to succeed;
(4) retries are never abandoned silently — after `max_attempts` an item
enters a visible `BLOCKED`/`FAILED` state with its error. Under these
assumptions, **every READY representation and every READY or RETIRED target
binding of an indexed spec is reflected, within one completed backstop cycle
after it became durable, in `index_entries` desired state and in
`match_tasks`; every resulting mutation and task then reaches a terminal
consistent state (APPLIED/DONE/INCOMPLETE) or a visible BLOCKED/FAILED
state.** No numerical bound is claimed: a cycle's duration is rows ÷ rate
plus the interval (≈ 1 h + 20 min for the base 1-year representation count
of ≈ 237 k at 200 rows/s — arithmetic, not measured).

**D. Ownership.** P9 owns representation, target and event behaviour
unchanged. P10 owns the hint reader, the backstop scans, index mutations,
generations, promotion, index repair, low-information classification and
match tasks. Any P9 code or contract change (e.g. a commit follow-up, a
consumer group) would need separate approval.

Every READY representation of an indexed spec gets `media_vs_targets` and
(if equivalence is enabled) `media_vs_media` tasks; every READY target
binding gets one `target_vs_media` task. Tasks are claimed with the P9 lease
pattern (Redis ZSET rebuilt from `match_due`, per-task Redis lease, Scylla
LWT on `epoch` for state transitions, Redis clock).

### 6.2 Direction A — new media against registered targets

1. **Index own vectors**: upsert the representation's segments into the media
   collection(s) of its spec (`wait=true`), record `index_entries`.
2. **Search targets**: every informative segment (§7.3) queries the target
   collection (active generation) of the same spec with adaptive k (§7.1);
   a segment whose best hit is a *current* low-information target segment
   does not vote (§7.3).
3. **Candidates**: hits ≥ floor grouped by target version; candidate iff ≥
   `min_hits` distinct query segments; ranked by (distinct hits, best score);
   capped at `cap_A` (§7.5).
4. **Verify** each candidate: load the target representation's artifact
   (verified SHA-256), run `temporal/v2` (§8).
5. **Commit** matches (§9) and finish the task (DONE or INCOMPLETE).

### 6.3 Direction B — new target against historical media

1. **Index the target**: upsert target segments into the target collection;
   raise the target-set revision and request the low-information refresh
   (§7.3); the new target is searchable at once (stale or unknown flags count
   as informative).
2. **Search media**: every target segment not *currently* flagged
   low-information queries the media collection (active generation)
   with adaptive k (k₀ → k_max, §7.1); saturation is recorded per segment.
3. **Candidates**: grouped by `content_id`, ≥ `min_hits`, ranked, written to
   `match_candidates` (≤ `cap_B_total`); the target's own reference content is
   excluded.
4. **Verify in pages** of `page_B` candidates; the task's cursor survives
   restarts; each page is one lease-bounded unit of work.
5. **Commit** each page's matches; the task ends DONE, or INCOMPLETE with
   reason `candidate_cap` / `saturated_segments` when bounds were hit (§7.5).

No media is re-encoded: direction B reads stored representations only (the
validation test asserts zero new encode attempts, validation §7).

### 6.4 Why no pair is missed by ordering

Each task indexes its own subject **before** it searches the opposite role,
and Qdrant applies an acknowledged `wait=true` upsert before answering later
searches. For a media M and a target T of the same spec: if M's search ran
before T was indexed, then T's indexing — and therefore T's later search —
happened after M was indexed, so T's search sees M. Either way at least one
of the two searches can retrieve the pair. The guarantee is about
*visibility*; whether ANN retrieves the pair is measured (validation §4).
Across index generations, live writes go to every non-dropped generation
(§12.3), so a promotion cannot lose a subject indexed during a build.

### 6.5 Spec coverage

A media representation and a target binding are comparable only under the
same spec. Pairs whose subjects have no common indexed spec are not compared;
the count of such subjects is exposed (`match status`,
`fingerprinter_match_spec_uncovered`) — they are resolved by a P9 backfill,
never by cross-spec comparison. P10 indexes `dinov2-seg/2` first and
registers `retire_guard(spec, "p10-index")` for each indexed spec.

## 7. Candidate generation and bounds (P10-D6)

### 7.1 Adaptive k

For each informative query segment: search with `k₀`; while the k-th score is
still ≥ `floor` and `k < k_max`, double k and search again. A segment that
ends at `k_max` with the k-th score ≥ floor is **saturated** — more
neighbours above the floor exist than were retrieved. Saturation is counted
per task and, if any segment saturates, the task's outcome records
`saturated_segments = n` (audit F-6, F-7: fixed k loses recall silently as
copies accumulate; adaptive k reached media recall 0.9965 at 19× growth vs
0.745 for k=100 with a 200 cap).

### 7.2 Initial values (provisional; final values from the tuning split, validation §3)

| Parameter | A (media → targets) | B (target → media) | Media ↔ media | Basis |
|---|---|---|---|---|
| `k₀` | 32 | 32 | 32 | audit §10.1 |
| `k_max` | 256 | 512 | 512 | B: 228 of 760 target segments saturated at 512 on the 19× corpus, recall 0.9965 |
| `floor` (cosine) | 0.60 | 0.70 | 0.70 | true-correspondence cosine p25 0.43, p50 0.71 (audit §7.1): the floor bounds work, precision comes from verification |
| `min_hits` (distinct query segments) | 2 | 2 | 2 | audit §10 |
| `cap_A` (target versions per media) | 50 | — | — | targets are distinct works; true matches per media are few |
| `page_B` | — | 200 | 200 | one lease-bounded verification step |
| `cap_B_total` (candidates per task) | — | 5,000 | 2,000 | beyond: INCOMPLETE `candidate_cap` |

### 7.3 Low-information segments (P10-D8, provisional; corrected C-2)

Identical black, fade, logo and credit frames occur in unrelated works (audit
F-2). `lowinfo/v1` stays **provisional**: it is used only after the I-1
measurement (validation §3.4) shows no recall loss.

**Rule.** A **target** segment is low-information at target-set revision `r`
if it has cosine ≥ 0.95 with a segment of a *different* target work in the
target set of revision `r` (targets are distinct works by definition, so
such a vector does not identify a work). Media segments are **never
classified or flagged**: whether a media segment is uninformative is decided
at query time from the target flags it hits (below). This removes the whole
class of stale media classifications (the first proposal stored a media flag
that a later target registration or retirement could silently invalidate).

**Revision.** `target_set_rev(spec).rev` is a counter incremented by LWT
(`IF rev = :old`) whenever P10 observes a change in the spec's target set:
a binding becoming READY, a target version RETIRED, or a binding replaced.
Observation happens in the targets backstop scan (§6.1) and in
`target_vs_media` task start; both compare the current members digest
(sorted READY, non-retired target representation ids) with
`members_digest` and bump only on a difference, so duplicate or replayed
observations do not bump twice.

**Classification records.** `lowinfo_class(spec, target_representation_id)`
holds `rev`, `rule`, the per-segment bitmap. Written only with
`… IF rev < :r` (or `IF NOT EXISTS`): a worker computing for an older
revision can never overwrite a newer result. The target points' payload
(`lowinfo`, `lowinfo_rev`) is a derived copy applied through the index
mutation protocol (§12.2; `desired_ver` includes `lowinfo_rev`).

**Which entries a change affects (exact).** When the target set changes from
`r−1` to `r` because target representation X was added or removed, the
affected targets are X itself and every target owning a segment with cosine
≥ 0.95 to any segment of X. They are found by an **exact** (non-approximate,
`exact=true`) search of X's segments over the *target* collection only —
bounded by the target-role size (base: ≈ 1,440 query segments × 0.6 M
target vectors), never over media. Every other target's flags are unchanged
by construction; their `rev` is advanced by a cheap LWT without
recomputation. This search is classification, not matching, and is not a
fallback for candidate generation.

**Refresh work.** `lowinfo_refresh(spec)` stores `requested_rev`; a leased
worker (owner/epoch pattern) computes the affected set for each revision
step, writes `lowinfo_class` rows (`IF rev < r`), enqueues the payload
mutations and sets `done_rev = r` (`IF epoch = mine`). A crash leaves
`done_rev < requested_rev`; the next worker resumes. Registrations during a
refresh only raise `requested_rev`; the worker loops until
`done_rev = requested_rev` (convergence to the latest revision). A weekly
full recomputation (all targets, exact over the target role) is the
backstop for any defect in the affected-set logic.

**Use at query time — stale means eligible.** A task reads the current
`rev` at start and records it.
- Direction A: a media segment does not vote only if its best target hit is
  a segment with `lowinfo = true` **and** `lowinfo_rev = rev`. Unknown or
  older `lowinfo_rev` ⇒ the hit counts as informative.
- Direction B: a target segment is skipped as a query only if its flag is
  current; otherwise it queries.
- Verification: a target segment is excluded from run counting only if its
  flag is current.
Stale or unknown classifications therefore cost extra work, never recall.
There is no brute-force fallback anywhere in this path.

**Failure.** A refresh that keeps failing becomes BLOCKED (visible); flags
stay at their old `rev`, which the rule above treats as informative.

**Metrics.** `lowinfo_targets{state=current|stale|unknown}`,
`lowinfo_refresh_pending_revs`, `lowinfo_refresh_lag_seconds`
(now − time of the oldest unprocessed revision), `lowinfo_refresh_failures_total`,
`match_lowinfo_excluded_total{kind}` (segments that did not vote/query/count).
Offline only: the number of labelled true matches that the filter would have
lost (validation §3.4).

### 7.4 Deduplication

Hits are deduplicated per (query segment, subject); candidates per subject
(target version or content). Media with the same `ContentId` are one subject
by construction (P8 level 3). Different `ContentId`s already linked as
level-5 equivalent are **still verified separately** (equivalence is not used
to skip verification in P10; it may become an optimization later only with
its own decision).

### 7.5 Bounds and what happens at a bound

| Bound | Effect when reached | Recorded as |
|---|---|---|
| `k_max` per segment | further neighbours not retrieved | `saturated_segments` on the task, metric |
| `cap_A` | lowest-ranked targets not verified | task INCOMPLETE `candidate_cap`, the truncated count, the rank threshold |
| `page_B` | next page in a new lease | progress cursor (not incomplete) |
| `cap_B_total` | candidates beyond rank `cap_B_total` not stored | task INCOMPLETE `candidate_cap`; `match retry --raise-cap` re-runs with a higher cap (audited) |
| verification timeout per candidate | candidate marked FAILED (retryable ×3) | per-candidate state; task INCOMPLETE `verification_failed` if retries exhausted |
| matcher concurrency | at most `matcher.workers` verifications per process | — |

Nothing is discarded without a recorded count and reason. An INCOMPLETE task
is not a "no match"; it is visible in `match status`, metrics and the
validation reports. There is **no** fallback to a full scan.

Verification cost is bounded per task: ≤ `cap × C_v(max duration)`, where
`C_v` is one dense similarity of two representations (≤ 1,440 × 1,440 × 768
FLOP ≈ 1.6 GFLOP for two 2-hour items, < 1 s on one core); measured in
validation §9.

## 8. Temporal verification `temporal/v2` (P10-D7)

ANN only proposes candidates; `temporal/v2` decides. It keeps V1's proven
structure (dense similarity of the two representations, one dominant
temporal offset, ordered runs, a minimum run length — the mechanism that
makes the zero-false-positive baseline possible) and changes what the audit
shows to be wrong:

| V1 `temporal_v1` | `temporal/v2` | Why |
|---|---|---|
| offset in segment-index units, tolerance 1 index | offset in **seconds** from `pts_s`; a media segment at time t is compared with the target segments whose sampled frames lie within ±`d` of `t + Δ` (the two neighbours in phase) and the max is used | sampling phase is the main loss (audit F-1) |
| single dominant offset | up to `n_offsets` (3) offset modes from the ANN hit votes, each verified, best kept | repeated scenes, intros |
| 0.90 segment and mean thresholds | `s_seg`, `s_mean`, `L` fitted on the training split | audit §7.3 |
| all segments count | target segments with a *current* low-information flag neither count nor break a run; stale/unknown flags count (§7.3) | audit F-2 |
| `score` = mean similarity | calibrated `confidence` (§9.1) | contract semantics |
| one run reported | every run ≥ `L` reported as an `AlignedSegment` (max 32) | evidence |

Deterministic: same inputs, same parameter set ⇒ same output, on any host
(float32 matrix products are computed in float64 as in V1 to avoid
order-dependent rounding; ties broken by indices). Drift (VFR/speed change,
audit M-8) remains a documented limitation; it is measured on the 24 VFR
SOAK items, not designed for.

V1 `match_segments` stays frozen as `temporal_v1` for baseline runs only.

## 9. Match decision, confidence, provenance, `match.found`

### 9.1 Verdict and confidence (P10-D9, corrected C-5)

Four quantities are kept apart and never substituted for each other:

| Quantity | Meaning | Where it appears |
|---|---|---|
| ANN similarity | cosine of two segment vectors | candidate generation, provenance |
| verifier score `s` | deterministic function of the verified run(s): run length in informative segments, mean similarity, coverage | `matches.provenance` |
| **`confidence`** | **estimated probability that this proposed, temporally verified match is correct**, conditional on the versioned pipeline (spec, candidate-generation parameters, `temporal/v2` parameter set, `lowinfo` rule) and on the calibration population | `match.found`, `matches` |
| operating-point precision/recall | TP/(TP+FP) and TP/(TP+FN) of all decisions at or above a threshold on a stated population | validation reports, `verifier_params` evidence; never in an event |

*Corrected:* the first proposal called `confidence` "precision among decisions
at least this strong" and fitted it with isotonic regression. Those are
different statistics: isotonic calibration estimates a per-decision outcome
probability; precision above a threshold is an aggregate that depends on the
score distribution and the threshold. `confidence` is now the calibrated
per-decision probability; operating-point precision is reported separately.

- **Calibration.** A monotone map `g(s)` fitted to the binary outcome
  "proposed match is correct" on calibration data that is disjoint from the
  final evaluation groups (validation §3). Isotonic regression is used only
  if the calibration data meet the minimum of validation §3.3; otherwise a
  two-parameter logistic (Platt) map is fitted if it passes the same
  reliability check; otherwise calibration is **unavailable**.
- **Calibration record.** `verifier_params.calibration` stores
  `calibration_id`, method, the identities of the training/calibration data
  (manifest SHA-256s, fold assignment), sample sizes (positives/negatives,
  films), the score definition version, the candidate-generation params
  digest, reliability results and `status ∈ {CALIBRATED, UNAVAILABLE,
  STALE}`.
- **Invalidation.** A change of the verifier parameters or score definition
  is a new technique version (§9.2) and needs its own calibration. A change
  of candidate-generation parameters, spec or `lowinfo` rule changes the
  population of proposed matches: the calibration becomes `STALE` until the
  reliability check is re-run on the new pipeline and passes.
- **Verdicts.** `confirmed` at or above the confirmation threshold `τ_c`,
  selected on training groups only (validation §3.2) as the highest-recall
  threshold with zero false positives on training data; `probable` between
  `τ_p` and `τ_c`.
- **Publication rule.** `match.found` requires a `confidence` ∈ [0,1] whose
  meaning is fixed by the contract field. A decision is **published only if
  its technique version has `status = CALIBRATED`**; otherwise it is stored
  (`matches`, `published_at` null, `calibration_id` null) and held — no
  probability is fabricated, and an uncalibrated score is never placed in the
  `confidence` field. Once calibration becomes available, held decisions of
  that technique version get `confidence = g(s)` and are released by an
  audited `match publish-held`. Only `confirmed` is published by default;
  `probable` only if `matching.publish_probable` is enabled.
- **Limits stated with every calibration.** Seven animated films from one
  reel; clips of one film are correlated (effective sample size closer to
  the number of films than to the number of pairs); no live action. With
  zero false positives in the calibration data the map saturates near 1 at
  the top and its resolution there is limited; reliability intervals are
  reported, not hidden.

### 9.2 Technique and provenance (P10-D10)

- `technique.name = "dinov2-seg-temporal"`, `technique.version = "2.<n>"`,
  where `<n>` names an immutable row in `verifier_params`. Any change that can
  change a decision (thresholds, rules, `lowinfo` version, calibration
  affecting the verdict) creates a new version, hence a new `match_id` — old
  decisions stay valid under their own version (contract rule, audit §5).
- Stored provenance per match (`matches.provenance`): spec digest(s), both
  representation ids, target representation id, verifier param digest,
  index generation and its params digest, Qdrant version, target-set
  revision and `lowinfo` rule, `calibration_id` (or null), ANN parameters
  (k₀, k_max, floor, min_hits, caps, ef, quantization/rescore), similarity
  metric, query and candidate counts, rank of this candidate, saturation and
  truncation counts of the task, `complete` flag, verification duration,
  matcher instance, timestamps.

### 9.3 Persistence and publication (P10-D11)

1. Write intent `match_commit(match_id, …)` (`match_intents`).
2. `INSERT … IF NOT EXISTS` into `matches` (LWT) with the key of §9.3a,
   then `matches_by_target` and `matches_by_id`. A duplicate decision with
   the same `match_id` is a no-op (same inputs ⇒ same decision; a different
   stored decision for the same id is an invariant violation, reported and
   never overwritten).
3. If the technique version is `CALIBRATED` (§9.1): build the `match.found`
   1.0 envelope (producer `fingerprinter/matcher`, idempotency key
   `match.match_id`), store it on the row, publish through the P9 publisher
   API (read-only reuse) with **held publication**: stream `match.found`
   registered by P10 (its own registry row) with `hold_until_registered =
   true` and no required group until P11/P12 register theirs. Held events
   stay in Scylla; `publisher republish` releases them. Otherwise the row
   stays unpublished (§9.1).
4. Delete the intent. The **P10 reconciler** (§12.6) resolves stale intents.

`match.found` carries: `content_id`, `representation_id`, `target`,
`target_representation_id`, `technique`, `verdict`, `confidence`, `segments`
(aligned runs), `decided_at`. ANN provenance and completeness are **not** in
the 1.0 schema; they live in Scylla and `match show`. An additive
`match.found` 1.1 with an optional `provenance` object is proposed as a
separate crawler2 contract change (P10-D12, deferred). Non-matches are never
events. An INCOMPLETE search is never expressed as an event in 1.0.

### 9.3a Match identity and the `matches` key (verified, corrected C-6)

Verified in code (crawler2 `contracts/.../ids.py`, `models/matching.py`;
fingerprinter `fingerprinter/spec.py`):

- `RepresentationId.for_content(content_id, spec.name, spec.version,
  spec.config_digest)`; `RepresentationId.for_target(target_id,
  target_version, spec.name, spec.version, spec.config_digest)`.
- `MatchId.of(representation_id, target_representation_id, technique.name,
  technique.version)`, validated by `MatchResult`.
- `_derive` hashes a tag and the length-prefixed parts with SHA-256 (first
  128 bits): injective in its inputs up to hash collision.
- `Spec.name = document.family`, `Spec.version = str(document.pipeline_version)`,
  both inside the canonical document whose SHA-256 is `config_digest`: **the
  spec digest determines the spec name and version.**
- Several valid representations of one `ContentId` coexist (one per spec:
  `dinov2-seg/1` and `/2` today; FP16 variants if ever enabled), and
  `target_representations` has CK `(version, spec_digest)`, so one target
  version can have several bindings.

Therefore the MatchId is a function of exactly
`(content_id, media_spec_digest, target_id, target_version,
target_spec_digest, technique_name, technique_version)`.

The first proposal keyed `matches` by `(content_id; target_id,
target_version, technique_version)`. **That key is not injective:** the same
content matched against the same target version under `dinov2-seg/1` and
under `dinov2-seg/2` with the same technique version gives two valid,
distinct MatchIds (different representation ids) and one key, so one record
would overwrite or block the other. A different `technique_name` collides
the same way.

Corrected key: PK `content_id`, CK `(target_id, target_version,
media_spec_digest, target_spec_digest, technique_name, technique_version)`.
It contains every MatchId input (spec name/version follow from the digests),
so distinct MatchIds have distinct keys, and a key determines its MatchId;
the writer asserts `match_id == MatchId.of(...)` of the key components.
`media_spec_digest = target_spec_digest` in P10 (no cross-spec matching);
both are kept so a future compatibility rule cannot reintroduce a collision.
Access patterns: by content (partition), by target version
(`matches_by_target`), by id (`matches_by_id`). The table stores **every
distinct immutable decision**, not "one current match per pair": a new
technique version adds rows and never replaces old ones.

### 9.4 Re-evaluation

A new technique version or index generation does not re-run old tasks
automatically. `match rerun --technique 2.<n> [--since …]` creates new tasks
explicitly (rate-limited, audited); old matches remain under their version.

## 10. Level-5 media equivalence (P10-D13)

- **Path:** `media_vs_media` tasks query the media collection of the same spec
  with adaptive k (§7), exclude the subject's own content, group by content,
  verify with `temporal/v2` and then apply the **equivalence rule**
  `equiv/v1`: one run covering ≥ 90 % of the informative segments of
  *both* items, absolute duration difference ≤ max(5 s, 2 %), and
  |offset| ≤ 10 s (container/packaging start differences). Partial overlap
  (a clip of a film) is a match-like relation, not equivalence, and is not
  recorded in P10.
- **Persistence:** `equivalence_links` rows in both orientations with state
  `LINKED`, confidence (calibrated on the equivalence set), technique version,
  evidence (alignment, coverage), provenance, `decided_at`, via an intent.
- **Reversible:** `equivalence revoke <a> <b> --reason …` sets `REVOKED` with
  an audit entry; nothing is deleted; a later technique version creates its
  own rows. **No `ContentId`, representation, media or observation is ever
  merged** (level 3 stays byte identity; P8 invariant).
- **Visibility to crawler2:** no contract event exists. P10 exposes the
  relation through the CLI and the table; a `media.equivalence_linked` event
  is a separate contract decision (P10-D13 part b, deferred).
- Equivalence can be disabled (`matching.equivalence_enabled`) without
  affecting A/B matching.

## 11. Concurrency, leases, idempotency, failures

### 11.1 Task state machine

`PENDING → INDEXED → SEARCHED → VERIFYING(page) → DONE | INCOMPLETE`;
`→ WAITING_INDEX` when the spec/role has no ACTIVE generation or Qdrant is unhealthy (re-tried
with backoff, not failed); `→ FAILED` only for non-retryable causes
(artifact SHA-256 mismatch, invalid vector, missing representation row),
each with a typed reason. Every transition is an LWT on `epoch`; a lost lease
returns the task to the due index (P9 §7 pattern). States are resumable at
each boundary: re-indexing is idempotent (deterministic ids), the candidate
list is durable, verification pages are idempotent (`match_id`).

### 11.2 Failure semantics

| Failure | Detection | Behaviour |
|---|---|---|
| Qdrant unreachable / timeout | `IndexUnavailableError` | task → `WAITING_INDEX`, backoff 30 s → 15 min; metric `fingerprinter_index_unavailable_total`; health endpoint reports degraded; no fallback |
| no ACTIVE generation / collection missing (index lost) | `IndexMissingError` | as above; operator runs `index rebuild` (§12) |
| Qdrant collection RED / point-count mismatch | `index check` | generation marked suspect; rebuild recommended; queries continue on the active generation unless the operator deactivates it |
| stale entry (point whose representation is retired/absent) | candidate validation against Scylla before verification | dropped from the candidate list, counted `index_stale`, repaired by deleting the subject's points |
| missing entry (READY representation without points) | `index check` / `index_entries` | re-indexed; counted `index_miss` |
| Scylla unavailable | `StorageUnavailableError`, `LwtUnknownError` | retry with backoff; LWT unknown resolved by SERIAL read (P9 pattern) |
| MinIO unavailable / artifact missing | typed error from `Objects` | candidate retryable (×3) → `verification_failed`; artifact SHA-256 mismatch → FAILED + operator alert (authoritative data corrupt) |
| verification timeout | per-candidate wall-clock limit (default 30 s) | candidate retry ×3 → `verification_failed` (task INCOMPLETE) |
| matcher crash | lease expiry | task resumes at its last durable state |
| duplicate hints / events / tasks | `IF NOT EXISTS`, deterministic ids | no-ops |
| concurrent indexers | idempotent upserts | converge |
| overload | bounded `matcher.workers`, due-index backpressure | latency rises; nothing dropped |
| index capacity reached | capacity guard (§13.3) | subject `BLOCKED: capacity`, metric + alert; never an OOM-risking write |

## 12. Index lifecycle: mutations, generations, rebuild, reconciliation (P10-D2, P10-D19, P10-D20; corrected C-3)

Qdrant and Scylla share no transaction. The protocol below is designed so
that every divergence between them is either prevented, or detectable by a
named check with a named source of truth and a deterministic repair. It is
owned by P10 and reuses P9's *patterns* (LWT epochs, leases, intents,
reconciler passes), not P9 code paths.

### 12.1 Generation parameters

A generation's params (Qdrant version, distance, HNSW `m`/`ef_construct`,
quantization, on-disk flags, payload schema version, `lowinfo` rule) are
written to `index_generations` before the collection is created and are
immutable; their digest appears in match provenance.

### 12.2 Mutation protocol (one row per subject = intent + bookkeeping)

A *subject* is a content representation (media role) or a target
representation (target role). Its row in `index_entries` is the durable
intent.

1. **Desired state (intent).** Any producer (task step, hint reader,
   backstop scan, lowinfo refresh) derives the desired state from the
   authoritative rows: `UPSERT` with `desired_ver` = artifact SHA-256 (+
   `lowinfo_rev` for targets) if the representation is READY (target: binding
   READY and target not RETIRED), else `DELETE`. If it differs from the row,
   write it with `UPDATE … SET desired_op, desired_ver, seq = :s+1, state =
   'PENDING' IF seq = :s` (or `IF NOT EXISTS`). Nothing touches Qdrant before
   this LWT succeeds. Equal desired state ⇒ no write (duplicate events are
   no-ops).
2. **Apply.** A worker takes the subject's Redis lease (avoidance only),
   reads the row (`seq = s`), re-reads the authoritative row and, if the
   desired state is no longer current, goes back to step 1. It reads the
   routing set = generations in states `BUILDING … DRAINING` (§12.4) with
   `routing_epoch`, then applies to each: UPSERT = upsert all points of the
   subject (deterministic ids, payload incl. `seq = s`), `wait=true`;
   DELETE = delete by filter `rep`/`trep` = subject, `wait=true`. Both are
   idempotent, so an **uncertain outcome** (timeout, connection loss) is
   handled by repeating the operation; there is no need to know whether the
   first attempt landed.
3. **Bookkeeping.** `UPDATE index_entries SET applied[g] = :s, state =
   'APPLIED' IF seq = :s` for the generations applied. If the LWT fails
   because `seq` advanced, a newer intent exists: the worker re-reads and
   re-applies the newer desired state itself (so the last writer to finish
   leaves Qdrant in the newest state). After `max_attempts` failures the row
   becomes `BLOCKED` with `last_error` (visible, retried by the reconciler).
4. **Completion.** A mutation is complete when `applied[g] = seq` for every
   generation that was in the routing set at the time of its routing epoch.

**What cannot be fenced, and how it is caught.** Qdrant has no conditional
writes, so a *stale* worker (lease lost, slow) can still land an older
UPSERT after a newer DELETE was applied and recorded, and then crash before
its failing LWT. Scylla alone cannot see that. It is caught by the Qdrant-side
verification of §12.6 (points carry `seq`; a point with `seq` < the row's
`seq`, or present while the row says DELETE, is stale) and repaired by
re-applying the row's desired state. The window is bounded by the
verification cycle, and stale points are also dropped at query time because
candidates are validated against Scylla before verification (§11.2).

| Crash point | State left | Recovery |
|---|---|---|
| before the intent LWT | nothing changed | next producer/backstop writes the intent |
| after intent, before Qdrant | row PENDING | reconciler applies |
| after Qdrant, before bookkeeping | Qdrant done, row PENDING | reconciler re-applies (idempotent), records |
| after bookkeeping | consistent | — |
| during DELETE (partial) | some points left, row PENDING | re-apply delete by filter |

### 12.3 Incremental updates

- New representation / binding → intent + apply (task step 1).
- Target retired → intent DELETE for its target representation; target-set
  revision bump (§7.3); its tasks stop; matches stay.
- Spec retirement → `spec retire` is refused while
  `retire_guard(spec, "p10-index")` exists; the operator first runs
  `index drop --spec …` (all generations → RETIRED, collections deleted),
  which removes the guard. The P10 guard row uses P9's existing table as P9
  designed it (P9 exit §7 handoff); P9 code is unchanged.

### 12.4 Generation state machine

| State | Meaning | Routed for live writes | Searched |
|---|---|---|---|
| `CREATED` | row written, collection created, `routing_epoch` bumped | yes | no |
| `BUILDING` | bulk load from the authoritative scan | yes | no |
| `CATCHING_UP` | lag scan until no subject lags | yes | no |
| `VALIDATING` | completeness and equivalence checks | yes | no |
| `READY` | validated, waiting for promotion | yes | no |
| `ACTIVE` | the generation readers use (exactly one per spec/role) | yes | yes |
| `DRAINING` | previous active, kept for `drain_grace_s` ≥ 2 × `active_ttl_s` + max task lease | yes | by readers with a cached old pointer only |
| `RETIRED` | collection deleted | no | no |
| `FAILED` | abandoned (from any non-active state); collection kept for inspection, then deleted by `index drop --generation` | no | no |

Legal transitions: CREATED→BUILDING→CATCHING_UP→VALIDATING→READY→ACTIVE→DRAINING→RETIRED;
VALIDATING→CATCHING_UP (lag found); any of CREATED…READY→FAILED. Every
transition is an LWT on the generation row conditioned on the builder's
`build_epoch`; ACTIVE is entered only through the promotion LWT.

**Ownership.** A builder holds `index_build_lease(spec, role)` (owner, epoch,
lease_until; acquire `IF epoch = :e`). A second rebuild cannot acquire it; a
stale builder's transitions fail on the epoch. A new holder adopts an
existing non-terminal generation and continues from its state and per-bucket
cursor, or marks it FAILED.

**Build and catch-up — the completeness proof.**
1. CREATED: insert the generation and bump `routing_epoch` (same LWT
   partition). From this moment every mutator that *reads the routing set
   after* the bump includes the generation.
2. BUILDING: for every bucket of the authoritative scan (§6.1 B), write the
   intent (step 1) for each subject and apply it to the new generation;
   record the bucket cursor after each page. A crash resumes from the cursor.
3. CATCHING_UP: wait until `boundary_at = created_at + max mutation lease +
   clock margin` has passed — after that, no mutator can still hold a routing
   set read before step 1. Then scan all `index_entries` partitions of
   (spec, role): every row with `applied[g] ≠ seq` (or missing) is applied
   to g by the pass itself and re-checked before the pass moves on. The step
   ends when one full pass has completed with every row satisfying
   `applied[g] = seq` at the moment it was checked (this terminates under
   continuous writes, because the pass repairs instead of waiting).
4. VALIDATING (§12.5). Any lag found → back to CATCHING_UP.
5. READY.
Every mutator writes its intent row *before* it reads the routing set
(§12.2), so a mutator that missed g has a row that existed before the
catch-up pass began; every subject has a row (the bulk load writes them
too); every row is either applied to g by its own mutator or by the pass.
Hence no subject can be missing from g at promotion (the `boundary_at` wait
is an extra margin, not required for this argument); tombstones are rows with `desired_op = DELETE`
and are checked the same way.

**Promotion.** One single-partition LWT batch on `index_generations(spec,
role)`: `SET active_generation = g, active_epoch = e+1, state(g) = ACTIVE,
state(old) = DRAINING IF active_epoch = e AND state(g) = 'READY'`. Readers
see either the old or the new pointer. Crash before: g stays READY and
continues to receive live writes; the lease holder (or the next one)
re-validates if READY is older than `revalidate_after_s`, then promotes.
Crash after: done; the reconciler moves DRAINING → RETIRED after the grace
and deletes the collection. A stale worker writing to a RETIRED generation
gets "collection not found", checks the generation state in Scylla and
treats it as a no-op; for any other state it is an error.

### 12.5 Validation and rebuild equivalence

A generation is promoted only if all of these hold:

| Check | Source of truth | Pass condition |
|---|---|---|
| completeness | `index_entries` | a full pass completed with every row at `applied[g] = seq` when checked |
| manifest | `index_entries` UPSERT rows | Qdrant count per subject (filter `rep`/`trep`) = `segment_count`; total = Σ |
| no extras | `index_entries` | Qdrant scroll of g (ids + payload `rep`/`trep`, `seq`): every point belongs to an UPSERT row with equal `seq` |
| content digest | artifacts | per subject, SHA-256 over (sorted point ids, float32 original bytes, canonical payload without `seq`) equals the digest computed from the artifact; bucket digests combined order-independently into the generation manifest digest |
| classification | `lowinfo_class` | target payload `lowinfo_rev` = the row's `rev` |
| params | `index_generations` | Qdrant collection config equals the recorded params |
| accessibility | — | health green, a fixed probe query set answers |

Two generations built from the same authoritative state must have **equal
manifest digests** (ids, vectors, roles, metadata, retirement state,
classification revision). ANN query output is approximate and is not
compared bit-for-bit; decision invariance is tested instead (validation §5).

### 12.6 P10 reconciler (P10-owned process; corrected C-4)

`python -m fingerprinter matcher reconcile run` — a P10 role, separate from
P9's `reconcile run`, with its own leases. Passes:

| Pass | Finds | Repair |
|---|---|---|
| intents | `index_entries` PENDING/BLOCKED older than grace; `match_intents` stale | apply / resolve |
| scans | §6.1 B (missed hints, retirements) | write desired state, tasks |
| Qdrant verification (per generation, scroll in id order, cursor persisted, rate-limited; full cycle ≤ `verify_cycle_s`, sampled vector digests) | point without UPSERT row (orphan); point `seq` < row `seq` (stale); row APPLIED but points missing or count ≠ `segment_count`; point present for DELETE row | re-apply the row's desired state; delete orphans by id |
| generations | stuck non-terminal generations, expired build leases, DRAINING past grace | resume, FAILED, RETIRED |
| lowinfo | `done_rev < requested_rev` | resume refresh |

| Mismatch | Source of truth | Repair |
|---|---|---|
| authoritative READY representation missing from Qdrant | Scylla `representations` | intent UPSERT + apply |
| Qdrant point without valid authoritative representation | Scylla | delete point |
| stale vector/metadata (`seq`) | `index_entries` desired state (derived from Scylla) | re-apply |
| `index_entries` APPLIED but point missing | Qdrant verification | re-apply, count `index_miss` |
| point without `index_entries` row | Scylla | if authoritative READY: write intent; else delete |
| abandoned intent | `index_entries` | apply |
| delete succeeded in one store only | `index_entries` DELETE | re-apply delete |
| generation content ≠ manifest | artifacts | stays non-active / FAILED; rebuild |

### 12.7 Loss of the complete index

Delete the Qdrant volume (or lose the host) ⇒ collections gone ⇒ tasks wait
(`WAITING_INDEX`) ⇒ `index rebuild` for each (spec, role) creates a new
generation from Scylla + MinIO (§12.4) ⇒ promotion ⇒ waiting tasks resume. The
`index_entries` `applied` maps for the lost generations are ignored (they
refer to RETIRED/FAILED generations). No match, task, equivalence or
representation fact is lost. The target role is rebuilt first so direction A
resumes before B.

### 12.8 Rebuild duration (extrapolated from measured lower bounds)

Measured: 315 vectors/s single-stream artifact reads (contended, HDD-backed
MinIO) and 310–500 points/s Qdrant REST ingest to "ready" (audit §11). With
r ≈ 400 vectors/s:

| Volume | Vectors | Duration at 400/s | At 3,000/s (target after gRPC + parallel readers; to be measured) |
|---|---|---|---|
| SOAK-scale (measured corpus) | 0.1 M | 4.4 min (measured 3.5–5.6 min) | — |
| Low 1 y (media) | 7.2 M | 5.0 h | 40 min |
| Base 1 y (media) | 23.5 M | 16.3 h | 2.2 h |
| Base 3 y (media) | 70.5 M | 49 h | 6.5 h |
| Targets, base | 0.6 M | 25 min | 3.3 min |

Confidence: low — rates measured once each, under contention, with REST.
Media-role volumes beyond the dev capacity (§13) are not rebuildable on the
dev host at all.

## 13. Resource budgets (P10-D14, corrected C-1)

Labels: **M** measured (audit §9, §11), **D** arithmetic derived from a
measurement, **E** estimate from data-structure sizes, **U** unknown.

### 13.1 What the existing Qdrant measurements prove (104,443 real vectors, int8 + on-disk originals, m=32)

| Quantity | Value | Label |
|---|---|---|
| idle process | 25 MiB | M |
| steady state after build, 512 MiB cap | 152–156 MiB ⇒ (156 − 25) MiB / 104,443 ≈ **1,315 B/vector** resident | M, D |
| build peak, 256 MiB cap | OOM-killed ⇒ peak need > 256 MiB ⇒ > **2,319 B/vector** above idle | M, D |
| build peak, 512 MiB cap | completed with `memory.peak` = the cap (reclaimable file pages included) ⇒ need ≤ 512 MiB ⇒ ≤ **4,889 B/vector** above idle | M, D |
| float32 in RAM, 3 GiB cap | 497 MiB after build (may include page cache) | M |
| disk per vector (originals + int8 + graph + payload) | ≈ 4.2 KB per generation | E (not measured; `du` not recorded) |

They prove that **104 k vectors build and serve inside 512 MiB**. They do
**not** prove that peak memory scales linearly, nor anything about a build
while another generation is resident, nor about live writes during a build.
Linear scaling is assumed below only to locate the risk.

### 13.2 Scenario table

Raw FP32 = vectors × 768 × 4 B = vectors × 3,072 B (GiB = 2³⁰ B). Steady =
25 MiB + 1,315 B × N (D, linear extrapolation). Build peak range = 25 MiB +
(2,319 … 4,889) B × N (D, linear extrapolation of bounds).

| Scenario | Role | Vectors | Raw FP32 | Steady (int8) | Build peak range | Status under 1.5 GiB |
|---|---|---|---|---|---|---|
| measured corpus | media | 104,443 | 0.299 GiB | 0.15 GiB (M) | 0.25–0.50 GiB (M bounds) | **fits (M)** |
| targets low | target | 60,000 | 0.172 GiB | 0.10 GiB | 0.15–0.30 GiB | likely fits (D) |
| targets base | target | 600,000 | **1.717 GiB** | 0.76 GiB | **1.32–2.76 GiB** | **UNRESOLVED** — needs E-R1 |
| targets high | target | 6,000,000 | 17.17 GiB | 7.4 GiB | 13–27 GiB | does not fit (D) |
| media low 1 y | media | 7,227,000 | 20.68 GiB | 8.9 GiB | 15.6–32.9 GiB | does not fit (D) |
| media base 1 y | media | 23,487,750 | 67.20 GiB | 28.8 GiB | 50.8–107 GiB | does not fit (D) |
| media base 3 y | media | 70,463,250 | 201.6 GiB | 86.3 GiB | 152–321 GiB | does not fit (D) |
| media high 1 y | media | 284,700,000 | 814.5 GiB | 349 GiB | 615–1,296 GiB | does not fit (D) |

Float32 vectors in RAM are not an option for any role beyond ≈ 0.3 M
vectors on the dev host (the base target role's raw vectors alone exceed
the limit). With int8 in RAM, the float32 originals stay on disk and are read
for rescoring: final scores remain exact cosines; recall with rescoring was
measured (0.992); a rebuild reads the originals from MinIO artifacts, not
from Qdrant.

Capacity of one 1.5 GiB instance by linear extrapolation (D, unproven):
single build ≈ 324 k–683 k vectors; with a second generation resident during
a rebuild ≈ 255 k–436 k. The base target scenario (600 k) lies inside that
uncertainty band — hence E-R1.

### 13.3 Development configuration

| Item | Value | Status |
|---|---|---|
| Qdrant | 1.15.1, `mem_limit` **1.5 GiB (hard; changed only by a separate decision)**, swap 0, 2 CPUs; both roles int8 `always_ram` + on-disk originals + on-disk payload, m=32 | proposed |
| capacity guard | `index.max_points` per Qdrant instance (all collections, all generations); the indexer refuses an UPSERT that would exceed it (subject → `BLOCKED: capacity`, metric, alert) instead of risking an OOM; rebuild refuses to start if active + new points would exceed it | proposed |
| `index.max_points` value | **250,000** until E-R1 passes (≤ the lower bound of the with-rebuild band); E-R1 sets the final value | provisional |
| matcher (each) | RSS ≤ 1 GiB, artifact LRU 256 MiB, 2 verification workers | proposed |
| rest of the host | crawler2 stack ≈ 4 GiB (P0); GPU encoder 2–3 GiB typical, > 5 GiB seen for a 4K source; desktop/browsers the remainder; Qdrant is **not** given the host's free memory | constraint |

The dev configuration is therefore suitable for development corpora
(L-PAIRS, SOAK-scale, the 7 targets) and for the I-2…I-5 tests; it is **not**
claimed to hold the base target scenario.

### 13.4 Decisive experiment E-R1 (capacity boundary)

| Item | Specification |
|---|---|
| question | does one 1.5 GiB Qdrant instance hold the base target role (≥ 600 k vectors) *including* a rebuild with both generations resident and live writes? |
| corpus | the 104,443 cached real vectors + 5 seeded random orthogonal rotations of the same set = 626,658 vectors (rotations keep norms and intra-copy geometry and avoid exact duplicates that would degenerate HNSW; valid for memory/latency, **not** for recall); payload of the target role |
| Qdrant | 1.15.1, container `--memory 1.5g --memory-swap 1.5g --cpus 2`, dev configuration of §13.3, gRPC batches of 1,000 |
| phases | (1) build g1 (626,658) and wait for green; (2) 3 × 760 real target queries, warm; (3) build g2 of the same set while g1 serves 10 queries/s and 1 subject/s is upserted into both; (4) promote, delete g1 |
| measurement | cgroup `memory.current` and `memory.stat` (anon/file) sampled every 1 s, `memory.peak`, `memory.events` (`oom_kill`), per phase; `du` of the storage volume per phase; query latency |
| pass | no `oom_kill`; anon (non-reclaimable) peak ≤ 1.35 GiB (10 % margin); query p95 ≤ 10 ms after warm-up in phases 2 and 3 |
| outcomes | pass ⇒ `max_points` = 626 k and the base target role is supported on the dev host; fail in phase 3 only ⇒ `max_points` = the largest single-generation size that passed, and a rebuild of a full instance needs a separate, approved procedure (e.g. an offline window or a second instance); fail in phase 1 ⇒ repeat at 313 k (3 copies) to set `max_points`; the base target scenario then needs the first deployable configuration below |
| cost | ≈ 30–60 min host time, run alone; no new data |

### 13.5 First deployable and scale-out configurations (input to P14, not validated)

- **First deployable:** Qdrant on a dedicated index host with SSD; memory
  sized from E-R1's measured per-vector peak × planned points × 2 (rebuild
  coexistence) + margin; the target role always in one instance; the media
  role sized by the ingestion rate actually observed (Q-1).
- **Scale-out:** Qdrant distributed mode (shards/replicas) or on-disk HNSW
  with PQ, each **re-measured for recall and peak memory before use** (PQ ×16
  measured at 104 k only: recall@100 0.982).

### 13.6 Disk

Per generation ≈ 4.2 KB/vector (E). During a rebuild two generations
coexist (×2) plus WAL and optimizer temp segments (U; E-R1 measures `du` per
phase). Dev: 600 k target vectors ≈ 2.5 GB per generation, ≈ 5 GB during a
rebuild; the dev disk has ≈ 290 GB free. Authoritative artifacts in MinIO are
not duplicated by the index.

## 14. Metrics and operational controls

Metrics (`fingerprinter_` prefix, P9 registry):

| Metric | Labels |
|---|---|
| `match_tasks` (gauge) | kind, state |
| `match_queries_total` | kind, outcome (`ok`, `index_unavailable`, `index_missing`) |
| `match_query_seconds` (histogram) | kind |
| `match_ann_hits_total`, `match_hits_above_floor_total` | kind |
| `match_lowinfo_segments_total` | kind |
| `match_candidates_total` | kind, stage (`grouped`, `dedup_removed`, `below_min_hits`, `stored`, `verified`, `truncated`) |
| `match_saturated_segments_total` | kind |
| `match_verify_seconds` (histogram), `match_verify_failures_total` | kind, reason |
| `match_decisions_total` | kind, verdict (`confirmed`, `probable`, `no_match`) |
| `match_incomplete_total` | kind, reason |
| `match_spec_uncovered` (gauge) | role |
| `index_points` (gauge) | spec, role, generation |
| `index_upsert_seconds`, `index_upsert_points_total` | role |
| `index_stale_total`, `index_miss_total`, `index_unavailable_total` | role |
| `index_build_progress` (gauge, buckets done) | spec, role, generation |
| `equivalence_links_total` | state |
| `index_entries` (gauge) | spec, role, state (`PENDING`, `APPLIED`, `BLOCKED`) |
| `index_verify_stale_total`, `index_verify_orphans_total` | role |
| `index_capacity_blocked_total` | role |
| `p10_hint_gaps_total`, `p10_scan_cycle`, `p10_scan_rows_total`, `p10_scan_cycle_seconds` | scan |
| `lowinfo_targets` (gauge) | spec, state (`current`, `stale`, `unknown`) |
| `lowinfo_refresh_pending_revs`, `lowinfo_refresh_lag_seconds`, `lowinfo_refresh_failures_total` | spec |
| `match_lowinfo_excluded_total` | kind |
| `match_unpublished_uncalibrated` (gauge) | technique version |

CLI (all state changes audited):
`index status|build|rebuild|resume|check [--sample N]|activate|drop`;
`match status|show <content|target>|retry [--raise-cap]|rerun|reconcile`;
`equivalence show|revoke`; `match publish-held`; `lowinfo status|refresh`;
service roles `matcher run` (tasks, hint reader) and `matcher reconcile run`
(the P10 reconciler, §12.6). P9's `reconcile run` is not changed.

Structured logs: one JSON line per task transition and per incomplete or
failed outcome, with task id, kind, spec, generation and counts.

## 15. Security and data handling

- Qdrant listens on the backend network/loopback only, with an API key
  (`FINGERPRINTER_INDEX__API_KEY`); no public port; no snapshots exported off
  the host.
- Vectors are derived from possibly copyrighted media; they are internal
  data. Points carry ids and timestamps only — no URLs, titles or page data.
- `Blast.mp4` and other local-only material are never indexed into shared or
  exported stores.
- The matcher does no network fetches of media (only MinIO/Scylla/Redis/
  Qdrant).

## 16. Multi-host behaviour

Any matcher on any host: shared Scylla state, Redis leases, one Qdrant
endpoint. Correctness does not depend on host-local state (the LRU cache is
validated by artifact SHA-256). Simulated two-host tests in P10 (two matcher
processes, one Qdrant, one stack), including concurrent indexing of the same
subject and lease takeover. Qdrant replication/sharding, network partitions
between hosts and the index, and real machines are **P14**.

## 17. Known limitations and deferred work

- Sampling phase (audit F-1) is reduced by the verifier, not removed; a
  denser-sampling spec is a future P9-style decision.
- VFR/speed drift not modelled (audit M-8).
- Thresholds are fitted on animated films from one reel (audit R-7); transfer
  to live action is unmeasured.
- Equivalence is not visible to crawler2 until a contract event exists.
- ANN provenance not in `match.found` 1.0.
- Rebuild rates are lower bounds; production-scale memory figures beyond
  104 k vectors are estimates.
- Real multi-machine, Qdrant replication: P14.
- Dev capacity is provisional (`max_points` 250 k) until E-R1 (§13.4).
- Qdrant writes cannot be fenced; stale points are found by the Qdrant
  verification cycle, not prevented (§12.2).
- Hints can be trimmed before P10 reads them; discovery latency is then the
  backstop cycle (§6.1).
- Calibrated confidence may be unavailable with the current labelled data;
  publication then waits (§9.1).

## 18. Rollout and increments (after approval)

| Increment | Content | Depends on | Exit |
|---|---|---|---|
| **I-1** (first) | (a) **V1 end-to-end baseline**, reproducible (validation §2.1) — before any verifier threshold is chosen; (b) verifier `temporal/v2`, `lowinfo/v1` evaluated offline on the vector cache against an unfiltered oracle; (c) nested film-grouped tuning and calibration; (d) dataset adequacy statement. No Qdrant. | D7, D8, D9, D10, D17 | baseline report; held-out P/R vs both V1 baselines; calibration status; param set `2.1` registered or an explicit "data insufficient" outcome (validation §2–§3) |
| I-2 | `vindex` + Qdrant (fingerprinter-owned dev compose), E-R1, generations, mutation protocol, builder/catch-up/validation/promotion, P10 reconciler (intents, scans, Qdrant verification), capacity guard | D1–D4, D14, **D19, D20**, E-R1 result | validation §5, §9 (index part), §10 |
| I-3 | match tasks, hint reader, direction A and B, candidates, commits with the corrected `matches` key, held `match.found` | D5, D6, D11, I-1 calibration status | validation §4, §6, §7, §9, §9a |
| I-4 | equivalence | D13, D18 | validation §8 |
| I-5 | failure injection, growth test, simulated two-host, M3 run, docs | all | M3 gate (validation §11); real multi-machine stays P14 |

Each increment is committed separately with tests and documentation.
Migrations are additive (P10 tables only). P10 can be disabled by not
running `matcher run` / `matcher reconcile run` — P9 behaviour is
unaffected. I-2 does not start until E-R1, D19 and D20 are approved; I-3
does not publish until a calibrated technique version exists.

## 19. Correction record (2026-10-15, after the first design review)

| # | Original assumption | Why unsafe / inconsistent | Correction | Decision impact | Evidence | Outstanding | Gate before implementation / M3 |
|---|---|---|---|---|---|---|---|
| C-1 | 1.5 GiB Qdrant; target role float32 in RAM; dev capacity "≈ 1 M vectors" | base target raw FP32 = 1.717 GiB > limit; steady-state bytes/vector were used as if they bounded the build peak | §4.2 both roles int8 + on-disk originals; §13 separates raw / steady / build peak / disk / host; capacity guard; `max_points` 250 k provisional | **changed** D2, D14 (still proposed); dev capacity **unresolved** | audit §9.3 measurements; arithmetic §13.2 | E-R1 | E-R1 before I-2; base target scenario not claimed |
| C-2 | stored `lowinfo` flags on media and target points | flags depend on the target set; a registration/retirement could leave stale flags that silently exclude candidates | §7.3: media never flagged; target flags versioned by `target_set_rev`; exact affected-set rule; LWT `IF rev <`; stale ⇒ eligible; refresh worker + weekly backstop | **changed** D8 (still provisional) | design reasoning; no measurement yet | validation §3.4 offline oracle comparison; tests L-1…L-10 | I-1 measurement before use |
| C-3 | "upsert, then record `index_entries`"; alias switch as promotion | no atomicity between Qdrant and Scylla; uncertain outcomes; stale writers; promotion of an incomplete generation; alias/Scylla disagreement | §12: intent-first mutation protocol with `seq`; Qdrant-side verification for unfenceable writes; generation state machine; build lease; completeness proof; single-LWT promotion; manifest digests | **new** D19; D2 clarified | P9 LWT/lease patterns (`intake.py`, `commit.py`) | tests R-1…R-13 | D19 approval before I-2 |
| C-4 | "P9 code unchanged" and "the P9 reconciler gains a P10 pass" | contradictory; couples P9 to index consistency | §6.1, §12.6: P10-owned hint reader (cursor after durable work, epoch fencing) and P10 reconciler with persisted backstop cursors; eventual-catch-up statement | **new** D20; D5 clarified | `publish.py` (XTRIM MINID; target events held), `intake.py` lease pattern, `db.py` schema | tests H-1…H-7 | D20 approval before I-2 |
| C-5 | `confidence` = "precision among decisions at least this strong", fitted by isotonic | conflates per-decision calibrated probability with threshold-level precision | §9.1: confidence = calibrated probability of correctness; operating-point P/R reported separately; nested grouped protocol; publish only when calibrated | **changed** D9 | statistics; seven-film data limits | validation §3; calibration may be unavailable | I-1 outcome; no publication without calibration |
| C-6 | `matches` key `(content_id; target_id, target_version, technique_version)` | not injective: two specs (or technique names) ⇒ two MatchIds, one key | §9.3a verified identity and corrected key; `matches_by_id` | **changed** D11 | `ids.py`, `models/matching.py`, `spec.py` | tests K-1…K-8 | — (design fix) |
| C-7 | V1 end-to-end baseline "measured in I-1" without being a prerequisite | a verifier tuned against a partial V1 path can look better than V1 | I-1 starts with the reproducible V1 end-to-end baseline; it gates threshold selection and M3 | **clarified** D17 | `worker/matching_handler.py` entry point | baseline run | before verifier thresholds and M3 |
