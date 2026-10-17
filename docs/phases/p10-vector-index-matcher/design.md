# P10 Design — vector index and matcher (proposal)

Status (2026-10-18): **APPROVED FOR IMPLEMENTATION; implementation in
progress** (operator authorization, decisions.md §0). This document stays the
design of record; where implementation evidence changed or settled a point,
the text says so and cites validation.md §13 (execution record). Before
2026-10-18: PROPOSED — awaiting operator design approval. Decisions are
numbered P10-D1…P10-D20 and tracked in [decisions.md](decisions.md); the
evidence is in [audit.md](audit.md) (§ references below are to the audit
unless stated otherwise); tests and acceptance criteria are in
[validation.md](validation.md); the index architecture is recorded as
[ADR-028 (proposed)](ADR-028-vector-index.md).

**Corrected 2026-10-15** after the first review (C-1…C-7, §19): resource
budget, low-information freshness, Qdrant/Scylla mutation and rebuild
protocol, P10-owned reconciliation, confidence semantics, `matches` key,
V1 end-to-end baseline as an I-1 prerequisite. Still a proposal.

**Second correction 2026-10-15** (C-8…C-11, §19): target backstop scan
pagination (§6.1), low-information freshness and affected-target enumeration
(§7.3), capacity accounting and E-R1 (§13.3–§13.4), mutation/rebuild
guarantees (§12.9). Still a proposal; nothing approved.

**Third correction 2026-10-17** (C-12, §19.1): stale DELETE after a newer
UPSERT — sequence-versioned point ids and sequence-bounded deletes (§4.3,
§12.2), strict vs eventual guarantees and task completion under a suspect
generation (§12.9 H, §11.1). Still a proposal; nothing approved.

**Fourth correction 2026-10-17** (C-13, §19.1): the C-12 rule "a task reads
`suspect_since` and may not finish DONE while the generation is suspect" was
a check-then-act across two partitions and did not hold. Replaced by the
durable index-integrity recovery protocol of **§12.10** (recovery ledger
`index_repairs`, run-numbered task reruns fenced on the task row, detection
before any repair, DRAINING search horizon); §11.1, §12.4, §12.6, §12.9 H
changed accordingly. Also: lowinfo witness cosines confirmed on artifact
vectors (§7.3); E-R1 phase 3 sized to the rebuild-admission requirement
(§13.3–§13.4). Still a proposal; nothing approved.

**Fifth correction 2026-10-17** (C-14, §19.2): consistency and edge-case
audit. Recovery occurrences get a unique `occurrence_id` and the task map is
keyed by it (C-13 reused one id across occurrences, so an old run could
satisfy a new loss); detection-versus-repair fence; unresolved closes keep
negatives provisional and an exact `settled` rule (§12.10); verifier inputs
independent of ANN (§8); first decision per `match_id` final (§9.3);
saturation ⇒ INCOMPLETE (§7.5); per-hit low-information exclusion (§6.2);
pinned search step and deadline (§11.1); one liveness predicate (§4.2);
capacity released only by the audit (§13.3); lost ACTIVE/DRAINING
collections (§12.4); `equiv/v1` coverage (§10). Still a proposal; nothing
approved.

**Sixth correction 2026-10-17** (C-15, §19.3): C-14 reset one
`index_repairs` row per (subject, generation) when a new occurrence opened,
so an occurrence closed `RERUN_INCOMPLETE`/`TASK_FAILED` survived only as
history text and stopped counting as unresolved. Now one row per occurrence
(`occurrence` in the clustering key), never reset; resolution is
per-occurrence with durable evidence; `recovery_unresolved` counts every
occurrence row; a retired subject resolves its unresolved occurrences
(§12.10). Still a proposal; nothing approved.

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
            index_active, index_entries, index_repairs, match_intents, verifier_params
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
| `fingerprinter/verify.py` | verifier `temporal/v2`, pure function of two `fp-seg-npz/v1` payloads, a registered parameter set and the set of excluded (usable low-information) target segments; it receives no ANN output (§8) |
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
  segments of every **live** target binding, keyed by the target
  representation. The two roles have different cardinalities and budgets
  (§13).
- **Liveness — one predicate used everywhere (corrected C-14).** A media
  subject is live iff `representations(content_id, spec).state = 'READY'`. A
  target subject is live iff `target_representations(tid, ver, spec).state =
  'READY'` **and** `targets(tid, ver).state ∉ {RETIRED, PURGED}`. (§4.2 said
  "not RETIRED" while §7.3 and §12.9 said "∉ {RETIRED, PURGED}".) Checked in
  P9 code (`commit.py`, `specs.py`, `targets.py`): a representation row is
  inserted once and becomes RETIRED only by spec retirement, which
  `retire_guard` blocks while P10 indexes the spec; a binding is bound READY
  once (`IF state = 'PENDING'`) and never unbound; a target version with a
  READY binding may still be REGISTERING or BUILD_FAILED for a moment
  (`bind_ready` moves it to READY right after the bind), and RETIRED/PURGED
  are never left. Hence liveness is **monotone**: once true it stays true
  until it becomes false for good. Desired state, candidate acceptance
  (§12.9 C), witness checks (§7.3), the backstop and the repair step all use
  this predicate.
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
| media | `uuid5(representation_id + ":" + seq + ":" + segment_index)` | `rep`, `cid` (content id), `spec` (full digest), `k` (segment index), `t` (pts_s), `seq` (mutation sequence, §12.2) |
| target | `uuid5(target_representation_id + ":" + seq + ":" + segment_index)` | `trep`, `tid`, `tver`, `rep` (content representation), `cid`, `spec`, `k`, `t`, `seq`, `lowinfo` (bool), `lowinfo_rev` (§7.3) |

`seq` is the subject's `index_entries` sequence of the intent being applied
(§12.2). Deterministic ids make every upsert of one intent idempotent across
hosts, retries and generations (duplicate events, concurrent indexers,
rebuild + live writes all converge). **Including `seq` in the id (corrected
C-12)** means an UPSERT of intent *u* writes only ids of *u*: it can never
overwrite the points of a newer intent, and the points of different intents
of one subject are distinct points. Index ids never appear in Scylla facts or
events; they are recomputed from domain ids and `seq`. Payload indexes:
`rep`, `trep`, `cid`, `tid` (keyword), `seq` (integer — required by the
sequence-bounded deletes of §12.2). Media points carry **no**
low-information flag (§7.3).

## 5. Data model (new tables, fingerprinter keyspace, no TTL)

All tables are P10-owned; no P9 table gains a column and no P9 code writes
them.

| Table | Key | Content |
|---|---|---|
| `index_generations` | PK `(spec_digest, role)`, CK `generation`; **static** `active_generation`, `active_epoch`, `routing_epoch`, `next_generation` | per generation: state (§12.4), collection, params JSON + digest, `boundary_at`, per-bucket build cursor, expected/actual counts, `reserved_points` (§13.3), manifest digest, build owner epoch, `detect_count` (LWT-incremented per detected loss), `validated_detect_count`, `suspect_since` / `suspect_reason` (operator-visible flag; **never gates task completion**, §12.10), `audit_state` / `audit_cursor` (per-subject audit, §12.10), `promoted_at`, `drain_horizon_at`, `last_clean_verify_at`, timestamps, audit. Generation numbers come from `next_generation` and are never reused |
| `index_repairs` | PK `(spec_digest, role, bucket)`, CK `(subject_id, generation, occurrence)`, clustering `occurrence DESC` | **the index-integrity recovery ledger, one row per occurrence** (§12.10; corrected C-14, C-15): `occurrence_id` timeuuid (never reused), `state` OPEN/REPAIRED/RERUN_PENDING/CLOSED, `detections` int, `gen_marked` bool, `detected_at`, `detected_by`, `row_seq`, `expected_count`, `observed_count`, `repaired_at`, `required_run map<text, int>` (task kind → run), `close_reason`, `closed_at`, `resolution` PENDING/RESOLVED, `resolution_reason`, `resolved_by map<text, int>` (kind → qualifying DONE run), `resolved_at`, `attempts`, `next_attempt_at`, `last_error`. A row is created once by `INSERT … IF NOT EXISTS`, never reset, never deleted while the spec is indexed; there is no head row (the latest occurrence of (S, g) is read) |
| `index_capacity` | PK `instance` (one Qdrant instance), CK `holder` | the capacity ledger (§13.3; corrected C-14): **static** `max_points` (copied from config, audited), `reserved` bigint, `updated_at` — `reserved` changed only by LWT `IF reserved = :r`; one clustering row per rebuild admission (`holder = gen:<spec12>:<role>:<g>`, `points`), written in the same conditional batch as the reservation |
| `index_build_lease` | PK `(spec_digest, role)` | `owner`, `epoch`, `lease_until` — one builder per (spec, role) (P9 intake lease pattern) |
| `index_entries` | PK `(spec_digest, role, bucket)`, CK `subject_id` | **the durable mutation intent and its bookkeeping** (§12.2): `desired_op` UPSERT/DELETE, `desired_ver` (artifact SHA-256, plus `lowinfo_rev` for targets), `seq` bigint, `applied map<int, bigint>` (generation → applied seq), `segment_count`, `state` PENDING/APPLIED/BLOCKED, `attempts`, `last_error`, `updated_at` |
| `p10_readers` | PK `reader` | hint-reader lease and cursor: `owner`, `epoch`, `lease_until`, `cursor_id`, `last_gap_at` (§6.1) |
| `p10_scans` | PK `(scan, spec_digest)`, CK `bucket` | backstop scan cursor, `cycle`, `cycle_started_at`, `last_key`, `owner_epoch`, `updated_at` (§6.1). `reps`: one row per bucket, `last_key` = last `content_id`. `targets`: one row (`bucket = 0`), `last_key` = (`last_token`, `last_target_id`) of the last **completely processed partition** |
| `target_set_rev` | PK `spec_digest` | `rev` bigint, `members_digest`, `updated_at` — target-set revision for low-information classification (§7.3) |
| `lowinfo_class` | PK `spec_digest`, CK `target_representation_id` | `rev`, `rule` (`lowinfo/v1`), `status` (`COMPLETE`/`PARTIAL`), per-segment flag bitmap, `witnesses map<int, frozen<list<text>>>` (segment → ≤ 4 witness target representation ids of other works), `computed_at` (§7.3) |
| `lowinfo_refresh` | PK `spec_digest` | `requested_rev`, `done_rev`, `full_requested` bool, owner/epoch/lease, `last_error` (§7.3) |
| `match_tasks` | PK `subject_id`, CK `(spec_digest, kind)` | `kind ∈ {media_vs_targets, target_vs_media, media_vs_media}`; state machine §11.1; lease fields (`epoch`), **`run` int** (current run number), `run_started_at` (set by the claim LWT of the run), `done_run` int and `done_state` (DONE/INCOMPLETE) of the last run that finished, `done_ok_run` int (last run that finished DONE; C-15), `recovery_runs map<timeuuid, int>` (recovery **occurrence id** → the run it requires, §12.10; corrected C-14), `rerun_reason`; attempt, failures, `next_attempt_at`; `searched_generation` (pinned by the run's search step, §11.1); target-set rev used; ANN statistics JSON; candidate counts; `incomplete_reason`; timestamps |
| `match_due` | PK `(due_day, shard)`, CK `(due_at, subject_id, spec_digest, kind)` | discoverability index of non-terminal tasks (P9 `encode_due` pattern) |
| `match_candidates` | PK `(subject_id, spec_digest, kind, generation, run)`, CK `rank` | durable candidate list of **one run** of a task (bounded by its cap), per-candidate state; a new run never reuses an older run's list (§12.10) |
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
| target collection | live `target_representations` (§4.2) → bound content representation → artifact |
| `index_entries` | recomputable from the same authoritative rows (desired state); applied state is re-verified against Qdrant (§12.6) |
| `lowinfo_class`, target `lowinfo` payload | recomputed by `lowinfo/v1` (full recomputation, §7.3); used only through the per-flag witness check against P9 rows |
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
| `targets` | `target_representations` PK `target_id` CK `(version, spec_digest)` (small table, no bucket) joined with `targets(target_id, version).state` | **partition-granular** token walk, below (corrected C-8) |

**Targets scan pagination (corrected C-8).** Schema facts (`db.py`):
`target_representations` PRIMARY KEY `(target_id, version, spec_digest)` —
partition key `target_id`, clustering `(version, spec_digest)`, so one
partition holds every version × spec binding of a target and **several rows
share one token**; the keyspace is created with `tablets = {'enabled':
false}` (Murmur3 vnodes); P9 never deletes from the table (retirement is a
`targets.state` change; a binding goes PENDING→READY once, by LWT `IF state
= 'PENDING'`, `targets.py`). The first proposal's row cursor `token(target_id)
> last_token LIMIT 200` could end a page inside a partition and then skip
that partition's remaining rows on every cycle. Corrected procedure:

1. *Partition keys page*: `SELECT DISTINCT target_id, token(target_id) FROM
   target_representations WHERE token(target_id) >= :t LIMIT 100` (a token
   restriction on the partition key and `DISTINCT` on the full partition key
   are standard CQL; results come in token order). Checked 2026-10-15: the
   statement is accepted by the deployed Scylla 6.2.3 (read-only run against
   an existing, empty P9 test keyspace); ordering and completeness on
   populated data are verified by H-9. A cycle starts at `:t = -2⁶³`
   (Murmur3 minimum).
2. *Whole partition*: for each key in order, read the partition completely —
   `SELECT * FROM target_representations WHERE target_id = ?` (driver paging
   inside the single partition; a partition is versions × specs, small) and
   `SELECT version, state FROM targets WHERE target_id = ?` — and upsert the
   desired state of every binding (index entry, task, membership).
3. *Cursor*: only after **every row of the partition** has been made durable,
   `UPDATE p10_scans SET last_key = (token, target_id) IF owner_epoch = :mine`.
   The scanner holds the lease row `p10_readers("scan:targets")` and, on
   takeover, stamps its epoch into the scan row (`SET owner_epoch = :e IF
   owner_epoch < :e`), so a stale scanner cannot move the cursor (its
   already-durable work stays valid). The cursor therefore never
   names a partially processed partition; no page can end inside one.
4. *Resume*: from the persisted `last_token` with `>=` (not `>`): the last
   completed partition — and any other partition with the same token
   (Murmur3 collisions) — is processed again; this is harmless because every
   step is an idempotent upsert of desired state derived from the current
   authoritative rows. Keys at that token whose `target_id` equals
   `last_target_id` may be skipped as an optimisation only.
5. *Completion*: a partition is complete at step 3; a page is complete when
   all its keys are; the cycle is complete when a keys page returns fewer
   than 100 keys (end of the ring): then `cycle += 1`, `cycle_started_at =
   now`, cursor reset to −2⁶³. If a page's last token equals `:t` (≥ 100
   partitions sharing one token — not expected), the scanner stops with the
   visible error `scan_stalled` instead of skipping.
6. *Failure mid-page or mid-partition*: the cursor still names the last
   completed partition; the restart re-processes from there (duplicates,
   never gaps).

**Why no existing row can be skipped.** Under the §6.1 C assumptions plus
(a) rows of `target_representations` are never deleted, (b) the token of a
partition key never changes, and (c) a token-ordered walk from −2⁶³ visits
every partition whose token is ≥ the cursor: every partition present when a
cycle starts has a token ≥ −2⁶³ and is visited by that cycle, in full,
because the cursor only passes partitions processed in full. A row inserted
(or a `targets.state` changed) after its partition was visited is seen in the
*next* cycle, whose start precedes nothing it could miss. Hence every row
existing at the start of cycle *c* is processed during cycle *c*, and every
change is observed within at most two cycles of the moment it is written.
Assumption (c) is standard Cassandra/Scylla token-range behaviour for vnode
keyspaces; it is verified by test H-6/H-9 against Scylla 6.2.3, not assumed
beyond that.

The `reps` scan has no such hazard: its cursor is a clustering key
(`content_id`) inside one partition `(spec_digest, bucket)`, so `content_id
> ?` resumes exactly after the last processed row.

Per row the scan computes the desired state (index entry, match tasks,
target-set membership) and upserts it idempotently. Rows inserted behind a
cursor are reached in the next cycle; rows are never deleted by P9
(representations become RETIRED; targets RETIRED), so retirements are seen
as state changes. Rate limits: `reconcile.scan_rows_per_s` (default 200 on
the dev host) and an interval between cycles (`reps`: 1 h; `targets`: 60 s).
Progress metrics: `p10_scan_cycle`, `p10_scan_rows_total`,
`p10_scan_cycle_seconds`. One scanner per scan (same lease pattern as the
reader, rows `p10_readers("scan:reps")`, `("scan:targets")`; both scans
stamp `owner_epoch` into their `p10_scans` rows as described above).

**C. Eventual catch-up guarantee.** Assumptions: (1) the authoritative rows
remain readable; (2) the backstop scanner runs and completes cycles; (3)
Scylla, MinIO and Qdrant are reachable often enough for retries to succeed;
(4) retries are never abandoned silently — after `max_attempts` an item
enters a visible `BLOCKED`/`FAILED` state with its error. Under these
assumptions, **every READY representation and every READY or RETIRED target
binding of an indexed spec is reflected, by the end of the first backstop
cycle that starts after it became durable (C-8 argument above), in `index_entries` desired state and in
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
2. **Search targets**: every media segment (media segments are never
   classified, §7.3) queries the target collection of the same spec — the
   generation pinned by the search step (§11.1) — with adaptive k (§7.1);
   a **hit** on a target segment with a *usable* low-information flag does
   not vote, the segment's other hits still do (corrected C-14: the earlier
   rule silenced the whole query segment when its *best* hit was flagged,
   which can drop a true, lower-scoring hit on an unflagged segment of
   another target).
3. **Candidates**: hits ≥ floor grouped by target version; candidate iff ≥
   `min_hits` distinct query segments; ranked by (distinct hits, best score);
   capped at `cap_A` (§7.5).
4. **Verify** each candidate: load the target representation's artifact
   (verified SHA-256), run `temporal/v2` (§8).
5. **Commit** matches (§9) and finish the run with the completion LWT `IF
   epoch = :e AND run = :r` (§11.1): INCOMPLETE if any bound of §7.5 was hit
   (`candidate_cap`, `saturated_segments`, `verification_failed`), otherwise
   DONE. DONE asserts that this
   run's search and verification finished on the generation it observed; it
   does **not** assert index integrity — pairs hidden by a later-detected
   point loss are re-covered by the lost subject's own rerun (§12.10).

### 6.3 Direction B — new target against historical media

1. **Index the target**: upsert target segments into the target collection;
   raise the target-set revision and request the low-information refresh
   (§7.3); the new target is searchable at once (a flag that is not usable
   counts as informative).
2. **Search media**: every target segment without a *usable*
   low-information flag (§7.3) queries the media collection (the generation
   pinned by the search step, §11.1) with adaptive k (k₀ → k_max, §7.1);
   saturation is recorded per segment.
3. **Candidates**: grouped by `content_id`, ≥ `min_hits`, ranked, written to
   `match_candidates` (≤ `cap_B_total`); the target's own reference content is
   excluded.
4. **Verify in pages** of `page_B` candidates; the task's cursor survives
   restarts; each page is one lease-bounded unit of work.
5. **Commit** each page's matches; the task ends DONE, or INCOMPLETE with
   reason `candidate_cap` / `saturated_segments` / `verification_failed`
   when a bound was hit (§7.5) — the same rule as direction A.

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
(§12.3), so a promotion cannot lose a subject indexed during a build. The
argument also needs the subject's points to *stay*: no stale index operation
can remove the points of a subject's current intent (§12.9 S2). Points lost
by any other cause (storage fault, operator action, defect) are a detected
and repaired condition: a durable recovery obligation, repair, then a rerun
of the lost subject's own task started after the repair (§12.10) — an
eventual, not a strict, guarantee. The rerun re-establishes this section's
argument for every pair the loss could have hidden: S's own tasks search
again, and every X that searched while S was missing had already been
indexed (X indexes before it searches), so S's rerun can retrieve X.

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
neighbours above the floor exist than were retrieved. Stopping condition:
the k-th score < floor (complete for that segment) or k = `k_max`
(saturated); queries per segment ≤ log₂(k_max/k₀) + 1 (4–5), hits per
segment ≤ k_max. Saturation is counted per task and, if any segment
saturates, the run ends **INCOMPLETE `saturated_segments = n`** in every
direction (corrected C-14: §7.5 previously only recorded it, contradicting
§6.3 and D6; a saturated segment may hide a true candidate, so the run
cannot claim completeness) (audit F-6, F-7: fixed k loses recall silently as
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

**Rule.** A **target** segment is low-information if it has cosine ≥ 0.95
with a segment of a *different* target work in the **authoritative** target
set (targets are distinct works by definition, so
such a vector does not identify a work). Media segments are **never
classified or flagged**: whether a media segment is uninformative is decided
at query time from the target flags it hits (below). This removes the whole
class of stale media classifications (the first proposal stored a media flag
that a later target registration or retirement could silently invalidate).

**Four different "revisions" — not one (corrected C-9).**

| State | Where | Trusted for exclusion? |
|---|---|---|
| authoritative target set | P9 rows: live target bindings (§4.2 predicate) | the only truth; may change at any moment, and P10 learns of it only through the targets scan (target events are held, §6.1) |
| last observed set | `target_set_rev(spec)`: `rev`, `members_digest` | **no** — it lags the authoritative set by up to two scan cycles |
| classification of one target | `lowinfo_class(spec, trep)`: `rev`, `status`, bitmap, witnesses | only through the per-flag witness check below |
| payload copy | target point `lowinfo`, `lowinfo_rev` | **no** — a hint that a Scylla read is worth doing |

`target_set_rev.rev` is a counter incremented by LWT (`IF rev = :old`) when
P10 *observes* a change of members digest (sorted ids of READY, non-retired
target representations) — in the targets scan and at `target_vs_media` task
start — so duplicate observations do not bump twice. It orders refresh work
and fences classification writes; it does **not** certify that the
classification matches the authoritative set, because a target can become
READY, be replaced or be RETIRED in P9 before P10 observes it. A task cannot
learn the authoritative set cheaply, so freshness is established per flag,
not per revision.

**Monotonicity (why additions are safe and removals are not).** The rule is
existential: segment *s* of target *T* is low-information iff **some** live
target of a different work has a segment with cosine ≥ 0.95 to *s*. Adding
targets can only add flags; removing targets can only remove flags. A
missing flag costs work, never recall; a flag that survives the removal of
its last justification can suppress evidence. Hence:
- an *unobserved addition* (or replacement's new version) ⇒ some flags are
  missing ⇒ safe (more segments eligible) until the refresh adds them;
- an *unobserved removal* (retirement, purge, replaced old version) ⇒ a flag
  may be unjustified ⇒ must not exclude. This is closed by the witness check.

**Classification records.** `lowinfo_class(spec, trep)` holds `rev`,
`rule`, `status`, the per-segment bitmap and, for every flagged segment, up
to 4 **witnesses**: target representation ids of *other* `target_id`s that
have a segment with cosine ≥ 0.95 to it, preferring distinct `target_id`s. A
flag is written only together with ≥ 1 witness whose cosine ≥ 0.95 was
**recomputed on the SHA-256-verified artifact vectors** of both target
representations (corrected C-13: the Qdrant score only nominates a witness;
a corrupted or stale vector in the index can therefore not create a flag). Writes use `… IF rev < :r` (or `IF NOT EXISTS`) for
a full classification of *T*, and `IF rev = :read` read-modify-write for an
incremental bit/witness change, so a worker computing for an older revision
can never overwrite a newer record. `status = PARTIAL` (a classification of
*T* that did not finish all of *T*'s segments) never excludes anything.

**Usable flag — the only condition under which evidence is suppressed.** A
segment *s* of target *T* is excluded (does not vote in direction A, does not
query in direction B, does not count in verification) only if **all** hold:
1. `lowinfo.enabled` (set only after validation §3.4 passes);
2. `lowinfo_class(spec, T)` exists with the configured `rule` and `status =
   COMPLETE`, and bit *s* is set;
3. **at least one witness of *s* is authoritatively live at check time**:
   point reads at QUORUM of `target_representations(tid, ver, spec)` and
   `targets(tid, ver)` satisfying the §4.2 liveness predicate — the same rows
   the targets scan reads; results cached for the duration of one task.
   A witness's vectors cannot have changed: a target representation id is
   bound once (P9 `bind_ready … IF state = 'PENDING'`) to an immutable
   artifact.
Otherwise *s* is **eligible**. Missing record, PARTIAL record, unknown rule,
no live witness, Scylla read failure (⇒ the task retries; it never excludes
on an unknown) — all ⇒ eligible. The payload `lowinfo` only decides whether
the check is worth doing: payload `false` ⇒ eligible without a read; payload
`true` ⇒ check 1–3. A stale payload therefore can only reduce exclusion. Each
task records the `target_set_rev` it read and the count of excluded segments
per target (provenance), not a claim of freshness.

*Why this is sound under unobserved changes:* condition 3 re-establishes the
existential rule against the authoritative rows at decision time, using a
witness pair whose similarity was measured on immutable vectors; target
additions cannot invalidate it (monotonicity). A witness retired *after* the
check is a change after the decision. When a refresh later clears a flag of
*T* (its last witness died), it enqueues a `target_vs_media` re-run of *T*
(§6.3), which re-covers every media pair whose evidence that flag could have
suppressed.

**Which entries a change affects, and complete enumeration.**
- *Removal of X* (observed): affected = every record whose witness lists
  contain X, found **exactly** from Scylla by reading the spec's
  `lowinfo_class` partition (one row per target representation; bounded by
  the target count) — no ANN involved. For each: drop X from the witness
  lists; a segment left with no live witness is recomputed by query (next
  item) and cleared if none is found.
- *Addition of X* (observed): X's own classification queries each of X's
  segments; the other targets that gain a flag are the owners of the hits.
- *Query used*: Qdrant search on the target collection of the active
  generation with `params.exact = true` (no HNSW), `params.quantization.ignore
  = true` (float32 originals, not int8 — so no quantization error can create
  a flag), `score_threshold = 0.95`, filter `tid ≠ X's target_id`, `limit =
  L` (256). Hits are candidates for witnesses; each is confirmed on the
  artifact vectors before it is recorded (above). The search covers the *target* collection only — bounded by the
  target-role size (base: ≈ 1,440 query segments × 0.6 M target vectors),
  never media; it is classification, not matching, and is never a fallback
  for candidate generation. There is no brute-force fallback anywhere in
  this path.
- *Completeness rule*: a query returning fewer than `L` hits returned every
  point at or above the threshold in the queried generation. A query
  returning exactly `L` is **truncated**: for X's own segment it still proves
  the flag (≥ 1 witness exists), but the set of *other* targets that gain a
  flag is incomplete — the step is recorded `enumeration = TRUNCATED`,
  `done_rev` is **not** advanced for it, and `full_requested` is set (the full
  recomputation below establishes completeness). Truncation can only leave
  flags missing (safe).
- *Coverage of the queried generation*: points missing from it (pending
  mutations) also only leave flags missing; stale points of dead targets can
  only propose witnesses that fail condition 3. The Qdrant parameters are
  documented `SearchParams`; their behaviour on 1.15.1 is verified by L-14,
  not assumed.
- *Interaction with index recovery (§12.10)*: a target whose points are lost
  from the queried generation can only fail to be found as a witness (flags
  missing — safe) or make a recomputation clear a flag (safe; it enqueues the
  `target_vs_media` re-run of the flag's owner as above). The repair step of a
  target-role obligation enqueues the repaired target's classification step
  (as for an observed addition), so flags the loss left missing are
  restored; exclusion safety never depends on index health.

The target points' payload (`lowinfo`, `lowinfo_rev`) is a derived copy
applied through the index mutation protocol (§12.2; `desired_ver` includes
`lowinfo_rev`).

**Refresh work.** `lowinfo_refresh(spec)` stores `requested_rev`; a leased
worker (owner/epoch) processes revision steps in order, writes records,
enqueues payload mutations (§12.2) and sets `done_rev = r` (`IF epoch =
:mine AND done_rev = r − 1`) only when the step's enumeration was complete.
A crash leaves `done_rev < requested_rev`; the next owner resumes the step
(all writes idempotent). Target-set changes during a refresh only raise
`requested_rev`; the worker loops to the latest. `done_rev` is a *progress*
marker for operations and metrics; exclusion safety never depends on it
(conditions 1–3). Exclusions are therefore never globally disabled by a lag:
a flag is usable whenever its own witness check passes, and unusable
otherwise.

**Full recomputation (backstop).** Weekly, and whenever `full_requested`:
recompute every target's classification from scratch over the target role
with the same exact query (paged per target; any truncated query marks that
target PARTIAL), compare with the stored records, repair, and report
`lowinfo_backstop_diff_total{kind = missing_flag | extra_flag | missing_record
| witness_dead}`. `extra_flag` (a stored flag the recomputation does not
reproduce) is a defect alarm: the witness check proves a witness is *live*,
not that the stored similarity is right, so a corrupted record is outside
the safety argument and is caught here (validation L-7). On completion, `done_rev` = the revision the
recomputation started from.

**Failure.** A refresh failing `max_attempts` times becomes BLOCKED (visible,
`last_error`); records keep their old content and are used only through
conditions 1–3. If the Scylla reads of condition 3 fail, the task retries
(`WAITING`), it does not exclude.

**Metrics.** `lowinfo_targets{state=complete|partial|missing}`,
`lowinfo_refresh_pending_revs`, `lowinfo_refresh_lag_seconds`
(now − time of the oldest unprocessed revision), `lowinfo_refresh_failures_total`,
`lowinfo_enumeration_truncated_total`, `lowinfo_witness_checks_total{result =
live | dead | error}`, `lowinfo_backstop_diff_total{kind}`,
`match_lowinfo_excluded_total{kind}` (segments that did not vote/query/count).
Offline only: the number of labelled true matches that the filter would have
lost (validation §3.4).

### 7.4 Deduplication

Hits are deduplicated per (query segment, subject) — this also collapses
the short-lived duplicate points of two `seq` values of one subject (§12.2) —
candidates per subject
(target version or content). Media with the same `ContentId` are one subject
by construction (P8 level 3). Different `ContentId`s already linked as
level-5 equivalent are **still verified separately** (equivalence is not used
to skip verification in P10; it may become an optimization later only with
its own decision).

### 7.5 Bounds and what happens at a bound

| Bound | Effect when reached | Recorded as |
|---|---|---|
| `k_max` per segment | further neighbours not retrieved | task INCOMPLETE `saturated_segments` (count), metric |
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
| single dominant offset | up to `n_offsets` (3) offset modes taken from the **dense similarity of the two artifacts** (histogram of the per-media-segment best-match offsets, deterministic tie-break), each verified, all runs ≥ `L` kept | repeated scenes, intros, several clips of one source |
| 0.90 segment and mean thresholds | `s_seg`, `s_mean`, `L` fitted on the training split | audit §7.3 |
| all segments count | target segments with a *usable* low-information flag (§7.3) neither count nor break a run; all other segments count | audit F-2 |
| `score` = mean similarity | calibrated `confidence` (§9.1) | contract semantics |
| one run reported | every run ≥ `L` reported as an `AlignedSegment` (max 32) | evidence |

Deterministic: same inputs, same parameter set ⇒ same output, on any host
(float32 matrix products are computed in float64 as in V1 to avoid
order-dependent rounding; ties broken by indices). **The inputs are exactly
the two artifacts, the parameter set and the exclusion set** — no ANN hit,
score, rank, generation or direction enters the decision (corrected C-14:
the first proposal took the offset modes from ANN hit votes, so the decision
for one `match_id` could differ between direction A and B, between
generations and between exact and ANN search, contradicting the
drop/rebuild criterion of validation §5 and "ANN only proposes"). The
exclusion set (target segments with a usable flag at verification time) is
recorded in provenance as a digest and count. Segment duration and timebase
come from the artifact's `pts_s`; different frame rates and VFR enter only
through `pts_s`; reordered segments break a run (runs are monotone in
both time axes); duplicate and repeated scenes produce several offset
modes, each verified separately. Drift (VFR/speed change,
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
  audited `match publish-held` (per row: `SET confidence, calibration_id IF
  calibration_id = null`, then the envelope LWT of §9.3 step 3; repeating
  it is a no-op). Calibration that becomes `STALE` stops new publications
  of that technique version; already-published events are not retracted
  (the contract has no retraction) and keep their `calibration_id` for
  audit. Only `confirmed` is published by default;
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
  revision and `lowinfo` rule, the exclusion-set digest and count (§8),
  task run number and `rerun_reason` (and the occurrence id for an
  obligation-driven run), `calibration_id` (or null), ANN parameters
  (k₀, k_max, floor, min_hits, caps, ef, quantization/rescore), similarity
  metric, query and candidate counts, rank of this candidate, saturation and
  truncation counts of the task, `complete` flag, verification duration,
  matcher instance, timestamps.

### 9.3 Persistence and publication (P10-D11)

1. Write intent `match_commit(match_id, …)` (`match_intents`).
2. `INSERT … IF NOT EXISTS` into `matches` (LWT) with the key of §9.3a,
   then `matches_by_target` and `matches_by_id`. **The first committed
   decision for a `match_id` is the decision** (immutable; corrected C-14).
   A later verification of the same pair under the same technique version
   is normally skipped (a candidate whose `match_id` already has a row is
   counted `already_decided`, not re-verified — this also deduplicates the
   A and B directions and reruns). If one does run (e.g. a race between A
   and B), its insert is a no-op; when its result differs, the difference is
   recorded (`match_decision_divergence_total`, audit entry with both input
   digests), never overwritten. A difference is expected only from a
   different exclusion set (low-information flags whose witnesses changed,
   §7.3) — the artifacts and parameters are immutable — so it is a
   provenance fact, not an invariant violation; any other cause is an alarm.
   (The earlier text called every difference an invariant violation, which
   flagged legitimate lowinfo-driven differences.)
3. If the technique version is `CALIBRATED` (§9.1): build the `match.found`
   1.0 envelope (producer `fingerprinter/matcher`, idempotency key
   `match.match_id`), store it and its `event_id` on the row by LWT `IF
   event_id = null` (a retry or a second publisher reads the stored envelope
   instead of building a new one, so one `match_id` never gets two event
   ids), publish through the P9 publisher
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
The only automatic reruns are the obligation-driven runs of §12.10 (a
detected index loss); they use the currently registered technique version
and record `rerun_reason = index_repair`.

## 10. Level-5 media equivalence (P10-D13)

- **Path:** `media_vs_media` tasks query the media collection of the same spec
  with adaptive k (§7), exclude the subject's own content, group by content,
  verify with `temporal/v2` and then apply the **equivalence rule**
  `equiv/v1`: one run covering ≥ 90 % of **all** segments of *both* items
  (no low-information exclusion applies between media: only target segments
  are ever classified, §7.3 — corrected C-14, the earlier "informative
  segments" was undefined for media), absolute duration difference ≤
  max(5 s, 2 %), and
  |offset| ≤ 10 s (container/packaging start differences). Partial overlap
  (a clip of a film) is a match-like relation, not equivalence, and is not
  recorded in P10.
- **Persistence:** `equivalence_links` rows in both orientations with state
  `LINKED`, confidence (calibrated on the equivalence set), technique version,
  evidence (alignment, coverage), provenance, `decided_at`, via an intent.
- **Reversible:** `equivalence revoke <a> <b> --reason …` sets `REVOKED` in
  both orientations through an intent (two partitions, resolved by the P10
  reconciler), with an audit entry; nothing is deleted; a rerun under the
  same technique version cannot re-link a revoked pair (`IF NOT EXISTS`
  keeps the REVOKED row); a later technique version creates its own rows.
  ANN similarity and metadata never create a link: only `equiv/v1` on the
  two artifacts does. **No `ContentId`, representation, media or observation is ever
  merged** (level 3 stays byte identity; P8 invariant).
- **Visibility to crawler2:** no contract event exists. P10 exposes the
  relation through the CLI and the table; a `media.equivalence_linked` event
  is a separate contract decision (P10-D13 part b, deferred).
- Equivalence can be disabled (`matching.equivalence_enabled`) without
  affecting A/B matching.

## 11. Concurrency, leases, idempotency, failures

### 11.1 Task state machine (corrected C-13)

A task row (`match_tasks`, one per subject × spec × kind) executes numbered
**runs**. States of the current run:

`PENDING → INDEXED → SEARCHED → VERIFYING(page) → DONE | INCOMPLETE`;
`→ WAITING_INDEX` when the spec/role has no ACTIVE generation, Qdrant is
unhealthy, or the subject is capacity-blocked (retried with backoff, never
failed); `→ FAILED` only for non-retryable causes of the **subject itself**
(its artifact SHA-256 mismatch, invalid vector, missing representation row),
each with a typed reason; candidate failures never fail the task (below).

| Class | States | Meaning |
|---|---|---|
| in progress | PENDING, INDEXED, SEARCHED, VERIFYING | owned through a lease; resumable at each boundary |
| waiting / retryable | WAITING_INDEX (reason `unavailable`, `missing`, `capacity`); also any state with `next_attempt_at` set after a retryable error (storage unavailable, unknown LWT, witness read failure) | retried with backoff; never silently dropped; visible in `match status` |
| run-terminal, re-openable | DONE, INCOMPLETE (with `incomplete_reason`) | this run finished; the row can be re-opened only by a **run bump** (an obligation, §12.10, or an audited `match rerun`/`retry`) |
| failed | FAILED | non-retryable; operator alert; an obligation reaching a FAILED task closes as `TASK_FAILED` — an **unresolved** close that keeps the spec's negatives provisional (§12.10), never silently |

*Corrected:* C-12 added "the generation the task searched is `suspect` ⇒
WAITING_INDEX(suspect); the task may not finish DONE while it is". That rule
read `index_generations` and then wrote `match_tasks` — two partitions, no
common Paxos instance — so a task paused between the read and its DONE LWT
finished DONE after the generation was marked. It is **removed**: no task
state depends on `suspect_since`. Correctness after an index loss comes from
the recovery protocol of §12.10, whose only atomic step on a task is a
single-row LWT.

**Run fencing.** Every transition of a run is an LWT `IF epoch = :e AND run =
:r` on the task row. The first claim of run *r* sets `run_started_at` (`IF run
= :r AND run_started_at = null`). The completion LWT is `SET state = 'DONE' |
'INCOMPLETE', done_run = :r, done_state = <the same> [, done_ok_run = :r
when DONE] IF epoch = :e AND run = :r`. A **run bump**
(`SET run = :r+1, state = 'PENDING', run_started_at = null,
recovery_runs[:occurrence_id] = :r+1, rerun_reason = :why, next_attempt_at =
:now IF run = :r AND state IN (<every non-FAILED state>)`; an audited `match
rerun`/`retry` uses the same statement without the map entry, and a retry of
a FAILED task conditions on `state = 'FAILED'` instead) invalidates every in-flight
transition of run *r* (a run not yet claimed is instead tagged by a
coalescing LWT, §12.10): a worker of run *r* that resumes fails its next LWT on
`run`, re-reads, and stops. Bump and completion are LWTs on the **same row**,
so Scylla's Paxos orders them: either run *r* completed first (and the bump
re-opens the row) or the bump came first (and run *r* can no longer
complete). A new run never reuses an earlier run's candidate list
(`match_candidates` is keyed by `run`); matches an earlier run committed
remain valid (they are artifact-verified, §12.9 C).

**Search step (corrected C-14).** The search step of a run resolves the
active generation once, records it with the `SEARCHED` transition as
`searched_generation`, and sends every query of the step (all query
segments, every adaptive-k round) to that one collection. The step has a
deadline `step_start + search_timeout_s` on the matcher's clock, where
`step_start` is no earlier than the time the pointer (cached ≤
`index.active_ttl_s`) was read; no query is sent after the deadline — the
step is abandoned and repeated with a fresh pointer. This is what bounds the
DRAINING search horizon of §12.4 (`search_timeout_s` bounds the whole step,
not one query). `SEARCHED` is written only after **every** query segment's
search returned: a failed, timed-out or "not found" query fails the step
(retry / `WAITING_INDEX`), and is never counted as zero hits. A repeated
search step writes a new candidate list for the new
`(searched_generation, run)`; verification reads the list named by the task
row. The low-information classification queries of §7.3 obey the same
pinning and deadline.

**Candidate failures do not fail the task.** A candidate whose artifact is
missing, unreadable or fails its SHA-256 check is marked FAILED
(`artifact_unavailable` / `artifact_corrupt`, the latter with an operator
alert); the run ends INCOMPLETE `verification_failed`. The task itself goes
FAILED only when the **subject's own** row or artifact is invalid
(corrected C-14: the earlier text let one corrupt candidate artifact fail the
subject's task, which would also close the subject's recovery obligations as
`TASK_FAILED`).

A lost lease returns the task to the due index (P9 §7 pattern). Re-indexing
is idempotent (deterministic ids), the candidate list is durable per run,
verification pages are idempotent (`match_id`). Unknown LWT outcome on any
transition: SERIAL read of the row; the worker continues only if the row
shows its own `epoch` and `run` with the intended state.

### 11.2 Failure semantics

| Failure | Detection | Behaviour |
|---|---|---|
| Qdrant unreachable / timeout | `IndexUnavailableError` | task → `WAITING_INDEX`, backoff 30 s → 15 min; metric `fingerprinter_index_unavailable_total`; health endpoint reports degraded; no fallback |
| no ACTIVE generation / collection missing (index lost) | `IndexMissingError` | as above; operator runs `index rebuild` (§12) |
| Qdrant collection RED / point-count mismatch / suspected loss of unknown extent | `index check`, health, operator | per-subject **audit** of the generation requested (§12.10); every short subject gets a recovery obligation; queries continue (an unreadable collection fails searches loudly — `IndexUnavailableError`, never an empty result); if the audit cannot complete, the conservative role-wide obligation of §12.10 applies |
| point loss of a subject detected (verification count, apply pre-count, audit) | §12.10 | obligation durable before any repair; repair; rerun of the subject's own tasks after the repair; tasks of other subjects are not gated |
| stale entry (point whose representation is retired/absent) | candidate check against P9 rows before verification (§12.9 C) | dropped from the candidate list, counted `index_stale`, repaired by deleting the subject's points; check unreadable ⇒ task retries, never accepts |
| missing entry (READY representation without points) | `index check` / `index_entries` | re-indexed; counted `index_miss` |
| Scylla unavailable | `StorageUnavailableError`, `LwtUnknownError` | retry with backoff; LWT unknown resolved by SERIAL read (P9 pattern) |
| MinIO unavailable / artifact missing | typed error from `Objects` | candidate retryable (×3) → `verification_failed` (task INCOMPLETE); a **candidate's** artifact SHA-256 mismatch → that candidate FAILED `artifact_corrupt`, task INCOMPLETE `verification_failed`, operator alert; the **subject's own** artifact mismatch → task FAILED + operator alert (authoritative data corrupt) (§11.1) |
| verification timeout | per-candidate wall-clock limit (default 30 s) | candidate retry ×3 → `verification_failed` (task INCOMPLETE) |
| matcher crash | lease expiry | task resumes at its last durable state |
| duplicate hints / events / tasks | `IF NOT EXISTS`, deterministic ids | no-ops |
| concurrent indexers | idempotent upserts | converge |
| overload | bounded `matcher.workers`, due-index backpressure | latency rises; nothing dropped |
| index capacity reached | capacity ledger (§13.3) | subject `BLOCKED: capacity`, task `WAITING_INDEX(capacity)`, metric + alert; rebuild `CAPACITY_BLOCKED` before creating anything; never an OOM-risking write |

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
   live, §4.2), else `DELETE`. If it differs from the row,
   write it with `UPDATE … SET desired_op, desired_ver, seq = :s+1, state =
   'PENDING' IF seq = :s` (or `IF NOT EXISTS`). Nothing touches Qdrant before
   this LWT succeeds; an unknown LWT outcome (`LwtUnknownError`) is resolved
   by a SERIAL read first, and while it is unresolved Qdrant is not touched
   (Invariant A, §12.9). Equal desired state ⇒ no write (duplicate events are
   no-ops).
2. **Apply.** A worker takes the subject's Redis lease (avoidance only),
   reads the row (`seq = s`), re-reads the authoritative row and, if the
   desired state is no longer current, goes back to step 1. It reads the
   routing set = generations in states `CREATED … DRAINING` (§12.4) with
   `routing_epoch` — a fresh QUORUM read for every apply, never a cached
   copy — then, for an UPSERT whose `seq` is not yet applied to a routed
   generation, reserves its points there in the capacity ledger (§13.3;
   refused ⇒ `BLOCKED: capacity`, no Qdrant write). **Pre-count (C-13):** for
   each routed generation in a *searched* state (ACTIVE, DRAINING) whose
   `applied[g]` already holds a sequence `p`, the worker first counts `subject
   = S AND seq = p` in g; a count below `segment_count` is a detected loss
   and the recovery obligation of §12.10 is made durable **before** the apply
   proceeds, so a later apply can never silently heal a gap that tasks may
   have searched. (After a crash between cleanup and bookkeeping the `p`
   points may be gone legitimately; that case is recorded conservatively —
   an extra obligation costs one rerun, never a missed pair.) Then it applies
   to each (corrected C-12):
   - **UPSERT at `u`**: (a) upsert all points of the subject with ids of
     `u` and payload `seq = u`, `wait=true`; then (b) **cleanup**: delete by
     filter `subject = S AND seq < u`, `wait=true`. Order (a) before (b), so
     the subject is never without a complete point set while its intent is
     UPSERT.
   - **DELETE at `d`**: delete by filter `subject = S AND seq ≤ d`,
     `wait=true`.
   (`subject` = payload `rep` for media, `trep` for targets; `seq` is a
   `range` condition on the integer payload index.) An operation derived
   from intent *x* can therefore touch only points with `seq ≤ x` — never
   points of a newer intent. All three steps are idempotent, so an
   **uncertain outcome** (timeout, connection loss, HTTP
   5xx/503) is handled by repeating the operation; there is no need to know
   whether the first attempt landed. A **permanent** failure (HTTP 4xx other
   than 404/409/429: wrong dimension, invalid payload) is not retried: the row
   becomes `BLOCKED` with `last_error`. "Collection not found" is a no-op only
   if Scylla says the generation is RETIRED or FAILED; otherwise it is an
   invariant violation (generation audit requested, §12.10; alert).
3. **Bookkeeping.** `UPDATE index_entries SET applied[g] = :s IF seq = :s`
   for each generation applied (map-entry updates of different generations
   on one row are serialized by the LWT and never overwrite each other);
   `state = 'APPLIED'` is set in the same LWT only when `applied` then holds
   `:s` for **every** generation of the routing set the worker read —
   otherwise the row stays PENDING (corrected C-14: the earlier text set
   APPLIED after any one generation, while R-4 expected PENDING). `state` is
   a summary for operators; every correctness check (catch-up, completeness,
   detection, capacity audit) reads `applied[g]` per generation, never
   `state`. If the LWT fails
   because `seq` advanced, a newer intent exists: the worker re-reads and
   re-applies the newer desired state itself (so the last writer to finish
   leaves Qdrant in the newest state). After `max_attempts` failures the row
   becomes `BLOCKED` with `last_error` (visible, retried by the reconciler).
4. **Completion.** A mutation is complete when `applied[g] = seq` for every
   generation that was in the routing set at the time of its routing epoch.

**What cannot be fenced, and how it is handled.** Qdrant has no conditional
writes, a Redis lease does not fence a write already sent, and a failing
Scylla LWT afterwards cannot undo a Qdrant operation that has landed. A
*stale* worker (lease lost, paused) can therefore land an operation of an
older intent at any time. Both directions, with the C-12 protocol:
- **older UPSERT after a newer DELETE or UPSERT**: it adds points of the
  older `seq` (distinct ids); it overwrites nothing of the newer intent.
  Extra points, false-positive side — handled below.
- **older DELETE after a newer UPSERT** (the C-12 race: DELETE 11 read,
  worker paused, UPSERT 12 applied and recorded, DELETE 11 lands): its
  filter `seq ≤ 11` does not match the `seq = 12` points, which survive.
  Without the `seq` bound (first and second proposal: delete by subject
  only) it would have removed them, leaving a live subject missing from the
  index — a false-negative risk that verification could only repair later.
- **older UPSERT 10 then older DELETE 11, both after UPSERT 12**: the
  UPSERT adds `seq = 10` points, the DELETE removes `seq ≤ 11` — the
  `seq = 12` points are untouched (with seq-independent ids the UPSERT 10
  would first have overwritten them, and the DELETE then removed them).
Extra older-`seq` points **cannot produce an accepted
match** (Invariant C, §12.9: candidates are checked against authoritative P9
rows and verified on artifact vectors), and they are **found and removed** by
the Qdrant-side verification of §12.6 (points carry `seq`; a point with
`seq` < the row's `seq`, or present while the row says DELETE, is stale). No
time bound on the stale write itself is claimed (a paused process can resume
at any time); the bound is on detection after it lands: one verification
cycle (`verify_cycle_s`).

| Crash point | State left | Recovery |
|---|---|---|
| before the intent LWT | nothing changed | next producer/backstop writes the intent |
| after intent, before Qdrant | row PENDING | reconciler applies |
| after Qdrant, before bookkeeping | Qdrant done, row PENDING | reconciler re-applies (idempotent), records |
| after bookkeeping | consistent | — |
| during DELETE (partial) | some points left, row PENDING | re-apply delete by filter `seq ≤ d` |
| during UPSERT, after (a), before (b) | new and old `seq` points coexist (duplicates collapse in §7.4), row PENDING | re-apply (a) (idempotent) and (b) |

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
| `CREATED` | row written (number from `next_generation`, never reused), capacity reserved (§13.3), collection created, `routing_epoch` bumped | yes | no |
| `BUILDING` | bulk load from the authoritative scan | yes | no |
| `CATCHING_UP` | lag scan until no subject lags | yes | no |
| `VALIDATING` | completeness and equivalence checks | yes | no |
| `READY` | validated, waiting for promotion | yes | no |
| `ACTIVE` | the generation readers use (exactly one per spec/role) | yes | yes |
| `DRAINING` | previous active, kept for `drain_grace_s` ≥ 2 × `active_ttl_s` + max task lease | yes | by readers with a cached old pointer only |
| `RETIRED` | collection deleted | no | no |
| `FAILED` | abandoned (from any non-active state; the transition bumps `routing_epoch`); collection kept for inspection, then deleted by `index drop --generation`; the next capacity audit then releases its holder row and points (§13.3) | no | no |

Legal transitions: CREATED→BUILDING→CATCHING_UP→VALIDATING→READY→ACTIVE→DRAINING→RETIRED;
VALIDATING→CATCHING_UP (lag found); READY→VALIDATING (re-validation, failed
`detect_count` fence); any of CREATED…READY→FAILED. Every
transition is an LWT on the generation row conditioned on the builder's
`build_epoch`; ACTIVE is entered only through the promotion LWT and left only
through a later promotion (→ DRAINING). There is **no ACTIVE→FAILED**: an
ACTIVE generation whose collection is lost or unreadable stays ACTIVE —
every search of it fails loudly (`IndexMissingError`/`IndexUnavailableError`,
tasks `WAITING_INDEX`, never an empty result) — until a rebuilt generation is
promoted over it (§12.7). **DRAINING → RETIRED** requires the grace period and
either a clean post-horizon verification pass (below) or, if g cannot be
read, the durable unauditable-fallback occurrences of §12.10 for every
UPSERT row with `applied[g]` set (corrected C-14: the earlier text required a
pass that cannot run on a lost collection, leaving no path to RETIRED).

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
4. VALIDATING (§12.5), including a fresh authoritative coverage pass (one
   complete backstop walk of the spec's authoritative rows, §6.1 B, whose
   every READY subject must have an UPSERT row with `applied[g] = seq`).
   Any lag found → back to CATCHING_UP.
5. READY.
Every mutator writes its intent row *before* it reads the routing set
(§12.2), so a mutator that missed g has a row that existed before the
catch-up pass began; every subject has a row (the bulk load writes them
too); every row is either applied to g by its own mutator or by the pass.
Hence no subject can be missing from g at promotion through the mutation
protocol — stale operations included, since they cannot remove current
points (§12.9 S2); a non-protocol loss is handled by §12.10 (the `boundary_at` wait
is an extra margin, not required for this argument); tombstones are rows with `desired_op = DELETE`
and are checked the same way.

**Promotion.** One single-partition LWT batch on `index_generations(spec,
role)`: `SET active_generation = g, active_epoch = e+1, state(g) = ACTIVE,
state(old) = DRAINING IF active_epoch = e AND state(g) = 'READY'`. Readers
see either the old or the new pointer. Crash before: g stays READY and
continues to receive live writes; the lease holder (or the next one)
re-validates if READY is older than `revalidate_after_s`, then promotes.
The batch also carries `AND detect_count(g) = :v`, where `:v` is the
`validated_detect_count` that the VALIDATING → READY LWT recorded (a CQL
condition compares a column with a value). The verification pass also
scans READY generations; a short count there only raises `detect_count`
(no obligation: g is not searched). A loss detected in g after validation
therefore fails the promotion and sends g back to VALIDATING. Detection marks `detect_count` by
LWT in the same partition, so this condition is a real atomic fence between
detection *marking* and promotion; it is an operational guard only —
correctness does not depend on it, because a loss in a promoted generation is
covered by its obligation (§12.10). The promotion LWT records `promoted_at`
and `drain_horizon_at(old) = promoted_at + active_ttl_s + search_timeout_s +
clock_margin_s`, after which no reader can still be searching the old
generation. Crash after: done; the reconciler moves DRAINING → RETIRED after
the grace **and** after one complete verification pass over the DRAINING
generation that **started after `drain_horizon_at`** (a loss that existed
while any reader could search it is still present then — only the pre-counted
apply path or an obligation repairs points — and gets its obligation,
§12.10), and deletes the collection. *(Corrected C-13: the pass previously
only had to start after promotion, so a loss occurring after a subject was
checked but before the last search of the old generation went undetected.)* A stale worker writing to a RETIRED generation
gets "collection not found", checks the generation state in Scylla and
treats it as a no-op; for any other state it is an error.

### 12.5 Validation and rebuild equivalence

A generation is promoted only if all of these hold:

| Check | Source of truth | Pass condition |
|---|---|---|
| completeness | `index_entries` | a full pass completed with every row at `applied[g] = seq` when checked |
| authoritative coverage | P9 rows (`representations_by_spec`/`representations`; `target_representations` × `targets`) | a complete walk started after CATCHING_UP ended found no READY subject without an UPSERT row applied to g, and no live-row/DELETE disagreement |
| manifest | `index_entries` UPSERT rows | Qdrant count per subject with filter `subject = S AND seq = row.seq` = `segment_count`; total = Σ |
| no extras | `index_entries` | Qdrant scroll of g (ids + payload `rep`/`trep`, `seq`): every point belongs to an UPSERT row with `seq` ≤ row `seq`; points with `seq` < row `seq` are removed by cleanup (§12.2 (b)) and the check repeated; a point of a DELETE row, of no row, or with `seq` > row `seq` fails validation (back to CATCHING_UP; a `seq` above the row is an invariant violation, alert) |
| content digest | artifacts | per subject, SHA-256 over (sorted segment indexes, float32 original bytes, canonical payload without `seq`) of the points with `seq = row.seq` equals the digest computed from the artifact (point ids are excluded because they contain `seq`); bucket digests combined order-independently into the generation manifest digest |
| classification | `lowinfo_class` | target payload `lowinfo_rev` = the row's `rev` |
| params | `index_generations` | Qdrant collection config equals the recorded params |
| capacity | `index_capacity`, `index_generations.reserved_points` | Qdrant exact count of g ≤ `reserved_points`; ledger ≥ Σ exact counts of all non-RETIRED collections |
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
| generations | stuck non-terminal generations, expired build leases, DRAINING past grace and past a clean post-horizon pass | resume, FAILED, RETIRED |
| repairs (C-13, C-15) | every `index_repairs` occurrence row not CLOSED with `resolution = RESOLVED` — older occurrences of a subject included (64 buckets per spec/role, cursor persisted); generations with `audit_state = REQUESTED/RUNNING` | drive OPEN → REPAIRED → RERUN_PENDING → CLOSED, re-check unresolved closes for `resolve` (§12.10); run the per-subject audit |
| lowinfo | `done_rev < requested_rev`, `full_requested` | resume refresh / full recomputation |
| capacity audit | ledger vs holder rows, pending UPSERT rows and Qdrant exact counts (§13.3 order a–d) | set `reserved` to the bound by CAS — the **only** path that lowers it (C-14) |

Repair table (Invariant G, §12.9). Every repair is a re-application of the
row's desired state or a delete by deterministic id, so repeating a repair —
including after a reconciler crash mid-pass (passes resume from persisted
cursors; R-21) — converges to the same state.

| Condition | Source of truth | Detection | Repair | Idempotent because | Observable | Test |
|---|---|---|---|---|---|---|
| missing point, subject never applied (authoritative READY, no row or row PENDING for g) | P9 rows → `index_entries` desired UPSERT | authoritative scan finds no row; intents pass | intent (if absent) + apply — a protocol lag, not a loss: the subject's own task has not passed INDEXED (§6.4) | deterministic ids | `index_miss_total` | R-2, R-4 |
| orphan point (no `index_entries` row) | P9 rows | verification scroll | authoritative READY ⇒ write intent + apply; else delete by id | delete/upsert by id | `index_verify_orphans_total` | R-16 |
| stale `seq` (point `seq` < row `seq`) | `index_entries` | verification scroll | if the row's current points are short in a searched generation, that is a loss: obligation first (*loss* row below); then cleanup `seq < row.seq` | ids include `seq`; filter bounded by `seq` | `index_verify_stale_total` | R-14, R-18, R-23 |
| point present for a DELETE row | `index_entries` (DELETE) | verification scroll, count by filter | delete by filter `subject AND seq ≤ d` | delete by filter | `index_verify_stale_total` | R-5, R-14 |
| **loss**: row APPLIED for g (searched state) but current points short (count of `seq = row.seq` < `segment_count`), found by verification, apply pre-count or audit | Qdrant (observed) vs `index_entries` | verification count (re-read row and re-count once to exclude an in-flight new intent; still short or ambiguous ⇒ loss) | **§12.10**: obligation `index_repairs(S, g)` durable before any repair and before the pass cursor passes S; generation marked; re-apply; confirm; run bump of S's own tasks; close after the bumped runs finish | occurrence open `IF NOT EXISTS` / `IF occurrence AND occurrence_id`, fresh `occurrence_id` per occurrence; run bump `IF run`, task map keyed by `occurrence_id`; repair by deterministic ids | `index_repairs{state}`, `index_miss_total`, `index_generation_suspect`, `match_task_reruns_total{reason=index_repair}` | R-17, R-25, R-26, R-27 |
| abandoned intent (PENDING/BLOCKED past grace) | `index_entries` | intents pass | apply (BLOCKED: retry with backoff, stays visible) | as above | `index_entries{state}` | R-2, R-3 |
| mutation not in every routed generation | `index_entries.applied` vs generations in `CREATED…DRAINING` | intents pass and catch-up pass | apply to the missing generation | as above | `index_entries{state=PENDING}` | R-4 |
| incomplete / failed generation | `index_generations` + build lease | generations pass (expired lease, stuck state) | resume under a new lease, or FAILED + `index drop --generation` | state transitions are epoch-fenced LWTs | `index_build_progress`, generation state | R-6, R-13, R-20 |
| generation content ≠ manifest | artifacts | validation digest check (§12.5) or `index check` | generation stays non-active / FAILED; rebuild | new generation number | generation state, alert | R-6, §5 |
| ledger differs from the conservative bound | `index_capacity` holder rows + pending rows + Qdrant counts | capacity audit | set `reserved` to the bound (the only lowering path) | CAS `IF reserved = :r`, bound recomputed from durable state | `index_capacity_reserved_points` | CAP-6, CAP-9 |

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

### 12.9 Guarantees (Invariants A–G) and routing races (corrected C-11)

States and their source of truth:

| State | Source of truth |
|---|---|
| what should be indexed | P9 rows (`representations`, `target_representations` × `targets`) |
| desired index state per subject | `index_entries` (`desired_op`, `desired_ver`, `seq`) — derived from P9 rows, re-derivable |
| what a worker attempted | nothing durable beyond the `seq` it read; never trusted |
| what Qdrant contains | Qdrant (observed by verification and validation scrolls) |
| which generations receive a mutation | `index_generations` states `CREATED…DRAINING`, read fresh per apply |
| what readers search | static `active_generation` (one LWT) |
| what may be accepted as match evidence | P9 rows + SHA-verified artifacts only (Invariant C) |

**A — Durable desired state.** No Qdrant write happens for a subject unless
an `index_entries` row with that desired state and `seq` was read back from
Scylla by the writer (§12.2 steps 1–2). A failed intent LWT ⇒ no Qdrant call;
an unknown outcome ⇒ SERIAL read first. Bulk load and catch-up use the same
rows.

**B — Idempotent recovery.** UPSERT (ids of its `seq`, full payload), its
cleanup (`seq < u`) and DELETE (`seq ≤ d`) are idempotent, so retrying any uncertain
outcome converges to the row's desired state; classification of errors
(retryable vs permanent) is §12.2 step 2. A retry always re-reads the row and
applies the *current* desired state, not the one first attempted: a worker
whose DELETE 11 timed out and whose row now says UPSERT 12 retries with
UPSERT 12, and even if its first DELETE 11 lands late it cannot touch the
`seq = 12` points (S2 below).

**C — Stale writes cannot become accepted evidence.** Qdrant only proposes
candidates. Before a candidate is verified, the matcher checks (cached per
task): (1) the hit's payload `spec` equals the query spec and the point id
equals `uuid5(subject:seq:k)` of its payload — otherwise the point is foreign
(orphan repair); (2) the subject is **authoritatively live** (§4.2
predicate) at QUORUM; (3) `temporal/v2` runs on the
**artifact** vectors (SHA-256 verified), never on Qdrant vectors. No
sequence comparison is required for acceptance: a live subject's vectors are
immutable (its representation id is bound to one artifact), so a stale point
of a live subject proposes the same candidate a fresh one would; the `seq`
comparison (`point.seq < row.seq`) is used to *count and repair* staleness
(`index_verify_stale_candidates_total`, repair enqueued). A dead subject's
point is dropped and its subject queued for delete repair. If the check reads
fail (Scylla unavailable), the candidate is neither accepted nor rejected:
the task retries with backoff and cannot finish DONE. Low-information
payloads are never trusted (§7.3). Hence a stale write can at worst cost
work or delay, never create a match.

**D — Deletions.** A DELETE row is never removed while the spec is indexed
(`index drop` removes rows with their collections); its `seq` stays and
`applied[g]` must reach it for every generation in `CREATED…DRAINING`.
- partial delete / delete in one generation only ⇒ row stays PENDING for the
  missing generations; delete by filter repeated (R-5, R-4);
- stale UPSERT after DELETE ⇒ cannot pass Invariant C (subject dead);
  removed by verification (R-14);
- delayed worker targeting a RETIRED generation ⇒ collection gone,
  generation numbers never reused ⇒ the write fails ("not found") and is a
  no-op because Scylla says RETIRED (R-11);
- rebuild meeting a DELETE row ⇒ applies the DELETE to g (no-op on an empty
  collection, but records `applied[g]`), so tombstones are part of the
  completeness check;
- crash after Qdrant delete, before bookkeeping ⇒ PENDING; re-apply (R-3);
- **stale DELETE after a newer UPSERT** (C-12) ⇒ its `seq ≤ d` filter cannot
  match the newer points; nothing to repair (R-22); also when a stale UPSERT
  of an even older `seq` landed in between (R-23).

**E — Generation completeness at promotion.** "Complete" is defined at the
promotion LWT instant P, with writes continuing, as: (E1) every
`index_entries` row of (spec, role) whose current `seq` was written **before
g was created** has `applied[g] = seq` (established by the catch-up pass and
re-checked in VALIDATING); (E2) every row whose current `seq` was written
**after** g was created is routed to g by its own mutator, because the
mutator reads the routing set after its intent LWT (Invariant A order) —
until applied, that row is PENDING exactly as it is for the active
generation, and its task has not yet passed its INDEXED step; (E3) the
authoritative coverage walk (§12.5) found every READY subject with an UPSERT
row applied to g; (E4) the no-extras scroll found no point outside UPSERT
rows with `seq` ≤ row `seq` after cleanup; (E5) manifest and content digests match the artifacts;
(E6) params and capacity checks pass. A point written by a stale worker after
E4 is not excluded by these checks — it is covered by Invariant C and the
verification cycle, which is why promotion does not claim "no stale point
can ever exist", only "no stale point can be accepted". A stale operation
landing after VALIDATING, while READY, or right after promotion **cannot
invalidate E1–E3**: it can add older-`seq` points but cannot remove the
current points of any subject (S2). Promotion is therefore not invalidated by
stale writes; only a non-protocol loss can make a validated generation
incomplete, and that is handled by H. If READY waits longer
than `revalidate_after_s`, VALIDATING is repeated before promotion.

Four different properties, kept apart (C-14):

| Property | Holds | Kind |
|---|---|---|
| P1 completeness at the promotion instant (E1–E6) | yes, by the catch-up pass, coverage walk and validation checks | protocol, single instant |
| P2 the current points of every live subject stay in g after promotion while writes continue | yes against every protocol and stale operation (S2) | strict, **conditional on R-24** (Qdrant filter semantics) and on Qdrant keeping acknowledged writes |
| P3 stale older-`seq` extras disappear | eventually: within one verification cycle after they land; never accepted meanwhile (C) | eventual |
| P4 every point a search needs is present while the search runs | **not guaranteed**: a non-protocol loss can remove points at any time; handled after detection by §12.10 (invariant R) | not guaranteed — recovery only |

"Complete generation" in this document means P1 (+ P2 under its conditions),
never P4.

**F — Rebuild failure.** The active pointer changes only through the
promotion LWT (`IF active_epoch = e AND state(g) = 'READY'`), so a failed or
abandoned build cannot change it. A failed generation goes FAILED (routing
epoch bumped, no longer written or searched), stays inspectable, and its
collection deletion lets the next capacity audit release its reservation (§13.3). Live mutations are
not lost: their rows are in `index_entries` and were applied to the active
generation independently. A later builder either adopts the non-terminal
generation (resumes from its state and bucket cursor under a new
`build_epoch`) or marks it FAILED and creates `next_generation`; there is
never a second ACTIVE because ACTIVE is entered only by that LWT.

**G — Reconciliation.** The §12.6 repair table, with the source of truth,
detection, repair, idempotency argument, metric and test for each condition.
Convergence statement: if mutations stop and Scylla/Qdrant/MinIO are
available, one completed verification cycle plus one completed intents pass
leave Qdrant equal to the desired state for every routed generation; with
mutations continuing, every divergence that exists at the start of a cycle
is repaired or reported BLOCKED by the end of that cycle.

**H — Index completeness and task completion (corrected C-12; task completion corrected C-13).**

| Guarantee | Kind | Mechanism |
|---|---|---|
| S1 no Qdrant write without a durable, read-back intent | strict | Invariant A |
| S2 once UPSERT *u* is applied to g, the `seq = u` points of S stay in g until an intent newer than *u* is applied | strict (under: Qdrant applies filters as documented and loses no acknowledged data — R-24) | ids contain `seq`; every removal is bounded by `seq ≤ x` with *x* the operation's own intent; a stale operation has *x* < *u* |
| S3 no accepted match from a stale, foreign or dead-subject point | strict | Invariant C |
| S4 one ACTIVE generation; pointer changed only by the promotion LWT | strict | Invariant F |
| S5 a rerun request for S and the completion of S's current run are linearized; a run that may have searched before S's repair cannot discharge S's recovery obligation | strict (single-row Paxos on the task row) | §11.1 run fencing, §12.10 |
| removal of extra older-`seq` points | eventual: one verification cycle after they land | §12.6 |
| detection of a non-protocol loss, repair, and re-search of every pair the loss may have hidden | eventual: durable obligation at detection; detection within one verification cycle of a persisting loss; rerun scheduled by the reconciler | §12.10 (invariant R) |

Answers to the C-12 questions, as corrected by C-13: (1) a stale DELETE
**cannot** remove a newer UPSERT's points; (2) because point ids contain
`seq` and the DELETE filter is `seq ≤ d`; (3) a missing-point state can still
arise from non-protocol causes (storage fault, manual deletion, a defect)
and is detected by the verification count of `seq = row.seq` points, by the
apply pre-count (§12.2) and by the generation audit (§12.10); (4)
**corrected:** a task *can* finish DONE on a generation from which points are
missing — before detection nothing can know, and after detection the C-12
read of `suspect_since` followed by the task LWT was not a fence (§11.1,
§12.10). The design no longer claims otherwise: DONE asserts only that the
run finished on the generation as observed; (5) every detected loss of S
creates a durable obligation, closed only after a run of each of S's own
tasks, numbered after S's repair, has finished (§12.10); index gaps cause
*absent candidates*, never a stored `no_match` verdict (verdicts are computed
on artifacts), so the rerun can always add the match; (6) no — a stale
operation after validation cannot remove current points (E above); (7) the
same right after promotion; a DRAINING generation gets one complete
verification pass that starts after its search horizon before RETIRED
(§12.4); (8) the table above. Temporary false negatives are possible **only**
after a non-protocol loss, from the loss until S's qualifying rerun finishes —
a known limitation (§17).

**Routing races** (all rely on: intent LWT → fresh routing read → apply →
`IF seq` bookkeeping; CREATED bumps `routing_epoch` in the same partition
that holds the generation rows).

| Race | Outcome |
|---|---|
| mutation in flight when g enters CREATED/BUILDING | its intent precedes creation ⇒ E1 ⇒ the catch-up pass applies it to g (R-12) |
| routing read just **before** creation | the routing set lacks g; the row's `seq` predates creation ⇒ E1 (R-12) |
| routing read just **after** creation | g included; mutator applies it (E2) |
| g enters CATCHING_UP | the pass checks every row; rows changing during the pass are covered by E2 or re-checked (R-7) |
| mutation changes while catch-up is processing its subject | the pass's `IF seq = s` fails ⇒ it re-reads and applies the newer state, or leaves it to the newer mutator (E2); a pass that crashes after a stale write is covered by C and the no-extras check (R-7) |
| g READY while live writes continue | g is routed in READY (E2); promotion re-validates if READY is old |
| promotion while workers cache the old pointer | readers may search DRAINING for ≤ `active_ttl_s`; DRAINING is still routed for writes; grace ≥ 2 × TTL + max task lease (R-10) |
| stale worker writes to DRAINING | applied normally (still routed) |
| stale worker writes to RETIRED | "not found" ⇒ no-op, generation number never reused (R-11) |

**Assumptions requiring implementation-level verification:** Qdrant applies
an acknowledged `wait=true` upsert/delete before answering later searches on
the same collection (single node; §6.4); `exact = true` and
`quantization.ignore = true` behave as documented on 1.15.1 (L-14); Scylla
QUORUM reads after a P9 LWT write on the same single-DC cluster observe it
(P9 uses the same pattern); the worker's clock margin used by `boundary_at`
is not needed for correctness (E1/E2), only as a margin; the execution
semantics of the run-fence and obligation LWT conditions (`IN`, `= null`,
map-entry updates, list append inside a conditional update, a conditional
batch with a static-column condition and a clustering-row insert — the
capacity admission) on Scylla 6.2.3 (R-26; only acceptance by `prepare` was
checked, and only for `IN`, `!=`, `= null`, `<=`); that a `Dot` collection
returns the stored float32 originals bit-identically (the manifest/content
digest of §12.5 depends on it; R-24 (vii)).

### 12.10 Index-integrity recovery: obligations and task reruns (P10-D19, P10-D20; corrected C-13, C-14)

This section is the authoritative description of what happens to tasks and
results when index points are found missing. §11.1, §12.6, §12.9 H, the
ADR and validation R-17/R-25/R-26 refer to it.

**Why the C-12 rule did not hold.** C-12 said a task reads `suspect_since` of
the generation it searched and may finish DONE only if it is unset. The read
is on `index_generations(spec, role)`, the DONE LWT on `match_tasks(subject)`
— different partitions, therefore different Paxos instances, and no
condition of the DONE LWT refers to the generation:

| Step | Matcher task X | Verifier |
|---|---|---|
| T1 | reads `suspect_since = null` | |
| T2 | paused after the read | |
| T3 | | finds S's points missing in g, sets `suspect_since` |
| T4 | | repairs S, arranges reruns |
| T5 | resumes; DONE LWT `IF epoch = :e` succeeds | |

A second read just before the LWT only narrows the window. Independently of
the race, the rule could never protect the tasks that matter most: a loss
happens at an unknown time `t_loss` and is detected later, at `t_detect`;
every task that searched g in `[t_loss, t_detect)` finished DONE before any
flag existed and missed the pair in exactly the same way.

**Datastore facts the protocol relies on (Scylla 6.2.3, vnodes, as `db.py`
configures it).** Guaranteed by Scylla: an LWT is a Paxos round on **one
partition**; LWTs on one partition are linearizable; a conditional `BATCH` is
allowed only within one partition; there is no multi-partition transaction;
a timed-out LWT has an unknown outcome, resolved by a SERIAL read (P9
`LwtUnknownError`); a plain write and an LWT on the same cells are not
ordered with each other, so every cell used as a fence below is written only
by LWT. Checked read-only on 2026-10-17 against the running Scylla 6.2.3 by
**preparing** (not executing) statements on an existing test table: `IF
epoch = ? AND mode IN (…)`, `IF … != …`, `IF … = null` and `IF … <= ?` are
accepted. Their execution semantics are exercised by R-26, not assumed
from the prepare. Guaranteed only by the application protocol: everything
else in this section.

**Option A (strict generation-health fence) — evaluated, rejected.** A real
fence between "generation becomes suspect" and "task completes" needs both
writes in one partition: either every task completion of a (spec, role)
commits inside `index_generations(spec, role)` (one Paxos partition for all
completions of the role, contended with builds, promotion and detections —
an unbounded hotspot, and a change of where task state lives), or
completions commit into N fence shards and detection marks all N shards
(N separate fences, not one atomic transition). Either way it fences only
completions *after* the marking; the tasks of `[t_loss, t_detect)` still need
a post-hoc recovery, and once that recovery exists the racing task is just
one more task in that interval. The fence would add contention and a schema
relocation without removing the recovery or strengthening any guarantee
about pairs. **Rejected.** Where a single partition already exists, the
design does use a real fence: the rerun request versus completion on the
lost subject's own task row (S5), and detection marking versus promotion in
`index_generations` (§12.4).

**Selected protocol (Option B): durable revalidation keyed by the lost
subject.** A loss of S's current points from a searched generation g can
hide only pairs (S, X) whose X-side search queried g while S was missing. By
§6.4 every such X was indexed before it searched. Hence a run of **S's own
tasks whose search starts after S is repaired in g** (or after g stops being
searched) sees every such X in the role it searches, and every X' that
searches g after the repair sees S. The obligation is therefore attached to
S and its own tasks — a bounded, known set — not to the unknown set of tasks
that searched g. Tasks of other subjects are never gated, re-opened or
invalidated.

| Role of S | Tasks re-run (kinds) | Pairs re-covered |
|---|---|---|
| media | `media_vs_targets`; `media_vs_media` if equivalence is enabled | (S, target) hidden from `target_vs_media` searches; (S, media) hidden from `media_vs_media` searches |
| target | `target_vs_media` | (S, media) hidden from `media_vs_targets` searches |

**Times.** `t_loss` (points disappear; unknowable), `t_detect` (obligation
durable), `t_repair` (S's current points confirmed present in g, or g
confirmed RETIRED/FAILED), `t_bump` (run-bump LWT on S's task row, after
`t_repair`), `t_close` (every bumped run finished).

**Detection — where a loss is found, and the rule every detector follows.**

| Detector | Scope |
|---|---|
| Qdrant verification pass (§12.6) | ACTIVE and DRAINING generations, every subject once per `verify_cycle_s`; per-subject count of `seq = row.seq` (READY generations too, for the promotion fence only) |
| apply pre-count (§12.2) | any apply to a searched generation where the subject was already applied |
| generation audit | a full per-subject count of g, requested by `index check` (RED, total-count mismatch), health findings or the operator; `audit_state` REQUESTED → RUNNING → DONE with a persisted bucket cursor |
| DRAINING post-horizon pass (§12.4) | the last check of a generation before RETIRED |

Rule: a detector makes the obligation durable **before** it (a) advances its
cursor past S and (b) writes anything that repairs S in g. A short count is
re-checked once against a fresh read of the `index_entries` row (an
in-flight new intent may legitimately be mid-cleanup); still short, or
ambiguous ⇒ loss. Conservative errors cost one rerun, never a missed pair.

**Detection completeness.** The only writers of index points are the §12.2
protocol (pre-counted for searched generations) and obligation repairs. A
loss that persists until a count of S in g is taken is therefore detected
while g is ACTIVE (verification cycle) or DRAINING (post-horizon pass),
before any write can heal it. Not covered: a *transient* loss that Qdrant
itself reverts before any count observes it (contrary to the trust
assumption that acknowledged writes stay durable and visible, §12.9; a known
limitation, §17), and a searched generation destroyed without its final pass
(handled by the fallback below).

**Obligation occurrences and their identities (corrected C-14, C-15).**

- **One row per occurrence; no head row.** The ledger table is
  `index_repairs` with PK `(spec_digest, role, bucket = shard_of(subject,
  64))`, CK `(subject_id, generation, occurrence)`, clustering order
  `occurrence DESC`. An **occurrence** is one detected loss episode of S in
  g that must be repaired and then re-searched by S's own tasks. Its row is
  created once (`INSERT … IF NOT EXISTS`), **never reset, never overwritten
  by a later occurrence and never deleted while the spec is indexed**. The
  "current occurrence" of (S, g) is simply the row with the highest
  `occurrence`; it is read, not stored.
- **Identity.** (`occurrence` int = 1, 2, …; `occurrence_id` timeuuid). The
  `occurrence_id` is generated fresh by the detector that opens the
  occurrence (time-based, node-unique, never reused) and written only by the
  creating INSERT. A task row records the run an occurrence requires in
  **`recovery_runs map<timeuuid, int>` keyed by `occurrence_id`** (§5,
  §11.1): an entry of one occurrence can never be read as the bump of
  another, and correctness depends neither on deleting nor on keeping
  entries.
- *Corrected C-14:* C-13 kept one `obligation_id` for every occurrence of a
  row and keyed the task map by it, so after a re-open the bump step took
  the first occurrence's finished run as the new requirement.
- *Corrected C-15:* C-14 kept one row per (S, g) and **reset** it when a new
  occurrence opened, keeping the ended occurrence only as a text entry in
  `history`. An occurrence closed `RERUN_INCOMPLETE` or `TASK_FAILED` and
  then superseded by a successful second occurrence disappeared from
  `recovery_unresolved` (computed from the current row only), so negatives
  could be reported `settled` while the first loss's pairs might still be
  hidden; after a restart nothing machine-readable said which runs the old
  occurrence still needed. Now every occurrence keeps its own row, state and
  requirements until it is resolved, and occurrences never supersede each
  other.
- *Why not a head row plus an occurrence table:* the two would be in
  different tables, therefore different partitions and different Paxos
  instances; opening an occurrence would need two LWTs that can be split by
  a crash (occurrence without head, or head without occurrence). A
  same-partition conditional batch over two tables does not exist (one
  batch with conditions must target one partition of one table), and a
  multi-row conditional batch within `index_repairs` would add a datastore
  feature to verify. With one row per occurrence every transition is a
  **single-row LWT**, and the only multi-row operation — finding the latest
  occurrence of (S, g) — is a read.

Columns of an occurrence row, all written by LWT and all initialised
explicitly by the creating INSERT (no null-as-default in any condition):
`occurrence_id`, `state` OPEN/REPAIRED/RERUN_PENDING/CLOSED (initial OPEN),
`detections` int (0), `gen_marked` bool (false), `detected_at`,
`detected_by`, `row_seq`, `expected_count`, `observed_count`, `repaired_at`
(null), `required_run map<text, int>` (task kind → run; {} until
RERUN_PENDING), `close_reason` (null), `closed_at` (null), `resolution`
(`PENDING` until resolved, then `RESOLVED`), `resolution_reason`
(`RERUN_DONE`, `LATER_RUN`, `SUBJECT_RETIRED`; null), `resolved_by map<text, int>`
(task kind → qualifying DONE run; {}), `resolved_at` (null), `attempts`
(0), `next_attempt_at`, `last_error` (null).

**Reading the latest occurrence of (S, g).** `SELECT … WHERE spec_digest = ?
AND role = ? AND bucket = ? AND subject_id = ? AND generation = ? LIMIT 1`
at LOCAL_SERIAL (clustering order puts the highest `occurrence` first). The
protocol does not depend on that read being a serial slice read: the
equivalent walk — `INSERT` occurrence 1 `IF NOT EXISTS`; a failed insert
returns the existing row; continue with *o* + 1 until an insert succeeds or
an existing row is the highest — uses only single-row LWTs. **Validated
2026-10-18 (validation §9a.4, §13.2):** on the deployed single-node Scylla
6.2.3 the slice read executes at `LOCAL_SERIAL`, returns the highest
occurrence after acknowledged inserts and after LWTs whose outcome was
unknown to the client, and six concurrent detectors acting on it produced no
duplicate, no skipped number and no merge past OPEN. The implementation
(`fingerprinter/recovery.py`) therefore uses the slice read; the walk stays
the documented (and tested) fallback, e.g. for a deployment where the slice
read is not available.

**Transitions — each one single-row LWT on the occurrence row** (`O` =
`occurrence_id = :oid`; the key fixes `occurrence`):

| Transition | Actor | Statement | What it protects |
|---|---|---|---|
| open | detector | `INSERT (…, occurrence = :n, occurrence_id = :new, state = 'OPEN', detections = 0, gen_marked = false, required_run = {}, resolution = 'PENDING', resolved_by = {}, …) IF NOT EXISTS`, with `:n` = 1 if (S, g) has no row, else latest + 1 **and only if the latest is past OPEN** (an OPEN latest is merged instead) | one row per occurrence number; a new loss after a confirmed repair gets its own row; the older occurrence keeps its row, state and requirements and proceeds independently (nothing is superseded) |
| merge | detector | `UPDATE SET detections = :d + 1, observed_count = … IF O AND state = 'OPEN' AND detections = :d` on the latest occurrence | a loss seen while the repair is unconfirmed invalidates a confirmation read before it |
| mark | reconciler | `index_generations`: `SET detect_count = :c + 1, suspect_since, suspect_reason IF detect_count = :c`; then `SET gen_marked = true IF O AND state = 'OPEN'` | promotion fence and operator flag (§12.4); a crash between the two raises `detect_count` twice — harmless, the fence only needs an increase |
| repaired | reconciler | `SET state = 'REPAIRED', repaired_at = :t IF O AND state = 'OPEN' AND gen_marked = true AND detections = :d` | `:d` read (SERIAL) after the repair write and before the confirming count |
| rerun pending | reconciler | `SET required_run = :m, state = 'RERUN_PENDING' IF O AND state = 'REPAIRED'` | `:m` built only from `recovery_runs[:oid]` of S's task rows |
| close | reconciler | `SET state = 'CLOSED', close_reason = :r, closed_at = :t [, resolution = 'RESOLVED', resolution_reason = :r, resolved_by = :b, resolved_at = :t when :r ∈ {RERUN_DONE, SUBJECT_RETIRED}] IF O AND state = 'RERUN_PENDING'` (for `SUBJECT_RETIRED`: `IF O AND state IN ('OPEN', 'REPAIRED', 'RERUN_PENDING')`) | close and, when already satisfied, resolution in one LWT |
| resolve | reconciler | `SET resolution = 'RESOLVED', resolution_reason = :why, resolved_by = :b, resolved_at = :t IF O AND state = 'CLOSED' AND resolution = 'PENDING'` | an unresolved close becomes resolved exactly once, with its evidence (`LATER_RUN` + qualifying runs, or `SUBJECT_RETIRED`) |

**Unknown LWT outcome** (`LwtUnknownError`): the actor makes no further
write for S (no repair, no cursor advance, no bump) until a SERIAL read of
the occurrence row has decided it:

| Transition | Applied iff the SERIAL read shows | If not applied |
|---|---|---|
| open | the row with key `:n` has `occurrence_id = :new` (the actor's own id — decisive) | another detector created `:n`: OPEN ⇒ merge into it; past OPEN ⇒ open `:n + 1` |
| merge | not decidable (another detector may have merged): row still OPEN with `detections > :d` ⇒ satisfied; `= :d` ⇒ retry; row past OPEN ⇒ **open** the next occurrence | (a needless occurrence costs one rerun, never a missed pair) |
| mark, repaired, rerun pending, close, resolve | `state` / `resolution` at or past the target value (another reconciler's success is equivalent: values derive from durable rows) | re-read the inputs and repeat |

**Repair (OPEN → REPAIRED).** SERIAL read of the occurrence (`O`). Re-read
the `index_entries` row and the authoritative P9 row. Subject no longer live
(§4.2 liveness, desired DELETE) ⇒ close `SUBJECT_RETIRED` (its tasks stop,
§12.3; pairs it lost while live are not recovered — §17). If g is RETIRED or
FAILED (SERIAL read), the repair is confirmed without a write. Otherwise
re-apply the row's **current** desired state to g through §12.2 — including
its capacity reservation (the lost points were already removed from the
audit's counts, so the repair reserves `segment_count` in g like any apply)
— with this exception: the §12.2 pre-count of a repair write is satisfied by
the OPEN occurrence the repairer holds and records no new detection. Then:
SERIAL read of `detections` (`:d`), exact count `subject = S AND seq =
row.seq` = `segment_count`, `repaired` LWT with `:d`. A failed condition ⇒
repeat the repair (idempotent ids). A target-role repair also enqueues the
target's classification step (§7.3).

**Run bump (REPAIRED → RERUN_PENDING), per task kind k of S** (the kinds of
the table above whose task rows exist or are required by the configuration
at bump time; the set is fixed in `required_run`). Write the `match_due`
entry first (idempotent), then SERIAL read `match_tasks(S, spec, k)`:
- `recovery_runs[:oid]` present ⇒ this occurrence's bump already happened
  (crash or unknown outcome earlier); use that run number;
- row absent ⇒ `INSERT … run = 1, state = 'PENDING', run_started_at = null,
  recovery_runs = {:oid: 1} IF NOT EXISTS`;
- state PENDING with `run_started_at = null` (run *r* not claimed, so it
  will search after now) ⇒ coalesce: `SET recovery_runs[:oid] = :r IF run =
  :r AND run_started_at = null`;
- state FAILED ⇒ no bump; required run := *r* + 1 (the run an audited `match
  retry` would create);
- otherwise ⇒ bump: `SET run = :r+1, state = 'PENDING', run_started_at =
  null, recovery_runs[:oid] = :r+1, rerun_reason = 'index_repair' IF run =
  :r AND state IN (<non-FAILED states>)`.
A failed condition ⇒ re-read and repeat; unknown outcome ⇒ SERIAL read
(`recovery_runs[:oid]` present ⇔ applied: only this occurrence writes that
key). Then the `rerun pending` LWT records `required_run`. The bump is the
only atomic step on the task and it lives on S's own row: Scylla linearizes
it with the completion LWT of run *r* (§11.1), so a run that may have
searched before the repair cannot satisfy `required_run` (S5).

**Close and resolution — precise rules.** Per kind k the task row gives
`done_run` (last finished run), `done_state` and **`done_ok_run`** (last run
that finished DONE; set by the DONE completion LWT, §11.1):
- kind k is **finished** iff `done_run ≥ required_run[k]`, or the task is
  FAILED with `done_run < required_run[k]`;
- kind k is **satisfied** iff `done_ok_run ≥ required_run[k]` — a run
  numbered at or after the required one started after this occurrence's
  repair (S5) and searched to completion (DONE excludes caps, saturation and
  verification failures, §7.5);
- the occurrence **closes** when every kind is finished (or the subject is no
  longer live); `close_reason` = `RERUN_DONE` if every kind is satisfied,
  else `TASK_FAILED` if some unsatisfied kind's task is FAILED, else
  `RERUN_INCOMPLETE`;
- the occurrence is **resolved** iff it closed `RERUN_DONE` or
  `SUBJECT_RETIRED` (resolution written by the close LWT), or — after an
  unresolved close — every kind became satisfied later (resolve LWT,
  `resolved_by` = the qualifying `done_ok_run` per kind);
- one later DONE run can satisfy several occurrences at once (every
  occurrence whose `required_run[k]` ≤ that run, of any generation of S):
  each is resolved by its own LWT, exactly once (`IF resolution =
  'PENDING'`), each recording the run as evidence; a run below an
  occurrence's requirement never resolves it, whatever happened to newer
  occurrences;
- an occurrence whose kinds are only partly satisfied stays **unresolved**
  (e.g. `media_vs_media` required while equivalence was enabled and never
  re-run after it was disabled — resolved only by an audited `match rerun`
  of that kind);
- `TASK_FAILED` / `RERUN_INCOMPLETE` stay visible on their own rows (state
  CLOSED, `resolution = PENDING`, alert/metric) until resolved. No terminal
  condition is equated with "no match": a FAILED task that is never retried
  leaves its occurrence unresolved for good, and every negative of the spec
  provisional (Option B's residual uncertainty, stated in §17);
- subject retired: `SUBJECT_RETIRED` is resolved (the subject is out of the
  live matching scope; pairs it lost while live are a documented loss, §17).
  This also applies **after** an unresolved close: once S is no longer live
  its tasks stop and can never produce the qualifying run, so the resolve
  LWT records `resolution_reason = SUBJECT_RETIRED` (corrected C-15b:
  otherwise one retired target with an unresolved occurrence would keep
  every negative of the spec provisional forever);
- generation retired or lost: the repair is confirmed by RETIRED and the
  rerun requirement is unchanged; a rebuild creates no occurrence and
  removes none (occurrence rows of retired generations stay until
  resolved).

**Discovery and the two counters.** The reconciler's `repairs` pass scans
all 64 buckets of `index_repairs` for (spec, role) with a persisted cursor
and processes every row that is not (`CLOSED` and `RESOLVED`): it drives
OPEN/REPAIRED/RERUN_PENDING rows forward and re-checks CLOSED-PENDING rows
for `resolve`. No row is ever found through another row, so an unresolved
occurrence cannot be hidden by a later occurrence of the same (S, g), and a
restart needs nothing but the table. From the same rows:
- `recovery_open(spec)` = rows in OPEN, REPAIRED or RERUN_PENDING (both
  roles) + generation audits REQUESTED/RUNNING;
- `recovery_unresolved(spec)` = rows CLOSED with `resolution = PENDING`
  (both roles).
Neither counter can decrease because a newer occurrence opened or closed:
a row leaves the first set only by its own close and the second only by its
own resolve. The scan cost is proportional to the occurrences ever opened
for the spec (losses, plus the unauditable fallback), not to the corpus.

**Concurrent and delayed actors.**

| Situation | Outcome |
|---|---|
| two detectors, (S, g) without rows | one INSERT of occurrence 1 wins; the other reads it OPEN and merges |
| two detectors, latest occurrence past OPEN | one INSERT of the next number wins; the other fails, reads it OPEN and merges |
| detector vs repair confirmation | merge before the repairer's `:d` read ⇒ the confirming count (taken later) is valid; merge after it ⇒ `repaired` fails, repair repeated; `repaired` first ⇒ the merge fails and the detector opens the next occurrence |
| worker of occurrence *o* delayed after occurrence *o* + 1 opened | it acts on row *o* only (key and `occurrence_id`); its steps are still valid for *o* — *o* is not superseded and must still close and resolve on its own requirement; it can never write row *o* + 1; its task-row writes use `recovery_runs[oid_o]` and can only move a run forward (one extra run at worst) |
| duplicate reconcilers | pass lease; every step conditioned on `O`, `state` or `resolution`; equal results |
| loss of S in two generations (ACTIVE and DRAINING) | separate rows, separate requirements; the bumps may coalesce into one run, which may then satisfy both |
| reconciler or process restart | all state in Scylla; the `repairs` pass resumes from its bucket cursor; Redis due ZSET rebuilt from `match_due` |

**Crash, retry and failure handling.**

| Event | State left | Recovery |
|---|---|---|
| detector crashes before the open/merge LWT | cursor not past S, no repair written | restart re-counts S; the loss cannot have been healed meanwhile (only pre-counted applies and obligation repairs write) |
| open/merge LWT outcome unknown | unknown | SERIAL read as above; neither cursor advance nor repair until decided |
| crash after OPEN, before the mark | `gen_marked = false` | reconciler marks, then continues (`repaired` requires the mark) |
| repair worker crashes mid-repair / before REPAIRED | OPEN | re-apply (idempotent ids), new `:d` read, re-confirm |
| crash after a bump, before `required_run` recorded | task row holds `recovery_runs[:oid]` | the next pass reads it; never bumps twice for one `occurrence_id` |
| crash after an unresolved close, before any resolve | CLOSED, `resolution = PENDING` | the next pass re-checks it; resolves only when every kind is satisfied |
| rerun worker crash, lease expiry, duplicate delivery | run in progress | P9 lease pattern; the same run resumes; duplicates fail on `epoch`/`run` |
| step keeps failing (Scylla/Qdrant down) | row unchanged | `attempts`, `next_attempt_at` (30 s → 15 min), `last_error`; after `max_attempts` metric `index_repairs_blocked` and alert, still retried; never dropped |
| g retired while an occurrence is open | — | repair confirmed by RETIRED; reruns follow |
| a searched generation cannot be audited (collection lost or unreadable before its final pass, `index drop --force-unverified`) | extent unknown | **fallback:** an occurrence (`detected_by = unauditable`; merged into an OPEN latest, else a new one) for every UPSERT row with `applied[g]` set (exact enumeration from Scylla), all durable **before** g may become RETIRED (§12.4); each repair is then confirmed by RETIRED and its reruns follow, rate-limited (`reconcile.rerun_per_s`), audited, visible; a whole-index loss (R-20) uses the same rule |

**Invariant R (the corrected guarantee).** For every non-protocol loss of the
current points of a live subject S from a generation g while g is searched
that a detector observes:
- R1 an occurrence row of `index_repairs` for (S, g) is open (or the loss is
  merged into the open latest one) durably before any write repairs S in g
  and before the detecting pass moves past S;
- R2 occurrence rows are never reset or deleted; an occurrence becomes CLOSED
  only by a close LWT issued after, for every task kind of S, a run numbered
  by a task-row LWT that followed **its own** REPAIRED transition has
  finished (DONE or INCOMPLETE), or with the visible terminal reason
  `TASK_FAILED` / `SUBJECT_RETIRED`; it is **resolved** only when every kind
  has a DONE run at or after its requirement (or `SUBJECT_RETIRED`), and no
  other occurrence's progress resolves, closes or replaces it (C-15);
- R3 a run of S that may have searched before the occurrence's repair cannot
  satisfy R2 (S5);
- R4 a task of any other subject finishes DONE on what it observed; that DONE
  is never re-opened by the obligation and asserts nothing about index
  integrity;
- R5 occurrence identities are unique: a task-map entry recorded for one
  occurrence can never be taken as another's bump, and no step of one
  occurrence can change another's row (each transition is a single-row LWT
  on its own row, conditioned on `O`); a single later DONE run may satisfy
  several occurrences only through each one's own requirement;
- R6 REPAIRED means: a confirming count taken after the repair write found
  S's current points complete, and no loss observation of this occurrence was
  recorded after the repairer's `:d` read. A loss after the confirming count
  is either merged before `repaired` (which then fails) or opens a new
  occurrence.
Consequence: every pair (S, X) the loss could have hidden is searched again
by S's qualifying run with X's points visible (§6.4) — unless X's points are
also lost, in which case X's own obligation re-runs X. As in §6.4 this is a
*visibility* guarantee; retrieval by ANN is measured (validation §4).
**Not guaranteed:** that no task finishes DONE during a loss (impossible
before detection); a bound on detection latency beyond one verification
cycle for a persisting loss; recovery of undetected transient losses, of
subjects retired before their rerun, or of pairs whose qualifying rerun is
INCOMPLETE or FAILED (those closes stay **unresolved** and keep every
negative of the spec provisional, below).

**Results and publication.**
- A positive decision is never invalidated by an index defect: it is computed
  on SHA-verified artifacts after an authoritative liveness check (§12.9 C);
  missing or stale points only remove or add candidates. Published
  `match.found` envelopes remain valid; nothing is retracted; the contract is
  unchanged.
- P10 stores and publishes no negative result. "No match" is a *reported
  status*, never an event or a row.
- **When a "no match" is `settled` (corrected C-14, C-15).** For subject X under
  spec s, `match status` reports `settled` only if, read in this order:
  (1) for every task kind of X, `run = done_run` (no newer run pending) and
  `done_state = DONE` — which by §7.5 means no candidate cap, no saturated
  segment, no verification failure, every query segment answered by the
  run's pinned generation within its search deadline (§11.1), and only
  usable low-information flags excluded; (2) afterwards, for spec s (both
  roles), `recovery_open = 0` (no occurrence row in
  OPEN/REPAIRED/RERUN_PENDING, no pending or running generation audit)
  **and** `recovery_unresolved = 0` (no occurrence row CLOSED with
  `resolution = PENDING`, **including every older occurrence of a subject
  whose latest occurrence is resolved** — C-15), both counted by a full scan
  of the occurrence rows, never from a gauge or from the latest occurrence
  only. Otherwise
  it is `provisional` with the failing conditions listed (`incomplete:
  <reason>`, `rerun pending`, `recovery open`, `recovery unresolved`).
  `settled` means "no *known* uncertainty"; it is not proof of absence — an
  undetected loss can exist (invariant R, not guaranteed). *(Corrected: C-13
  settled on `recovery_open = 0` alone, so an obligation closed
  `TASK_FAILED` or `RERUN_INCOMPLETE` — pairs possibly still hidden — let
  other subjects' negatives be reported settled; C-14 still counted only the
  current row of each (S, g), so a newer resolved occurrence hid an older
  unresolved one — C-15.)* The scan is a snapshot: an occurrence opened in a
  bucket after the scan passed it is a change after the decision, exactly
  like a loss detected later.
- Publication is still held only for calibration (§9.1). Matches found by a
  rerun are new decisions, published like any other (idempotent `match_id`).
- Validation gates (M3, drop/rebuild equivalence, growth, direction B)
  compare decision sets only at `recovery_open = 0` **and**
  `recovery_unresolved = 0`, and record the obligation counts.

*Generation flag.* `suspect_since` is cleared by `SET suspect_since = null IF
detect_count = :c` only after (i) a scan of all 64 `index_repairs` buckets of
(spec, role) finds no row of g in OPEN/REPAIRED/RERUN_PENDING and (ii) a
complete verification pass of g that started after the last detection found
no loss; `:c` is read before (i), so a concurrent detection fails the clear.
"Trusted again" means exactly (i) + (ii). The flag gates promotion (§12.4)
and retirement bookkeeping, never a task.

**Interactions.** Mutation intents: repairs use §12.2 and never bypass
intents or the capacity ledger. Promotion: the `detect_count` fence (§12.4).
Retirement: the post-horizon pass or, for an unauditable generation, the
fallback occurrences (§12.4); obligations outlive retirement.
Rebuild: a new generation is built from authority and needs no obligation;
the old generation's obligations still require their reruns. Low-information:
§7.3. Contention: obligations exist only after losses; the bump and the
completion stay on the task's own row; detection marks one generation row;
no path of normal task completion touches a shared partition.

**Schema implications (P10 tables only; none exists yet, so no migration).**
New `index_repairs`, one row per occurrence (`occurrence` in the
clustering key, `occurrence_id`, `detections`, `resolution`, `resolved_by` —
C-14, C-15); `match_tasks` gains `run`, `run_started_at`,
`done_run`, `done_state`, `done_ok_run`, `recovery_runs` (keyed by `occurrence_id`, C-14),
`rerun_reason`, `searched_generation`; `match_candidates` gains
`run` in its partition key; `index_generations` gains `detect_count`,
`validated_detect_count`, `audit_state`/`audit_cursor`, `promoted_at`,
`drain_horizon_at`. No P8 or P9 table, code or contract changes.

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
| capacity guard | `index.max_points` = the maximum number of **resident points in one Qdrant instance**: all specs, both roles, every generation whose collection exists (CREATED, BUILDING, CATCHING_UP, VALIDATING, READY, ACTIVE, DRAINING, and FAILED until its collection is deleted); enforced through the `index_capacity` ledger (below) | proposed |
| `index.max_points` value | **250,000 resident points** until E-R1 provides a measured value (it lies below the lower bound, ≈ 255 k, of the extrapolated *with-rebuild* band of §13.2, i.e. it already assumes two generations resident). Because a rebuild needs both generations resident, the largest (spec, role) of N points that can still be rebuilt in place satisfies resident + ⌈1.05 N⌉ ≤ `max_points` − `capacity_margin_points`, i.e. N ≈ (`max_points` − margin − every other resident collection) / 2.05 | provisional |
| matcher (each) | RSS ≤ 1 GiB, artifact LRU 256 MiB, 2 verification workers | proposed |
| rest of the host | crawler2 stack ≈ 4 GiB (P0); GPU encoder 2–3 GiB typical, > 5 GiB seen for a 4K source; desktop/browsers the remainder; Qdrant is **not** given the host's free memory | constraint |

The dev configuration is therefore suitable for development corpora
(L-PAIRS, SOAK-scale, the 7 targets) and for the I-2…I-5 tests; it is **not**
claimed to hold the base target scenario. (Example: the 104,443-vector
corpus as the media role plus the 7 targets ≈ 10 k target points: steady
≈ 114.4 k resident; media-role rebuild admission needs 114,443 +
⌈1.05 × 104,443⌉ = 224,109 ≤ 250,000 − 10,000 — admitted.)

**Capacity accounting (corrected C-10, C-14).**
- *What is counted:* reserved points, a conservative upper bound of resident
  points. Ledger `index_capacity(instance)`: static `reserved`, changed only
  by LWT `IF reserved = :r` (compare-and-set loop), plus one clustering row
  per admitted rebuild (`holder`, `points`).
- *When it is checked:* (1) **rebuild admission**, before CREATED: `P =
  projected(g) = Σ segment_count of the (spec, role)'s UPSERT rows +
  index.rebuild_headroom_points` (live growth during the build; default 5 %
  of projected); if `reserved + P > max_points − index.capacity_margin_points`
  the rebuild is refused with `CAPACITY_BLOCKED` (no generation row, no
  collection; the active generation untouched) and the requirement is
  reported; otherwise **one conditional batch on the ledger partition** sets
  `reserved = :r + P IF reserved = :r` and inserts the holder row
  `gen:<spec12>:<role>:<g>` with `points = P`; only then is the CREATED row
  (with `reserved_points = P`) written. The builder's bulk-load pages draw on
  P and write nothing to the ledger. (2) **Every live UPSERT whose `seq` is
  not yet applied to a generation** (a new subject, or a new intent of an
  indexed one — its new-`seq` points coexist with the old ones until
  cleanup, C-12), before the Qdrant call: reserve `segment_count` × each such
  routed generation in the ledger; refused ⇒ row `BLOCKED: capacity`, no
  Qdrant write. Every apply attempt of a `seq` not yet recorded in
  `applied[g]` reserves, and an unknown CAS outcome is retried
  (at-least-once: a duplicate reservation only makes the ledger too high,
  which is safe, and the next audit removes it).
- *Release — only by the capacity audit (corrected C-14).* No mutation,
  cleanup, DELETE or collection deletion decrements `reserved` itself. (The
  C-10 text released after each confirmed reduction; a retry after a crash
  or after an unknown CAS outcome could release the same points twice and
  leave the ledger **below** the resident count — the unsafe direction —
  until the next audit.) The audit is the single lowering path, runs every
  `capacity_audit_s` (default 60 s) and right after cleanups, DELETE batches
  and collection deletions, and computes, **in this order**: (a) SERIAL read
  of `reserved = r`; (b) holder rows and generation rows; (c) for every
  UPSERT row not in `BLOCKED: capacity` and routed generation g with
  `applied[g] ≠ seq`, `segment_count` (a blocked row was refused before any
  Qdrant write of that attempt, so it holds no reservation; points an
  earlier crashed attempt did write are counted in (d)); (d) exact Qdrant
  counts of every existing collection.
  Bound `B = Σ_{g with a holder} max(holder points, count_g) + Σ_{other
  existing collections} count_g + Σ (c)`. It writes `reserved = B IF reserved
  = r` (if anything was reserved meanwhile the CAS fails and the audit
  repeats). Why B ≥ the true need: a reservation made before (a) is either
  a holder row (read in (b)), or belongs to a row whose `applied[g] ≠ seq`
  until its bookkeeping — read in (c) before the counts (d), so a write that
  lands between (c) and (d) is counted twice (safe) and a write whose
  bookkeeping precedes (c) also precedes (d) and is counted in (d). A holder
  row is deleted (same batch as a re-computed `reserved`) only after its
  generation is RETIRED or FAILED with the collection deletion confirmed, or
  — for a crashed admission with no generation row — after the build lease
  of (spec, role) has expired. A DRAINING generation stays counted until its
  collection is deleted.
- *Two generations:* during a rebuild the ledger holds active + DRAINING (if
  any) + the new generation's holder; nothing about a second generation
  bypasses it. A live write into the building generation reserves in the
  ledger as well (counted twice until the next audit: conservative).
- *Drift:* every residual error leaves the ledger too **high** (safe) until
  the next audit, except points the audit cannot see yet: a stale worker
  re-landing older-`seq` points after (d), or an obligation repair whose
  reservation an audit removed before its write landed — each bounded by
  concurrent appliers or repairers × the largest subject (≤ 1,440 points
  each) — covered by
  `index.capacity_margin_points` (default 10,000), which admission and
  UPSERT checks subtract from `max_points`, once.
- *Visible, not slow:* `index_capacity_reserved_points`,
  `index_capacity_limit_points`, `index_capacity_blocked_total`,
  `index_rebuild_capacity_blocked`; rows in `BLOCKED: capacity`; tasks whose
  subject is capacity-blocked are `WAITING_INDEX` with reason `capacity`
  (distinct from `unavailable`); `index status` prints the ledger and the
  admission requirement. Recovery: the reconciler retries `BLOCKED:
  capacity` rows after any release; raising `max_points` is a configuration
  change audited in the ledger row and allowed only to a value measured by
  E-R1 (D14).

### 13.4 Decisive experiment E-R1 (capacity boundary; corrected C-10)

E-R1 runs **before I-2**, i.e. before any P10 indexer, ledger or guard
exists. It is therefore a harness run directly against Qdrant
(`benchmarks/p10-index/capacity_e_r1.py`, to be written when E-R1 is
authorised), on a **dedicated, separately named test container and volume**
(`qdrant-e-r1`), never on the dev instance. The production guard is not
overridden or weakened: it does not exist yet, and when it does its value
comes from this measurement. What E-R1 must test is the *same accounting*:
resident points of all collections in the instance, two generations during a
rebuild. The harness counts resident points exactly (`count` with `exact =
true` per collection at each phase boundary, and its own running total in
between) and enforces a **test ceiling** `P_test = 1,330,000` resident
points: it stops live writes if they would exceed it and records that.

*What phase 3 must reach (corrected C-13).* The guard admits a rebuild of a
(spec, role) only if `reserved + projected ≤ max_points − margin` (§13.3),
and `max_points` may be set only to a measured passing total. For the
rebuild E-R1 models to be admissible under the limit E-R1 produces, phase 3
must therefore hold **at least** the admission requirement resident:
`R_req(N) = (N + 10,000) + ⌈1.05 N⌉ + 10,000` (resident before the rebuild +
projected with 5 % headroom + `capacity_margin_points`). For N = 626,658:
636,658 + 657,991 + 10,000 = **1,304,649** resident (≤ `P_test`). The
previous text stopped phase 3 at the end of the build (≈ 1.30 M only at a
26-min build, less with faster gRPC ingest) and then subtracted the margin
from the measured total, so the guard would have refused the very rebuild
E-R1 had measured.

*Why E-R1 is conservative for the instance-wide limit (checked by C-12):*
the target payload is a superset of the media payload, so a resident point
of the measurement costs at least what a media point costs; but the dev
instance holds **both roles** (during a target rebuild: target old + target
new + media ACTIVE), and per-collection overhead (segments, WAL, optimizer)
is not proportional to points. E-R1 therefore keeps a third, media-role
collection of 10,000 points (media payload, same parameters) resident in all
phases, counted in the resident total; and the C-12 protocol's transient
duplicates (new-`seq` points before cleanup) are exercised by making every
10th live write a re-UPSERT of an existing subject at a new `seq` followed by
cleanup.

| Item | Specification |
|---|---|
| question | does one 1.5 GiB Qdrant instance hold a 626,658-point target-role generation **and** its rebuild (two generations resident, live writes, queries)? |
| corpus | the 104,443 cached real vectors + 5 seeded random orthogonal rotations = 626,658 vectors (norms and intra-copy geometry kept, no exact duplicates; valid for memory/latency, **not** for recall); target-role payload |
| Qdrant | 1.15.1, `--memory 1.5g --memory-swap 1.5g --cpus 2`, the §13.3 collection configuration, gRPC batches of 1,000 |
| live writes | from phase 3 on: one 12-segment write per second into **every** routed generation (both in phase 3), ids of the C-12 scheme, points counted; 9 of 10 are new subjects, every 10th a re-UPSERT of an existing subject at a new `seq` followed by its `seq <` cleanup |
| query workload | 10 single-segment searches/s, `limit 100`, rescoring on, concurrency 2, from the 760 real target-query segments |
| phases (resident points, incl. the 10,000-point media collection) | (1) build g1: 636,658; wait for green; (2) warm queries 3 × 760: 636,658; (3) build g2 of the same set while g1 serves queries and live writes go to both: 1,263,316 + 2 × live; **after the build, keep queries and live writes running until resident ≥ `R_req` = 1,304,649** (≈ 20.7 k live points per generation, ≈ 29 min of live writes in total; phase 3 never ends below `R_req`); (4) promote (harness pointer), delete g1: 636,658 + live; (5) 5 min steady queries |
| measurement (every 1 s, per phase) | cgroup `memory.current`, `memory.stat` (`anon`, `file`), `memory.peak`, `memory.events` (`oom_kill`); `du` of the volume; resident point count and generation count; query latency; live-write latency |
| pass | no `oom_kill`; `anon` peak ≤ 1.35 GiB (10 % margin); query p95 ≤ 10 ms in phases 2, 3 and 5 after a 60 s warm-up; exact counts at every phase boundary equal the harness's resident total |
| how the limit is set | `max_points` := the largest **resident total** measured in a run in which every phase passed (the phase-3 peak, which is ≥ `R_req`). The margin is **not** subtracted here — the guard subtracts it once at every check (§13.3). Never an extrapolated value, never above a measured pass. With a pass at 1,304,649 the guard admits exactly the modelled rebuild: 636,658 + 657,991 ≤ 1,304,649 − 10,000 |
| fail in phase 3 only (one generation fits, two do not) | `max_points` := the phase-3 resident total of the largest repeat that passes in full (repeat with 3 copies, N = 313,329, `R_req` = 662,325; then 2 copies, N = 208,886, `R_req` = 448,217); an in-place rebuild of a larger role on the dev host is **not supported**; an offline rebuild (stop, drop, rebuild) or a second instance would each need a separate approved procedure |
| fail in phase 1 | repeat at 3 and 2 copies as above; if even 2 copies fail, repeat with 1 copy (N = 104,443, `R_req` = 234,109) and **lower** `max_points` to the largest fully passing measured total, below 250,000 if so; if 1 copy fails too, P10 indexing on the dev host is blocked pending a separate decision |
| what a failure can never do | raise `max_points`; the provisional 250,000 stays until a full pass at ≥ 250,000 resident is measured |
| cost | ≈ 60–100 min host time per run, run alone; no new data; container and volume removed afterwards |

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
| `lowinfo_targets` (gauge) | spec, state (`complete`, `partial`, `missing`) |
| `lowinfo_refresh_pending_revs`, `lowinfo_refresh_lag_seconds`, `lowinfo_refresh_failures_total`, `lowinfo_enumeration_truncated_total` | spec |
| `lowinfo_witness_checks_total` | result (`live`, `dead`, `error`) |
| `lowinfo_backstop_diff_total` | spec, kind |
| `index_capacity_reserved_points`, `index_capacity_limit_points` (gauges) | instance |
| `index_rebuild_capacity_blocked` (gauge) | spec, role |
| `index_verify_stale_candidates_total` (stale point proposed a candidate; dropped) | role |
| `index_generation_suspect` (gauge), `match_task_reruns_total{reason=index_repair}` | spec, role, generation |
| `index_repairs` (gauge), `index_repairs_blocked` (gauge), `index_repair_oldest_open_seconds` (gauge), `index_repairs_closed_total` | spec, role, state / close_reason |
| `index_audit_progress` (gauge, buckets done), `match_recovery_open` (gauge, §12.10) | spec, role, generation |
| `p10_scan_stalled_total` | scan |
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
- Dev capacity is provisional (`max_points` 250 k resident points per
  instance, all generations) until E-R1 (§13.4); a role larger than half the
  remaining capacity cannot be rebuilt in place.
- Qdrant writes cannot be fenced; a stale DELETE cannot remove a newer
  intent's points (`seq`-versioned ids, `seq`-bounded deletes, §12.2 — relies
  on Qdrant's integer `range` filter, R-24); stale older points are found by the Qdrant
  verification cycle, not prevented, and cannot be accepted as evidence
  (§12.9 C). No time bound on a paused stale writer is claimed.
- Temporary false negatives remain possible after a **non-protocol** loss
  of index points (storage fault, manual deletion, defect): from the loss
  until the lost subject's qualifying rerun finishes (§12.10, invariant R).
  Tasks of other subjects may finish DONE during that interval — before
  detection nothing can prevent it; their DONE asserts no index integrity and
  every "no match" is provisional until `recovery_open = 0` and
  `recovery_unresolved = 0` (§12.10). Stale index
  operations cannot cause such losses (S2).
- Not recovered: a transient loss that Qdrant reverts before any count
  observes it (outside the trust assumption of §12.9), and pairs of a subject
  retired before its rerun (`SUBJECT_RETIRED`). An unauditable searched
  generation falls back to rerunning every subject indexed in it
  (rate-limited, §12.10).
- A new intent of an indexed subject briefly doubles its points (new `seq`
  before cleanup); counted by the capacity ledger, collapsed by §7.4.
- Low-information exclusions cost one Scylla point read per distinct witness
  per task (§7.3); unobserved additions leave flags missing until the
  refresh (safe, but less filtering).
- Targets-scan completeness relies on token-ordered walks of a vnode
  keyspace (verified by H-6/H-9, §6.1).
- Hints can be trimmed before P10 reads them; discovery latency is then the
  backstop cycle (§6.1).
- Calibrated confidence may be unavailable with the current labelled data;
  publication then waits (§9.1).
- "No match" is a reported status, `settled` only under the §12.10 rule
  (own run DONE, no recovery open or unresolved in the spec) and even then
  not a proof of absence. One obligation closed `RERUN_INCOMPLETE` or
  `TASK_FAILED` keeps every negative of its spec provisional until resolved
  (C-14).
- A saturated query segment makes the run INCOMPLETE (C-14). On heavily
  duplicated corpora this can make many direction-B runs INCOMPLETE (audit:
  228 of 760 target segments saturated at `k_max` 512 on the 19× corpus);
  `k_max` and the floor must be tuned so that saturation is rare at the
  expected growth — measured in I-1/§6, not assumed.
- The first committed decision of a `match_id` is final for its technique
  version; a later run that would decide differently (only possible through
  a different low-information exclusion set) is recorded, not applied (§9.3).
- An occurrence whose rerun is INCOMPLETE or FAILED stays unresolved until a
  later DONE run of every required kind (or the subject's retirement); a
  FAILED task that is never retried keeps the spec's negatives provisional
  indefinitely — visible, not hidden (C-15).

## 18. Rollout and increments (after approval)

| Increment | Content | Depends on | Exit |
|---|---|---|---|
| **I-1** (first) | (a) **V1 end-to-end baseline**, reproducible (validation §2.1) — before any verifier threshold is chosen; (b) verifier `temporal/v2`, `lowinfo/v1` evaluated offline on the vector cache against an unfiltered oracle; (c) nested film-grouped tuning and calibration; (d) dataset adequacy statement. No Qdrant. | D7, D8, D9, D10, D17 | baseline report; held-out P/R vs both V1 baselines; calibration status; param set `2.1` registered or an explicit "data insufficient" outcome (validation §2–§3) |
| I-2 | `vindex` + Qdrant (fingerprinter-owned dev compose), generations, mutation protocol (with apply pre-count), builder/catch-up/validation/promotion (detection fence, DRAINING horizon), P10 reconciler (intents, scans, Qdrant verification, audit, `repairs` pass over `index_repairs`), capacity guard; R-24 is the first test run | D1–D4, D14, **D19, D20**, a passed E-R1 (run separately, before I-2) | validation §5, §9 (index part), §10; R-1…R-24 |
| I-3 | match tasks with run fencing (§11.1), hint reader, direction A and B, candidates, commits with the corrected `matches` key, held `match.found`, obligation-driven reruns | D5, D6, D11, D19, I-1 calibration status | validation §4, §6, §7, §9, §9a incl. R-25, R-26 |
| I-4 | equivalence | D13, D18 | validation §8 |
| I-5 | failure injection, growth test, simulated two-host, M3 run, docs | all | M3 gate (validation §11); real multi-machine stays P14 |

Each increment is committed separately with tests and documentation.
Migrations are additive (P10 tables only). P10 can be disabled by not
running `matcher run` / `matcher reconcile run` — P9 behaviour is
unaffected. I-2 does not start until the design is approved (D1–D4, D14, D19,
D20) **and** E-R1 has passed; I-3 does not publish until a calibrated
technique version exists.

**Status vocabulary (2026-10-17).** *Design corrected*: C-1…C-15 written.
*Design reviewed*: reviews 1–3 done; C-13…C-15 not yet reviewed. *Decision
approved*: none. *Implementation completed*: none. *Validation passed*: none
(no P10 test, E-R1 or I-1 measurement has run). *Permitted next*: design
review of C-13 and the approval decisions of decisions.md §2; after the
relevant approvals, I-1 (no Qdrant) and E-R1 (harness only) may run; I-2
only after both D-approvals and an E-R1 pass.

**Facts still unknown, how they are established, what they block.**

| Fact | Established by | Blocks |
|---|---|---|
| V1 end-to-end P/R on L-PAIRS / N-SOAK | I-1 (a), validation §2.1 | threshold choice, M3 |
| `temporal/v2` held-out P/R, calibration adequacy | **measured 2026-10-18** (validation §13.3): offline held-out P 0.9989 / R 0.812; calibration UNAVAILABLE | publication (held), M3 (needs the live path) |
| `lowinfo/v1` `lost_tp` | **measured 2026-10-18**: `lost_tp` = 1, 12 flagged ⇒ not enabled | enabling lowinfo (not I-2) |
| 1.5 GiB holds two generations at ≥ `R_req` | E-R1 | I-2 (and the `max_points` value) |
| Qdrant 1.15.1 integer `range`/`match` filter, boundary and subject scoping semantics | **established 2026-10-18: R-24 pass** (validation §13.1) | — (protocol used as designed) |
| Scylla 6.2.3 execution semantics of the run-fence and obligation LWTs (`IN`, `= null`, map-entry set, list append in an LWT) and of the capacity admission batch (static condition + clustering insert) | **statement forms established 2026-10-18** (validation §13.2); the protocol-level R-26/CAP-9 scenarios are separate tests | I-3 exit; I-2 exit (scenarios) |
| Scylla 6.2.3 executes a LOCAL_SERIAL read of a clustering slice with `LIMIT 1` (latest occurrence of (S, g)) | **established 2026-10-18** (validation §13.2, single node) | — |
| `Dot` collections return stored float32 vectors bit-identically | **established 2026-10-18: R-24 (vii) pass** | — |
| saturation rate at the tuned `k_max` | I-1 tuning, validation §6 | practicality of direction B (INCOMPLETE rate), not correctness |
| targets-scan completeness on populated data | H-9 | D20 / I-2 exit |
| `exact` + `quantization.ignore` on 1.15.1 | **established 2026-10-18** (L-14 Qdrant part, validation §13.1) | — |

## 19. Correction record (2026-10-15, after the first design review)

| # | Original assumption | Why unsafe / inconsistent | Correction | Decision impact | Evidence | Outstanding | Gate before implementation / M3 |
|---|---|---|---|---|---|---|---|
| C-1 | 1.5 GiB Qdrant; target role float32 in RAM; dev capacity "≈ 1 M vectors" | base target raw FP32 = 1.717 GiB > limit; steady-state bytes/vector were used as if they bounded the build peak | §4.2 both roles int8 + on-disk originals; §13 separates raw / steady / build peak / disk / host; capacity guard; `max_points` 250 k provisional | **changed** D2, D14 (still proposed); dev capacity **unresolved** | audit §9.3 measurements; arithmetic §13.2 | E-R1 | E-R1 before I-2; base target scenario not claimed |
| C-2 | stored `lowinfo` flags on media and target points | flags depend on the target set; a registration/retirement could leave stale flags that silently exclude candidates | §7.3: media never flagged; target flags versioned by `target_set_rev`; exact affected-set rule; LWT `IF rev <`; stale ⇒ eligible; refresh worker + weekly backstop | **changed** D8 (still provisional) | design reasoning; no measurement yet | validation §3.4 offline oracle comparison; tests L-1…L-10 | I-1 measurement before use |
| C-3 | "upsert, then record `index_entries`"; alias switch as promotion | no atomicity between Qdrant and Scylla; uncertain outcomes; stale writers; promotion of an incomplete generation; alias/Scylla disagreement | §12: intent-first mutation protocol with `seq`; Qdrant-side verification for unfenceable writes; generation state machine; build lease; completeness proof; single-LWT promotion; manifest digests | **new** D19; D2 clarified | P9 LWT/lease patterns (`intake.py`, `commit.py`) | tests R-1…R-15 (R-16…R-21 added by C-11) | D19 approval before I-2 |
| C-4 | "P9 code unchanged" and "the P9 reconciler gains a P10 pass" | contradictory; couples P9 to index consistency | §6.1, §12.6: P10-owned hint reader (cursor after durable work, epoch fencing) and P10 reconciler with persisted backstop cursors; eventual-catch-up statement | **new** D20; D5 clarified | `publish.py` (XTRIM MINID; target events held), `intake.py` lease pattern, `db.py` schema | tests H-1…H-9 | D20 approval before I-2 |
| C-5 | `confidence` = "precision among decisions at least this strong", fitted by isotonic | conflates per-decision calibrated probability with threshold-level precision | §9.1: confidence = calibrated probability of correctness; operating-point P/R reported separately; nested grouped protocol; publish only when calibrated | **changed** D9 | statistics; seven-film data limits | validation §3; calibration may be unavailable | I-1 outcome; no publication without calibration |
| C-6 | `matches` key `(content_id; target_id, target_version, technique_version)` | not injective: two specs (or technique names) ⇒ two MatchIds, one key | §9.3a verified identity and corrected key; `matches_by_id` | **changed** D11 | `ids.py`, `models/matching.py`, `spec.py` | tests K-1…K-8 | — (design fix) |
| C-7 | V1 end-to-end baseline "measured in I-1" without being a prerequisite | a verifier tuned against a partial V1 path can look better than V1 | I-1 starts with the reproducible V1 end-to-end baseline; it gates threshold selection and M3 | **clarified** D17 | `worker/matching_handler.py` entry point | baseline run | before verifier thresholds and M3 |

### 19.1 Second correction pass (2026-10-15, after the second review)

| # | Original assumption | Why unsafe / inconsistent | Correction | Decision impact | Evidence | Outstanding | Gate |
|---|---|---|---|---|---|---|---|
| C-8 | targets backstop: `token(target_id) > last_token LIMIT 200`, cursor = last token per page | `target_representations` has clustering rows `(version, spec_digest)` under one `target_id`; a page ending inside a partition makes `>` skip the rest of that partition on every cycle | §6.1: partition-granular walk (`SELECT DISTINCT … token >= :t`), whole partition read, cursor `(token, target_id)` of the last fully processed partition, epoch-fenced, `>=` resume; completeness argument; `scan_stalled` instead of skipping | D5 clarified | `db.py` schema (PK `(target_id, version, spec_digest)`, tablets off), `targets.py` (no deletes; bind `IF state = 'PENDING'`; retirement in `targets`) | H-6, H-9 on Scylla 6.2.3 | D20 / I-2 |
| C-9 | a classification at the current `target_set_rev` is fresh; affected targets found by "exact search" | `rev` is only what P10 has *observed*; a retirement P10 has not yet seen leaves a flag that suppresses evidence; a top-`L` search is not an enumeration | §7.3: four states separated; exclusion only through a **usable** flag — COMPLETE record **and** a witness authoritatively live at check time (point reads of P9 rows); removals found exactly from witness lists; `exact` + `quantization.ignore` + `score_threshold`; a result of size `L` = truncated ⇒ `done_rev` not advanced, full recomputation; stale payload never trusted | D8 changed (still provisional) | design reasoning; schema; Qdrant `SearchParams` (to be verified, L-14) | L-1…L-16; validation §3.4 | enabling the filter |
| C-10 | `max_points` 250 k "per instance" and E-R1 with 626 k per generation | the guard as written forbids its own experiment; the meaning (per generation / per instance / during rebuild) was not fixed; extrapolated outcomes could raise the limit | §13.3: resident points per instance across all existing collections, ledger with reservation-before-write, rebuild admission reserves the projected generation; §13.4: E-R1 on a dedicated container with a test ceiling, counting the same way (≈ 1.25–1.29 M resident in phase 3; revised by C-12 to include a media collection and by C-13 to ≥ `R_req` = 1,304,649); limit set only from a measured full pass, never raised by a failure | D14 changed | arithmetic; earlier measurements unchanged | E-R1; CAP-1…CAP-8 | E-R1 before I-2 |
| C-11 | "the reconciler eventually fixes stale writes"; completeness proof without race list | no statement of what may be accepted as evidence while a stale point exists; routing set said `BUILDING…` while CREATED was routed; unknown intent LWT outcome not handled | §12.9 Invariants A–G with sources of truth, routing-race table; candidate acceptance only on live P9 rows + artifact vectors; authoritative coverage walk before promotion; generation numbers never reused; error classes; reconciliation table with detection/idempotency/metric/test | D19, D20 clarified | §12 text; `db.py` LWT unknown handling | R-1…R-21 | D19/D20 approval before I-2 |
| C-12 (2026-10-17) | point ids `uuid5(subject:k)`; DELETE = delete by filter on the subject only; "stale writes are repaired within one verification cycle" | a stale DELETE (intent 11) landing after a newer UPSERT (12) was applied and recorded removes the newer points; also a stale UPSERT 10 overwrites the 12 points before a stale DELETE 11 removes them. A live subject goes missing from the index → candidate retrieval can miss a true match; a task could finish DONE meanwhile; the stale-UPSERT tests covered only the opposite direction | §4.3 ids include `seq`; §12.2 UPSERT = write ids of `u` then cleanup `seq < u`, DELETE = `seq ≤ d`; §12.9 S2 strict (a stale operation can no longer remove current points) and H (non-protocol loss: `suspect` generation, no DONE while suspect — *superseded by C-13* — re-run of the repaired subject's task, DRAINING verified before RETIRED); manifest by segment index; capacity reserves new-`seq` points; E-R1 adds a resident media collection and re-UPSERTs | **changed** D4 (point id); D19, D14 clarified | Qdrant `range` filter on an integer payload index and delete-by-filter are documented API; behaviour on 1.15.1 **not yet executed** (R-24) | R-22…R-25 (planned); E-R1 | D4/D19 approval before I-2 |
| C-13 (2026-10-17) | (a) C-12: a task reads `suspect_since` and may finish DONE only if unset (`WAITING_INDEX(suspect)` otherwise); (b) DRAINING verified by a pass "started after promotion"; (c) lowinfo witness cosine = Qdrant float32 score; (d) E-R1 limit = phase-3 peak − margin | (a) check-then-act across `index_generations` and `match_tasks` (different partitions, no shared Paxos instance): a task paused after its read finishes DONE after the marking; and no rule at detection time can cover tasks that finished between the loss and its detection; (b) a loss after a subject was checked but before the last search of the old generation goes undetected; any repair/apply could silently heal an unrecorded gap; (c) a corrupted index vector could create a flag; (d) margin subtracted twice and phase 3 below the admission requirement ⇒ the guard would refuse the measured rebuild | §12.10: Option A (generation-health fence) evaluated and rejected; durable recovery ledger `index_repairs` keyed by the lost subject; detection durable before repair and cursor advance; apply pre-count; generation audit and unauditable fallback; run-numbered tasks with single-row run fencing (§11.1); invariant R replaces "no DONE while suspect"; promotion fenced on `detect_count`; DRAINING pass after the search horizon; results: positives never invalidated, negatives provisional until `recovery_open = 0`; §7.3 witness cosine on artifacts; §13.4 phase 3 ≥ `R_req`, limit = measured total, margin applied once | D19, D20 clarified (D19 text corrected); D8 clarified; D14 clarified | Scylla single-partition LWT/conditional-batch rules (documented); LWT condition forms prepared (not executed) on the running Scylla 6.2.3 | R-25 revised, R-26 new; R-24 expanded; E-R1 | D19/D20 approval before I-2; R-26 before I-3 exit |

### 19.2 Fifth correction pass (2026-10-17, C-14: consistency and edge-case audit)

Each item: the failure, the violated invariant, why the previous text failed,
the correction, and its test. "Design" = fixed in this document; nothing is
implemented or run.

| # | Failure scenario | Invariant violated | Why the previous text failed | Correction | Test |
|---|---|---|---|---|---|
| C-14a | loss 1 of S in g: open, repair, bump (task map `obligation_runs[oid] = 2`), run 2 DONE, CLOSED; loss 2 re-opens the **same** `obligation_id`; the bump step finds `obligation_runs[oid]` "already present", records run 2 (already finished) and closes without a new rerun | R2/R3: an occurrence closes only after a run started after **its** repair | occurrence number changed but the identity used by the task map did not; correctness depended on a "cosmetic" cleanup of old map entries; occurrence-scoped fields were not all reset | §12.10: fresh `occurrence_id` per occurrence, changed with `occurrence` in one LWT; every transition conditioned on both; task map `recovery_runs` keyed by `occurrence_id`; all occurrence-scoped fields written at open; unknown outcomes decided by the actor's own new id; actor-interaction table | R-27 (§9a.2) |
| C-14b | second loss while OPEN, after the repairer's confirming count but before its REPAIRED LWT: merged into OPEN, then REPAIRED; S's rerun and a later X both miss the pair | R1/R6: no unrecorded loss after a confirmation | "OPEN ⇒ no-op" merge; REPAIRED had no fence against a concurrent detection | `detections` counter: merge `IF … detections = :d`; `repaired` `IF … detections = :d` with `:d` read after the repair write; a failed merge past OPEN opens a new occurrence | R-28 |
| C-14c | rerun of S ends INCOMPLETE (cap) or S's task is FAILED; obligation CLOSED; `recovery_open = 0`, so other subjects' "no match" is reported settled while (S, X) may still be hidden | no silent negatives | settlement read `recovery_open` only; INCOMPLETE/FAILED closes were terminal | unresolved closes (`RERUN_INCOMPLETE`, `TASK_FAILED`) counted in `recovery_unresolved` until a later DONE run resolves them; `done_state` on the task; exact `settled` rule (§12.10) | R-27 variant u1/u2; gates use both counts |
| C-14d | one candidate's artifact is corrupt ⇒ S's task FAILED ⇒ S's obligation closes `TASK_FAILED` | task failure reflects the subject | §11.1/§11.2 did not distinguish the subject's artifact from a candidate's | candidate FAILED `artifact_corrupt`, run INCOMPLETE `verification_failed`; task FAILED only for the subject's own data | G-10 (expanded) |
| C-14e | the same pair verified in direction A (offset modes from A's ANN votes) and in B (different votes), or before/after a rebuild: different runs, verdicts or confidence for one `match_id` | ANN only proposes; decisions reproducible from stored provenance; drop/rebuild identical decisions | `temporal/v2` took offset modes from ANN hit votes | offsets from the dense similarity of the two artifacts; verifier inputs = artifacts + params + exclusion set (recorded) | V-1 (§5), §5 exact-vs-ANN comparison |
| C-14f | lowinfo witness retires between two runs; the second verification of a pair gives a different decision for the same `match_id` ⇒ "invariant violation" alarm; held release / republish could build a second envelope with a new event id | MatchId idempotency; one envelope per decision | "different stored decision = violation"; release and envelope writes unconditioned | first committed decision is final; divergence recorded; already-decided candidates skipped; envelope/event id `IF event_id = null`; release `IF calibration_id = null` | K-9, K-10 |
| C-14g | a run with saturated segments ends DONE (§7.5) and its negatives look complete | INCOMPLETE when completeness is not established | §7.5 recorded saturation only, §6.3 and D6 said INCOMPLETE | INCOMPLETE `saturated_segments` in every direction; explicit stopping condition and bounds (§7.1) | G-15 |
| C-14h | direction A: a media segment's best hit is a flagged black frame of T1; its true, lower hit on an unflagged segment of T2 is silenced | filtering never silently omits a true positive beyond the measured gate | per-segment exclusion keyed on the best hit | per-hit exclusion | L-17 |
| C-14i | a run caches the old pointer, searches DRAINING segment by segment past `drain_horizon_at`; the post-horizon pass has already checked S; S is lost afterwards and the late queries miss it undetected | DRAINING horizon premise | `search_timeout_s` was not defined as a bound on the whole search step; a run could mix generations; a failed segment query was not defined | search step pins one generation, step deadline, `SEARCHED` only when every query answered; failed query ≠ zero hits | R-10 (expanded), G-1 |
| C-14j | three liveness predicates ("not RETIRED", "∉ {RETIRED, PURGED}") for targets | one authoritative liveness | written separately per section | one predicate (§4.2), checked against P9 code, shown monotone | K-11 |
| C-14k | worker applies g1 of two routed generations, sets `state = APPLIED`, crashes | R-4 expects PENDING | bookkeeping set APPLIED per generation | APPLIED only when every routed generation is applied; checks read `applied[g]` | R-4 |
| C-14l | a cleanup is retried after a crash (or its release CAS times out) and releases the same points twice; ledger below resident count; admission over-admits | ledger is a conservative upper bound | per-operation releases are not idempotent | only the capacity audit lowers `reserved`, from durable state in a fixed read order; admission holder rows in the ledger partition (one conditional batch) | CAP-5, CAP-6, CAP-9 |
| C-14m | ACTIVE collection lost: no legal transition (no ACTIVE→FAILED); after a rebuild the old generation is DRAINING but its required post-horizon pass cannot run ⇒ never RETIRED | every failure has a recovery path | transitions table and RETIRED rule assumed a readable collection | ACTIVE stays ACTIVE (searches fail loudly) until superseded; DRAINING→RETIRED by clean pass **or** durable unauditable-fallback occurrences | R-20, R-25(d) |
| C-14n | `equiv/v1` "≥ 90 % of the informative segments" — media segments are never classified | defined rule | lowinfo applies to targets only | coverage over all segments; revoke via intent; no re-link of REVOKED | §8 E-SET cases |
| C-14o | "complete generation" read as "every point present during every search" | claims match the protocol | properties not separated | P1–P4 table (§12.9 E) | — (wording) |
| C-14p | R-26 d1 expected `detect_count` + 1 exactly; a crash between mark and `gen_marked` raises it twice | tests assert design properties only | the mark is not idempotent (two partitions) and does not need to be | expectation: `detect_count` increased ≥ 1 per occurrence | R-26 |
| C-14q | R-22's fixture (DELETE 11 of a live subject, then UPSERT 12) is not produced by P9 paths: liveness is monotone (§4.2) | tests state what they inject | fixture described as a natural sequence | kept as an injected robustness test of S2 (S2 must not depend on monotonicity) | R-22 |

Not changed by C-14, and still open: R-24 (Qdrant filter semantics), E-R1,
H-9 (populated-data scan), L-14, R-26 execution semantics, I-1 baseline,
calibration adequacy — design §18 "facts still unknown". No decision was
approved; D6, D7, D8, D9, D11, D13, D14, D19, D20 texts are clarified or
changed (decisions.md §5).

### 19.3 Sixth correction (2026-10-17, C-15: unresolved occurrence history)

| # | Failure scenario | Invariant violated | Why C-14 failed | Correction | Test |
|---|---|---|---|---|---|
| C-15a | occurrence 1 of (S, g) repaired; its rerun ends FAILED (or INCOMPLETE) ⇒ CLOSED unresolved; a second loss opens occurrence 2, which **reset the same row** (C-14 open-again) and moved occurrence 1 into a `history` text entry; occurrence 2 closes `RERUN_DONE`; `recovery_unresolved`, computed from current rows, is 0 ⇒ negatives reported `settled` while occurrence 1's pairs may still be hidden; after a restart nothing machine-readable records which runs occurrence 1 still needs | no silent negatives; R2 (an occurrence is satisfied only by runs after **its** repair); `recovery_unresolved` monotone except by an occurrence's own resolution | one mutable row per (S, g); old occurrences reduced to text; the counter read only the current row | `index_repairs` CK `(subject_id, generation, occurrence)`: one row per occurrence, created once, never reset or deleted; no head row (the latest occurrence is read; single-row `IF NOT EXISTS` walk as fallback); occurrences never supersede each other; per-kind `required_run`, `done_ok_run` on the task, `resolution`/`resolution_reason`/`resolved_by` per occurrence; both counters from a full scan of occurrence rows | R-29 v1–v12 (§9a.3); R-27 revised |
| C-15b | target T has an occurrence closed unresolved; T is then retired; T's tasks stop, so no qualifying DONE run can ever come; `recovery_unresolved` stays > 0 for the spec forever | every failure has a defined end state; settlement reachable for live scope | `SUBJECT_RETIRED` applied only to occurrences not yet closed | resolve with `resolution_reason = SUBJECT_RETIRED` when the subject is no longer live | R-29 v13 |

Cross-check of C-15 against C-13/C-14: unique `occurrence_id` and
`recovery_runs` keyed by it are unchanged; every `index_repairs` transition
is still a single-row LWT, now on the occurrence's own row (no cross-row
atomicity is assumed); a second loss while OPEN merges, while
REPAIRED/RERUN_PENDING/CLOSED (resolved or not) opens a new row — the older
occurrence continues on its own requirement instead of being superseded;
run fencing (S5), the detection fence of promotion, the DRAINING horizon,
the unauditable fallback, rebuild and `index drop` behave as before;
publication and `match.found` are unaffected. No decision was approved.

## 20. Implementation notes (2026-10-18)

The implementation follows §3–§16; module map in `docs/p10-matcher.md` §6.
Where the code makes a choice the design left open, or differs in a way a
reviewer should know, it is listed here (none weakens an invariant):

| # | Point | Implementation | Why / impact |
|---|---|---|---|
| N-1 | Qdrant client | REST via `requests` (already a dependency); E-R1 harness uses `qdrant-client` gRPC in the isolated benchmark venv, as §13.4 specifies | no new service dependency; parameters such as `exact`, `quantization.ignore` passed literally (R-24/L-14 verified) |
| N-2 | latest occurrence of (S, g) | the `LOCAL_SERIAL … LIMIT 1` slice read (validated §9a.4) | the walk stays documented and tested as fallback |
| N-3 | hint reader | run by the P10 reconciler cycle (`reconcile_once`), not by `matcher run` | one owner per reader row either way (epoch-fenced); the task workers stay pure |
| N-4 | task queue | Redis ZSET rebuilt from `match_due` at every worker round; per-task Redis lease for avoidance; correctness from the `epoch`/`run` LWTs | as §6.1 D |
| N-5 | search deadline | measured on the matcher's monotonic clock from the pointer read; every round checks it and passes the remaining time as the request timeout | bounds the DRAINING horizon as §11.1 requires |
| N-6 | verification timeout | measured after `verify()` returns (CPython cannot pre-empt a pure computation); an over-long verification counts as a failed attempt | the bound is enforced per attempt, not mid-computation; measured verification times are < 1 s (validation §13) |
| N-7 | lowinfo refresh | on a revision change every member without a current COMPLETE record is reclassified (full per-target exact searches), instead of incremental owner updates | same records (deterministic), more work per revision; acceptable at the 7-target scale; revisit before the base target scenario. Not enabled anyway (validation §13.3) |
| N-8 | `reps` scan pacing | one page per bucket per reconciler call, rate-limited by `scan_rows_per_s` | cycle completion as §6.1 B |
| N-9 | a subject no longer live | its task run ends INCOMPLETE `subject_not_live` (the task "stops", §12.3); its occurrences close/resolve `SUBJECT_RETIRED` | an INCOMPLETE status never asserts completeness |
| N-10 | `match rerun --raise-cap` | per-task `cap_override` column (additive) used by the search step | audited (`audit_log`) |
| N-11 | content digest | per subject SHA-256 over (segment index, float32 bytes, canonical payload without `seq`), bucket digests sorted then hashed, generation digest over buckets in order | R-24 (vii) makes Qdrant-side and artifact-side digests comparable bit for bit |
| N-12 | test fixtures | integration tests ingest through P9's own request → claim → commit and `TargetManager.register`, with artifacts built from vectors (cached real `dinov2-seg/2` vectors or seeded synthetic ones) — no GPU encode in tests | the P10 path under test is unchanged; P9 encoding is P9's gate |
| N-13 | own content in direction A | a target's reference content is itself a media subject; its `media_vs_targets` run excludes hits whose payload content id is its own (§6.3 states the exclusion for direction B only) | found by test K-9: without it every reference content "matched" its own target — a self-match, not a finding; symmetric with §6.3 |
