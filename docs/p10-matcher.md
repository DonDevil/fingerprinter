# P10 vector index and matcher (`fingerprinter/`, phase P10)

Local guide for developing, testing and running the P10 matcher: the
derived vector index (Qdrant), matching in both directions with ANN
candidate reduction and temporal verification, index generations, the
mutation/recovery protocol and the P10 reconciler. The design of record and
all evidence are in [`phases/p10-vector-index-matcher/`](phases/p10-vector-index-matcher/README.md);
executed results are only in its `validation.md` §13.

**Status (2026-10-18, after the exit-gate pass): implemented; 105 integration/unit tests pass on the real stack; M3 not passed** (gates and evidence: `validation.md` §11, §13; owner decisions: `decisions.md` §6).

## 1. Boundaries

- P10 code lives in `fingerprinter/` next to P9 and reads P9 tables only
  (`representations`, `representations_by_spec`, `targets`,
  `target_representations`); it writes only its own tables
  (`fingerprinter/p10db.py`), its own `stream_registry` row
  (`match.found.v1`) and `retire_guard(spec, 'p10-index')`.
- No crawler2 code or contract changes; `match.found` 1.0 is used as is.
- V1 (`matching/`, `worker/`, …) is untouched; it is run only as the
  frozen baseline (`benchmarks/p10-index/v1_e2e_baseline.py`).

## 2. The index service (dev)

Qdrant 1.15.1, pinned by digest, owned by the fingerprinter
(`deploy/qdrant/compose.yaml`): loopback ports 16333 (HTTP) and 16334
(gRPC), API key, hard 1.5 GiB memory limit with no swap, 2 CPUs.

```bash
cd ~/anti_piracy/fingerprinter
umask 077; printf 'FINGERPRINTER_INDEX__API_KEY=%s\n' "$(head -c 24 /dev/urandom | base64 | tr -d '/+=')" > deploy/qdrant/.env
sg docker -c "docker compose -f deploy/qdrant/compose.yaml --env-file deploy/qdrant/.env up -d"
```

`deploy/qdrant/.env` is untracked (`.gitignore`). The index is derived
state: deleting the volume loses nothing an `index rebuild` cannot restore.

## 3. Configuration

`FINGERPRINTER_<SECTION>__<FIELD>` as for P9 (`fingerprinter/config.py`).
P10 sections: `index` (Qdrant URL/key, capacity ledger, generation timing),
`matching` (candidate policy, verification, tasks, publication) and
`matcher_reconcile` (backstop scans, repairs, audits). Start-up refuses
inconsistent bounds (e.g. `index.max_points` other than the provisional
250,000 unless `index.max_points_evidence` is the path of a **passing,
gate-eligible E-R1 report** whose measured total is ≥ the value (CAP-8),
`drain_grace_s` below `2 × active_ttl_s + task_lease_s`, `k0 > k_max`, an
unknown `matching.equivalence_rule`). `matcher run` also refuses to start
when a candidate floor exceeds the configured technique's `s_seg` (a floor
above `s_seg` would drop confirmable pairs).

Values changed by the 2026-10-18 exit-gate pass (validation §13.17–§13.19):
`index.search_ef` 384 (minimum HNSW ef; with ef = k, queries inside dense
near-duplicate clusters missed their own neighbourhood), `matching.k_max_b`
4096 (a target segment of a much-copied film has up to 3,208 true
neighbours ≥ 0.70 at 19x; 512 saturated every direction-B run), and the
adapter requests uncompressed responses (`accept-encoding: identity`;
gzip made small searches take ≈ 40 ms). `matching.equivalence_rule`
selects an immutable equivalence rule version (`equiv/v1` default,
`equiv/v2` registered; links of a non-v1 rule are keyed
`<technique>+<rule>`).

## 4. Tests

```bash
cd ~/anti_piracy/fingerprinter
env/bin/python -m pytest -p no:cacheprovider tests/p10 -q          # unit tests only (integration skip)
. scripts/p10-env.sh                                                # crawler2 stack + dev Qdrant
env/bin/python -m pytest -p no:cacheprovider tests/p10 -q          # unit + integration against the real pinned services
P10_EVIDENCE_DIR=benchmarks/p10-index/results/evidence env/bin/python -m pytest -p no:cacheprovider tests/p10 -q
```

`scripts/p10-env.sh` uses keyspace `fingerprinter_it_p10`, Redis prefix
`fpit10:` and buckets `fp-it10-*`; every test wipes that keyspace, uses its
own collection prefix (`index.collection_prefix`) and deletes the Qdrant
collections it created. Several deployments may share one Qdrant instance
only with distinct `FINGERPRINTER_INDEX__COLLECTION_PREFIX` values (design
§20 N-14); the capacity ledger assumes one deployment per instance.

## 5. Benchmarks

| Script | Purpose |
|---|---|
| `benchmarks/p10-index/v1_e2e_baseline.py` | I-1 (a): the V1 worker handler end to end on L-PAIRS |
| `benchmarks/p10-index/tune_eval.py` | I-1 (b–d): nested film-grouped tuning, calibration, lowinfo oracle comparison |
| `benchmarks/p10-index/capacity_e_r1.py` | E-R1: capacity boundary on a dedicated `qdrant-e-r1` container (isolated venv) |
| `benchmarks/p10-index/corpus.py` | L-PAIRS / N-SOAK populations, film folds, Wilson intervals |
| `benchmarks/p10-index/m3_live.py` | I-5: ingest through P9, index, production matcher, nested evaluation on live candidates, `--drop-rebuild` (`--exact-rerun`: every task re-run with brute-force search, harness-only), `--soak [--soak-limit N]`, `--rerun-targets`, `--rerun-all` (dedicated `*_m3` keyspace) |
| `benchmarks/p10-index/ann_recall.py` | §4: segment recall@k, per-query latency, media-level candidate recall in both directions (ANN vs exact, provenance-relevant subjects) |
| `benchmarks/p10-index/direction_b_neighbours.py` | §6/§13.18: exact neighbour counts per target segment at base/10x/19x; floor comparison with 2.1 |
| `benchmarks/p10-index/e_r1_diag.py` | §13.20: which reads make a query slow (rescore × payload at lowered memory caps), dedicated `qdrant-e-r1d` container |
| `benchmarks/p10-index/make_eset.py`, `eset_eval.py`, `p8_set.py` | §8: E-SET (and the disjoint E-TUNE with `--avoid`), offline/live equivalence (configured rule), P8's 6 s set |
| `benchmarks/p10-index/equiv_tune.py` | §13.19: equivalence-only segment parameters tuned on E-TUNE, evaluated once on E-SET |

Exact command lines are in `validation.md` §13. Vector caches, per-pair logs
and per-second samples stay outside git in `~/anti_piracy/datasets/p10/`.

## 6. Modules

| Module | Responsibility (design §) |
|---|---|
| `vindex.py` | `VectorIndex` protocol, `QdrantIndex` REST adapter, point ids, seq-bounded filters, typed errors (§4) |
| `p10db.py` | P10 tables, additive columns (§5) |
| `p10ctx.py` | shared context, subjects, the one liveness predicate, SHA-verified artifact LRU (§4.2, §12.9 C) |
| `generations.py` | generation state machine, reader pointer (static column), promotion batch, build lease (§12.4) |
| `capacity.py` | capacity ledger: reservation before writes, rebuild admission batch, audit (the only lowering path) (§13.3) |
| `indexing.py` | mutation protocol with pre-count, builder / catch-up / validation / manifest digests, verification pass, DRAINING retirement (§12.2–§12.6) |
| `recovery.py` | `index_repairs` occurrences, repair, run bumps, close/resolve, `recovery_open` / `recovery_unresolved`, `settled` (§12.10) |
| `candidates.py` | pinned search step with deadline, adaptive k, per-hit exclusion, grouping, caps, saturation (§7, §11.1) |
| `verify.py` | `temporal/v2`, `equiv/v1` (§8, §10) |
| `confidence.py` | immutable technique versions, calibration status, confidence (§9.1–§9.2) |
| `lowinfo.py` | `lowinfo/v1` records, usable-flag check, refresh, full recomputation — implemented, **not enabled** (§7.3) |
| `matching.py` | run-fenced tasks, due index/queue, run steps, match/equivalence commits via intents, held `match.found` (§6, §9, §10, §11) |
| `discovery.py` | hint reader, `reps` and `targets` backstop scans (§6.1) |
| `p10system.py` | wiring, P10 reconciler passes, `matcher run` loop, `index init/drop`, status (§12.6) |
| `p10cli.py` | operator commands (§14) |

## 7. Operating the matcher

First-time setup of a keyspace that already has the P9 schema and a default spec:

```bash
python -m fingerprinter matcher migrate                          # P10 tables, match.found.v1 registry row (held), ledger
python -m fingerprinter match params-register --version 2.1 --from benchmarks/p10-index/results/tune-eval.json
python -m fingerprinter index init --spec <spec digest>          # retire_guard(spec, 'p10-index')
python -m fingerprinter index rebuild --spec <digest> --role target --promote   # target role first (§12.7)
python -m fingerprinter index rebuild --spec <digest> --role media --promote
```

Service roles: `python -m fingerprinter matcher run` (task workers) and
`python -m fingerprinter matcher reconcile` (hint reader, backstop scans,
intents, verification, generations, repairs, capacity audit; `--once` for one
forced pass). P9's `reconcile run` is not involved. Stopping both stops P10;
P9 is unaffected.

| Command | Purpose |
|---|---|
| `index status` | generations, pointer, entry states (incl. `BLOCKED:capacity`), ledger, recovery counters |
| `index check --spec D --role R [--generation G]` | verification pass + manifest digest vs. the artifact-only digest (exit 1 on a difference or a short subject) |
| `index audit --spec D --role R --generation G` | request a per-subject audit (`audit_state = REQUESTED`) |
| `index activate …` | promote a READY generation (single LWT batch, detection-fenced) |
| `index drop --spec D` | retire and delete every generation, remove intents and the retire guard |
| `match status <subject> --spec D` | `settled` or `provisional` with reasons (exit 1 when provisional) |
| `match summary`, `match show --content C | --target T` | task states, stored decisions, unpublished count |
| `match rerun|retry <subject> --spec D --kind K [--raise-cap N]` | audited run bump (retry of a FAILED task) |
| `match publish-held --version V` | release held decisions — refused unless the technique version is CALIBRATED |
| `equivalence show|revoke` | level-5 links (revocation keeps the evidence) |
| `lowinfo status|refresh --spec D` | classification records (no effect while `matching.lowinfo_enabled = false`) |

## 8. What an operator sees

| Situation | Where |
|---|---|
| no match, settled | `match status` → `settled` (own runs DONE, `recovery_open = 0`, `recovery_unresolved = 0`) |
| incomplete search | task `INCOMPLETE` with `incomplete_reason` (`saturated_segments=n`, `candidate_cap truncated=n`, `verification_failed=n`); `match status` → `provisional (…)`; `fingerprinter_match_incomplete_total` |
| index down / generation missing / capacity | task `WAITING_INDEX` with `waiting_reason` `unavailable` / `missing` / `capacity`; `fingerprinter_match_waiting_index_total`, `fingerprinter_index_unavailable_total` |
| capacity rejection | entry `BLOCKED:capacity`; rebuild returns `CAPACITY_BLOCKED` with the requirement; `fingerprinter_index_capacity_blocked_total` |
| stale / orphan / dead-subject points | `fingerprinter_index_verify_stale_total`, `…_orphans_total`, `…_stale_candidates_total`; candidates `DROPPED` |
| recovery in progress | `index_repairs` rows OPEN/REPAIRED/RERUN_PENDING; `fingerprinter_match_recovery_open`; generation `suspect_since` |
| unresolved recovery | CLOSED rows with `resolution = PENDING`; `fingerprinter_match_recovery_unresolved`; every negative of the spec provisional |
| held publication | `matches.published_at` null; `match summary` → `unpublished`; `fingerprinter_match_unpublished_uncalibrated` |
| subject's own data invalid | task `FAILED` with `failure` (`artifact_corrupt`, `artifact_missing`, `invalid_vector`), error log line |
| rebuild progress / failure | `index status` generation states; `fingerprinter_index_build_progress`; FAILED generation with `failure` |

Structured JSON log lines (`fingerprinter.matching`, `.recovery`, `.indexing`,
`.discovery`) carry subject, kind, spec, run and generation; no media
content or credentials are logged.

## 9. Troubleshooting

- *Tasks stay `WAITING_INDEX(missing)`*: the spec/role has no ACTIVE generation — run `index rebuild … --promote`.
- *`CAPACITY_BLOCKED`*: the projected generation does not fit `max_points − margin`; the limit may only be raised to a value measured by E-R1 (`index.max_points_evidence`).
- *`index check` reports a manifest mismatch*: the generation differs from the artifacts; rebuild (a new generation number), never edit points by hand.
- *`match publish-held` refused*: the technique version is not CALIBRATED (2.1 is UNAVAILABLE, validation §13.3).
