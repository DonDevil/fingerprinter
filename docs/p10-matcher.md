# P10 vector index and matcher (`fingerprinter/`, phase P10)

Local guide for developing, testing and running the P10 matcher: the
derived vector index (Qdrant), matching in both directions with ANN
candidate reduction and temporal verification, index generations, the
mutation/recovery protocol and the P10 reconciler. The design of record and
all evidence are in [`phases/p10-vector-index-matcher/`](phases/p10-vector-index-matcher/README.md);
executed results are only in its `validation.md` §13.

**Status (2026-10-18): implementation in progress; M3 not passed.**

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
250,000 without `index.max_points_evidence`, `drain_grace_s` below
`2 × active_ttl_s + task_lease_s`, `k0 > k_max`).

## 4. Tests

```bash
cd ~/anti_piracy/fingerprinter
env/bin/python -m pytest -p no:cacheprovider tests/p10 -q          # unit tests only (integration skip)
. scripts/p10-env.sh                                                # crawler2 stack + dev Qdrant
env/bin/python -m pytest -p no:cacheprovider tests/p10 -q          # unit + integration against the real pinned services
P10_EVIDENCE_DIR=benchmarks/p10-index/results/evidence env/bin/python -m pytest -p no:cacheprovider tests/p10 -q
```

`scripts/p10-env.sh` uses keyspace `fingerprinter_it_p10`, Redis prefix
`fpit10:` and buckets `fp-it10-*`; every test wipes that keyspace and
deletes the Qdrant collections it created.

## 5. Benchmarks

| Script | Purpose |
|---|---|
| `benchmarks/p10-index/v1_e2e_baseline.py` | I-1 (a): the V1 worker handler end to end on L-PAIRS |
| `benchmarks/p10-index/tune_eval.py` | I-1 (b–d): nested film-grouped tuning, calibration, lowinfo oracle comparison |
| `benchmarks/p10-index/capacity_e_r1.py` | E-R1: capacity boundary on a dedicated `qdrant-e-r1` container (isolated venv) |
| `benchmarks/p10-index/corpus.py` | L-PAIRS / N-SOAK populations, film folds, Wilson intervals |

Exact command lines are in `validation.md` §13. Vector caches, per-pair logs
and per-second samples stay outside git in `~/anti_piracy/datasets/p10/`.
