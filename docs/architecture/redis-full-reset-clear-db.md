# `target.cli clear-db` — full-namespace Redis reset for a fresh run

## Status: COMPLETE

## 1. Problem

The crawler has had a `--clear-db` flag on `main.py` for a while (it wipes
the URL frontier and, when Redis-backed, calls
`RedisURLFrontier.clear()`/`RedisMediaEvidenceStore.clear()` — a
namespace-scoped SCAN+DELETE, not `FLUSHDB`; see the sibling crawler repo's
`docs/architecture/history/clear-db-redis-gap-audit.md` for the incident
that made this rigorous). The fingerprinter had **no equivalent** — nothing
in this repo cleared `fingerprint:*` in bulk. Starting a fresh
crawl+fingerprint run meant registered-target metadata was fine (it's
supposed to persist), but stale job/state/retry/result/match keys from a
previous run could silently poison a new one: an old job's
`fingerprint:job:{id}:state` hash, a stale `fingerprint:matches:target:*`
ZSET entry, or leftover `fingerprint:submission:{id}` markers
(`integration/keys.py`) all live indefinitely in Redis with no TTL on most
of them, and nothing swept them.

## 2. What was added

`work_queue/admin.py::clear_all(redis_client, *, namespace="fingerprint",
include_targets=False)` — a SCAN+DELETE over `{namespace}:*`, the same
pattern the crawler's own `clear()` methods use (never `KEYS`, never
`FLUSHDB`, so it never touches a key outside the fingerprinter's own prefix
even if the Redis db is shared, and never blocks other clients on a large
keyspace).

Every fingerprinter Redis key already lives under one `fingerprint:`
prefix:

| Source | Key shape |
| --- | --- |
| `work_queue/keys.py` | `fingerprint:jobs:stream:{priority}`, `fingerprint:job:{id}:state`, `fingerprint:job:{id}:result`, `fingerprint:retry:delayed:{priority}`, `fingerprint:results:stream:{priority}`, `fingerprint:matches:target:{id}:{version}` |
| `integration/keys.py` | `fingerprint:submission:{id}` |
| `target/keys.py` | `fingerprint:target:{id}:{version}`, `fingerprint:target:content:{sha256}`, `fingerprint:target:index`, `fingerprint:target:{id}:{version}:embeddings`, `fingerprint:target:{id}:{version}:segment_embeddings`, `fingerprint:lock:target:{cache_key}`, `fingerprint:lock:target-record:{id}:{version}` |

That single shared prefix is what makes one SCAN sufficient — no need to
enumerate every module's key shape by hand.

**`clear_all()` excludes `fingerprint:target:*` and `fingerprint:lock:*` by
default.** Registered targets and their built embeddings are reference/
catalog data an operator registers once (`target.cli add` / `build`), not
per-run state — they are not a source of cross-run "poisoning" the way
stale job/result/match state is, and rebuilding them means re-uploading
media and re-running DINOv2 embedding, which is expensive. Pass
`include_targets=True` for a genuine full wipe (e.g. tearing down a test
environment entirely).

Wired into the existing operator CLI as a new subcommand, `target.cli`
(chosen over adding flags to `worker/main.py`: that module is a
long-running, fleet-able daemon with no argparse today, and a per-worker
`--clear-db` would race — or wipe in-flight jobs out from under sibling
workers on a crash-restart. `target.cli` is already this repo's one-shot
operator tool, run by hand before starting the worker fleet, exactly
mirroring how the crawler's own `--clear-db` is a flag on its one-shot
`main.py` invocation):

```
python -m target.cli clear-db [--include-targets] [--json] [--debug]
```

```python
def _cmd_clear_db(context: _Context, args: argparse.Namespace) -> None:
    deleted = clear_all(context.redis_client, include_targets=args.include_targets)
    ...
```

`_Context` (`target/cli.py`) gained a `redis_client` field so this handler
can reach the raw connection — every other subcommand only needed the
`TargetService`/`TargetRegistry` layer above it.

## 3. Usage

```
# Fresh run: clear job/result/retry/match/submission-marker state,
# keep registered targets and their embeddings.
python -m target.cli clear-db

# Full wipe, including registered targets (e.g. tearing down a test env).
python -m target.cli clear-db --include-targets

# Scriptable form.
python -m target.cli clear-db --json
# {"status": "ok", "deleted_keys": 42, "include_targets": false}
```

Run once, by hand, before starting the worker fleet for a new run — never
as part of a supervised/auto-restart worker command line, since that would
wipe in-flight jobs on every crash-restart. Same `REDIS_URL` requirement as
every other `target.cli` subcommand (see that module's docstring): run it
wired to the same Redis the worker fleet uses, or it clears the wrong
database.

## 4. Interaction with the crawler/bridge

The crawler's `evidence:*` namespace and this repo's `fingerprint:*`
namespace are separate prefixes that, in the deployed pipeline, sit in the
same physical Redis instance/db (see the sibling crawler repo's
`bridge/fingerprint_result_consumer.py::_blocking_read_client` docstring).
A full pipeline reset is therefore three independent, prefix-scoped calls,
each safe to run in any order:

- `python main.py --clear-db` (crawler repo) — clears the URL frontier and
  `evidence:*`.
- `python -m target.cli clear-db` (this repo) — clears `fingerprint:*`
  (this doc).
- `bridge.main --clear-db` / `bridge.result_consumer_main --clear-db`
  (crawler repo) — convenience flags added alongside this change that
  clear both `evidence:*` and `fingerprint:*` from the bridge's own
  entrypoints, for operators who run the bridge processes independently of
  the crawler's own CLI. See the crawler repo's
  `docs/architecture/bridge-clear-db.md`.

This repo deliberately has no code that imports or is imported by the
crawler repo (see `docs/architecture/phase-12-crawler-fingerprinter-integration.md`),
so the bridge's `fingerprint:*` sweep is its own small, independent
SCAN+DELETE (`crawler/bridge/redis_reset.py`), not a call into
`clear_all()` — kept in sync by hand (same target/lock exclusion, same
prefix).

## 5. Tests

`tests/test_admin.py` (new): `clear_all()` deletes job/result state,
preserves `target:*`/`lock:*` by default, deletes them with
`include_targets=True`, and is a no-op on an empty namespace.

`tests/test_target_cli.py` (extended):
`test_cli_clear_db_wipes_run_state_but_preserves_targets_by_default` and
`test_cli_clear_db_include_targets_wipes_everything` exercise the
subcommand end-to-end against real local Redis (test db 15, this repo's
existing convention), asserting a previously-registered target survives
the default call and is gone after `--include-targets`.

**Measured, this session:**

```
python -m pytest tests/test_admin.py tests/test_target_cli.py -q
  -> 26 passed
```

## 6. Limitations

- No interactive confirmation prompt, matching the crawler's own
  `--clear-db` (which also clears immediately on the flag alone). The
  safety margin is `--include-targets` being opt-in, not a prompt.
- Does not touch local filesystem caches (`TARGET_CACHE_PATH` /
  `SHARED_ARTIFACT_STORE_PATH` pooled/segment embedding files). Orphaned
  cache entries after `--include-targets` are harmless (content-addressed
  by `content_sha256`, never read once the corresponding Redis metadata is
  gone) but are not reclaimed by this command; that remains a separate,
  unaddressed disk-space concern, not a data-correctness one.
