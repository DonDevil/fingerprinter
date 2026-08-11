# Phase 3 — Retry / Backoff Contract

## Objective

Give fingerprint jobs a retry contract: a handler-reported transient
failure gets a delayed retry with exponential backoff; a permanent failure
(or a transient one that has exhausted `max_attempts`) becomes a terminal
`failed` state. Synthetic jobs only, per the architecture proposal's
Stream + delayed-retry-ZSET model (§2, §3, §7).

## Retry state machine

```
claim (attempt N)
   |
   v
handler(job)
   |
   +-- returns normally ---------------------> completed + XACK
   |
   +-- raises TransientFailure
   |       |
   |       +-- N >= max_attempts ------------> failed + XACK  (terminal)
   |       |
   |       +-- N <  max_attempts
   |              |
   |              v
   |         retry_scheduled: ZADD retry ZSET (score = due_at) + XACK
   |              |
   |              v (promote_due_retries, once due_at <= now)
   |         XADD onto the stream (new entry, same job_id)
   |              |
   |              v
   |         claim (attempt N+1) --- loops back to "handler(job)"
   |
   +-- raises PermanentFailure -----------------------------> failed + XACK
   |
   +-- raises anything else --------------------------------> not caught here;
           entry stays pending in the PEL, indistinguishable from a
           worker crash. Phase 2's XAUTOCLAIM recovery handles it —
           see "Failure classification" below.
```

`claimed` / `completed` / `rejected` are unchanged from Phase 1. Two new
terminal-ish statuses: `retry_scheduled` (between XACK of the failed
attempt and the retry becoming claimable again) and `failed`.

## Transient / permanent semantics

Two exceptions in `worker/fingerprint_worker.py`, re-exported from
`worker/__init__.py`:

- `TransientFailure` — handler raises this for a retryable condition.
  Schedules a retry (subject to `max_attempts`).
- `PermanentFailure` — handler raises this for a non-retryable condition.
  Always terminal, `max_attempts` is irrelevant.

`Worker.process_claim(entry, handler)` is the single dispatch point: calls
`handler(entry.job)`, and only these two exception types are caught. Any
other exception is **not** caught — it propagates out of `process_claim`
and the entry is simply never finalized, so it stays in the PEL exactly as
if the worker process had crashed. This deliberately reuses [Phase 2's
crash recovery](phase-02-lease-crash-recovery.md) instead of building a
third "unknown failure" code path: an unclassified handler bug and an
actual process crash get identical treatment (`XAUTOCLAIM` reclaims it
after `lease_ms`, `attempt` increments, it's retried like any other
reclaim). `process_claim` is also what the stale-reclaim path
(`_maybe_reclaim_stale`) calls, so a reclaimed job goes through the exact
same success/transient/permanent handling as a fresh claim.

## `max_attempts` meaning

**`max_attempts` is the maximum number of total processing attempts,
including the first — not the maximum number of retries.** This is the
interpretation already implied by Phase 1/2: `attempt` starts at `1` on
the very first claim (`state["attempt"] == "1"` after `claim_one()`, per
Phase 1's own test) and increments on every subsequent claim, including a
Phase 2 crash-reclaim. Phase 3 keeps that counter as the sole source of
truth and compares it directly:

```python
if entry.attempt >= job.max_attempts:
    # terminal failure — no more retries
```

So `max_attempts=3` means attempts `1, 2, 3` are made (one initial +
two retries) and a transient failure on attempt 3 is terminal. This
required no schema change — `Job.max_attempts` (Phase 1) and the `attempt`
state field (Phase 1/2) already meant this; Phase 3 just enforces it at
the retry-scheduling decision point.

## Backoff formula

Configurable exponential backoff, `Worker(retry_base_delay_s=1.0,
retry_max_delay_s=60.0)`:

```
delay(attempt) = min(base_delay * 2 ** (attempt - 1), max_delay)
```

`attempt` is the attempt number that just failed. So with the defaults, a
failure on attempt 1 schedules a retry ~1s later, attempt 2 ~2s later,
attempt 3 ~4s later, capped at 60s. No jitter — not required by the
architecture proposal for this phase, and deterministic delays are what
the tests need.

## Redis keys

One new key, matching the proposal's `fingerprint:retry:delayed` naming,
extended with the same per-priority segment `stream_key()` already uses:

```
fingerprint:retry:delayed:{priority}   ZSET
    score  = due_at (unix seconds, float)
    member = JSON-encoded job stream fields (Job.to_stream_fields())
```

`work_queue/keys.py`: `retry_zset_key(priority)`. No changes to
`Job`/`REQUIRED_FIELDS` or to the state hash's field *names* — Phase 3
only adds new values (`retry_scheduled`, `failed`) to the existing
`status` field, plus a few new hash fields (`last_failure`,
`scheduled_at`, `next_attempt_at`, `failure_reason`, `failed_at`) that are
just informational, not part of the fencing/claim contract.

## Failure → retry transition (crash safety)

The scheduling step — state hash write, ZADD into the retry ZSET, and
XACK of the original stream entry — runs as one atomic Lua script
(`_SCHEDULE_RETRY_IF_CURRENT`), the same shape as Phase 2's
`_COMPLETE_IF_CURRENT`:

1. `HGET` the state hash's `attempt`; if it doesn't match the attempt this
   worker claimed at, stop — stale worker, no-op (see "Stale-worker
   fencing" below).
2. `HSET` status → `retry_scheduled` + bookkeeping fields.
3. `ZADD` the retry ZSET.
4. `XACK` the original entry.

Because 2–4 happen inside one Redis-atomic script execution, there is no
window where the job has left the stream/PEL (via XACK) but isn't yet in
the retry ZSET, or vice versa — a crash either happens strictly before the
script runs (job stays in the PEL, Phase 2 recovery reclaims it normally —
no retry-ZSET involvement at all, which is fine) or strictly after (job is
fully in the retry ZSET and off the PEL). There's no partial state to land
in. Same script shape is reused for terminal failure
(`_FAIL_IF_CURRENT`): state write + XACK, gated by the same fencing check.

## Retry → stream transition (crash safety)

`Worker.promote_due_retries()` is the explicit method the brief asked for
— the worker's `run()` loop calls it once per iteration, and it's also
callable directly (used heavily by the tests). For each ZSET member with
`score <= now` (fetched via `ZRANGEBYSCORE ... LIMIT`), it runs
`_PROMOTE_RETRY`:

```lua
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed == 0 then return false end
local fields = cjson.decode(ARGV[1])
-- ... build XADD args from fields ...
return redis.call(unpack(args))
```

`ZREM` and `XADD` happen inside one script execution, so — same argument as
above — there's no gap between "left the retry ZSET" and "back on the
stream." This also makes promotion **idempotent per member**: `ZREM`
returns `0` if another call already removed it (a racing promoter, or a
duplicate call in the same process), and the script does nothing further
in that case — no second `XADD`, no duplicate stream entry. A promoter
"restart" is a non-event: all retry state lives in the ZSET in Redis, not
in any promoter's memory, so a brand-new `Worker` instance calling
`promote_due_retries()` picks up exactly what's still due, same as the
original one would have. (This mirrors Phase 2's observation that every
worker doubles as its own janitor — here, every worker doubles as its own
retry promoter too, no dedicated process.)

## Stale-worker fencing after retry

Retry reuses the *existing* `attempt` fencing mechanism unchanged — no new
mechanism was needed. A promoted retry is claimed like any other entry:
`claim_one()` bumps `attempt` via the same `_next_attempt()`/
`mark_claimed()` path Phase 2 already used for crash-reclaims. So if
worker A claims attempt 1, fails transiently (attempt 1 recorded), and the
retry later gets promoted and claimed by worker B as attempt 2, worker A's
old `ClaimedEntry` (still carrying `attempt=1`) is stale by the same
definition Phase 2 established. Both `ack()` and the new
`_SCHEDULE_RETRY_IF_CURRENT`/`_FAIL_IF_CURRENT` scripts check
`entry.attempt` against the hash's current `attempt` before doing
anything — so worker A can neither complete, reschedule, nor fail the job
out from under worker B once ownership has moved on via a retry, exactly
as it can't after an XAUTOCLAIM reclaim.

## Tests

`tests/test_retry.py`, 10 tests, run against the same local Redis db 15 as
Phase 1/2, with `retry_base_delay_s=0.1` / `retry_max_delay_s=0.3` so
due-time waits stay under ~0.5s total per test:

1. `test_transient_failure_schedules_retry`
2. `test_retry_not_promoted_before_due_time`
3. `test_retry_promoted_after_due_time_and_can_be_claimed_again` (covers
   both "promoted after due" and "can be claimed again," and asserts
   `attempt == 2`)
4. `test_transient_failure_then_retry_then_success` (full
   transient → retry → success cycle)
5. `test_permanent_failure_is_terminal_no_retry_scheduled`
6. `test_max_attempts_prevents_further_retries`
7. `test_backoff_increases_and_caps_at_max_delay` (observes
   `next_attempt_at - scheduled_at` across three real failures: 0.1s →
   0.2s → capped 0.3s)
8. `test_duplicate_promotion_does_not_create_duplicate_retry_work`
9. `test_promoter_restart_does_not_lose_scheduled_retry`
10. `test_stale_worker_fencing_still_works_after_retry`

Run: `.venv/bin/python -m pytest tests/test_retry.py tests/test_crash_recovery.py tests/test_worker.py tests/test_producer.py`
— all 24 tests (9 Phase 1 + 5 Phase 2 + 10 Phase 3) pass, stable across
repeated runs.

## Limitations

- No jitter — concurrent retries with identical backoff parameters become
  due at the same instant. Not required for this phase; worth revisiting
  once there's a real multi-worker fleet where synchronized retry storms
  matter.
- `promote_due_retries()` has no dedicated cadence/interval gate (unlike
  `_maybe_reclaim_stale`'s `reclaim_interval_ms`) — `run()` calls it every
  loop iteration. Cheap (`ZRANGEBYSCORE` + LIMIT) but not tuned; a
  dedicated interval could be added the same way Phase 2 added one for
  reclaim, if profiling ever shows it matters.
- No dead-letter tooling/listing for terminally `failed` jobs — the state
  hash records the failure, but there's no query surface beyond
  `HGETALL fingerprint:job:{job_id}:state` for a known `job_id`.
- `promote_due_retries()`'s `ZRANGEBYSCORE` + per-member script is O(due
  count) round trips, not a single batched Lua call — fine at synthetic-job
  scale, would want batching for a high-throughput retry ZSET.
- No test exercises true concurrent promotion (two threads/processes
  calling `promote_due_retries()` at the same instant on the same due
  member) — the duplicate-promotion test calls it twice sequentially,
  which exercises the same `ZREM`-returns-0 code path but isn't a real
  race. `ZREM` is Redis-atomic so this is believed safe, not proven under
  real concurrency here.
- Retry payload is the job's own fields re-serialized (`to_stream_fields()`
  JSON-encoded) — if `Job`'s schema changes in a way that isn't a pure
  round-trip through `to_stream_fields()`/`from_stream_fields()`, the retry
  path would need to change too. Not an issue today since Phase 3 made no
  `Job` schema changes.

## Deferred work

Same bucket as Phase 1/2, unchanged: media download, ffmpeg, DINOv2, GPU,
target cache, result stream, crawler integration, object storage,
monitoring, Redis HA, production deployment. Also still deferred:
HTTP/media-specific failure classification (only generic
transient/permanent exist so far, per this phase's explicit scope).
