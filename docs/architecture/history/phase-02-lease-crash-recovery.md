# Phase 2 — Redis Lease / Crash Recovery

## Objective

Make fingerprint jobs recoverable when a worker dies after claiming a job
but before completing it, using Redis Streams' Pending Entries List (PEL)
per the architecture proposal (§3): `XREADGROUP` → PEL → idle timeout →
`XAUTOCLAIM` → new worker. Ownership/recovery only — no retries/backoff,
result stream, or pipeline stages (see Deferred, Phase 1).

## Redis PEL / XAUTOCLAIM design

- **Lease = PEL idle time.** No manual heartbeat. Redis tracks time-since-
  delivery per pending entry automatically from `XREADGROUP`/`XAUTOCLAIM`.
- **Stale threshold**: `Worker(lease_ms=...)`, constructor arg, default
  30000. Passed straight through as `XAUTOCLAIM`'s `MIN-IDLE-TIME`.
- **Recovery cadence**: `Worker(reclaim_interval_ms=...)`, default equal to
  `lease_ms`. `run()` calls `_maybe_reclaim_stale()` once per loop
  iteration, gated by a monotonic-clock check against this interval, before
  every blocking `claim_one()` — matches the proposal's "any worker, before
  blocking on new work, runs `XAUTOCLAIM`."
- **Worker identity**: unchanged from Phase 1 — `consumer_name` (defaults
  to `worker-{host}-{pid}-{thread}`) is both the consumer-group identity
  Redis uses for PEL ownership and the `worker_id` written to job state.
- `Worker.reclaim_stale()` is a public method independent of `run()`'s
  cadence gate, so tests (and janitor-style callers) can trigger a
  reclaim pass on demand.

## Ownership semantics / attempt as fencing token

Phase 1's state hash already had an `attempt` field (bumped on every claim,
read-modify-write from prior state). Phase 2 reuses it, unchanged in shape,
as a fencing token — no new state fields:

- `claim_one()` and `reclaim_stale()` both call the same `_next_attempt()`
  and `mark_claimed()` — a reclaim is just another claim of the same
  `job_id`, so `attempt` increments exactly the same way a fresh claim
  would (requirement: attempt count reflects reprocessing).
- `ClaimedEntry` now carries the `attempt` it was claimed at. Completion
  (`ack()`) presents that number back to Redis as its fencing token.

## Stale-worker protection

`ack()` no longer writes state directly. It runs one atomic Lua script
(`_COMPLETE_IF_CURRENT`) that, in a single round trip:

1. `HGET`s the current `attempt` off the state hash.
2. If it doesn't match the attempt the caller claimed at, **returns 0 and
   does nothing else** — no state write, no `XACK`.
3. If it matches, `HSET`s `status=completed` + `completed_at`, then `XACK`s
   the entry, and returns 1.

The critical property this buys: a stale worker's `ack()` doesn't just fail
to overwrite the *state* — it also never reaches the `XACK` call. If it
did, `XACK` would blindly remove the entry from the PEL by entry ID
regardless of current owner, which would delete the new owner's in-flight
lease record; if the new owner then crashed, `XAUTOCLAIM` would never see
that entry again and the job would be lost permanently. Gating the `XACK`
inside the same compare-and-set closes that hole. `ack()` returns `bool` now
(`True`/`False`) instead of `None`; `run()`'s loop doesn't need the result —
losing the race just means the new owner already owns finalization.

Since `XREADGROUP`/`XAUTOCLAIM` assignment and `_next_attempt`/`mark_claimed`
are each single Redis round trips issued sequentially by one worker, and
Redis executes them atomically per-command, at most one consumer holds a
given `job_id`'s current-attempt state at a time — the fencing check only
needs to compare a single integer, not a full owner token.

## Crash-recovery sequence

```
Worker A: XREADGROUP → PEL (attempt=1, owner=A)
Worker A: crashes — no XACK, no further contact
   ... MIN-IDLE-TIME elapses ...
Worker B: XAUTOCLAIM MIN-IDLE-TIME=lease_ms → same entry, PEL owner=B
Worker B: mark_claimed(attempt=2, worker_id=B)   # normal job path from here
Worker B: handler(job) → ack() → attempt(2) == state.attempt(2) → HSET completed + XACK
Worker A: (wakes up) ack(entry@attempt=1) → attempt(1) != state.attempt(2) → no-op, returns False
```

## Delivery semantics

At-least-once, explicitly. A job can execute more than once after a worker
failure (Worker A may have gotten partway through processing before dying).
Nothing in this phase claims exactly-once — Phase 2 only guarantees that
exactly one attempt's completion is allowed to become the durable outcome,
and that the PEL entry isn't lost or double-freed along the way.

## Graceful shutdown

Unchanged from Phase 1: `stop()` only flips an event checked between
blocking `XREADGROUP` calls; a job already claimed and mid-handler is never
touched by shutdown. New in Phase 2: the reclaim pass is also gated by the
same stop check (`run()` re-checks `_stop_event` immediately after
`_maybe_reclaim_stale()`), so shutdown can't kick off a fresh reclaim+handle
cycle it won't be able to finish acking either.

## Tests

`tests/test_crash_recovery.py`, 5 tests, run against the same local Redis
db 15 as Phase 1 (`FINGERPRINTER_TEST_REDIS_URL`), using a 150ms test-only
`lease_ms` so idle-wait sleeps stay short and deterministic:

1. `test_no_premature_reclaim_before_lease_expiry` — reclaim attempted
   immediately after claim finds nothing.
2. `test_crash_recovery_reclaims_and_reprocesses` — the full 10-step
   scenario from the task brief: A claims, "crashes," B reclaims after the
   lease expires, attempt goes 1→2, B completes and ACKs, no pending entry
   remains, and A's subsequent stale `ack()` returns `False` without
   disturbing B's completed state or re-pending the entry.
3. `test_multiple_workers_do_not_repeatedly_steal_a_live_reclaimed_job` —
   after B reclaims, C's immediate reclaim attempt gets nothing (B's
   `XAUTOCLAIM` reset the idle clock).
4. `test_run_loop_reclaims_stale_jobs_through_the_normal_handler_path` —
   `run()` (not just the low-level `reclaim_stale()`) picks up a stale job
   and drives it through the same `handler`/`ack` path as a fresh claim.
5. `test_graceful_shutdown_does_not_ack_unfinished_work` — a claimed,
   unhandled job stays pending (`XPENDING` count 1) across `stop()`.

Run: `.venv/bin/python -m pytest tests/test_crash_recovery.py tests/test_worker.py tests/test_producer.py`
— all 14 tests (9 Phase 1 + 5 Phase 2) pass.

## Limitations

- No heartbeat/self-extend for long-running jobs (proposal §3 mentions
  periodic self-`XCLAIM` as a future option) — a job that legitimately runs
  longer than `lease_ms` will be reclaimed and double-processed. At-least-
  once delivery already requires downstream idempotency for that case, so
  this phase doesn't add one.
- `reclaim_stale()` reclaims onto whichever consumer calls it — no
  dedicated janitor role; every worker doubles as its own janitor per the
  proposal's "any worker process, self-healing, no single instance" note.
- Single fencing dimension (`attempt`, an integer). No explicit
  `owner`/`worker_id` check in the CAS — sufficient here because attempt is
  strictly monotonic per `job_id` and the state hash has one writer per
  attempt, but a future phase with concurrent-completion semantics beyond
  "one owner at a time" would need to revisit this.
- No test for a worker's `reclaim_stale()` racing another worker's
  `reclaim_stale()` on the exact same entry at the Redis level (real
  concurrency, not just a second call after the first already won) — `XAUTOCLAIM`
  is Redis-atomic so this is believed safe, but it isn't exercised here.
- No retry-count-vs-`max_attempts` terminal-failure path yet — this phase
  only proves reclaim increments `attempt`; deciding what happens when
  `attempt >= max_attempts` is deferred with everything else in that
  bucket.

## Deferred work

Same list as Phase 1, unchanged: delayed retries/backoff, result stream,
media download, DINOv2, target cache, GPU, crawler integration, object
storage, monitoring, Redis HA.
