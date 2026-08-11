# Phase 4 — Result Contract

## Objective

Give a completed fingerprint job a durable, typed result: `worker → result
record → result stream → completed job`. Synthetic results only, per the
architecture proposal — no media/DINOv2/pHash/audio, no crawler
integration. This phase proves the result contract, not the pipeline.

## Result schema

Split into two types (`work_queue/results.py`) so a handler only supplies
what it computed, not queue context it doesn't own:

- **`Result`** — the handler's output: `decision`, `algorithm`,
  `processing_started_at`, `processing_completed_at`, optional
  `confidence`, optional `summary`. `processing_duration` is a derived
  property, not stored input.
- **`ResultRecord`** — the durable record: a `Result` plus job-queue
  context assembled by `Worker.commit_result()` — `job_id`,
  `media_evidence_id`, `target_id`, `target_version`, `attempt`,
  `worker_id`, `result_version` (`RESULT_SCHEMA_VERSION = 1`).

`ResultDecision` is a three-way string enum (`match`, `no_match`,
`processing_failure`), same plain-class style as `JobStatus`. No
boolean-only result exists anywhere in this schema — `processing_failure`
is a first-class decision (fingerprinting ran but couldn't reach a
determination — e.g. corrupt media, algorithm error), not folded into
`no_match`.

`algorithm` is a free-form string (synthetic values only here, e.g.
`"synthetic-v0"`) rather than a DINOv2-specific field set, so the schema
doesn't need to change when a later phase adds a second technique — a
handler that combines techniques can report something like
`"dinov2+phash"` without a schema migration.

## Redis storage

Two new keys (`work_queue/keys.py`), consistent with Phase 1-3 naming:

- `fingerprint:job:{job_id}:result` — **Hash**, one durable per-job result
  record (`ResultRecord.to_hash_fields()`), analogous to the existing
  `fingerprint:job:{job_id}:state` hash.
- `fingerprint:results:stream:{priority}` — **Stream**, one event per
  commit, `XADD`ed with `ResultRecord.to_event_fields()`: `job_id`,
  `media_evidence_id`, `target_id`, `attempt`, `decision`,
  `result_version`, `worker_id`, `completed_at`. Deliberately minimal —
  not a copy of the full record — since its only job is letting a
  downstream consumer identify the event and `HGETALL` the durable hash
  for the rest.

`ResultStore.get(job_id)` is the read-only accessor for the hash.

## Terminal result lifecycle / atomicity

Reusing Phase 2's `attempt`-fencing pattern, `Worker.commit_result(entry,
result)` runs one new atomic Lua script, `_COMMIT_RESULT_IF_CURRENT`,
extending the shape of Phase 2/3's CAS scripts:

1. `HGET` the state hash's `attempt`; if it doesn't match what this
   worker claimed at, **return immediately** — no writes, no XACK.
2. `HSET` state hash `status=completed`.
3. `HSET` the result hash (`cjson.decode`d from a JSON `ARGV`, same
   pattern `_PROMOTE_RETRY` already used for dynamic field lists).
4. `XADD` the result-stream event.
5. `XACK` the original stream entry.

Steps 2-5 happen inside one Redis-atomic script execution, so there is no
window where any subset of {result written, state completed, event
emitted, entry ACKed} happens without the rest — the task brief's "result
write / state=completed / XACK" ordering concern is closed by never
splitting it into separate round trips, not by careful sequencing of
separate calls. This is the same technique Phase 3 used for
`_SCHEDULE_RETRY_IF_CURRENT` (state write + ZADD + XACK in one script);
Phase 4 just adds a second HSET and an XADD to the same shape.

`process_claim()` (`worker/fingerprint_worker.py`) is the single dispatch
point: a handler that returns a `Result` gets it committed via
`commit_result()`; a handler that returns `None` still gets a plain
`ack()`, unchanged from Phase 1-3. Existing handlers need no changes.

## Stale-worker fencing

Identical mechanism to Phase 2/3, extended to cover the result write and
the stream event, not just the state hash and XACK: gating the entire
script (including the `HSET` result and `XADD` event) behind the same
`attempt` compare-and-set means a worker that lost ownership via
`XAUTOCLAIM` or a promoted retry can neither:

- write a result for an attempt that's no longer current,
- emit a result-stream event for one, nor
- `XACK` the current owner's still-in-flight PEL entry —

all for the same reason Phase 2 established: gating the `XACK` inside the
CAS closes the "stale worker deletes the new owner's PEL entry by ID"
hole. `commit_result()` returns the result-stream event id on success, or
`None` if the attempt was stale (matching `ack()`'s `bool` convention,
adapted to return the id instead since callers may want it).

## Delivery semantics

**At-least-once**, explicitly chosen, matching Phase 2/3's precedent — no
exactly-once claim anywhere in this contract. The atomic script prevents a
*stale* attempt from emitting a duplicate/conflicting event, but it does
**not** make a single attempt's commit idempotent against itself: nothing
invalidates `attempt` on the state hash after a successful commit, so if a
worker's client retries the same `commit_result()` call (e.g. after an
ambiguous network response), the script's CAS check still passes and a
second result-stream event is emitted (verified by
`test_duplicate_result_events_can_be_identified_safely`). This is the
correct, cheaper trade-off given the task brief's explicit "prefer
at-least-once, do not claim exactly-once" — a downstream consumer dedupes
using `(job_id, attempt)` from the event fields, which is stable and
available without a separate round trip to the result hash.

## Synthetic result flow

`tests/test_results.py`'s handlers build a `Result` directly (no real
fingerprinting) and return it from the handler passed to
`process_claim()`/`run()`. All three decisions (`match`, `no_match`,
`processing_failure`) go through the exact same `commit_result()` path —
`processing_failure` is a result, not a `TransientFailure`/
`PermanentFailure`, so it does not interact with Phase 3's retry
machinery at all; the job reaches `state=completed` with a result
recording that the determination failed, which is itself useful
downstream information distinct from a worker crash or a retry-exhausted
job.

## Tests

`tests/test_results.py`, 11 tests, run against the same local Redis db 15
as Phase 1-3 (`FINGERPRINTER_TEST_REDIS_URL`), `lease_ms=150` for the
crash-recovery-style tests:

1. `test_successful_result_is_persisted`
2. `test_result_stream_event_is_emitted`
3. `test_match_result_is_represented_correctly`
4. `test_no_match_result_is_represented_correctly`
5. `test_processing_failure_result_is_represented_correctly` (also asserts
   the job still reaches `completed`, and that a failure to determine
   omits `confidence` rather than storing a fake value)
6. `test_result_contains_attempt_information` (fails transiently once,
   then succeeds on the promoted retry attempt=2; asserts both the result
   hash and the stream event carry `attempt=2`)
7. `test_stale_attempt_cannot_overwrite_newer_result`
8. `test_stale_attempt_cannot_ack_or_remove_newer_pel_entry` (asserts PEL
   pending-count and owner are untouched by the stale worker's rejected
   commit, before the real owner finalizes)
9. `test_duplicate_result_events_can_be_identified_safely` (two commits
   for the same still-current attempt produce two stream entries sharing
   `(job_id, attempt)` — the dedup key a consumer would use)
10. `test_result_remains_available_after_worker_exits`
11. `test_completed_job_always_has_a_result_under_normal_operation`

Run: `.venv/bin/python -m pytest tests/` — all 35 tests pass (9 Phase 1 +
5 Phase 2 + 10 Phase 3 + 11 Phase 4), stable across repeated runs.

## Limitations

- **Two completion paths still exist.** `ack()` (Phase 1-3, handler
  returns `None`) still marks a job `completed` with no result at all —
  this is intentional backward compatibility (existing tests/handlers are
  unchanged), but it means "every `completed` job has a result" is only
  guaranteed for handlers that opt in by returning a `Result`, not as a
  Redis-enforced invariant across the whole state space. A future phase
  wiring in real fingerprinting should make every real handler return a
  `Result` so this stops being a distinction that matters in practice.
- **No batching/trimming for the result stream.** `XADD` grows
  `fingerprint:results:stream:{priority}` unboundedly, same as the job
  stream in prior phases — no `MAXLEN`, no consumer group on the result
  stream itself (a downstream consumer is expected to use its own
  `XREAD`/consumer-group position, not one this phase defines).
- **No downstream consumer implemented.** This phase proves the producer
  side (worker → result → stream) only; no code reads
  `fingerprint:results:stream:*` other than the tests' raw `XRANGE`.
- **Commit-retry idempotency is a consumer responsibility, not a Redis
  guarantee.** As noted under Delivery semantics, a second
  `commit_result()` call for the same still-current attempt succeeds
  again rather than being rejected as a no-op. This was a deliberate
  choice (matches the at-least-once brief) but means a worker-side retry
  of its own commit call is indistinguishable, from Redis's perspective,
  from an intentional duplicate — both produce a second stream event.
- **`confidence`/`summary` are optional, omitted (not empty-stringed) from
  the hash when absent** — a consumer must check for key presence, not
  assume every result field exists.
- **No dead-letter/query surface** for results beyond a known `job_id`
  (`ResultStore.get`) or raw `XRANGE` on the results stream — same
  limitation Phase 3 noted for the `failed` state.

## Deferred work

Same bucket as Phase 1-3, unchanged: media download, ffmpeg, DINOv2,
pHash, audio fingerprinting, GPU, target management, crawler integration,
object storage, monitoring, production deployment. Also still deferred:
a real downstream result consumer, result-stream trimming/retention
policy, and rewriting the top-level architecture document.
