# Phase 1 — Redis Job Contract

## Objective

Stand up the minimal Redis coordination surface between a job producer and a
fingerprint worker: `XADD` → stream → `XREADGROUP` claim → job state → `XACK`.
No media handling, no DINOv2, no matching — synthetic jobs only. This phase
proves the contract, not the pipeline.

## Chosen Redis primitives

- **Stream** (`XADD`/`XREADGROUP`/`XACK`) for the job queue, per the
  architecture proposal (§3) — gives atomic claim and a built-in Pending
  Entries List for free, instead of reimplementing claim/lease on top of
  `List`/`BRPOPLPUSH`.
- **Hash** for per-job state (`fingerprint:job:{job_id}:state`).
- **Consumer group** `fingerprinter-workers`, shared by all workers on a
  stream, so `XREADGROUP` fans jobs out without duplicate delivery.

## Job schema

Exactly the nine fields required by the architecture proposal, flattened to
strings for the stream entry (`techniques` joined with `,`, `max_attempts`
stringified):

`job_id, media_evidence_id, media_url, media_type, source_domain, target_id,
target_version, techniques, max_attempts`

`work_queue/jobs.py` defines `Job` (frozen dataclass) plus
`Job.from_stream_fields` / `Job.to_stream_fields` for the round trip.
Validation is structural only (required fields present, `max_attempts` a
positive int, `techniques` non-empty) — no business-rule validation, since
that belongs to later pipeline stages.

## Claim lifecycle

1. Producer (`work_queue/producer.py`) `XADD`s a job. The stream entry *is*
   the job — nothing is written at creation time beyond that.
2. Worker (`worker/fingerprint_worker.py`) calls `claim_one()`, which does
   `XREADGROUP ... COUNT 1 BLOCK <ms> STREAMS <stream> >`. Redis atomically
   assigns the entry to the calling consumer's PEL, so two workers in the
   same group never receive the same entry.
3. On a structurally valid entry: worker upserts state to `claimed`
   (worker_id, attempt, claimed_at) and returns the parsed `Job`.
4. On a malformed entry: state is set to `rejected` (reason recorded) and the
   entry is `XACK`'d immediately — a bad entry can never become valid via
   redelivery, so leaving it pending would just be a permanent PEL leak.
5. `ack(entry)` marks state `completed` and `XACK`s. Result writing,
   retries, `XAUTOCLAIM` stale-reclaim, and delayed-retry promotion are all
   deferred (see below) — this phase only proves the happy-path claim → ack
   loop and the reject-on-malformed path.

## State representation

`fingerprint:job:{job_id}:state` hash, status ∈ `{claimed, completed,
rejected}`. `attempt` increments each time the *same* `job_id` is claimed
(read from prior state, defaulting to 0), matching the proposal's
`attempt+=1` on claim. No `pending`/`created` status exists yet since state
is only written starting at claim time, per §3 of the proposal.

## Tests

`tests/test_producer.py`, `tests/test_worker.py` — 9 tests, all passing,
against a real local Redis on db 15 (flushed before/after each test via the
`redis_client` fixture in `tests/conftest.py`, overridable with
`FINGERPRINTER_TEST_REDIS_URL`):

1. producer creates a valid stream entry
2. consumer group can read a job
3. worker receives the correct job
4. job state becomes `claimed`
5. successful processing ACKs the job
6. acknowledged job is no longer pending (`XPENDING` summary = 0)
7. malformed job is rejected clearly (rejected state + ACKed, not left
   pending)
8. two workers racing `claim_one()` never both get the entry
9. graceful shutdown: `stop()` lets a blocked `run()` loop exit within one
   `block_ms` window, without dropping/acking anything

Run: `.venv/bin/python -m pytest tests/` (needs a local Redis; see
`requirements.txt` / `.venv`).

## Important decisions

- **Package layout mirrors `old/`'s convention** (`work_queue/`, `worker/`
  as top-level packages under the repo root) rather than nesting everything
  under a package literally named `fingerprinter`, which would have
  collided with the repo directory's own name.
- **Malformed jobs are rejected, not retried.** A structural parse failure
  (missing field, non-numeric `max_attempts`) is a producer bug, not a
  transient condition — redelivery cannot fix it, so it's ACKed immediately
  instead of occupying the PEL until a lease timeout.
- **State starts at `claimed`, not `created`.** Matches the proposal
  exactly (§3): the stream entry is authoritative for "does this job exist,"
  the state hash is authoritative for "what's happened to it since a worker
  touched it."
- **Graceful shutdown is bounded by `block_ms`, not instant.** `stop()` sets
  an event checked between blocking `XREADGROUP` calls; there is no
  mid-block interrupt. This is intentional — Redis Streams has no cheap way
  to cancel an in-flight blocking read, and a worker that's actually
  mid-job shouldn't be interrupted anyway.

## Limitations

- Single-priority stream only exercised (`stream_key()` defaults to
  `fingerprint:jobs:stream:default`); multi-priority routing is not tested.
- No lease/heartbeat, no `XAUTOCLAIM`, no dead-worker recovery.
- No retry counting against `max_attempts`, no delayed-retry ZSET.
- No result schema, no results stream.
- No target registry, no locks, no object storage.
- Tests exercise a single process with in-process threads for the
  concurrency check (item 8) and shutdown check (item 9) — no multi-process
  or multi-host testing.

## Deferred work

Everything listed as out-of-scope in the task brief: DINOv2, media
download/ffmpeg, GPU service, target cache, object storage, retry janitor,
delayed retries, fingerprint matching, result consumer, crawler
integration, deployment, monitoring, auth, Redis HA. Also deferred:
rewriting the top-level architecture document (this is a history/phase note,
not a spec update).
