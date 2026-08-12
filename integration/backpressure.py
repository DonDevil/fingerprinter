"""Admission-control backpressure — phase brief, "Backpressure" (mandatory).

The crawler can discover candidates far faster than CPU-bound DINOv2
inference can process them (Phase 11 measured ~1.03 jobs/s aggregate
throughput for 4 CPU worker processes on one 6-core host, correctly
thread-pinned — phase-11 doc §19a/§23). Without a bound, unbounded crawler
discovery would grow the job stream (and the worker fleet's backlog)
without limit. Per the brief: "do not hard-code an arbitrary queue limit
without documenting why" — see `DEFAULT_MAX_OUTSTANDING_JOBS` below.

`count_outstanding` measures "outstanding" as *not-yet-claimed backlog*
(`lag`) plus *claimed-but-not-yet-acked* in-flight work (`pending`), read
from `XINFO GROUPS` — the same consumer-group Redis already tracks for
`worker.fingerprint_worker.Worker.reclaim_stale()`'s `XAUTOCLAIM` call, so
this adds no new bookkeeping structure, only a read of what Redis Streams
already computes.
"""
from __future__ import annotations

from typing import Optional

from redis import Redis
from redis.exceptions import ResponseError

from work_queue.keys import CONSUMER_GROUP, stream_key

# PROVISIONAL, not load-tested. Justification: Phase 11 measured a single
# CPU worker process at ~1.08 jobs/s (warm cache, 6 torch threads,
# phase-11 doc §19b, worker_count=1) and confirmed multi-process CPU
# scaling only works with per-process thread pinning (§19a/§23), reaching
# ~1.035 jobs/s combined *throughput headroom* at worker_count=4 on this
# 6-core dev machine before the RAM safety gate stopped further scaling.
# Treating "~1 job/s per host, a handful of hosts" as a rough steady-state
# floor, 500 outstanding jobs bounds worst-case backlog-clearing latency to
# roughly 500-1000s (8-17 minutes) at that floor — large enough to absorb
# a realistic crawl burst without stalling crawler admission on every
# single job, small enough that a stuck/slow fingerprinter fleet doesn't
# let the backlog grow indefinitely. This has no labeled SLA behind it
# (none exists yet in this project) and must be retuned once real
# multi-host throughput and an actual backlog-latency target exist —
# REQUIRES MULTI-HOST VALIDATION, see phase-12 doc.
DEFAULT_MAX_OUTSTANDING_JOBS = 500


def _ensure_group(redis_client: Redis, stream: str) -> None:
    """Idempotent consumer-group creation, mirroring
    `worker.fingerprint_worker.Worker._ensure_group`. `XINFO GROUPS` only
    reports a group that exists — without this, a submitter running before
    any worker process has ever started would see no group at all and
    silently report zero backlog forever, defeating the backpressure bound
    this module exists to enforce. Creating the group early is harmless:
    it is the exact same idempotent call every `Worker` already makes at
    construction time, just made by whichever side (submitter or worker)
    happens to run first."""
    try:
        redis_client.xgroup_create(stream, CONSUMER_GROUP, id="0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def count_outstanding(redis_client: Redis, priority: str) -> Optional[int]:
    """`lag + pending` for the fingerprint consumer group on `priority`'s
    stream. Returns `0` if the group has no backlog. Returns `None` only
    if Redis cannot report `lag` for the group at all (per the Redis
    Streams docs, `lag` can be `None` if the group's counters were made
    inconsistent by an `XDEL`/`XTRIM` — no such operation exists anywhere
    in this codebase today, so this is not expected to trigger in
    practice, but the caller must still handle it — see
    `FingerprintJobSubmitter.submit`).
    """
    stream = stream_key(priority)
    _ensure_group(redis_client, stream)
    groups = redis_client.xinfo_groups(stream)

    for group in groups:
        if group.get("name") != CONSUMER_GROUP:
            continue
        lag = group.get("lag")
        pending = group.get("pending") or 0
        if lag is None:
            return None
        return int(lag) + int(pending)
    return 0
