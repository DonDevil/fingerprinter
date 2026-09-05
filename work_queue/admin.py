"""Full-namespace Redis reset for "start a new run from scratch" (testing
and operator resets only -- never on a hot path).

Every fingerprinter Redis key -- job/state/retry/results/matches
(`work_queue/keys.py`), submission markers (`integration/keys.py`), and
target metadata/content-index/embeddings/locks (`target/keys.py`) -- lives
under one shared `fingerprint:` prefix. That makes a namespace-scoped
SCAN+DELETE sufficient for a full reset, mirroring the sibling crawler
repo's own `RedisURLFrontier.clear()` / `RedisMediaEvidenceStore.clear()`
(`crawler/core/redis_frontier.py`, `crawler/storage/redis_media_evidence_store.py`)
rather than a blanket `FLUSHDB`, which would also wipe any unrelated key
sharing the same Redis database/index.

`clear_all()` defaults to leaving `fingerprint:target:*` and
`fingerprint:lock:*` alone: registered targets and their built embeddings
are reference/catalog data an operator registers once (`target.cli add` /
`build`), not per-run state -- unlike job/result/retry/match state, they
are not a source of "poisoning" between crawl/fingerprint runs, and
rebuilding them (re-uploading media, re-running DINOv2 embedding) is
expensive. Pass `include_targets=True` for a genuine full wipe.
"""
from __future__ import annotations

from redis import Redis

NAMESPACE = "fingerprint"

# Key segments (right after the namespace prefix) left untouched unless
# `include_targets=True` -- see module docstring.
_TARGET_SEGMENTS = ("target:", "lock:")


def clear_all(redis_client: Redis, *, namespace: str = NAMESPACE, include_targets: bool = False) -> int:
    """Delete every key under `{namespace}:*` -- job/jobs/retry/results/
    matches/submission-marker state, and (only if `include_targets=True`)
    registered targets, their content index, embeddings, and locks.

    Returns the number of keys deleted. Uses SCAN (never KEYS/FLUSHDB), the
    same pattern the crawler's own `clear()` methods use, so this is safe to
    run against a large keyspace without blocking other clients.
    """
    pattern = f"{namespace}:*"
    exclude_prefixes = () if include_targets else tuple(f"{namespace}:{segment}" for segment in _TARGET_SEGMENTS)

    deleted = 0
    cursor = 0
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=200)
        if keys:
            if exclude_prefixes:
                keys = [key for key in keys if not key.startswith(exclude_prefixes)]
            if keys:
                deleted += redis_client.delete(*keys)
        if cursor == 0:
            break
    return deleted
