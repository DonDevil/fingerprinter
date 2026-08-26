"""Redis key conventions for target metadata.

Explicit `fingerprint:target:*` namespace, kept separate from the
`fingerprint:job:*` / `fingerprint:jobs:*` / `fingerprint:results:*`
namespaces Phases 1-4 already use. Only small metadata lives under these
keys — never embedding vectors (see `target/cache.py`'s module docstring
for the storage boundary this enforces).
"""

_CONTENT_INDEX_MEMBER_SEP = "\x1f"  # unit separator: avoids collision with ids/versions containing ':'


def target_key(target_id: str, target_version: str) -> str:
    return f"fingerprint:target:{target_id}:{target_version}"


def target_content_index_key(content_sha256: str) -> str:
    return f"fingerprint:target:content:{content_sha256}"


def target_index_key() -> str:
    """The list-all-targets Redis Set (target-management design doc, S8).
    A single, unparameterized key -- one Set holding every registered
    (target_id, target_version) pair, member-encoded the same way as
    `target_content_index_key()` (via `encode_content_index_member`), so
    `TargetRegistry.list_targets()` is O(number of registered targets),
    never a keyspace SCAN."""
    return "fingerprint:target:index"


def target_embeddings_key(target_id: str, target_version: str) -> str:
    return f"fingerprint:target:{target_id}:{target_version}:embeddings"


def target_segment_embeddings_key(target_id: str, target_version: str) -> str:
    return f"fingerprint:target:{target_id}:{target_version}:segment_embeddings"


def target_lock_key(cache_key: str) -> str:
    """Namespace for `target/lock.py`'s build-on-miss lock, matching
    `docs/design/design-proposal-1.md` §8's `fingerprint:lock:target:{cache_key}`
    convention. `cache_key` is `target.versioning.cache_entry_key(...)` —
    the full (target_id, target_version, content_sha256, spec) identity, so
    the lock is scoped to one exact representation, not the whole target."""
    return f"fingerprint:lock:target:{cache_key}"


def target_record_lock_key(target_id: str, target_version: str) -> str:
    """Namespace for the target-lifecycle `RedisLock` (target-management
    design doc, S9): guards every mutation of one (target_id, target_version)
    identity — register, metadata update, delete. Deliberately a distinct
    key shape from `target_lock_key()` above (which scopes a *build-on-miss*
    lock to a full `cache_entry_key`, one exact embedding representation) so
    a lifecycle-mutation lock and a build-on-miss lock for the same target
    can never collide or be confused for one another."""
    return f"fingerprint:lock:target-record:{target_id}:{target_version}"


def encode_content_index_member(target_id: str, target_version: str) -> str:
    return f"{target_id}{_CONTENT_INDEX_MEMBER_SEP}{target_version}"


def decode_content_index_member(member: str) -> tuple[str, str]:
    target_id, target_version = member.split(_CONTENT_INDEX_MEMBER_SEP, 1)
    return target_id, target_version
