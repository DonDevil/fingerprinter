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


def target_embeddings_key(target_id: str, target_version: str) -> str:
    return f"fingerprint:target:{target_id}:{target_version}:embeddings"


def target_segment_embeddings_key(target_id: str, target_version: str) -> str:
    return f"fingerprint:target:{target_id}:{target_version}:segment_embeddings"


def encode_content_index_member(target_id: str, target_version: str) -> str:
    return f"{target_id}{_CONTENT_INDEX_MEMBER_SEP}{target_version}"


def decode_content_index_member(member: str) -> tuple[str, str]:
    target_id, target_version = member.split(_CONTENT_INDEX_MEMBER_SEP, 1)
    return target_id, target_version
