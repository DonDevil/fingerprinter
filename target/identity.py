"""Target identity: the typed record for a known piece of target media.

A `TargetRecord` identifies *what* a target is, independent of where it
currently lives on disk. Per the phase brief, identity must not depend on
the local filename — `content_sha256` (streamed off the file's bytes, never
the path) is what two differently-named files with identical content share,
and is the field embedding-cache compatibility keys off (see
`target/versioning.py`).

`target_version` is a separate, caller-assigned label (matches the
`target_version` field `work_queue.jobs.Job` already carries from Phase 1)
— it is *not* derived from content. Two registrations can share a
`target_version` string only if the caller intends them to; content-based
duplicate detection is a separate operation (`TargetRegistry.
find_by_content_hash`), not something `target_version` equality implies.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Union

DEFAULT_HASH_CHUNK_SIZE = 65536


def sha256_file(path: Union[str, Path], chunk_size: int = DEFAULT_HASH_CHUNK_SIZE) -> str:
    """Stream a file's content through SHA-256. Never reads the filename or
    any filesystem metadata into the hash — only bytes on disk, so identical
    content under different names/paths always hashes identically."""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class TargetRecord:
    """Identity + bookkeeping for one (target_id, target_version) registration.

    Only fields justified by this phase's brief: identity (target_id,
    target_version), content identity (content_sha256), where the bytes
    currently live (media_path — informational, not identity), cheap media
    facts (media_metadata, e.g. ffprobe-style container fields), and
    timestamps.
    """

    target_id: str
    target_version: str
    media_path: str
    content_sha256: str
    media_metadata: Mapping[str, object] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    def to_hash_fields(self) -> dict[str, str]:
        return {
            "target_id": self.target_id,
            "target_version": self.target_version,
            "media_path": self.media_path,
            "content_sha256": self.content_sha256,
            "media_metadata": json.dumps(dict(self.media_metadata), sort_keys=True),
            "created_at": repr(self.created_at),
            "updated_at": repr(self.updated_at),
        }

    @classmethod
    def from_hash_fields(cls, data: Mapping[str, str]) -> "TargetRecord":
        return cls(
            target_id=data["target_id"],
            target_version=data["target_version"],
            media_path=data["media_path"],
            content_sha256=data["content_sha256"],
            media_metadata=json.loads(data.get("media_metadata") or "{}"),
            created_at=float(data["created_at"]),
            updated_at=float(data["updated_at"]),
        )
