"""Phase 13D — shared artifact storage.

The Phase 13D audit (`docs/architecture/phase-13d-multi-host-target-cache-
audit.md`) found that `FilesystemEmbeddingCache`/`FilesystemSegmentEmbeddingCache`
(`target/cache.py`, `target/segment_cache.py`) store embedding vectors on
whichever host's local disk happens to build them, so `RedisLock`
(`target/lock.py`) correctly serializes *who builds* fleet-wide but the
artifact it protects is invisible to every other host — a losing host's
poll loop can never observe a winning host's result (audit §4/§5). A second,
adjacent gap (audit §3.5): `TargetRecord.media_path` is likewise host-local,
so even a fixed embedding cache would not let a losing host build the
target at all if the raw media never reaches it.

This module is the storage boundary both gaps are fixed through: a generic,
content-addressed blob store (`SharedArtifactStore`) backed by a directory
that MUST be a genuinely shared mount across every host in the fleet (NFS,
a cluster filesystem, or equivalent) — the audit's Option A1, chosen over
Option B (Redis-backed embedding storage: would put an extrapolated ~5 GiB
of vector data into the same Redis instance job/lock coordination already
depends on, per audit §10/§11) and over Option A2 (S3-compatible object
storage: no client library is a dependency of this project today, and the
audit explicitly says not to invent cloud endpoints/credentials that don't
exist). See `docs/architecture/phase-13d-distributed-target-artifacts.md`
for the full architecture writeup.

This is deliberately the *smallest* backend abstraction that lets the fix
proceed without inventing infrastructure this repository has no evidence
of: a plain directory, written with the same tempfile + `os.replace` atomic
pattern the existing per-host caches already use (`target/cache.py`'s
`_atomic_write`), just pointed at shared storage instead of local disk.
Swapping in a real object-storage backend later means implementing this
same three-method interface again, not redesigning any caller.

Failure semantics (audit §11): a read/write that fails because the shared
mount is unreachable raises `SharedArtifactStoreError` — it is never
silently treated as "not found" (which would look like a cache miss) and
this module never falls back to a local-only directory. A cache miss (key
absent) is a `None`/`False` return; storage being unreachable is an
exception. Callers (see `worker/matching_handler.py`) map that exception
onto `TransientFailure`, retried through the existing job retry machinery
-- no new retry system.

Deterministic addressing (audit §8): every key this module is ever given
is derived from `target.versioning.cache_entry_key()` or a target's
`content_sha256` -- both pure functions of target/spec identity, with no
hostname, PID, worker ID, or local timestamp. Two hosts computing a key
for the same logical artifact always agree, which is what makes sharing a
directory across hosts by key alone correct.
"""
from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Optional, Union


class SharedArtifactStoreError(OSError):
    """The shared artifact store could not be read from or written to --
    e.g. the shared mount is unreachable, full, or unwritable. Distinct
    from a normal miss (`None`/`False`): a caller must not treat this as
    "not cached," only as "cache unavailable right now." See module
    docstring, "Failure semantics"."""


class SharedArtifactStore:
    """Content-addressed blob store, one file per key, under a shared root
    directory. `key` may contain `/` for namespacing (e.g.
    `"pooled/<cache_entry_key>.json"`, `"media/<content_sha256>"`) and is
    otherwise opaque to this class -- callers own key derivation.

    Construction fails fast (`SharedArtifactStoreError`) if the root can't
    be created/accessed, rather than silently proceeding against a
    directory that might just be local disk masquerading as shared storage
    -- this module cannot verify the mount is genuinely shared (that's an
    operator/deployment fact, see the Phase 13D doc's "Configuration"
    section), only that it is at least reachable at startup.
    """

    def __init__(self, root: Union[str, Path]):
        self._root = Path(root)
        try:
            self._root.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise SharedArtifactStoreError(
                f"cannot access shared artifact store root {self._root}: {exc}"
            ) from exc

    @property
    def root(self) -> Path:
        return self._root

    def _path_for(self, key: str) -> Path:
        path = self._root / key
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise SharedArtifactStoreError(
                f"cannot create shared artifact store namespace for {key!r}: {exc}"
            ) from exc
        return path

    def exists(self, key: str) -> bool:
        return self._path_for(key).exists()

    def delete(self, key: str) -> bool:
        """Remove the blob at `key` if present. True iff something was
        deleted. Raises SharedArtifactStoreError on an unreachable/
        unwritable store, same failure semantics as get_bytes/put_bytes --
        never conflates "absent" with "store unreachable"."""
        path = self._path_for(key)
        if not path.exists():
            return False
        try:
            path.unlink()
            return True
        except OSError as exc:
            raise SharedArtifactStoreError(f"failed to delete {key!r} from shared artifact store: {exc}") from exc

    def get_bytes(self, key: str) -> Optional[bytes]:
        """`None` iff the key is absent. Raises `SharedArtifactStoreError`
        if the key exists but cannot be read (unreachable mount, permission
        error, etc.) -- never conflated with "absent"."""
        path = self._path_for(key)
        if not path.exists():
            return None
        try:
            return path.read_bytes()
        except OSError as exc:
            raise SharedArtifactStoreError(f"failed to read {key!r} from shared artifact store: {exc}") from exc

    def put_bytes(self, key: str, data: bytes) -> None:
        """Atomic write (tempfile in the same directory + `os.replace`) --
        a concurrent reader never observes a partially written entry.
        Idempotent: writing the same bytes twice under the same key is
        safe, the second write just replaces the first with identical
        content."""
        path = self._path_for(key)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-", suffix=".part")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            os.replace(tmp, path)
        except OSError as exc:
            Path(tmp).unlink(missing_ok=True)
            raise SharedArtifactStoreError(f"failed to write {key!r} to shared artifact store: {exc}") from exc
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def put_file(self, key: str, src_path: Union[str, Path]) -> None:
        """Same atomicity/idempotency guarantee as `put_bytes`, streamed
        rather than buffered fully in memory -- for larger artifacts (target
        media) where reading the whole file into a `bytes` object first is
        wasteful."""
        path = self._path_for(key)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-", suffix=".part")
        try:
            with os.fdopen(fd, "wb") as out_f, open(src_path, "rb") as in_f:
                shutil.copyfileobj(in_f, out_f)
            os.replace(tmp, path)
        except OSError as exc:
            Path(tmp).unlink(missing_ok=True)
            raise SharedArtifactStoreError(f"failed to write {key!r} to shared artifact store: {exc}") from exc
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def get_file(self, key: str, dest_path: Union[str, Path]) -> bool:
        """Stream a copy of `key` to `dest_path` (a plain, non-atomic local
        write -- `dest_path` is the caller's own private scratch location,
        not a shared path other readers observe, so only the *source* read
        needs the store's atomicity guarantee, which `put_bytes`/`put_file`
        already provide). Returns `False` iff the key is absent; raises
        `SharedArtifactStoreError` on an unreachable/unreadable store."""
        path = self._path_for(key)
        if not path.exists():
            return False
        try:
            dest = Path(dest_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "rb") as in_f, open(dest, "wb") as out_f:
                shutil.copyfileobj(in_f, out_f)
            return True
        except OSError as exc:
            raise SharedArtifactStoreError(f"failed to read {key!r} from shared artifact store: {exc}") from exc


class SharedTargetMediaStore:
    """Fleet-accessible target media (audit §3.5): raw target bytes,
    content-addressed by `content_sha256` (the same field
    `target.versioning.cache_entry_key()` already keys embeddings on), in
    the *same* `SharedArtifactStore` the embedding caches use -- Option 1
    from the Phase 13D brief ("target media itself is stored in the same
    shared artifact/object storage"), chosen over a target `MediaAcquirer`
    analog because no target URL/acquisition source exists anywhere in this
    codebase to acquire from (`register_target` has no production call
    site -- audit §3.5 -- so there is nothing safe to invent a downloader
    against).

    `publish` is called by whichever host runs target registration
    (`TargetRegistry.register_target`, see its docstring); `fetch_to_temp`
    is called by a build-on-miss winner that finds `record.media_path`
    absent on its own disk (`worker/matching_handler.py`'s
    `_target_artifact`)."""

    _PREFIX = "media"

    def __init__(self, store: SharedArtifactStore):
        self._store = store

    def _key(self, content_sha256: str) -> str:
        return f"{self._PREFIX}/{content_sha256}"

    def publish(self, content_sha256: str, local_media_path: Union[str, Path]) -> None:
        """Push a target's media bytes into shared storage, addressed by
        the same content hash used for cache-identity/invalidation.
        Idempotent -- republishing identical bytes under the same hash is
        a safe no-op write."""
        self._store.put_file(self._key(content_sha256), local_media_path)

    def delete(self, content_sha256: str) -> bool:
        """Remove the published media for `content_sha256` if present. True
        iff something was deleted. Callers (TargetRegistry.delete_target)
        must confirm no other (target_id, target_version) still references
        this hash (via find_by_content_hash) before calling this -- this
        method itself has no reference-counting logic; it deletes
        unconditionally whatever is at this content hash's key."""
        return self._store.delete(self._key(content_sha256))

    def fetch_to_temp(self, content_sha256: str, suffix: str = "") -> Optional[Path]:
        """Copy the media for `content_sha256` into a fresh local temp
        file and return its path, or `None` if it was never published.
        Caller owns cleanup (mirrors `acquisition.artifact.MediaArtifact
        .cleanup()`'s "caller owns lifetime" contract)."""
        fd, tmp = tempfile.mkstemp(prefix="target-media-", suffix=suffix)
        os.close(fd)
        tmp_path = Path(tmp)
        found = self._store.get_file(self._key(content_sha256), tmp_path)
        if not found:
            tmp_path.unlink(missing_ok=True)
            return None
        return tmp_path
