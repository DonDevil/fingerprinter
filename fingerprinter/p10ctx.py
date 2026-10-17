"""Shared P10 context: subjects, the one liveness predicate, verified artifacts (design §4.2, §12.9 C).

A *subject* is a content representation (media role) or a target
representation (target role) of one spec. Its authoritative facts are P9
rows, read here and never written:

- media: ``representations(content_id, spec)``; live iff ``state = 'READY'``;
- target: ``target_representations(tid, ver, spec)`` x ``targets(tid, ver)``;
  live iff the binding is ``READY`` **and** the target is not ``RETIRED`` or
  ``PURGED`` (corrected C-14j: one predicate everywhere; a READY binding of a
  target still ``REGISTERING`` or ``BUILD_FAILED`` counts as live).

Liveness is monotone (design §4.2), which every caller may rely on but none
needs for safety. Artifacts are read through ``Objects.read_verified`` (size
and SHA-256 checked on every read; ``IntegrityError`` otherwise) and decoded
into ``Vectors``; a bounded LRU keyed by SHA-256 avoids re-reading — a cache,
never authoritative.
"""

from __future__ import annotations

import json
import threading
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from fingerprinter.artifact import load as load_artifact
from fingerprinter.config import Settings
from fingerprinter.coord import Clock, Coord
from fingerprinter.db import SERIAL, Db
from fingerprinter.objects import Objects, parse_uri
from fingerprinter.store import Store, shard_of
from fingerprinter.verify import Vectors
from fingerprinter.vindex import MEDIA, TARGET, VectorIndex

BUCKETS = 64
TARGET_DEAD_STATES = ("RETIRED", "PURGED")
KIND_A, KIND_B, KIND_EQ = "media_vs_targets", "target_vs_media", "media_vs_media"
OPPOSITE = {KIND_A: TARGET, KIND_B: MEDIA, KIND_EQ: MEDIA}
ROLE_OF_KIND = {KIND_A: MEDIA, KIND_B: TARGET, KIND_EQ: MEDIA}


def _noop(point: str, **ctx: Any) -> None:
    return None


@dataclass(frozen=True)
class Subject:
    """Authoritative view of one subject at one read (never cached across tasks)."""

    role: str
    spec_digest: str
    subject_id: str                 # representation_id (media) / target_representation_id (target)
    content_id: str
    representation_id: str | None   # the content representation holding the artifact
    target_id: str | None = None
    target_version: int | None = None
    artifact_uri: str | None = None
    artifact_sha256: str | None = None
    artifact_size: int | None = None
    segment_count: int = 0
    live: bool = False
    reason: str = ""

    @property
    def bucket(self) -> int:
        return bucket_of(self.role, self.subject_id, self.content_id)

    def meta(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)

    @classmethod
    def from_meta(cls, text: str) -> Subject:
        return cls(**json.loads(text))


def bucket_of(role: str, subject_id: str, content_id: str) -> int:
    """Design §5: media rows by ``shard_of(content_id, 64)``, target rows by the target representation id."""
    return shard_of(content_id if role == MEDIA else subject_id, BUCKETS)


def media_live(rep: dict[str, Any] | None) -> bool:
    return rep is not None and rep.get("state") == "READY"


def target_live(binding: dict[str, Any] | None, target: dict[str, Any] | None) -> bool:
    """The one liveness predicate of a target subject (design §4.2, C-14j)."""
    return (binding is not None and binding.get("state") == "READY" and target is not None
            and target.get("state") not in TARGET_DEAD_STATES)


class Artifacts:
    """SHA-verified artifact reads with a bounded LRU of decoded vectors (by SHA-256)."""

    def __init__(self, objects: Objects, max_bytes: int) -> None:
        self.objects = objects
        self.max_bytes = max_bytes
        self._lru: OrderedDict[str, tuple[Vectors, int]] = OrderedDict()
        self._bytes = 0
        self._lock = threading.Lock()

    def load(self, uri: str, size: int, sha256: str) -> Vectors:
        with self._lock:
            hit = self._lru.get(sha256)
            if hit is not None:
                self._lru.move_to_end(sha256)
                return hit[0]
        bucket, key = parse_uri(uri)
        data = self.objects.read_verified(bucket, key, int(size), sha256)  # IntegrityError on mismatch
        arrays, _ = load_artifact(data)
        vec = Vectors.from_arrays(arrays, sha256)
        cost = int(vec.vectors.nbytes + vec.pts_s.nbytes + vec.segment_index.nbytes)
        with self._lock:
            if cost <= self.max_bytes:
                self._lru[sha256] = (vec, cost)
                self._bytes += cost
                while self._bytes > self.max_bytes and self._lru:
                    _, (_, c) = self._lru.popitem(last=False)
                    self._bytes -= c
        return vec

    def load_subject(self, s: Subject) -> Vectors:
        if not (s.artifact_uri and s.artifact_sha256 and s.artifact_size is not None):
            raise ValueError(f"subject {s.subject_id} has no artifact ({s.reason})")
        return self.load(s.artifact_uri, s.artifact_size, s.artifact_sha256)


class P10:
    """Everything a P10 component needs, built from P9's ``Services`` (no P9 object is changed)."""

    def __init__(self, settings: Settings, db: Db, coord: Coord, objects: Objects, store: Store,
                 index: VectorIndex, *, hook: Callable[..., None] = _noop) -> None:
        self.settings = settings
        self.db = db
        self.coord = coord
        self.objects = objects
        self.store = store
        self.index = index
        self.hook = hook
        self.artifacts = Artifacts(objects, settings.matching.artifact_cache_bytes)

    @classmethod
    def from_services(cls, svc: Any, index: VectorIndex | None = None, **kw: Any) -> P10:
        from fingerprinter.vindex import QdrantIndex

        ix = svc.settings.index
        index = index or QdrantIndex(ix.url, ix.api_key, timeout_s=ix.timeout_s)
        return cls(svc.settings, svc.db, svc.coord, svc.objects, svc.store, index, **kw)

    @property
    def clock(self) -> Clock:
        return self.store.clock

    def now(self) -> datetime:
        return self.store.now()

    @property
    def instance(self) -> str:
        return self.store.instance

    def key(self, *parts: str) -> str:
        return self.coord.key("p10", *parts)

    # --- authoritative reads (P9 rows) -----------------------------------------------

    def media_subject(self, content_id: str, spec_digest: str, *, serial: bool = False) -> Subject:
        rep = self.db.one("SELECT * FROM {ks}.representations WHERE content_id = ? AND spec_digest = ?",
                          content_id, spec_digest, level=SERIAL if serial else "quorum")
        if rep is None:
            return Subject(MEDIA, spec_digest, "", content_id, None, reason="representation row missing")
        return Subject(MEDIA, spec_digest, rep["representation_id"], content_id, rep["representation_id"],
                       artifact_uri=rep["artifact_uri"], artifact_sha256=rep["artifact_sha256"],
                       artifact_size=rep["artifact_size"], segment_count=int(rep["segment_count"] or 0),
                       live=media_live(rep), reason=str(rep.get("state")))

    def target_subject(self, target_id: str, version: int, spec_digest: str) -> Subject:
        binding = self.db.one("SELECT * FROM {ks}.target_representations WHERE target_id = ? AND version = ? "
                              "AND spec_digest = ?", target_id, version, spec_digest)
        target = self.db.one("SELECT target_id, version, state, content_id FROM {ks}.targets WHERE target_id = ? "
                             "AND version = ?", target_id, version)
        return self._target_subject(binding, target, spec_digest, target_id, version)

    def _target_subject(self, binding: dict[str, Any] | None, target: dict[str, Any] | None, spec_digest: str,
                        target_id: str, version: int) -> Subject:
        if binding is None:
            return Subject(TARGET, spec_digest, "", "", None, target_id, version, reason="binding missing")
        trep = binding.get("target_representation_id") or ""
        cid = binding.get("content_id") or (target or {}).get("content_id") or ""
        live = target_live(binding, target)
        if not trep:
            return Subject(TARGET, spec_digest, "", cid, None, target_id, version, live=False,
                           reason=f"binding {binding.get('state')}")
        rep = self.db.one("SELECT * FROM {ks}.representations WHERE content_id = ? AND spec_digest = ?",
                          cid, spec_digest)
        if rep is None:
            return Subject(TARGET, spec_digest, trep, cid, binding.get("representation_id"), target_id, version,
                           live=False, reason="content representation missing")
        return Subject(TARGET, spec_digest, trep, cid, rep["representation_id"], target_id, version,
                       artifact_uri=rep["artifact_uri"], artifact_sha256=rep["artifact_sha256"],
                       artifact_size=rep["artifact_size"], segment_count=int(rep["segment_count"] or 0),
                       live=live,
                       reason=f"binding {binding.get('state')} target {(target or {}).get('state')}")

    def refresh(self, s: Subject) -> Subject:
        """A fresh authoritative read of the same subject (liveness checks, repair, desired state)."""
        if s.role == MEDIA:
            return self.media_subject(s.content_id, s.spec_digest)
        if s.target_id is None or s.target_version is None:
            raise ValueError(f"target subject {s.subject_id} without target id/version")
        return self.target_subject(s.target_id, s.target_version, s.spec_digest)

    def indexed_specs(self) -> list[str]:
        """Specs P10 indexes: those holding ``retire_guard(spec, 'p10-index')`` (design §6.5, §12.3)."""
        rows = self.db.rows("SELECT spec_digest, holder FROM {ks}.retire_guard")
        return sorted({r["spec_digest"] for r in rows if r["holder"] == "p10-index"})
