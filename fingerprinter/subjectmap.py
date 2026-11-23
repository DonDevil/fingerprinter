"""FP-05 Investigation A: the sidecar subject map — point id -> the candidate-search metadata of that point.

Qdrant point ids are ``uuid5(subject:seq:k)`` (one-way), so everything a candidate-search hit is read for
(``CANDIDATE_PAYLOAD_FIELDS``: rep, cid, trep, tid, tver, seq, k, spec, lowinfo) must be stored somewhere
else if Qdrant is not to return it. This module is that "somewhere else": a compact, per-collection
(= per generation) lookup table that lives in the fingerprinter process.

Properties the design depends on (docs: ``workspace/benchmarks/fp05/investigation-a/``):

* **Derived state.** Every entry is a pure function of (subject, seq, k) and the subject's authoritative
  facts, exactly as ``indexing.payload_for`` writes them; the map can always be rebuilt (from the
  authoritative store through the Indexer, or - for verification and recovery - from Qdrant's own payloads).
* **Bound to one collection.** A generation's collection name is never reused (design §4.2), and the map's
  manifest records collection, spec, role and generation; ``load`` refuses a directory that does not match.
* **Never trusted blindly.** ``verify`` compares the live entry count with Qdrant's exact count. A hit whose
  id is not in the map is resolved from Qdrant (``resolve_hits``), so a missing entry costs latency, never
  correctness. An entry is immutable for its id (the id contains the seq), so a *stale extra* entry can
  never be returned by Qdrant and is harmless; ``remove_subject`` still deletes them (memory, not safety).
* **Compact.** Sorted numpy columns (33 bytes/point) plus a small overlay of live writes; no per-point
  Python object in the frozen base.
* **Crash safe.** ``save`` writes a complete directory beside the target and ``os.replace``s it in; live
  writes go to an append-only, CRC-protected journal that is written *before* the Qdrant write (map ⊇ Qdrant).
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import threading
import time
import uuid
import zlib
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

FORMAT = "fp-subjectmap/1"
MASK = (1 << 64) - 1
ARRAYS = ("hi", "lo", "subj", "seq", "k", "low")
DTYPES = {"hi": np.uint64, "lo": np.uint64, "subj": np.uint32, "seq": np.int64, "k": np.int32, "low": np.uint8}


class SubjectMapError(Exception):
    """The map is unusable for the collection asked for (wrong generation, corrupt, incomplete)."""


def _split(point_id: str) -> tuple[int, int]:
    n = int(str(point_id).replace("-", ""), 16)   # a canonical uuid string; Qdrant returns exactly that form
    if n >> 128:
        raise ValueError(f"not a uuid: {point_id!r}")
    return n >> 64, n & MASK


@dataclass(frozen=True)
class SubjectInfo:
    subject_id: str
    rep: str | None
    cid: str | None
    spec: str | None
    trep: str | None
    tid: str | None
    tver: int | None

    def base_payload(self) -> dict[str, Any]:
        d: dict[str, Any] = {"rep": self.rep, "cid": self.cid, "spec": self.spec}
        if self.trep is not None:
            d["trep"], d["tid"], d["tver"] = self.trep, self.tid, self.tver
        return {k: v for k, v in d.items() if v is not None}

    @property
    def has_target(self) -> bool:
        return self.trep is not None


class SubjectMap:
    """Map of one collection. Thread-safe for concurrent ``get`` and (serialised) writes."""

    def __init__(self, collection: str, *, spec: str, role: str, generation: int) -> None:
        self.collection, self.spec, self.role, self.generation = collection, spec, role, int(generation)
        self._lock = threading.RLock()    # readers' lock: held only for in-memory work, never across disk I/O
        self._wlock = threading.RLock()   # serialises writers, journal fsyncs and checkpoints
        self._subjects: list[SubjectInfo] = []
        self._subject_idx: dict[SubjectInfo, int] = {}
        self._subject_by_id: dict[str, int] = {}
        self._base: dict[str, np.ndarray] = {n: np.empty(0, DTYPES[n]) for n in ARRAYS}
        self._base_dead: set[int] = set()                       # tombstoned base ids (int form)
        self._delta: dict[int, tuple[int, int, int, int]] = {}   # id -> (subj, seq, k, low)
        self._shadow: set[int] = set()                          # delta ids that override a live base entry
        self._journal: Path | None = None
        self._journal_fh: Any = None
        self.stats = {"hits": 0, "misses": 0}

    # --- size / accounting -------------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._base["hi"]) - len(self._base_dead) + len(self._delta) - len(self._shadow)

    def nbytes(self) -> int:
        """Bytes held in memory by this map (numpy columns + overlay + subject table; an estimate for the
        Python containers, an exact count for the columns)."""
        with self._lock:
            cols = sum(a.nbytes for a in self._base.values())
            overlay = len(self._delta) * 150 + len(self._base_dead) * 60
            table = len(self._subjects) * 700
            return cols + overlay + table

    # --- writes ------------------------------------------------------------------------------------

    def _subject(self, subject_id: str, rep, cid, spec, trep, tid, tver) -> int:
        info = SubjectInfo(subject_id, rep, cid, spec, trep, tid, None if tver is None else int(tver))
        i = self._subject_idx.get(info)
        if i is None:
            i = len(self._subjects)
            self._subjects.append(info)
            self._subject_idx[info] = i
            self._subject_by_id.setdefault(subject_id, i)
        return i

    def record(self, point_id: str, payload: dict[str, Any], subject_field: str) -> None:
        """Record one point exactly as it is about to be written to Qdrant (write-ahead: call before upsert)."""
        self.record_many([(point_id, payload)], subject_field)

    def record_many(self, points: Iterable[tuple[str, dict[str, Any]]], subject_field: str) -> int:
        n = 0
        entries = []
        with self._wlock:
            with self._lock:
                for pid_, pl in points:
                    sidx = self._subject(pl[subject_field], pl.get("rep"), pl.get("cid"), pl.get("spec"),
                                         pl.get("trep"), pl.get("tid"), pl.get("tver"))
                    hi, lo = _split(pid_)
                    ent = (sidx, int(pl["seq"]), int(pl["k"]), 1 if pl.get("lowinfo") else 0)
                    self._set_delta(hi, lo, ent)
                    entries.append((pid_, ent, self._subjects[sidx]))
                    n += 1
            self._journal_append(entries)    # fsync outside the readers' lock; still before the caller's Qdrant write
        return n

    def remove_subject(self, subject_id: str, *, seq_lt: int | None = None, seq_lte: int | None = None) -> int:
        """Forget the entries of ``subject_id`` below a seq (the map-side twin of ``subject_seq_lt/lte``)."""
        lim, strict = (seq_lt, True) if seq_lt is not None else (seq_lte, False)
        if lim is None:
            raise ValueError("seq_lt or seq_lte required")
        removed = 0
        with self._wlock:
            sidxs = {i for i, s in enumerate(self._subjects) if s.subject_id == subject_id}
            dead: list[int] = []
            if len(self._base["hi"]) and sidxs:        # the O(N) scan runs without the readers' lock (base is immutable)
                seq_col = self._base["seq"]
                mask = np.isin(self._base["subj"], np.fromiter(sidxs, np.uint32)) & (seq_col < lim if strict
                                                                                    else seq_col <= lim)
                dead = [(int(self._base["hi"][i]) << 64) | int(self._base["lo"][i]) for i in np.nonzero(mask)[0]]
            with self._lock:
                for key, (si, seq, _k, _l) in list(self._delta.items()):
                    if si in sidxs and (seq < lim if strict else seq <= lim):
                        del self._delta[key]
                        self._shadow.discard(key)
                        removed += 1
                for key in dead:
                    if key not in self._base_dead:
                        self._base_dead.add(key)
                        removed += 1
            self._journal_append([("-", (subject_id, lim, strict), None)])
        return removed

    # --- reads -------------------------------------------------------------------------------------

    def _live_in_base(self, key: int, hi: int, lo: int) -> bool:
        if key in self._base_dead:
            return False
        bhi = self._base["hi"]
        if not len(bhi):
            return False
        i = int(np.searchsorted(bhi, np.uint64(hi)))
        blo = self._base["lo"]
        while i < len(bhi) and int(bhi[i]) == hi:
            if int(blo[i]) == lo:
                return True
            i += 1
        return False

    def _set_delta(self, hi: int, lo: int, ent: tuple[int, int, int, int]) -> None:
        key = (hi << 64) | lo
        if key not in self._delta and self._live_in_base(key, hi, lo):
            self._shadow.add(key)
        self._delta[key] = ent

    def get(self, ids: Sequence[str]) -> list[dict[str, Any] | None]:
        """Metadata (the nine candidate-search keys, absent keys omitted) per id; ``None`` for an unknown id."""
        out: list[dict[str, Any] | None] = []
        with self._lock:
            n = len(ids)
            parts = [_split(i) for i in ids]
            bhi = self._base["hi"]
            pos = np.searchsorted(bhi, np.fromiter((p[0] for p in parts), np.uint64, n)) if len(bhi) else None
            blo, bsub, bseq, bk, blow = (self._base[x] for x in ("lo", "subj", "seq", "k", "low"))
            for j, (hi, lo) in enumerate(parts):
                key = (hi << 64) | lo
                e = self._delta.get(key)
                if e is None and key not in self._base_dead and pos is not None:
                    i = int(pos[j])
                    while i < len(bhi) and int(bhi[i]) == hi:
                        if int(blo[i]) == lo:
                            e = (int(bsub[i]), int(bseq[i]), int(bk[i]), int(blow[i]))
                            break
                        i += 1
                if e is None:
                    self.stats["misses"] += 1
                    out.append(None)
                    continue
                self.stats["hits"] += 1
                info = self._subjects[e[0]]
                d = info.base_payload()
                d["seq"], d["k"] = e[1], e[2]
                if info.has_target:
                    d["lowinfo"] = bool(e[3])
                out.append(d)
        return out

    def items(self) -> Iterable[tuple[str, dict[str, Any]]]:
        """Every live entry as (point id, payload) - for verification and tests (not a hot path)."""
        with self._lock:
            ids = []
            for i in range(len(self._base["hi"])):
                key = (int(self._base["hi"][i]) << 64) | int(self._base["lo"][i])
                if key not in self._base_dead and key not in self._delta:
                    ids.append(key)
            ids.extend(self._delta)
        strs = [str(uuid.UUID(int=k)) for k in ids]
        for s, d in zip(strs, self.get(strs), strict=True):
            yield s, d  # type: ignore[misc]

    # --- generation consistency --------------------------------------------------------------------

    def verify(self, qdrant_count: int) -> bool:
        """The map holds exactly as many live entries as the collection holds points (exact count)."""
        return len(self) == int(qdrant_count)

    def resolve_hits(self, hits: list[Any], fetch_missing) -> int:
        """Fill ``hit.payload`` of every hit from the map; ids the map does not know are fetched from Qdrant
        through ``fetch_missing(ids) -> {id: payload}``. Returns the number of misses (never silent)."""
        metas = self.get([h.id for h in hits])
        missing = [h.id for h, m in zip(hits, metas, strict=True) if m is None]
        fetched = fetch_missing(missing) if missing else {}
        for h, m in zip(hits, metas, strict=True):
            h.payload.clear()
            h.payload.update(m if m is not None else fetched.get(h.id, {}))
        return len(missing)

    # --- persistence -------------------------------------------------------------------------------

    def _compacted(self) -> tuple[dict[str, np.ndarray], list[SubjectInfo]]:
        """Called with ``_wlock`` held (no writer can change the state; readers are not blocked)."""
        if True:
            keep = np.ones(len(self._base["hi"]), bool)
            if self._base_dead and len(keep):
                dead_hi = {k >> 64 for k in self._base_dead}
                for i in np.nonzero(np.isin(self._base["hi"], np.fromiter(dead_hi, np.uint64)))[0]:
                    if ((int(self._base["hi"][i]) << 64) | int(self._base["lo"][i])) in self._base_dead:
                        keep[i] = False
            cols = {n: self._base[n][keep] for n in ARRAYS}
            if self._delta:
                keys = list(self._delta)
                ents = [self._delta[k] for k in keys]
                add = {"hi": np.array([k >> 64 for k in keys], np.uint64), "lo": np.array([k & MASK for k in keys], np.uint64),
                       "subj": np.array([e[0] for e in ents], np.uint32), "seq": np.array([e[1] for e in ents], np.int64),
                       "k": np.array([e[2] for e in ents], np.int32), "low": np.array([e[3] for e in ents], np.uint8)}
                # a delta entry overrides a base entry with the same id
                if len(cols["hi"]):
                    dk = set(keys)
                    drop = np.zeros(len(cols["hi"]), bool)
                    for i in np.nonzero(np.isin(cols["hi"], add["hi"]))[0]:
                        if ((int(cols["hi"][i]) << 64) | int(cols["lo"][i])) in dk:
                            drop[i] = True
                    cols = {n: cols[n][~drop] for n in ARRAYS}
                cols = {n: np.concatenate([cols[n], add[n]]) for n in ARRAYS}
            order = np.lexsort((cols["lo"], cols["hi"]))
            return {n: cols[n][order] for n in ARRAYS}, list(self._subjects)

    def save(self, root: Path) -> Path:
        """Atomically write the complete map to ``root/<collection>/`` (checkpoint; resets the journal)."""
        with self._wlock:
            return self._save(Path(root))

    def _save(self, root: Path) -> Path:
        final = root / self.collection
        tmp = root / f".{self.collection}.tmp-{os.getpid()}-{int(time.time() * 1000)}"
        tmp.mkdir(parents=True)
        cols, subjects = self._compacted()
        digests = {}
        for n in ARRAYS:
            p = tmp / f"{n}.bin"
            with open(p, "wb") as fh:
                fh.write(cols[n].tobytes())
                fh.flush()
                os.fsync(fh.fileno())
            digests[n] = hashlib.sha256(cols[n].tobytes()).hexdigest()
        subj_doc = [[s.subject_id, s.rep, s.cid, s.spec, s.trep, s.tid, s.tver] for s in subjects]
        (tmp / "subjects.json").write_text(json.dumps(subj_doc, separators=(",", ":")))
        manifest = {"format": FORMAT, "collection": self.collection, "spec": self.spec, "role": self.role,
                    "generation": self.generation, "count": int(len(cols["hi"])), "digests": digests,
                    "subjects_sha256": hashlib.sha256((tmp / "subjects.json").read_bytes()).hexdigest(),
                    "created_at": time.time(), "complete": True}
        (tmp / "MANIFEST.json").write_text(json.dumps(manifest, indent=1, sort_keys=True))
        fd = os.open(tmp, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        if final.exists():
            old = root / f".{self.collection}.old-{os.getpid()}-{int(time.time() * 1000)}"
            os.replace(final, old)
            os.replace(tmp, final)
            shutil.rmtree(old, ignore_errors=True)
        else:
            os.replace(tmp, final)
        subject_idx = {s: i for i, s in enumerate(subjects)}
        by_id: dict[str, int] = {}
        for i, s in enumerate(subjects):
            by_id.setdefault(s.subject_id, i)
        self._close_journal()
        with self._lock:                       # the only moment readers wait: pointer swaps, no I/O
            self._base = cols
            self._base_dead = set()
            self._delta = {}
            self._shadow = set()
            self._subjects = subjects
            self._subject_idx = subject_idx
            self._subject_by_id = by_id
        self.attach_journal(final / "journal.log", fresh=True)
        return final

    @classmethod
    def load(cls, root: Path, collection: str, *, spec: str, role: str, generation: int,
             mmap: bool = False, check_digests: bool = True) -> SubjectMap:
        """Load and validate; raises ``SubjectMapError`` for anything that is not exactly this generation's map."""
        d = Path(root) / collection
        try:
            m = json.loads((d / "MANIFEST.json").read_text())
        except (OSError, ValueError) as exc:
            raise SubjectMapError(f"no readable manifest for {collection}: {exc}") from exc
        if m.get("format") != FORMAT or not m.get("complete"):
            raise SubjectMapError(f"{collection}: unsupported or incomplete map ({m.get('format')})")
        for key, want in (("collection", collection), ("spec", spec), ("role", role), ("generation", int(generation))):
            if m.get(key) != want:
                raise SubjectMapError(f"{collection}: manifest {key}={m.get(key)!r}, expected {want!r}")
        sm = cls(collection, spec=spec, role=role, generation=generation)
        for n in ARRAYS:
            p = d / f"{n}.bin"
            a = (np.memmap(p, dtype=DTYPES[n], mode="r") if mmap and p.stat().st_size
                 else np.fromfile(p, dtype=DTYPES[n]))
            if check_digests and hashlib.sha256(np.asarray(a).tobytes()).hexdigest() != m["digests"][n]:
                raise SubjectMapError(f"{collection}: {n}.bin checksum mismatch")
            if len(a) != m["count"]:
                raise SubjectMapError(f"{collection}: {n}.bin has {len(a)} rows, manifest says {m['count']}")
            sm._base[n] = a
        raw = (d / "subjects.json").read_bytes()
        if hashlib.sha256(raw).hexdigest() != m["subjects_sha256"]:
            raise SubjectMapError(f"{collection}: subjects.json checksum mismatch")
        for row in json.loads(raw):
            sm._subject(row[0], *row[1:])
        if len(sm._base["subj"]) and int(sm._base["subj"].max()) >= len(sm._subjects):
            raise SubjectMapError(f"{collection}: entry refers to an unknown subject")
        sm._replay(d / "journal.log")
        sm.attach_journal(d / "journal.log")
        return sm

    # --- journal (write-ahead, CRC protected) -------------------------------------------------------

    def attach_journal(self, path: Path, *, fresh: bool = False) -> None:
        with self._lock:
            self._close_journal()
            self._journal = Path(path)
            if fresh and self._journal.exists():
                self._journal.unlink()
            self._journal.parent.mkdir(parents=True, exist_ok=True)
            self._journal_fh = open(self._journal, "ab")

    def _close_journal(self) -> None:
        if self._journal_fh is not None:
            self._journal_fh.close()
            self._journal_fh = None

    def close(self) -> None:
        with self._lock:
            self._close_journal()

    def _journal_append(self, entries: list[tuple[Any, Any, Any]]) -> None:
        if self._journal_fh is None:
            return
        for pid_, ent, info in entries:
            if pid_ == "-":
                rec = {"op": "-", "s": ent[0], "lim": ent[1], "strict": ent[2]}
            else:
                rec = {"op": "+", "id": pid_, "e": list(ent), "s": [info.subject_id, info.rep, info.cid, info.spec,
                                                                     info.trep, info.tid, info.tver]}
            body = json.dumps(rec, separators=(",", ":")).encode()
            self._journal_fh.write(b"%08x " % zlib.crc32(body) + body + b"\n")
        self._journal_fh.flush()
        os.fsync(self._journal_fh.fileno())

    def _replay(self, path: Path) -> None:
        if not path.exists():
            return
        with open(path, "rb") as fh:
            for line in fh:
                crc, _, body = line.rstrip(b"\n").partition(b" ")
                if not line.endswith(b"\n") or int(crc, 16) != zlib.crc32(body):
                    break  # a torn tail from a crash: everything before it is intact
                rec = json.loads(body)
                if rec["op"] == "+":
                    hi, lo = _split(rec["id"])
                    sidx = self._subject(*rec["s"])
                    self._set_delta(hi, lo, (sidx, rec["e"][1], rec["e"][2], rec["e"][3]))
                else:
                    self._remove_replay(rec["s"], rec["lim"], rec["strict"])

    def _remove_replay(self, subject_id: str, lim: int, strict: bool) -> None:
        self.remove_subject(subject_id, **({"seq_lt": lim} if strict else {"seq_lte": lim}))


class SubjectMaps:
    """Registry of the maps of one deployment: ``resolve(pointer)`` returns a *verified* map or ``None``
    (None = use the Qdrant payload path; the baseline behaviour). ``root`` None = feature off."""

    def __init__(self, root: Path | None, *, mmap: bool = False, verify_ttl_s: float = 30.0) -> None:
        self.root = None if root is None else Path(root)
        self.mmap = mmap
        self.verify_ttl_s = verify_ttl_s
        self._maps: dict[str, SubjectMap] = {}
        self._verified: dict[str, float] = {}
        self._refused: dict[str, str] = {}
        self._lock = threading.RLock()

    @property
    def enabled(self) -> bool:
        return self.root is not None

    def create(self, collection: str, *, spec: str, role: str, generation: int,
               journal: bool = False) -> SubjectMap:
        """A new, empty map for a collection that is about to be built (call before its first upsert).
        The bulk build is not journalled (a crashed build is rebuilt, and its generation is not searched);
        ``save`` checkpoints it and starts the write-ahead journal for live writes."""
        if self.root is None:
            raise SubjectMapError("subject maps are disabled")
        sm = SubjectMap(collection, spec=spec, role=role, generation=generation)
        if journal:
            sm.attach_journal(self.root / collection / "journal.log", fresh=True)
        with self._lock:
            self._maps[collection] = sm
            self._verified.pop(collection, None)
        return sm

    def get(self, collection: str) -> SubjectMap | None:
        with self._lock:
            return self._maps.get(collection)

    def drop(self, collection: str) -> None:
        """The generation is gone (retired/failed): forget the map and delete its files."""
        with self._lock:
            sm = self._maps.pop(collection, None)
            self._verified.pop(collection, None)
            self._refused.pop(collection, None)
        if sm is not None:
            sm.close()
        if self.root is not None:
            shutil.rmtree(self.root / collection, ignore_errors=True)

    def resolve(self, collection: str, *, spec: str, role: str, generation: int, count_fn) -> SubjectMap | None:
        """The map for ``collection`` if (and only if) it is this generation's and its count equals Qdrant's."""
        if self.root is None:
            return None
        now = time.monotonic()
        with self._lock:
            sm = self._maps.get(collection)
            if sm is None:
                try:
                    sm = SubjectMap.load(self.root, collection, spec=spec, role=role, generation=generation,
                                         mmap=self.mmap)
                except SubjectMapError as exc:
                    self._refused[collection] = str(exc)
                    return None
                self._maps[collection] = sm
            elif (sm.collection, sm.spec, sm.role, sm.generation) != (collection, spec, role, int(generation)):
                self._refused[collection] = "registry entry belongs to another generation"
                return None
            if now - self._verified.get(collection, -1e9) < self.verify_ttl_s:
                return sm
        ok = sm.verify(count_fn(collection))
        with self._lock:
            if ok:
                self._verified[collection] = now
                self._refused.pop(collection, None)
                return sm
            self._refused[collection] = f"count mismatch: map {len(sm)}"
            self._verified.pop(collection, None)
            return None

    def refusals(self) -> dict[str, str]:
        with self._lock:
            return dict(self._refused)

    def seal(self, index: Any, collection: str, *, spec: str, role: str, generation: int, exact: int) -> None:
        """Generation activation gate (called by validation): the map of ``collection`` must hold exactly
        ``exact`` entries and is then checkpointed to disk atomically (``MANIFEST.json`` is written last).
        A missing or mismatching map is rebuilt from Qdrant's own payloads (recovery of derived state); if it
        still disagrees the generation must not become READY (``SubjectMapError``)."""
        if self.root is None:
            return
        with self._lock:
            sm = self._maps.get(collection)
        if sm is None or len(sm) != int(exact) or (sm.collection, sm.spec, sm.role, sm.generation) != (
                collection, spec, role, int(generation)):
            sm = self.rebuild_from_index(index, collection, spec=spec, role=role, generation=generation)
        if len(sm) != int(exact):
            raise SubjectMapError(f"{collection}: map holds {len(sm)} entries, Qdrant {exact} (writes raced the rebuild?)")
        sm.save(self.root)
        with self._lock:
            self._verified[collection] = time.monotonic()
            self._refused.pop(collection, None)

    def rebuild_from_index(self, index: Any, collection: str, *, spec: str, role: str, generation: int) -> SubjectMap:
        """Rebuild a map by scrolling the collection's payloads (no vectors). Used for recovery and for verification
        against what Qdrant actually stores; the authoritative rebuild is the Indexer's normal write path."""
        from fingerprinter.vindex import SUBJECT_FIELD

        sm = SubjectMap(collection, spec=spec, role=role, generation=generation)
        batch: list[tuple[str, dict[str, Any]]] = []
        for pt in index.scroll_all(collection, page=1000, with_vector=False):
            batch.append((pt.id, pt.payload))
            if len(batch) >= 5000:
                sm.record_many(batch, SUBJECT_FIELD[role])
                batch = []
        if batch:
            sm.record_many(batch, SUBJECT_FIELD[role])
        with self._lock:
            old = self._maps.get(collection)
            self._maps[collection] = sm
        if old is not None:
            old.close()
        return sm
