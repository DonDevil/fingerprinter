"""Index mutations, builds, validation and verification (design §12.2-§12.6, §12.9; P10-D4, D19, D20).

**Mutation protocol** (one ``index_entries`` row per subject = the durable intent):

1. *Intent*: the desired state is derived from the authoritative P9 rows
   (UPSERT with ``desired_ver`` = artifact SHA-256 [+ lowinfo revision] when
   the subject is live, else DELETE) and written with ``… seq = s + 1 IF seq =
   s`` (or ``IF NOT EXISTS``). Nothing touches Qdrant before that LWT is
   known to have applied (unknown outcome ⇒ SERIAL read first: Invariant A).
2. *Apply*: re-read the row and the authoritative row; read the routing set
   fresh (generations CREATED…DRAINING); reserve capacity for every routed
   generation whose ``applied[g]`` is not yet ``s`` (refused ⇒ BLOCKED,
   no Qdrant call); **pre-count** the previously applied sequence in every
   searched generation (a short count is a detected loss: the occurrence is
   made durable before this apply can heal it — C-13); then per generation:
   UPSERT ``u`` writes the ids of ``u`` (``uuid5(subject:u:k)``, payload
   ``seq = u``) and then deletes ``subject AND seq < u``; DELETE ``d`` deletes
   ``subject AND seq <= d`` (C-12; boundaries verified on 1.15.1 by R-24).
3. *Bookkeeping*: ``applied = applied + {g: s, …}`` and, only when every
   routed generation holds ``s``, ``state = 'APPLIED'`` — ``IF seq = s``. A
   failed condition means a newer intent: the worker applies that one.

A stale worker can therefore only add older-``seq`` points (never accepted
as evidence: Invariant C; removed by the verification pass) and can never
remove the current points of a subject (S2).
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import numpy as np

from fingerprinter import metrics
from fingerprinter.capacity import Capacity, CapacityBlockedError, holder_of
from fingerprinter.coord import utc
from fingerprinter.db import SERIAL, LwtUnknownError
from fingerprinter.errors import StorageUnavailableError
from fingerprinter.generations import (
    ACTIVE,
    BUILDING,
    CATCHING_UP,
    CREATED,
    DRAINING,
    FAILED,
    NON_TERMINAL_BUILD,
    READY,
    RETIRED,
    SEARCHED,
    VALIDATING,
    Generations,
)
from fingerprinter.objects import IntegrityError, ObjectMissingError
from fingerprinter.p10ctx import BUCKETS, P10, Subject
from fingerprinter.recovery import Recovery
from fingerprinter.vindex import (
    MEDIA,
    SUBJECT_FIELD,
    TARGET,
    GenerationParams,
    IndexMissingError,
    IndexPermanentError,
    IndexUnavailableError,
    InvalidVectorError,
    Point,
    check_unit,
    point_id,
    subject_seq_eq,
    subject_seq_lt,
    subject_seq_lte,
)

log = logging.getLogger("fingerprinter.indexing")
UPSERT, DELETE = "UPSERT", "DELETE"
PENDING, APPLIED, BLOCKED = "PENDING", "APPLIED", "BLOCKED"


class WaitingIndexError(Exception):
    """The subject cannot be applied now: no ACTIVE generation, index unavailable, or capacity."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason


class SubjectInvalidError(Exception):
    """The subject's own authoritative data is unusable (artifact SHA mismatch, invalid vectors, missing row)."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason


@dataclass
class ApplyResult:
    state: str
    seq: int
    applied: dict[int, int] = field(default_factory=dict)
    detected: list[int] = field(default_factory=list)


def payload_for(s: Subject, k: int, t: float, seq: int | None, lowinfo: tuple[bool, int] | None) -> dict[str, Any]:
    p: dict[str, Any] = {"rep": s.representation_id, "cid": s.content_id, "spec": s.spec_digest, "k": int(k),
                         "t": float(t)}
    if s.role == TARGET:
        p.update({"trep": s.subject_id, "tid": s.target_id, "tver": s.target_version,
                  "lowinfo": bool(lowinfo[0]) if lowinfo else False, "lowinfo_rev": int(lowinfo[1]) if lowinfo else 0})
    if seq is not None:
        p["seq"] = int(seq)
    return p


def subject_digest(segment_index: list[int], vectors: list[bytes], payloads: list[dict[str, Any]]) -> str:
    """Content digest of one subject (design §12.5): sorted segment indexes, float32 bytes, canonical
    payload without ``seq``; point ids are excluded (they contain ``seq``)."""
    h = hashlib.sha256()
    for k, v, p in sorted(zip(segment_index, vectors, payloads), key=lambda x: x[0]):
        h.update(int(k).to_bytes(8, "big", signed=True))
        h.update(v)
        h.update(json.dumps({x: y for x, y in p.items() if x != "seq"}, sort_keys=True, separators=(",", ":")).encode())
    return h.hexdigest()


class Indexer:
    def __init__(self, p10: P10, gens: Generations, capacity: Capacity, recovery: Recovery) -> None:
        self.p = p10
        self.db = p10.db
        self.gens = gens
        self.capacity = capacity
        self.recovery = recovery
        self.lowinfo: Any = None  # set by the wiring; provides record(spec, trep) -> (rev, flags) | None
        self.hook = p10.hook

    # --- rows ---------------------------------------------------------------------------

    def entry(self, spec: str, role: str, bucket: int, subject_id: str, *,
              serial: bool = True) -> dict[str, Any] | None:
        return self.db.one("SELECT * FROM {ks}.index_entries WHERE spec_digest = ? AND role = ? AND bucket = ? "
                           "AND subject_id = ?", spec, role, bucket, subject_id, level=SERIAL if serial else "quorum")

    def entries(self, spec: str, role: str, bucket: int) -> list[dict[str, Any]]:
        return self.db.rows("SELECT * FROM {ks}.index_entries WHERE spec_digest = ? AND role = ? AND bucket = ?",
                            spec, role, bucket)

    def lowinfo_of(self, s: Subject) -> tuple[int, set[int]] | None:
        if s.role != TARGET or self.lowinfo is None:
            return None
        return self.lowinfo.record_flags(s.spec_digest, s.subject_id)

    def desired(self, s: Subject) -> tuple[str, str]:
        if not s.live:
            return DELETE, ""
        ver = s.artifact_sha256 or ""
        if s.role == TARGET:
            li = self.lowinfo_of(s)
            ver += f"+li{li[0] if li else 0}"
        return UPSERT, ver

    # --- step 1: intent ----------------------------------------------------------------------

    def write_intent(self, s: Subject) -> dict[str, Any]:
        """Record the desired state (no-op when equal). Returns the row as read back (SERIAL)."""
        if not s.subject_id:
            raise SubjectInvalidError("subject_missing", s.reason)
        op, ver = self.desired(s)
        b = s.bucket
        for _ in range(20):
            row = self.entry(s.spec_digest, s.role, b, s.subject_id)
            if row is not None and row["desired_op"] == op and row["desired_ver"] == ver:
                return row
            now = self.p.now()
            try:
                if row is None:
                    res = self.db.lwt(
                        "INSERT INTO {ks}.index_entries (spec_digest, role, bucket, subject_id, desired_op, "
                        "desired_ver, seq, applied, segment_count, state, attempts, subject, updated_at) "
                        "VALUES (?,?,?,?,?,?,1,?,?,'PENDING',0,?,?) IF NOT EXISTS",
                        s.spec_digest, s.role, b, s.subject_id, op, ver, {}, s.segment_count, s.meta(), now)
                else:
                    res = self.db.lwt(
                        "UPDATE {ks}.index_entries SET desired_op = ?, desired_ver = ?, seq = ?, state = 'PENDING', "
                        "blocked_reason = null, attempts = 0, segment_count = ?, subject = ?, updated_at = ? "
                        "WHERE spec_digest = ? AND role = ? AND bucket = ? AND subject_id = ? IF seq = ?",
                        op, ver, int(row["seq"]) + 1, s.segment_count, s.meta(), now, s.spec_digest, s.role, b,
                        s.subject_id, row["seq"])
            except LwtUnknownError:
                continue  # SERIAL re-read decides; Qdrant is untouched meanwhile (Invariant A)
            self.hook("indexer.after_intent", subject=s.subject_id, applied=res.applied)
            if res.applied:
                continue  # read back
        raise LwtUnknownError("intent did not converge")

    # --- steps 2-3: apply ---------------------------------------------------------------------

    def apply(self, spec: str, role: str, bucket: int, subject_id: str, *, force_gens: set[int] = frozenset(),
              covered_gens: set[int] = frozenset(), repairing: set[int] = frozenset(),
              only_gens: set[int] | None = None) -> ApplyResult:
        """Apply the row's current desired state to every routed generation (or ``only_gens``).

        ``force_gens``: re-write even if ``applied[g] = seq`` (a repair); ``covered_gens``: reservation
        held by a rebuild admission (no per-subject reservation); ``repairing``: generations whose
        pre-count is satisfied by an OPEN occurrence the caller holds."""
        lease = self.p.key("lease", "ie", spec[-12:], role, subject_id)
        token = self.p.instance
        self.p.coord.acquire(lease, token, int(self.p.settings.index.mutation_lease_s * 1000))
        try:
            for _ in range(10):
                row = self.entry(spec, role, bucket, subject_id)
                if row is None:
                    raise SubjectInvalidError("no_intent", subject_id)
                s = int(row["seq"])
                subject = Subject.from_meta(row["subject"])
                fresh = self.p.refresh(subject)
                if self.desired(fresh) != (row["desired_op"], row["desired_ver"]):
                    self.write_intent(fresh)
                    continue
                _, routed = self.gens.routing(spec, role)
                applied = {int(k): int(v) for k, v in (row["applied"] or {}).items()}
                todo = [g for g in routed if applied.get(g["generation"]) != s or g["generation"] in force_gens]
                if only_gens is not None:
                    # restrict what is written, never what "complete" means: APPLIED is judged against
                    # the full routing set (C-14k, R-4)
                    todo = [g for g in todo if g["generation"] in only_gens]
                if not todo:
                    return self._bookkeep(row, s, {}, routed, applied)
                try:
                    done, detected = self._apply_to(row, fresh, s, todo, applied, covered_gens, repairing)
                except CapacityBlockedError as exc:
                    self._block(row, "capacity", str(exc))
                    raise WaitingIndexError("capacity", str(exc)) from exc
                except (IndexPermanentError, InvalidVectorError) as exc:
                    self._block(row, "permanent", str(exc))
                    if isinstance(exc, InvalidVectorError):
                        raise SubjectInvalidError("invalid_vector", str(exc)) from exc
                    raise
                except IntegrityError as exc:
                    self._block(row, "artifact_corrupt", str(exc))
                    raise SubjectInvalidError("artifact_corrupt", str(exc)) from exc
                except ObjectMissingError as exc:
                    self._block(row, "artifact_missing", str(exc))
                    raise SubjectInvalidError("artifact_missing", str(exc)) from exc
                except (IndexUnavailableError, StorageUnavailableError) as exc:
                    self._retry_later(row, str(exc))
                    metrics.P10_INDEX_UNAVAILABLE.labels(role=role).inc()
                    raise WaitingIndexError("unavailable", str(exc)) from exc
                self.hook("indexer.before_bookkeeping", subject=subject_id, seq=s)
                res = self._bookkeep(row, s, done, routed, applied)
                res.detected = detected
                if res.state != "SUPERSEDED":
                    return res
            raise LwtUnknownError("apply did not converge")
        finally:
            self.p.coord.release(lease, token)

    def _apply_to(self, row: dict[str, Any], s_now: Subject, s: int, todo: list[dict[str, Any]],
                  applied: dict[int, int], covered: set[int], repairing: set[int]) -> tuple[dict[int, int], list[int]]:
        role, sid = row["role"], row["subject_id"]
        op = row["desired_op"]
        n = int(row["segment_count"] or 0)
        detected: list[int] = []
        if op == UPSERT:
            reserve = sum(n for g in todo if g["generation"] not in covered)
            self.capacity.reserve(reserve)
            for g in todo:  # pre-count (C-13): never let this apply heal an unrecorded gap
                p = applied.get(g["generation"])
                if g["state"] in SEARCHED and p is not None and p != s and g["generation"] not in repairing:
                    have = self._count(g, role, sid, p)
                    if have is not None and have < n:
                        self.recovery.detect(row["spec_digest"], role, sid, s_now.content_id, g["generation"],
                                             row_seq=p, expected=n, observed=have, detected_by="apply_precount")
                        detected.append(g["generation"])
            points = self._points(s_now, s)
        done: dict[int, int] = {}
        for g in todo:
            name = g["collection"]
            t0 = time.monotonic()
            try:
                if op == UPSERT:
                    batch = self.p.settings.index.upsert_batch
                    for i in range(0, len(points), batch):
                        self.p.index.upsert(name, points[i:i + batch])
                    self.hook("indexer.after_upsert", subject=sid, seq=s, generation=g["generation"])
                    self.p.index.delete_by_filter(name, subject_seq_lt(role, sid, s))
                    metrics.P10_UPSERT_POINTS.labels(role=role).inc(len(points))
                else:
                    self.hook("indexer.before_delete", subject=sid, seq=s, generation=g["generation"])
                    self.p.index.delete_by_filter(name, subject_seq_lte(role, sid, s))
            except IndexMissingError:
                state = (self.gens.row(row["spec_digest"], role, g["generation"]) or {}).get("state")
                if state in (RETIRED, FAILED):
                    continue  # "not found" is a no-op only because Scylla says so (design §12.4)
                self.gens.set_cols(row["spec_digest"], role, g["generation"], audit_state="REQUESTED")
                log.error(json.dumps({"event": "collection_missing", "generation": g["generation"], "state": state}))
                raise WaitingIndexError("missing", f"collection {name} missing while {state}") from None
            metrics.P10_UPSERT_SECONDS.labels(role=role).observe(time.monotonic() - t0)
            done[g["generation"]] = s
        return done, detected

    def _count(self, g: dict[str, Any], role: str, sid: str, seq: int) -> int | None:
        try:
            return self.p.index.count(g["collection"], subject_seq_eq(role, sid, seq))
        except IndexMissingError:
            return None

    def _points(self, s: Subject, seq: int) -> list[Point]:
        vec = self.p.artifacts.load_subject(s)
        check_unit(vec.vectors)
        flags: set[int] = set()
        rev = 0
        li = self.lowinfo_of(s)
        if li is not None:
            rev, flags = li
        out = []
        for k, t, v in zip(vec.segment_index, vec.pts_s, vec.vectors):
            lowinfo = (int(k) in flags, rev) if s.role == TARGET else None
            out.append(Point(point_id(s.subject_id, seq, int(k)), v, payload_for(s, int(k), float(t), seq, lowinfo)))
        return out

    def _bookkeep(self, row: dict[str, Any], s: int, done: dict[int, int], routed: list[dict[str, Any]],
                  prev: dict[int, int]) -> ApplyResult:
        merged = {**prev, **done}
        complete = all(merged.get(g["generation"]) == s for g in routed)
        state = APPLIED if complete else PENDING
        try:
            res = self.db.lwt("UPDATE {ks}.index_entries SET applied = applied + ?, state = ?, attempts = 0, "
                              "last_error = null, blocked_reason = null, updated_at = ? WHERE spec_digest = ? "
                              "AND role = ? AND bucket = ? AND subject_id = ? IF seq = ?", done, state, self.p.now(),
                              row["spec_digest"], row["role"], row["bucket"], row["subject_id"], s)
            ok = res.applied
        except LwtUnknownError:
            cur = self.entry(row["spec_digest"], row["role"], row["bucket"], row["subject_id"])
            ok = cur is not None and int(cur["seq"]) == s and all((cur["applied"] or {}).get(g) == s for g in done)
        if not ok:
            return ApplyResult("SUPERSEDED", s)  # a newer intent exists: the caller loop applies it
        return ApplyResult(state, s, merged)

    def _block(self, row: dict[str, Any], reason: str, error: str) -> None:
        metrics.P10_ENTRIES_BLOCKED.labels(role=row["role"], reason=reason).inc()
        self.db.lwt("UPDATE {ks}.index_entries SET state = 'BLOCKED', blocked_reason = ?, last_error = ?, "
                    "updated_at = ? WHERE spec_digest = ? AND role = ? AND bucket = ? AND subject_id = ? IF seq = ?",
                    reason, error[:300], self.p.now(), row["spec_digest"], row["role"], row["bucket"],
                    row["subject_id"], row["seq"])

    def _retry_later(self, row: dict[str, Any], error: str) -> None:
        attempts = int(row.get("attempts") or 0) + 1
        delay = min(30 * 2 ** min(attempts - 1, 5), 900)
        state = BLOCKED if attempts >= self.p.settings.index.mutation_max_attempts else PENDING
        self.db.lwt("UPDATE {ks}.index_entries SET attempts = ?, last_error = ?, next_attempt_at = ?, state = ?, "
                    "blocked_reason = ? WHERE spec_digest = ? AND role = ? AND bucket = ? AND subject_id = ? "
                    "IF seq = ?",
                    attempts, error[:300], self.p.now() + timedelta(seconds=delay), state,
                    "retries_exhausted" if state == BLOCKED else None, row["spec_digest"], row["role"], row["bucket"],
                    row["subject_id"], row["seq"])

    def ensure(self, s: Subject, **kw: Any) -> ApplyResult:
        """Intent + apply (task INDEXED step, hint reader, backstop scans, build pages)."""
        row = self.write_intent(s)
        return self.apply(s.spec_digest, s.role, row["bucket"], s.subject_id, **kw)

    # --- content digests (§12.5) ---------------------------------------------------------------

    def artifact_digest(self, s: Subject) -> str:
        vec = self.p.artifacts.load_subject(s)
        li = self.lowinfo_of(s)
        rev, flags = (li if li else (0, set()))
        payloads = [payload_for(s, int(k), float(t), None, ((int(k) in flags), rev) if s.role == TARGET else None)
                    for k, t in zip(vec.segment_index, vec.pts_s)]
        return subject_digest([int(k) for k in vec.segment_index],
                              [v.astype(np.float32).tobytes() for v in vec.vectors],
                              payloads)

    def index_digest(self, collection: str, role: str, subject_id: str, seq: int) -> tuple[str, int]:
        pts = list(self.p.index.scroll_all(collection, subject_seq_eq(role, subject_id, seq), with_vector=True))
        return subject_digest([int(p.payload["k"]) for p in pts],
                              [np.asarray(p.vector, dtype=np.float32).tobytes() for p in pts],
                              [p.payload for p in pts]), len(pts)


def all_subjects(p10: P10, spec: str, role: str) -> list[Subject]:
    """The authoritative walk of one (spec, role): every subject P9 rows know of, live or not."""
    out: list[Subject] = []
    if role == MEDIA:
        for b in range(BUCKETS):
            for r in p10.db.rows("SELECT content_id FROM {ks}.representations_by_spec WHERE spec_digest = ? "
                                 "AND bucket = ?", spec, b):
                out.append(p10.media_subject(r["content_id"], spec))
    else:
        for r in p10.db.stream("SELECT target_id, version, spec_digest FROM {ks}.target_representations"):
            if r["spec_digest"] == spec:
                out.append(p10.target_subject(r["target_id"], r["version"], spec))
    return [s for s in out if s.subject_id]


class Builder:
    """Rebuild a (spec, role) as a new generation (design §12.4-§12.5): admission, build, catch-up,
    validation; promotion is a separate, explicit step."""

    def __init__(self, p10: P10, gens: Generations, indexer: Indexer, capacity: Capacity) -> None:
        self.p = p10
        self.gens = gens
        self.indexer = indexer
        self.capacity = capacity
        self.hook = p10.hook

    def build(self, spec: str, role: str, *, params: GenerationParams | None = None,
              wait_boundary: bool = True) -> dict[str, Any]:
        epoch = self.gens.acquire_lease(spec, role)
        if epoch is None:
            return {"state": "LEASE_HELD", "detail": "another builder holds the build lease"}
        try:
            g = self._adopt(spec, role, epoch)
            if g is None:
                g = self._admit_and_create(spec, role, epoch, params or GenerationParams())
                if g is None:
                    return {"state": "CAPACITY_BLOCKED", **self._requirement(spec, role)}
            return self._drive(spec, role, g, epoch, wait_boundary)
        finally:
            self.gens.release_lease(spec, role, epoch)

    def _requirement(self, spec: str, role: str) -> dict[str, Any]:
        points = sum(s.segment_count for s in all_subjects(self.p, spec, role) if s.live)
        led = self.capacity.read()
        projected = Capacity.projected(points, self.p.settings.index.rebuild_headroom)
        return {"projected": projected, "reserved": led["reserved"], "limit": self.capacity.limit,
                "needed_total": led["reserved"] + projected}

    def _adopt(self, spec: str, role: str, epoch: int) -> int | None:
        _, gens = self.gens.partition(spec, role)
        live = [g for g, r in sorted(gens.items()) if r["state"] in NON_TERMINAL_BUILD]
        if not live:
            return None
        g = live[-1]
        old = gens[g]["build_epoch"]
        self.p.db.lwt("UPDATE {ks}.index_generations SET build_epoch = ? WHERE spec_digest = ? AND role = ? "
                      "AND generation = ? IF build_epoch = ?", epoch, spec, role, g, old)
        return g

    def _admit_and_create(self, spec: str, role: str, epoch: int, params: GenerationParams) -> int | None:
        req = self._requirement(spec, role)
        g = self.gens.next_number(spec, role)
        if not self.capacity.admit_rebuild(holder_of(spec, role, g), req["projected"]):
            log.warning(json.dumps({"event": "rebuild_capacity_blocked", "spec": spec, "role": role, **req}))
            return None
        self.hook("builder.after_admission", generation=g)
        if not self.gens.create(spec, role, g, params, epoch, req["projected"]):
            raise RuntimeError("generation create lost a race under the build lease")
        row = self.gens.row(spec, role, g)
        self.p.index.create_collection(row["collection"], params, role)
        self.gens.transition(spec, role, g, CREATED, BUILDING, build_epoch=epoch)
        return g

    def _drive(self, spec: str, role: str, g: int, epoch: int, wait_boundary: bool) -> dict[str, Any]:
        for _ in range(20):
            row = self.gens.row(spec, role, g)
            state = row["state"]
            if state == CREATED:
                if not self.p.index.collection_exists(row["collection"]):
                    self.p.index.create_collection(row["collection"], GenerationParams.from_document(
                        json.loads(row["params"])), role)
                self.gens.transition(spec, role, g, CREATED, BUILDING, build_epoch=epoch)
            elif state == BUILDING:
                self._bulk(spec, role, g, row, epoch)
                self.gens.transition(spec, role, g, BUILDING, CATCHING_UP, build_epoch=epoch)
            elif state == CATCHING_UP:
                if wait_boundary:
                    while self.p.now() < utc(row["boundary_at"]):
                        self.gens.renew_lease(spec, role, epoch)
                        time.sleep(1.0)
                self.catch_up(spec, role, g)
                self.gens.transition(spec, role, g, CATCHING_UP, VALIDATING, build_epoch=epoch)
            elif state == VALIDATING:
                report = self.validate(spec, role, g)
                if report["pass"]:
                    self.gens.transition(spec, role, g, VALIDATING, READY, build_epoch=epoch,
                                         manifest_digest=report["manifest_digest"],
                                         validated_detect_count=int(self.gens.row(spec, role, g)["detect_count"] or 0),
                                         validated_at=self.p.now(), ready_at=self.p.now(),
                                         actual_count=report["total_points"])
                    return {"state": READY, "generation": g, **report}
                if report.get("lag"):
                    self.gens.transition(spec, role, g, VALIDATING, CATCHING_UP, build_epoch=epoch)
                else:
                    self.gens.transition(spec, role, g, VALIDATING, FAILED, build_epoch=epoch,
                                         failure=json.dumps(report.get("failures"))[:2000])
                    return {"state": FAILED, "generation": g, **report}
            elif state == READY:
                return {"state": READY, "generation": g}
            else:
                return {"state": state, "generation": g}
            self.gens.renew_lease(spec, role, epoch)
        return {"state": "STUCK", "generation": g}

    def _bulk(self, spec: str, role: str, g: int, row: dict[str, Any], epoch: int) -> None:
        cursor = {int(k): v for k, v in (row.get("build_cursor") or {}).items()}
        subjects = all_subjects(self.p, spec, role)
        by_bucket: dict[int, list[Subject]] = defaultdict(list)
        for s in subjects:
            by_bucket[s.bucket].append(s)
        for b in range(BUCKETS):
            if cursor.get(b) == "done":
                continue
            for s in sorted(by_bucket.get(b, []), key=lambda x: x.subject_id):
                try:
                    self.indexer.ensure(s, covered_gens={g})
                except SubjectInvalidError as exc:
                    log.error(json.dumps({"event": "build_subject_invalid", "subject": s.subject_id,
                                          "reason": exc.reason}))
            if not self.p.db.lwt("UPDATE {ks}.index_generations SET build_cursor[?] = 'done' WHERE spec_digest = ? "
                                 "AND role = ? AND generation = ? IF build_epoch = ?", b, spec, role, g, epoch).applied:
                raise RuntimeError("build lease lost (build_epoch changed)")
            metrics.P10_BUILD_PROGRESS.labels(spec=spec[:19], role=role, generation=str(g)).set(b + 1)
            self.hook("builder.after_bucket", generation=g, bucket=b)
            self.gens.renew_lease(spec, role, epoch)

    def catch_up(self, spec: str, role: str, g: int) -> int:
        """One full pass over the (spec, role) rows; every row not at ``applied[g] = seq`` is applied by
        the pass itself and re-checked (terminates under continuous writes)."""
        fixed = 0
        for b in range(BUCKETS):
            for e in self.indexer.entries(spec, role, b):
                if (e["applied"] or {}).get(g) == e["seq"]:
                    continue
                for _ in range(5):
                    try:
                        self.indexer.apply(spec, role, b, e["subject_id"], covered_gens={g})
                    except SubjectInvalidError:
                        break
                    cur = self.indexer.entry(spec, role, b, e["subject_id"])
                    if (cur["applied"] or {}).get(g) == cur["seq"]:
                        fixed += 1
                        break
        return fixed

    # --- validation (§12.5) -------------------------------------------------------------------

    def validate(self, spec: str, role: str, g: int) -> dict[str, Any]:
        row = self.gens.row(spec, role, g)
        name = row["collection"]
        params = GenerationParams.from_document(json.loads(row["params"]))
        failures: list[str] = []
        lag = False
        entries: dict[str, dict[str, Any]] = {}
        for b in range(BUCKETS):
            for e in self.indexer.entries(spec, role, b):
                entries[e["subject_id"]] = e
        # completeness: every row at applied[g] = seq
        behind = [sid for sid, e in entries.items() if (e["applied"] or {}).get(g) != e["seq"]]
        if behind:
            lag = True
            failures.append(f"completeness: {len(behind)} rows not applied to g{g}")
        # authoritative coverage
        for s in all_subjects(self.p, spec, role):
            e = entries.get(s.subject_id)
            if s.live and (e is None or e["desired_op"] != UPSERT):
                lag = True
                failures.append(f"coverage: live {s.subject_id} has no UPSERT row")
            elif not s.live and e is not None and e["desired_op"] == UPSERT:
                lag = True
                failures.append(f"coverage: dead {s.subject_id} still UPSERT")
        # manifest + content digests
        total = 0
        bucket_digests: dict[int, list[str]] = defaultdict(list)
        for sid, e in entries.items():
            if e["desired_op"] != UPSERT:
                continue
            subject = self.p.refresh(Subject.from_meta(e["subject"]))
            idx_digest, n = self.indexer.index_digest(name, role, sid, int(e["seq"]))
            total += n
            if n != int(e["segment_count"] or 0):
                lag = True
                failures.append(f"manifest: {sid} has {n} of {e['segment_count']} points")
                continue
            art = self.indexer.artifact_digest(subject)
            if art != idx_digest:
                failures.append(f"content digest mismatch for {sid}")
            bucket_digests[int(e["bucket"])].append(art)
        # no extras (scroll every point)
        seen_stale = 0
        for pt in self.p.index.scroll_all(name, page=512):
            sid = pt.payload.get(SUBJECT_FIELD[role])
            e = entries.get(sid)
            seq = int(pt.payload.get("seq", -1))
            if e is None or e["desired_op"] != UPSERT or seq > int(e["seq"]):
                lag = True
                failures.append(f"extra point {pt.id} of {sid} seq {seq}")
                if e is not None and seq > int(e["seq"]):
                    log.error(json.dumps({"event": "invariant_violation_seq_above_row", "subject": sid}))
            elif seq < int(e["seq"]):
                seen_stale += 1
                self.p.index.delete_by_filter(name, subject_seq_lt(role, sid, int(e["seq"])))
        if seen_stale:
            lag = True
            failures.append(f"stale points removed: {seen_stale} (re-check)")
        # classification revision (target role)
        if role == TARGET and self.indexer.lowinfo is not None:
            for sid, e in entries.items():
                if e["desired_op"] == UPSERT and not str(e["desired_ver"]).endswith(
                        f"+li{(self.indexer.lowinfo_of(Subject.from_meta(e['subject'])) or (0, set()))[0]}"):
                    lag = True
                    failures.append(f"classification: {sid} payload revision behind")
        # params
        info = self.p.index.collection_info(name)
        cfg = info.get("config", {})
        hnsw = cfg.get("hnsw_config", {})
        vec = cfg.get("params", {}).get("vectors", {})
        if hnsw.get("m") != params.hnsw_m or hnsw.get("ef_construct") != params.ef_construct or \
                vec.get("size") != params.dim or str(vec.get("distance")).lower() != params.distance.lower():
            failures.append("params: collection config differs from the recorded params")
        # capacity
        exact = self.p.index.count(name)
        if exact > int(row["reserved_points"] or 0):
            failures.append(f"capacity: {exact} points > reserved {row['reserved_points']}")
        # accessibility
        if not self.p.index.health().get("ok"):
            failures.append("accessibility: index not healthy")
        manifest = hashlib.sha256()
        for b in sorted(bucket_digests):
            manifest.update(hashlib.sha256("".join(sorted(bucket_digests[b])).encode()).digest())
        ok = not failures
        return {"pass": ok, "lag": lag, "failures": failures[:50], "total_points": total, "exact_count": exact,
                "subjects": sum(1 for e in entries.values() if e["desired_op"] == UPSERT),
                "manifest_digest": manifest.hexdigest()}

    def manifest_digest(self, spec: str, role: str) -> str:
        """The digest an artifact-only build would have (``index check``, §5 acceptance (a))."""
        bucket_digests: dict[int, list[str]] = defaultdict(list)
        for s in all_subjects(self.p, spec, role):
            if s.live:
                bucket_digests[s.bucket].append(self.indexer.artifact_digest(s))
        manifest = hashlib.sha256()
        for b in sorted(bucket_digests):
            manifest.update(hashlib.sha256("".join(sorted(bucket_digests[b])).encode()).digest())
        return manifest.hexdigest()


class Checker:
    """The Qdrant-side verification pass (design §12.6) and DRAINING retirement (§12.4)."""

    def __init__(self, p10: P10, gens: Generations, indexer: Indexer, recovery: Recovery) -> None:
        self.p = p10
        self.gens = gens
        self.indexer = indexer
        self.recovery = recovery
        self.hook = p10.hook

    def verify_generation(self, spec: str, role: str, g: int) -> dict[str, int]:
        row = self.gens.row(spec, role, g)
        if row is None or row["state"] in (RETIRED, FAILED, CREATED, BUILDING):
            return {}
        started = self.p.now()
        self.gens.set_cols(spec, role, g, verify_started_at=started)
        name, state = row["collection"], row["state"]
        out = {"rows": 0, "short": 0, "stale": 0, "orphans": 0, "deleted_rows_points": 0, "above_row": 0}
        entries: dict[str, dict[str, Any]] = {}
        for b in range(BUCKETS):  # (1) per-subject counts of the current seq
            for e in self.indexer.entries(spec, role, b):
                entries[e["subject_id"]] = e
                out["rows"] += 1
                applied = (e["applied"] or {}).get(g)
                if e["desired_op"] == UPSERT and applied == e["seq"]:
                    n = self.p.index.count(name, subject_seq_eq(role, e["subject_id"], int(e["seq"])))
                    if n < int(e["segment_count"] or 0):
                        cur = self.indexer.entry(spec, role, b, e["subject_id"])  # re-check once
                        n2 = self.p.index.count(name, subject_seq_eq(role, e["subject_id"], int(cur["seq"])))
                        if cur["seq"] == e["seq"] and (cur["applied"] or {}).get(g) == cur["seq"] and \
                                n2 >= int(cur["segment_count"] or 0):
                            continue
                        out["short"] += 1
                        self.hook("verifier.after_count_before_obligation", subject=e["subject_id"], generation=g)
                        if state in SEARCHED:
                            meta = Subject.from_meta(e["subject"])
                            self.recovery.detect(spec, role, e["subject_id"], meta.content_id, g, row_seq=int(e["seq"]),
                                                 expected=int(e["segment_count"]), observed=min(n, n2),
                                                 detected_by="verification")
                        else:  # READY: only the promotion fence
                            c = int((self.gens.row(spec, role, g) or {}).get("detect_count") or 0)
                            self.p.db.lwt("UPDATE {ks}.index_generations SET detect_count = ? WHERE spec_digest = ? "
                                          "AND role = ? AND generation = ? IF detect_count = ?",
                                          c + 1, spec, role, g, c)
                elif e["desired_op"] == "DELETE" and applied is not None:
                    n = self.p.index.count(name, subject_seq_lte(role, e["subject_id"], int(e["seq"])))
                    if n:
                        self.p.index.delete_by_filter(name, subject_seq_lte(role, e["subject_id"], int(e["seq"])))
                        out["deleted_rows_points"] += n
                        metrics.P10_VERIFY_STALE.labels(role=role).inc(n)
        stale_subjects: set[str] = set()
        orphan_ids: list[str] = []
        for pt in self.p.index.scroll_all(name, page=self.p.settings.matcher_reconcile.verify_page):  # (2) extras
            sid = pt.payload.get(SUBJECT_FIELD[role])
            seq = int(pt.payload.get("seq", -1))
            e = entries.get(sid)
            if e is None:
                orphan_ids.append(pt.id)
            elif e["desired_op"] == UPSERT and seq < int(e["seq"]):
                stale_subjects.add(sid)
            elif e["desired_op"] == UPSERT and seq > int(e["seq"]):
                out["above_row"] += 1
                log.error(json.dumps({"event": "invariant_violation_seq_above_row", "subject": sid, "point": pt.id}))
        for sid in stale_subjects:
            e = entries[sid]
            before = self.p.index.count(name, subject_seq_lt(role, sid, int(e["seq"])))
            self.p.index.delete_by_filter(name, subject_seq_lt(role, sid, int(e["seq"])))
            out["stale"] += before
            metrics.P10_VERIFY_STALE.labels(role=role).inc(before)
        if orphan_ids:
            out["orphans"] = self._orphans(spec, role, name, orphan_ids)
        if out["short"] == 0:
            self.gens.set_cols(spec, role, g, last_clean_verify_at=self.p.now())
            self.recovery.clear_suspect(spec, role, g, started)
        return out

    def _orphans(self, spec: str, role: str, name: str, ids: list[str]) -> int:
        metrics.P10_VERIFY_ORPHANS.labels(role=role).inc(len(ids))
        pts = self.p.index.retrieve(name, ids, with_vector=False)
        delete: list[str] = []
        for pt in pts:
            meta = None
            if role == MEDIA and pt.payload.get("cid"):
                meta = self.p.media_subject(pt.payload["cid"], spec)
            elif role == TARGET and pt.payload.get("tid") is not None:
                meta = self.p.target_subject(pt.payload["tid"], int(pt.payload.get("tver") or 0), spec)
            if meta is not None and meta.live and meta.subject_id == pt.payload.get(SUBJECT_FIELD[role]):
                self.indexer.ensure(meta)   # adopt: intent + apply (the orphan id itself is then stale or current)
            else:
                delete.append(pt.id)
        self.p.index.delete_ids(name, delete)
        return len(ids)

    def retire_draining(self, spec: str, role: str, g: int, *, unauditable: bool = False) -> bool:
        """DRAINING → RETIRED after the grace and a clean verification pass that started after the
        search horizon — or, if g cannot be read, after the durable unauditable-fallback occurrences."""
        row = self.gens.row(spec, role, g)
        if row is None or row["state"] != DRAINING:
            return False
        now = self.p.now()
        grace = timedelta(seconds=self.p.settings.index.drain_grace_s)
        horizon = utc(row.get("drain_horizon_at"))
        # the grace counts from the moment g started draining (its search horizon was recorded then);
        # a DRAINING row's own promoted_at is when it became ACTIVE, long before
        if horizon is None or now < horizon or now < horizon + grace:
            return False
        if unauditable or not self.p.index.collection_exists(row["collection"]):
            self.recovery.unauditable_fallback(spec, role, g)
        else:
            res = self.verify_generation(spec, role, g)
            started = utc((self.gens.row(spec, role, g) or {}).get("verify_started_at"))
            if horizon is not None and (started is None or started < horizon):
                return False
            if res.get("short"):
                return False  # occurrences opened; retire after they are repaired (repair confirmed by RETIRED)
        self.p.index.delete_collection(row["collection"])
        return self.gens.transition(spec, role, g, DRAINING, RETIRED, build_epoch=None)


def ready_gens(gens: dict[int, dict[str, Any]]) -> list[int]:
    return [g for g, r in gens.items() if r["state"] == READY]


def is_searched(state: str) -> bool:
    return state in (ACTIVE, DRAINING)
