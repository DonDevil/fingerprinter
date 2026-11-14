"""Index-integrity recovery: occurrences, repair and task reruns (design §12.10; P10-D19/D20; C-13…C-15).

A non-protocol loss of a live subject S's current points from a searched
generation g opens an **occurrence**: its own ``index_repairs`` row (CK
``subject_id, generation, occurrence``; ``occurrence DESC``), with a fresh
``occurrence_id`` timeuuid, created once by ``INSERT … IF NOT EXISTS`` and never
reset or deleted. Every transition is a single-row LWT on that row:

  open → (merge)* → mark → REPAIRED → RERUN_PENDING → CLOSED [→ resolve]

The latest occurrence of (S, g) is found with the ``LOCAL_SERIAL`` slice read
``… LIMIT 1`` — validated on Scylla 6.2.3 by validation §9a.4
(``tests/p10/test_scylla_semantics.py``); every step acting on its result is
still guarded by its own single-row condition.

Reruns: S's own tasks are bumped (``recovery_runs[occurrence_id]``, single-row
LWT on the task row, design §11.1) only after REPAIRED; an occurrence closes
when every required kind finished a run at or after its requirement, and is
*resolved* only when each kind has a DONE run at or after it (or S retired).
Nothing here gates other subjects' tasks (R4); their negatives are
provisional until ``recovery_open = 0`` and ``recovery_unresolved = 0``.
"""

from __future__ import annotations

import contextlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from fingerprinter import metrics
from fingerprinter.coord import utc
from fingerprinter.db import SERIAL, LwtUnknownError
from fingerprinter.generations import FAILED, RETIRED, SEARCHED, Generations
from fingerprinter.p10ctx import BUCKETS, KIND_A, KIND_B, KIND_EQ, P10, Subject, bucket_of
from fingerprinter.vindex import MEDIA, subject_seq_eq

OPEN, REPAIRED, RERUN_PENDING, CLOSED = "OPEN", "REPAIRED", "RERUN_PENDING", "CLOSED"
PENDING, RESOLVED = "PENDING", "RESOLVED"
RERUN_DONE, RERUN_INCOMPLETE, TASK_FAILED, SUBJECT_RETIRED, LATER_RUN = (
    "RERUN_DONE", "RERUN_INCOMPLETE", "TASK_FAILED", "SUBJECT_RETIRED", "LATER_RUN")
TASK_NON_FAILED = ["PENDING", "INDEXED", "SEARCHED", "VERIFYING", "DONE", "INCOMPLETE", "WAITING_INDEX"]

KEY = "spec_digest = ? AND role = ? AND bucket = ? AND subject_id = ? AND generation = ?"


def _no_hook(point: str, **ctx: Any) -> None:
    return None


@dataclass
class Occurrence:
    spec: str
    role: str
    bucket: int
    subject_id: str
    generation: int
    occurrence: int
    occurrence_id: uuid.UUID
    row: dict[str, Any] = field(default_factory=dict)

    def key(self) -> tuple:
        return (self.spec, self.role, self.bucket, self.subject_id, self.generation, self.occurrence)


def kinds_for(role: str, equivalence_enabled: bool) -> list[str]:
    if role == MEDIA:
        return [KIND_A] + ([KIND_EQ] if equivalence_enabled else [])
    return [KIND_B]


class Recovery:
    def __init__(self, p10: P10, gens: Generations) -> None:
        self.p = p10
        self.db = p10.db
        self.gens = gens
        self.indexer: Any = None   # set by the wiring (repair re-applies through the mutation protocol)
        self.tasks: Any = None     # set by the wiring (due index for bumped runs)
        self.hook = p10.hook

    # --- reads ---------------------------------------------------------------------------

    def latest(self, spec: str, role: str, bucket: int, subject_id: str, g: int) -> dict[str, Any] | None:
        """The highest occurrence of (S, g): LOCAL_SERIAL slice read, ``LIMIT 1`` (validated, §9a.4)."""
        return self.db.one(f"SELECT * FROM {{ks}}.index_repairs WHERE {KEY} LIMIT 1",  # noqa: S608
                           spec, role, bucket, subject_id, g, level=SERIAL)

    def read(self, occ: Occurrence) -> dict[str, Any] | None:
        return self.db.one(f"SELECT * FROM {{ks}}.index_repairs WHERE {KEY} AND occurrence = ?",  # noqa: S608
                           occ.spec, occ.role, occ.bucket, occ.subject_id, occ.generation, occ.occurrence,
                           level=SERIAL)

    def occurrences(self, spec: str, role: str | None = None) -> list[dict[str, Any]]:
        """Every occurrence row of the spec (both roles unless one is named) — a full scan of 64 buckets."""
        out: list[dict[str, Any]] = []
        for r in ((role,) if role else ("media", "target")):
            for b in range(BUCKETS):
                out.extend(self.db.rows("SELECT * FROM {ks}.index_repairs WHERE spec_digest = ? AND role = ? "
                                        "AND bucket = ?", spec, r, b))
        return out

    # --- detection (open / merge) ----------------------------------------------------------

    def detect(self, spec: str, role: str, subject_id: str, content_id: str, g: int, *, row_seq: int,
               expected: int, observed: int, detected_by: str) -> Occurrence:
        """Make a loss of (S, g) durable: merge into an OPEN latest occurrence, else open the next one.
        Returns only once the occurrence is durable (callers advance cursors / repair after this)."""
        bucket = bucket_of(role, subject_id, content_id)
        for _ in range(100):
            cur = self.latest(spec, role, bucket, subject_id, g)
            if cur is not None and cur["state"] == OPEN:
                d = int(cur["detections"] or 0)
                try:
                    res = self.db.lwt(
                        f"UPDATE {{ks}}.index_repairs SET detections = ?, observed_count = ? WHERE {KEY} "  # noqa: S608
                        "AND occurrence = ? IF occurrence_id = ? AND state = 'OPEN' AND detections = ?",
                        d + 1, observed, spec, role, bucket, subject_id, g, cur["occurrence"], cur["occurrence_id"], d)
                    applied = res.applied
                except LwtUnknownError:
                    row = self.latest(spec, role, bucket, subject_id, g)
                    applied = row is not None and row["occurrence"] == cur["occurrence"] and row["state"] == OPEN \
                        and int(row["detections"] or 0) > d
                if applied:
                    metrics.P10_INDEX_MISS.labels(role=role).inc()
                    return Occurrence(spec, role, bucket, subject_id, g, cur["occurrence"], cur["occurrence_id"])
                continue  # changed meanwhile (merged by another, or moved past OPEN): re-read
            n = 1 if cur is None else int(cur["occurrence"]) + 1
            oid = uuid.uuid1()
            now = self.p.now()
            try:
                res = self.db.lwt(
                    "INSERT INTO {ks}.index_repairs (spec_digest, role, bucket, subject_id, generation, occurrence, "
                    "occurrence_id, state, detections, gen_marked, detected_at, detected_by, row_seq, expected_count, "
                    "observed_count, required_run, resolution, resolved_by, attempts) "
                    "VALUES (?,?,?,?,?,?,?,'OPEN',0,false,?,?,?,?,?,?,'PENDING',?,0) IF NOT EXISTS",
                    spec, role, bucket, subject_id, g, n, oid, now, detected_by, row_seq, expected, observed, {}, {})
                applied = res.applied
            except LwtUnknownError:
                row = self.read(Occurrence(spec, role, bucket, subject_id, g, n, oid))
                applied = row is not None and row["occurrence_id"] == oid  # the actor's own id decides
            if applied:
                self.hook("recovery.after_open", subject=subject_id, generation=g, occurrence=n)
                metrics.P10_INDEX_MISS.labels(role=role).inc()
                self._log("occurrence_open", spec=spec, role=role, subject=subject_id, generation=g, occurrence=n,
                          occurrence_id=str(oid), detected_by=detected_by, expected=expected, observed=observed)
                return Occurrence(spec, role, bucket, subject_id, g, n, oid)
        raise LwtUnknownError("occurrence detection did not converge")

    # --- the repairs pass --------------------------------------------------------------------

    def step(self, row: dict[str, Any]) -> str:
        """Drive one occurrence row one or more transitions forward. Returns its state afterwards."""
        occ = Occurrence(row["spec_digest"], row["role"], row["bucket"], row["subject_id"], row["generation"],
                         row["occurrence"], row["occurrence_id"], row)
        for _ in range(6):
            row = self.read(occ)
            if row is None:
                return "missing"
            occ.row = row
            state = row["state"]
            if state == CLOSED:
                if row["resolution"] == PENDING:  # an unresolved close is re-checked on every pass
                    self._resolve(occ)
                return CLOSED
            nxt = row.get("next_attempt_at")
            if nxt is not None and utc(nxt) > self.p.now():
                return state
            try:
                if state == OPEN and not self._repair(occ):
                    return OPEN
                if state == REPAIRED:
                    self._rerun_pending(occ)
                elif state == RERUN_PENDING and not self._close(occ):
                    return RERUN_PENDING
            except Exception as exc:  # noqa: BLE001 - recorded on the row, retried with backoff, never dropped
                self._failed_attempt(occ, exc)
                return state
        return occ.row.get("state", "?")

    def _failed_attempt(self, occ: Occurrence, exc: Exception) -> None:
        rc = self.p.settings.matcher_reconcile
        attempts = int(occ.row.get("attempts") or 0) + 1
        delay = min(rc.repair_backoff_base_s * 2 ** (attempts - 1), rc.repair_backoff_max_s)
        self.db.lwt(f"UPDATE {{ks}}.index_repairs SET attempts = ?, next_attempt_at = ?, last_error = ? "  # noqa: S608
                    f"WHERE {KEY} AND occurrence = ? IF occurrence_id = ?", attempts,
                    self.p.now() + timedelta(seconds=delay), f"{type(exc).__name__}: {exc}"[:300],
                    occ.spec, occ.role, occ.bucket, occ.subject_id, occ.generation, occ.occurrence, occ.occurrence_id)
        self._log("occurrence_step_failed", subject=occ.subject_id, occurrence=occ.occurrence, attempts=attempts,
                  error=f"{type(exc).__name__}: {exc}"[:200])

    def _subject(self, occ: Occurrence) -> tuple[Subject | None, dict[str, Any] | None]:
        entry = self.indexer.entry(occ.spec, occ.role, occ.bucket, occ.subject_id)
        if entry is None or not entry.get("subject"):
            return None, entry
        return self.p.refresh(Subject.from_meta(entry["subject"])), entry

    def mark(self, occ: Occurrence) -> None:
        """Generation flag and promotion fence (``detect_count``), then ``gen_marked``."""
        for _ in range(50):
            g = self.gens.row(occ.spec, occ.role, occ.generation)
            if g is None:
                break
            c = int(g["detect_count"] or 0)
            res = self.db.lwt("UPDATE {ks}.index_generations SET detect_count = ?, suspect_since = ?, "
                              "suspect_reason = ? WHERE spec_digest = ? AND role = ? AND generation = ? "
                              "IF detect_count = ?",
                              c + 1, utc(g.get("suspect_since")) or self.p.now(),
                              f"loss of {occ.subject_id} (occurrence {occ.occurrence})", occ.spec, occ.role,
                              occ.generation, c)
            if res.applied:
                metrics.P10_GENERATION_SUSPECT.labels(spec=occ.spec[:19], role=occ.role,
                                                      generation=str(occ.generation)).set(1)
                break
        self.db.lwt(f"UPDATE {{ks}}.index_repairs SET gen_marked = true WHERE {KEY} AND occurrence = ? "  # noqa: S608
                    "IF occurrence_id = ? AND state = 'OPEN'", occ.spec, occ.role, occ.bucket, occ.subject_id,
                    occ.generation, occ.occurrence, occ.occurrence_id)

    def _repair(self, occ: Occurrence) -> bool:
        row = occ.row
        if not row["gen_marked"]:
            self.mark(occ)
            row = self.read(occ) or row
            occ.row = row
            if row["state"] != OPEN or not row["gen_marked"]:
                return False
        subject, entry = self._subject(occ)
        if subject is None or not subject.live:
            return self._close_retired(occ)
        g = self.gens.row(occ.spec, occ.role, occ.generation)
        if g is not None and g["state"] not in (RETIRED, FAILED):
            # re-apply the current desired state to g through the mutation protocol (capacity included);
            # the pre-count of this write is satisfied by the OPEN occurrence we hold
            self.indexer.write_intent(subject)
            self.indexer.apply(occ.spec, occ.role, subject.bucket, occ.subject_id,
                               force_gens={occ.generation}, repairing={occ.generation})
            self.hook("reconciler.after_repair_write", subject=occ.subject_id, occurrence=occ.occurrence)
        fresh = self.read(occ)
        d = int(fresh["detections"] or 0)
        if g is not None and g["state"] not in (RETIRED, FAILED):
            entry = self.indexer.entry(occ.spec, occ.role, occ.bucket, occ.subject_id)
            n = self.p.index.count(g["collection"], subject_seq_eq(occ.role, occ.subject_id, int(entry["seq"])))
            if n != int(entry["segment_count"] or 0):
                raise RuntimeError(f"repair not confirmed: {n} of {entry['segment_count']} points")
        self.hook("reconciler.after_confirm_before_repaired", subject=occ.subject_id, occurrence=occ.occurrence)
        try:
            ok = self.db.lwt(f"UPDATE {{ks}}.index_repairs SET state = 'REPAIRED', repaired_at = ? WHERE {KEY} "  # noqa: S608
                             "AND occurrence = ? IF occurrence_id = ? AND state = 'OPEN' AND gen_marked = true "
                             "AND detections = ?", self.p.now(), occ.spec, occ.role, occ.bucket, occ.subject_id,
                             occ.generation, occ.occurrence, occ.occurrence_id, d).applied
        except LwtUnknownError:
            ok = (self.read(occ) or {}).get("state") != OPEN
        if ok and occ.role == "target" and self.indexer.lowinfo is not None:
            self.indexer.lowinfo.request(occ.spec, reason=f"repaired {occ.subject_id}")
        return ok

    def _rerun_pending(self, occ: Occurrence) -> None:
        subject, _ = self._subject(occ)
        if subject is None or not subject.live:
            self._close_retired(occ)
            return
        required: dict[str, int] = {}
        for kind in self._kinds(occ, subject):
            self.hook("reconciler.before_bump", subject=occ.subject_id, occurrence=occ.occurrence, kind=kind)
            required[kind] = self.tasks.bump_for_occurrence(subject, kind, occ.occurrence_id)
        self.hook("reconciler.after_bump_before_record", subject=occ.subject_id, occurrence=occ.occurrence)
        # unknown outcome: the next pass re-reads; the bump itself is idempotent per occurrence id
        with contextlib.suppress(LwtUnknownError):
            self.db.lwt(f"UPDATE {{ks}}.index_repairs SET required_run = ?, state = 'RERUN_PENDING' WHERE {KEY} "  # noqa: S608
                        "AND occurrence = ? IF occurrence_id = ? AND state = 'REPAIRED'", required, occ.spec, occ.role,
                        occ.bucket, occ.subject_id, occ.generation, occ.occurrence, occ.occurrence_id)

    def _kinds(self, occ: Occurrence, subject: Subject) -> list[str]:
        kinds = set(kinds_for(occ.role, self.p.settings.matching.equivalence_enabled))
        for t in self.tasks.rows_for_subject(subject):
            if t["kind"] in (KIND_A, KIND_B, KIND_EQ):
                kinds.add(t["kind"])
        return sorted(kinds)

    def _kind_status(self, subject: Subject, required: dict[str, int]) -> tuple[bool, bool, bool, dict[str, int]]:
        """(all finished, all satisfied, some unsatisfied kind FAILED, qualifying runs)."""
        finished, satisfied, failed = True, True, False
        qualifying: dict[str, int] = {}
        for kind, req in required.items():
            t = self.tasks.row(subject.subject_id, subject.spec_digest, kind)
            done_run = int((t or {}).get("done_run") or 0)
            ok_run = int((t or {}).get("done_ok_run") or 0)
            is_failed = (t or {}).get("state") == "FAILED"
            if not (done_run >= req or (is_failed and done_run < req)):
                finished = False
            if ok_run >= req:
                qualifying[kind] = ok_run
            else:
                satisfied = False
                failed = failed or is_failed
        return finished, satisfied, failed, qualifying

    def _close(self, occ: Occurrence) -> bool:
        subject, _ = self._subject(occ)
        if subject is None or not subject.live:
            return self._close_retired(occ)
        required = dict(occ.row.get("required_run") or {})
        finished, satisfied, failed, qualifying = self._kind_status(subject, required)
        if not finished:
            return False
        reason = RERUN_DONE if satisfied else (TASK_FAILED if failed else RERUN_INCOMPLETE)
        self.hook("reconciler.before_close", subject=occ.subject_id, occurrence=occ.occurrence)
        now = self.p.now()
        if reason == RERUN_DONE:
            cql = (f"UPDATE {{ks}}.index_repairs SET state = 'CLOSED', close_reason = ?, closed_at = ?, "  # noqa: S608
                   f"resolution = 'RESOLVED', resolution_reason = ?, resolved_by = ?, resolved_at = ? WHERE {KEY} "
                   "AND occurrence = ? IF occurrence_id = ? AND state = 'RERUN_PENDING'")
            args = [reason, now, reason, qualifying, now]
        else:
            cql = (f"UPDATE {{ks}}.index_repairs SET state = 'CLOSED', close_reason = ?, closed_at = ? WHERE {KEY} "  # noqa: S608
                   "AND occurrence = ? IF occurrence_id = ? AND state = 'RERUN_PENDING'")
            args = [reason, now]
        try:
            ok = self.db.lwt(cql, *args, occ.spec, occ.role, occ.bucket, occ.subject_id, occ.generation,
                             occ.occurrence, occ.occurrence_id).applied
        except LwtUnknownError:
            ok = (self.read(occ) or {}).get("state") == CLOSED
        if ok:
            metrics.P10_REPAIRS_CLOSED.labels(close_reason=reason).inc()
            self._log("occurrence_closed", subject=occ.subject_id, occurrence=occ.occurrence, reason=reason,
                      resolved=reason == RERUN_DONE)
        return ok

    def _close_retired(self, occ: Occurrence) -> bool:
        now = self.p.now()
        row = occ.row
        if row.get("state") == CLOSED:
            return self._resolve(occ)
        try:
            ok = self.db.lwt(f"UPDATE {{ks}}.index_repairs SET state = 'CLOSED', close_reason = ?, closed_at = ?, "  # noqa: S608
                             f"resolution = 'RESOLVED', resolution_reason = ?, resolved_at = ? WHERE {KEY} AND "
                             "occurrence = ? IF occurrence_id = ? AND state IN ('OPEN', 'REPAIRED', 'RERUN_PENDING')",
                             SUBJECT_RETIRED, now, SUBJECT_RETIRED, now, occ.spec, occ.role, occ.bucket,
                             occ.subject_id, occ.generation, occ.occurrence, occ.occurrence_id).applied
        except LwtUnknownError:
            ok = (self.read(occ) or {}).get("state") == CLOSED
        if ok:
            metrics.P10_REPAIRS_CLOSED.labels(close_reason=SUBJECT_RETIRED).inc()
        return ok

    def _resolve(self, occ: Occurrence) -> bool:
        """An unresolved close becomes resolved exactly once, by its own LWT, with its evidence."""
        subject, _ = self._subject(occ)
        required = dict(occ.row.get("required_run") or {})
        if subject is None or not subject.live:
            why, by = SUBJECT_RETIRED, {}
        else:
            _, satisfied, _, qualifying = self._kind_status(subject, required)
            if not satisfied:
                return False
            why, by = LATER_RUN, qualifying
        self.hook("reconciler.before_resolve", subject=occ.subject_id, occurrence=occ.occurrence)
        try:
            ok = self.db.lwt(f"UPDATE {{ks}}.index_repairs SET resolution = 'RESOLVED', resolution_reason = ?, "  # noqa: S608
                             f"resolved_by = ?, resolved_at = ? WHERE {KEY} AND occurrence = ? IF occurrence_id = ? "
                             "AND state = 'CLOSED' AND resolution = 'PENDING'", why, by, self.p.now(), occ.spec,
                             occ.role, occ.bucket, occ.subject_id, occ.generation, occ.occurrence,
                             occ.occurrence_id).applied
        except LwtUnknownError:
            ok = (self.read(occ) or {}).get("resolution") == RESOLVED
        if ok:
            self._log("occurrence_resolved", subject=occ.subject_id, occurrence=occ.occurrence, reason=why)
        return ok

    def run_pass(self, spec: str) -> dict[str, int]:
        """The reconciler's ``repairs`` pass: every row not (CLOSED and RESOLVED), both roles, all buckets."""
        out = {"processed": 0, "open": 0, "unresolved": 0}
        for role in ("media", "target"):
            for b in range(BUCKETS):
                for row in self.db.rows("SELECT * FROM {ks}.index_repairs WHERE spec_digest = ? AND role = ? "
                                        "AND bucket = ?", spec, role, b):
                    if row["state"] == CLOSED and row["resolution"] == RESOLVED:
                        continue
                    out["processed"] += 1
                    self.step(row)
        o, u = self.counters(spec)
        out["open"], out["unresolved"] = o, u
        return out

    # --- counters and settlement --------------------------------------------------------------

    def counters(self, spec: str) -> tuple[int, int]:
        """(``recovery_open``, ``recovery_unresolved``) from a full scan of the occurrence rows."""
        rows = self.occurrences(spec)
        open_ = sum(1 for r in rows if r["state"] in (OPEN, REPAIRED, RERUN_PENDING))
        for role in ("media", "target"):
            _, gens = self.gens.partition(spec, role, serial=False)
            open_ += sum(1 for g in gens.values() if g.get("audit_state") in ("REQUESTED", "RUNNING"))
        unresolved = sum(1 for r in rows if r["state"] == CLOSED and r["resolution"] == PENDING)
        metrics.P10_RECOVERY_OPEN.labels(spec=spec[:19]).set(open_)
        metrics.P10_RECOVERY_UNRESOLVED.labels(spec=spec[:19]).set(unresolved)
        max_attempts = self.p.settings.matcher_reconcile.repair_max_attempts
        for role in ("media", "target"):
            for st in (OPEN, REPAIRED, RERUN_PENDING, CLOSED):
                metrics.P10_REPAIRS.labels(role=role, state=st).set(
                    sum(1 for r in rows if r["role"] == role and r["state"] == st))
            # an occurrence whose repair keeps failing stays OPEN and is retried with backoff, never dropped;
            # past repair_max_attempts it is reported (alert), and the report clears once it is REPAIRED
            metrics.P10_REPAIRS_BLOCKED.labels(role=role).set(
                sum(1 for r in rows if r["role"] == role and r["state"] == OPEN
                    and int(r.get("attempts") or 0) >= max_attempts))
        return open_, unresolved

    def settled(self, subject_id: str, spec: str, kinds: list[str]) -> dict[str, Any]:
        """``match status`` for one subject's "no match" (design §12.10): own runs first, then the counters."""
        reasons: list[str] = []
        for kind in kinds:
            t = self.tasks.row(subject_id, spec, kind)
            if t is None:
                reasons.append(f"{kind}: no task")
                continue
            if t.get("done_run") is None or int(t["run"]) != int(t["done_run"]):
                reasons.append(f"{kind}: rerun pending")
            elif t.get("done_state") != "DONE":
                reasons.append(f"{kind}: incomplete: {t.get('incomplete_reason') or t.get('state')}")
        o, u = self.counters(spec)
        if o:
            reasons.append(f"recovery open ({o})")
        if u:
            reasons.append(f"recovery unresolved ({u})")
        return {"subject": subject_id, "spec": spec, "status": "settled" if not reasons else "provisional",
                "reasons": reasons, "recovery_open": o, "recovery_unresolved": u}

    # --- generation audit and the unauditable fallback ----------------------------------------

    def audit_generation(self, spec: str, role: str, g: int) -> dict[str, int]:
        """Per-subject count of g (``audit_state`` REQUESTED → RUNNING → DONE, bucket cursor persisted)."""
        row = self.gens.row(spec, role, g)
        if row is None:
            return {}
        if row.get("audit_state") in (None, "DONE"):
            self.gens.set_cols(spec, role, g, audit_state="RUNNING", audit_cursor=0)
        elif row.get("audit_state") == "REQUESTED":
            self.gens.set_cols(spec, role, g, audit_state="RUNNING", audit_cursor=int(row.get("audit_cursor") or 0))
        start = int((self.gens.row(spec, role, g) or {}).get("audit_cursor") or 0)
        short = 0
        for b in range(start, BUCKETS):
            for e in self.db.rows("SELECT * FROM {ks}.index_entries WHERE spec_digest = ? AND role = ? AND bucket = ?",
                                  spec, role, b):
                if e["desired_op"] != "UPSERT" or (e["applied"] or {}).get(g) != e["seq"]:
                    continue
                n = self.p.index.count(row["collection"], subject_seq_eq(role, e["subject_id"], int(e["seq"])))
                if n < int(e["segment_count"] or 0):
                    meta = Subject.from_meta(e["subject"])
                    self.detect(spec, role, e["subject_id"], meta.content_id, g, row_seq=int(e["seq"]),
                                expected=int(e["segment_count"]), observed=n, detected_by="audit")
                    short += 1
            self.gens.set_cols(spec, role, g, audit_cursor=b + 1)
        self.gens.set_cols(spec, role, g, audit_state="DONE")
        return {"short": short}

    def unauditable_fallback(self, spec: str, role: str, g: int) -> int:
        """An occurrence for every UPSERT row with ``applied[g]`` set — durable before g may become RETIRED."""
        opened = 0
        for b in range(BUCKETS):
            for e in self.db.rows("SELECT * FROM {ks}.index_entries WHERE spec_digest = ? AND role = ? AND bucket = ?",
                                  spec, role, b):
                if e["desired_op"] != "UPSERT" or (e["applied"] or {}).get(g) is None:
                    continue
                meta = Subject.from_meta(e["subject"])
                self.detect(spec, role, e["subject_id"], meta.content_id, g, row_seq=int(e["seq"]),
                            expected=int(e["segment_count"] or 0), observed=-1, detected_by="unauditable")
                opened += 1
        return opened

    def clear_suspect(self, spec: str, role: str, g: int, verify_started_at: Any) -> bool:
        """Clear ``suspect_since`` only after (i) no row of g is open and (ii) a clean verification pass
        started after the last detection; fenced by ``detect_count``."""
        row = self.gens.row(spec, role, g)
        if row is None or row.get("suspect_since") is None:
            return False
        c = int(row["detect_count"] or 0)
        for r in self.occurrences(spec, role):
            if r["generation"] == g and r["state"] in (OPEN, REPAIRED, RERUN_PENDING):
                return False
        last = max((utc(r["detected_at"]) for r in self.occurrences(spec, role) if r["generation"] == g
                    and r.get("detected_at") is not None), default=None)
        if last is not None and (verify_started_at is None or utc(verify_started_at) <= last):
            return False
        ok = self.db.lwt("UPDATE {ks}.index_generations SET suspect_since = null, suspect_reason = null "
                         "WHERE spec_digest = ? AND role = ? AND generation = ? IF detect_count = ?",
                         spec, role, g, c).applied
        if ok:
            metrics.P10_GENERATION_SUSPECT.labels(spec=spec[:19], role=role, generation=str(g)).set(0)
        return ok

    def _log(self, event: str, **kw: Any) -> None:
        import logging

        logging.getLogger("fingerprinter.recovery").info(json.dumps({"event": event, **kw}, default=str))


def searched(state: str) -> bool:
    return state in SEARCHED
