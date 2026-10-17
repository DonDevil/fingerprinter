"""Index capacity ledger (design §13.3; P10-D14; corrected C-10, C-14).

``index_capacity(instance)`` counts **reserved points**, a conservative upper
bound of the points resident in one Qdrant instance (all specs, roles and
generations whose collection exists, FAILED ones until deleted):

- static ``reserved`` changes only by LWT ``IF reserved = :r`` (CAS loop);
- a live UPSERT whose ``seq`` is not yet applied to a routed generation
  reserves ``segment_count`` per such generation **before** its Qdrant call;
  refused ⇒ the caller marks the row ``BLOCKED: capacity`` and writes nothing;
- a rebuild is admitted before anything is created, by **one conditional
  batch** on the ledger partition: ``reserved = r + P IF reserved = r`` plus
  the holder row ``gen:<spec12>:<role>:<g>`` (CAP-9, executed on Scylla 6.2.3);
- nothing but :meth:`Capacity.audit` lowers ``reserved`` (C-14l); the audit
  recomputes the bound from durable state in the fixed order (a)-(d).

Every check subtracts ``capacity_margin_points`` from ``max_points`` once.
``max_points`` is the configured value; any value other than the provisional
250,000 must name its E-R1 report (``index.max_points_evidence``, CAP-8).
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any

from fingerprinter import metrics
from fingerprinter.db import SERIAL, LwtUnknownError
from fingerprinter.p10ctx import BUCKETS, P10

LIVE_STATES = ("CREATED", "BUILDING", "CATCHING_UP", "VALIDATING", "READY", "ACTIVE", "DRAINING")


class CapacityBlockedError(Exception):
    """A reservation was refused: writing would exceed ``max_points - margin``."""

    def __init__(self, needed: int, reserved: int, limit: int) -> None:
        super().__init__(f"capacity: reserved {reserved} + {needed} > limit {limit}")
        self.needed, self.reserved, self.limit = needed, reserved, limit


@dataclass
class AuditResult:
    reserved_before: int
    bound: int
    applied: bool
    holders_removed: list[str]
    parts: dict[str, Any]


class Capacity:
    def __init__(self, p10: P10) -> None:
        self.p = p10
        self.db = p10.db
        self.ix = p10.settings.index
        self.instance = self.ix.instance

    @property
    def limit(self) -> int:
        return self.ix.max_points - self.ix.capacity_margin_points

    def ensure(self, actor: str = "matcher") -> dict[str, Any]:
        """Create the ledger row once; keep its static ``max_points`` equal to the configuration (audited)."""
        now = self.p.now()
        entry = json.dumps({"at": now.isoformat(), "actor": actor, "action": "init", "max_points": self.ix.max_points,
                            "evidence": self.ix.max_points_evidence})
        self.db.lwt("INSERT INTO {ks}.index_capacity (instance, max_points, reserved, updated_at, audit) "
                    "VALUES (?,?,0,?,?) IF NOT EXISTS", self.instance, self.ix.max_points, now, [entry])
        row = self.read()
        if row["max_points"] != self.ix.max_points:
            change = json.dumps({"at": now.isoformat(), "actor": actor, "action": "max_points",
                                 "from": row["max_points"], "to": self.ix.max_points,
                                 "evidence": self.ix.max_points_evidence})
            self.db.lwt("UPDATE {ks}.index_capacity SET max_points = ?, audit = audit + ? WHERE instance = ? "
                        "IF max_points = ?", self.ix.max_points, [change], self.instance, row["max_points"])
        metrics.P10_CAPACITY_LIMIT.labels(instance=self.instance).set(self.ix.max_points)
        return self.read()

    def read(self) -> dict[str, Any]:
        rows = self.db.rows("SELECT * FROM {ks}.index_capacity WHERE instance = ?", self.instance, level=SERIAL)
        if not rows:
            return {"reserved": 0, "max_points": self.ix.max_points, "holders": {}}
        first = rows[0]
        holders = {r["holder"]: int(r["points"] or 0) for r in rows if r.get("holder")}
        return {"reserved": int(first["reserved"] or 0), "max_points": int(first["max_points"] or 0),
                "holders": holders, "updated_at": first.get("updated_at")}

    def reserve(self, points: int) -> int:
        """Reserve ``points`` (CAS loop). Raises :class:`CapacityBlockedError`; returns the new ``reserved``.

        An unknown CAS outcome is retried: a duplicate reservation only makes the ledger too high (safe)."""
        if points <= 0:
            return self.read()["reserved"]
        for _ in range(50):
            r = self.read()["reserved"]
            if r + points > self.limit:
                metrics.P10_CAPACITY_BLOCKED.inc()
                raise CapacityBlockedError(points, r, self.limit)
            try:
                res = self.db.lwt("UPDATE {ks}.index_capacity SET reserved = ?, updated_at = ? WHERE instance = ? "
                                  "IF reserved = ?", r + points, self.p.now(), self.instance, r)
            except LwtUnknownError:
                continue  # at-least-once: re-read and reserve again (over-reservation is safe)
            if res.applied:
                metrics.P10_CAPACITY_RESERVED.labels(instance=self.instance).set(r + points)
                return r + points
        raise LwtUnknownError("capacity reservation did not converge")

    def admit_rebuild(self, holder: str, projected: int) -> bool:
        """Rebuild admission (before CREATED): one conditional batch, static condition + holder row."""
        for _ in range(50):
            led = self.read()
            if holder in led["holders"]:
                return True  # an earlier attempt (unknown outcome) already admitted it
            r = led["reserved"]
            if r + projected > self.limit:
                metrics.P10_REBUILD_CAPACITY_BLOCKED.labels(holder=holder).set(projected)
                return False
            now = self.p.now()
            try:
                res = self.db.lwt(
                    "BEGIN BATCH "
                    "UPDATE {ks}.index_capacity SET reserved = ?, updated_at = ? WHERE instance = ? IF reserved = ?; "
                    "INSERT INTO {ks}.index_capacity (instance, holder, points, created_at) VALUES (?,?,?,?); "
                    "APPLY BATCH", r + projected, now, self.instance, r, self.instance, holder, projected, now)
            except LwtUnknownError:
                continue
            if res.applied:
                metrics.P10_REBUILD_CAPACITY_BLOCKED.labels(holder=holder).set(0)
                return True
        raise LwtUnknownError("rebuild admission did not converge")

    @staticmethod
    def projected(points: int, headroom: float) -> int:
        return math.ceil(points * (1.0 + headroom))

    # --- the only lowering path --------------------------------------------------------

    def audit(self) -> AuditResult:
        """Recompute the conservative bound and CAS it (design §13.3, order (a)-(d))."""
        from fingerprinter.vindex import IndexMissingError

        for _ in range(20):
            led = self.read()                                                   # (a)
            r = led["reserved"]
            gens: list[dict[str, Any]] = []                                     # (b)
            for spec in self.p.indexed_specs() + self._ledger_specs(led):
                for role in ("media", "target"):
                    gens.extend(self.db.rows("SELECT * FROM {ks}.index_generations WHERE spec_digest = ? AND role = ?",
                                             spec, role, level=SERIAL))
            gens = [g for g in gens if g.get("generation") is not None]
            gens_by_holder = {holder_of(g["spec_digest"], g["role"], g["generation"]): g for g in gens}
            pending = 0                                                          # (c)
            for spec, role in {(g["spec_digest"], g["role"]) for g in gens}:
                routed = [g["generation"] for g in gens if g["spec_digest"] == spec and g["role"] == role
                          and g["state"] in LIVE_STATES]
                for b in range(BUCKETS):
                    for row in self.db.rows("SELECT desired_op, seq, applied, segment_count, state, blocked_reason "
                                            "FROM {ks}.index_entries WHERE spec_digest = ? AND role = ? AND bucket = ?",
                                            spec, role, b):
                        if row["desired_op"] != "UPSERT" or (row["state"] == "BLOCKED"
                                                              and row["blocked_reason"] == "capacity"):
                            continue
                        applied = row["applied"] or {}
                        pending += sum(int(row["segment_count"] or 0) for g in routed if applied.get(g) != row["seq"])
            counts: dict[str, int] = {}                                         # (d)
            for g in gens:
                if not g.get("collection"):
                    continue  # a RETIRED generation is counted too until its collection is really gone
                try:
                    h = holder_of(g["spec_digest"], g["role"], g["generation"])
                    counts[h] = self.p.index.count(g["collection"])
                except IndexMissingError:
                    continue
            bound = pending
            for h, c in counts.items():
                bound += max(led["holders"].get(h, 0), c)
            removable = []
            for h, pts in led["holders"].items():
                if h in counts:
                    continue
                g = gens_by_holder.get(h)
                if g is not None and g["state"] in ("RETIRED", "FAILED"):
                    removable.append(h)   # collection deletion confirmed (absent from the counts)
                elif g is None and self._admission_orphan(h):
                    removable.append(h)   # crashed admission, build lease expired
                else:
                    bound += pts          # still held (e.g. a generation row about to be created)
            parts = {"pending_unapplied": pending, "collections": counts, "holders": led["holders"],
                     "removable_holders": removable}
            stmts = ["UPDATE {ks}.index_capacity SET reserved = ?, updated_at = ? WHERE instance = ? IF reserved = ?"]
            args: list[Any] = [bound, self.p.now(), self.instance, r]
            for h in removable:
                stmts.append("DELETE FROM {ks}.index_capacity WHERE instance = ? AND holder = ?")
                args.extend([self.instance, h])
            cql = stmts[0] if len(stmts) == 1 else "BEGIN BATCH " + "; ".join(stmts) + "; APPLY BATCH"
            try:
                res = self.db.lwt(cql, *args)
            except LwtUnknownError:
                continue
            if res.applied:
                metrics.P10_CAPACITY_RESERVED.labels(instance=self.instance).set(bound)
                return AuditResult(r, bound, True, removable, parts)
        return AuditResult(r, bound, False, [], parts)

    def _ledger_specs(self, led: dict[str, Any]) -> list[str]:
        """Specs that still hold generations although no longer listed as indexed (after ``index drop``)."""
        rows = self.db.rows("SELECT DISTINCT spec_digest, role FROM {ks}.index_generations")
        return sorted({r["spec_digest"] for r in rows} - set(self.p.indexed_specs()))

    def _admission_orphan(self, holder: str) -> bool:
        try:
            _, spec12, role, _g = holder.split(":")
        except ValueError:
            return False
        for row in self.db.rows("SELECT * FROM {ks}.index_build_lease"):
            if row["spec_digest"].split(":", 1)[-1][:12] == spec12 and row["role"] == role:
                lease = row.get("lease_until")
                return lease is None or self.p.now() > lease.replace(tzinfo=self.p.now().tzinfo)
        return True


def holder_of(spec_digest: str, role: str, generation: int) -> str:
    return f"gen:{spec_digest.split(':', 1)[-1][:12]}:{role}:{int(generation)}"
