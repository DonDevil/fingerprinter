"""Index generations, the reader pointer and the build lease (design §4.2, §12.1, §12.4; P10-D2, D19).

One Scylla partition ``index_generations(spec, role)`` holds every generation
row of that (spec, role) and, as **static** columns, ``active_generation`` /
``active_epoch`` (what readers search — never a Qdrant alias),
``routing_epoch`` (bumped whenever the set of routed generations changes) and
``next_generation`` (generation numbers are never reused).

State machine: CREATED → BUILDING → CATCHING_UP → VALIDATING → READY →
ACTIVE → DRAINING → RETIRED; VALIDATING → CATCHING_UP (lag); READY →
VALIDATING (re-validation, failed detection fence); CREATED…READY → FAILED.
There is no ACTIVE → FAILED (C-14m). Every transition is an LWT on the
generation row conditioned on the current state and on the builder's
``build_epoch``; ACTIVE is entered only by the promotion batch (one LWT,
fenced on ``active_epoch``, the target's ``state = 'READY'`` and its
``detect_count`` = the value validation recorded).
"""

from __future__ import annotations

import contextlib
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from fingerprinter.coord import utc
from fingerprinter.db import SERIAL, LwtUnknownError
from fingerprinter.p10ctx import P10
from fingerprinter.vindex import GenerationParams, collection_name

CREATED, BUILDING, CATCHING_UP, VALIDATING, READY = "CREATED", "BUILDING", "CATCHING_UP", "VALIDATING", "READY"
ACTIVE, DRAINING, RETIRED, FAILED = "ACTIVE", "DRAINING", "RETIRED", "FAILED"
ROUTED = (CREATED, BUILDING, CATCHING_UP, VALIDATING, READY, ACTIVE, DRAINING)
SEARCHED = (ACTIVE, DRAINING)
NON_TERMINAL_BUILD = (CREATED, BUILDING, CATCHING_UP, VALIDATING, READY)
LEGAL = {
    CREATED: {BUILDING, FAILED}, BUILDING: {CATCHING_UP, FAILED}, CATCHING_UP: {VALIDATING, FAILED},
    VALIDATING: {READY, CATCHING_UP, FAILED}, READY: {VALIDATING, FAILED}, DRAINING: {RETIRED},
}


class GenerationError(Exception):
    pass


@dataclass
class Pointer:
    generation: int
    collection: str
    epoch: int
    read_at: datetime
    read_mono: float
    """``time.monotonic()`` when the pointer was read: the search step's deadline counts from here."""


class Generations:
    def __init__(self, p10: P10) -> None:
        self.p = p10
        self.db = p10.db
        self._cache: dict[tuple[str, str], tuple[float, Pointer | None]] = {}

    # --- reads ------------------------------------------------------------------------

    def partition(self, spec: str, role: str, *, serial: bool = True) -> tuple[dict[str, Any], dict[int, dict]]:
        rows = self.db.rows("SELECT * FROM {ks}.index_generations WHERE spec_digest = ? AND role = ?", spec, role,
                            level=SERIAL if serial else "quorum")
        static = rows[0] if rows else {}
        gens = {int(r["generation"]): r for r in rows if r.get("generation") is not None}
        return static, gens

    def row(self, spec: str, role: str, g: int) -> dict[str, Any] | None:
        return self.db.one("SELECT * FROM {ks}.index_generations WHERE spec_digest = ? AND role = ? AND generation = ?",
                           spec, role, g, level=SERIAL)

    def routing(self, spec: str, role: str) -> tuple[int, list[dict[str, Any]]]:
        """The routing set, read fresh at QUORUM for every apply (design §12.2 step 2)."""
        static, gens = self.partition(spec, role, serial=False)
        return int(static.get("routing_epoch") or 0), [g for _, g in sorted(gens.items()) if g["state"] in ROUTED]

    def active(self, spec: str, role: str, *, fresh: bool = False) -> Pointer | None:
        """The reader pointer, cached ≤ ``index.active_ttl_s`` (design §4.2); ``read_at`` bounds the
        search step's deadline (design §11.1)."""
        key = (spec, role)
        ttl = self.p.settings.index.active_ttl_s
        hit = self._cache.get(key)
        if hit and not fresh and time.monotonic() - hit[0] <= ttl:
            return hit[1]
        mono = time.monotonic()
        static, gens = self.partition(spec, role, serial=False)
        g = int(static.get("active_generation") or 0)
        ptr = None
        if g and g in gens:
            ptr = Pointer(g, gens[g]["collection"], int(static.get("active_epoch") or 0), self.p.now(), mono)
        self._cache[key] = (mono, ptr)
        return ptr

    def forget(self) -> None:
        self._cache.clear()

    # --- creation and transitions ------------------------------------------------------

    def init_partition(self, spec: str, role: str) -> None:
        self.db.lwt("INSERT INTO {ks}.index_generations (spec_digest, role, active_generation, active_epoch, "
                    "routing_epoch, next_generation) VALUES (?,?,0,0,0,1) IF NOT EXISTS", spec, role)

    def next_number(self, spec: str, role: str) -> int:
        self.init_partition(spec, role)
        static, _ = self.partition(spec, role)
        return int(static["next_generation"])

    def create(self, spec: str, role: str, g: int, params: GenerationParams, build_epoch: int,
               reserved_points: int) -> bool:
        """CREATED: row + ``routing_epoch`` bump + ``next_generation`` advance, one conditional batch."""
        static, _ = self.partition(spec, role)
        now = self.p.now()
        ix = self.p.settings.index
        boundary = now + timedelta(seconds=ix.mutation_lease_s + ix.clock_margin_s)
        name = collection_name(spec, role, g, self.p.settings.index.collection_prefix)
        audit = [json.dumps({"at": now.isoformat(), "to": CREATED, "by": self.p.instance})]
        try:
            res = self.db.lwt(
                "BEGIN BATCH "
                "UPDATE {ks}.index_generations SET next_generation = ?, routing_epoch = ? "
                "WHERE spec_digest = ? AND role = ? IF next_generation = ?; "
                "INSERT INTO {ks}.index_generations (spec_digest, role, generation, state, collection, params, "
                "params_digest, build_epoch, boundary_at, reserved_points, detect_count, validated_detect_count, "
                "build_cursor, created_at, updated_at, audit) VALUES (?,?,?,'CREATED',?,?,?,?,?,?,0,0,?,?,?,?); "
                "APPLY BATCH",
                g + 1, int(static.get("routing_epoch") or 0) + 1, spec, role, g,
                spec, role, g, name, json.dumps(params.document(), sort_keys=True), params.digest(), build_epoch,
                boundary, reserved_points, {}, now, now, audit)
        except LwtUnknownError:
            row = self.row(spec, role, g)
            return row is not None and row["build_epoch"] == build_epoch
        return res.applied

    def transition(self, spec: str, role: str, g: int, frm: str, to: str, *, build_epoch: int | None,
                   **cols: Any) -> bool:
        if to not in LEGAL.get(frm, set()):
            raise GenerationError(f"illegal transition {frm} -> {to}")
        now = self.p.now()
        sets = ["state = ?", "updated_at = ?", "audit = audit + ?"]
        args: list[Any] = [to, now, [json.dumps({"at": now.isoformat(), "from": frm, "to": to,
                                                 "by": self.p.instance, **{k: str(v) for k, v in cols.items()}})]]
        for k, v in cols.items():
            sets.append(f"{k} = ?")
            args.append(v)
        cond = "state = ?" + (" AND build_epoch = ?" if build_epoch is not None else "")
        args += [spec, role, g, frm] + ([build_epoch] if build_epoch is not None else [])
        try:
            ok = self.db.lwt(f"UPDATE {{ks}}.index_generations SET {', '.join(sets)} WHERE spec_digest = ? "  # noqa: S608 - column names are code constants; values are bound
                             f"AND role = ? AND generation = ? IF {cond}", *args).applied
        except LwtUnknownError:
            row = self.row(spec, role, g)
            ok = row is not None and row["state"] == to
        if ok and to == FAILED:
            self.bump_routing(spec, role)
        return ok

    def bump_routing(self, spec: str, role: str) -> None:
        for _ in range(20):
            static, _ = self.partition(spec, role)
            e = int(static.get("routing_epoch") or 0)
            if self.db.lwt("UPDATE {ks}.index_generations SET routing_epoch = ? WHERE spec_digest = ? AND role = ? "
                           "IF routing_epoch = ?", e + 1, spec, role, e).applied:
                return

    def set_cols(self, spec: str, role: str, g: int, *, expect_state: str | None = None, **cols: Any) -> bool:
        sets = ", ".join(f"{k} = ?" for k in cols)
        args = [*cols.values(), spec, role, g]
        if expect_state is None:
            return self.db.lwt(f"UPDATE {{ks}}.index_generations SET {sets} WHERE spec_digest = ? AND role = ? "  # noqa: S608 - column names are code constants; values are bound
                               "AND generation = ? IF EXISTS", *args).applied
        return self.db.lwt(f"UPDATE {{ks}}.index_generations SET {sets} WHERE spec_digest = ? AND role = ? "  # noqa: S608 - column names are code constants; values are bound
                           "AND generation = ? IF state = ?", *args, expect_state).applied

    def promote(self, spec: str, role: str, g: int) -> bool:
        """One LWT batch: pointer + ACTIVE + DRAINING, fenced on ``active_epoch``, READY and the
        detection count recorded by validation (design §12.4)."""
        static, gens = self.partition(spec, role)
        row = gens.get(g)
        if row is None or row["state"] != READY:
            return False
        e = int(static.get("active_epoch") or 0)
        old = int(static.get("active_generation") or 0)
        now = self.p.now()
        ix, m = self.p.settings.index, self.p.settings.matching
        validated = utc(row.get("validated_at"))
        if validated is None or now - validated > timedelta(seconds=ix.revalidate_after_s):
            # R-9: a validation older than revalidate_after_s does not promote; the generation goes back
            # to VALIDATING and the next build (which adopts it) validates it again
            self.transition(spec, role, g, READY, VALIDATING, build_epoch=None)
            self.forget()
            return False
        horizon = now + timedelta(seconds=ix.active_ttl_s + m.search_timeout_s + ix.clock_margin_s)
        stmts = ["UPDATE {ks}.index_generations SET active_generation = ?, active_epoch = ? "
                 "WHERE spec_digest = ? AND role = ? IF active_epoch = ?",
                 "UPDATE {ks}.index_generations SET state = 'ACTIVE', promoted_at = ?, updated_at = ? "
                 "WHERE spec_digest = ? AND role = ? AND generation = ? IF state = 'READY' AND detect_count = ?"]
        args: list[Any] = [g, e + 1, spec, role, e, now, now, spec, role, g, int(row["validated_detect_count"] or 0)]
        if old and old in gens:
            stmts.append("UPDATE {ks}.index_generations SET state = 'DRAINING', drain_horizon_at = ?, updated_at = ? "
                         "WHERE spec_digest = ? AND role = ? AND generation = ? IF state = 'ACTIVE'")
            args += [horizon, now, spec, role, old]
        try:
            ok = self.db.lwt("BEGIN BATCH " + "; ".join(stmts) + "; APPLY BATCH", *args).applied
        except LwtUnknownError:
            static, _ = self.partition(spec, role)
            ok = int(static.get("active_generation") or 0) == g and int(static.get("active_epoch") or 0) == e + 1
        if not ok:
            fresh = self.row(spec, role, g)
            if fresh is not None and fresh["state"] == READY and \
                    int(fresh["detect_count"] or 0) != int(fresh["validated_detect_count"] or 0):
                # a loss was detected in g after validation: back to VALIDATING (design §12.4)
                self.transition(spec, role, g, READY, VALIDATING, build_epoch=None)
        self.forget()
        return ok

    # --- build lease (P9 intake pattern) ------------------------------------------------

    def acquire_lease(self, spec: str, role: str) -> int | None:
        row = self.db.one("SELECT * FROM {ks}.index_build_lease WHERE spec_digest = ? AND role = ?", spec, role,
                          level=SERIAL)
        now = self.p.now()
        ttl = timedelta(seconds=self.p.settings.index.build_lease_s)
        if row is None:
            ok = self.db.lwt("INSERT INTO {ks}.index_build_lease (spec_digest, role, owner, epoch, lease_until) "
                             "VALUES (?,?,?,1,?) IF NOT EXISTS", spec, role, self.p.instance, now + ttl).applied
            return 1 if ok else None
        if row["owner"] != self.p.instance and row["lease_until"] and utc(row["lease_until"]) > now:
            return None
        epoch = int(row["epoch"] or 0) + 1
        ok = self.db.lwt("UPDATE {ks}.index_build_lease SET owner = ?, epoch = ?, lease_until = ? "
                         "WHERE spec_digest = ? AND role = ? IF epoch = ?", self.p.instance, epoch, now + ttl, spec,
                         role, row["epoch"]).applied
        return epoch if ok else None

    def renew_lease(self, spec: str, role: str, epoch: int) -> bool:
        try:
            return self._renew(spec, role, epoch)
        except LwtUnknownError:
            return False  # the next transition's build_epoch condition is the real fence

    def _renew(self, spec: str, role: str, epoch: int) -> bool:
        return self.db.lwt("UPDATE {ks}.index_build_lease SET lease_until = ? WHERE spec_digest = ? AND role = ? "
                           "IF owner = ? AND epoch = ?", self.p.now() + timedelta(
                               seconds=self.p.settings.index.build_lease_s), spec, role, self.p.instance, epoch).applied

    def release_lease(self, spec: str, role: str, epoch: int) -> None:
        """Advisory early release (expiry covers a failure); an unknown outcome is ignored."""
        with contextlib.suppress(LwtUnknownError):
            self._release(spec, role, epoch)

    def _release(self, spec: str, role: str, epoch: int) -> None:
        self.db.lwt("UPDATE {ks}.index_build_lease SET lease_until = ? WHERE spec_digest = ? AND role = ? "
                    "IF owner = ? AND epoch = ?", self.p.now(), spec, role, self.p.instance, epoch)
