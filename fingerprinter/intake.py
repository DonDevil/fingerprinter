"""Intake of ``encode.requested`` without a consumer group (design §6, P9-D6/D27).

A consumer group on crawler2's stream could stall crawler2's relay (ADR-022,
B.5 #1), so one leased reader follows the stream with ``XRANGE`` from a durable
cursor. Stream IDs are compared only as ``(ms, seq)`` pairs, never by ID
arithmetic. Losses are detected by **counts**: with head-only trimming,
``T = entries-added − length`` entries were ever removed and they are the
oldest ones; the reader's ``consumed_ordinal`` ``o`` counts entries up to its
cursor. ``T > o`` ⇒ ``T − o`` entries after the cursor were trimmed unread.

On a gap the reader records it and **continues** (halting would let crawler2
trim more). Gap records never close by themselves except
REPLAY_OBSERVING → CLOSED_REPLAYED/REPLAY_INCOMPLETE (observed); loss is
accepted only by an explicit, audited ``accept-gap`` — never automatically,
and an accepted gap is never reported as recovered (approved trade-off, D27).
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import redis
from antipiracy_contracts.events import ContractError, decode_event_as
from antipiracy_contracts.events.fingerprinting import EncodeRequested
from pydantic import ValidationError

from fingerprinter import metrics
from fingerprinter.coord import utc
from fingerprinter.db import SERIAL
from fingerprinter.errors import StorageUnavailableError
from fingerprinter.publish import INTAKE
from fingerprinter.requests import Requests
from fingerprinter.store import Store

RUNNING, HALTED = "RUNNING", "HALTED"
OPEN = "OPEN"
REPLAY_REQUESTED = "REPLAY_REQUESTED"
REPLAY_OBSERVING = "REPLAY_OBSERVING"
REPLAY_INCOMPLETE = "REPLAY_INCOMPLETE"
REPLAY_UNAVAILABLE = "REPLAY_UNAVAILABLE"
CLOSED_REPLAYED = "CLOSED_REPLAYED"
CLOSED_ACCEPTED = "CLOSED_ACCEPTED"
CLOSED = (CLOSED_REPLAYED, CLOSED_ACCEPTED)


def sid(value: str | None) -> tuple[int, int]:
    """A stream ID as the ``(ms, seq)`` pair it is compared by."""
    if not value:
        return (0, 0)
    ms, _, seq = value.partition("-")
    return int(ms), int(seq or 0)


def id_time(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromtimestamp(sid(value)[0] / 1000, tz=UTC)


def _t(value: Any) -> str | None:
    if value is None:
        return None
    return value.decode() if isinstance(value, bytes) else str(value)


class IntakeHalted(Exception):
    pass


@dataclass
class CycleResult:
    applied: int = 0
    invalid: int = 0
    duplicates: int = 0
    gaps_opened: int = 0
    idle: bool = False
    halted: str | None = None
    lost_lease: bool = False


class Intake:
    def __init__(self, store: Store, requests: Requests) -> None:
        self.store = store
        self.db = store.db
        self.requests = requests
        self.cfg = store.settings.intake
        self.reader = self.cfg.reader
        self.stream = store.coord.stream(INTAKE)
        self.client = store.coord.client
        self.owner = store.instance
        self.epoch: int | None = None
        self.hook = lambda point, **ctx: None

    # --- state ------------------------------------------------------------------

    def state(self) -> dict[str, Any] | None:
        return self.db.one(
            "SELECT * FROM {ks}.intake_state WHERE reader = ?", self.reader, level=SERIAL
        )

    def _info(self) -> dict[str, Any] | None:
        try:
            raw: Any = self.client.xinfo_stream(self.stream)
        except redis.ResponseError as exc:
            if "no such key" in str(exc).lower():
                return None
            raise StorageUnavailableError(str(exc)) from exc
        return raw

    def init(self, mode: str, *, actor: str, reason: str = "") -> dict[str, Any]:
        """``init --from-start`` / ``--from-now`` (§6.8). Refused if already initialized."""
        now = self.store.now()
        info = self._info()
        if mode == "from-start":
            cursor, ordinal = None, 0
        elif mode == "from-now":
            if not reason:
                raise ValueError("--from-now needs --reason")
            cursor = _t(info["last-generated-id"]) if info else None
            ordinal = int(info["entries-added"]) if info else 0
        else:
            raise ValueError(mode)
        res = self.db.lwt(
            "INSERT INTO {ks}.intake_state (reader, owner, epoch, lease_until, cursor_id, "
            "consumed_ordinal, mode, halt_reason, initialized_at, init_mode, invalid_total) "
            "VALUES (?, null, 0, ?, ?, ?, 'RUNNING', null, ?, ?, 0) IF NOT EXISTS",
            self.reader,
            now,
            cursor,
            ordinal,
            now,
            mode,
        )
        if not res.applied:
            raise ValueError("intake already initialized (use reinit after a stream reset)")
        if mode == "from-now" and ordinal:
            self._new_gap(
                "bootstrap",
                after=None,
                first=cursor,
                missing=ordinal,
                now=now,
                state=CLOSED_ACCEPTED,
                audit=[
                    self.store.audit_entry(
                        now, actor, "init-from-now", reason=reason, missing_count=ordinal
                    )
                ],
            )
        self.store.audit(actor, "intake-init", {"mode": mode, "reason": reason, "ordinal": ordinal})
        return {"cursor_id": cursor, "consumed_ordinal": ordinal, "mode": mode}

    def reinit_after_reset(self, *, actor: str, reason: str) -> None:
        st = self.state()
        if st is None or st["mode"] != HALTED or st["halt_reason"] != "stream_reset":
            raise ValueError("reinit --after-reset only from HALTED(stream_reset)")
        res = self.db.lwt(
            "UPDATE {ks}.intake_state SET cursor_id = null, consumed_ordinal = 0, mode = 'RUNNING', "
            "halt_reason = null, init_mode = 'reinit-after-reset' WHERE reader = ? IF mode = 'HALTED'",
            self.reader,
        )
        if not res.applied:
            raise ValueError("state changed concurrently")
        self.store.audit(actor, "intake-reinit", {"reason": reason})

    # --- lease (§6.2) -----------------------------------------------------------

    def acquire(self) -> bool:
        st = self.state()
        if st is None:
            return False
        now = self.store.now()
        ttl = timedelta(seconds=self.cfg.lease_ttl_s)
        if st["owner"] == self.owner and self.epoch == st["epoch"] and utc(st["lease_until"]) > now:
            return True
        if st["owner"] is not None and utc(st["lease_until"]) > now:
            return False
        epoch = int(st["epoch"] or 0) + 1
        res = self.db.lwt(
            "UPDATE {ks}.intake_state SET owner = ?, epoch = ?, lease_until = ? WHERE reader = ? "
            "IF epoch = ?",
            self.owner,
            epoch,
            now + ttl,
            self.reader,
            st["epoch"],
        )
        if res.applied:
            self.epoch = epoch
        return res.applied

    def renew(self) -> bool:
        if self.epoch is None:
            return False
        now = self.store.now()
        res = self.db.lwt(
            "UPDATE {ks}.intake_state SET lease_until = ? WHERE reader = ? IF owner = ? AND epoch = ?",
            now + timedelta(seconds=self.cfg.lease_ttl_s),
            self.reader,
            self.owner,
            self.epoch,
        )
        if not res.applied:
            self.epoch = None
        return res.applied

    # --- read cycle (§6.4) ---------------------------------------------------

    def cycle(self) -> CycleResult:
        result = CycleResult()
        st = self.state()
        if st is None:
            result.halted = "uninitialized"
            return result
        if st["mode"] == HALTED:
            result.halted = st["halt_reason"]
            return result
        if self.epoch is None or st["epoch"] != self.epoch or st["owner"] != self.owner:
            result.lost_lease = True
            return result
        cursor: str | None = st["cursor_id"]
        o = int(st["consumed_ordinal"] or 0)
        now = self.store.now()
        try:
            pipe = self.client.pipeline(transaction=True)
            pipe.xinfo_stream(self.stream)
            if cursor:
                pipe.xrange(self.stream, min="(" + cursor, max="+", count=self.cfg.batch)
            else:
                pipe.xrange(self.stream, min="-", max="+", count=self.cfg.batch)
            info, entries = pipe.execute(raise_on_error=False)
        except redis.RedisError as exc:
            raise StorageUnavailableError(f"intake read: {exc}") from exc
        if isinstance(info, Exception):
            if "no such key" not in str(info).lower():
                raise StorageUnavailableError(str(info))
            if o == 0:
                result.idle = True
                return result
            self._halt_reset(st, now, "stream missing with consumed_ordinal > 0")
            result.halted = "stream_reset"
            return result
        if isinstance(entries, Exception):
            raise StorageUnavailableError(str(entries))
        added = int(info["entries-added"])
        length = int(info["length"])
        trimmed = added - length
        last_gen = _t(info["last-generated-id"])
        max_del = _t(info.get("max-deleted-entry-id"))
        first = info.get("first-entry")
        first_id = _t(first[0]) if first else None
        metrics.INTAKE_LAG.set(max(added - o, 0))
        if added < o or (cursor and sid(last_gen) < sid(cursor)):
            self._halt_reset(st, now, f"entries-added {added} < ordinal {o} or id regressed")
            result.halted = "stream_reset"
            return result
        new_o = o
        if trimmed > o:
            self._gap(
                "trim",
                after=cursor,
                first=first_id,
                missing=trimmed - o,
                ordinal=o,
                now=now,
            )
            new_o = trimmed
            result.gaps_opened += 1
        elif cursor and max_del and max_del != "0-0" and sid(max_del) > sid(cursor):
            self._gap("tombstone", after=cursor, first=max_del, missing=None, ordinal=o, now=now, gap_id=f"tombstone:{max_del}")
            result.gaps_opened += 1
        last = cursor
        for entry_id, fields in entries:
            eid = _t(entry_id)
            outcome = self._apply(eid, fields)
            if outcome == "invalid":
                result.invalid += 1
            elif outcome == "duplicate":
                result.duplicates += 1
            else:
                result.applied += 1
            last = eid
            new_o += 1
        self.hook("before_cursor_cas", last=last)
        if last != cursor or new_o != o:
            res = self.db.lwt(
                "UPDATE {ks}.intake_state SET cursor_id = ?, consumed_ordinal = ? WHERE reader = ? "
                "IF epoch = ? AND cursor_id = ?",
                last,
                new_o,
                self.reader,
                self.epoch,
                cursor,
            )
            if not res.applied:
                self.epoch = None
                result.lost_lease = True
                return result
            self._observe_replays(last, now)
        if not entries and trimmed <= o:
            result.idle = True
        return result

    def _apply(self, entry_id: str, fields: dict[Any, Any]) -> str:
        raw = fields.get(b"envelope") or fields.get("envelope")
        try:
            if raw is None:
                raise ValueError("entry has no envelope field")
            env = decode_event_as(raw, EncodeRequested)
        except (ContractError, ValidationError, ValueError) as exc:
            self.db.run(
                "INSERT INTO {ks}.intake_invalid (reader, entry_id, reason, seen_at) VALUES (?,?,?,?)",
                self.reader,
                entry_id,
                f"{type(exc).__name__}: {exc}"[:300],
                self.store.now(),
            )
            metrics.INTAKE_APPLIED.labels(outcome="invalid").inc()
            return "invalid"
        outcome = self.requests.upsert_event(env)
        metrics.INTAKE_APPLIED.labels(outcome=outcome).inc()
        return "duplicate" if outcome == "duplicate" else "applied"

    def _halt_reset(self, st: dict[str, Any], now: datetime, why: str) -> None:
        self._gap("stream_reset", after=st["cursor_id"], first=None, missing=None,
                  ordinal=int(st["consumed_ordinal"] or 0), now=now)
        self.db.lwt(
            "UPDATE {ks}.intake_state SET mode = 'HALTED', halt_reason = 'stream_reset' "
            "WHERE reader = ? IF epoch = ?",
            self.reader,
            self.epoch,
        )
        self.store.audit("intake", "halt", {"reason": "stream_reset", "detail": why})

    # --- gaps (§6.5) ------------------------------------------------------------

    def _gap(
        self,
        kind: str,
        *,
        after: str | None,
        first: str | None,
        missing: int | None,
        ordinal: int,
        now: datetime,
        gap_id: str | None = None,
    ) -> None:
        gid = gap_id or f"{kind}:{after or '0-0'}:{ordinal}"
        existing = self.gap(gid)
        if existing is not None:
            if missing is not None and existing["state"] == OPEN and (existing["missing_count"] or 0) < missing:
                self.db.lwt(
                    "UPDATE {ks}.intake_gaps SET missing_count = ? WHERE reader = ? AND gap_id = ? "
                    "IF state = 'OPEN'",
                    missing,
                    self.reader,
                    gid,
                )
            return
        self._new_gap(kind, after=after, first=first, missing=missing, now=now, gap_id=gid)
        self._mark_incomplete(after, first, now)
        metrics.INTAKE_OPEN_GAPS.set(len(self.open_gaps()))

    def _new_gap(
        self,
        kind: str,
        *,
        after: str | None,
        first: str | None,
        missing: int | None,
        now: datetime,
        state: str = OPEN,
        gap_id: str | None = None,
        audit: list[str] | None = None,
    ) -> None:
        margin = timedelta(seconds=self.cfg.replay_margin_s)
        start = id_time(after)
        self.db.lwt(
            "INSERT INTO {ks}.intake_gaps (reader, gap_id, detected_at, kind, after_cursor, "
            "first_retained, missing_count, window_from, window_to, state, audit) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?) IF NOT EXISTS",
            self.reader,
            gap_id or f"{kind}:{after or '0-0'}:{now.timestamp()}",
            now,
            kind,
            after,
            first,
            missing,
            (start - margin) if start else None,
            id_time(first) or now,
            state,
            audit or [self.store.audit_entry(now, "intake", "detected", kind=kind, missing_count=missing)],
        )

    def _mark_incomplete(self, after: str | None, first: str | None, now: datetime) -> None:
        """A new gap overlapping an observed replay range makes that replay incomplete."""
        for g in self.gaps():
            if g["state"] != REPLAY_OBSERVING:
                continue
            lo, hi = sid(g["replay_start_id"]), sid(g["replay_end_id"])
            if sid(after) < hi and (first is None or sid(first) > lo):
                self._transition(g, REPLAY_OBSERVING, REPLAY_INCOMPLETE, "intake", "replay-overlap-gap", now)

    def _observe_replays(self, cursor: str | None, now: datetime) -> None:
        for g in self.gaps():
            if g["state"] == REPLAY_OBSERVING and sid(cursor) >= sid(g["replay_end_id"]):
                self._transition(g, REPLAY_OBSERVING, CLOSED_REPLAYED, "intake", "replay-observed", now)
        metrics.INTAKE_OPEN_GAPS.set(len(self.open_gaps()))

    def gap(self, gap_id: str) -> dict[str, Any] | None:
        return self.db.one(
            "SELECT * FROM {ks}.intake_gaps WHERE reader = ? AND gap_id = ?",
            self.reader,
            gap_id,
            level=SERIAL,
        )

    def gaps(self) -> list[dict[str, Any]]:
        return self.db.rows("SELECT * FROM {ks}.intake_gaps WHERE reader = ?", self.reader)

    def open_gaps(self) -> list[dict[str, Any]]:
        return [g for g in self.gaps() if g["state"] not in CLOSED]

    def _transition(
        self, g: dict[str, Any], frm: str, to: str, actor: str, action: str, now: datetime, **extra: Any
    ) -> bool:
        sets = ", ".join(f"{k} = ?" for k in extra)
        res = self.db.lwt(
            "UPDATE {ks}.intake_gaps SET state = ?, audit = audit + ?"
            + (", " + sets if sets else "")
            + " WHERE reader = ? AND gap_id = ? IF state = ?",
            to,
            [self.store.audit_entry(now, actor, action, frm=frm, to=to, **extra)],
            *extra.values(),
            self.reader,
            g["gap_id"],
            frm,
        )
        return res.applied

    def _require(self, gap_id: str, allowed: tuple[str, ...]) -> dict[str, Any]:
        g = self.gap(gap_id)
        if g is None:
            raise ValueError(f"no gap {gap_id}")
        if g["state"] not in allowed:
            raise ValueError(f"gap {gap_id} is {g['state']}; allowed from {', '.join(allowed)}")
        return g

    def replay_start(self, gap_id: str, frm: datetime, to: datetime, *, actor: str) -> None:
        g = self._require(gap_id, (OPEN, REPLAY_INCOMPLETE))
        info = self._info()
        start_id = _t(info["last-generated-id"]) if info else "0-0"
        if not self._transition(
            g, g["state"], REPLAY_REQUESTED, actor, "replay-start", self.store.now(),
            replay_from=frm, replay_to=to, replay_start_id=start_id,
        ):
            raise ValueError("gap changed concurrently")

    def replay_done(self, gap_id: str, *, actor: str) -> None:
        g = self._require(gap_id, (REPLAY_REQUESTED,))
        info = self._info()
        end_id = _t(info["last-generated-id"]) if info else "0-0"
        if not self._transition(
            g, REPLAY_REQUESTED, REPLAY_OBSERVING, actor, "replay-done", self.store.now(),
            replay_end_id=end_id,
        ):
            raise ValueError("gap changed concurrently")

    def mark_unreplayable(self, gap_id: str, *, actor: str, reason: str) -> None:
        g = self._require(gap_id, (OPEN, REPLAY_INCOMPLETE))
        self._transition(g, g["state"], REPLAY_UNAVAILABLE, actor, "mark-unreplayable",
                         self.store.now())
        self.store.audit(actor, "gap-unreplayable", {"gap_id": gap_id, "reason": reason})

    def accept(self, gap_id: str, *, operator: str, reason: str) -> dict[str, Any]:
        """Explicit acceptance of possible permanent loss (§6.7). Never automatic;
        does not move the cursor; the gap is CLOSED_ACCEPTED, not recovered."""
        if not reason.strip() or not operator.strip():
            raise ValueError("accept-gap needs --operator and --reason")
        g = self._require(gap_id, (OPEN, REPLAY_INCOMPLETE, REPLAY_UNAVAILABLE))
        now = self.store.now()
        if not self._transition(
            g, g["state"], CLOSED_ACCEPTED, operator, "accept-gap", now,
        ):
            raise ValueError("gap changed concurrently")
        detail = {
            "gap_id": gap_id,
            "reason": reason,
            "missing_count": g["missing_count"],
            "window": [str(g["window_from"]), str(g["window_to"])],
            "replay_attempts": sum(1 for a in g["audit"] or [] if '"replay-start"' in a),
        }
        self.db.run(
            "UPDATE {ks}.intake_gaps SET audit = audit + ? WHERE reader = ? AND gap_id = ?",
            [self.store.audit_entry(now, operator, "accept-gap-detail", **detail)],
            self.reader,
            gap_id,
        )
        self.store.audit(operator, "accept-gap", detail)
        metrics.INTAKE_OPEN_GAPS.set(len(self.open_gaps()))
        return detail

    def status(self) -> dict[str, Any]:
        st = self.state()
        info = self._info()
        gaps = self.gaps()
        open_ = [g for g in gaps if g["state"] not in CLOSED]
        added = int(info["entries-added"]) if info else 0
        ordinal = int(st["consumed_ordinal"] or 0) if st else 0
        return {
            "reader": self.reader,
            "initialized": st is not None,
            "mode": st["mode"] if st else HALTED,
            "halt_reason": (st["halt_reason"] if st else "uninitialized"),
            "cursor_id": st["cursor_id"] if st else None,
            "consumed_ordinal": ordinal,
            "entries_added": added,
            "lag_entries": max(added - ordinal, 0),
            "open_gaps": [
                {k: (str(v) if isinstance(v, datetime) else v) for k, v in g.items() if k != "audit"}
                for g in open_
            ],
            "closed_gaps": len(gaps) - len(open_),
            "invalid_entries": len(
                self.db.rows("SELECT entry_id FROM {ks}.intake_invalid WHERE reader = ?", self.reader)
            ),
            "healthy": st is not None and st["mode"] == RUNNING and not open_,
        }

    # --- loop ------------------------------------------------------------------

    def run(self, stop: threading.Event) -> None:
        last_renew = 0.0
        import time

        while not stop.is_set():
            if self.epoch is None and not self.acquire():
                stop.wait(self.cfg.lease_renew_s)
                continue
            if time.monotonic() - last_renew > self.cfg.lease_renew_s:
                if not self.renew():
                    continue
                last_renew = time.monotonic()
            result = self.cycle()
            if result.halted:
                stop.wait(self.cfg.poll_interval_s * 5)
            elif result.idle:
                stop.wait(self.cfg.poll_interval_s)


def gap_json(g: dict[str, Any]) -> dict[str, Any]:
    out = {k: (str(v) if isinstance(v, datetime) else v) for k, v in g.items()}
    out["audit"] = [json.loads(a) for a in g.get("audit") or []]
    return out
