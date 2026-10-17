"""Work discovery: hint reader and backstop scans (design §6.1; P10-D5, D20; corrected C-4, C-8).

Authoritative trigger = Scylla state; streams are only hints. No P9 code,
table, stream registration or reconciler pass is changed.

- **Hint reader** (``representation.ready.v1``, group-less, best effort): one
  owner per reader row ``p10_readers('rep-ready')`` (lease + epoch, P9 intake
  pattern). Each entry only causes an idempotent upsert of desired state
  derived from the *current* authoritative rows; the cursor advances by
  ``IF epoch = :mine AND cursor_id = :prev`` only after that work is durable.
  A trimmed range jumps to the oldest retained entry, records the gap and
  asks for a backstop pass.
- **reps scan** per indexed spec and bucket of ``representations_by_spec``
  (clustering cursor ``content_id > ?``), cursor persisted per page.
- **targets scan**: partition-granular token walk of
  ``target_representations`` (``SELECT DISTINCT target_id, token(target_id)
  … WHERE token(target_id) >= ? LIMIT n``), each partition read in full, the
  cursor ``(token, target_id)`` names only completely processed partitions and
  resumes with ``>=``; ``scan_stalled`` instead of skipping. Both scans stamp
  their lease epoch into ``p10_scans.owner_epoch`` so a stale scanner cannot
  move a cursor.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import timedelta
from typing import Any

import redis
from antipiracy_contracts.events import decode_event_as
from antipiracy_contracts.events.fingerprinting import RepresentationReady

from fingerprinter import metrics
from fingerprinter.coord import utc
from fingerprinter.db import SERIAL
from fingerprinter.errors import StorageUnavailableError
from fingerprinter.p10ctx import BUCKETS, KIND_B, P10, Subject
from fingerprinter.publish import READY

log = logging.getLogger("fingerprinter.discovery")
MIN_TOKEN = -(2**63)


class ScanStalledError(Exception):
    pass


class Leases:
    """``p10_readers`` lease rows: acquire ``IF epoch = ?``, renew ``IF owner = ? AND epoch = ?``."""

    def __init__(self, p10: P10) -> None:
        self.p = p10
        self.epochs: dict[str, int] = {}

    def acquire(self, name: str) -> int | None:
        db, now = self.p.db, self.p.now()
        ttl = timedelta(seconds=self.p.settings.matcher_reconcile.lease_ttl_s)
        row = db.one("SELECT * FROM {ks}.p10_readers WHERE reader = ?", name, level=SERIAL)
        if row is None:
            if db.lwt("INSERT INTO {ks}.p10_readers (reader, owner, epoch, lease_until, gaps) VALUES (?,?,1,?,0) "
                      "IF NOT EXISTS", name, self.p.instance, now + ttl).applied:
                self.epochs[name] = 1
                return 1
            return None
        mine = self.epochs.get(name)
        if row["owner"] == self.p.instance and mine == row["epoch"] and utc(row["lease_until"]) > now:
            db.lwt("UPDATE {ks}.p10_readers SET lease_until = ? WHERE reader = ? IF owner = ? AND epoch = ?",
                   now + ttl, name, self.p.instance, mine)
            return mine
        if row["owner"] is not None and row["lease_until"] and utc(row["lease_until"]) > now:
            return None
        e = int(row["epoch"] or 0) + 1
        if db.lwt("UPDATE {ks}.p10_readers SET owner = ?, epoch = ?, lease_until = ? WHERE reader = ? IF epoch = ?",
                  self.p.instance, e, now + ttl, name, row["epoch"]).applied:
            self.epochs[name] = e
            return e
        return None


class Discovery:
    def __init__(self, p10: P10, indexer: Any, tasks: Any, lowinfo: Any) -> None:
        self.p = p10
        self.db = p10.db
        self.indexer = indexer
        self.tasks = tasks
        self.lowinfo = lowinfo
        self.leases = Leases(p10)
        self.hook = p10.hook

    # --- desired state of one subject (shared by every producer) ---------------------------

    def desire(self, s: Subject) -> None:
        """Index intent + tasks for one subject: idempotent; derived from the authoritative read."""
        if not s.subject_id:
            return
        self.indexer.write_intent(s)
        if s.live:
            from fingerprinter.matching import kinds_of

            for kind in kinds_of(s, self.p):
                self.tasks.create(s, kind)

    # --- hint reader ------------------------------------------------------------------------

    def read_hints(self, *, batch: int | None = None) -> dict[str, int]:
        out = {"entries": 0, "applied": 0, "invalid": 0, "gaps": 0}
        epoch = self.leases.acquire("rep-ready")
        if epoch is None:
            out["not_owner"] = 1
            return out
        row = self.db.one("SELECT * FROM {ks}.p10_readers WHERE reader = 'rep-ready'", level=SERIAL)
        cursor = row.get("cursor_id")
        stream = self.p.coord.stream(READY)
        client = self.p.coord.client
        n = batch or self.p.settings.matcher_reconcile.reader_batch
        try:
            first = client.xrange(stream, min="-", max="+", count=1)
            if cursor:
                entries = client.xrange(stream, min="(" + cursor, max="+", count=n)
            else:
                entries = client.xrange(stream, min="-", max="+", count=n)
        except redis.RedisError as exc:
            raise StorageUnavailableError(f"hint read: {exc}") from exc
        if cursor and first:
            fid = first[0][0].decode() if isinstance(first[0][0], bytes) else first[0][0]
            if _sid(fid) > _sid(cursor) and not client.xrange(stream, min=cursor, max=cursor):
                # entries after our cursor were trimmed before we read them: jump, record, ask for a backstop pass
                out["gaps"] += 1
                metrics.P10_HINT_GAPS.inc()
                self.db.lwt("UPDATE {ks}.p10_readers SET last_gap_at = ?, gaps = ? WHERE reader = 'rep-ready' "
                            "IF epoch = ?", self.p.now(), int(row.get("gaps") or 0) + 1, epoch)
                self.request_backstop("hint gap")
        specs = set(self.p.indexed_specs())
        last = cursor
        for eid, fields in entries:
            eid = eid.decode() if isinstance(eid, bytes) else eid
            out["entries"] += 1
            raw = fields.get(b"envelope") or fields.get("envelope")
            try:
                env = decode_event_as(raw, RepresentationReady)
                rep = env.payload.representation
                spec = str(rep.spec.config_digest)
            except Exception:  # noqa: BLE001 - an undecodable hint is counted and skipped (the backstop covers it)
                out["invalid"] += 1
                last = eid
                continue
            if spec in specs:
                self.desire(self.p.media_subject(str(rep.content_id), spec))
                out["applied"] += 1
            last = eid
        self.hook("reader.before_cursor_lwt", last=last)
        if last != cursor:
            res = self.db.lwt("UPDATE {ks}.p10_readers SET cursor_id = ? WHERE reader = 'rep-ready' "
                              "IF epoch = ? AND cursor_id = ?", last, epoch, cursor)
            if not res.applied:
                self.leases.epochs.pop("rep-ready", None)
                out["lost_lease"] = 1
        return out

    def request_backstop(self, why: str) -> None:
        """Hints were lost: the next reconciler cycle runs the reps scan without waiting for its interval."""
        self.backstop_requested = True
        log.info(json.dumps({"event": "backstop_requested", "why": why}))

    backstop_requested = False

    # --- scans -----------------------------------------------------------------------------------

    def _stamp(self, scan: str, spec: str, bucket: int, epoch: int) -> dict[str, Any]:
        row = self.db.one("SELECT * FROM {ks}.p10_scans WHERE scan = ? AND spec_digest = ? AND bucket = ?",
                          scan, spec, bucket, level=SERIAL)
        if row is None:
            self.db.lwt("INSERT INTO {ks}.p10_scans (scan, spec_digest, bucket, cycle, owner_epoch, done) "
                        "VALUES (?,?,?,0,?,false) IF NOT EXISTS", scan, spec, bucket, epoch)
        elif int(row.get("owner_epoch") or 0) < epoch:
            self.db.lwt("UPDATE {ks}.p10_scans SET owner_epoch = ? WHERE scan = ? AND spec_digest = ? AND bucket = ? "
                        "IF owner_epoch < ?", epoch, scan, spec, bucket, epoch)
        return self.db.one("SELECT * FROM {ks}.p10_scans WHERE scan = ? AND spec_digest = ? AND bucket = ?",
                           scan, spec, bucket, level=SERIAL)

    def _cursor(self, scan: str, spec: str, bucket: int, epoch: int, **cols: Any) -> None:
        sets = ", ".join(f"{k} = ?" for k in cols)
        res = self.db.lwt(f"UPDATE {{ks}}.p10_scans SET {sets}, updated_at = ? WHERE scan = ? AND spec_digest = ? "  # noqa: S608
                          "AND bucket = ? IF owner_epoch = ?", *cols.values(), self.p.now(), scan, spec, bucket, epoch)
        if not res.applied:
            raise StorageUnavailableError(f"{scan} scan lease lost")

    def scan_reps(self, spec: str, *, max_rows: int | None = None) -> dict[str, int]:
        """One pass over every bucket from its cursor (a page per bucket per call, paced); a cycle ends
        when all 64 buckets end, then ``cycle += 1`` and the cursors reset."""
        epoch = self.leases.acquire("scan:reps")
        if epoch is None:
            return {"not_owner": 1}
        rc = self.p.settings.matcher_reconcile
        page = rc.scan_page
        rows_seen = 0
        all_done = True
        for b in range(BUCKETS):
            st = self._stamp("reps", spec, b, epoch)
            if st.get("done"):
                continue
            last = st.get("last_key") or ""
            rows = self.db.rows("SELECT content_id FROM {ks}.representations_by_spec WHERE spec_digest = ? AND "
                                "bucket = ? AND content_id > ? LIMIT ?", spec, b, last, page)
            for r in rows:
                self.desire(self.p.media_subject(r["content_id"], spec))
                rows_seen += 1
                metrics.P10_SCAN_ROWS.labels(scan="reps").inc()
                time.sleep(1.0 / rc.scan_rows_per_s)
            done = len(rows) < page
            all_done = all_done and done
            self._cursor("reps", spec, b, epoch, last_key=rows[-1]["content_id"] if rows else last, done=done)
            if max_rows is not None and rows_seen >= max_rows:
                return {"rows": rows_seen, "cycle_complete": 0}
        if all_done:
            for b in range(BUCKETS):
                st = self._stamp("reps", spec, b, epoch)
                self._cursor("reps", spec, b, epoch, last_key="", done=False, cycle=int(st.get("cycle") or 0) + 1,
                             cycle_started_at=self.p.now())
            metrics.P10_SCAN_CYCLE.labels(scan="reps").inc()
        return {"rows": rows_seen, "cycle_complete": int(all_done)}

    def scan_targets(self, *, keys_page: int | None = None, fetch_size: int = 500,
                     max_partitions: int | None = None) -> dict[str, int]:
        """The partition-granular token walk (C-8). One row ``p10_scans('targets', '*', 0)``."""
        epoch = self.leases.acquire("scan:targets")
        if epoch is None:
            return {"not_owner": 1}
        rc = self.p.settings.matcher_reconcile
        n = keys_page or rc.targets_keys_page
        specs = set(self.p.indexed_specs())
        st = self._stamp("targets", "*", 0, epoch)
        last_token, last_tid = _parse_key(st.get("last_key"))
        partitions = 0
        visited: list[tuple[str, int, str]] = []
        while True:
            keys = self.db.rows("SELECT DISTINCT target_id, token(target_id) AS t FROM {ks}.target_representations "
                                "WHERE token(target_id) >= ? LIMIT ?", last_token, n)
            if len(keys) >= n and all(int(k["t"]) == last_token for k in keys):
                metrics.P10_SCAN_STALLED.labels(scan="targets").inc()
                raise ScanStalledError(f"{n} partitions share token {last_token}: refusing to skip")
            for k in keys:
                tok, tid = int(k["t"]), k["target_id"]
                if tok == last_token and tid == last_tid:
                    continue  # the `>=` resume re-reads the last completed partition: an optimisation only
                self.hook("scan.before_partition", target_id=tid)
                bindings = list(self.db.stream("SELECT * FROM {ks}.target_representations WHERE target_id = ?", tid,
                                               fetch_size=fetch_size))
                targets = {r["version"]: r for r in self.db.rows("SELECT * FROM {ks}.targets WHERE target_id = ?", tid)}
                for bnd in bindings:
                    visited.append((tid, bnd["version"], bnd["spec_digest"]))
                    if bnd["spec_digest"] not in specs:
                        continue
                    s = self.p._target_subject(bnd, targets.get(bnd["version"]), bnd["spec_digest"], tid,
                                               bnd["version"])
                    self.desire(s)
                    metrics.P10_SCAN_ROWS.labels(scan="targets").inc()
                self.hook("scan.after_partition_before_cursor", target_id=tid)
                self._cursor("targets", "*", 0, epoch, last_key=f"{tok}:{tid}")  # only after the whole partition
                last_token, last_tid = tok, tid
                partitions += 1
                if max_partitions is not None and partitions >= max_partitions:
                    return {"partitions": partitions, "cycle_complete": 0, "visited": len(visited)}
            if len(keys) < n:
                break
        self._cursor("targets", "*", 0, epoch, last_key=f"{MIN_TOKEN}:", cycle=int(st.get("cycle") or 0) + 1,
                     cycle_started_at=self.p.now())
        metrics.P10_SCAN_CYCLE.labels(scan="targets").inc()
        if self.lowinfo is not None:
            for spec in specs:
                self.lowinfo.observe(spec)
        self.last_visited = visited
        return {"partitions": partitions, "cycle_complete": 1, "visited": len(visited)}


def _sid(value: str) -> tuple[int, int]:
    ms, _, seq = value.partition("-")
    return int(ms), int(seq or 0)


def _parse_key(key: str | None) -> tuple[int, str]:
    if not key:
        return MIN_TOKEN, ""
    tok, _, tid = key.partition(":")
    return int(tok), tid


__all__ = ["KIND_B", "Discovery", "Leases", "ScanStalledError"]
