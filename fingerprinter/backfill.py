"""Backfill (design §11.2, P9-D11/D15): re-encode contents of ``from_spec`` under ``to_spec``.

- **Eligible:** contents with a READY ``from_spec`` representation, enumerated
  from ``representations_by_spec`` bucket by bucket (cursor per bucket in
  ``backfill_progress``); optional ``targets_only``.
- **Work:** a request ``(content_id, to_spec_digest)`` at priority 10 with the
  content's original sources (external locators and/or the target reference);
  origin ``backfill`` — no ``encode.failed`` exists for such a request.
- **Bounds:** ``max_in_flight`` non-terminal backfill requests; pauses while the
  live queue is above ``queue_high_water`` (live work keeps priority).
- **Control:** pause/resume/cancel are LWTs on the job's ``epoch``; a crashed
  runner resumes from the last bucket cursor; re-running a bucket is idempotent.
- **Never destructive:** no row or object of ``from_spec`` is touched.
- **Source loss (D11):** candidate bytes are not archived; a content whose
  sources vanished or changed ends FAILED and is listed per content in
  ``status`` (``source_lost``) — never counted as processed. Its old
  representation stays available.
"""

from __future__ import annotations

import json
import time
from collections import Counter
from typing import Any

from fingerprinter.coord import uuid7
from fingerprinter.errors import FingerprinterError
from fingerprinter.requests import IN_PROGRESS, PENDING, Requests, Source
from fingerprinter.store import Store

SOURCE_LOSS = {"source_unreachable", "source_forbidden"}


class BackfillError(FingerprinterError):
    pass


class Backfill:
    def __init__(self, store: Store, requests: Requests) -> None:
        self.store = store
        self.db = store.db
        self.requests = requests
        self.cfg = store.settings.backfill

    def job(self, job_id: str) -> dict[str, Any]:
        row = self.db.one("SELECT * FROM {ks}.backfill_jobs WHERE job_id = ?", job_id, level="serial")
        if row is None:
            raise BackfillError(f"no backfill job {job_id}")
        return row

    def create(self, from_spec: str, to_spec: str, *, actor: str, targets_only: bool = False) -> str:
        to_row = self.store.spec_row(to_spec)
        if to_row is None or to_row["status"] != "active":
            raise BackfillError("backfill target spec must exist and be active (gated specs are refused)")
        if self.store.spec_row(from_spec) is None:
            raise BackfillError("unknown from_spec")
        job_id = uuid7()
        now = self.store.now()
        self.db.run(
            "INSERT INTO {ks}.backfill_jobs (job_id, from_spec, to_spec, state, epoch, filters, "
            "created_at, updated_at, buckets, audit) VALUES (?,?,?,'CREATED',0,?,?,?,?,?)",
            job_id, from_spec, to_spec, json.dumps({"targets_only": targets_only}), now, now,
            self.cfg.buckets, [self.store.audit_entry(now, actor, "create")],
        )
        return job_id

    def _set_state(self, job_id: str, frm: tuple[str, ...], to: str, actor: str) -> None:
        job = self.job(job_id)
        if job["state"] not in frm:
            raise BackfillError(f"job is {job['state']}; expected one of {frm}")
        res = self.db.lwt(
            "UPDATE {ks}.backfill_jobs SET state = ?, epoch = ?, updated_at = ?, audit = audit + ? "
            "WHERE job_id = ? IF epoch = ?",
            to, int(job["epoch"]) + 1, self.store.now(),
            [self.store.audit_entry(self.store.now(), actor, to.lower())], job_id, job["epoch"],
        )
        if not res.applied:
            raise BackfillError("job changed concurrently")

    def pause(self, job_id: str, *, actor: str) -> None:
        self._set_state(job_id, ("CREATED", "RUNNING"), "PAUSED", actor)

    def resume(self, job_id: str, *, actor: str) -> None:
        self._set_state(job_id, ("PAUSED",), "RUNNING", actor)

    def cancel(self, job_id: str, *, actor: str) -> None:
        self._set_state(job_id, ("CREATED", "RUNNING", "PAUSED"), "CANCELLED", actor)

    def _in_flight(self, job_id: str, to_spec: str) -> int:
        n = 0
        for item in self.db.rows("SELECT content_id FROM {ks}.backfill_items WHERE job_id = ?", job_id):
            req = self.requests.get(item["content_id"], to_spec, serial=False)
            if req is not None and req["state"] in (PENDING, IN_PROGRESS):
                n += 1
        return n

    def _sources(self, content_id: str) -> tuple[str | None, list[Source]]:
        digest, sources = None, {}
        for req in self.db.rows("SELECT * FROM {ks}.encode_requests WHERE content_id = ?", content_id):
            digest = digest or req["content_digest"]
            for key, doc in (req["sources"] or {}).items():
                sources.setdefault(key, Source(key, json.loads(doc)))
        return digest, list(sources.values())

    def run(self, job_id: str, *, actor: str, max_items: int | None = None,
            wait_s: float = 1.0, max_wait_s: float = 30.0) -> dict[str, int]:
        job = self.job(job_id)
        if job["state"] == "CREATED":
            self._set_state(job_id, ("CREATED",), "RUNNING", actor)
            job = self.job(job_id)
        if job["state"] != "RUNNING":
            raise BackfillError(f"job is {job['state']}")
        epoch = job["epoch"]
        filters = json.loads(job["filters"] or "{}")
        to_spec, from_spec = job["to_spec"], job["from_spec"]
        counts = {"enqueued": 0, "skipped_present": 0, "skipped_filter": 0, "no_source": 0}
        processed = 0
        for bucket in range(int(job["buckets"])):
            prog = self.db.one(
                "SELECT * FROM {ks}.backfill_progress WHERE job_id = ? AND bucket = ?", job_id, bucket
            )
            if prog is not None and prog["done"]:
                continue
            cursor = prog["cursor"] if prog else ""
            rows = self.db.rows(
                "SELECT content_id FROM {ks}.representations_by_spec WHERE spec_digest = ? AND bucket = ? "
                "AND content_id > ?",
                from_spec, bucket, cursor,
            )
            for r in rows:
                if max_items is not None and processed >= max_items:
                    return counts
                current = self.job(job_id)
                if current["epoch"] != epoch or current["state"] != "RUNNING":
                    return counts  # paused/cancelled/taken over
                waited = 0.0
                while (self._in_flight(job_id, to_spec) >= self.cfg.max_in_flight
                       or self.store.coord.queue_depth() > self.cfg.queue_high_water):
                    if waited >= max_wait_s:
                        return counts
                    time.sleep(wait_s)
                    waited += wait_s
                cid = r["content_id"]
                self._one(job_id, cid, to_spec, filters, counts)
                processed += 1
                self.db.run(
                    "INSERT INTO {ks}.backfill_progress (job_id, bucket, cursor, done) VALUES (?,?,?,false)",
                    job_id, bucket, cid,
                )
            self.db.run(
                "INSERT INTO {ks}.backfill_progress (job_id, bucket, cursor, done) VALUES (?,?,?,true)",
                job_id, bucket, rows[-1]["content_id"] if rows else cursor,
            )
        self._set_state(job_id, ("RUNNING",), "ENQUEUED", actor)
        return counts

    def _one(self, job_id: str, cid: str, to_spec: str, filters: dict[str, Any], counts: dict[str, int]) -> None:
        targets = self.db.rows("SELECT * FROM {ks}.targets_by_content WHERE content_id = ?", cid)
        if filters.get("targets_only") and not targets:
            counts["skipped_filter"] += 1
            return
        rep = self.store.representation(cid, to_spec)
        if rep is not None:
            counts["skipped_present"] += 1
            return
        digest, sources = self._sources(cid)
        if digest is None or not sources:
            counts["no_source"] += 1
            self._item(job_id, cid, "no_source", "no recorded source")
            return
        for t in targets:
            if t["state"] in ("RETIRED", "PURGED"):
                continue
            self.db.run(
                "INSERT INTO {ks}.target_representations (target_id, version, spec_digest, state, "
                "content_id) VALUES (?,?,?,'PENDING',?) IF NOT EXISTS",
                t["target_id"], t["version"], to_spec, cid,
            )
        first, *rest = sources
        self.requests.upsert_internal(cid, to_spec, digest=digest, source=first,
                                      priority=self.cfg.priority, origin="backfill")
        for src in rest:
            self.requests.upsert_internal(cid, to_spec, digest=digest, source=src,
                                          priority=self.cfg.priority, origin="backfill")
        self._item(job_id, cid, "enqueued", None)
        counts["enqueued"] += 1

    def _item(self, job_id: str, cid: str, outcome: str, detail: str | None) -> None:
        self.db.run(
            "INSERT INTO {ks}.backfill_items (job_id, content_id, outcome, detail, updated_at) "
            "VALUES (?,?,?,?,?)",
            job_id, cid, outcome, detail, self.store.now(),
        )

    def status(self, job_id: str) -> dict[str, Any]:
        job = self.job(job_id)
        states: Counter[str] = Counter()
        failures: Counter[str] = Counter()
        source_lost, failed = [], []
        for item in self.db.rows("SELECT * FROM {ks}.backfill_items WHERE job_id = ?", job_id):
            if item["outcome"] == "no_source":
                states["NO_SOURCE"] += 1
                source_lost.append({"content_id": item["content_id"], "detail": item["detail"]})
                continue
            req = self.requests.get(item["content_id"], job["to_spec"], serial=False)
            state = req["state"] if req else "MISSING"
            states[state] += 1
            if state == "FAILED":
                failures[req["last_failure"]] += 1
                entry = {"content_id": item["content_id"], "failure": req["last_failure"],
                         "detail": req["last_detail"], "bad_sources": dict(req["bad_sources"] or {})}
                failed.append(entry)
                if req["last_failure"] in SOURCE_LOSS:
                    source_lost.append(entry)
        return {
            "job_id": job_id,
            "state": job["state"],
            "from_spec": job["from_spec"],
            "to_spec": job["to_spec"],
            "requests": dict(states),
            "failures_by_class": dict(failures),
            "failed": failed,
            "source_lost": source_lost,
            "complete": job["state"] == "ENQUEUED" and not states.get(PENDING) and not states.get(IN_PROGRESS),
        }
