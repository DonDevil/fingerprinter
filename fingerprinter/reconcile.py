"""Reconciler and orphan sweeper (design §8.4–§8.5, P9-D20).

**Intent pass** (every ``reconciler.interval_s``): every intent older than
``intent_grace_s`` is resolved against its authoritative fact (SERIAL read):
complete missing follow-ups and publication, or clean up.

**Full pass** (daily, token-range scan): defence in depth if an intent were
ever lost — rows with a false flag or a null ack and no open intent get a new
intent; non-terminal requests without their due entry get it back; non-terminal
requests whose representation exists converge to READY; FAILED requests with an
unpublished envelope get an intent; target bindings/events are repaired by the
target manager hook.

**Sweeper** (after the intent pass): deletes only objects older than
``orphan_grace_s`` (> ``encoder.attempt_timeout_s``) that the authoritative row
does not reference, that no open intent names, and whose attempt is not alive.

Implementation narrowing (design §22): for an intent whose attempt is dead and
whose fact is absent, the reconciler deletes the intent but leaves the object to
the sweeper's grace, so a paused stale worker that still wins its insert can
never reference a deleted object.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from fingerprinter import metrics
from fingerprinter.commit import Committer
from fingerprinter.coord import utc
from fingerprinter.objects import Objects, parse_rep_key, parse_uri
from fingerprinter.publish import FAILED, Outcome
from fingerprinter.requests import FAILED_STATE, IN_PROGRESS, PENDING, Requests
from fingerprinter.store import Intent, Store


@dataclass
class PassReport:
    seen: int = 0
    completed: int = 0
    cleaned: int = 0
    held: int = 0
    skipped: int = 0
    repairs: dict[str, int] = field(default_factory=dict)

    def repair(self, kind: str) -> None:
        self.repairs[kind] = self.repairs.get(kind, 0) + 1
        metrics.REPAIRS.labels(kind=kind).inc()


class Reconciler:
    def __init__(self, store: Store, requests: Requests, committer: Committer, objects: Objects) -> None:
        self.store = store
        self.db = store.db
        self.requests = requests
        self.committer = committer
        self.objects = objects
        self.target_hooks: dict[str, Callable[[Intent, PassReport], None]] = {}
        """``target_event`` / ``target_register`` resolvers, set by the target manager."""
        self.full_pass_hooks: list[Callable[[PassReport], None]] = []

    def _attempt_alive(self, content_id: str, attempt_id: str | None, now: datetime) -> bool:
        if attempt_id is None:
            return False
        for req in self.db.rows(
            "SELECT state, attempt_id, lease_until FROM {ks}.encode_requests WHERE content_id = ?",
            content_id,
        ):
            if (
                req["state"] == IN_PROGRESS
                and req["attempt_id"] == attempt_id
                and utc(req["lease_until"]) > now
            ):
                return True
        return False

    # --- intent pass ----------------------------------------------------------

    def intent_pass(self, *, grace_s: float | None = None) -> PassReport:
        grace = timedelta(
            seconds=self.store.settings.reconciler.intent_grace_s if grace_s is None else grace_s
        )
        now = self.store.now()
        report = PassReport()
        open_count = 0
        for intent in list(self.store.intents()):
            open_count += 1
            report.seen += 1
            if intent.created_at and now - intent.created_at < grace:
                report.skipped += 1
                continue
            handler = getattr(self, f"_resolve_{intent.kind}", None)
            if handler is None:
                hook = self.target_hooks.get(intent.kind)
                if hook is None:
                    report.skipped += 1
                    continue
                hook(intent, report)
                continue
            handler(intent, report, now)
        for day in self.store.intent_days():
            self.store.drop_intent_day_if_empty(day, now.strftime("%Y-%m-%d"))
        metrics.INTENTS_OPEN.set(open_count - report.completed - report.cleaned)
        return report

    def _resolve_rep_commit(self, intent: Intent, report: PassReport, now: datetime) -> None:
        key = intent.fact_key
        row = self.store.representation(key["content_id"], key["spec_digest"], serial=True)
        if row is not None and row["artifact_attempt_id"] == intent.attempt_id:
            if self.committer.complete(row, intent):
                report.completed += 1
                report.repair("rep_followups")
            else:
                report.held += 1
            return
        if row is not None:
            # The fact belongs to another attempt: this intent's object is an orphan.
            if intent.artifact_uri:
                bucket, obj = parse_uri(intent.artifact_uri)
                self.objects.delete(bucket, obj)
                metrics.ORPHANS.labels(kind="loser").inc()
            self.store.intent_del(intent)
            report.cleaned += 1
            report.repair("rep_loser_cleanup")
            return
        if self._attempt_alive(key["content_id"], intent.attempt_id, now):
            report.skipped += 1
            return
        # No fact and the attempt is dead: the request is requeued by the due index;
        # the object is left to the sweeper's grace (module docstring).
        self.store.intent_del(intent)
        report.cleaned += 1
        report.repair("rep_dead_attempt")

    def _resolve_request_failed(self, intent: Intent, report: PassReport, now: datetime) -> None:
        key = intent.fact_key
        row = self.requests.get(key["content_id"], key["spec_key"])
        if row is None or row["failed_event_id"] != intent.event_id:
            self.store.intent_del(intent)  # the FAILED transition never applied
            report.cleaned += 1
            return
        if row["failed_event_published_at"] is not None:
            self.store.intent_del(intent)
            report.completed += 1
            return
        out = self.requests.publish_failed(intent)
        if out is Outcome.PUBLISHED:
            report.completed += 1
            report.repair("failed_published")
        else:
            report.held += 1

    def _resolve_republish(self, intent: Intent, report: PassReport, now: datetime) -> None:
        out = self.requests.publish_intent(intent)
        if out is Outcome.PUBLISHED:
            report.completed += 1
        else:
            report.held += 1

    # --- full pass --------------------------------------------------------------

    def full_pass(self) -> PassReport:
        report = PassReport()
        intents = list(self.store.intents())
        open_reps = {
            (i.fact_key.get("content_id"), i.fact_key.get("spec_digest"))
            for i in intents
            if i.kind == "rep_commit"
        }
        open_failed = {i.event_id for i in intents if i.kind == "request_failed"}
        for row in self.db.stream("SELECT * FROM {ks}.representations"):
            report.seen += 1
            if row["state"] != "READY":
                continue
            done = row["f_index"] and row["f_request"] and row["f_targets"]
            if done and row["ready_event_published_at"] is not None:
                continue
            if (row["content_id"], row["spec_digest"]) in open_reps:
                continue
            self.store.intent_put(
                "rep_commit",
                {"content_id": row["content_id"], "spec_digest": row["spec_digest"]},
                attempt_id=row["artifact_attempt_id"],
                event_id=row["ready_event_id"],
                artifact_uri=row["artifact_uri"],
            )
            report.repair("rep_intent_recreated")
        for req in self.requests.all():
            report.seen += 1
            state = req["state"]
            if state in (PENDING, IN_PROGRESS):
                rep = self.store.representation(req["content_id"], req["resolved_spec"])
                if rep is not None and rep["state"] == "READY":
                    fresh = self.requests.get(req["content_id"], req["spec_key"])
                    if fresh is not None and self.requests.mark_ready(fresh, rep["representation_id"]):
                        report.repair("request_converged_ready")
                        continue
                due = self.requests.due_time(req)
                if due is not None and not self.store.due_exists(req["content_id"], req["spec_key"], due):
                    self.store.due_put(req["content_id"], req["spec_key"], due)
                    metrics.DUE_REPAIRED.inc()
                    report.repair("due_entry_recreated")
            elif (
                state == FAILED_STATE
                and req["failed_envelope"]
                and req["failed_event_published_at"] is None
                and req["failed_event_id"] not in open_failed
            ):
                self.store.intent_put(
                    "request_failed",
                    {
                        "content_id": req["content_id"],
                        "spec_key": req["spec_key"],
                        "episode": req["episode"],
                    },
                    event_id=req["failed_event_id"],
                    stream=FAILED,
                    envelope=req["failed_envelope"],
                )
                report.repair("failed_intent_recreated")
        for hook in self.full_pass_hooks:
            hook(report)
        return report

    # --- sweeper ----------------------------------------------------------------

    def sweep(self, *, grace_s: float | None = None) -> dict[str, int]:
        grace = timedelta(seconds=self.store.settings.orphan_grace_s if grace_s is None else grace_s)
        now = self.store.now()
        cutoff = now - grace
        counts = {"kept": 0, "deleted": 0, "skipped_intent": 0, "skipped_active": 0, "aborted_uploads": 0}
        intents = list(self.store.intents())
        intent_attempts = {i.attempt_id for i in intents if i.attempt_id}
        intent_uploads = {
            (i.payload or {}).get("staging_key") for i in intents if i.kind == "target_register"
        }
        intent_digests = {
            (i.payload or {}).get("sha256") for i in intents if i.kind == "target_register"
        }
        bucket = self.objects.cfg.representations_bucket
        for obj in self.objects.list(bucket, "rep/"):
            if obj.last_modified is None or obj.last_modified > cutoff:
                continue
            parsed = parse_rep_key(obj.key)
            if parsed is None:
                continue
            spec_hex, content_id, attempt_id = parsed
            row = self.store.representation(content_id, "sha256:" + spec_hex, serial=True)
            if row is not None and row["artifact_attempt_id"] == attempt_id:
                counts["kept"] += 1
                continue
            if attempt_id in intent_attempts:
                counts["skipped_intent"] += 1
                continue
            if self._attempt_alive(content_id, attempt_id, now):
                counts["skipped_active"] += 1
                continue
            self.objects.delete(bucket, obj.key)
            counts["deleted"] += 1
            metrics.ORPHANS.labels(kind="rep").inc()
        tbucket = self.objects.cfg.targets_bucket
        for obj in self.objects.list(tbucket, "staging/"):
            if obj.last_modified is None or obj.last_modified > cutoff:
                continue
            if obj.key in intent_uploads:
                counts["skipped_intent"] += 1
                continue
            self.objects.delete(tbucket, obj.key)
            counts["deleted"] += 1
            metrics.ORPHANS.labels(kind="staging").inc()
        for obj in self.objects.list(tbucket, "ref/"):
            if obj.last_modified is None or obj.last_modified > cutoff:
                continue
            sha = obj.key.removeprefix("ref/")
            if sha in intent_digests or self._ref_referenced(sha):
                counts["kept"] += 1
                continue
            self.objects.delete(tbucket, obj.key)
            counts["deleted"] += 1
            metrics.ORPHANS.labels(kind="ref").inc()
        for b in (bucket, tbucket):
            for key, upload_id, initiated in self.objects.incomplete_uploads(b, ""):
                if initiated is not None and initiated < cutoff:
                    self.objects.abort_upload(b, key, upload_id)
                    counts["aborted_uploads"] += 1
        return counts

    def _ref_referenced(self, sha_hex: str) -> bool:
        from fingerprinter.targets import content_id_of_sha

        cid = content_id_of_sha(sha_hex)
        rows = self.db.rows(
            "SELECT state FROM {ks}.targets_by_content WHERE content_id = ?", str(cid)
        )
        return any(r["state"] != "PURGED" for r in rows)


def summarize(report: PassReport) -> dict[str, Any]:
    return {
        "seen": report.seen,
        "completed": report.completed,
        "cleaned": report.cleaned,
        "held": report.held,
        "skipped": report.skipped,
        "repairs": report.repairs,
    }
