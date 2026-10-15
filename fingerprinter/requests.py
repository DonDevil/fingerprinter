"""Authoritative encode requests (design §6.10–§7.4, P9-D8/D9/D13/D23).

``encode_requests (content_id, spec_key)`` is the only authority on request
state: ``PENDING`` (``next_attempt_at``) → ``IN_PROGRESS`` (``attempt_id``,
``lease_until``) → ``READY`` | ``FAILED``. **Every** write to a request row is a
conditional write (LWT); transitions out of IN_PROGRESS are fenced by
``attempt_id`` so a stale worker can never move a request it no longer owns.

Accounting (§7.2): ``claims`` is incremented by every claim and never decreases;
``failures`` counts only failures attributed to the request; released and
abandoned attempts do not count. FAILED at ``failures ≥ max_attempts`` or at
``claims − claims_base ≥ max_claims`` (``claim_budget_exhausted``). A reopen or
operator retry starts a new episode: ``failures = 0``, ``claims_base = claims``.

``encode.failed`` (§7.4) is published only for requests that have a real
``encode.requested`` event: ``request_id`` = first event id by arrival,
``attempt`` = ``claims`` (monotonic, so every failure of a request has a
distinct idempotency key). Target-only and backfill-only requests publish none;
their failure is visible on the target / in the backfill report.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from antipiracy_contracts.events import EventEnvelope, encode_event, new_event
from antipiracy_contracts.events.fingerprinting import (
    EncodeFailed,
    EncodeFailure,
    EncodeRequested,
    RepresentationReady,
)
from antipiracy_contracts.ids import ContentId, EventId
from antipiracy_contracts.models.representations import Representation, RepresentationSpec

from fingerprinter import metrics
from fingerprinter.coord import utc
from fingerprinter.db import SERIAL
from fingerprinter.errors import EncodeError
from fingerprinter.publish import FAILED, READY, Outcome, Publisher
from fingerprinter.store import Intent, Store, request_member

PENDING, IN_PROGRESS, READY_STATE, FAILED_STATE = "PENDING", "IN_PROGRESS", "READY", "FAILED"
DEFAULT = "default"
MAX_LWT_RETRIES = 20
STALE_DUE_GRACE = timedelta(seconds=120)


@dataclass(frozen=True)
class Source:
    key: str
    doc: dict[str, Any]

    @property
    def type(self) -> str:
        return str(self.doc["type"])


def external_source(env: EventEnvelope[EncodeRequested], at: datetime) -> Source:
    payload = env.payload
    doc = {
        "type": "external_http",
        "media": json.loads(payload.source.model_dump_json()),
        "referer": json.loads(payload.referer.model_dump_json()) if payload.referer else None,
        "first_event_id": str(env.event_id),
        "added_at": at.isoformat(),
    }
    return Source("http:" + str(payload.source.locator.url), doc)


def internal_source(bucket: str, key: str, size: int, sha256_hex: str, added_by: str) -> Source:
    doc = {
        "type": "internal_object",
        "bucket": bucket,
        "key": key,
        "size": size,
        "sha256": sha256_hex,
        "added_by": added_by,
    }
    return Source("obj:" + key, doc)


def spec_key_of(required: RepresentationSpec | None) -> str:
    return DEFAULT if required is None else str(required.config_digest)


class Requests:
    def __init__(self, store: Store, publisher: Publisher) -> None:
        self.store = store
        self.db = store.db
        self.pub = publisher
        self.cfg = store.settings.encoder
        self.on_failed: list[Callable[[dict[str, Any]], None]] = []
        """Hooks run after a request becomes FAILED (the target manager marks BUILD_FAILED)."""

    # --- reads --------------------------------------------------------------

    def get(self, content_id: str, spec_key: str, *, serial: bool = True) -> dict[str, Any] | None:
        return self.db.one(
            "SELECT * FROM {ks}.encode_requests WHERE content_id = ? AND spec_key = ?",
            content_id,
            spec_key,
            level=SERIAL if serial else "quorum",
        )

    def all(self) -> list[dict[str, Any]]:
        return list(self.db.stream("SELECT * FROM {ks}.encode_requests"))

    @staticmethod
    def due_time(row: dict[str, Any]) -> datetime | None:
        if row["state"] == PENDING:
            return utc(row["next_attempt_at"])
        if row["state"] == IN_PROGRESS:
            return utc(row["lease_until"])
        return None

    # --- intake upsert (§6.10) ---------------------------------------------

    def upsert_event(self, env: EventEnvelope[EncodeRequested]) -> str:
        payload = env.payload
        content_id = str(payload.content.content_id)
        spec_key = spec_key_of(payload.required_spec)
        event_id = str(env.event_id)
        now = self.store.now()
        source = external_source(env, now)
        required = payload.required_spec.model_dump_json() if payload.required_spec else None
        for _ in range(MAX_LWT_RETRIES):
            row = self.get(content_id, spec_key)
            if row is None:
                created = self._create(
                    content_id,
                    spec_key,
                    digest=str(payload.content.digest),
                    source=source,
                    priority=int(payload.priority),
                    origin="event",
                    event_id=event_id,
                    required=required,
                    now=now,
                )
                if created is not None:
                    return created
                continue
            if event_id in (row["request_event_ids"] or set()):
                return "duplicate"
            outcome = self._apply_new_event(row, event_id, source, int(payload.priority), now)
            if outcome is not None:
                return outcome
        raise RuntimeError(f"upsert of {content_id}/{spec_key} did not converge")

    def upsert_internal(
        self,
        content_id: str,
        spec_key: str,
        *,
        digest: str,
        source: Source,
        priority: int,
        origin: str,
    ) -> str:
        """Target manager / backfill request: no ``encode.requested`` event exists."""
        now = self.store.now()
        for _ in range(MAX_LWT_RETRIES):
            row = self.get(content_id, spec_key)
            if row is None:
                created = self._create(
                    content_id,
                    spec_key,
                    digest=digest,
                    source=source,
                    priority=priority,
                    origin=origin,
                    event_id=None,
                    required=None,
                    now=now,
                )
                if created is not None:
                    return created
                continue
            state = row["state"]
            if state in (PENDING, IN_PROGRESS):
                res = self.db.lwt(
                    "UPDATE {ks}.encode_requests SET sources = sources + ?, priority = ?, "
                    "updated_at = ? WHERE content_id = ? AND spec_key = ? IF state = ? AND episode = ?",
                    {source.key: json.dumps(source.doc)},
                    max(int(row["priority"] or 0), priority),
                    now,
                    content_id,
                    spec_key,
                    state,
                    row["episode"],
                )
                if res.applied:
                    return "source_added"
                continue
            if state == FAILED_STATE and source.key not in (row["tried_sources"] or set()):
                if self._reopen(row, now, extra_source=source):
                    return "reopened"
                continue
            return state.lower()
        raise RuntimeError(f"upsert of {content_id}/{spec_key} did not converge")

    def _create(
        self,
        content_id: str,
        spec_key: str,
        *,
        digest: str,
        source: Source,
        priority: int,
        origin: str,
        event_id: str | None,
        required: str | None,
        now: datetime,
    ) -> str | None:
        resolved = self.store.default_spec_digest() if spec_key == DEFAULT else spec_key
        rep = self.store.representation(content_id, resolved, serial=True)
        state = READY_STATE if rep is not None and rep["state"] == "READY" else PENDING
        if state == PENDING:
            self.store.due_put(content_id, spec_key, now)
        res = self.db.lwt(
            "INSERT INTO {ks}.encode_requests (content_id, spec_key, content_digest, resolved_spec, "
            "required_spec, origin, state, priority, sources, request_event_ids, first_event_id, "
            "next_attempt_at, claims, claims_base, failures, episode, representation_id, "
            "created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0,0,0,1,?,?,?) IF NOT EXISTS",
            content_id,
            spec_key,
            digest,
            resolved,
            required,
            origin,
            state,
            priority,
            {source.key: json.dumps(source.doc)},
            {event_id} if event_id else set(),
            event_id,
            now,
            rep["representation_id"] if state == READY_STATE else None,
            now,
            now,
        )
        if not res.applied:
            if state == PENDING:
                self.store.due_del(content_id, spec_key, now)
            return None
        metrics.REQUESTS.labels(outcome="created" if state == PENDING else "reused").inc()
        if state == READY_STATE and rep is not None:
            self._republish_ready(rep, cause=event_id)
            return "reused"
        return "created"

    def _apply_new_event(
        self, row: dict[str, Any], event_id: str, source: Source, priority: int, now: datetime
    ) -> str | None:
        content_id, spec_key, state = row["content_id"], row["spec_key"], row["state"]
        sources = dict(row["sources"] or {})
        new_sources = {} if source.key in sources else {source.key: json.dumps(source.doc)}
        first = row["first_event_id"] or event_id
        if state in (PENDING, IN_PROGRESS):
            res = self.db.lwt(
                "UPDATE {ks}.encode_requests SET request_event_ids = request_event_ids + ?, "
                "sources = sources + ?, priority = ?, first_event_id = ?, updated_at = ? "
                "WHERE content_id = ? AND spec_key = ? IF state = ? AND episode = ? "
                "AND first_event_id = ?",
                {event_id},
                new_sources,
                max(int(row["priority"] or 0), priority),
                first,
                now,
                content_id,
                spec_key,
                state,
                row["episode"],
                row["first_event_id"],
            )
            return "source_added" if res.applied else None
        if state == READY_STATE:
            res = self._add_event(row, event_id, first, now)
            if not res:
                return None
            rep = self.store.representation(content_id, row["resolved_spec"])
            if rep is not None:
                self._republish_ready(rep, cause=event_id)
            return "ready_republished"
        # FAILED: reopen only on a new event with an untried locator, or after an
        # elapsed retryable failure (§6.10, P9-D8).
        untried = source.key not in (row["tried_sources"] or set()) and source.key not in sources
        elapsed = bool(row["last_retryable"]) and now >= self._next_after_failure(row)
        if untried or elapsed:
            if self._reopen(row, now, extra_source=source, event_id=event_id, first=first):
                return "reopened"
            return None
        if not self._add_event(row, event_id, first, now):
            return None
        if row["failed_envelope"]:
            self._republish(FAILED, row["failed_event_id"], "encode.failed", row["failed_envelope"])
        return "failed_republished"

    def _add_event(self, row: dict[str, Any], event_id: str, first: str, now: datetime) -> bool:
        return self.db.lwt(
            "UPDATE {ks}.encode_requests SET request_event_ids = request_event_ids + ?, "
            "first_event_id = ?, updated_at = ? WHERE content_id = ? AND spec_key = ? "
            "IF state = ? AND episode = ?",
            {event_id},
            first,
            now,
            row["content_id"],
            row["spec_key"],
            row["state"],
            row["episode"],
        ).applied

    def _next_after_failure(self, row: dict[str, Any]) -> datetime:
        failed_at = utc(row["last_failed_at"]) or self.store.now()
        return failed_at + self.backoff(max(int(row["failures"] or 1) - 1, 0))

    def backoff(self, k: int) -> timedelta:
        return timedelta(seconds=self.cfg.backoff_base_s * (3**k))

    def _reopen(
        self,
        row: dict[str, Any],
        now: datetime,
        *,
        extra_source: Source | None = None,
        event_id: str | None = None,
        first: str | None = None,
        actor: str | None = None,
    ) -> bool:
        """FAILED → PENDING as a new episode (failures reset, claim bound re-based)."""
        content_id, spec_key = row["content_id"], row["spec_key"]
        self.store.due_put(content_id, spec_key, now)
        res = self.db.lwt(
            "UPDATE {ks}.encode_requests SET state = 'PENDING', failures = 0, claims_base = ?, "
            "episode = ?, next_attempt_at = ?, bad_sources = {{}}, sources = sources + ?, "
            "request_event_ids = request_event_ids + ?, first_event_id = ?, updated_at = ? "
            "WHERE content_id = ? AND spec_key = ? IF state = 'FAILED' AND episode = ?",
            int(row["claims"] or 0),
            int(row["episode"] or 1) + 1,
            now,
            {extra_source.key: json.dumps(extra_source.doc)} if extra_source else {},
            {event_id} if event_id else set(),
            first if first is not None else row["first_event_id"],
            now,
            content_id,
            spec_key,
            row["episode"],
        )
        if not res.applied:
            self.store.due_del(content_id, spec_key, now)
            return False
        metrics.REQUESTS.labels(outcome="reopened").inc()
        if actor:
            self.store.audit(actor, "encode-retry", {"content_id": content_id, "spec_key": spec_key})
        return True

    def retry(self, content_id: str, spec_key: str, *, actor: str) -> bool:
        """Operator ``fingerprinter-encode retry`` (§6.11)."""
        row = self.get(content_id, spec_key)
        if row is None or row["state"] != FAILED_STATE:
            return False
        return self._reopen(row, self.store.now(), actor=actor)

    # --- republish ------------------------------------------------------------

    def ready_envelope(self, rep_row: dict[str, Any], *, event_id: EventId | None = None, cause: str | None = None) -> EventEnvelope[RepresentationReady]:
        spec = self.store.spec(rep_row["spec_digest"])
        representation = Representation(
            representation_id=rep_row["representation_id"],
            content_id=ContentId(rep_row["content_id"]),
            spec=spec.contract,
            created_at=utc(rep_row["created_at"]),
            segment_count=rep_row["segment_count"],
            media_duration_seconds=rep_row["duration_s"],
        )
        return new_event(
            RepresentationReady(representation=representation),
            producer=self.store.producer(),
            occurred_at=utc(rep_row["created_at"]),
            causation_id=EventId(cause) if cause else None,
            event_id=event_id,
        )

    def _republish_ready(self, rep_row: dict[str, Any], *, cause: str | None) -> None:
        env = self.ready_envelope(rep_row, cause=cause)
        self._republish(READY, str(env.event_id), "representation.ready", encode_event(env).decode())

    def _republish(self, stream: str, event_id: str, event_type: str, envelope: str) -> None:
        intent = self.store.intent_put(
            "republish",
            {"stream": stream},
            event_id=event_id,
            stream=stream,
            envelope=envelope,
            payload={"event_type": event_type},
        )
        self.publish_intent(intent)

    def publish_intent(self, intent: Intent) -> Outcome:
        """Publish a ``republish`` intent; it is deleted after XADD (no fact to ack)."""
        assert intent.stream and intent.envelope and intent.event_id
        out = self.pub.publish(
            intent.stream, intent.event_id, (intent.payload or {})["event_type"], intent.envelope
        )
        if out is Outcome.PUBLISHED:
            self.store.intent_del(intent)
        return out

    # --- claim, renew, release (§7.2) --------------------------------------

    def claim(self, content_id: str, spec_key: str, attempt_id: str) -> dict[str, Any] | None:
        row = self.get(content_id, spec_key)
        now = self.store.now()
        if row is None or row["state"] != PENDING:
            return None
        if utc(row["next_attempt_at"]) > now:
            return None
        if int(row["claims"] or 0) - int(row["claims_base"] or 0) >= self.cfg.max_claims:
            self._fail_pending(row, now)
            return None
        lease_until = now + timedelta(seconds=self.cfg.attempt_lease_s)
        self.store.due_put(content_id, spec_key, lease_until)
        claims = int(row["claims"] or 0) + 1
        res = self.db.lwt(
            "UPDATE {ks}.encode_requests SET state = 'IN_PROGRESS', attempt_id = ?, lease_until = ?, "
            "claims = ?, updated_at = ? WHERE content_id = ? AND spec_key = ? "
            "IF state = 'PENDING' AND claims = ? AND episode = ?",
            attempt_id,
            lease_until,
            claims,
            now,
            content_id,
            spec_key,
            row["claims"],
            row["episode"],
        )
        if not res.applied:
            self.store.due_del(content_id, spec_key, lease_until)
            return None
        self.store.due_del(content_id, spec_key, utc(row["next_attempt_at"]))
        metrics.CLAIMS.inc()
        row.update(state=IN_PROGRESS, attempt_id=attempt_id, lease_until=lease_until, claims=claims)
        return row

    def renew(self, row: dict[str, Any]) -> bool:
        """Durable attempt-lease renewal (§7.2). False = superseded: stop the attempt."""
        now = self.store.now()
        new = now + timedelta(seconds=self.cfg.attempt_lease_s)
        content_id, spec_key = row["content_id"], row["spec_key"]
        self.store.due_put(content_id, spec_key, new)
        res = self.db.lwt(
            "UPDATE {ks}.encode_requests SET lease_until = ? WHERE content_id = ? AND spec_key = ? "
            "IF state = 'IN_PROGRESS' AND attempt_id = ?",
            new,
            content_id,
            spec_key,
            row["attempt_id"],
        )
        if not res.applied:
            self.store.due_del(content_id, spec_key, new)
            return False
        self.store.due_del(content_id, spec_key, utc(row["lease_until"]))
        row["lease_until"] = new
        return True

    def release(self, row: dict[str, Any], *, reason: str) -> bool:
        """IN_PROGRESS → PENDING without an attributed failure (controlled release)."""
        now = self.store.now()
        content_id, spec_key = row["content_id"], row["spec_key"]
        self.store.due_put(content_id, spec_key, now)
        res = self.db.lwt(
            "UPDATE {ks}.encode_requests SET state = 'PENDING', attempt_id = null, lease_until = null, "
            "next_attempt_at = ?, last_detail = ?, updated_at = ? WHERE content_id = ? "
            "AND spec_key = ? IF state = 'IN_PROGRESS' AND attempt_id = ?",
            now,
            f"released:{reason}"[:200],
            now,
            content_id,
            spec_key,
            row["attempt_id"],
        )
        if not res.applied:
            self.store.due_del(content_id, spec_key, now)
            return False
        self.store.due_del(content_id, spec_key, utc(row["lease_until"]))
        metrics.RELEASES.labels(reason=reason).inc()
        return True

    def expire(self, row: dict[str, Any]) -> bool:
        """An abandoned attempt (lease expired) goes back to PENDING; not a failure."""
        now = self.store.now()
        content_id, spec_key = row["content_id"], row["spec_key"]
        self.store.due_put(content_id, spec_key, now)
        res = self.db.lwt(
            "UPDATE {ks}.encode_requests SET state = 'PENDING', attempt_id = null, lease_until = null, "
            "next_attempt_at = ?, last_detail = 'lease_expired', updated_at = ? WHERE content_id = ? "
            "AND spec_key = ? IF state = 'IN_PROGRESS' AND attempt_id = ? AND lease_until = ?",
            now,
            now,
            content_id,
            spec_key,
            row["attempt_id"],
            row["lease_until"],
        )
        if not res.applied:
            self.store.due_del(content_id, spec_key, now)
            return False
        self.store.due_del(content_id, spec_key, utc(row["lease_until"]))
        metrics.RELEASES.labels(reason="lease_expired").inc()
        return True

    # --- completion -------------------------------------------------------------

    def mark_ready(self, row: dict[str, Any], representation_id: str) -> bool:
        """Follow-up 4b: fenced by ``attempt_id`` when IN_PROGRESS; any non-terminal row
        whose representation exists converges to READY (reconciler, reuse)."""
        now = self.store.now()
        state = row["state"]
        if state == IN_PROGRESS:
            cond, args = "IF state = 'IN_PROGRESS' AND attempt_id = ?", [row["attempt_id"]]
        elif state == PENDING:
            cond, args = "IF state = 'PENDING' AND claims = ?", [row["claims"]]
        else:
            return state == READY_STATE
        res = self.db.lwt(
            "UPDATE {ks}.encode_requests SET state = 'READY', attempt_id = null, lease_until = null, "
            "representation_id = ?, updated_at = ? WHERE content_id = ? AND spec_key = ? " + cond,
            representation_id,
            now,
            row["content_id"],
            row["spec_key"],
            *args,
        )
        if res.applied:
            self.store.due_del(row["content_id"], row["spec_key"], self.due_time(row))
        return res.applied

    def fail(
        self,
        row: dict[str, Any],
        err: EncodeError,
        source_key: str | None = None,
        *,
        bad: dict[str, str] | None = None,
    ) -> str:
        """End an attempt with an attributed failure. Returns the new state, or
        ``superseded`` if the attempt no longer owns the request."""
        now = self.store.now()
        fresh = self.get(row["content_id"], row["spec_key"])
        if fresh is None or fresh["state"] != IN_PROGRESS or fresh["attempt_id"] != row["attempt_id"]:
            return "superseded"
        bad = {**dict(fresh["bad_sources"] or {}), **(bad or {})}
        if err.scope == "source" and not err.retryable and source_key:
            bad[source_key] = f"{err.failure.value}:{err.detail}"[:200]
        usable = [k for k in (fresh["sources"] or {}) if k not in bad]
        failures = int(fresh["failures"] or 0)
        terminal = False
        if err.scope == "source" and not err.retryable and usable:
            delay = timedelta(0)  # another source is still untried or usable
        elif not err.retryable:
            terminal = True
        else:
            failures += 1
            if failures >= self.cfg.max_attempts:
                terminal = True
            delay = self.backoff(failures - 1)
        if terminal:
            return self._to_failed(fresh, err, now, bad=bad, failures=failures, fenced=True)
        next_at = now + delay
        self.store.due_put(row["content_id"], row["spec_key"], next_at)
        res = self.db.lwt(
            "UPDATE {ks}.encode_requests SET state = 'PENDING', attempt_id = null, lease_until = null, "
            "next_attempt_at = ?, failures = ?, bad_sources = ?, last_failure = ?, "
            "last_retryable = ?, last_detail = ?, last_failed_at = ?, updated_at = ? "
            "WHERE content_id = ? AND spec_key = ? IF state = 'IN_PROGRESS' AND attempt_id = ?",
            next_at,
            failures,
            bad,
            err.failure.value,
            err.retryable,
            err.detail,
            now,
            now,
            row["content_id"],
            row["spec_key"],
            row["attempt_id"],
        )
        if not res.applied:
            self.store.due_del(row["content_id"], row["spec_key"], next_at)
            return "superseded"
        self.store.due_del(row["content_id"], row["spec_key"], utc(fresh["lease_until"]))
        metrics.ATTEMPT_FAILURES.labels(failure=err.failure.value).inc()
        return PENDING

    def _fail_pending(self, row: dict[str, Any], now: datetime) -> str:
        err = EncodeError(
            EncodeFailure.INTERNAL_ERROR, "claim_budget_exhausted", retryable=True, scope="attempt"
        )
        return self._to_failed(row, err, now, bad=dict(row["bad_sources"] or {}),
                               failures=int(row["failures"] or 0), fenced=False)

    def _to_failed(
        self,
        row: dict[str, Any],
        err: EncodeError,
        now: datetime,
        *,
        bad: dict[str, str],
        failures: int,
        fenced: bool,
    ) -> str:
        content_id, spec_key = row["content_id"], row["spec_key"]
        attempt = max(int(row["claims"] or 0), 1)
        envelope = event_id = None
        intent: Intent | None = None
        if row["first_event_id"]:
            required = (
                RepresentationSpec.model_validate_json(row["required_spec"])
                if row["required_spec"]
                else None
            )
            env = new_event(
                EncodeFailed(
                    request_id=EventId(row["first_event_id"]),
                    content_id=ContentId(content_id),
                    failure=err.failure,
                    retryable=err.retryable,
                    attempt=attempt,
                    spec=required,
                    detail=err.detail or None,
                ),
                producer=self.store.producer(),
                occurred_at=now,
                causation_id=EventId(row["first_event_id"]),
            )
            envelope, event_id = encode_event(env).decode(), str(env.event_id)
            intent = self.store.intent_put(
                "request_failed",
                {"content_id": content_id, "spec_key": spec_key, "episode": row["episode"]},
                event_id=event_id,
                stream=FAILED,
                envelope=envelope,
            )
        if fenced:
            cond, args = "IF state = 'IN_PROGRESS' AND attempt_id = ?", [row["attempt_id"]]
        else:
            cond, args = "IF state = 'PENDING' AND claims = ? AND episode = ?", [
                row["claims"],
                row["episode"],
            ]
        res = self.db.lwt(
            "UPDATE {ks}.encode_requests SET state = 'FAILED', attempt_id = null, lease_until = null, "
            "failures = ?, bad_sources = ?, last_failure = ?, last_retryable = ?, last_detail = ?, "
            "last_failed_at = ?, failed_envelope = ?, failed_event_id = ?, "
            "failed_event_published_at = null, tried_sources = tried_sources + ?, updated_at = ? "
            "WHERE content_id = ? AND spec_key = ? " + cond,
            failures,
            bad,
            err.failure.value,
            err.retryable,
            err.detail,
            now,
            envelope,
            event_id,
            set((row["sources"] or {}).keys()),
            now,
            content_id,
            spec_key,
            *args,
        )
        if not res.applied:
            if intent is not None:
                self.store.intent_del(intent)
            return "superseded"
        self.store.due_del(content_id, spec_key, self.due_time(row))
        metrics.REQUESTS.labels(outcome="failed").inc()
        metrics.ATTEMPT_FAILURES.labels(failure=err.failure.value).inc()
        failed = dict(row, state=FAILED_STATE, last_failure=err.failure.value,
                      last_detail=err.detail, last_retryable=err.retryable)
        for hook in self.on_failed:
            hook(failed)
        if intent is not None:
            self.publish_failed(intent)
        return FAILED_STATE

    def publish_failed(self, intent: Intent) -> Outcome:
        out = self.pub.publish(FAILED, str(intent.event_id), "encode.failed", str(intent.envelope))
        if out is Outcome.PUBLISHED:
            key = intent.fact_key
            self.db.lwt(
                "UPDATE {ks}.encode_requests SET failed_event_published_at = ? WHERE content_id = ? "
                "AND spec_key = ? IF failed_event_id = ?",
                self.store.now(),
                key["content_id"],
                key["spec_key"],
                intent.event_id,
            )
            self.store.intent_del(intent)
        return out

    # --- scheduler (§7.3) ---------------------------------------------------

    def schedule(self, *, limit: int = 1000) -> dict[str, int]:
        """Scan due partitions ≤ now: enqueue due PENDING rows, expire dead
        IN_PROGRESS leases, delete stale entries. Redis loss is repaired here."""
        now = self.store.now()
        today = now.strftime("%Y-%m-%d")
        counts = {"enqueued": 0, "expired": 0, "stale": 0}
        for day in self.store.due_days():
            if day > today:
                break
            entries = self.store.due_scan(day, now, limit)
            for e in entries:
                cid, key, due_at = e["content_id"], e["spec_key"], utc(e["due_at"])
                row = self.get(cid, key)
                if row is None or self.due_time(row) != due_at:
                    # Not the row's due time. A writer writes the new entry *before* its
                    # transition, so a fresh entry may belong to a transition in flight:
                    # only entries older than the grace are stale and removable.
                    if now - due_at > STALE_DUE_GRACE:
                        self.store.due_del(cid, key, due_at)
                        counts["stale"] += 1
                    continue
                if row["state"] == IN_PROGRESS:
                    if self.expire(row):
                        counts["expired"] += 1
                        row = self.get(cid, key)
                    else:
                        continue
                if row is not None and row["state"] == PENDING:
                    score = (100 - int(row["priority"] or 0)) * 1e13 + due_at.timestamp() * 1000
                    self.store.coord.enqueue(request_member(cid, key), score)
                    counts["enqueued"] += 1
            if not entries:
                self.store.drop_due_day_if_empty(day, today)
        return counts
