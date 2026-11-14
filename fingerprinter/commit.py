"""Representation commit protocol (design §8.2–§8.3, P9-D13/D20).

1. Upload the artifact to the attempt-unique key and verify it by read-back.
2. Write intent ``rep_commit`` (no TTL) **before** the fact.
3. ``INSERT INTO representations … IF NOT EXISTS`` — **the authoritative commit
   point**. A representation exists iff this row exists. An unknown LWT outcome
   is resolved by a SERIAL read before anything else.
4. Winner: follow-ups, each idempotent and flagged on the row by LWT
   (index → request READY → target bindings → publication + ack); then the
   intent is deleted. Held publication leaves the intent for the reconciler.
5. Loser: deletes its own object (never the referenced one), converges its
   request to READY, re-publishes ``representation.ready``, deletes its intent.

No code path updates the artifact columns of an existing row.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from antipiracy_contracts.events import encode_event
from antipiracy_contracts.ids import EventId

from fingerprinter import metrics
from fingerprinter.coord import utc
from fingerprinter.db import LwtUnknownError
from fingerprinter.objects import Objects, parse_uri, rep_key, uri
from fingerprinter.publish import READY, Outcome
from fingerprinter.requests import Requests
from fingerprinter.spec import Spec
from fingerprinter.store import Intent, Store


def _no_hook(point: str, **ctx: Any) -> None:
    return None


@dataclass
class Artifact:
    data: bytes
    sha256: str
    segment_count: int
    duration_s: float | None
    runtime_fingerprint: str
    device: str
    compute_dtype: str

    @property
    def size(self) -> int:
        return len(self.data)


class Committer:
    def __init__(
        self,
        store: Store,
        requests: Requests,
        objects: Objects,
        *,
        hook: Callable[..., None] = _no_hook,
    ) -> None:
        self.store = store
        self.db = store.db
        self.requests = requests
        self.pub = requests.pub
        self.objects = objects
        self.bucket = objects.cfg.representations_bucket
        self.hook = hook
        self.bind_targets: Callable[[dict[str, Any]], None] = lambda rep: None
        """Set by the target manager: follow-up 4c (design §10)."""

    # --- commit -------------------------------------------------------------

    def commit(self, request: dict[str, Any], spec: Spec, art: Artifact) -> str:
        content_id = request["content_id"]
        attempt_id = request["attempt_id"]
        digest = str(spec.digest)
        key = rep_key(spec.hex, content_id, attempt_id)
        self.objects.put_bytes(self.bucket, key, art.data, art.sha256)
        self.hook("after_upload", key=key)
        now = self.store.now()
        rep_row = {
            "content_id": content_id,
            "spec_digest": digest,
            "representation_id": str(spec.representation_id(content_id)),
            "created_at": now,
            "segment_count": art.segment_count,
            "duration_s": art.duration_s,
        }
        env = self.requests.ready_envelope(rep_row, cause=request.get("first_event_id"))
        envelope = encode_event(env).decode()
        artifact_uri = uri(self.bucket, key)
        intent = self.store.intent_put(
            "rep_commit",
            {"content_id": content_id, "spec_digest": digest},
            attempt_id=attempt_id,
            event_id=str(env.event_id),
            artifact_uri=artifact_uri,
        )
        self.hook("after_intent", intent=intent)
        applied = self._insert(rep_row, request, attempt_id, artifact_uri, art, envelope, env.event_id)
        self.hook("after_row", applied=applied)
        row = self.store.representation(content_id, digest, serial=True)
        assert row is not None
        if row["artifact_attempt_id"] == attempt_id:
            metrics.COMMITTED.inc()
            self.complete(row, intent)
            return "won"
        self.lose(row, request, intent)
        return "lost"

    def _insert(
        self,
        rep: dict[str, Any],
        request: dict[str, Any],
        attempt_id: str,
        artifact_uri: str,
        art: Artifact,
        envelope: str,
        event_id: EventId,
    ) -> bool:
        args = (
            rep["content_id"],
            rep["spec_digest"],
            rep["representation_id"],
            attempt_id,
            artifact_uri,
            art.sha256,
            art.size,
            art.segment_count,
            art.duration_s,
            art.runtime_fingerprint,
            art.device,
            art.compute_dtype,
            request["spec_key"],
            envelope,
            str(event_id),
            rep["created_at"],
        )
        for _ in range(5):
            try:
                return self.db.lwt(
                    "INSERT INTO {ks}.representations (content_id, spec_digest, representation_id, "
                    "state, artifact_attempt_id, artifact_uri, artifact_sha256, artifact_size, "
                    "segment_count, duration_s, runtime_fingerprint, device, compute_dtype, "
                    "request_spec_key, ready_envelope, ready_event_id, f_index, f_request, f_targets, "
                    "created_at) VALUES (?,?,?,'READY',?,?,?,?,?,?,?,?,?,?,?,?,false,false,false,?) "
                    "IF NOT EXISTS",
                    *args,
                ).applied
            except LwtUnknownError:
                # Unknown outcome: the SERIAL read decides (design §8.2 step 3).
                row = self.store.representation(rep["content_id"], rep["spec_digest"], serial=True)
                if row is not None:
                    return bool(row["artifact_attempt_id"] == attempt_id)
        raise LwtUnknownError("representation insert outcome unresolved")

    # --- follow-ups (worker or reconciler) ----------------------------------

    def _flag(self, row: dict[str, Any], column: str) -> None:
        self.db.lwt(
            f"UPDATE {{ks}}.representations SET {column} = true WHERE content_id = ? "  # noqa: S608
            "AND spec_digest = ? IF EXISTS",
            row["content_id"],
            row["spec_digest"],
        )
        row[column] = True

    def complete(self, row: dict[str, Any], intent: Intent | None) -> bool:
        """Run every missing follow-up; delete the intent when all are done and the
        event is acknowledged. False if publication is held/back-pressured."""
        if not row["f_index"]:
            self.db.run(
                "INSERT INTO {ks}.representations_by_spec (spec_digest, bucket, content_id, "
                "representation_id) VALUES (?,?,?,?)",
                row["spec_digest"],
                self.store.bucket_of(row["content_id"]),
                row["content_id"],
                row["representation_id"],
            )
            self.hook("after_index")
            self._flag(row, "f_index")
        if not row["f_request"]:
            self._converge_requests(row)
            self._flag(row, "f_request")
        if not row["f_targets"]:
            self.bind_targets(row)
            self._flag(row, "f_targets")
        if row["ready_event_published_at"] is None:
            out = self.pub.publish(
                READY, row["ready_event_id"], "representation.ready", row["ready_envelope"]
            )
            metrics.EVENTS.labels(stream=READY, outcome=out.value).inc()
            if out is not Outcome.PUBLISHED:
                return False
            self.hook("after_xadd")
            self.db.lwt(
                "UPDATE {ks}.representations SET ready_event_published_at = ? WHERE content_id = ? "
                "AND spec_digest = ? IF ready_event_id = ?",
                self.store.now(),
                row["content_id"],
                row["spec_digest"],
                row["ready_event_id"],
            )
            row["ready_event_published_at"] = self.store.now()
        if intent is not None:
            self.hook("before_intent_delete")
            self.store.intent_del(intent)
        return True

    def _converge_requests(self, rep: dict[str, Any]) -> None:
        """Every non-terminal request of this content resolving to this spec → READY."""
        for req in self.db.rows(
            "SELECT * FROM {ks}.encode_requests WHERE content_id = ?", rep["content_id"]
        ):
            if req["resolved_spec"] != rep["spec_digest"]:
                continue
            fresh = self.requests.get(req["content_id"], req["spec_key"])
            if fresh is not None and fresh["state"] in ("PENDING", "IN_PROGRESS"):
                self.requests.mark_ready(fresh, rep["representation_id"])

    def lose(self, row: dict[str, Any], request: dict[str, Any], intent: Intent) -> None:
        attempt_id = request["attempt_id"]
        if row["artifact_attempt_id"] != attempt_id and intent.artifact_uri:
            bucket, key = parse_uri(intent.artifact_uri)
            self.objects.delete(bucket, key)
        fresh = self.requests.get(request["content_id"], request["spec_key"])
        if fresh is not None and fresh["state"] in ("PENDING", "IN_PROGRESS"):
            self.requests.mark_ready(fresh, row["representation_id"])
        env = self.requests.ready_envelope(row, cause=request.get("first_event_id"))
        self.requests._republish(  # noqa: SLF001 - same package
            READY, str(env.event_id), "representation.ready", encode_event(env).decode()
        )
        self.store.intent_del(intent)


def manifest(**fields: Any) -> bytes:
    return json.dumps(fields, sort_keys=True, default=str).encode()


__all__ = ["Artifact", "Committer", "manifest", "utc"]
