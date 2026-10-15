"""Target manager (design §10, P9-D14) on the same representation infrastructure.

Reference upload, content-addressed (the final key is known before it is written):

1. hash the local file once (SHA-256 ``D``, size ``n``); ``n > 8 GiB`` is refused;
2. intent ``target_register`` before any object is written;
3. reuse ``ref/<D>`` if it exists with size ``n`` and metadata ``sha256 = D``;
4. otherwise upload to ``staging/<upload_id>`` re-hashing the bytes sent; a
   different hash (file changed between passes) deletes the staging object and
   refuses the registration;
5. promote by server-side copy to ``ref/<D>`` (multipart copy above the 5 GiB S3
   single-copy limit) with ``sha256 = D``, verify by ``StatObject``, delete staging;
6. facts: ``targets_by_content`` → ``targets`` (LWT) → binding PENDING → request
   upsert (``internal_object`` source, priority 90) → SERIAL read of the
   representation: if it exists (reuse) the manager binds READY itself, otherwise
   commit follow-up 4c binds it; then the intent is deleted.

``target.registered``/``target.retired`` are durable facts on the target row,
published through intents and **held** until a consumer registers (§9.1).
Purge deletes ``ref/<D>`` only when no other target version references it.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from antipiracy_contracts.digests import ContentDigest
from antipiracy_contracts.events import encode_event, new_event
from antipiracy_contracts.events.targets import TargetRegistered, TargetRetired
from antipiracy_contracts.ids import ContentId, TargetId
from antipiracy_contracts.models.targets import Target, TargetKind, TargetRef
from antipiracy_contracts.ownership import Component

from fingerprinter.coord import utc, uuid7
from fingerprinter.db import SERIAL
from fingerprinter.errors import FingerprinterError
from fingerprinter.objects import META_SHA, ref_key
from fingerprinter.publish import TARGET_REGISTERED, TARGET_RETIRED, Outcome
from fingerprinter.requests import DEFAULT, internal_source
from fingerprinter.store import Intent

SCHEME = "full-bytes/v1"
TARGET_PRIORITY = 90
REGISTERING, READY, BUILD_FAILED, RETIRED, PURGED = (
    "REGISTERING", "READY", "BUILD_FAILED", "RETIRED", "PURGED",
)


class TargetError(FingerprinterError):
    pass


def content_id_of_sha(sha_hex: str) -> ContentId:
    return ContentId.of(SCHEME, ContentDigest("sha256:" + sha_hex))


def _hash_file(path: Path) -> tuple[str, int]:
    sha, n = hashlib.sha256(), 0
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            sha.update(chunk)
            n += len(chunk)
    return sha.hexdigest(), n


def _noop(point: str, **ctx: Any) -> None:
    return None


@dataclass
class Registration:
    target_id: str
    version: int
    content_id: str
    sha256: str
    size: int
    reused_object: bool
    reused_representation: bool
    idempotent: bool
    state: str


class TargetManager:
    def __init__(self, services: Any, *, hook: Callable[..., None] = _noop) -> None:
        self.s = services
        self.store = services.store
        self.db = services.db
        self.objects = services.objects
        self.bucket = services.objects.cfg.targets_bucket
        self.hook = hook

    def install(self) -> TargetManager:
        self.s.committer.bind_targets = self.bind_for_representation
        self.s.reconciler.target_hooks["target_event"] = self._resolve_event
        self.s.reconciler.target_hooks["target_register"] = self._resolve_register
        self.s.reconciler.full_pass_hooks.append(self._full_pass)
        self.s.requests.on_failed.append(self.on_request_failed)
        return self

    # --- registration -------------------------------------------------------

    def register(
        self,
        path: Path,
        *,
        title: str,
        kind: str,
        aliases: tuple[str, ...] = (),
        release_year: int | None = None,
        target_id: str | None = None,
        actor: str,
    ) -> Registration:
        TargetKind(kind)  # validate early
        sha, size = _hash_file(path)
        if size > self.s.settings.acquire.max_bytes:
            raise TargetError(f"reference media is {size} bytes; limit {self.s.settings.acquire.max_bytes}")
        upload_id = uuid7()
        staging = f"staging/{upload_id}"
        intent = self.store.intent_put(
            "target_register",
            {"upload_id": upload_id},
            payload={"sha256": sha, "size": size, "target_id": target_id, "staging_key": staging},
        )
        self.hook("after_register_intent")
        ref = ref_key(sha)
        info = self.objects.stat(self.bucket, ref)
        reused = info is not None and info.size == size and info.metadata.get(META_SHA) == sha
        if not reused:
            sent = self.objects.put_file_hashing(self.bucket, staging, path, size)
            self.hook("after_staging")
            if sent != sha:
                self.objects.delete(self.bucket, staging)
                self.store.intent_del(intent)
                raise TargetError("reference media changed between hashing and upload; refused")
            self.objects.copy_with_sha(self.bucket, staging, ref, size, sha)
            info = self.objects.stat(self.bucket, ref)
            if info is None or info.size != size or info.metadata.get(META_SHA) != sha:
                raise TargetError("promoted reference object failed verification")
            self.hook("after_promote")
            self.objects.delete(self.bucket, staging)
        reg = self._facts(sha, size, title=title, kind=kind, aliases=aliases,
                          release_year=release_year, target_id=target_id, actor=actor)
        reg.reused_object = reused
        self.store.intent_del(intent)
        return reg

    def _facts(
        self,
        sha: str,
        size: int,
        *,
        title: str,
        kind: str,
        aliases: tuple[str, ...],
        release_year: int | None,
        target_id: str | None,
        actor: str,
    ) -> Registration:
        cid = str(content_id_of_sha(sha))
        tid = target_id or str(TargetId.new())
        spec_digest = self.store.default_spec_digest()
        for _ in range(10):
            versions = self.versions(tid)
            for v in versions:
                if v["content_id"] == cid and v["state"] not in (RETIRED, PURGED):
                    return Registration(tid, v["version"], cid, sha, size, False, False, True, v["state"])
            version = max((v["version"] for v in versions), default=0) + 1
            now = self.store.now()
            self.db.run(
                "INSERT INTO {ks}.targets_by_content (content_id, target_id, version, state) "
                "VALUES (?,?,?,?)",
                cid, tid, version, REGISTERING,
            )
            res = self.db.lwt(
                "INSERT INTO {ks}.targets (target_id, version, title, kind, aliases, release_year, "
                "content_id, ref_sha256, ref_size, ref_key, state, spec_digest, registered_at, audit) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) IF NOT EXISTS",
                tid, version, title, kind, list(aliases), release_year, cid, sha, size,
                ref_key(sha), REGISTERING, spec_digest, now,
                [self.store.audit_entry(now, actor, "register", content_id=cid)],
            )
            if not res.applied:
                if res.row.get("content_id") != cid:
                    self.db.run(
                        "DELETE FROM {ks}.targets_by_content WHERE content_id = ? AND target_id = ? "
                        "AND version = ?",
                        cid, tid, version,
                    )
                continue
            self.hook("after_target_row")
            self.db.run(
                "INSERT INTO {ks}.target_representations (target_id, version, spec_digest, state, "
                "content_id) VALUES (?,?,?,'PENDING',?)",
                tid, version, spec_digest, cid,
            )
            # The content's 'default' request may already resolve to an older default
            # spec; then the target's spec is requested explicitly so its binding can
            # complete without waiting for a backfill.
            existing = self.s.requests.get(cid, DEFAULT, serial=False)
            key = DEFAULT if existing is None or existing["resolved_spec"] == spec_digest else spec_digest
            self.s.requests.upsert_internal(
                cid,
                key,
                digest="sha256:" + sha,
                source=internal_source(self.bucket, ref_key(sha), size, sha, f"{tid}/{version}"),
                priority=TARGET_PRIORITY,
                origin="target",
            )
            rep = self.store.representation(cid, spec_digest, serial=True)
            reused = rep is not None and rep["state"] == "READY"
            if reused:
                self.bind_ready(tid, version, spec_digest, rep)
            row = self.target(tid, version)
            return Registration(tid, version, cid, sha, size, False, reused, False,
                                row["state"] if row else REGISTERING)
        raise TargetError("target version allocation did not converge")

    # --- reads --------------------------------------------------------------

    def versions(self, tid: str) -> list[dict[str, Any]]:
        return self.db.rows("SELECT * FROM {ks}.targets WHERE target_id = ?", tid, level=SERIAL)

    def target(self, tid: str, version: int) -> dict[str, Any] | None:
        return self.db.one(
            "SELECT * FROM {ks}.targets WHERE target_id = ? AND version = ?", tid, version, level=SERIAL
        )

    def bindings(self, tid: str) -> list[dict[str, Any]]:
        return self.db.rows("SELECT * FROM {ks}.target_representations WHERE target_id = ?", tid)

    def list(self) -> list[dict[str, Any]]:
        return self.db.rows("SELECT * FROM {ks}.targets")

    def show(self, tid: str) -> dict[str, Any]:
        out = []
        for v in self.versions(tid):
            out.append({
                **{k: (str(x) if not isinstance(x, (int, str, list, type(None))) else x)
                   for k, x in v.items() if not k.endswith("_envelope") and k != "audit"},
                "bindings": [b for b in self.bindings(tid) if b["version"] == v["version"]],
                "event_pending": {
                    "registered": bool(v["registered_envelope"]) and v["registered_event_published_at"] is None,
                    "retired": bool(v["retired_envelope"]) and v["retired_event_published_at"] is None,
                },
            })
        return {"target_id": tid, "versions": out}

    # --- binding (design §10) -------------------------------------------------

    def bind_for_representation(self, rep: dict[str, Any]) -> None:
        """Commit follow-up 4c: bind every PENDING binding of this content and spec."""
        for row in self.db.rows(
            "SELECT * FROM {ks}.targets_by_content WHERE content_id = ?", rep["content_id"]
        ):
            if row["state"] in (RETIRED, PURGED):
                continue
            self.bind_ready(row["target_id"], row["version"], rep["spec_digest"], rep)

    def bind_ready(self, tid: str, version: int, spec_digest: str, rep: dict[str, Any]) -> bool:
        spec = self.store.spec(spec_digest)
        res = self.db.lwt(
            "UPDATE {ks}.target_representations SET state = 'READY', representation_id = ?, "
            "target_representation_id = ?, bound_at = ? WHERE target_id = ? AND version = ? "
            "AND spec_digest = ? IF state = 'PENDING'",
            rep["representation_id"],
            str(spec.target_representation_id(TargetId(tid), version)),
            self.store.now(),
            tid, version, spec_digest,
        )
        target = self.target(tid, version)
        if target is None or target["spec_digest"] != spec_digest:
            return res.applied
        if target["state"] in (REGISTERING, BUILD_FAILED):
            self._ready(target)
        return res.applied

    def _ready(self, target: dict[str, Any]) -> None:
        now = self.store.now()
        model = Target(
            ref=TargetRef(target_id=TargetId(target["target_id"]), version=target["version"]),
            title=target["title"],
            kind=TargetKind(target["kind"]),
            aliases=tuple(target["aliases"] or ()),
            release_year=target["release_year"],
            registered_at=utc(target["registered_at"]),
        )
        env = new_event(TargetRegistered(target=model),
                        producer=self.store.producer(Component.TARGET_MANAGER), occurred_at=now)
        envelope = encode_event(env).decode()
        intent = self.store.intent_put(
            "target_event",
            {"target_id": target["target_id"], "version": target["version"], "event": "registered"},
            event_id=str(env.event_id),
            stream=TARGET_REGISTERED,
        )
        res = self.db.lwt(
            "UPDATE {ks}.targets SET state = 'READY', registered_envelope = ?, registered_event_id = ?, "
            "failure = null WHERE target_id = ? AND version = ? IF state = ?",
            envelope, str(env.event_id), target["target_id"], target["version"], target["state"],
        )
        if not res.applied:
            self.store.intent_del(intent)
            return
        self._by_content_state(target, READY)
        self.publish_event(intent)

    def _by_content_state(self, target: dict[str, Any], state: str) -> None:
        self.db.run(
            "UPDATE {ks}.targets_by_content SET state = ? WHERE content_id = ? AND target_id = ? "
            "AND version = ?",
            state, target["content_id"], target["target_id"], target["version"],
        )

    def on_request_failed(self, req: dict[str, Any]) -> None:
        """A failed build is visible on the target (BUILD_FAILED); no encode.failed exists
        for a target-only request."""
        for row in self.db.rows(
            "SELECT * FROM {ks}.targets_by_content WHERE content_id = ?", req["content_id"]
        ):
            target = self.target(row["target_id"], row["version"])
            if target is None or target["state"] != REGISTERING:
                continue
            if target["spec_digest"] != req["resolved_spec"]:
                continue
            res = self.db.lwt(
                "UPDATE {ks}.targets SET state = 'BUILD_FAILED', failure = ? WHERE target_id = ? "
                "AND version = ? IF state = 'REGISTERING'",
                f"{req.get('last_failure')}:{req.get('last_detail')}"[:200],
                target["target_id"], target["version"],
            )
            if res.applied:
                self._by_content_state(target, BUILD_FAILED)

    # --- events -----------------------------------------------------------------

    def publish_event(self, intent: Intent) -> Outcome:
        key = intent.fact_key
        target = self.target(key["target_id"], key["version"])
        if target is None:
            self.store.intent_del(intent)
            return Outcome.PUBLISHED
        event = key["event"]
        envelope = target[f"{event}_envelope"]
        if target[f"{event}_event_published_at"] is not None or envelope is None:
            self.store.intent_del(intent)
            return Outcome.PUBLISHED
        stream = TARGET_REGISTERED if event == "registered" else TARGET_RETIRED
        out = self.s.pub.publish(stream, target[f"{event}_event_id"], f"target.{event}", envelope)
        if out is Outcome.PUBLISHED:
            self.db.lwt(
                f"UPDATE {{ks}}.targets SET {event}_event_published_at = ? WHERE target_id = ? "  # noqa: S608
                f"AND version = ? IF {event}_event_id = ?",
                self.store.now(), key["target_id"], key["version"], target[f"{event}_event_id"],
            )
            self.store.intent_del(intent)
        return out

    def _resolve_event(self, intent: Intent, report: Any) -> None:
        out = self.publish_event(intent)
        if out is Outcome.PUBLISHED:
            report.completed += 1
        else:
            report.held += 1

    def _resolve_register(self, intent: Intent, report: Any) -> None:
        """Crash recovery: delete a leftover staging object, drop the intent; an
        unreferenced ``ref/<D>`` is left to the sweeper's grace rule."""
        staging = (intent.payload or {}).get("staging_key")
        if staging:
            self.objects.delete(self.bucket, staging)
        self.store.intent_del(intent)
        report.cleaned += 1
        report.repair("target_register_resolved")

    def republish(self, stream: str = TARGET_REGISTERED) -> int:
        """Re-emit the stored envelopes (same event_ids) of current target versions."""
        event = "registered" if stream == TARGET_REGISTERED else "retired"
        count = 0
        for t in self.list():
            env = t[f"{event}_envelope"]
            if not env:
                continue
            if self.s.pub.publish(stream, t[f"{event}_event_id"], f"target.{event}", env) is Outcome.PUBLISHED:
                count += 1
        return count

    def _full_pass(self, report: Any) -> None:
        open_events = {
            (i.fact_key.get("target_id"), i.fact_key.get("version"), i.fact_key.get("event"))
            for i in self.store.intents() if i.kind == "target_event"
        }
        for b in self.db.stream("SELECT * FROM {ks}.target_representations"):
            if b["state"] != "PENDING":
                continue
            rep = self.store.representation(b["content_id"], b["spec_digest"])
            if rep is not None and rep["state"] == "READY":
                self.bind_ready(b["target_id"], b["version"], b["spec_digest"], rep)
                report.repair("target_binding")
        for t in self.list():
            for event in ("registered", "retired"):
                if t[f"{event}_envelope"] and t[f"{event}_event_published_at"] is None and (
                    t["target_id"], t["version"], event
                ) not in open_events:
                    self.store.intent_put(
                        "target_event",
                        {"target_id": t["target_id"], "version": t["version"], "event": event},
                        event_id=t[f"{event}_event_id"],
                        stream=TARGET_REGISTERED if event == "registered" else TARGET_RETIRED,
                    )
                    report.repair("target_event_intent")

    # --- retire / purge -----------------------------------------------------

    def retire(self, tid: str, *, reason: str, actor: str) -> int:
        versions = [v for v in self.versions(tid) if v["state"] not in (RETIRED, PURGED)]
        if not versions:
            raise TargetError(f"target {tid} has no active version")
        now = self.store.now()
        latest = max(versions, key=lambda v: v["version"])
        env = new_event(
            TargetRetired(target_id=TargetId(tid), retired_at=now, reason=reason[:200] or None),
            producer=self.store.producer(Component.TARGET_MANAGER),
            occurred_at=now,
        )
        intent = self.store.intent_put(
            "target_event",
            {"target_id": tid, "version": latest["version"], "event": "retired"},
            event_id=str(env.event_id),
            stream=TARGET_RETIRED,
        )
        for v in versions:
            is_latest = v["version"] == latest["version"]
            self.db.lwt(
                "UPDATE {ks}.targets SET state = 'RETIRED', retired_at = ?, retire_reason = ?, "
                "retired_envelope = ?, retired_event_id = ?, audit = audit + ? WHERE target_id = ? "
                "AND version = ? IF state = ?",
                now, reason, encode_event(env).decode() if is_latest else None,
                str(env.event_id) if is_latest else None,
                [self.store.audit_entry(now, actor, "retire", reason=reason)],
                tid, v["version"], v["state"],
            )
            self._by_content_state(v, RETIRED)
        self.publish_event(intent)
        return len(versions)

    def purge(self, tid: str, *, actor: str) -> list[str]:
        """Delete reference media of a retired target; ``ref/<D>`` only when no other
        (non-purged) target version references it. Representations are kept."""
        versions = self.versions(tid)
        if any(v["state"] != RETIRED and v["state"] != PURGED for v in versions):
            raise TargetError("purge needs a retired target")
        deleted = []
        for v in versions:
            self.db.lwt(
                "UPDATE {ks}.targets SET state = 'PURGED', audit = audit + ? WHERE target_id = ? "
                "AND version = ? IF state = 'RETIRED'",
                [self.store.audit_entry(self.store.now(), actor, "purge")], tid, v["version"],
            )
            self._by_content_state(v, PURGED)
        for sha in {v["ref_sha256"] for v in versions}:
            cid = str(content_id_of_sha(sha))
            others = [
                r for r in self.db.rows(
                    "SELECT * FROM {ks}.targets_by_content WHERE content_id = ?", cid
                ) if r["state"] != PURGED
            ]
            if not others:
                self.objects.delete(self.bucket, ref_key(sha))
                deleted.append(sha)
        self.store.audit(actor, "target-purge", {"target_id": tid, "deleted_refs": deleted})
        return deleted


def registration_json(reg: Registration) -> str:
    return json.dumps(reg.__dict__, sort_keys=True)
