"""Shared Scylla state: specs, roles, runtime profiles, due index, intents, audit.

Write-order rules implemented here (design §7.3, §8.1):

- a due entry (and its ``encode_due_days`` row) is written **before** the
  transition that sets that due time and deleted only **after** it succeeded;
- an intent (and its ``intent_days`` row) is written **before** the fact's LWT
  and deleted only after its work is complete and acknowledged on the fact.
"""

from __future__ import annotations

import json
import os
import socket
import zlib
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from antipiracy_contracts.ownership import Component, Producer, ServiceName

from fingerprinter.config import Settings
from fingerprinter.coord import Clock, Coord, utc, uuid7
from fingerprinter.db import SERIAL, Db
from fingerprinter.runtime import RuntimeFingerprint, RuntimeProfile
from fingerprinter.spec import Spec

SPEC_STATUSES = ("gated", "active", "deprecated", "retired")


def day_of(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%d")


def shard_of(key: str, shards: int) -> int:
    return zlib.crc32(key.encode()) % shards


def request_member(content_id: str, spec_key: str) -> str:
    return f"{content_id}|{spec_key}"


def parse_member(member: str) -> tuple[str, str]:
    content_id, _, spec_key = member.partition("|")
    return content_id, spec_key


@dataclass
class Intent:
    day: str
    shard: int
    intent_id: str
    kind: str
    fact_key: dict[str, Any]
    attempt_id: str | None = None
    event_id: str | None = None
    artifact_uri: str | None = None
    stream: str | None = None
    envelope: str | None = None
    payload: dict[str, Any] | None = None
    created_at: datetime | None = None
    holder: str | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Intent:
        return cls(
            day=row["day"],
            shard=row["shard"],
            intent_id=row["intent_id"],
            kind=row["kind"],
            fact_key=json.loads(row["fact_key"] or "{}"),
            attempt_id=row.get("attempt_id"),
            event_id=row.get("event_id"),
            artifact_uri=row.get("artifact_uri"),
            stream=row.get("stream"),
            envelope=row.get("envelope"),
            payload=json.loads(row["payload"]) if row.get("payload") else None,
            created_at=utc(row.get("created_at")),
            holder=row.get("holder"),
        )


class Store:
    def __init__(self, db: Db, coord: Coord, settings: Settings, *, role: str = "encoder") -> None:
        self.db = db
        self.coord = coord
        self.settings = settings
        self.clock: Clock = coord.clock
        self.instance = f"{settings.host_id}:{role}:{os.getpid()}:{socket.gethostname()}"[:200]
        self._specs: dict[str, Spec] = {}

    def now(self) -> datetime:
        return self.clock.now()

    def producer(self, component: Component = Component.ENCODER) -> Producer:
        return Producer(
            service=ServiceName.FINGERPRINTER, component=component, instance=self.instance
        )

    # --- audit --------------------------------------------------------------

    def audit(self, actor: str, action: str, detail: dict[str, Any]) -> None:
        now = self.now()
        self.db.run(
            "INSERT INTO {ks}.audit_log (day, at, actor, action, detail) VALUES (?, now(), ?, ?, ?)",
            day_of(now),
            actor,
            action,
            json.dumps(detail, sort_keys=True, default=str),
        )

    @staticmethod
    def audit_entry(now: datetime, actor: str, action: str, **detail: Any) -> str:
        return json.dumps(
            {"at": now.isoformat(), "actor": actor, "action": action, **detail},
            sort_keys=True,
            default=str,
        )

    # --- specs and roles (design §11.1) ------------------------------------

    def add_spec(self, spec: Spec, *, status: str, actor: str, origin_profile: str) -> bool:
        if status not in SPEC_STATUSES:
            raise ValueError(status)
        now = self.now()
        res = self.db.lwt(
            "INSERT INTO {ks}.specs (spec_digest, name, version, document, status, compute_dtype, "
            "origin_profile, created_at, updated_at, audit) VALUES (?,?,?,?,?,?,?,?,?,?) IF NOT EXISTS",
            str(spec.digest),
            spec.name,
            spec.version,
            spec.canonical.decode(),
            status,
            spec.compute_dtype,
            origin_profile,
            now,
            now,
            [self.audit_entry(now, actor, "add", status=status)],
        )
        return res.applied

    def spec_row(self, digest: str) -> dict[str, Any] | None:
        return self.db.one("SELECT * FROM {ks}.specs WHERE spec_digest = ?", digest)

    def spec(self, digest: str) -> Spec:
        cached = self._specs.get(digest)
        if cached is None:
            row = self.spec_row(digest)
            if row is None:
                raise KeyError(f"unknown spec {digest}")
            cached = Spec.from_json(row["document"])
            if str(cached.digest) != digest:
                raise ValueError(f"stored spec document does not hash to {digest}")
            self._specs[digest] = cached
        return cached

    def specs(self) -> list[dict[str, Any]]:
        return self.db.rows("SELECT * FROM {ks}.specs")

    def set_spec_status(
        self, digest: str, *, expect: str, status: str, actor: str, evidence: str | None = None
    ) -> bool:
        now = self.now()
        res = self.db.lwt(
            "UPDATE {ks}.specs SET status = ?, updated_at = ?, evidence = ?, audit = audit + ? "
            "WHERE spec_digest = ? IF status = ?",
            status,
            now,
            evidence,
            [self.audit_entry(now, actor, "status", frm=expect, to=status, evidence=evidence)],
            digest,
            expect,
        )
        return res.applied

    def role(self, role: str = "default") -> dict[str, Any] | None:
        return self.db.one("SELECT * FROM {ks}.spec_roles WHERE role = ?", role, level=SERIAL)

    def default_spec_digest(self) -> str:
        row = self.role("default")
        if row is None or not row.get("spec_digest"):
            raise LookupError("no default spec set (fingerprinter-spec set-default)")
        return str(row["spec_digest"])

    def set_role(self, digest: str, *, actor: str, role: str = "default") -> bool:
        spec = self.spec_row(digest)
        if spec is None or spec["status"] != "active":
            return False  # gated/deprecated/retired specs can never become the default
        now = self.now()
        current = self.role(role)
        entry = [self.audit_entry(now, actor, "set", spec=digest)]
        if current is None:
            return self.db.lwt(
                "INSERT INTO {ks}.spec_roles (role, spec_digest, epoch, updated_at, audit) "
                "VALUES (?,?,1,?,?) IF NOT EXISTS",
                role,
                digest,
                now,
                entry,
            ).applied
        return self.db.lwt(
            "UPDATE {ks}.spec_roles SET spec_digest = ?, epoch = ?, updated_at = ?, "
            "audit = audit + ? WHERE role = ? IF epoch = ?",
            digest,
            int(current["epoch"]) + 1,
            now,
            entry,
            role,
            current["epoch"],
        ).applied

    # --- runtime profiles (design §3.5) ------------------------------------

    def put_profile(self, profile: RuntimeProfile, *, actor: str) -> bool:
        now = self.now()
        return self.db.lwt(
            "INSERT INTO {ks}.runtime_profiles (profile, document, admitted_variants, created_at, "
            "audit) VALUES (?,?,?,?,?) IF NOT EXISTS",
            profile.name,
            profile.document(),
            {"cpu"} if "cpu" in profile.variants else set(),
            now,
            [self.audit_entry(now, actor, "create", variants=sorted(profile.variants))],
        ).applied

    def profile(self, name: str) -> tuple[RuntimeProfile, set[str]] | None:
        row = self.db.one("SELECT * FROM {ks}.runtime_profiles WHERE profile = ?", name)
        if row is None:
            return None
        return RuntimeProfile.from_document(row["document"]), set(row["admitted_variants"] or ())

    def admit_variant(self, name: str, device: str, *, actor: str, evidence: str) -> None:
        now = self.now()
        self.db.run(
            "UPDATE {ks}.runtime_profiles SET admitted_variants = admitted_variants + ?, "
            "audit = audit + ? WHERE profile = ?",
            {device},
            [self.audit_entry(now, actor, "admit", device=device, evidence=evidence)],
            name,
        )

    def approve_compat(self, digest: str, profile: str, *, actor: str, evidence: str) -> None:
        self.db.run(
            "INSERT INTO {ks}.spec_runtime_compat (spec_digest, profile, approved_by, approved_at, "
            "evidence) VALUES (?,?,?,?,?)",
            digest,
            profile,
            actor,
            self.now(),
            evidence,
        )

    def compat_profiles(self, digest: str) -> list[str]:
        rows = self.db.rows(
            "SELECT profile FROM {ks}.spec_runtime_compat WHERE spec_digest = ?", digest
        )
        return [r["profile"] for r in rows]

    def runtime_check(self, digest: str, fp: RuntimeFingerprint) -> str | None:
        """None if ``fp`` may produce ``digest``; else why not (fail closed)."""
        reasons = []
        for name in self.compat_profiles(digest) or []:
            found = self.profile(name)
            if found is None:
                reasons.append(f"{name}: profile missing")
                continue
            reason = found[0].mismatch(fp, found[1])
            if reason is None:
                return None
            reasons.append(f"{name}: {reason}")
        return "; ".join(reasons) or "no runtime profile allowed for this spec"

    # --- due index (design §7.3) -------------------------------------------

    def due_put(self, content_id: str, spec_key: str, due_at: datetime) -> None:
        day = day_of(due_at)
        self.db.run("INSERT INTO {ks}.encode_due_days (due_day) VALUES (?)", day)
        self.db.run(
            "INSERT INTO {ks}.encode_due (due_day, shard, due_at, content_id, spec_key) "
            "VALUES (?,?,?,?,?)",
            day,
            shard_of(content_id, self.settings.due_shards),
            due_at,
            content_id,
            spec_key,
        )

    def due_del(self, content_id: str, spec_key: str, due_at: datetime | None) -> None:
        if due_at is None:
            return
        self.db.run(
            "DELETE FROM {ks}.encode_due WHERE due_day = ? AND shard = ? AND due_at = ? "
            "AND content_id = ? AND spec_key = ?",
            day_of(due_at),
            shard_of(content_id, self.settings.due_shards),
            due_at,
            content_id,
            spec_key,
        )

    def due_days(self) -> list[str]:
        return sorted(r["due_day"] for r in self.db.rows("SELECT due_day FROM {ks}.encode_due_days"))

    def due_scan(self, day: str, upto: datetime, limit: int = 1000) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for shard in range(self.settings.due_shards):
            out.extend(
                self.db.rows(
                    "SELECT * FROM {ks}.encode_due WHERE due_day = ? AND shard = ? AND due_at <= ? "
                    "LIMIT ?",
                    day,
                    shard,
                    upto,
                    limit,
                )
            )
        return out

    def due_exists(self, content_id: str, spec_key: str, due_at: datetime) -> bool:
        return (
            self.db.one(
                "SELECT content_id FROM {ks}.encode_due WHERE due_day = ? AND shard = ? AND "
                "due_at = ? AND content_id = ? AND spec_key = ?",
                day_of(due_at),
                shard_of(content_id, self.settings.due_shards),
                due_at,
                content_id,
                spec_key,
            )
            is not None
        )

    def drop_due_day_if_empty(self, day: str, today: str) -> bool:
        # A day is removed only once it is at least two days old, so a writer that
        # computed "yesterday" just before midnight can never lose its entry.
        cutoff = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        if day >= cutoff:
            return False
        for shard in range(self.settings.due_shards):
            if self.db.one(
                "SELECT content_id FROM {ks}.encode_due WHERE due_day = ? AND shard = ? LIMIT 1",
                day,
                shard,
            ):
                return False
        self.db.run("DELETE FROM {ks}.encode_due_days WHERE due_day = ?", day)
        return True

    # --- intents (design §8.1) ---------------------------------------------

    def intent_put(self, kind: str, fact_key: dict[str, Any], **fields: Any) -> Intent:
        now = self.now()
        intent_id = uuid7()
        intent = Intent(
            day=day_of(now),
            shard=shard_of(intent_id, self.settings.intent_shards),
            intent_id=intent_id,
            kind=kind,
            fact_key=fact_key,
            created_at=now,
            holder=self.instance,
            **fields,
        )
        self.db.run("INSERT INTO {ks}.intent_days (day) VALUES (?)", intent.day)
        self.db.run(
            "INSERT INTO {ks}.intents (day, shard, intent_id, kind, fact_key, attempt_id, event_id, "
            "artifact_uri, stream, envelope, payload, created_at, holder) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            intent.day,
            intent.shard,
            intent.intent_id,
            kind,
            json.dumps(fact_key, sort_keys=True),
            intent.attempt_id,
            intent.event_id,
            intent.artifact_uri,
            intent.stream,
            intent.envelope,
            json.dumps(intent.payload, sort_keys=True) if intent.payload is not None else None,
            now,
            self.instance,
        )
        return intent

    def intent_del(self, intent: Intent) -> None:
        self.db.run(
            "DELETE FROM {ks}.intents WHERE day = ? AND shard = ? AND intent_id = ?",
            intent.day,
            intent.shard,
            intent.intent_id,
        )

    def intent_days(self) -> list[str]:
        return sorted(r["day"] for r in self.db.rows("SELECT day FROM {ks}.intent_days"))

    def intents(self) -> Iterator[Intent]:
        for day in self.intent_days():
            for shard in range(self.settings.intent_shards):
                for row in self.db.rows(
                    "SELECT * FROM {ks}.intents WHERE day = ? AND shard = ?", day, shard
                ):
                    yield Intent.from_row(row)

    def drop_intent_day_if_empty(self, day: str, today: str) -> bool:
        if day >= today:
            return False
        for shard in range(self.settings.intent_shards):
            if self.db.one(
                "SELECT intent_id FROM {ks}.intents WHERE day = ? AND shard = ? LIMIT 1", day, shard
            ):
                return False
        self.db.run("DELETE FROM {ks}.intent_days WHERE day = ?", day)
        return True

    def open_intent_for_attempt(self, attempt_id: str) -> bool:
        return any(i.attempt_id == attempt_id for i in self.intents())

    # --- representations (read side) ---------------------------------------

    def representation(self, content_id: str, spec_digest: str, *, serial: bool = False) -> dict[str, Any] | None:
        return self.db.one(
            "SELECT * FROM {ks}.representations WHERE content_id = ? AND spec_digest = ?",
            content_id,
            spec_digest,
            level=SERIAL if serial else "quorum",
        )

    def bucket_of(self, content_id: str) -> int:
        return shard_of(content_id, self.settings.backfill.buckets)
