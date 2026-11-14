"""Event publication (design §9, P9-D7/D21/D22).

- **Stream registry and holds:** each fingerprinter stream has
  ``required_groups`` and ``hold_until_registered``. A stream's events are
  published only if the hold is off, or every required group exists and the set
  is non-empty. Otherwise they stay **held** (intent + stored envelope in Scylla,
  no TTL). Nothing is published before its required consumers exist.
- **ADR-022 rules on our own streams:** before a write, the stream is trimmed
  exactly (``XTRIM MINID``) to the oldest entry any group still needs (never
  past an unacknowledged entry); if the next envelope would exceed
  ``stream_max_bytes`` of unacknowledged entries, publication waits
  (backpressure). Facts stay durable meanwhile.
- **XADD is not processing:** the caller acknowledges "appended" on the fact
  row; downstream processing is the consumer group's business.

Entry fields match crawler2's transport: ``event_id``, ``event_type``, ``envelope``.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

import redis

from fingerprinter.coord import Coord
from fingerprinter.errors import StorageUnavailableError
from fingerprinter.store import Store

ENTRY_OVERHEAD_BYTES = 1024

READY = "representation.ready.v1"
FAILED = "encode.failed.v1"
TARGET_REGISTERED = "target.registered.v1"
TARGET_RETIRED = "target.retired.v1"
INTAKE = "encode.requested.v1"

SEED: dict[str, tuple[set[str], bool]] = {
    READY: ({"media-registry"}, True),
    FAILED: ({"media-registry"}, True),
    TARGET_REGISTERED: (set(), True),
    TARGET_RETIRED: (set(), True),
}


class Outcome(StrEnum):
    PUBLISHED = "published"
    HELD = "held"
    BACKPRESSURE = "backpressure"


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _id(entry_id: str) -> tuple[int, int]:
    ms, _, seq = entry_id.partition("-")
    return int(ms), int(seq or 0)


def _next_id(entry_id: str) -> str:
    ms, seq = _id(entry_id)
    return f"{ms}-{seq + 1}"


class Publisher:
    def __init__(self, store: Store) -> None:
        self.store = store
        self.coord: Coord = store.coord
        self.budget = store.settings.publisher.stream_max_bytes

    def stream_key(self, name: str) -> str:
        return self.coord.stream(name)

    # --- registry ---------------------------------------------------------

    def seed(self) -> None:
        now = self.store.now()
        for name, (groups, hold) in SEED.items():
            self.store.db.lwt(
                "INSERT INTO {ks}.stream_registry (stream, required_groups, hold_until_registered, "
                "audit) VALUES (?,?,?,?) IF NOT EXISTS",
                name,
                groups,
                hold,
                [self.store.audit_entry(now, "migration", "seed", groups=sorted(groups), hold=hold)],
            )

    def registry(self, name: str) -> dict[str, Any] | None:
        return self.store.db.one("SELECT * FROM {ks}.stream_registry WHERE stream = ?", name)

    def groups(self, name: str) -> list[dict[str, Any]]:
        try:
            infos: Any = self.coord.client.xinfo_groups(self.stream_key(name))
        except redis.ResponseError as exc:
            if "no such key" in str(exc).lower():
                return []
            raise StorageUnavailableError(str(exc)) from exc
        except redis.RedisError as exc:
            raise StorageUnavailableError(str(exc)) from exc
        out = []
        for info in infos:
            group = _text(info["name"])
            pending = int(info["pending"])
            oldest = None
            if pending:
                summary: Any = self.coord.client.xpending(self.stream_key(name), group)
                oldest = _text(summary["min"]) if summary.get("min") else None
            out.append(
                {
                    "name": group,
                    "pending": pending,
                    "oldest": oldest,
                    "last_delivered": _text(info["last-delivered-id"]),
                }
            )
        return out

    def held(self, name: str) -> tuple[bool, str]:
        row = self.registry(name)
        if row is None:
            return True, "stream not registered"
        if not row["hold_until_registered"]:
            return False, "hold disabled"
        required = set(row["required_groups"] or ())
        if not required:
            return True, "no consumer registered"
        present = {g["name"] for g in self.groups(name)}
        missing = sorted(required - present)
        if missing:
            return True, f"required group(s) missing: {', '.join(missing)}"
        return False, "all required groups present"

    def register_consumer(self, name: str, group: str, *, actor: str) -> None:
        if group not in {g["name"] for g in self.groups(name)}:
            raise ValueError(f"group {group!r} does not exist on {self.stream_key(name)}")
        now = self.store.now()
        self.store.db.run(
            "UPDATE {ks}.stream_registry SET required_groups = required_groups + ?, "
            "hold_until_registered = true, audit = audit + ? WHERE stream = ?",
            {group},
            [self.store.audit_entry(now, actor, "register-consumer", group=group)],
            name,
        )

    # --- publication -------------------------------------------------------

    def publish(self, name: str, event_id: str, event_type: str, envelope: str) -> Outcome:
        held, _ = self.held(name)
        if held:
            return Outcome.HELD
        key = self.stream_key(name)
        groups = self.groups(name)
        try:
            if groups:
                floor = min(
                    (
                        g["oldest"] if g["pending"] and g["oldest"] else _next_id(g["last_delivered"])
                        for g in groups
                    ),
                    key=_id,
                )
                self.coord.client.xtrim(key, minid=floor, approximate=False)
                used = int(self.coord.client.memory_usage(key, samples=0) or 0)
                if self.coord.client.xlen(key) == 0:
                    used = 0
                size = len(envelope.encode()) + ENTRY_OVERHEAD_BYTES
                if used > 0 and used + size > self.budget:
                    return Outcome.BACKPRESSURE
            self.coord.client.xadd(
                key, {"event_id": event_id, "event_type": event_type, "envelope": envelope}
            )
        except redis.RedisError as exc:
            raise StorageUnavailableError(f"publish {name}: {exc}") from exc
        return Outcome.PUBLISHED

    def status(self) -> list[dict[str, Any]]:
        out = []
        for name in SEED:
            held, why = self.held(name)
            row = self.registry(name) or {}
            out.append(
                {
                    "stream": name,
                    "held": held,
                    "reason": why,
                    "required_groups": sorted(row.get("required_groups") or ()),
                    "groups": self.groups(name),
                }
            )
        return out
