"""Match tasks, verification, commits and held publication (design §6.2-§6.4, §9, §10, §11; P10-D5/D6/D9/D11/D13).

A task row ``match_tasks(subject, spec, kind)`` executes numbered **runs**:
``PENDING → INDEXED → SEARCHED → VERIFYING(page) → DONE | INCOMPLETE``, with
``WAITING_INDEX`` (no ACTIVE generation, index unavailable, capacity) and
``FAILED`` (only for the subject's *own* invalid data). Every transition is
an LWT ``IF epoch = :e AND run = :r`` (design §11.1); the run bump of the
recovery protocol is a single-row LWT on the same row, so Scylla's Paxos
orders it with the run's completion (S5).

Run steps:

1. **INDEXED** — the subject's own points are applied (mutation protocol) to
   every routed generation of its role, and to the ACTIVE one in particular
   (ordering argument §6.4: index before search).
2. **SEARCHED** — one search step pinned to one generation of the opposite
   role (``candidates.Searcher``); the bounded candidate list of *this run*
   is written to ``match_candidates(…, generation, run)``.
3. **VERIFYING** — pages of candidates; each candidate is accepted only after
   (i) its point id equals ``uuid5(subject:seq:k)`` of its payload, (ii) its
   subject is authoritatively live (QUORUM read of P9 rows, §4.2), (iii)
   ``temporal/v2`` decided on the SHA-verified **artifact** vectors (Invariant
   C). A corrupt *candidate* artifact fails that candidate only (C-14d).
4. **completion** — DONE only if no bound was hit; else INCOMPLETE with the
   reason (``saturated_segments``, ``candidate_cap``, ``verification_failed``).

Commits (design §9.3): intent → ``INSERT matches … IF NOT EXISTS`` (the first
committed decision per ``match_id`` is final; a later different result is
recorded as a divergence, never applied) → reverse rows → if the technique
is CALIBRATED, the envelope is stored once (``IF event_id = null``) and
published through the P9 publisher on P10's own held stream
``match.found.v1``. Otherwise the decision is stored and held.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Any

from antipiracy_contracts.events import encode_event, new_event
from antipiracy_contracts.events.fingerprinting import MatchFound
from antipiracy_contracts.ids import ContentId, MatchId, RepresentationId, TargetId
from antipiracy_contracts.models.matching import AlignedSegment, MatchResult, MatchVerdict
from antipiracy_contracts.models.matching import Technique as ContractTechnique
from antipiracy_contracts.models.targets import TargetRef
from antipiracy_contracts.ownership import Component

from fingerprinter import metrics
from fingerprinter.candidates import (
    NoActiveGenerationError,
    Searcher,
    SearchStepAbandonedError,
    policy_for,
)
from fingerprinter.confidence import CALIBRATED, Technique, Techniques
from fingerprinter.coord import utc, uuid7
from fingerprinter.db import SERIAL, LwtUnknownError
from fingerprinter.errors import StorageUnavailableError
from fingerprinter.generations import Generations
from fingerprinter.indexing import Indexer, SubjectInvalidError, WaitingIndexError
from fingerprinter.objects import IntegrityError, ObjectMissingError
from fingerprinter.p10ctx import KIND_A, KIND_B, KIND_EQ, OPPOSITE, P10, ROLE_OF_KIND, Subject
from fingerprinter.publish import Outcome, Publisher
from fingerprinter.store import day_of, shard_of
from fingerprinter.verify import TECHNIQUE_NAME, Vectors, Verification, equivalence, verify
from fingerprinter.vindex import MEDIA, TARGET, IndexMissingError, IndexUnavailableError, point_id

log = logging.getLogger("fingerprinter.matching")
MATCH_FOUND = "match.found.v1"
PENDING, INDEXED, SEARCHED, VERIFYING = "PENDING", "INDEXED", "SEARCHED", "VERIFYING"
DONE, INCOMPLETE, WAITING_INDEX, FAILED = "DONE", "INCOMPLETE", "WAITING_INDEX", "FAILED"
NON_FAILED = [PENDING, INDEXED, SEARCHED, VERIFYING, DONE, INCOMPLETE, WAITING_INDEX]
DUE_SHARDS = 16

_POP = """
local items = redis.call('ZPOPMIN', KEYS[1], 1)
if #items == 0 then return nil end
local member = items[1]
local lease = ARGV[1] .. member
if redis.call('SET', lease, ARGV[2], 'NX', 'PX', ARGV[3]) then return member end
return {member, 'leased'}
"""


class LeaseLost(Exception):  # noqa: N818 - control flow: the run moved on without this worker
    pass


def member_of(subject_id: str, spec: str, kind: str) -> str:
    return f"{subject_id}|{spec}|{kind}"


def parse_member(m: str) -> tuple[str, str, str]:
    a, b, c = m.split("|")
    return a, b, c


# --- the task rows ---------------------------------------------------------------------------


class TaskStore:
    def __init__(self, p10: P10) -> None:
        self.p = p10
        self.db = p10.db
        self.coord = p10.coord
        self.queue = p10.key("q", "match")
        self._pop = p10.coord.client.register_script(_POP)

    def row(self, subject_id: str, spec: str, kind: str) -> dict[str, Any] | None:
        return self.db.one("SELECT * FROM {ks}.match_tasks WHERE subject_id = ? AND spec_digest = ? AND kind = ?",
                           subject_id, spec, kind, level=SERIAL)

    def rows_for_subject(self, s: Subject) -> list[dict[str, Any]]:
        return [r for r in self.db.rows("SELECT * FROM {ks}.match_tasks WHERE subject_id = ?", s.subject_id)
                if r["spec_digest"] == s.spec_digest]

    def all(self) -> list[dict[str, Any]]:
        return list(self.db.stream("SELECT * FROM {ks}.match_tasks"))

    # --- due index (P9 encode_due pattern; Redis queue rebuilt from it) ---

    def due_put(self, subject_id: str, spec: str, kind: str, due_at: datetime) -> None:
        day = day_of(due_at)
        self.db.run("INSERT INTO {ks}.match_due_days (due_day) VALUES (?)", day)
        self.db.run("INSERT INTO {ks}.match_due (due_day, shard, due_at, subject_id, spec_digest, kind) "
                    "VALUES (?,?,?,?,?,?)", day, shard_of(subject_id, DUE_SHARDS), due_at, subject_id, spec, kind)
        self.coord.call(self.coord.client.zadd, self.queue, {member_of(subject_id, spec, kind): due_at.timestamp()})

    def due_del(self, subject_id: str, spec: str, kind: str, due_at: datetime | None) -> None:
        if due_at is None:
            return
        self.db.run("DELETE FROM {ks}.match_due WHERE due_day = ? AND shard = ? AND due_at = ? AND subject_id = ? "
                    "AND spec_digest = ? AND kind = ?", day_of(due_at), shard_of(subject_id, DUE_SHARDS), due_at,
                    subject_id, spec, kind)

    def due_clear(self, subject_id: str, spec: str, kind: str) -> int:
        """Delete every due row of one task (re-enqueues add rows at new due times)."""
        n = 0
        shard = shard_of(subject_id, DUE_SHARDS)
        for d in self.db.rows("SELECT due_day FROM {ks}.match_due_days"):
            for r in self.db.rows("SELECT due_at FROM {ks}.match_due WHERE due_day = ? AND shard = ? "
                                  "AND subject_id = ? AND spec_digest = ? AND kind = ? ALLOW FILTERING",
                                  d["due_day"], shard, subject_id, spec, kind):
                self.due_del(subject_id, spec, kind, utc(r["due_at"]))
                n += 1
        self.coord.call(self.coord.client.zrem, self.queue, member_of(subject_id, spec, kind))
        return n

    def schedule(self, horizon_s: float = 60.0) -> int:
        """Rebuild the Redis queue from ``match_due`` (anything due within the horizon)."""
        upto = self.p.now() + timedelta(seconds=horizon_s)
        n = 0
        for d in sorted(r["due_day"] for r in self.db.rows("SELECT due_day FROM {ks}.match_due_days")):
            if d > day_of(upto):
                continue
            for shard in range(DUE_SHARDS):
                for r in self.db.rows("SELECT * FROM {ks}.match_due WHERE due_day = ? AND shard = ? AND due_at <= ?",
                                      d, shard, upto):
                    self.coord.call(self.coord.client.zadd, self.queue,
                                    {member_of(r["subject_id"], r["spec_digest"], r["kind"]):
                                     utc(r["due_at"]).timestamp()})
                    n += 1
        return n

    def pop(self, token: str, lease_ms: int) -> str | None:
        now = self.p.now().timestamp()
        head = self.coord.call(self.coord.client.zrange, self.queue, 0, 0, withscores=True)
        if not head or head[0][1] > now:
            return None
        reply = self.coord.call(self._pop, keys=[self.queue], args=[self.p.key("lease", "task", ""), token, lease_ms])
        if reply is None or isinstance(reply, list):
            return None
        return reply.decode() if isinstance(reply, bytes) else reply

    def release(self, member: str, token: str) -> None:
        self.coord.release(self.p.key("lease", "task", member), token)

    # --- creation and run bumps ---

    def create(self, s: Subject, kind: str) -> bool:
        now = self.p.now()
        if self.row(s.subject_id, s.spec_digest, kind) is not None:
            return False
        self.due_put(s.subject_id, s.spec_digest, kind, now)
        return self.db.lwt(
            "INSERT INTO {ks}.match_tasks (subject_id, spec_digest, kind, state, epoch, run, recovery_runs, attempt, "
            "failures, due_at, subject, created_at, updated_at) VALUES (?,?,?,'PENDING',0,1,?,0,0,?,?,?,?) "
            "IF NOT EXISTS", s.subject_id, s.spec_digest, kind, {}, now, s.meta(), now, now).applied

    def bump_for_occurrence(self, s: Subject, kind: str, oid: uuid.UUID) -> int:
        """The run an occurrence requires (design §12.10 'Run bump'); idempotent per occurrence id."""
        now = self.p.now()
        self.due_put(s.subject_id, s.spec_digest, kind, now)  # the due entry first (idempotent)
        for _ in range(50):
            row = self.row(s.subject_id, s.spec_digest, kind)
            try:
                if row is None:
                    res = self.db.lwt(
                        "INSERT INTO {ks}.match_tasks (subject_id, spec_digest, kind, state, epoch, "
                        "run, recovery_runs, "
                        "rerun_reason, attempt, failures, due_at, subject, created_at, updated_at) "
                        "VALUES (?,?,?,'PENDING',0,1,?,'index_repair',0,0,?,?,?,?) IF NOT EXISTS",
                        s.subject_id, s.spec_digest, kind, {oid: 1}, now, s.meta(), now, now)
                    if res.applied:
                        return 1
                    continue
                runs = row.get("recovery_runs") or {}
                if oid in runs:
                    return int(runs[oid])
                r = int(row["run"])
                if row["state"] == PENDING and row.get("run_started_at") is None:
                    res = self.db.lwt("UPDATE {ks}.match_tasks SET recovery_runs[?] = ? WHERE subject_id = ? AND "
                                      "spec_digest = ? AND kind = ? IF run = ? AND run_started_at = null", oid, r,
                                      s.subject_id, s.spec_digest, kind, r)
                    if res.applied:
                        return r
                    continue
                if row["state"] == FAILED:
                    return r + 1  # the run an audited `match retry` would create
                res = self.db.lwt("UPDATE {ks}.match_tasks SET run = ?, state = 'PENDING', run_started_at = null, "
                                  "recovery_runs[?] = ?, rerun_reason = 'index_repair', "
                                  "next_attempt_at = ?, due_at = ? "
                                  "WHERE subject_id = ? AND spec_digest = ? AND kind = ? IF run = ? AND state IN ?",
                                  r + 1, oid, r + 1, now, now, s.subject_id, s.spec_digest, kind, r, NON_FAILED)
                if res.applied:
                    metrics.P10_RERUNS.labels(reason="index_repair").inc()
                    return r + 1
            except LwtUnknownError:
                continue  # SERIAL re-read: recovery_runs[oid] present <=> applied (only this occurrence writes it)
        raise LwtUnknownError("run bump did not converge")

    def rerun(self, s: Subject, kind: str, *, reason: str, actor: str, cap: int | None = None) -> int | None:
        """Audited ``match rerun`` / ``match retry`` (``retry`` of a FAILED task conditions on FAILED)."""
        now = self.p.now()
        self.due_put(s.subject_id, s.spec_digest, kind, now)
        row = self.row(s.subject_id, s.spec_digest, kind)
        if row is None:
            self.create(s, kind)
            return 1
        r = int(row["run"])
        cond, args = ("state = ?", ["FAILED"]) if row["state"] == FAILED else ("state IN ?", [NON_FAILED])
        res = self.db.lwt(f"UPDATE {{ks}}.match_tasks SET run = ?, state = 'PENDING', "  # noqa: S608
                          "run_started_at = null, "
                          "rerun_reason = ?, next_attempt_at = ?, due_at = ?, failure = null, cap_override = ? "
                          f"WHERE subject_id = ? AND spec_digest = ? AND kind = ? IF run = ? AND {cond}",
                          r + 1, reason, now, now, cap, s.subject_id, s.spec_digest, kind, r, *args)
        self.p.store.audit(actor, "match-rerun", {"subject": s.subject_id, "kind": kind, "run": r + 1,
                                                  "reason": reason, "cap": cap, "applied": res.applied})
        if res.applied:
            metrics.P10_RERUNS.labels(reason=reason).inc()
        return r + 1 if res.applied else None


# --- commits and publication -----------------------------------------------------------------


class MatchCommitter:
    def __init__(self, p10: P10, techniques: Techniques) -> None:
        self.p = p10
        self.db = p10.db
        self.techniques = techniques
        self.pub = Publisher(p10.store)
        self.hook = p10.hook

    def seed_stream(self) -> None:
        """P10's own registry row (P9 publisher pattern): held until a consumer group registers."""
        self.db.lwt("INSERT INTO {ks}.stream_registry (stream, required_groups, hold_until_registered, audit) "
                    "VALUES (?,?,?,?) IF NOT EXISTS", MATCH_FOUND, set(), True,
                    [self.p.store.audit_entry(self.p.now(), "matcher", "seed", groups=[], hold=True)])

    def intent_put(self, kind: str, key: dict[str, Any], payload: dict[str, Any]) -> tuple[str, int, str]:
        now = self.p.now()
        iid = uuid7()
        day, shard = day_of(now), shard_of(iid, 16)
        self.db.run("INSERT INTO {ks}.match_intent_days (day) VALUES (?)", day)
        self.db.run("INSERT INTO {ks}.match_intents (day, shard, intent_id, kind, fact_key, "
                    "payload, created_at, holder) "
                    "VALUES (?,?,?,?,?,?,?,?)", day, shard, iid, kind, json.dumps(key, sort_keys=True),
                    json.dumps(payload, sort_keys=True, default=str), now, self.p.instance)
        return day, shard, iid

    def intent_del(self, ref: tuple[str, int, str]) -> None:
        self.db.run("DELETE FROM {ks}.match_intents WHERE day = ? AND shard = ? AND intent_id = ?", *ref)

    def intents(self) -> list[dict[str, Any]]:
        out = []
        for d in self.db.rows("SELECT day FROM {ks}.match_intent_days"):
            for shard in range(16):
                out.extend(self.db.rows("SELECT * FROM {ks}.match_intents WHERE day = ? AND shard = "
                                        "?", d["day"], shard))
        return out

    def match_row(self, key: dict[str, Any]) -> dict[str, Any] | None:
        return self.db.one("SELECT * FROM {ks}.matches WHERE content_id = ? AND target_id = ? AND target_version = ? "
                           "AND media_spec_digest = ? AND target_spec_digest = ? AND technique_name = ? "
                           "AND technique_version = ?", *self._key_args(key), level=SERIAL)

    @staticmethod
    def _key_args(k: dict[str, Any]) -> list[Any]:
        return [k["content_id"], k["target_id"], int(k["target_version"]), k["media_spec_digest"],
                k["target_spec_digest"], k["technique_name"], k["technique_version"]]

    def decided(self, match_id: str) -> bool:
        return self.db.one("SELECT match_id FROM {ks}.matches_by_id WHERE match_id = ?", match_id) is not None

    def commit(self, media: Subject, target: Subject, tech: Technique, res: Verification,
               provenance: dict[str, Any]) -> tuple[str, bool]:
        """Persist one verified decision (verdict confirmed/probable). Returns (match_id, newly decided)."""
        mid = str(MatchId.of(RepresentationId(media.subject_id), RepresentationId(target.subject_id), tech.name,
                             tech.version))
        key = {"content_id": media.content_id, "target_id": target.target_id, "target_version": target.target_version,
               "media_spec_digest": media.spec_digest, "target_spec_digest": target.spec_digest,
               "technique_name": tech.name, "technique_version": tech.version}
        segs = [r.aligned(tech.params.segment_duration_s) for r in res.runs]
        payload = {"match_id": mid, "representation_id": media.subject_id,
                   "target_representation_id": target.subject_id,
                   "verdict": res.verdict, "score": res.score, "alignment": segs,
                   "verifier_params_digest": res.params_digest, "exclusion_digest": res.exclusion_digest,
                   "provenance": provenance}
        ref = self.intent_put("match_commit", key, payload)
        self.hook("committer.after_intent", match_id=mid)
        newly = self._apply_commit(key, payload)
        self.hook("committer.after_matches_row", match_id=mid)
        self._reverse_rows(key, mid)
        self.publish_if_calibrated(key, tech)
        self.intent_del(ref)
        return mid, newly

    def _apply_commit(self, key: dict[str, Any], pl: dict[str, Any]) -> bool:
        expected = str(MatchId.of(RepresentationId(pl["representation_id"]),
                                  RepresentationId(pl["target_representation_id"]), key["technique_name"],
                                  key["technique_version"]))
        if expected != pl["match_id"]:  # the writer asserts the key determines the MatchId (C-6)
            raise ValueError("match_id is not MatchId.of(the key components)")
        try:
            res = self.db.lwt(
                "INSERT INTO {ks}.matches (content_id, target_id, target_version, "
                "media_spec_digest, target_spec_digest, "
                "technique_name, technique_version, match_id, representation_id, target_representation_id, verdict, "
                "score, alignment, verifier_params_digest, exclusion_digest, provenance, decided_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) IF NOT EXISTS",
                *self._key_args(key), pl["match_id"], pl["representation_id"], pl["target_representation_id"],
                pl["verdict"], pl["score"], json.dumps(pl["alignment"]), pl["verifier_params_digest"],
                pl["exclusion_digest"], json.dumps(pl["provenance"], sort_keys=True, default=str), self.p.now())
            applied = res.applied
            existing = res.row
        except LwtUnknownError:
            existing = self.match_row(key) or {}
            applied = existing.get("provenance") == json.dumps(pl["provenance"], sort_keys=True, default=str)
        if applied:
            metrics.P10_DECISIONS.labels(kind=pl["provenance"].get("kind", "?"), verdict=pl["verdict"]).inc()
            return True
        # the first committed decision is final; a different later result is recorded, never applied (C-14f)
        if existing and (existing.get("verdict") != pl["verdict"] or existing.get("score") != pl["score"]
                         or existing.get("alignment") != json.dumps(pl["alignment"])):
            metrics.P10_DIVERGENCE.inc()
            self.p.store.audit("matcher", "match-decision-divergence", {
                "match_id": pl["match_id"],
                "first": {"verdict": existing.get("verdict"), "score": existing.get("score"),
                                                      "exclusion_digest": existing.get("exclusion_digest")},
                "later": {"verdict": pl["verdict"], "score": pl["score"], "exclusion_digest": pl["exclusion_digest"]}})
        return False

    def _reverse_rows(self, key: dict[str, Any], mid: str) -> None:
        self.db.run("INSERT INTO {ks}.matches_by_target (target_id, target_version, content_id, media_spec_digest, "
                    "target_spec_digest, technique_name, technique_version, match_id) VALUES (?,?,?,?,?,?,?,?)",
                    key["target_id"], int(key["target_version"]), key["content_id"], key["media_spec_digest"],
                    key["target_spec_digest"], key["technique_name"], key["technique_version"], mid)
        self.db.run("INSERT INTO {ks}.matches_by_id (match_id, content_id, target_id, "
                    "target_version, media_spec_digest, "
                    "target_spec_digest, technique_name, technique_version) VALUES (?,?,?,?,?,?,?,?)",
                    mid, *self._key_args(key))

    def publish_if_calibrated(self, key: dict[str, Any], tech: Technique) -> str:
        """Held publication (design §9.1, §9.3): only a CALIBRATED technique version has a confidence."""
        row = self.match_row(key)
        if row is None:
            return "missing"
        if row.get("published_at") is not None:
            return "published"
        if tech.status != CALIBRATED:
            metrics.P10_UNPUBLISHED.labels(version=tech.version).inc()
            return "held_uncalibrated"
        if row["verdict"] != "confirmed" and not self.p.settings.matching.publish_probable:
            return "not_published_probable"
        conf = row.get("confidence")
        if row.get("calibration_id") is None:
            conf = tech.confidence(float(row["score"]))
            self.db.lwt("UPDATE {ks}.matches SET confidence = ?, calibration_id = ? WHERE content_id = ? "
                        "AND target_id = ? AND target_version = ? AND media_spec_digest = ? AND target_spec_digest = ? "
                        "AND technique_name = ? AND technique_version = ? IF calibration_id = null",
                        conf, tech.calibration_id, *self._key_args(key))
            row = self.match_row(key)
            conf = row["confidence"]
        if row.get("event_id") is None:
            result = MatchResult(
                match_id=MatchId(row["match_id"]), content_id=ContentId(row["content_id"]),
                representation_id=RepresentationId(row["representation_id"]),
                target=TargetRef(target_id=TargetId(row["target_id"]), version=int(row["target_version"])),
                target_representation_id=RepresentationId(row["target_representation_id"]),
                technique=ContractTechnique(name=tech.name, version=tech.version),
                verdict=MatchVerdict(row["verdict"]), confidence=float(conf),
                segments=tuple(AlignedSegment(**s) for s in json.loads(row["alignment"])),
                decided_at=utc(row["decided_at"]))
            env = new_event(MatchFound(match=result), producer=self.p.store.producer(Component.MATCHER),
                            occurred_at=self.p.now())
            self.db.lwt("UPDATE {ks}.matches SET envelope = ?, event_id = ? WHERE content_id = ? AND target_id = ? "
                        "AND target_version = ? AND media_spec_digest = ? AND target_spec_digest = ? "
                        "AND technique_name = ? AND technique_version = ? IF event_id = null",
                        encode_event(env).decode(), str(env.event_id), *self._key_args(key))
            row = self.match_row(key)  # a concurrent publisher's envelope wins: one event id per match_id
        out = self.pub.publish(MATCH_FOUND, row["event_id"], "match.found", row["envelope"])
        metrics.EVENTS.labels(stream=MATCH_FOUND, outcome=out.value).inc()
        if out is Outcome.PUBLISHED:
            self.db.lwt("UPDATE {ks}.matches SET published_at = ? WHERE content_id = ? AND target_id = ? "
                        "AND target_version = ? AND media_spec_digest = ? AND target_spec_digest = ? "
                        "AND technique_name = ? AND technique_version = ? IF event_id = ?", self.p.now(),
                        *self._key_args(key), row["event_id"])
        return out.value

    def publish_held(self, version: str, *, actor: str) -> dict[str, int]:
        """Audited ``match publish-held``: refuses unless the technique version is CALIBRATED."""
        tech = self.techniques.get(version)
        if tech.status != CALIBRATED:
            raise ValueError(f"technique {tech.name} {version} is {tech.status}: no calibrated confidence exists")
        out: dict[str, int] = {}
        for row in self.db.stream("SELECT * FROM {ks}.matches"):
            if row["technique_version"] != version or row.get("published_at") is not None:
                continue
            key = {k: row[k] for k in ("content_id", "target_id", "target_version", "media_spec_digest",
                                       "target_spec_digest", "technique_name", "technique_version")}
            r = self.publish_if_calibrated(key, tech)
            out[r] = out.get(r, 0) + 1
        self.p.store.audit(actor, "match-publish-held", {"version": version, **out})
        return out

    # --- equivalence links (design §10) ---

    def link(self, a: Subject, b: Subject, tech: Technique, evidence: dict[str, Any],
             provenance: dict[str, Any]) -> bool:
        ref = self.intent_put("equiv_link", {"a": a.content_id, "b": b.content_id, "spec": a.spec_digest,
                                             "version": tech.version}, {"evidence": evidence, "provenance": provenance})
        applied = self._link_rows(a.content_id, b.content_id, a.spec_digest, tech, evidence, provenance)
        self.intent_del(ref)
        return applied

    def _link_rows(self, a: str, b: str, spec: str, tech: Technique, evidence: dict[str, Any],
                   provenance: dict[str, Any]) -> bool:
        applied = False
        now = self.p.now()
        for x, y in ((a, b), (b, a)):
            res = self.db.lwt("INSERT INTO {ks}.equivalence_links (content_id, other_content_id, spec_digest, "
                              "technique_name, technique_version, state, evidence, provenance, decided_at, audit) "
                              "VALUES (?,?,?,?,?,'LINKED',?,?,?,?) IF NOT EXISTS", x, y, spec, tech.name, tech.version,
                              json.dumps(evidence, sort_keys=True, default=str),
                              json.dumps(provenance, sort_keys=True, default=str), now,
                              [json.dumps({"at": now.isoformat(), "action": "link"})])
            applied = applied or res.applied
        if applied:
            metrics.P10_EQUIVALENCE.labels(state="LINKED").inc()
        return applied

    def revoke(self, a: str, b: str, spec: str, version: str, *, reason: str, actor: str) -> int:
        ref = self.intent_put("equiv_revoke", {"a": a, "b": b, "spec": spec, "version": version},
                              {"reason": reason, "actor": actor})
        n = 0
        now = self.p.now()
        for x, y in ((a, b), (b, a)):
            res = self.db.lwt("UPDATE {ks}.equivalence_links SET state = 'REVOKED', revoked_at = ?, audit = audit + ? "
                              "WHERE content_id = ? AND other_content_id = ? AND spec_digest = ? "
                              "AND technique_name = ? "
                              "AND technique_version = ? IF state = 'LINKED'", now,
                              [json.dumps({"at": now.isoformat(), "action": "revoke", "actor": actor,
                                           "reason": reason})], x, y, spec, TECHNIQUE_NAME, version)
            n += int(res.applied)
        self.p.store.audit(actor, "equivalence-revoke", {"a": a, "b": b, "spec": spec, "version": version,
                                                         "reason": reason, "rows": n})
        self.intent_del(ref)
        metrics.P10_EQUIVALENCE.labels(state="REVOKED").inc(n)
        return n

    def resolve_intent(self, row: dict[str, Any]) -> str:
        """The P10 reconciler's ``match_intents`` pass: re-apply the idempotent steps of a stale intent."""
        key, pl = json.loads(row["fact_key"]), json.loads(row["payload"] or "{}")
        ref = (row["day"], row["shard"], row["intent_id"])
        if row["kind"] == "match_commit":
            self._apply_commit(key, pl)
            self._reverse_rows(key, pl["match_id"])
            self.publish_if_calibrated(key, self.techniques.get(key["technique_version"]))
        elif row["kind"] == "equiv_link":
            tech = self.techniques.get(key["version"])
            self._link_rows(key["a"], key["b"], key["spec"], tech, pl["evidence"], pl["provenance"])
        elif row["kind"] == "equiv_revoke":
            self.revoke(key["a"], key["b"], key["spec"], key["version"], reason=pl["reason"], actor=pl["actor"])
        self.intent_del(ref)
        return row["kind"]


# --- the run executor ------------------------------------------------------------------------


class Matcher:
    def __init__(self, p10: P10, tasks: TaskStore, gens: Generations, indexer: Indexer, searcher: Searcher,
                 techniques: Techniques, committer: MatchCommitter, lowinfo: Any) -> None:
        self.p = p10
        self.db = p10.db
        self.tasks = tasks
        self.gens = gens
        self.indexer = indexer
        self.searcher = searcher
        self.techniques = techniques
        self.committer = committer
        self.lowinfo = lowinfo
        self.hook = p10.hook

    # --- transitions ---

    def _lwt(self, row: dict[str, Any], sets: dict[str, Any]) -> None:
        cols = ", ".join(f"{k} = ?" for k in sets)
        args = [*sets.values(), row["subject_id"], row["spec_digest"], row["kind"], row["epoch"], row["run"]]
        try:
            ok = self.db.lwt(f"UPDATE {{ks}}.match_tasks SET {cols}, updated_at = ? WHERE subject_id = ? "  # noqa: S608
                             "AND spec_digest = ? AND kind = ? IF epoch = ? AND run = ?",
                             *args[:len(sets)], self.p.now(), *args[len(sets):]).applied
        except LwtUnknownError:
            cur = self.tasks.row(row["subject_id"], row["spec_digest"], row["kind"]) or {}
            # continue only if the row shows our epoch and run with the intended state (design §11.1)
            ok = cur.get("epoch") == row["epoch"] and cur.get("run") == row["run"] and \
                all(cur.get(k) == v for k, v in sets.items() if k in ("state", "done_run", "verify_cursor"))
        if not ok:
            raise LeaseLost(f"task {row['subject_id']}/{row['kind']} run {row['run']} superseded")
        row.update(sets)

    def _backoff(self, row: dict[str, Any]) -> float:
        m = self.p.settings.matching
        return min(m.backoff_base_s * 2 ** min(int(row.get("attempt") or 0), 8), m.backoff_max_s)

    def _wait(self, row: dict[str, Any], reason: str, detail: str, *, keep_state: bool = False) -> str:
        due = self.p.now() + timedelta(seconds=self._backoff(row))
        self.tasks.due_put(row["subject_id"], row["spec_digest"], row["kind"], due)
        sets: dict[str, Any] = {"next_attempt_at": due, "due_at": due, "waiting_reason": f"{reason}: {detail}"[:300],
                                "attempt": int(row.get("attempt") or 0) + 1}
        if not keep_state:
            sets["state"] = WAITING_INDEX
            metrics.P10_WAITING.labels(kind=row["kind"], reason=reason).inc()
        self._lwt(row, sets)
        self._log(row, "waiting", reason=reason, detail=detail[:200])
        return f"waiting:{reason}"

    # --- one task ---

    def run_member(self, member: str, token: str) -> str:
        """Run one queue member popped (and leased) with ``token``; the Redis lease is avoidance only."""
        sid, spec, kind = parse_member(member)
        try:
            return self.run_task(sid, spec, kind)
        finally:
            self.tasks.release(member, token)

    def run_task(self, sid: str, spec: str, kind: str) -> str:
        row = self.tasks.row(sid, spec, kind)
        if row is None:
            return "missing"
        if row["state"] == FAILED or (row["state"] in (DONE, INCOMPLETE) and row.get("done_run") == row["run"]):
            self.tasks.due_clear(sid, spec, kind)
            return "terminal"
        nxt = utc(row.get("next_attempt_at"))
        if nxt is not None and nxt > self.p.now():
            self.tasks.due_put(sid, spec, kind, nxt)
            return "not_due"
        subject = self.p.refresh(Subject.from_meta(row["subject"]))
        # claim: a new epoch; the first claim of run r sets run_started_at
        epoch = int(row["epoch"] or 0) + 1
        lease_until = self.p.now() + timedelta(seconds=self.p.settings.matching.task_lease_s)
        if row.get("run_started_at") is None:
            res = self.db.lwt("UPDATE {ks}.match_tasks SET epoch = ?, owner = ?, lease_until = ?, run_started_at = ? "
                              "WHERE subject_id = ? AND spec_digest = ? AND kind = ? IF epoch = ? AND run = ? "
                              "AND run_started_at = null", epoch, self.p.instance, lease_until, self.p.now(), sid, spec,
                              kind, row["epoch"], row["run"])
        else:
            res = self.db.lwt("UPDATE {ks}.match_tasks SET epoch = ?, owner = ?, lease_until = ? WHERE subject_id = ? "
                              "AND spec_digest = ? AND kind = ? IF epoch = ? AND run = ?", epoch, self.p.instance,
                              lease_until, sid, spec, kind, row["epoch"], row["run"])
        if not res.applied:
            return "claim_lost"
        row = self.tasks.row(sid, spec, kind)
        if row["epoch"] != epoch:
            return "claim_lost"
        try:
            return self._run(row, subject)
        except LeaseLost:
            return "superseded"
        except SubjectInvalidError as exc:
            log.error(json.dumps({"event": "task_failed_subject_invalid", "subject": sid, "kind": kind,
                                  "reason": exc.reason, "detail": str(exc)[:300]}))
            self._lwt(row, {"state": FAILED, "failure": f"{exc.reason}: {exc}"[:300]})
            self.tasks.due_clear(sid, spec, kind)
            return f"failed:{exc.reason}"
        except NoActiveGenerationError as exc:
            return self._wait(row, "missing", str(exc))
        except WaitingIndexError as exc:
            return self._wait(row, exc.reason, str(exc))
        except IndexUnavailableError as exc:
            metrics.P10_QUERIES.labels(kind=kind, outcome="index_unavailable").inc()
            return self._wait(row, "unavailable", str(exc))
        except IndexMissingError as exc:
            metrics.P10_QUERIES.labels(kind=kind, outcome="index_missing").inc()
            return self._wait(row, "missing", str(exc))
        except SearchStepAbandonedError as exc:
            return self._wait(row, "search_deadline", str(exc), keep_state=True)
        except (StorageUnavailableError, LwtUnknownError) as exc:
            return self._wait(row, "storage", str(exc), keep_state=True)

    def _run(self, row: dict[str, Any], subject: Subject) -> str:
        if not subject.subject_id or not subject.live:
            if not subject.subject_id:
                raise SubjectInvalidError("subject_missing", subject.reason)
            # a subject that is no longer live stops (design §12.3); nothing to search for it
            self._complete(row, INCOMPLETE, "subject_not_live", {})
            return "stopped"
        if row["state"] in (PENDING, WAITING_INDEX, INDEXED) or row.get("searched_run") != row["run"]:
            self._index_own(row, subject)
            self._search(row, subject)
        return self._verify(row, subject)

    def _index_own(self, row: dict[str, Any], subject: Subject) -> None:
        own_role = ROLE_OF_KIND[row["kind"]]
        res = self.indexer.ensure(subject)
        ptr = self.gens.active(subject.spec_digest, own_role, fresh=True)
        if ptr is None:
            raise NoActiveGenerationError(subject.spec_digest, own_role)
        if res.applied.get(ptr.generation) != res.seq:
            raise WaitingIndexError("missing", f"own subject not yet applied to ACTIVE g{ptr.generation}")
        self._lwt(row, {"state": INDEXED, "waiting_reason": None})
        self.hook("matcher.after_indexed", subject=subject.subject_id, kind=row["kind"], run=row["run"])

    def _query_vectors(self, subject: Subject) -> Vectors:
        return self.p.artifacts.load_subject(subject)  # IntegrityError -> the subject's own data: FAILED

    def _search(self, row: dict[str, Any], subject: Subject) -> None:
        kind, spec = row["kind"], subject.spec_digest
        try:
            query = self._query_vectors(subject)
        except IntegrityError as exc:
            raise SubjectInvalidError("artifact_corrupt", str(exc)) from exc
        except ObjectMissingError as exc:
            raise SubjectInvalidError("artifact_missing", str(exc)) from exc
        cache: dict[str, bool] = {}
        skip: set[int] = set()
        hit_excluded = None
        rev = None
        if kind == KIND_B and self.lowinfo is not None:
            rev = self.lowinfo.observe(spec)
            skip = set(self.lowinfo.usable_flags(spec, subject.subject_id, cache))  # flagged segments do not query
        elif kind == KIND_A and self.lowinfo is not None:
            hit_excluded = self.lowinfo.hit_excluded(spec, cache, {})
            rev = self.lowinfo.current_rev(spec)
        policy = policy_for(self.p, kind, row.get("cap_override"))
        exclude = subject.content_id  # never a candidate of its own content, in any direction (design §6.3, N-13)
        out = self.searcher.search(spec, kind, query, policy, skip_query=skip, exclude_key=exclude,
                                   hit_excluded=hit_excluded)
        g, r = out.pointer.generation, int(row["run"])
        for rank, cand in enumerate(out.candidates):
            self.db.run("INSERT INTO {ks}.match_candidates (subject_id, spec_digest, kind, generation, run, rank, "
                        "other_id, other, hits, best_score, state, attempts, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,"
                        "'PENDING',0,?)", row["subject_id"], spec, kind, g, r, rank, cand.key,
                        json.dumps({**cand.payload, "point_id": cand.point_id}, sort_keys=True), len(cand.segments),
                        cand.best, self.p.now())
        stats = {**out.stats, "run": r, "target_set_rev": rev, "qdrant_version": self._qdrant_version(),
                 "params_digest": self._gen_params_digest(spec, OPPOSITE[kind], g)}
        self.hook("matcher.before_searched", subject=row["subject_id"], kind=kind, run=r)
        self._lwt(row, {"state": SEARCHED, "searched_generation": g, "searched_run": r, "verify_cursor": 0,
                        "candidate_count": len(out.candidates), "ann_stats": json.dumps(stats, sort_keys=True),
                        "target_set_rev": rev, "attempt": 0})

    _qv: str | None = None

    def _qdrant_version(self) -> str:
        if self._qv is None:
            try:
                self._qv = self.p.index.version()
            except Exception:  # noqa: BLE001 - provenance only
                self._qv = "unknown"
        return self._qv

    def _gen_params_digest(self, spec: str, role: str, g: int) -> str | None:
        row = self.gens.row(spec, role, g)
        return None if row is None else row["params_digest"]

    def _verify(self, row: dict[str, Any], subject: Subject) -> str:
        kind, spec, r, g = row["kind"], subject.spec_digest, int(row["run"]), int(row["searched_generation"])
        stats = json.loads(row.get("ann_stats") or "{}")
        tech = self.techniques.get(self.p.settings.matching.technique_version)
        page = self.p.settings.matching.page_b if kind in (KIND_B, KIND_EQ) else self.p.settings.matching.cap_a
        cands = self.db.rows("SELECT * FROM {ks}.match_candidates WHERE subject_id = ? AND "
                             "spec_digest = ? AND kind = ? "
                             "AND generation = ? AND run = ?", row["subject_id"], spec, kind, g, r)
        cands.sort(key=lambda c: c["rank"])
        cursor = int(row.get("verify_cursor") or 0)
        query = None
        cache: dict[str, bool] = {}
        while cursor < len(cands):
            if query is None:
                try:
                    query = self._query_vectors(subject)
                except IntegrityError as exc:
                    raise SubjectInvalidError("artifact_corrupt", str(exc)) from exc
                except ObjectMissingError as exc:
                    raise SubjectInvalidError("artifact_missing", str(exc)) from exc
            batch = cands[cursor:cursor + page]
            self._lwt(row, {"state": VERIFYING})
            for c in batch:
                if c["state"] in ("VERIFIED", "SKIPPED", "DROPPED", "FAILED"):
                    continue
                self._candidate(row, subject, query, c, tech, stats, cache)
            cursor += len(batch)
            self.hook("matcher.after_page", subject=row["subject_id"], kind=kind, run=r, cursor=cursor)
            self._lwt(row, {"verify_cursor": cursor})
        cands = self.db.rows("SELECT state, reason FROM {ks}.match_candidates WHERE subject_id = ? AND spec_digest = ? "
                             "AND kind = ? AND generation = ? AND run = ?", row["subject_id"], spec, kind, g, r)
        failed = sum(1 for c in cands if c["state"] == "FAILED")
        reason = None
        if stats.get("saturated"):
            reason = f"saturated_segments={stats['saturated']}"
        elif stats.get("truncated"):
            reason = f"candidate_cap truncated={stats['truncated']}"
        elif failed:
            reason = f"verification_failed={failed}"
        self.hook("matcher.before_completion_lwt", subject=row["subject_id"], kind=kind, run=r)
        self._complete(row, INCOMPLETE if reason else DONE, reason, stats)
        return "done" if reason is None else f"incomplete:{reason}"

    def _complete(self, row: dict[str, Any], state: str, reason: str | None, stats: dict[str, Any]) -> None:
        sets: dict[str, Any] = {"state": state, "done_run": int(row["run"]), "done_state": state,
                                "incomplete_reason": reason, "next_attempt_at": None, "lease_until": None}
        if state == DONE:
            sets["done_ok_run"] = int(row["run"])
        else:
            metrics.P10_INCOMPLETE.labels(kind=row["kind"], reason=(reason or "").split("=")[0].split(" ")[0]).inc()
        self._lwt(row, sets)
        self.tasks.due_clear(row["subject_id"], row["spec_digest"], row["kind"])
        self._log(row, "run_finished", state=state, reason=reason, candidates=row.get("candidate_count"))

    # --- one candidate ---

    def _cand_state(self, row: dict[str, Any], c: dict[str, Any], state: str, **cols: Any) -> None:
        sets = {"state": state, "updated_at": self.p.now(), **cols}
        cql = ", ".join(f"{k} = ?" for k in sets)
        self.db.run(f"UPDATE {{ks}}.match_candidates SET {cql} WHERE subject_id = ? AND spec_digest = ? "  # noqa: S608
                    "AND kind = ? AND generation = ? AND run = ? AND rank = ?", *sets.values(), row["subject_id"],
                    row["spec_digest"], row["kind"], c["generation"], c["run"], c["rank"])

    def _candidate(self, row: dict[str, Any], subject: Subject, query: Vectors, c: dict[str, Any], tech: Technique,
                   stats: dict[str, Any], cache: dict[str, bool]) -> None:
        kind, spec = row["kind"], subject.spec_digest
        other = json.loads(c["other"])
        role = OPPOSITE[kind]
        sid_field = "trep" if role == TARGET else "rep"
        # (i) the point is what its payload says (else foreign/orphan)
        if other.get("point_id") != point_id(other.get(sid_field) or "", int(other.get("seq") or -1),
                                             int(other.get("k") or -1)) or other.get("spec") != spec:
            self._cand_state(row, c, "DROPPED", reason="foreign_point")
            metrics.P10_INDEX_STALE.labels(role=role).inc()
            return
        # (ii) authoritative liveness (QUORUM read; a read failure propagates: the task retries)
        if role == TARGET:
            o = self.p.target_subject(other["tid"], int(other["tver"]), spec)
        else:
            o = self.p.media_subject(other["cid"], spec)
        if not o.live or o.subject_id != other.get(sid_field):
            self._cand_state(row, c, "DROPPED", reason="subject_not_live")
            metrics.P10_INDEX_STALE.labels(role=role).inc()
            if o.subject_id:
                self.indexer.write_intent(o)  # the delete repair is a new desired state (DELETE)
            return
        entry = self.indexer.entry(spec, role, o.bucket, o.subject_id, serial=False)
        if entry is not None and int(other.get("seq") or 0) < int(entry["seq"]):
            metrics.P10_STALE_CANDIDATES.labels(role=role).inc()  # counted; the live subject is still valid
        media, target = (subject, o) if kind == KIND_A else (o, subject) if kind == KIND_B else (subject, o)
        if kind != KIND_EQ:
            mid = str(MatchId.of(RepresentationId(media.subject_id), RepresentationId(target.subject_id),
                                 tech.name, tech.version))
            if self.committer.decided(mid):
                self._cand_state(row, c, "SKIPPED", reason="already_decided", match_id=mid)
                return
        # (iii) the other side's artifact, SHA-verified; a corrupt candidate fails only itself
        attempts = int(c.get("attempts") or 0)
        try:
            ov = self.p.artifacts.load_subject(o)
        except IntegrityError as exc:
            log.error(json.dumps({"event": "candidate_artifact_corrupt", "subject": o.subject_id, "error": str(exc)}))
            metrics.P10_VERIFY_FAILURES.labels(kind=kind, reason="artifact_corrupt").inc()
            self._cand_state(row, c, "FAILED", reason="artifact_corrupt", attempts=attempts + 1)
            return
        except (StorageUnavailableError, ObjectMissingError) as exc:
            reason = "artifact_unavailable"
            state = "FAILED" if attempts + 1 >= self.p.settings.matching.verify_retries else "PENDING"
            metrics.P10_VERIFY_FAILURES.labels(kind=kind, reason=reason).inc()
            self._cand_state(row, c, state, reason=f"{reason}: {exc}"[:200], attempts=attempts + 1)
            if state == "PENDING":
                raise StorageUnavailableError(f"candidate artifact unavailable: {exc}") from exc
            return
        mv, tv = (query, ov) if kind == KIND_A else (ov, query) if kind == KIND_B else (query, ov)
        t0 = time.monotonic()
        if kind == KIND_EQ:
            a, b, va, vb = (media, target, mv, tv) if media.content_id <= target.content_id else (target, media, tv, mv)
            eq = equivalence(va, vb, tech.params)
            dt = time.monotonic() - t0
            metrics.P10_VERIFY_SECONDS.labels(kind=kind).observe(dt)
            if dt > self.p.settings.matching.verify_timeout_s:
                self._cand_state(row, c, "FAILED", reason="verification_timeout", attempts=attempts + 1)
                return
            if eq.linked:
                prov = self._provenance(row, subject, o, tech, eq.verification, stats, c, dt)
                self.committer.link(a, b, tech, eq.evidence(), prov)
            self._cand_state(row, c, "VERIFIED", outcome="linked" if eq.linked else "not_equivalent")
            return
        excluded = frozenset()
        if self.lowinfo is not None:
            excluded = self.lowinfo.usable_flags(spec, target.subject_id, cache)
        res = verify(mv, tv, tech.params, excluded)
        dt = time.monotonic() - t0
        metrics.P10_VERIFY_SECONDS.labels(kind=kind).observe(dt)
        if dt > self.p.settings.matching.verify_timeout_s:
            state = "FAILED" if attempts + 1 >= self.p.settings.matching.verify_retries else "PENDING"
            self._cand_state(row, c, state, reason="verification_timeout", attempts=attempts + 1)
            metrics.P10_VERIFY_FAILURES.labels(kind=kind, reason="timeout").inc()
            return
        if res.verdict is None:
            metrics.P10_DECISIONS.labels(kind=kind, verdict="no_match").inc()
            self._cand_state(row, c, "VERIFIED", outcome="no_match")
            return
        prov = self._provenance(row, subject, o, tech, res, stats, c, dt)
        mid, _ = self.committer.commit(media, target, tech, res, prov)
        self._cand_state(row, c, "VERIFIED", outcome=res.verdict, match_id=mid)

    def _provenance(self, row: dict[str, Any], subject: Subject, other: Subject, tech: Technique, res: Verification,
                    stats: dict[str, Any], c: dict[str, Any], seconds: float) -> dict[str, Any]:
        occ = [str(k) for k, v in (row.get("recovery_runs") or {}).items() if int(v) == int(row["run"])]
        ix = self.p.settings.index
        return {"kind": row["kind"], "spec_digest": subject.spec_digest, "subject": subject.subject_id,
                "other": other.subject_id, "media_artifact_sha256": res.inputs.get("media_sha256"),
                "target_artifact_sha256": res.inputs.get("target_sha256"), "verifier": tech.params.verifier,
                "verifier_params_digest": tech.params_digest, "technique": [tech.name, tech.version],
                "index_generation": stats.get("generation"), "index_params_digest": stats.get("params_digest"),
                "qdrant_version": stats.get("qdrant_version"), "target_set_rev": stats.get("target_set_rev"),
                "lowinfo_rule": "lowinfo/v1" if self.p.settings.matching.lowinfo_enabled else "disabled",
                "exclusion_digest": res.exclusion_digest, "excluded_segments": res.excluded,
                "run": int(row["run"]), "rerun_reason": row.get("rerun_reason"), "occurrence_ids": occ,
                "calibration_id": tech.calibration_id, "calibration_status": tech.status,
                "ann": {**(stats.get("policy") or {}), "hnsw_ef": ix.search_ef, "quantization": "int8",
                        "rescore": True, "oversampling": ix.oversampling, "metric": "dot (cosine of unit vectors)"},
                "query_segments": stats.get("query_segments"), "queries": stats.get("queries"),
                "candidates": stats.get("stored"), "rank": int(c["rank"]), "candidate_hits": int(c["hits"] or 0),
                "saturated": stats.get("saturated"), "truncated": stats.get("truncated"),
                "search_complete": not stats.get("saturated") and not stats.get("truncated"),
                "verification_seconds": round(seconds, 4), "verdict": res.verdict, "score": res.score,
                "matcher_instance": self.p.instance, "decided_at": self.p.now().isoformat()}

    def _log(self, row: dict[str, Any], event: str, **kw: Any) -> None:
        log.info(json.dumps({"event": event, "subject": row["subject_id"], "kind": row["kind"],
                             "spec": row["spec_digest"][:19], "run": row.get("run"),
                             "generation": row.get("searched_generation"), **kw}, default=str))


def media_kinds(p10: P10) -> list[str]:
    return [KIND_A] + ([KIND_EQ] if p10.settings.matching.equivalence_enabled else [])


def kinds_of(s: Subject, p10: P10) -> list[str]:
    return media_kinds(p10) if s.role == MEDIA else [KIND_B]
