"""Low-information target segments ``lowinfo/v1`` (design §7.3; P10-D8, corrected C-2, C-9, C-13, C-14).

Rule: a **target** segment is low-information if it has cosine ≥ 0.95 with a
segment of a target of a *different* ``target_id`` in the authoritative
target set. Media segments are never classified.

Records ``lowinfo_class(spec, trep)``: ``rev``, ``rule``, ``status``
(COMPLETE/PARTIAL), the flagged segment set and up to 4 **witnesses** per
flagged segment (``trep|tid|tver`` of other targets). A witness is recorded
only after its cosine was recomputed on the SHA-verified artifact vectors of
both targets (C-13); the Qdrant score only nominates it. Writes are fenced:
``IF NOT EXISTS`` / ``IF rev < :r`` for a full record, ``IF rev = :read`` for
an incremental change. A recomputation at the *same* revision may replace
the record (``IF rev <= :r``): it is a deterministic function of the same
authoritative set; an older revision can never overwrite a newer one.

**Usable flag — the only way evidence is ever suppressed:** (1)
``matching.lowinfo_enabled``; (2) a COMPLETE record of this rule with the bit
set; (3) at least one witness authoritatively live *now* (QUORUM point reads
of ``target_representations`` x ``targets``, the §4.2 predicate; cached per
task). Anything else — missing/PARTIAL record, dead witnesses, a read
failure (the task retries) — leaves the segment **eligible**. Payload
``lowinfo`` is only a hint that the check is worth doing.

Status 2026-10-18: implemented; **not enabled** — its I-1 adoption gate did
not pass (validation §13.3).
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

import numpy as np

from fingerprinter import metrics
from fingerprinter.db import SERIAL
from fingerprinter.errors import StorageUnavailableError
from fingerprinter.generations import Generations
from fingerprinter.p10ctx import P10, Subject, target_live
from fingerprinter.vindex import TARGET, IndexServiceError, SearchRequest

RULE = "lowinfo/v1"
COMPLETE, PARTIAL = "COMPLETE", "PARTIAL"
MAX_WITNESSES = 4
MAX_PAGES = 8   # exact queries per segment while enumerating past a truncated (size-L) result
log = logging.getLogger("fingerprinter.lowinfo")


def witness_ref(s: Subject) -> str:
    return f"{s.subject_id}|{s.target_id}|{s.target_version}"


def parse_witness(ref: str) -> tuple[str, str, int]:
    trep, tid, tver = ref.split("|")
    return trep, tid, int(tver)


class LowInfo:
    def __init__(self, p10: P10, gens: Generations) -> None:
        self.p = p10
        self.db = p10.db
        self.gens = gens
        self.indexer: Any = None
        self.tasks: Any = None

    @property
    def enabled(self) -> bool:
        return self.p.settings.matching.lowinfo_enabled

    # --- records --------------------------------------------------------------------

    def record(self, spec: str, trep: str) -> dict[str, Any] | None:
        return self.db.one("SELECT * FROM {ks}.lowinfo_class WHERE spec_digest = ? AND target_representation_id = ?",
                           spec, trep, level=SERIAL)

    def records(self, spec: str) -> list[dict[str, Any]]:
        return self.db.rows("SELECT * FROM {ks}.lowinfo_class WHERE spec_digest = ?", spec)

    def record_flags(self, spec: str, trep: str) -> tuple[int, set[int]] | None:
        """(rev, flags) of a COMPLETE record — the derived payload copy (never trusted for exclusion)."""
        r = self.record(spec, trep)
        if r is None or r.get("rule") != RULE or r.get("status") != COMPLETE:
            return None
        return int(r["rev"]), {int(x) for x in (r.get("flags") or ())}

    # --- the usable-flag check (conditions 1-3) -----------------------------------------------

    def witness_live(self, spec: str, ref: str, cache: dict[str, bool]) -> bool:
        if ref in cache:
            return cache[ref]
        trep, tid, tver = parse_witness(ref)
        try:
            binding = self.db.one("SELECT * FROM {ks}.target_representations WHERE target_id = ? AND version = ? "
                                  "AND spec_digest = ?", tid, tver, spec)
            target = self.db.one("SELECT state FROM {ks}.targets WHERE target_id = ? AND version = ?", tid, tver)
        except StorageUnavailableError:
            metrics.P10_LOWINFO_WITNESS.labels(result="error").inc()
            raise  # the task retries: never exclude on an unknown (L-16)
        live = target_live(binding, target) and (binding or {}).get("target_representation_id") == trep
        metrics.P10_LOWINFO_WITNESS.labels(result="live" if live else "dead").inc()
        cache[ref] = live
        return live

    def usable_flags(self, spec: str, trep: str, cache: dict[str, bool]) -> frozenset[int]:
        """The exclusion set of one target subject at check time (may be empty)."""
        if not self.enabled:
            return frozenset()
        r = self.record(spec, trep)
        if r is None or r.get("rule") != RULE or r.get("status") != COMPLETE:
            return frozenset()
        out = set()
        witnesses = r.get("witnesses") or {}
        for seg in r.get("flags") or ():
            if any(self.witness_live(spec, w, cache) for w in witnesses.get(seg, ())):
                out.add(int(seg))
        return frozenset(out)

    def hit_excluded(self, spec: str, cache: dict[str, bool], flags_cache: dict[str, frozenset[int]]):
        """Direction A per-hit predicate (C-14h): a hit on a target segment with a usable flag does not vote."""
        def check(payload: dict[str, Any]) -> bool:
            if not self.enabled or not payload.get("lowinfo"):
                return False  # payload false ⇒ eligible without a read (a stale payload can only reduce exclusion)
            trep = payload.get("trep")
            if trep not in flags_cache:
                flags_cache[trep] = self.usable_flags(spec, trep, cache)
            return int(payload.get("k", -1)) in flags_cache[trep]
        return check

    # --- target set revision -------------------------------------------------------------------

    def members(self, spec: str) -> list[Subject]:
        out = []
        for r in self.db.stream("SELECT target_id, version, spec_digest FROM {ks}.target_representations"):
            if r["spec_digest"] == spec:
                s = self.p.target_subject(r["target_id"], r["version"], spec)
                if s.live and s.subject_id:
                    out.append(s)
        return out

    def observe(self, spec: str, members: list[Subject] | None = None) -> int:
        """Bump ``target_set_rev`` once per observed change of the members digest; request a refresh."""
        members = self.members(spec) if members is None else members
        digest = hashlib.sha256(",".join(sorted(m.subject_id for m in members)).encode()).hexdigest()
        for _ in range(20):
            row = self.db.one("SELECT * FROM {ks}.target_set_rev WHERE spec_digest = ?", spec, level=SERIAL)
            if row is None:
                if self.db.lwt("INSERT INTO {ks}.target_set_rev (spec_digest, rev, members_digest, updated_at) "
                               "VALUES (?,1,?,?) IF NOT EXISTS", spec, digest, self.p.now()).applied:
                    self.request(spec, reason="first observation", rev=1)
                    return 1
                continue
            if row["members_digest"] == digest:
                return int(row["rev"])
            r = int(row["rev"])
            if self.db.lwt("UPDATE {ks}.target_set_rev SET rev = ?, members_digest = ?, updated_at = ? "
                           "WHERE spec_digest = ? IF rev = ?", r + 1, digest, self.p.now(), spec, r).applied:
                self.request(spec, reason="members changed", rev=r + 1)
                return r + 1
        raise StorageUnavailableError("target_set_rev did not converge")

    def current_rev(self, spec: str) -> int:
        row = self.db.one("SELECT rev FROM {ks}.target_set_rev WHERE spec_digest = ?", spec)
        return int(row["rev"]) if row else 0

    def request(self, spec: str, *, reason: str, rev: int | None = None, full: bool = False) -> None:
        rev = rev or self.current_rev(spec)
        self.db.lwt("INSERT INTO {ks}.lowinfo_refresh (spec_digest, requested_rev, done_rev, full_requested, epoch, "
                    "state, attempts, requested_at) VALUES (?,?,0,?,0,'PENDING',0,?) IF NOT EXISTS", spec, rev, full,
                    self.p.now())
        row = self.db.one("SELECT * FROM {ks}.lowinfo_refresh WHERE spec_digest = ?", spec, level=SERIAL)
        if row and (int(row["requested_rev"] or 0) < rev or (full and not row["full_requested"])):
            self.db.lwt("UPDATE {ks}.lowinfo_refresh SET requested_rev = ?, full_requested = ?, requested_at = ? "
                        "WHERE spec_digest = ? IF requested_rev = ?", max(rev, int(row["requested_rev"] or 0)),
                        bool(full or row["full_requested"]), self.p.now(), spec, row["requested_rev"])
        log.info(json.dumps({"event": "lowinfo_refresh_requested", "spec": spec[:19], "rev": rev, "reason": reason}))

    # --- classification -----------------------------------------------------------------------

    def classify(self, spec: str, t: Subject, rev: int) -> dict[str, Any]:
        """Full classification of one target at revision ``rev`` (exact searches on the active target
        generation, ``exact`` + ``quantization.ignore`` + ``score_threshold``; witnesses confirmed on
        artifacts). Returns {flags, witnesses, status, truncated}.

        Completeness (design §7.3, C-9): a query returning exactly ``L`` hits is truncated. Such a segment
        is enumerated further with the target ids already seen added to ``must_not`` (each page excludes
        at least one more target, so a page shorter than ``L`` proves every remaining copy was seen), until
        it has ``MAX_WITNESSES`` witnesses or the enumeration is complete. A segment still truncated after
        ``MAX_PAGES`` makes the record PARTIAL (it then excludes nothing)."""
        m = self.p.settings.matching
        ptr = self.gens.active(spec, TARGET, fresh=True)
        if ptr is None:
            raise StorageUnavailableError("no ACTIVE target generation for classification")
        vec = self.p.artifacts.load_subject(t)

        def request(v: np.ndarray, tids: list[str]) -> SearchRequest:
            return SearchRequest(vector=v, limit=m.lowinfo_limit, exact=True, ignore_quantization=True,
                                 score_threshold=m.lowinfo_threshold,
                                 must_not=[{"key": "tid", "match": {"any": tids}}])

        def search(reqs: list[SearchRequest]) -> list[list[Any]]:
            return self.p.index.search_batch(ptr.collection, reqs, hnsw_ef=None, oversampling=1.0,
                                             timeout_s=m.search_timeout_s)

        results = search([request(v, [t.target_id]) for v in vec.vectors])
        flags: set[int] = set()
        witnesses: dict[int, list[str]] = {}
        truncated = 0
        others: dict[str, Subject] = {}
        for k, v, hits in zip(vec.segment_index, vec.vectors, results, strict=True):
            chosen: list[str] = []
            seen_tid: set[str] = set()
            excluded = [t.target_id]
            for page in range(MAX_PAGES):
                for h in sorted(hits, key=lambda x: (-x.score, x.id)):
                    pl = h.payload
                    if pl.get("tid") == t.target_id or pl.get("tid") in seen_tid:
                        continue
                    o = others.get(pl.get("trep"))
                    if o is None:
                        o = self.p.target_subject(pl["tid"], int(pl["tver"]), spec)
                        others[pl["trep"]] = o
                    if not o.live or o.subject_id != pl.get("trep"):
                        continue
                    ov = self.p.artifacts.load_subject(o)
                    idx = np.where(ov.segment_index == int(pl["k"]))[0]
                    if not idx.size:
                        continue
                    cos = float(v.astype(np.float64) @ ov.vectors[idx[0]].astype(np.float64))
                    if cos >= m.lowinfo_threshold:  # confirmed on SHA-verified artifacts (C-13)
                        chosen.append(witness_ref(o))
                        seen_tid.add(pl["tid"])
                    if len(chosen) >= MAX_WITNESSES:
                        break
                if len(chosen) >= MAX_WITNESSES or len(hits) < m.lowinfo_limit:
                    break                       # enough witnesses, or every copy above the threshold seen
                metrics.P10_LOWINFO_TRUNCATED.inc()
                if page == MAX_PAGES - 1:
                    truncated += 1              # enumeration not completed: the record cannot be COMPLETE
                    break
                excluded += sorted({str(h.payload.get("tid")) for h in hits} - set(excluded))
                hits = search([request(v, excluded)])[0]
            if chosen:
                flags.add(int(k))
                witnesses[int(k)] = chosen
        status = COMPLETE if not truncated else PARTIAL
        self._write_full(spec, t, rev, flags, witnesses, status)
        return {"flags": sorted(flags), "witnesses": witnesses, "status": status, "truncated": truncated}

    def _write_full(self, spec: str, t: Subject, rev: int, flags: set[int], witnesses: dict[int, list[str]],
                    status: str) -> bool:
        now = self.p.now()
        res = self.db.lwt("INSERT INTO {ks}.lowinfo_class (spec_digest, target_representation_id, target_id, version, "
                          "rev, rule, status, flags, witnesses, computed_at) VALUES (?,?,?,?,?,?,?,?,?,?) "
                          "IF NOT EXISTS", spec, t.subject_id, t.target_id, t.target_version, rev, RULE, status,
                          flags, witnesses, now)
        if res.applied:
            return True
        return self.db.lwt("UPDATE {ks}.lowinfo_class SET rev = ?, rule = ?, status = ?, flags = ?, witnesses = ?, "
                           "computed_at = ? WHERE spec_digest = ? AND target_representation_id = ? IF rev <= ?",
                           rev, RULE, status, flags, witnesses, now, spec, t.subject_id, rev).applied

    def refresh(self, spec: str) -> dict[str, Any]:
        """Bring records to the latest observed revision: classify additions, drop removed witnesses,
        recompute segments that lost their last witness; payload copies follow via intents.

        A failure (index or storage unavailable) is counted on the refresh row; after
        ``matching.lowinfo_max_attempts`` the row is BLOCKED (visible, ``last_error``) and still retried.
        Records keep their old content and are used only through conditions 1-3 meanwhile."""
        row = self.db.one("SELECT * FROM {ks}.lowinfo_refresh WHERE spec_digest = ?", spec, level=SERIAL)
        if row is None:
            return {"state": "idle"}
        rev = int(row["requested_rev"] or 0)
        if rev <= int(row["done_rev"] or 0) and not row["full_requested"]:
            return {"state": "idle", "rev": rev}
        try:
            return self._refresh(spec, row, rev)
        except (IndexServiceError, StorageUnavailableError) as exc:
            self._failed(spec, row, exc)
            raise

    def _failed(self, spec: str, row: dict[str, Any], exc: Exception) -> None:
        attempts = int(row.get("attempts") or 0) + 1
        state = "BLOCKED" if attempts >= self.p.settings.matching.lowinfo_max_attempts else "PENDING"
        self.db.run("UPDATE {ks}.lowinfo_refresh SET attempts = ?, state = ?, last_error = ?, updated_at = ? "
                    "WHERE spec_digest = ?", attempts, state, f"{type(exc).__name__}: {exc}"[:300], self.p.now(),
                    spec)
        metrics.P10_LOWINFO_REFRESH_FAILURES.inc()
        log.warning(json.dumps({"event": "lowinfo_refresh_failed", "spec": spec[:19], "attempts": attempts,
                                "state": state, "error": str(exc)[:200]}))

    def _unindexed(self, spec: str, members: dict[str, Subject]) -> int:
        """Members whose points are not in the queried (ACTIVE target) generation yet: an exact
        enumeration over that generation cannot see them, so a step that ran now is not complete."""
        ptr = self.gens.active(spec, TARGET, fresh=True)
        if ptr is None or self.indexer is None:
            return len(members)
        n = 0
        for m in members.values():
            e = self.indexer.entry(spec, TARGET, m.bucket, m.subject_id)
            if e is None or e["desired_op"] != "UPSERT" or (e["applied"] or {}).get(ptr.generation) is None:
                n += 1
        return n

    def _refresh(self, spec: str, row: dict[str, Any], rev: int) -> dict[str, Any]:
        members = {m.subject_id: m for m in self.members(spec)}
        unindexed = self._unindexed(spec, members)
        recs = {r["target_representation_id"]: r for r in self.records(spec)}
        changed: set[str] = set()
        cleared: set[str] = set()
        partial: set[str] = set()
        for trep, r in recs.items():  # removals: exact enumeration from the witness lists (no ANN)
            if trep not in members:
                continue
            wit = {int(k): list(v) for k, v in (r.get("witnesses") or {}).items()}
            new_w = {k: [w for w in v if parse_witness(w)[0] in members] for k, v in wit.items()}
            if new_w != wit:
                t = members[trep]
                res = self.classify(spec, t, rev)
                if set(res["flags"]) < {int(x) for x in (r.get("flags") or ())}:
                    cleared.add(trep)
                if res["status"] != COMPLETE:
                    partial.add(trep)
                changed.add(trep)
        for trep, t in members.items():  # additions: classify every member without a current record
            r = recs.get(trep)
            if r is None or int(r["rev"] or 0) < rev or r.get("status") != COMPLETE or row["full_requested"]:
                if self.classify(spec, t, rev)["status"] != COMPLETE:
                    partial.add(trep)
                changed.add(trep)
        for trep in changed:  # payload copies through the mutation protocol (desired_ver carries the rev)
            if self.indexer is not None:
                self.indexer.write_intent(members[trep])
        for trep in cleared:  # a cleared flag may have suppressed evidence: re-cover the owner's pairs
            if self.tasks is not None:
                self.tasks.rerun(members[trep], "target_vs_media", reason="lowinfo_flag_cleared", actor="lowinfo")
        if partial or unindexed:
            # the step's enumeration was not complete (a truncated query, or members whose points the
            # queried generation does not hold yet): done_rev is not advanced, a full recomputation is
            # requested; the records written meanwhile only ever miss flags (fail-closed)
            state = "TRUNCATED" if partial else "WAITING_INDEX"
            self.db.lwt("UPDATE {ks}.lowinfo_refresh SET full_requested = true, state = ?, attempts = 0, "
                        "last_error = null, updated_at = ? WHERE spec_digest = ? IF requested_rev = ?",
                        state, self.p.now(), spec, rev)
            log.warning(json.dumps({"event": "lowinfo_step_incomplete", "spec": spec[:19], "rev": rev,
                                    "partial_records": len(partial), "unindexed_members": unindexed}))
            return {"state": state.lower(), "rev": rev, "changed": len(changed), "cleared": len(cleared),
                    "partial": len(partial), "unindexed": unindexed}
        self.db.lwt("UPDATE {ks}.lowinfo_refresh SET done_rev = ?, full_requested = false, state = 'DONE', "
                    "attempts = 0, last_error = null, updated_at = ? WHERE spec_digest = ? IF requested_rev = ?",
                    rev, self.p.now(), spec, rev)
        return {"state": "done", "rev": rev, "changed": len(changed), "cleared": len(cleared)}

    def full_recompute(self, spec: str) -> dict[str, int]:
        """Backstop: recompute every record from scratch and report differences (``lowinfo_backstop_diff``)."""
        rev = max(self.current_rev(spec), 1)
        before = {r["target_representation_id"]: r for r in self.records(spec)}
        diff = {"missing_flag": 0, "extra_flag": 0, "missing_record": 0, "witness_dead": 0}
        cache: dict[str, bool] = {}
        for t in self.members(spec):
            old = before.get(t.subject_id)
            if old is None:
                diff["missing_record"] += 1
            for ws in ((old or {}).get("witnesses") or {}).values():
                if not any(self.witness_live(spec, w, cache) for w in ws):
                    diff["witness_dead"] += 1
            res = self.classify(spec, t, rev)
            new_flags = set(res["flags"])
            old_flags = {int(x) for x in ((old or {}).get("flags") or ())}
            diff["missing_flag"] += len(new_flags - old_flags) if old else 0
            diff["extra_flag"] += len(old_flags - new_flags)
            if self.indexer is not None:
                self.indexer.write_intent(t)
        for kind, n in diff.items():
            metrics.P10_LOWINFO_DIFF.labels(kind=kind).inc(n)
        return diff
