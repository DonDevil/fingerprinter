"""Wiring of the P10 components and the P10 reconciler (design §3, §12.6; P10-D20).

``P10System`` builds every component from one ``P10`` context and connects
the cross-references (recovery repairs through the indexer; the indexer
reads lowinfo records; recovery bumps runs through the task store).

The reconciler (``matcher reconcile run``) is a P10 role separate from P9's
``reconcile run``. Each pass is idempotent and resumable from durable state:

| pass | does |
|---|---|
| intents | ``index_entries`` PENDING/BLOCKED older than the grace → apply; stale ``match_intents`` → resolve |
| scans | reps / targets backstop scans (§6.1) |
| verification | per searched or READY generation, at most once per ``verify_cycle_s`` |
| generations | DRAINING → RETIRED (grace + clean post-horizon pass or unauditable fallback); audits requested |
| repairs | every ``index_repairs`` row not CLOSED+RESOLVED (§12.10) |
| lowinfo | refresh when enabled |
| capacity | the audit — the only path that lowers the ledger |
"""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import timedelta
from typing import Any

from fingerprinter.candidates import Searcher
from fingerprinter.capacity import Capacity
from fingerprinter.confidence import Techniques
from fingerprinter.coord import utc
from fingerprinter.discovery import Discovery
from fingerprinter.errors import StorageUnavailableError
from fingerprinter.generations import DRAINING, READY, RETIRED, SEARCHED, Generations
from fingerprinter.indexing import BLOCKED, PENDING, Builder, Checker, Indexer, SubjectInvalidError, WaitingIndexError
from fingerprinter.lowinfo import LowInfo
from fingerprinter.matching import MatchCommitter, Matcher, TaskStore
from fingerprinter.p10ctx import BUCKETS, P10
from fingerprinter.p10db import migrate_p10
from fingerprinter.recovery import Recovery
from fingerprinter.vindex import IndexServiceError

log = logging.getLogger("fingerprinter.p10")
GUARD = "p10-index"


class P10System:
    def __init__(self, p10: P10) -> None:
        self.p = p10
        self.gens = Generations(p10)
        self.capacity = Capacity(p10)
        self.recovery = Recovery(p10, self.gens)
        self.indexer = Indexer(p10, self.gens, self.capacity, self.recovery)
        self.builder = Builder(p10, self.gens, self.indexer, self.capacity)
        self.checker = Checker(p10, self.gens, self.indexer, self.recovery)
        self.lowinfo = LowInfo(p10, self.gens)
        self.techniques = Techniques(p10)
        self.tasks = TaskStore(p10)
        self.committer = MatchCommitter(p10, self.techniques)
        self.searcher = Searcher(p10, self.gens)
        self.matcher = Matcher(p10, self.tasks, self.gens, self.indexer, self.searcher, self.techniques,
                               self.committer, self.lowinfo)
        self.discovery = Discovery(p10, self.indexer, self.tasks, self.lowinfo)
        self.recovery.indexer = self.indexer
        self.recovery.tasks = self.tasks
        self.indexer.lowinfo = self.lowinfo
        self.lowinfo.indexer = self.indexer
        self.lowinfo.tasks = self.tasks
        self._last: dict[str, float] = {}

    @classmethod
    def connect(cls, svc: Any, **kw: Any) -> P10System:
        return cls(P10.from_services(svc, **kw))

    # --- administration -------------------------------------------------------------------

    def migrate(self) -> None:
        migrate_p10(self.p.db)
        self.committer.seed_stream()
        self.capacity.ensure()

    def init_spec(self, spec: str, *, actor: str) -> None:
        """Index a spec: ``retire_guard(spec, 'p10-index')`` (P9's table, as designed) + generation partitions."""
        self.p.db.run("INSERT INTO {ks}.retire_guard (spec_digest, holder) VALUES (?, ?)", spec, GUARD)
        for role in ("media", "target"):
            self.gens.init_partition(spec, role)
        self.p.store.audit(actor, "index-init", {"spec": spec})

    def drop_spec(self, spec: str, *, actor: str) -> dict[str, int]:
        """``index drop --spec``: every generation RETIRED and deleted, intents removed, guard removed.
        Occurrence rows stay (they are resolved by the subject's retirement or later runs)."""
        out = {"generations": 0, "entries": 0}
        for role in ("media", "target"):
            _, gens = self.gens.partition(spec, role)
            for g, row in gens.items():
                if row["state"] != RETIRED and row.get("collection"):
                    self.p.index.delete_collection(row["collection"])
                self.p.db.run("UPDATE {ks}.index_generations SET state = 'RETIRED', updated_at = ? "
                              "WHERE spec_digest = ? "
                              "AND role = ? AND generation = ?", self.p.now(), spec, role, g)
                out["generations"] += 1
            self.p.db.run("UPDATE {ks}.index_generations SET active_generation = 0 WHERE spec_digest = ? AND role = ?",
                          spec, role)
            for b in range(BUCKETS):
                out["entries"] += len(self.indexer.entries(spec, role, b))
                self.p.db.run("DELETE FROM {ks}.index_entries WHERE spec_digest = ? AND role = ? AND bucket = ?",
                              spec, role, b)
        self.p.db.run("DELETE FROM {ks}.retire_guard WHERE spec_digest = ? AND holder = ?", spec, GUARD)
        self.p.store.audit(actor, "index-drop", {"spec": spec, **out})
        self.capacity.audit()
        return out

    def drop_failed_generation(self, spec: str, role: str, g: int, *, actor: str) -> dict[str, Any]:
        """``index drop --generation``: delete a FAILED generation's collection (kept for inspection until
        now); the next capacity audit then releases its holder row and points (design §12.4, §13.3)."""
        row = self.gens.row(spec, role, g)
        if row is None or row["state"] != "FAILED":
            raise ValueError(f"generation {g} is {None if row is None else row['state']}: only FAILED can be dropped")
        deleted = self.p.index.delete_collection(row["collection"])
        self.p.store.audit(actor, "index-drop-generation", {"spec": spec, "role": role, "generation": g})
        audit = self.capacity.audit()
        return {"collection_deleted": deleted, "ledger_before": audit.reserved_before, "ledger_after": audit.bound,
                "holders_released": audit.holders_removed}

    def rebuild(self, spec: str, role: str, *, promote: bool, wait_boundary: bool = True) -> dict[str, Any]:
        res = self.builder.build(spec, role, wait_boundary=wait_boundary)
        if promote and res.get("state") == READY:
            res["promoted"] = self.gens.promote(spec, role, res["generation"])
        return res

    def status(self) -> dict[str, Any]:
        out: dict[str, Any] = {"specs": {}, "capacity": self.capacity.read()}
        for spec in self.p.indexed_specs():
            spec_out: dict[str, Any] = {}
            for role in ("media", "target"):
                static, gens = self.gens.partition(spec, role, serial=False)
                states: dict[str, int] = {}
                for b in range(BUCKETS):
                    for e in self.indexer.entries(spec, role, b):
                        key = e["state"] if e["state"] != BLOCKED else f"BLOCKED:{e.get('blocked_reason')}"
                        states[key] = states.get(key, 0) + 1
                spec_out[role] = {"active_generation": static.get("active_generation"),
                                  "generations": {g: {k: r.get(k) for k in ("state", "collection", "detect_count",
                                                                            "suspect_since", "audit_state",
                                                                            "manifest_digest", "actual_count")}
                                                  for g, r in gens.items()},
                                  "entries": states}
            o, u = self.recovery.counters(spec)
            spec_out["recovery_open"], spec_out["recovery_unresolved"] = o, u
            out["specs"][spec] = spec_out
        return out

    # --- the reconciler ----------------------------------------------------------------------

    def _due(self, name: str, every_s: float) -> bool:
        now = time.monotonic()
        if now - self._last.get(name, -1e18) >= every_s:
            self._last[name] = now
            return True
        return False

    def reconcile_once(self, *, force: bool = False) -> dict[str, Any]:
        rc, ix = self.p.settings.matcher_reconcile, self.p.settings.index
        report: dict[str, Any] = {}

        def guarded(name: str, fn: Any) -> None:
            try:
                report[name] = fn()
            except (StorageUnavailableError, IndexServiceError) as exc:
                report[name] = {"error": f"{type(exc).__name__}: {exc}"[:300]}
                log.warning(json.dumps({"event": "p10_reconcile_pass_unavailable", "pass": name,
                                        "error": str(exc)[:200]}))

        guarded("hints", self.discovery.read_hints)
        guarded("intents", self.intents_pass)
        specs = self.p.indexed_specs()
        if force or self.discovery.backstop_requested or self._due("reps", rc.reps_interval_s):
            self.discovery.backstop_requested = False
            guarded("scan_reps", lambda: {s[:19]: self.discovery.scan_reps(s) for s in specs})
        if force or self._due("targets", rc.targets_interval_s):
            guarded("scan_targets", self.discovery.scan_targets)
        if force or self._due("verify", ix.verify_cycle_s):
            guarded("verify", self.verify_pass)
        guarded("generations", self.generations_pass)
        guarded("repairs", lambda: {s[:19]: self.recovery.run_pass(s) for s in specs})
        if self.lowinfo.enabled:
            guarded("lowinfo", lambda: {s[:19]: self.lowinfo.refresh(s) for s in specs})
        if force or self._due("capacity", rc.capacity_audit_s):
            guarded("capacity", lambda: self.capacity.audit().__dict__)
        return report

    def intents_pass(self) -> dict[str, int]:
        out = {"applied": 0, "blocked_retried": 0, "match_intents": 0}
        grace = timedelta(seconds=self.p.settings.matcher_reconcile.intent_grace_s)
        now = self.p.now()
        for spec in self.p.indexed_specs():
            for role in ("media", "target"):
                for b in range(BUCKETS):
                    for e in self.indexer.entries(spec, role, b):
                        if e["state"] not in (PENDING, BLOCKED):
                            continue
                        if e.get("updated_at") and utc(e["updated_at"]) > now - grace:
                            continue
                        nxt = utc(e.get("next_attempt_at"))
                        if nxt is not None and nxt > now:
                            continue
                        try:
                            self.indexer.apply(spec, role, b, e["subject_id"])
                            out["applied"] += 1
                        except (WaitingIndexError, SubjectInvalidError, IndexServiceError):
                            out["blocked_retried"] += 1
        for row in self.committer.intents():
            if row.get("created_at") and utc(row["created_at"]) > now - grace:
                continue
            self.committer.resolve_intent(row)
            out["match_intents"] += 1
        return out

    def verify_pass(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for spec in self.p.indexed_specs():
            for role in ("media", "target"):
                _, gens = self.gens.partition(spec, role, serial=False)
                for g, row in gens.items():
                    if row["state"] in SEARCHED or row["state"] == READY:
                        out[f"{spec[:12]}:{role}:g{g}"] = self.checker.verify_generation(spec, role, g)
        return out

    def generations_pass(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for spec in self.p.indexed_specs():
            for role in ("media", "target"):
                _, gens = self.gens.partition(spec, role, serial=False)
                for g, row in gens.items():
                    if row.get("audit_state") in ("REQUESTED", "RUNNING"):
                        if self.p.index.collection_exists(row["collection"]):
                            out[f"audit:{role}:g{g}"] = self.recovery.audit_generation(spec, role, g)
                        else:
                            opened = self.recovery.unauditable_fallback(spec, role, g)
                            out[f"audit:{role}:g{g}"] = {"unauditable": opened}
                            self.gens.set_cols(spec, role, g, audit_state="DONE")
                    if row["state"] == DRAINING:
                        out[f"retire:{role}:g{g}"] = self.checker.retire_draining(spec, role, g)
        return out

    # --- service loops ------------------------------------------------------------------------

    def matcher_loop(self, stop: threading.Event, *, until_idle: bool = False) -> dict[str, int]:
        """``matcher run``: rounds of (rebuild the queue from ``match_due`` → workers drain it). With
        ``until_idle`` it returns after a round that processed nothing."""
        m = self.p.settings.matching
        self.matcher.check_policies()   # ConfigError: refuse to start rather than search with a lossy floor
        counts: dict[str, int] = {}
        lease_ms = int(m.task_lease_s * 1000)
        lock = threading.Lock()

        def worker(done: list[int]) -> None:
            token = f"{self.p.instance}:{threading.get_ident()}"
            while not stop.is_set():
                member = self.tasks.pop(token, lease_ms)
                if member is None:
                    return
                try:
                    out = self.matcher.run_member(member, token)
                except Exception as exc:
                    log.exception(json.dumps({"event": "task_crashed", "member": member}))
                    out = f"crashed:{type(exc).__name__}"
                with lock:
                    key = out.split(":")[0]
                    counts[key] = counts.get(key, 0) + 1
                    if key not in ("terminal", "not_due", "missing", "claim_lost", "crashed"):
                        done[0] += 1  # only progress keeps an until-idle loop going (a crash is logged, not
                        # progress: a task that crashes on every claim must not keep until_idle spinning)

        while not stop.is_set():
            self.tasks.schedule(horizon_s=0)
            done = [0]
            threads = [threading.Thread(target=worker, args=(done,), daemon=True) for _ in range(m.workers)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            if until_idle and done[0] == 0:
                return counts
            stop.wait(0 if until_idle else m.poll_interval_s)
        return counts
