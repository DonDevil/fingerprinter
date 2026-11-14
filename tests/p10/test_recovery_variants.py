"""Crash, uncertainty and duplicate variants of R-26, R-27 and R-29 (validation §9a.1-§9a.3) — real stack.

The base procedures are in ``test_recovery.py``; each variant here is a
separate run of the full procedure with one injected fault, ending in the
procedure's common final state. Crashes are raising hooks; an unknown LWT
outcome is a wrapper that lets Scylla apply the statement and then raises
``LwtUnknownError``; a process restart is a fresh ``P10System`` on the same
connections (every in-memory cache and timer discarded).
"""

from __future__ import annotations

import threading
import time
import uuid

import pytest

from fingerprinter import metrics
from fingerprinter.db import LwtUnknownError
from fingerprinter.p10ctx import KIND_A, KIND_B
from fingerprinter.p10system import P10System
from fingerprinter.recovery import CLOSED, OPEN, RERUN_PENDING, RESOLVED, Occurrence
from fingerprinter.vindex import TARGET, IndexUnavailableError, subject_seq_eq
from tests.p10.conftest import VECTORS
from tests.p10.test_flow import clip, film
from tests.p10.test_recovery import has_match, lose, occurrences

pytestmark = pytest.mark.p10_integration


class CrashError(Exception):
    pass


class Kill(BaseException):  # noqa: N818 - a process death: escapes every `except Exception` (kill -9)
    pass


@pytest.fixture(autouse=True)
def _need_vectors():
    if not (VECTORS / "l-pairs").exists():
        pytest.skip("cached L-PAIRS vectors not present")


@pytest.fixture
def world(p10):
    t = p10.register_target(*film("sintel"), title="Sintel")
    p10.bootstrap()
    p10.drive()
    return p10, t


def restart(p10) -> None:
    """Every P10 process restarted: a fresh system on the same connections (no in-memory state kept)."""
    p10.sys = P10System(p10.sys.p)


def unknown_once(monkeypatch, db, needle: str, *, after_apply: bool = True) -> list[bool]:
    """The first LWT whose CQL contains ``needle`` is executed, then reported as of unknown outcome."""
    real = db.lwt
    fired: list[bool] = []

    def lwt(cql, *args):
        if needle in cql and not fired:
            res = real(cql, *args)
            fired.append(res.applied)
            raise LwtUnknownError("injected: outcome unknown")
        return real(cql, *args)

    monkeypatch.setattr(db, "lwt", lwt)
    return fired


def occ_obj(row) -> Occurrence:
    return Occurrence(row["spec_digest"], row["role"], row["bucket"], row["subject_id"], row["generation"],
                      row["occurrence"], row["occurrence_id"], row)


def blocked_gauge() -> float:
    return metrics.P10_REPAIRS_BLOCKED.labels(role=TARGET)._value.get()


# --- R-26 (§9a.1): steps 1-9 with every crash, uncertainty and duplicate variant ----------------

R26_VARIANTS = ["base", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "d1", "d2"]


@pytest.mark.parametrize("variant", R26_VARIANTS)
def test_r26_full_procedure_variants(world, monkeypatch, variant) -> None:
    """R-26 a, steps 1-9 (§9a.1), one run per variant. Final state in every variant: one occurrence CLOSED
    RERUN_DONE, one (M, T) row, T at run 2, M at run 1; ``index_repairs_blocked`` raised in step 6 and
    cleared afterwards; the occurrence never absent after step 3 nor CLOSED before T's run 2 finished."""
    p10, (tid, ver, _) = world
    rc = p10.sys.p.settings.matcher_reconcile.model_copy(update={"repair_max_attempts": 1})
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"matcher_reconcile": rc})
    db = p10.s.db
    t = p10.target(tid, ver)
    g = lose(p10, t)                                                  # 1. non-protocol loss
    m = p10.ingest_media(*clip("lp-000-sintel-re-encode.mp4"))
    ms = p10.media(m)
    p10.sys.discovery.scan_reps(p10.spec)
    at_p1, release = threading.Event(), threading.Event()
    order: list[str] = []

    def p1(**ctx):
        if ctx["kind"] == KIND_A and ctx["subject"] == ms.subject_id:
            order.append("M_at_P1")
            at_p1.set()
            release.wait(60)

    p10.hooks.on("matcher.before_completion_lwt", p1)
    worker = threading.Thread(target=p10.run_matcher)
    worker.start()                                                     # 2. M blocks at P1
    assert at_p1.wait(90)
    # 3. one verification pass records the loss (variants c1, c2, d1 inject here)
    if variant == "c1":
        def die(**_):
            raise CrashError("verifier killed at P2")
        p10.hooks.on("verifier.after_count_before_obligation", die)
        with pytest.raises(CrashError):
            p10.sys.verify_pass()
        p10.hooks.on("verifier.after_count_before_obligation", lambda **_: None)
        assert occurrences(p10, t.subject_id) == []                    # nothing durable, T not healed
        seq = int(p10.sys.indexer.entry(p10.spec, TARGET, t.bucket, t.subject_id)["seq"])
        assert p10.index.count(p10.sys.gens.active(p10.spec, TARGET, fresh=True).collection,
                               subject_seq_eq(TARGET, t.subject_id, seq)) == 0
        p10.sys.verify_pass()                                          # restart re-counts T
    elif variant == "c2":
        fired = unknown_once(monkeypatch, db, "INSERT INTO {ks}.index_repairs")
        p10.sys.verify_pass()
        monkeypatch.undo()
        assert fired == [True]
    elif variant == "d1":
        ths = [threading.Thread(target=p10.sys.verify_pass) for _ in range(2)]
        for th in ths:
            th.start()
        for th in ths:
            th.join(60)
        p10.sys.verify_pass()
    else:
        p10.sys.verify_pass()
    occ = occurrences(p10, t.subject_id)
    assert len(occ) == 1 and occ[0]["occurrence"] == 1 and occ[0]["state"] == OPEN
    order.append("occurrence_durable")
    detect_before = int(p10.sys.gens.row(p10.spec, TARGET, g)["detect_count"] or 0)
    release.set()                                                      # 4. M's completion LWT
    worker.join(90)
    p10.hooks.clear()
    assert order == ["M_at_P1", "occurrence_durable"]
    assert p10.sys.tasks.row(ms.subject_id, p10.spec, KIND_A)["state"] == "DONE"     # 5. not gated
    assert not has_match(p10, m, tid)
    assert p10.sys.recovery.settled(ms.subject_id, p10.spec, [KIND_A])["status"] == "provisional"
    # 6. the repair write blocked (Qdrant 503): OPEN, attempts > 0, last_error; never closed or deleted
    real_upsert = p10.index.upsert

    def unavailable(name, points):
        if any(pt.payload.get("trep") == t.subject_id for pt in points):
            raise IndexUnavailableError("injected 503 for the repair write")
        return real_upsert(name, points)

    monkeypatch.setattr(p10.index, "upsert", unavailable)
    p10.sys.recovery.run_pass(p10.spec)
    row = occurrences(p10, t.subject_id)[0]
    assert row["state"] == OPEN and int(row["attempts"]) > 0 and "503" in (row["last_error"] or "")
    assert blocked_gauge() >= 1                                        # index_repairs_blocked raised
    monkeypatch.setattr(p10.index, "upsert", real_upsert)
    db.run("UPDATE {ks}.index_repairs SET next_attempt_at = null WHERE spec_digest = ? AND role = ? AND bucket = ? "
           "AND subject_id = ? AND generation = ? AND occurrence = 1", p10.spec, TARGET, row["bucket"],
           t.subject_id, g)
    # 7. repair, confirm, REPAIRED, bump T run 1 -> 2, RERUN_PENDING (variants c3, c4, c5, d2, c7)
    if variant == "c3":
        def die3(**_):
            raise CrashError("reconciler killed at P3")
        p10.hooks.on("reconciler.after_repair_write", die3)
        p10.sys.recovery.run_pass(p10.spec)                           # the crash is recorded on the row
        p10.hooks.clear()
        assert occurrences(p10, t.subject_id)[0]["state"] == OPEN
        db.run("UPDATE {ks}.index_repairs SET next_attempt_at = null WHERE spec_digest = ? AND role = ? AND "
               "bucket = ? AND subject_id = ? AND generation = ? AND occurrence = 1", p10.spec, TARGET,
               row["bucket"], t.subject_id, g)
        p10.sys.recovery.run_pass(p10.spec)                           # repair re-applied idempotently
    elif variant == "c4":
        def die4(**_):
            raise CrashError("reconciler killed at P4")
        p10.hooks.on("reconciler.after_bump_before_record", die4)
        p10.sys.recovery.run_pass(p10.spec)
        p10.hooks.clear()
        assert occurrences(p10, t.subject_id)[0]["state"] == "REPAIRED"
        assert p10.sys.tasks.row(t.subject_id, p10.spec, KIND_B)["run"] == 2
        db.run("UPDATE {ks}.index_repairs SET next_attempt_at = null WHERE spec_digest = ? AND role = ? AND "
               "bucket = ? AND subject_id = ? AND generation = ? AND occurrence = 1", p10.spec, TARGET,
               row["bucket"], t.subject_id, g)
        p10.sys.recovery.run_pass(p10.spec)                           # records recovery_runs[oid] = 2
    elif variant == "c5":
        fired = unknown_once(monkeypatch, db, "UPDATE {ks}.match_tasks SET run = ?")
        p10.sys.recovery.run_pass(p10.spec)
        monkeypatch.undo()
        assert fired == [True]
    elif variant == "d2":
        other = P10System(p10.sys.p)
        ths = [threading.Thread(target=p10.sys.recovery.run_pass, args=(p10.spec,)),
               threading.Thread(target=other.recovery.run_pass, args=(p10.spec,))]
        for th in ths:
            th.start()
        for th in ths:
            th.join(60)
    else:
        p10.sys.recovery.run_pass(p10.spec)
    row = occurrences(p10, t.subject_id)[0]
    tb = p10.sys.tasks.row(t.subject_id, p10.spec, KIND_B)
    assert row["state"] == RERUN_PENDING and row["required_run"] == {KIND_B: 2}, row
    assert tb["run"] == 2 and tb["recovery_runs"] == {row["occurrence_id"]: 2}
    assert int(p10.sys.gens.row(p10.spec, TARGET, g)["detect_count"] or 0) >= detect_before
    p10.sys.recovery.counters(p10.spec)
    assert blocked_gauge() == 0                                        # cleared after the repair
    if variant == "c7":
        restart(p10)
    # 8. T's run 2 held before its search: two reconciler passes keep it RERUN_PENDING
    p10.sys.recovery.run_pass(p10.spec)
    p10.sys.recovery.run_pass(p10.spec)
    assert occurrences(p10, t.subject_id)[0]["state"] == RERUN_PENDING
    # 9. T's run 2 (variant c6: its worker dies mid-VERIFYING, lease expiry, resume)
    if variant == "c6":
        real_cand = p10.sys.matcher._candidate
        died: list[int] = []

        def die6(row_, *a, **k):
            if row_["kind"] == KIND_B and not died:
                died.append(int(row_["run"]))
                raise CrashError("run-2 worker killed mid-VERIFYING")
            return real_cand(row_, *a, **k)

        monkeypatch.setattr(p10.sys.matcher, "_candidate", die6)
        p10.run_matcher()
        monkeypatch.setattr(p10.sys.matcher, "_candidate", real_cand)
        assert died == [2]
        assert p10.sys.tasks.row(t.subject_id, p10.spec, KIND_B)["state"] == "VERIFYING"
        assert occurrences(p10, t.subject_id)[0]["state"] == RERUN_PENDING
        time.sleep(p10.sys.p.settings.matching.task_lease_s + 0.5)
    for _ in range(8):
        p10.run_matcher()
        p10.sys.recovery.run_pass(p10.spec)
        if occurrences(p10, t.subject_id)[0]["state"] == CLOSED:
            break
        time.sleep(0.5)
    occ = occurrences(p10, t.subject_id)
    tb = p10.sys.tasks.row(t.subject_id, p10.spec, KIND_B)
    assert len(occ) == 1 and occ[0]["state"] == CLOSED and occ[0]["close_reason"] == "RERUN_DONE"
    assert occ[0]["resolution"] == RESOLVED and occ[0]["resolved_by"] == {KIND_B: 2}
    assert tb["run"] == 2 and tb["done_run"] == 2
    assert p10.sys.tasks.row(ms.subject_id, p10.spec, KIND_A)["run"] == 1
    pairs = [r for r in db.rows("SELECT * FROM {ks}.matches") if r["content_id"] == m and r["target_id"] == tid]
    assert len(pairs) == 1
    by_id = [r for r in db.rows("SELECT * FROM {ks}.matches_by_id") if r["match_id"] == pairs[0]["match_id"]]
    assert len(by_id) == 1
    assert pairs[0]["envelope"] is None and pairs[0]["published_at"] is None   # 2.1 uncalibrated: held, no envelope
    if variant == "d1":
        assert int(occ[0]["detections"] or 0) >= 1 and occ[0]["gen_marked"]
    p10.sys.verify_pass()
    assert p10.sys.gens.row(p10.spec, TARGET, g).get("suspect_since") is None
    assert p10.sys.recovery.settled(ms.subject_id, p10.spec, [KIND_A])["status"] == "settled"


# --- shared helpers for R-27 / R-29 -----------------------------------------------------------


def unbackoff(p10, row) -> None:
    p10.s.db.run("UPDATE {ks}.index_repairs SET next_attempt_at = null WHERE spec_digest = ? AND role = ? AND bucket = ? "
                 "AND subject_id = ? AND generation = ? AND occurrence = ?", row["spec_digest"], row["role"],
                 row["bucket"], row["subject_id"], row["generation"], row["occurrence"])


def latest(p10, sid):
    return occurrences(p10, sid)[-1]


def settle_runs(p10, rounds: int = 6) -> None:
    """Run the matcher and the repairs pass until quiet (no verification pass: no new detections)."""
    for _ in range(rounds):
        p10.run_matcher()
        p10.sys.recovery.run_pass(p10.spec)


def lwt_log(monkeypatch, db) -> list[tuple]:
    """Every LWT with its CQL, arguments and ``[applied]`` result, in order."""
    real = db.lwt
    out: list[tuple] = []

    def lwt(cql, *args):
        res = real(cql, *args)
        out.append((cql, args, res.applied))
        return res

    monkeypatch.setattr(db, "lwt", lwt)
    return out


class Gate:
    """Holds one named thread at a hook point until released."""

    def __init__(self) -> None:
        self.at, self.go = threading.Event(), threading.Event()

    def hold(self) -> None:
        self.at.set()
        assert self.go.wait(120)


# --- R-27 (§9a.2): occurrence identity with stale workers, j-variants, C-13 negative control ------

R27_VARIANTS = ["base", "j1", "j2", "j3", "j4"]


@pytest.mark.parametrize("variant", R27_VARIANTS)
def test_r27_stale_workers_and_uncertainty_variants(world, monkeypatch, variant) -> None:
    """R-27 steps A-K (§9a.2) with the held stale workers W1 (P6, close of occurrence 1) and W2 (P8, bump
    for occurrence 1), one run per j-variant. Final state: two rows (oid1 CLOSED RERUN_DONE resolved by
    run 2; oid2 CLOSED RERUN_DONE resolved by run 3); T run = done_run = done_ok_run = 3, recovery_runs =
    {oid1: 2, oid2: 3}; (M1, T) and (M2, T); M1, M2 at run 1; no LWT conditioned on oid1 applied after
    step E; row 1 never modified after step B; occurrence 2 never CLOSED while done_run < 3."""
    p10, (tid, ver, _) = world
    db = p10.s.db
    t = p10.target(tid, ver)
    # A
    lose(p10, t)
    m1 = p10.ingest_media(*clip("lp-000-sintel-re-encode.mp4"))
    p10.sys.discovery.scan_reps(p10.spec)
    p10.run_matcher()
    assert not has_match(p10, m1, tid)
    p10.sys.verify_pass()
    occ1 = latest(p10, t.subject_id)
    oid1 = occ1["occurrence_id"]
    # B: R1 repairs (REPAIRED); W2 reads REPAIRED and is held at P8; R1 bumps and RERUN_PENDING
    assert p10.sys.recovery._repair(occ_obj(occ1))
    w2, w1 = Gate(), Gate()
    names = threading.local()

    def p8(**ctx):
        if getattr(names, "who", None) == "W2" and ctx["occurrence"] == 1:
            w2.hold()

    def p6(**ctx):
        if getattr(names, "who", None) == "W1" and ctx["occurrence"] == 1:
            w1.hold()

    p10.hooks.on("reconciler.before_bump", p8)
    p10.hooks.on("reconciler.before_close", p6)

    def run_as(who, row):
        names.who = who
        p10.sys.recovery.step(row)

    tw2 = threading.Thread(target=run_as, args=("W2", latest(p10, t.subject_id)))
    tw2.start()
    assert w2.at.wait(60)
    p10.sys.recovery.step(latest(p10, t.subject_id))                  # R1: bump run 2, RERUN_PENDING
    assert latest(p10, t.subject_id)["state"] == RERUN_PENDING
    p10.run_matcher()                                                  # run 2 finds (M1, T)
    assert has_match(p10, m1, tid)
    tw1 = threading.Thread(target=run_as, args=("W1", latest(p10, t.subject_id)))
    tw1.start()
    assert w1.at.wait(60)
    p10.sys.recovery.step(latest(p10, t.subject_id))                  # R1 closes occurrence 1
    row1 = occurrences(p10, t.subject_id)[0]
    assert row1["state"] == CLOSED and row1["close_reason"] == "RERUN_DONE"
    snapshot1 = dict(row1)
    # C
    assert p10.sys.tasks.row(t.subject_id, p10.spec, KIND_B)["recovery_runs"] == {oid1: 2}
    # D
    lose(p10, t)
    m2 = p10.ingest_media(*clip("lp-035-sintel-re-encode.webm"))
    p10.sys.discovery.scan_reps(p10.spec)
    p10.run_matcher()
    assert not has_match(p10, m2, tid)
    log = lwt_log(monkeypatch, db)
    mark_e = len(log)
    # E (j1: the INSERT's response dropped after Scylla applied it)
    if variant == "j1":
        real_lwt = db.lwt
        fired: list[bool] = []

        def drop_insert(cql, *args):
            res = real_lwt(cql, *args)
            if "INSERT INTO {ks}.index_repairs" in cql and not fired:
                fired.append(res.applied)
                raise LwtUnknownError("injected: occurrence-2 INSERT outcome unknown")
            return res

        monkeypatch.setattr(db, "lwt", drop_insert)
        p10.sys.verify_pass()
        monkeypatch.setattr(db, "lwt", real_lwt)
        assert fired == [True]
    else:
        p10.sys.verify_pass()
    rows = occurrences(p10, t.subject_id)
    assert [r["occurrence"] for r in rows] == [1, 2], rows
    occ2 = rows[1]
    oid2 = occ2["occurrence_id"]
    assert oid2 != oid1 and occ2["state"] == OPEN and not occ2["required_run"] and occ2["detections"] == 0
    assert dict(rows[0]) == snapshot1
    # F/G: repair occurrence 2, bump run 2 -> 3 (j2: the bump LWT times out; j3: killed before the bump +
    # restart of every process)
    if variant == "j2":
        real_lwt = db.lwt
        fired = []

        def drop_bump(cql, *args):
            res = real_lwt(cql, *args)
            if "UPDATE {ks}.match_tasks SET run = ?" in cql and not fired:
                fired.append(res.applied)
                raise LwtUnknownError("injected: bump outcome unknown")
            return res

        monkeypatch.setattr(db, "lwt", drop_bump)
        p10.sys.recovery.step(occ2)
        monkeypatch.setattr(db, "lwt", real_lwt)
        assert fired == [True]
    elif variant == "j3":
        def die(**ctx):
            if ctx["occurrence"] == 2:
                raise CrashError("R1 killed between REPAIRED and the bump")
        p10.hooks.on("reconciler.before_bump", die)
        p10.sys.recovery.step(occ2)
        assert latest(p10, t.subject_id)["state"] == "REPAIRED"
        assert p10.sys.tasks.row(t.subject_id, p10.spec, KIND_B)["run"] == 2
        unbackoff(p10, latest(p10, t.subject_id))
        hooks = p10.hooks
        restart(p10)                                                   # every P10 process restarted
        hooks.handlers.clear()
        p10.sys.recovery.run_pass(p10.spec)                            # resumes from Scylla alone
    else:
        p10.sys.recovery.step(occ2)
    occ2 = latest(p10, t.subject_id)
    tb = p10.sys.tasks.row(t.subject_id, p10.spec, KIND_B)
    assert occ2["state"] == RERUN_PENDING and occ2["required_run"] == {KIND_B: 3}
    assert tb["run"] == 3 and tb["recovery_runs"] == {oid1: 2, oid2: 3}
    for _ in range(2):                                                 # run 3 held before its search
        p10.sys.recovery.run_pass(p10.spec)
        assert latest(p10, t.subject_id)["state"] == RERUN_PENDING
    # H/I: release the stale workers of occurrence 1
    before = (dict(occurrences(p10, t.subject_id)[0]), dict(latest(p10, t.subject_id)),
              dict(p10.sys.tasks.row(t.subject_id, p10.spec, KIND_B)))
    w1.go.set()
    w2.go.set()
    tw1.join(60)
    tw2.join(60)
    p10.hooks.handlers.clear()
    after = (dict(occurrences(p10, t.subject_id)[0]), dict(latest(p10, t.subject_id)),
             dict(p10.sys.tasks.row(t.subject_id, p10.spec, KIND_B)))
    assert after[0] == before[0] and after[1] == before[1]
    assert after[2]["run"] == before[2]["run"] == 3 and after[2]["recovery_runs"] == before[2]["recovery_runs"]
    # K (j4: the close LWT for occurrence 2 times out)
    if variant == "j4":
        real_lwt = db.lwt
        fired = []

        def drop_close(cql, *args):
            res = real_lwt(cql, *args)
            if "SET state = 'CLOSED'" in cql and not fired:
                fired.append(res.applied)
                raise LwtUnknownError("injected: close outcome unknown")
            return res

        monkeypatch.setattr(db, "lwt", drop_close)
        settle_runs(p10)
        monkeypatch.setattr(db, "lwt", real_lwt)
        assert fired == [True]
    else:
        settle_runs(p10)
    rows = occurrences(p10, t.subject_id)
    tb = p10.sys.tasks.row(t.subject_id, p10.spec, KIND_B)
    assert [r["occurrence"] for r in rows] == [1, 2]
    assert rows[0]["close_reason"] == rows[1]["close_reason"] == "RERUN_DONE"
    assert rows[0]["resolution"] == rows[1]["resolution"] == RESOLVED
    assert rows[0]["resolved_by"] == {KIND_B: 2} and rows[1]["resolved_by"] == {KIND_B: 3}
    assert tb["run"] == tb["done_run"] == tb["done_ok_run"] == 3
    assert tb["recovery_runs"] == {oid1: 2, oid2: 3}
    assert has_match(p10, m1, tid) and has_match(p10, m2, tid)
    for m in (m1, m2):
        assert p10.sys.tasks.row(p10.media(m).subject_id, p10.spec, KIND_A)["run"] == 1
    assert dict(rows[0]) == snapshot1                                  # row 1 never modified after B
    oid1_applied = [c for c, a, ok in log[mark_e:] if ok and oid1 in a and "index_repairs" in c]
    assert not oid1_applied, oid1_applied[:3]
    assert p10.sys.recovery.counters(p10.spec) == (0, 0)


def test_r27_negative_control_c13_reused_id_closes_without_a_new_run(world, monkeypatch) -> None:
    """R-27 negative control: the same script against a stub of the C-13 protocol (the second loss
    re-opens the first occurrence with its id) closes occurrence 2 at step G without run 3 and leaves
    (M2, T) undiscovered — the difference the occurrence-identity protocol exists to prevent."""
    p10, (tid, ver, _) = world
    db = p10.s.db
    t = p10.target(tid, ver)
    lose(p10, t)
    p10.sys.verify_pass()
    settle_runs(p10)
    assert latest(p10, t.subject_id)["close_reason"] == "RERUN_DONE"
    real_detect = p10.sys.recovery.detect

    def c13_detect(spec, role, subject_id, content_id, g, **kw):
        cur = occurrences(p10, subject_id)
        if cur and cur[-1]["state"] == CLOSED:      # C-13: one obligation id per (S, g), re-opened in place
            r = cur[-1]
            db.run("UPDATE {ks}.index_repairs SET state = 'OPEN', gen_marked = false, required_run = ?, "
                   "resolution = 'PENDING', detections = 0 WHERE spec_digest = ? AND role = ? AND bucket = ? AND "
                   "subject_id = ? AND generation = ? AND occurrence = ?", {}, spec, role, r["bucket"], subject_id,
                   g, r["occurrence"])
            return occ_obj(r)
        return real_detect(spec, role, subject_id, content_id, g, **kw)

    monkeypatch.setattr(p10.sys.recovery, "detect", c13_detect)
    lose(p10, t)
    m2 = p10.ingest_media(*clip("lp-035-sintel-re-encode.webm"))
    p10.sys.discovery.scan_reps(p10.spec)
    p10.run_matcher()
    p10.sys.verify_pass()
    p10.sys.recovery.run_pass(p10.spec)                                # step G under the stub
    rows = occurrences(p10, t.subject_id)
    tb = p10.sys.tasks.row(t.subject_id, p10.spec, KIND_B)
    assert len(rows) == 1 and rows[0]["state"] == CLOSED                 # closed again ...
    assert tb["run"] == 2                                                # ... without run 3
    assert not has_match(p10, m2, tid)                                   # the pair the loss hid stays hidden


# --- R-29 (§9a.3): unresolved occurrences survive later occurrences ------------------------------


def corrupt(p10, s):
    """Make S's own artifact unreadable (SHA mismatch); returns a function restoring the exact bytes."""
    import io

    bucket, key = s.artifact_uri.removeprefix("s3://").split("/", 1)
    data = b"".join(p10.s.objects.stream(bucket, key))
    p10.s.objects.client.put_object(bucket, key, io.BytesIO(data[:-10] + b"0123456789"), len(data))
    p10.sys.p.artifacts._lru.clear()

    def restore():
        p10.s.objects.client.put_object(bucket, key, io.BytesIO(data), len(data))
        p10.sys.p.artifacts._lru.clear()
    return restore


def unresolved(p10) -> int:
    return p10.sys.recovery.counters(p10.spec)[1]


def probe(p10, x) -> dict:
    return p10.sys.recovery.settled(x.subject_id, p10.spec, [KIND_A])


@pytest.fixture
def r29(world):
    """R-29 fixture: T (sintel) with two true clips; X, an unrelated subject whose last run is DONE (the
    settlement probe)."""
    p10, t = world
    p10.ingest_media(*clip("lp-000-sintel-re-encode.mp4"))
    p10.ingest_media(*clip("lp-035-sintel-re-encode.webm"))
    x = p10.ingest_media(*clip("lp-029-elephants-dream-pillarbox.webm"))
    p10.drive()
    return p10, t, p10.media(x)


def repaired_and_bumped(p10, sid, *, hold_bump: bool = False) -> dict:
    """Drive the latest occurrence of ``sid`` through repair (and the bump unless ``hold_bump``)."""
    row = latest(p10, sid)
    if hold_bump:
        assert p10.sys.recovery._repair(occ_obj(row))
    else:
        p10.sys.recovery.step(row)
    return latest(p10, sid)


@pytest.mark.parametrize("variant", ["v1", "v1prime"])
def test_r29_v1_task_failed_close_survives_a_later_occurrence(r29, variant) -> None:
    """v1: occurrence 1's run 2 fails non-retryably (T's own artifact unreadable) ⇒ CLOSED TASK_FAILED,
    PENDING. Loss 2 opens occurrence 2 (a new row) while T is FAILED; the artifact is restored, its repair
    completes, an audited ``match retry`` creates run 3 before occurrence 2's bump (coalesced, required 3);
    run 3 DONE; occurrence 2 closes while occurrence 1 is still PENDING; then occurrence 1 is resolved by
    its own LWT (LATER_RUN, by run 3). ``recovery_unresolved`` = 1 throughout, X never settled.
    v1′: no retry — occurrence 2's bump finds T FAILED and closes TASK_FAILED: unresolved = 2 until a
    retry produces a DONE run ≥ 3."""
    p10, (tid, ver, _), x = r29
    t = p10.target(tid, ver)
    lose(p10, t)
    p10.sys.verify_pass()
    repaired_and_bumped(p10, t.subject_id)                           # run 2 required
    restore = corrupt(p10, t)
    settle_runs(p10)
    o1 = occurrences(p10, t.subject_id)[0]
    assert o1["state"] == CLOSED and o1["close_reason"] == "TASK_FAILED" and o1["resolution"] == "PENDING"
    assert p10.sys.tasks.row(t.subject_id, p10.spec, KIND_B)["state"] == "FAILED"
    samples = [unresolved(p10)]
    restore()
    lose(p10, t)
    p10.sys.verify_pass()
    rows = occurrences(p10, t.subject_id)
    assert [r["occurrence"] for r in rows] == [1, 2] and rows[1]["state"] == OPEN
    samples.append(unresolved(p10))
    assert probe(p10, x)["status"] == "provisional"
    if variant == "v1":
        repaired_and_bumped(p10, t.subject_id, hold_bump=True)
        assert p10.sys.tasks.rerun(t, KIND_B, reason="retry", actor="test") == 3     # audited retry first
        o2 = repaired_and_bumped(p10, t.subject_id)
        assert o2["required_run"] == {KIND_B: 3}
        p10.run_matcher()                                             # run 3 DONE
        p10.sys.recovery.step(latest(p10, t.subject_id))             # occurrence 2 closes ...
        o2 = latest(p10, t.subject_id)
        assert o2["close_reason"] == "RERUN_DONE" and o2["resolved_by"] == {KIND_B: 3}
        o1 = occurrences(p10, t.subject_id)[0]
        assert o1["resolution"] == "PENDING"                          # ... occurrence 1 still held
        samples.append(unresolved(p10))
        assert probe(p10, x)["status"] == "provisional"
        p10.sys.recovery.step(o1)                                     # its own resolve LWT
        o1 = occurrences(p10, t.subject_id)[0]
        assert o1["resolution"] == RESOLVED and o1["resolution_reason"] == "LATER_RUN"
        assert o1["resolved_by"] == {KIND_B: 3}
        assert samples == [1, 1, 1] and unresolved(p10) == 0
    else:
        p10.sys.recovery.step(latest(p10, t.subject_id))             # bump finds T FAILED: required 3
        o2 = latest(p10, t.subject_id)
        assert o2["required_run"] == {KIND_B: 3}
        p10.sys.recovery.run_pass(p10.spec)
        o2 = latest(p10, t.subject_id)
        assert o2["close_reason"] == "TASK_FAILED" and o2["resolution"] == "PENDING"
        for _ in range(2):
            settle_runs(p10, rounds=2)
            assert unresolved(p10) == 2 and probe(p10, x)["status"] == "provisional"
        assert p10.sys.tasks.rerun(t, KIND_B, reason="retry", actor="test") == 3
        settle_runs(p10)
        rows = occurrences(p10, t.subject_id)
        assert all(r["resolution"] == RESOLVED and r["resolution_reason"] == "LATER_RUN" for r in rows)
        assert unresolved(p10) == 0
    assert probe(p10, x)["status"] == "settled"


def test_r29_v2_v3_v11_v12_one_later_run_resolves_each_row_by_its_own_lwt(r29, monkeypatch) -> None:
    """v2: occurrence 1's run 2 INCOMPLETE (cap 1) ⇒ CLOSED RERUN_INCOMPLETE, PENDING, never removed or
    rewritten by occurrence 2's open or close. v3: one later DONE run (3) satisfies both; each row is
    resolved by its own LWT exactly once (``IF resolution = 'PENDING'``), duplicates rejected. v11: while
    occurrence 1 is PENDING and the latest occurrence RESOLVED, X is provisional. v12: X is settled only
    from the first scan after the resolve, and provisional while X has a newer run pending."""
    p10, (tid, ver, _), x = r29
    t = p10.target(tid, ver)
    db = p10.s.db
    db.run("UPDATE {ks}.match_tasks SET cap_override = 1 WHERE subject_id = ? AND spec_digest = ? AND kind = ?",
           t.subject_id, p10.spec, KIND_B)
    lose(p10, t)
    p10.sys.verify_pass()
    settle_runs(p10)
    o1 = occurrences(p10, t.subject_id)[0]
    assert o1["close_reason"] == "RERUN_INCOMPLETE" and o1["resolution"] == "PENDING"
    snap1 = dict(o1)
    lose(p10, t)
    p10.sys.verify_pass()
    assert dict(occurrences(p10, t.subject_id)[0]) == snap1           # untouched by occurrence 2's open
    db.run("UPDATE {ks}.match_tasks SET cap_override = null WHERE subject_id = ? AND spec_digest = ? AND kind = ?",
           t.subject_id, p10.spec, KIND_B)                             # cap restored before run 3
    log = lwt_log(monkeypatch, db)
    real_resolve = p10.sys.recovery._resolve
    resolve_calls: list[int] = []

    def held_resolve(occ):
        resolve_calls.append(occ.occurrence)
        if occ.occurrence == 1 and len([c for c in resolve_calls if c == 1]) == 1:
            return False                                              # the first pass does not reach it (held)
        return real_resolve(occ)

    monkeypatch.setattr(p10.sys.recovery, "_resolve", held_resolve)
    p10.sys.recovery.step(latest(p10, t.subject_id))                 # repair + bump run 3
    p10.run_matcher()
    p10.sys.recovery.step(latest(p10, t.subject_id))                 # occurrence 2 closes RERUN_DONE
    o1, o2 = occurrences(p10, t.subject_id)
    assert o2["close_reason"] == "RERUN_DONE" and o2["resolution"] == RESOLVED
    assert dict(o1) == snap1                                           # untouched by occurrence 2's close
    v11 = probe(p10, x)
    assert v11["status"] == "provisional" and v11["recovery_unresolved"] == 1      # v11
    p10.sys.recovery.step(o1)                                          # held: no resolve yet
    assert occurrences(p10, t.subject_id)[0]["resolution"] == "PENDING"
    # v3: two concurrent resolves of occurrence 1 — exactly one applies
    ths = [threading.Thread(target=p10.sys.recovery.step, args=(o1,)) for _ in range(2)]
    for th in ths:
        th.start()
    for th in ths:
        th.join(60)
    p10.sys.recovery.step(o1)
    o1 = occurrences(p10, t.subject_id)[0]
    assert o1["resolution"] == RESOLVED and o1["resolution_reason"] == "LATER_RUN" and o1["resolved_by"] == {KIND_B: 3}
    applied_resolves = [a for c, a, ok in log if ok and "SET resolution = 'RESOLVED'" in c]
    assert len(applied_resolves) == 1                                  # one applied resolve for row 1
    assert len([1 for c, a, ok in log if ok and "SET state = 'CLOSED'" in c and "resolution = 'RESOLVED'" in c]) == 1
    # v12: settled only after the resolve; provisional with a newer run of X pending
    assert probe(p10, x)["status"] == "settled"
    p10.sys.tasks.rerun(x, KIND_A, reason="v12", actor="test")         # X has a newer run pending
    assert probe(p10, x)["status"] == "provisional"


def test_r29_v4_unsatisfied_kind_keeps_occurrence_pending(r29) -> None:
    """v4: a media subject S with equivalence enabled; occurrence 1 requires media_vs_targets and
    media_vs_media; its media_vs_media rerun is INCOMPLETE ⇒ RERUN_INCOMPLETE. Equivalence is then disabled.
    (Plan text: occurrence 2 requires only media_vs_targets. Design §12.10's bump rule includes every kind
    whose task row exists, so occurrence 2 also requires media_vs_media; the implementation follows the
    design.) Pass criterion: occurrence 1 is resolved only after a media_vs_media run of S at or above its
    requirement finishes DONE (an audited ``match rerun --kind media_vs_media``)."""
    from fingerprinter.p10ctx import KIND_EQ

    p10, (tid, ver, _), _ = r29
    m = p10.sys.p.settings.matching.model_copy(update={"equivalence_enabled": True})
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"matching": m})
    v, ts = clip("lp-000-sintel-re-encode.mp4")
    s = p10.media(p10.ingest_media(v, ts, data=b"v4-S"))
    for i in range(2):                                                 # two equivalent copies: two EQ candidates
        p10.ingest_media(v, ts, data=f"v4-copy-{i}".encode())
    p10.drive()
    db = p10.s.db
    db.run("UPDATE {ks}.match_tasks SET cap_override = 1 WHERE subject_id = ? AND spec_digest = ? AND kind = ?",
           s.subject_id, p10.spec, KIND_EQ)
    lose(p10, s)
    p10.sys.verify_pass()
    settle_runs(p10)
    o1 = occurrences(p10, s.subject_id)[0]
    assert set(o1["required_run"]) == {KIND_A, KIND_EQ}
    assert o1["close_reason"] == "RERUN_INCOMPLETE" and o1["resolution"] == "PENDING"
    m = p10.sys.p.settings.matching.model_copy(update={"equivalence_enabled": False})
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"matching": m})
    lose(p10, s)
    p10.sys.verify_pass()
    settle_runs(p10)
    o1, o2 = occurrences(p10, s.subject_id)
    assert o1["resolution"] == "PENDING"                               # A's later DONE run does not resolve it
    assert unresolved(p10) >= 1
    db.run("UPDATE {ks}.match_tasks SET cap_override = null WHERE subject_id = ? AND spec_digest = ? AND kind = ?",
           s.subject_id, p10.spec, KIND_EQ)
    p10.sys.tasks.rerun(s, KIND_EQ, reason="match rerun --kind media_vs_media", actor="test")
    settle_runs(p10)
    o1, o2 = occurrences(p10, s.subject_id)
    assert o1["resolution"] == RESOLVED and o1["resolved_by"][KIND_EQ] >= o1["required_run"][KIND_EQ]
    assert o2["resolution"] == RESOLVED and unresolved(p10) == 0


def test_r29_v5_detector_crash_after_insert_merges_no_row_3(r29) -> None:
    """v5: the detector dies right after occurrence 2's INSERT applied (before its cursor advance and the
    mark): row 2 OPEN, ``gen_marked = false``; the restarted detector reads row 2 and merges — no row 3;
    the reconciler marks and proceeds."""
    p10, (tid, ver, _), _ = r29
    t = p10.target(tid, ver)
    lose(p10, t)
    p10.sys.verify_pass()
    settle_runs(p10)
    lose(p10, t)

    def die(**ctx):
        if ctx["occurrence"] == 2:
            raise CrashError("detector killed after the INSERT")

    p10.hooks.on("recovery.after_open", die)
    with pytest.raises(CrashError):
        p10.sys.verify_pass()
    p10.hooks.clear()
    o2 = latest(p10, t.subject_id)
    assert o2["occurrence"] == 2 and o2["state"] == OPEN and not o2["gen_marked"]
    restart(p10)
    p10.sys.verify_pass()
    rows = occurrences(p10, t.subject_id)
    assert [r["occurrence"] for r in rows] == [1, 2] and int(rows[1]["detections"]) >= 1
    settle_runs(p10)
    assert latest(p10, t.subject_id)["close_reason"] == "RERUN_DONE" and unresolved(p10) == 0


def test_r29_v6_v10_restart_rederives_from_rows(r29, monkeypatch) -> None:
    """v6: the reconciler dies right after an unresolved close LWT (before its metric/audit write), and
    later right after a resolve LWT; restarted, the pending row is re-checked and the resolved row is not
    touched again; counters are unchanged by a restart. v10: three subjects, five occurrences, two CLOSED
    PENDING in different partitions (target and media role); every P10 process restarted; the repairs
    pass finds both from the table alone; no row lost or duplicated."""
    p10, (tid, ver, _), x = r29
    t = p10.target(tid, ver)
    db = p10.s.db
    s2 = p10.media(p10.ingest_media(*clip("lp-035-sintel-re-encode.webm"), data=b"v10-s2"))
    p10.drive()

    def kill_on(event_name, *, resolved=None):
        real_log = p10.sys.recovery._log
        fired: list[dict] = []

        def log(event, **kw):
            if event == event_name and not fired and (resolved is None or kw.get("resolved") == resolved):
                fired.append(kw)
                raise Kill(f"killed right after the LWT of {event_name}")
            return real_log(event, **kw)

        monkeypatch.setattr(p10.sys.recovery, "_log", log)
        return fired

    # T occurrence 1: RERUN_INCOMPLETE — killed right after the unresolved close LWT (v6, first point)
    db.run("UPDATE {ks}.match_tasks SET cap_override = 1 WHERE subject_id = ? AND spec_digest = ? AND kind = ?",
           t.subject_id, p10.spec, KIND_B)
    lose(p10, t)
    p10.sys.verify_pass()
    fired = kill_on("occurrence_closed", resolved=False)
    with pytest.raises(Kill):
        settle_runs(p10)
    assert fired
    restart(p10)
    assert latest(p10, t.subject_id)["close_reason"] == "RERUN_INCOMPLETE"
    # X: two occurrences, both RERUN_DONE; S2: one occurrence TASK_FAILED (its own artifact unreadable)
    for _ in range(2):
        lose(p10, x)
        p10.sys.verify_pass()
        settle_runs(p10)
    lose(p10, s2)
    p10.sys.verify_pass()
    p10.sys.recovery.step(latest(p10, s2.subject_id))              # repaired, bumped
    restore = corrupt(p10, s2)                                       # then its rerun fails on its own artifact
    settle_runs(p10)
    # (fifth occurrence) X once more
    lose(p10, x)
    p10.sys.verify_pass()
    settle_runs(p10)
    rows = p10.sys.recovery.occurrences(p10.spec)
    pending = sorted((r["role"], r["subject_id"]) for r in rows if r["state"] == CLOSED and r["resolution"] == "PENDING")
    assert len(rows) == 5 and pending == sorted([("target", t.subject_id), ("media", s2.subject_id)]), rows
    snapshot = sorted((r["subject_id"], r["occurrence"], r["state"], r["resolution"], str(r["occurrence_id"]))
                      for r in rows)
    counters = p10.sys.recovery.counters(p10.spec)
    assert counters == (0, 2)
    resolved_ids = {r["occurrence_id"] for r in rows if r["resolution"] == RESOLVED}
    log = lwt_log(monkeypatch, db)
    restart(p10)                                                       # v10: every P10 process restarted
    assert p10.sys.recovery.counters(p10.spec) == counters
    out = p10.sys.recovery.run_pass(p10.spec)
    assert out["processed"] == 2                                       # both pending rows found from the table
    assert not [a for c, a, ok in log if "index_repairs" in c and any(i in a for i in resolved_ids)]
    after = sorted((r["subject_id"], r["occurrence"], r["state"], r["resolution"], str(r["occurrence_id"]))
                   for r in p10.sys.recovery.occurrences(p10.spec))
    assert after == snapshot
    # T's later DONE run: killed right after the resolve LWT (v6, second point)
    db.run("UPDATE {ks}.match_tasks SET cap_override = null WHERE subject_id = ? AND spec_digest = ? AND kind = ?",
           t.subject_id, p10.spec, KIND_B)
    p10.sys.tasks.rerun(t, KIND_B, reason="raise_cap", actor="test")
    p10.run_matcher()
    fired = kill_on("occurrence_resolved")
    with pytest.raises(Kill):
        p10.sys.recovery.run_pass(p10.spec)
    assert fired
    o1 = latest(p10, t.subject_id)
    assert o1["resolution"] == RESOLVED
    restart(p10)
    log = lwt_log(monkeypatch, db)
    p10.sys.recovery.run_pass(p10.spec)
    assert not [a for c, a, ok in log if "index_repairs" in c and o1["occurrence_id"] in a]   # not touched again
    assert p10.sys.recovery.counters(p10.spec) == (0, 1)
    restore()
    p10.sys.tasks.rerun(s2, KIND_A, reason="retry", actor="test")
    settle_runs(p10)
    assert p10.sys.recovery.counters(p10.spec) == (0, 0)


def test_r29_v7_v8_unknown_outcomes_and_concurrent_resolvers(r29, monkeypatch) -> None:
    """v7: the resolve LWT's response is dropped after it applied ⇒ RESOLVED, no second resolve; the open
    INSERT of occurrence 2 times out ⇒ the SERIAL read finds row 2 with the detector's own id, no row 3.
    v8: two reconcilers (two systems) resolve occurrence 1 concurrently ⇒ one applied, one rejected."""
    p10, (tid, ver, _), _ = r29
    t = p10.target(tid, ver)
    db = p10.s.db
    db.run("UPDATE {ks}.match_tasks SET cap_override = 1 WHERE subject_id = ? AND spec_digest = ? AND kind = ?",
           t.subject_id, p10.spec, KIND_B)
    lose(p10, t)
    p10.sys.verify_pass()
    settle_runs(p10)
    assert latest(p10, t.subject_id)["resolution"] == "PENDING"
    db.run("UPDATE {ks}.match_tasks SET cap_override = null WHERE subject_id = ? AND spec_digest = ? AND kind = ?",
           t.subject_id, p10.spec, KIND_B)
    lose(p10, t)
    fired = unknown_once(monkeypatch, db, "INSERT INTO {ks}.index_repairs")
    p10.sys.verify_pass()
    monkeypatch.undo()
    assert fired == [True] and [r["occurrence"] for r in occurrences(p10, t.subject_id)] == [1, 2]
    gate = threading.Barrier(2)
    p10.hooks.on("reconciler.before_resolve", lambda **_: gate.wait(30))
    log = lwt_log(monkeypatch, db)
    p10.sys.recovery.step(latest(p10, t.subject_id))                  # occurrence 2: repair + bump run 3
    p10.run_matcher()
    other = P10System(p10.sys.p)
    o1 = occurrences(p10, t.subject_id)[0]
    ths = [threading.Thread(target=p10.sys.recovery.step, args=(o1,)),
           threading.Thread(target=other.recovery.step, args=(o1,))]
    for th in ths:
        th.start()
    for th in ths:
        th.join(60)
    p10.hooks.clear()
    tries = [ok for c, a, ok in log if "SET resolution = 'RESOLVED'" in c]
    assert sorted(tries) == [False, True]                              # v8: one applied, one rejected
    # v7: the resolve of occurrence 2's close path is not needed; drop a resolve response on a fresh pending
    lose(p10, t)
    p10.sys.verify_pass()
    db.run("UPDATE {ks}.match_tasks SET cap_override = 1 WHERE subject_id = ? AND spec_digest = ? AND kind = ?",
           t.subject_id, p10.spec, KIND_B)
    settle_runs(p10)
    o3 = latest(p10, t.subject_id)
    assert o3["resolution"] == "PENDING"
    db.run("UPDATE {ks}.match_tasks SET cap_override = null WHERE subject_id = ? AND spec_digest = ? AND kind = ?",
           t.subject_id, p10.spec, KIND_B)
    p10.sys.tasks.rerun(t, KIND_B, reason="retry", actor="test")
    p10.run_matcher()
    fired = unknown_once(monkeypatch, db, "SET resolution = 'RESOLVED'")
    p10.sys.recovery.step(o3)
    monkeypatch.undo()
    assert fired == [True]
    log = lwt_log(monkeypatch, db)
    p10.sys.recovery.run_pass(p10.spec)
    assert latest(p10, t.subject_id)["resolution"] == RESOLVED
    assert not [1 for c, a, ok in log if "SET resolution = 'RESOLVED'" in c]        # no second resolve
    assert unresolved(p10) == 0


def test_r29_v9_held_close_of_an_older_occurrence_acts_on_its_row_only(r29) -> None:
    """v9: a reconciler that read occurrence 1 as RERUN_PENDING (its run finished) is held before its close;
    occurrence 2 opens and closes; the held worker resumes: its LWT acts on row 1 only (applied, as row 1
    is still RERUN_PENDING) and never writes row 2."""
    p10, (tid, ver, _), _ = r29
    t = p10.target(tid, ver)
    lose(p10, t)
    p10.sys.verify_pass()
    p10.sys.recovery.step(latest(p10, t.subject_id))                  # repair + bump run 2
    p10.run_matcher()                                                  # run 2 finished
    gate, who = Gate(), threading.local()
    p10.hooks.on("reconciler.before_close",
                 lambda **ctx: gate.hold() if getattr(who, "held", False) and ctx["occurrence"] == 1 else None)

    def held():
        who.held = True
        p10.sys.recovery.step(occurrences(p10, t.subject_id)[0])

    th = threading.Thread(target=held)
    th.start()
    assert gate.at.wait(60)
    # occurrence 1 is RERUN_PENDING, not OPEN: a new loss opens occurrence 2; it is driven directly
    lose(p10, t)
    p10.sys.verify_pass()
    o2 = latest(p10, t.subject_id)
    assert o2["occurrence"] == 2
    p10.sys.recovery.step(o2)
    p10.run_matcher()
    p10.sys.recovery.step(latest(p10, t.subject_id))
    snap2 = dict(latest(p10, t.subject_id))
    assert snap2["state"] == CLOSED
    gate.go.set()
    th.join(60)
    p10.hooks.clear()
    o1 = occurrences(p10, t.subject_id)[0]
    assert o1["state"] == CLOSED and o1["close_reason"] == "RERUN_DONE"      # applied on row 1
    assert dict(latest(p10, t.subject_id)) == snap2                           # row 2 unaffected


def test_r29_v13_retired_subject_resolves_its_unresolved_occurrence(r29) -> None:
    """v13: T has occurrence 1 CLOSED TASK_FAILED; T is retired: the occurrence resolves SUBJECT_RETIRED
    through its own LWT (the only way ``recovery_unresolved`` returns to 0); T's tasks stop."""
    p10, (tid, ver, _), _ = r29
    t = p10.target(tid, ver)
    lose(p10, t)
    p10.sys.verify_pass()
    p10.sys.recovery.step(latest(p10, t.subject_id))
    corrupt(p10, t)
    settle_runs(p10)
    o1 = latest(p10, t.subject_id)
    assert o1["close_reason"] == "TASK_FAILED" and o1["resolution"] == "PENDING" and unresolved(p10) == 1
    p10.s.targets.retire(tid, reason="v13", actor="test")
    settle_runs(p10)
    o1 = latest(p10, t.subject_id)
    assert o1["resolution"] == RESOLVED and o1["resolution_reason"] == "SUBJECT_RETIRED"
    assert unresolved(p10) == 0


def test_r29_negative_control_c14_reset_row_hides_the_unresolved_occurrence(r29, monkeypatch) -> None:
    """Negative control: against a stub of the C-14 protocol (one row per (S, g), reset on re-open) the v2
    script reports ``recovery_unresolved = 0`` after occurrence 2 closes, although occurrence 1's
    requirement was never satisfied by a DONE run it can name — the difference C-15 removes."""
    p10, (tid, ver, _), _ = r29
    t = p10.target(tid, ver)
    db = p10.s.db
    db.run("UPDATE {ks}.match_tasks SET cap_override = 1 WHERE subject_id = ? AND spec_digest = ? AND kind = ?",
           t.subject_id, p10.spec, KIND_B)
    lose(p10, t)
    p10.sys.verify_pass()
    settle_runs(p10)
    assert unresolved(p10) == 1
    real_detect = p10.sys.recovery.detect

    def c14_detect(spec, role, subject_id, content_id, g, **kw):
        cur = occurrences(p10, subject_id)
        if cur and cur[-1]["state"] == CLOSED:          # C-14: the same row reset to OPEN on re-open
            r = cur[-1]
            db.run("UPDATE {ks}.index_repairs SET state = 'OPEN', gen_marked = false, required_run = ?, "
                   "resolution = 'PENDING', close_reason = null, detections = 0, occurrence_id = ? "
                   "WHERE spec_digest = ? AND role = ? AND bucket = ? AND subject_id = ? AND generation = ? "
                   "AND occurrence = ?", {}, uuid.uuid1(), spec, role, r["bucket"], subject_id, g,
                   r["occurrence"])
            return occ_obj(occurrences(p10, subject_id)[-1])
        return real_detect(spec, role, subject_id, content_id, g, **kw)

    monkeypatch.setattr(p10.sys.recovery, "detect", c14_detect)
    db.run("UPDATE {ks}.match_tasks SET cap_override = null WHERE subject_id = ? AND spec_digest = ? AND kind = ?",
           t.subject_id, p10.spec, KIND_B)
    lose(p10, t)
    p10.sys.verify_pass()
    settle_runs(p10)
    assert len(occurrences(p10, t.subject_id)) == 1 and unresolved(p10) == 0   # the history is gone
