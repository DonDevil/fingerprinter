"""Execution semantics of the P10 LWT forms on the deployed Scylla 6.2.3 (tablets off).

Design §12.9/§12.10 list these as assumptions that were only *prepared*
(never executed) during the design review. Each test executes the exact
statement shape the implementation uses on the real P10 tables and inspects
``[applied]`` and the rows afterwards:

- R-26 forms on ``match_tasks``: ``IF run = ? AND run_started_at = null``,
  ``IF run = ? AND state IN (…)``, map-entry set ``recovery_runs[?] = ?``
  inside a conditional update, stale completion after a bump;
- list append inside a conditional update (``audit = audit + ?``);
- CAP-9: the capacity admission conditional batch (static ``IF reserved =``
  plus a clustering-row insert) applies together or not at all; static-only
  ``INSERT … IF NOT EXISTS``;
- the promotion batch: static ``IF active_epoch =`` plus row conditions
  ``IF state = 'READY' AND detect_count = ?`` on another row, atomic;
- §9a.4: the ``LOCAL_SERIAL`` slice read ``… LIMIT 1`` of ``index_repairs``
  (clustering ``occurrence DESC``) returns the highest occurrence right after
  an acknowledged ``INSERT … IF NOT EXISTS`` and after an LWT whose outcome
  was unknown to its client; concurrent detectors acting on it never create
  two rows for one number, never skip a number and never merge into an
  occurrence past OPEN.
"""

from __future__ import annotations

import threading
import time
import uuid
from datetime import UTC, datetime

import pytest
from cassandra import OperationTimedOut, WriteTimeout

from fingerprinter.db import SERIAL
from tests.p10.conftest import record_evidence

pytestmark = pytest.mark.p10_integration

NON_FAILED = ("PENDING", "INDEXED", "SEARCHED", "VERIFYING", "DONE", "INCOMPLETE", "WAITING_INDEX")


def now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def task(db, subject="S"):
    return db.one("SELECT * FROM {ks}.match_tasks WHERE subject_id = ? AND spec_digest = 'd' AND kind = 'k'",
                  subject, level=SERIAL)


def test_r26_task_row_forms(db) -> None:
    ev = {}
    r = db.lwt("INSERT INTO {ks}.match_tasks (subject_id, spec_digest, kind, state, epoch, run, recovery_runs) "
               "VALUES ('S','d','k','PENDING',0,1,?) IF NOT EXISTS", {})
    assert r.applied
    # claim of run 1: IF run = 1 AND run_started_at = null (column never written)
    claim = ("UPDATE {ks}.match_tasks SET run_started_at = ?, state = 'INDEXED', epoch = ? "
             "WHERE subject_id = 'S' AND spec_digest = 'd' AND kind = 'k' IF run = ? AND run_started_at = null")
    a = db.lwt(claim, now(), 1, 1)
    b = db.lwt(claim, now(), 2, 1)
    ev["claim_first"], ev["claim_second"] = a.applied, b.applied
    assert a.applied and not b.applied and b.row["run_started_at"] is not None
    # bump: IF run = 1 AND state IN (non-FAILED), with a map entry keyed by a timeuuid
    oid = uuid.uuid1()
    bump = ("UPDATE {ks}.match_tasks SET run = ?, state = 'PENDING', run_started_at = null, "
            "recovery_runs[?] = ?, rerun_reason = 'index_repair' "
            "WHERE subject_id = 'S' AND spec_digest = 'd' AND kind = 'k' IF run = ? AND state IN ?")
    c = db.lwt(bump, 2, oid, 2, 1, list(NON_FAILED))
    row = task(db)
    ev["bump"] = c.applied
    assert c.applied and row["run"] == 2 and row["run_started_at"] is None and row["recovery_runs"] == {oid: 2}
    # stale completion of run 1 after the bump is rejected and shows the new run
    done = ("UPDATE {ks}.match_tasks SET state = 'DONE', done_run = ?, done_state = 'DONE', done_ok_run = ? "
            "WHERE subject_id = 'S' AND spec_digest = 'd' AND kind = 'k' IF epoch = ? AND run = ?")
    d = db.lwt(done, 1, 1, 1, 1)
    ev["stale_completion_rejected"] = not d.applied
    assert not d.applied and d.row["run"] == 2
    # coalesce on an unclaimed run: IF run = 2 AND run_started_at = null
    oid2 = uuid.uuid1()
    e = db.lwt("UPDATE {ks}.match_tasks SET recovery_runs[?] = ? WHERE subject_id = 'S' AND spec_digest = 'd' "
               "AND kind = 'k' IF run = ? AND run_started_at = null", oid2, 2, 2)
    assert e.applied and task(db)["recovery_runs"] == {oid: 2, oid2: 2}
    # FAILED excluded from the bump's IN list
    assert db.lwt("UPDATE {ks}.match_tasks SET state = 'FAILED' WHERE subject_id = 'S' AND spec_digest = 'd' "
                  "AND kind = 'k' IF run = 2").applied
    f = db.lwt(bump, 3, uuid.uuid1(), 3, 2, list(NON_FAILED))
    ev["bump_of_failed_rejected"] = not f.applied
    assert not f.applied and f.row["state"] == "FAILED"
    # completion LWT of the current run with done_ok_run
    db.lwt("UPDATE {ks}.match_tasks SET state = 'VERIFYING', epoch = 7 WHERE subject_id = 'S' AND spec_digest = 'd' "
           "AND kind = 'k' IF state = 'FAILED'")
    g = db.lwt(done, 2, 2, 7, 2)
    assert g.applied and task(db)["done_ok_run"] == 2
    record_evidence("scylla-r26-task-forms", ev)


def test_list_append_and_null_conditions(db) -> None:
    db.lwt("INSERT INTO {ks}.index_generations (spec_digest, role, generation, state) VALUES ('d','media',1,'READY') "
           "IF NOT EXISTS")
    a = db.lwt("UPDATE {ks}.index_generations SET audit = audit + ? WHERE spec_digest = 'd' AND role = 'media' "
               "AND generation = 1 IF state = 'READY'", ["first"])
    b = db.lwt("UPDATE {ks}.index_generations SET audit = audit + ? WHERE spec_digest = 'd' AND role = 'media' "
               "AND generation = 1 IF state = 'READY'", ["second"])
    c = db.lwt("UPDATE {ks}.index_generations SET audit = audit + ? WHERE spec_digest = 'd' AND role = 'media' "
               "AND generation = 1 IF state = 'ACTIVE'", ["never"])
    row = db.one("SELECT audit, suspect_since FROM {ks}.index_generations WHERE spec_digest = 'd' AND role = 'media' "
                 "AND generation = 1", level=SERIAL)
    assert a.applied and b.applied and not c.applied and row["audit"] == ["first", "second"]
    # `= null` on a never-written column, then on a written one
    d = db.lwt("UPDATE {ks}.index_generations SET suspect_since = ? WHERE spec_digest = 'd' AND role = 'media' "
               "AND generation = 1 IF suspect_since = null", now())
    e = db.lwt("UPDATE {ks}.index_generations SET suspect_since = ? WHERE spec_digest = 'd' AND role = 'media' "
               "AND generation = 1 IF suspect_since = null", now())
    assert d.applied and not e.applied
    record_evidence("scylla-list-append-null", {"append": [a.applied, b.applied, c.applied], "audit": row["audit"],
                                                "null_first": d.applied, "null_second": e.applied})


def test_cap9_admission_batch_atomic(db) -> None:
    # static-only insert IF NOT EXISTS creates the ledger partition once
    a = db.lwt("INSERT INTO {ks}.index_capacity (instance, max_points, reserved) VALUES ('i1', 1000, 0) IF NOT EXISTS")
    b = db.lwt("INSERT INTO {ks}.index_capacity (instance, max_points, reserved) VALUES ('i1', 5, 5) IF NOT EXISTS")
    assert a.applied and not b.applied and b.row["reserved"] == 0
    batch = ("BEGIN BATCH "
             "UPDATE {ks}.index_capacity SET reserved = ?, updated_at = ? WHERE instance = 'i1' IF reserved = ?; "
             "INSERT INTO {ks}.index_capacity (instance, holder, points, created_at) VALUES ('i1', ?, ?, ?); "
             "APPLY BATCH")
    ok = db.lwt(batch, 300, now(), 0, "gen:x:media:2", 300, now())
    stale = db.lwt(batch, 700, now(), 0, "gen:x:media:3", 400, now())
    rows = db.rows("SELECT holder, points, reserved FROM {ks}.index_capacity WHERE instance = 'i1'", level=SERIAL)
    holders = {r["holder"]: r["points"] for r in rows if r["holder"]}
    assert ok.applied and not stale.applied
    assert holders == {"gen:x:media:2": 300} and rows[0]["reserved"] == 300
    # plain reservation CAS loop
    c = db.lwt("UPDATE {ks}.index_capacity SET reserved = ? WHERE instance = 'i1' IF reserved = ?", 312, 300)
    assert c.applied
    record_evidence("scylla-cap9-batch", {"static_insert_if_not_exists": [a.applied, b.applied],
                                          "batch_applied": ok.applied, "stale_batch_applied": stale.applied,
                                          "holders_after": holders})


def test_promotion_batch_atomic(db) -> None:
    for g, st in ((1, "ACTIVE"), (2, "READY")):
        db.lwt("INSERT INTO {ks}.index_generations (spec_digest, role, generation, state, detect_count) "
               "VALUES ('d','target',?,?,0) IF NOT EXISTS", g, st)
    db.lwt("UPDATE {ks}.index_generations SET active_generation = 1, active_epoch = 1 WHERE spec_digest = 'd' "
           "AND role = 'target' IF active_epoch = null")
    promote = ("BEGIN BATCH "
               "UPDATE {ks}.index_generations SET active_generation = ?, active_epoch = ? "
               "WHERE spec_digest = 'd' AND role = 'target' IF active_epoch = ?; "
               "UPDATE {ks}.index_generations SET state = 'ACTIVE', promoted_at = ? "
               "WHERE spec_digest = 'd' AND role = 'target' AND generation = ? "
               "IF state = 'READY' AND detect_count = ?; "
               "UPDATE {ks}.index_generations SET state = 'DRAINING', drain_horizon_at = ? "
               "WHERE spec_digest = 'd' AND role = 'target' AND generation = ? IF state = 'ACTIVE'; "
               "APPLY BATCH")
    # detection fence: detect_count differs from the validated value -> nothing applies
    db.lwt("UPDATE {ks}.index_generations SET detect_count = 1 WHERE spec_digest = 'd' AND role = 'target' "
           "AND generation = 2 IF detect_count = 0")
    fenced = db.lwt(promote, 2, 2, 1, now(), 2, 0, now(), 1)
    rows = {r["generation"]: r for r in db.rows("SELECT * FROM {ks}.index_generations WHERE spec_digest = 'd' "
                                                "AND role = 'target'", level=SERIAL)}
    assert not fenced.applied and rows[1]["active_generation"] == 1 and rows[2]["state"] == "READY"
    assert rows[1]["state"] == "ACTIVE"
    ok = db.lwt(promote, 2, 2, 1, now(), 2, 1, now(), 1)
    rows = {r["generation"]: r for r in db.rows("SELECT * FROM {ks}.index_generations WHERE spec_digest = 'd' "
                                                "AND role = 'target'", level=SERIAL)}
    assert ok.applied and rows[1]["active_generation"] == 2 and rows[1]["active_epoch"] == 2
    assert rows[2]["state"] == "ACTIVE" and rows[1]["state"] == "DRAINING"
    again = db.lwt(promote, 2, 3, 1, now(), 2, 1, now(), 1)
    assert not again.applied
    record_evidence("scylla-promotion-batch", {"fenced_by_detect_count": not fenced.applied, "promoted": ok.applied,
                                               "replay_rejected": not again.applied})


# --- §9a.4: LOCAL_SERIAL slice read of the latest occurrence ---------------------------

LATEST = ("SELECT occurrence, occurrence_id, state FROM {ks}.index_repairs WHERE spec_digest = 'd' AND role = 'target' "
          "AND bucket = 0 AND subject_id = ? AND generation = 1 LIMIT 1")
OPEN = ("INSERT INTO {ks}.index_repairs (spec_digest, role, bucket, subject_id, generation, occurrence, occurrence_id, "
        "state, detections, gen_marked, required_run, resolution, resolved_by, attempts) "
        "VALUES ('d','target',0,?,1,?,?,'OPEN',0,false,?,'PENDING',?,0) IF NOT EXISTS")
MERGE = ("UPDATE {ks}.index_repairs SET detections = ? WHERE spec_digest = 'd' AND role = 'target' AND bucket = 0 "
         "AND subject_id = ? AND generation = 1 AND occurrence = ? IF occurrence_id = ? AND state = 'OPEN' "
         "AND detections = ?")


def latest(db, subject):
    return db.one(LATEST, subject, level=SERIAL)


def test_9a4_serial_slice_read_returns_latest(db) -> None:
    ev: dict = {}
    # 1. accepted and executed at LOCAL_SERIAL
    assert latest(db, "T") is None
    stmt = db._stmt(LATEST, SERIAL)
    ev["consistency"] = str(stmt.consistency_level)
    from cassandra import ConsistencyLevel

    assert stmt.consistency_level == ConsistencyLevel.LOCAL_SERIAL
    # 2a. right after acknowledged inserts
    for n in (1, 2, 3):
        oid = uuid.uuid1()
        assert db.lwt(OPEN, "T", n, oid, {}, {}).applied
        got = latest(db, "T")
        assert got["occurrence"] == n and got["occurrence_id"] == oid
    ev["after_ack_insert"] = True
    # 2b. after an LWT whose outcome is unknown to its client (client-side timeout)
    session = db._session
    unknown = 0
    for n in range(4, 12):
        oid = uuid.uuid1()
        bound = db._stmt(OPEN, "quorum").bind(("T", n, oid, {}, {}))
        try:
            session.execute(bound, timeout=0.0005)
            outcome = "known"
        except (OperationTimedOut, WriteTimeout):
            outcome = "unknown"
            unknown += 1
        deadline = time.monotonic() + 5
        while True:  # the request may still be in flight; poll the SERIAL slice read
            got = latest(db, "T")
            if got["occurrence"] == n or time.monotonic() > deadline:
                break
            time.sleep(0.05)
        point = db.one("SELECT occurrence_id FROM {ks}.index_repairs WHERE spec_digest = 'd' AND role = 'target' "
                       "AND bucket = 0 AND subject_id = 'T' AND generation = 1 AND occurrence = ?", n, level=SERIAL)
        if point is None:  # not applied: the slice read must show the previous highest
            assert got["occurrence"] == n - 1, (outcome, got)
            assert db.lwt(OPEN, "T", n, oid, {}, {}).applied
            got = latest(db, "T")
        assert got["occurrence"] == n and got["occurrence_id"] == oid, (outcome, got)
    ev["unknown_outcomes_observed"] = unknown
    ev["after_unknown_outcome"] = True
    record_evidence("scylla-9a4-slice-read", ev)
    assert unknown > 0, "the client timeout did not produce any unknown outcome; requirement 2b not exercised"


def detector(db, subject: str, log: list, barrier: threading.Barrier) -> None:
    """One detection step of design §12.10 using the slice read; every write is a single-row LWT."""
    barrier.wait()
    for _ in range(50):
        cur = latest(db, subject)
        if cur is not None and cur["state"] == "OPEN":
            row = db.one("SELECT detections FROM {ks}.index_repairs WHERE spec_digest = 'd' AND role = 'target' "
                         "AND bucket = 0 AND subject_id = ? AND generation = 1 AND occurrence = ?", subject,
                         cur["occurrence"], level=SERIAL)
            r = db.lwt(MERGE, row["detections"] + 1, subject, cur["occurrence"], cur["occurrence_id"],
                       row["detections"])
            if r.applied:
                log.append(("merge", cur["occurrence"]))
                return
            continue
        n = 1 if cur is None else cur["occurrence"] + 1
        oid = uuid.uuid1()
        if db.lwt(OPEN, subject, n, oid, {}, {}).applied:
            log.append(("open", n))
            return
    log.append(("gave_up", None))


def close(db, subject: str, occurrence: int) -> None:
    assert db.lwt("UPDATE {ks}.index_repairs SET state = 'CLOSED' WHERE spec_digest = 'd' AND role = 'target' "
                  "AND bucket = 0 AND subject_id = ? AND generation = 1 AND occurrence = ? IF state = 'OPEN'",
                  subject, occurrence).applied


def test_9a4_concurrent_detectors_no_duplicate_no_skip(db) -> None:
    """Rounds of 6 concurrent detectors; after each round the open occurrence is closed (past OPEN)."""
    log: list = []
    for _round in range(5):
        barrier = threading.Barrier(6)
        threads = [threading.Thread(target=detector, args=(db, "C", log, barrier)) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        cur = latest(db, "C")
        close(db, "C", cur["occurrence"])
    rows = db.rows("SELECT occurrence, state, detections FROM {ks}.index_repairs WHERE spec_digest = 'd' "
                   "AND role = 'target' AND bucket = 0 AND subject_id = 'C' AND generation = 1", level=SERIAL)
    numbers = sorted(r["occurrence"] for r in rows)
    opens = [n for kind, n in log if kind == "open"]
    record_evidence("scylla-9a4-concurrent", {"occurrences": numbers, "opens": opens,
                                              "merges": sum(1 for k, _ in log if k == "merge"),
                                              "gave_up": sum(1 for k, _ in log if k == "gave_up")})
    assert numbers == [1, 2, 3, 4, 5]                    # no duplicate, no skipped number
    assert sorted(opens) == [1, 2, 3, 4, 5]               # exactly one opener per occurrence
    assert all(r["detections"] == 5 for r in rows)        # the 5 other detectors of each round merged
    assert not any(k == "gave_up" for k, _ in log)


def test_9a4_walk_fallback(db) -> None:
    """The documented walk: INSERT IF NOT EXISTS upward from occurrence 1 (single-row LWTs only)."""
    for _ in range(3):
        n = 1
        while True:
            r = db.lwt(OPEN, "W", n, uuid.uuid1(), {}, {})
            if r.applied:
                break
            assert r.row["occurrence"] == n  # a failed insert returns the existing row
            n += 1
        close(db, "W", n)
    numbers = sorted(r["occurrence"] for r in db.rows(
        "SELECT occurrence FROM {ks}.index_repairs WHERE spec_digest = 'd' AND role = 'target' AND bucket = 0 "
        "AND subject_id = 'W' AND generation = 1"))
    assert numbers == [1, 2, 3]
