"""Matrix cases that were unrun at the first M3 evaluation (validation §11, §13.21) — real stack.

Same conventions as ``test_mutation.py``: a raising hook models a crash at
that boundary, direct Qdrant calls model a paused/stale worker landing an
older operation or a non-protocol change, and wrappers around the adapter
model a dropped response. Every case ends with reconciler passes and the
parts of the §9a common pass condition that apply.
"""

from __future__ import annotations

import threading

import pytest

from fingerprinter.db import LwtUnknownError
from fingerprinter.indexing import APPLIED, BLOCKED, PENDING
from fingerprinter.p10ctx import KIND_A, KIND_B
from fingerprinter.vindex import (
    MEDIA,
    TARGET,
    IndexPermanentError,
    IndexUnavailableError,
    Point,
    subject_filter,
    subject_seq_eq,
    subject_seq_lte,
)
from tests.p10 import synth
from tests.p10.test_mutation import CrashError, active_collection, crash, entry

pytestmark = pytest.mark.p10_integration


def n_points(p10, s, flt=None) -> int:
    _, name = active_collection(p10, s.role)
    return p10.index.count(name, flt or subject_filter(s.role, s.subject_id))


def scroll(p10, s, seq: int) -> list[Point]:
    _, name = active_collection(p10, s.role)
    return list(p10.index.scroll_all(name, subject_seq_eq(s.role, s.subject_id, seq), with_vector=True))


def matches(p10) -> list[dict]:
    return list(p10.s.db.rows("SELECT * FROM {ks}.matches"))


@pytest.fixture
def world(p10):
    """A synthetic target T (300 s) and its index; a clip of T is a true match."""
    t = p10.register_target(*synth.film(31), title="T")
    p10.bootstrap()
    return p10, t


# --- §9a mutation races -----------------------------------------------------------------------


def test_r1_no_qdrant_write_before_the_intent_row_and_unknown_lwt(world, monkeypatch) -> None:
    """R-1: a crash before the intent LWT leaves nothing (no row, no points); the backstop writes the
    intent and applies. Variant: the intent LWT applies but its outcome is unknown — the SERIAL re-read
    decides, and no Qdrant call precedes a row that exists."""
    p10, _ = world
    s = p10.media(p10.ingest_media(*synth.random_subject(101)))
    assert entry(p10, s) is None and n_points(p10, s) == 0       # the crash point: nothing written yet
    real_upsert = p10.index.upsert
    seen: list[bool] = []

    def guarded_upsert(name, points):
        for pt in points:
            if pt.payload.get("rep") == s.subject_id:
                seen.append(entry(p10, s) is not None)
        return real_upsert(name, points)

    monkeypatch.setattr(p10.index, "upsert", guarded_upsert)
    real_lwt = p10.sys.indexer.db.lwt
    fired = []

    def unknown_once(cql, *args, **kw):
        res = real_lwt(cql, *args, **kw)
        if "INSERT INTO {ks}.index_entries" in cql and not fired:
            fired.append(res.applied)
            raise LwtUnknownError("injected: outcome unknown after apply")
        return res

    monkeypatch.setattr(p10.sys.indexer.db, "lwt", unknown_once)
    p10.reconcile()   # backstop scan (force) writes the intent, the intents pass applies
    monkeypatch.setattr(p10.sys.indexer.db, "lwt", real_lwt)
    p10.reconcile()
    e = entry(p10, s)
    assert fired, "the unknown-outcome path was not exercised"
    assert e["state"] == APPLIED and e["seq"] == 1                 # one intent despite the unknown outcome
    assert seen and all(seen)                                      # never a Qdrant write before the row
    assert n_points(p10, s) == s.segment_count


def test_r14_r19_stale_upsert_after_delete_dropped_then_removed(world) -> None:
    """R-14/R-19: T indexed at seq s; T retired ⇒ DELETE s+1 applied; a worker holding the old UPSERT
    lands T's seq-s points afterwards. While stale, a media task hitting T drops it (authoritative
    liveness) and no match is written; one verification cycle removes the points; the DELETE row keeps
    ``applied[g] = s+1``."""
    p10, (tid, ver, _) = world
    t = p10.target(tid, ver)
    s_up = int(entry(p10, t)["seq"])
    stale = scroll(p10, t, s_up)
    assert len(stale) == t.segment_count
    p10.s.targets.retire(tid, reason="test", actor="test")
    t2 = p10.target(tid, ver)
    p10.sys.indexer.ensure(t2)
    e = entry(p10, t2)
    g, name = active_collection(p10, TARGET)
    assert e["desired_op"] == "DELETE" and e["seq"] == s_up + 1 and e["applied"][g] == s_up + 1
    assert n_points(p10, t2) == 0
    p10.index.upsert(name, stale)                                   # W lands its stale UPSERT
    assert n_points(p10, t2) == t.segment_count
    m = p10.ingest_media(*synth.clip_of(31, 40.0, 90.0))
    p10.sys.discovery.desire(p10.media(m))
    p10.run_matcher()
    ta = p10.sys.tasks.row(p10.media(m).subject_id, p10.spec, KIND_A)
    cands = p10.s.db.rows("SELECT state, reason FROM {ks}.match_candidates WHERE subject_id = ? AND spec_digest = ? "
                          "AND kind = ? ALLOW FILTERING", ta["subject_id"], p10.spec, KIND_A)
    assert ta["state"] == "DONE" and cands and all(c["state"] == "DROPPED" for c in cands)
    assert {c["reason"] for c in cands} == {"subject_not_live"}
    assert not matches(p10)
    res = p10.sys.checker.verify_generation(p10.spec, TARGET, g)
    assert res["deleted_rows_points"] == t.segment_count
    assert n_points(p10, t2) == 0
    e = entry(p10, t2)
    assert e["desired_op"] == "DELETE" and e["applied"][g] == s_up + 1 and e["state"] == APPLIED


def test_r15_dropped_response_and_permanent_error(world, monkeypatch) -> None:
    """R-15: the response of an applied UPSERT is lost ⇒ retried, no duplicate points, ``attempts`` +1;
    a permanent 4xx ⇒ BLOCKED with the error and not retried by the intents pass."""
    p10, _ = world
    s = p10.media(p10.ingest_media(*synth.random_subject(102)))
    real = p10.index.upsert
    calls = []

    def drop_response(name, points):
        real(name, points)
        calls.append(len(points))
        if len(calls) == 1:
            raise IndexUnavailableError("injected: response dropped after the server applied")

    monkeypatch.setattr(p10.index, "upsert", drop_response)
    p10.sys.indexer.write_intent(s)
    with pytest.raises(Exception, match="unavailable|response dropped"):
        p10.sys.indexer.apply(p10.spec, MEDIA, s.bucket, s.subject_id)
    e = entry(p10, s)
    assert e["state"] == PENDING and int(e["attempts"]) == 1
    p10.s.db.run("UPDATE {ks}.index_entries SET next_attempt_at = null WHERE spec_digest = ? AND role = ? "
                 "AND bucket = ? AND subject_id = ?", p10.spec, MEDIA, s.bucket, s.subject_id)  # skip the backoff
    p10.sys.intents_pass()
    assert entry(p10, s)["state"] == APPLIED
    assert n_points(p10, s) == s.segment_count                     # no duplicates (deterministic ids)
    # permanent error
    s2 = p10.media(p10.ingest_media(*synth.random_subject(103)))

    def permanent(name, points):
        raise IndexPermanentError("PUT points: 400 wrong vector dimension (injected)")

    monkeypatch.setattr(p10.index, "upsert", permanent)
    p10.sys.indexer.write_intent(s2)
    with pytest.raises(IndexPermanentError):
        p10.sys.indexer.apply(p10.spec, MEDIA, s2.bucket, s2.subject_id)
    e2 = entry(p10, s2)
    assert e2["state"] == BLOCKED and e2["blocked_reason"] == "permanent" and "400" in e2["last_error"]
    monkeypatch.setattr(p10.index, "upsert", real)
    assert n_points(p10, s2) == 0


def test_r18_stale_lower_seq_write_lands_as_separate_points(world) -> None:
    """R-18: UPSERT s+1 supersedes s while the s worker is paused; the s worker then lands its write. Its
    points are **separate** (ids contain seq) and never overwrite seq s+1 (the s+1 count stays
    ``segment_count``); cleanup leaves only s+1. The Redis mutation lease does not fence the stale write."""
    p10, _ = world
    s = p10.media(p10.ingest_media(*synth.random_subject(104)))
    p10.sys.indexer.ensure(s)
    s1 = int(entry(p10, s)["seq"])
    stale = scroll(p10, s, s1)                    # what the paused s worker holds
    # a newer intent of S (injected desired-version change, C-14q) applied by a second worker
    p10.s.db.lwt("UPDATE {ks}.index_entries SET desired_ver = ?, seq = ?, state = 'PENDING' WHERE spec_digest = ? "
                 "AND role = ? AND bucket = ? AND subject_id = ? IF seq = ?", "forced-newer", s1 + 1, p10.spec,
                 MEDIA, s.bucket, s.subject_id, s1)
    g, name = active_collection(p10, MEDIA)
    # the second worker applies seq s1+1 directly (as apply would: upsert + cleanup of seq < s1+1)
    newer = [Point(p.id.replace(p.id, __import__("fingerprinter.vindex", fromlist=["point_id"]).point_id(
        s.subject_id, s1 + 1, int(p.payload["k"]))), p.vector, {**p.payload, "seq": s1 + 1}) for p in stale]
    p10.index.upsert(name, newer)
    p10.index.delete_by_filter(name, {"must": [{"key": "rep", "match": {"value": s.subject_id}},
                                               {"key": "seq", "range": {"lt": s1 + 1}}]})
    assert p10.index.count(name, subject_seq_eq(MEDIA, s.subject_id, s1 + 1)) == s.segment_count
    p10.index.upsert(name, stale)                 # the stale s worker resumes and writes
    assert p10.index.count(name, subject_seq_eq(MEDIA, s.subject_id, s1 + 1)) == s.segment_count  # untouched
    assert p10.index.count(name, subject_seq_eq(MEDIA, s.subject_id, s1)) == s.segment_count      # separate
    res = p10.sys.checker.verify_generation(p10.spec, MEDIA, g)
    assert res["stale"] == s.segment_count
    assert p10.index.count(name, subject_seq_eq(MEDIA, s.subject_id, s1)) == 0
    assert p10.index.count(name, subject_seq_eq(MEDIA, s.subject_id, s1 + 1)) == s.segment_count


# --- §9 G-* ----------------------------------------------------------------------------------


def test_g3_corruption_missing_subset_and_altered_payload(world) -> None:
    """G-3: delete a subset of a subject's points and alter another point's payload; verification
    reports the short subject (an occurrence) and repairs it; a candidate built from a point whose
    payload no longer matches its id is dropped as foreign, never accepted."""
    p10, (tid, ver, _) = world
    t = p10.target(tid, ver)
    g, name = active_collection(p10, TARGET)
    pts = scroll(p10, t, int(entry(p10, t)["seq"]))
    p10.index.delete_ids(name, [p.id for p in pts[:5]])
    victim = pts[10]
    p10.index.upsert(name, [Point(victim.id, victim.vector, {**victim.payload, "k": 9999})])   # altered payload
    res = p10.sys.checker.verify_generation(p10.spec, TARGET, g)
    assert res["short"] == 1
    assert p10.sys.recovery.counters(p10.spec)[0] >= 1            # an OPEN occurrence
    m = p10.ingest_media(*synth.clip_of(31, 40.0, 90.0))
    p10.sys.discovery.desire(p10.media(m))
    p10.run_matcher()
    ta = p10.sys.tasks.row(p10.media(m).subject_id, p10.spec, KIND_A)
    cands = p10.s.db.rows("SELECT * FROM {ks}.match_candidates WHERE subject_id = ? AND spec_digest = ? AND kind = ? "
                          "ALLOW FILTERING", ta["subject_id"], p10.spec, KIND_A)
    for c in cands:
        import json

        if json.loads(c["other"]).get("point_id") == victim.id:
            assert c["state"] == "DROPPED" and c["reason"] == "foreign_point"
    p10.drive()
    assert n_points(p10, t, subject_seq_eq(TARGET, t.subject_id, int(entry(p10, t)["seq"]))) == t.segment_count
    assert p10.sys.recovery.counters(p10.spec) == (0, 0)
    assert any(r["content_id"] == m for r in matches(p10))         # the pair is found after repair


def test_g5_duplicate_hints_and_target_events_one_task_one_decision(world) -> None:
    """G-5: replayed ``representation.ready`` entries and a duplicate target registration observation ⇒
    one task per (subject, kind), one decision per match_id."""
    p10, (tid, ver, _) = world
    m = p10.ingest_media(*synth.clip_of(31, 40.0, 90.0))
    client = p10.sys.p.coord.client
    from fingerprinter.publish import READY

    stream = p10.sys.p.coord.stream(READY)
    entries = client.xrange(stream, min="-", max="+")
    for _ in range(2):
        for _eid, fields in entries:
            client.xadd(stream, fields)                              # replay every hint twice
    p10.drive()
    p10.sys.discovery.desire(p10.target(tid, ver))                 # duplicate target observation
    p10.sys.discovery.scan_targets()
    p10.drive()
    ms = p10.media(m)
    assert p10.sys.tasks.row(ms.subject_id, p10.spec, KIND_A)["run"] == 1
    rows = [r for r in matches(p10) if r["content_id"] == m]
    assert len(rows) == 1
    assert len({r["match_id"] for r in matches(p10)}) == len(matches(p10))


def test_g6_concurrent_retries_one_decision(world) -> None:
    """G-6: ``match retry`` twice concurrently ⇒ one run bump each at most, one decision."""
    p10, _ = world
    m = p10.ingest_media(*synth.clip_of(31, 40.0, 90.0))
    p10.drive()
    ms = p10.media(m)
    before = matches(p10)
    assert len([r for r in before if r["content_id"] == m]) == 1
    out = []
    barrier = threading.Barrier(2)

    def retry():
        barrier.wait()
        out.append(p10.sys.tasks.rerun(ms, KIND_A, reason="operator_retry", actor="test"))

    th = [threading.Thread(target=retry) for _ in range(2)]
    for x in th:
        x.start()
    for x in th:
        x.join()
    p10.drive()
    row = p10.sys.tasks.row(ms.subject_id, p10.spec, KIND_A)
    assert row["state"] == "DONE" and row["run"] in (2, 3)
    assert [r["match_id"] for r in matches(p10) if r["content_id"] == m] == \
           [r["match_id"] for r in before if r["content_id"] == m]


def test_g7_concurrent_indexers_identical_points(world) -> None:
    """G-7: two matchers index the same subject concurrently ⇒ one point set (deterministic ids)."""
    p10, _ = world
    s = p10.media(p10.ingest_media(*synth.random_subject(105, 12)))
    barrier = threading.Barrier(2)
    errs = []

    def go():
        barrier.wait()
        try:
            p10.sys.indexer.ensure(s)
        except Exception as exc:  # noqa: BLE001 - recorded
            errs.append(exc)

    th = [threading.Thread(target=go) for _ in range(2)]
    for x in th:
        x.start()
    for x in th:
        x.join()
    p10.reconcile()
    e = entry(p10, s)
    assert e["state"] == APPLIED and e["seq"] == 1
    assert n_points(p10, s) == s.segment_count == len({p.id for p in scroll(p10, s, 1)})
    assert all(isinstance(x, LwtUnknownError | Exception) for x in errs)


def test_g8_retired_target_points_removed_stale_candidates_dropped(world) -> None:
    """G-8: retire a target: its points are removed by the DELETE intent; a search that still hits a
    stale point (before the delete is applied) drops the candidate and writes no match."""
    p10, (tid, ver, _) = world
    t = p10.target(tid, ver)
    p10.s.targets.retire(tid, reason="test", actor="test")
    m = p10.ingest_media(*synth.clip_of(31, 40.0, 90.0))
    p10.sys.discovery.desire(p10.media(m))
    p10.run_matcher()                                             # T's points still present (no reconcile)
    ms = p10.media(m)
    cands = p10.s.db.rows("SELECT state, reason FROM {ks}.match_candidates WHERE subject_id = ? AND spec_digest = ? "
                          "AND kind = ? ALLOW FILTERING", ms.subject_id, p10.spec, KIND_A)
    assert cands and {c["reason"] for c in cands} == {"subject_not_live"}
    assert not matches(p10)
    p10.drive()
    assert n_points(p10, t) == 0 and entry(p10, p10.target(tid, ver))["desired_op"] == "DELETE"


# --- §9d K-* ---------------------------------------------------------------------------------


def test_k3_two_target_versions_two_rows(world, tmp_path) -> None:
    """K-3: the same content against two versions of one target ⇒ two ``matches`` rows."""
    p10, (tid, ver, _) = world
    from fingerprinter.coord import uuid7

    path = tmp_path / "v2.bin"
    path.write_bytes(b"target v2 " + uuid7().encode())
    reg = p10.s.targets.register(path, title="T", kind="movie", target_id=tid, actor="test")
    req = next(r for r in p10.s.requests.all() if r["content_id"] == reg.content_id)
    row = p10.s.requests.claim(reg.content_id, req["spec_key"], uuid7())
    assert p10.s.committer.commit(row, p10.s.spec, p10.artifact(*synth.film(31))) == "won"
    assert reg.version == ver + 1
    m = p10.ingest_media(*synth.clip_of(31, 40.0, 90.0))
    p10.drive()
    rows = [r for r in matches(p10) if r["content_id"] == m]
    assert sorted(r["target_version"] for r in rows) == [ver, ver + 1]
    assert len({r["match_id"] for r in rows}) == 2


def test_k5_new_technique_version_new_rows_old_unchanged(world) -> None:
    """K-5: a new technique version gives new rows (new match_ids); the old rows are unchanged."""
    p10, _ = world
    m = p10.ingest_media(*synth.clip_of(31, 40.0, 90.0))
    p10.drive()
    old = {r["match_id"]: dict(r) for r in matches(p10) if r["content_id"] == m}
    assert len(old) == 1
    tech = p10.sys.techniques.get("2.1")
    p10.sys.techniques.register("2.1-k5test", tech.params, calibration=None, status="UNAVAILABLE",
                                calibration_id=None, actor="test", evidence="K-5 test-only version")
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"matching": p10.sys.p.settings.matching.model_copy(
        update={"technique_version": "2.1-k5test"})})
    p10.sys.tasks.rerun(p10.media(m), KIND_A, reason="k5", actor="test")
    p10.drive()
    rows = [r for r in matches(p10) if r["content_id"] == m]
    new = [r for r in rows if r["technique_version"] == "2.1-k5test"]
    assert len(new) == 1 and new[0]["match_id"] not in old
    for r in rows:
        if r["match_id"] in old:
            assert {k: r[k] for k in ("verdict", "score", "decided_at")} == \
                   {k: old[r["match_id"]][k] for k in ("verdict", "score", "decided_at")}


def test_k7_crash_between_matches_and_reverse_rows(world) -> None:
    """K-7: crash after the ``matches`` row, before ``matches_by_target``/``matches_by_id`` ⇒ the intent
    pass completes both, no duplicates."""
    p10, _ = world
    fired = []

    def once(match_id, **_):
        if not fired:   # the crash point: the matches row exists, the reverse rows do not
            fired.append((match_id, p10.s.db.one("SELECT * FROM {ks}.matches_by_id WHERE match_id = ?", match_id)))
            raise CrashError("after the matches row")

    p10.hooks.on("committer.after_matches_row", once)
    m = p10.ingest_media(*synth.clip_of(31, 40.0, 90.0))
    p10.drive()
    p10.hooks.clear()
    p10.drive()
    assert fired and fired[0][1] is None
    rows = [r for r in matches(p10) if r["content_id"] == m]
    mid = rows[0]["match_id"]
    assert mid == fired[0][0]
    assert len([r for r in matches(p10) if r["content_id"] == m]) == 1
    assert p10.s.db.one("SELECT * FROM {ks}.matches_by_id WHERE match_id = ?", mid) is not None
    assert len(p10.s.db.rows("SELECT * FROM {ks}.matches_by_target WHERE target_id = ? ALLOW FILTERING",
                             rows[0]["target_id"])) == 1
    assert not p10.sys.committer.intents()


# --- §9e CAP-* -------------------------------------------------------------------------------


def test_cap7_blocked_subject_applies_after_release(p10) -> None:
    """CAP-7: a subject BLOCKED on capacity is applied by the reconciler once capacity is released
    (a target retired, its points deleted, the audit lowers the ledger); its task resumes."""
    t = p10.register_target(*synth.film(32, 60), title="T")
    p10.bootstrap()
    used = p10.sys.capacity.read()["reserved"]
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"index": p10.sys.p.settings.index.model_copy(
        update={"max_points": 250_000, "capacity_margin_points": 250_000 - used - 5})})
    p10.sys.capacity.ix = p10.sys.p.settings.index
    s = p10.media(p10.ingest_media(*synth.random_subject(106, 8)))
    p10.sys.discovery.desire(s)
    p10.run_matcher()
    assert entry(p10, s)["state"] == BLOCKED and entry(p10, s)["blocked_reason"] == "capacity"
    assert p10.sys.tasks.row(s.subject_id, p10.spec, KIND_A)["state"] == "WAITING_INDEX"
    p10.s.targets.retire(t[0], reason="free capacity", actor="test")
    p10.drive()
    p10.sys.capacity.audit()
    # design §13.3: an admitted generation stays counted at max(holder, count) until it is RETIRED, so a
    # retirement inside it releases nothing yet (validation §13.21, CAP-7 finding) ...
    assert entry(p10, s)["state"] == BLOCKED
    # ... the release is the generation turnover: rebuild the target role, retire the old generation
    res = p10.sys.rebuild(p10.spec, TARGET, promote=True, wait_boundary=False)
    assert res["promoted"], res
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"index": p10.sys.p.settings.index.model_copy(
        update={"drain_grace_s": 0.0})})
    import time

    time.sleep(2.5)
    p10.sys.generations_pass()
    p10.sys.capacity.audit()
    p10.s.db.run("UPDATE {ks}.index_entries SET next_attempt_at = null WHERE spec_digest = ? AND role = ? "
                 "AND bucket = ? AND subject_id = ?", p10.spec, MEDIA, s.bucket, s.subject_id)
    p10.drive()
    assert entry(p10, s)["state"] == APPLIED and n_points(p10, s) == s.segment_count
    assert p10.sys.tasks.row(s.subject_id, p10.spec, KIND_A)["state"] in ("DONE", "INCOMPLETE")


def test_cap8_limit_only_from_a_passing_e_r1_report(tmp_path) -> None:
    """CAP-8 (unit): a max_points other than the provisional 250,000 needs a readable, passing,
    gate-eligible E-R1 report that measured at least that total; a label is not evidence."""
    import json

    from fingerprinter.config import Settings

    def ok(mp, ev):
        try:
            Settings.model_validate({"index": {"max_points": mp, "max_points_evidence": ev}})
            return True
        except Exception:  # noqa: BLE001
            return False

    assert ok(250_000, None)
    assert not ok(300_000, None) and not ok(300_000, "E-R1 run 7")
    failing = tmp_path / "fail.json"
    failing.write_text(json.dumps({"benchmark": "E-R1", "pass": False, "gate_eligible": True,
                                   "measured_max_resident_total": 900_000}))
    assert not ok(300_000, str(failing))
    passing = tmp_path / "pass.json"
    passing.write_text(json.dumps({"benchmark": "E-R1", "pass": True, "gate_eligible": True,
                                   "measured_max_resident_total": 900_000}))
    assert ok(900_000, str(passing)) and not ok(900_001, str(passing))


# --- §9b H-* ---------------------------------------------------------------------------------


def test_h8_eventual_catch_up_without_the_hint_reader(p10) -> None:
    """H-8: with the hint reader stopped entirely, every READY representation of a 200-item corpus gets
    an index entry and tasks by the end of the first backstop cycle that starts after ingestion."""
    t = p10.register_target(*synth.film(33, 60), title="T")
    p10.bootstrap()
    cids = [p10.ingest_media(*synth.random_subject(1000 + i, 3)) for i in range(200)]
    p10.sys.discovery.read_hints = lambda **_: {"stopped": 1}      # the reader never runs
    for spec in p10.sys.p.indexed_specs():
        while not p10.sys.discovery.scan_reps(spec).get("cycle_complete"):   # one backstop cycle
            pass
    missing = [c for c in cids if entry(p10, p10.media(c)) is None
               or p10.sys.tasks.row(p10.media(c).subject_id, p10.spec, KIND_A) is None]
    assert not missing, f"{len(missing)} of 200 not discovered"
    assert t and not [c for c in cids if p10.sys.tasks.row(p10.media(c).subject_id, p10.spec, KIND_B)]


def test_h6_reps_pagination_interrupted_mid_bucket_resumes(p10) -> None:
    """H-6: buckets with more than one page, the scan interrupted mid-cycle (its cursors persisted); a
    fresh Discovery (empty caches) resumes at the persisted ``(bucket, last content_id)`` and the cycle
    visits every row at least once (row-visit log)."""
    from fingerprinter.discovery import Discovery

    t = p10.register_target(*synth.film(34, 60), title="T")
    p10.bootstrap()
    cids = {p10.ingest_media(*synth.random_subject(2000 + i, 2)) for i in range(150)}
    rc = p10.sys.p.settings.matcher_reconcile
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"matcher_reconcile": rc.model_copy(
        update={"scan_page": 1})})                                   # 150 rows / 64 buckets: > 1 page per bucket
    visited: list[str] = []

    def logging(d):
        real = d.desire

        def desire(s):
            visited.append(s.content_id)
            return real(s)
        d.desire = desire
        return d

    first = logging(p10.sys.discovery)
    spec = p10.spec
    out = first.scan_reps(spec, max_rows=40)                         # "crash" after 40 rows, cursors persisted
    assert out.get("cycle_complete") == 0 and out["rows"] == 40
    stamps = p10.s.db.rows("SELECT * FROM {ks}.p10_scans WHERE scan = ? ALLOW FILTERING", "reps")
    assert any(r.get("last_key") for r in stamps)
    fresh = logging(Discovery(p10.sys.p, p10.sys.indexer, p10.sys.tasks, p10.sys.lowinfo))
    assert fresh.scan_reps(spec) == {"not_owner": 1}                 # the crashed scanner's lease still holds
    p10.s.clock.advance(seconds=rc.lease_ttl_s + 1)                  # ... until it expires
    outs = []
    for _ in range(500):
        outs.append(fresh.scan_reps(spec))
        if outs[-1].get("cycle_complete"):
            break
    assert "not_owner" not in outs[0] and outs[-1].get("cycle_complete") == 1
    media = {c for c in visited}
    assert cids <= media, f"{len(cids - media)} rows never visited in the cycle"
    assert t


# --- §9a generation lifecycle -----------------------------------------------------------------


def _one_active(p10, role: str) -> int:
    _, gens = p10.sys.gens.partition(p10.spec, role)
    act = [g for g, r in gens.items() if r["state"] == "ACTIVE"]
    assert len(act) == 1, gens
    return act[0]


def test_r8_crash_during_validation_never_promoted_then_revalidated(world, monkeypatch) -> None:
    """R-8: kill during validation ⇒ g stays non-active; promotion refuses; the next build adopts g,
    validates it in full and promotes once."""
    p10, _ = world
    p10.ingest_media(*synth.random_subject(110))
    p10.reconcile()
    old = _one_active(p10, MEDIA)
    real = p10.sys.builder.validate

    def die(*a, **k):
        raise CrashError("killed during validation")

    monkeypatch.setattr(p10.sys.builder, "validate", die)
    with pytest.raises(CrashError):
        p10.sys.rebuild(p10.spec, MEDIA, promote=True, wait_boundary=False)
    _, gens = p10.sys.gens.partition(p10.spec, MEDIA)
    g2 = max(gens)
    assert gens[g2]["state"] == "VALIDATING"
    assert not p10.sys.gens.promote(p10.spec, MEDIA, g2)          # never promoted on a partial validation
    assert _one_active(p10, MEDIA) == old
    monkeypatch.setattr(p10.sys.builder, "validate", real)
    res = p10.sys.rebuild(p10.spec, MEDIA, promote=True, wait_boundary=False)
    assert res["generation"] == g2 and res["state"] == "READY" and res["promoted"] and res["pass"]
    assert _one_active(p10, MEDIA) == g2


def test_r9_ready_not_promoted_live_writes_revalidation_promote_once(world) -> None:
    """R-9: g READY, crash before the promotion LWT; live writes continue to both generations; a
    validation older than ``revalidate_after_s`` is not promoted (back to VALIDATING, re-validated);
    then exactly one promotion: one ACTIVE, ``active_epoch`` +1 once."""
    p10, _ = world
    res = p10.sys.rebuild(p10.spec, MEDIA, promote=False, wait_boundary=False)
    g2 = res["generation"]
    assert res["state"] == "READY"
    s = p10.media(p10.ingest_media(*synth.random_subject(111)))
    p10.drive()                                                    # live write after READY
    e = entry(p10, s)
    assert e["applied"][g2] == e["seq"] and e["state"] == APPLIED
    epoch0 = int(p10.sys.gens.partition(p10.spec, MEDIA)[0]["active_epoch"])
    ix = p10.sys.p.settings.index
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"index": ix.model_copy(
        update={"revalidate_after_s": 0.001})})
    import time

    time.sleep(0.05)
    assert not p10.sys.gens.promote(p10.spec, MEDIA, g2)
    assert p10.sys.gens.row(p10.spec, MEDIA, g2)["state"] == "VALIDATING"
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"index": ix})
    res = p10.sys.rebuild(p10.spec, MEDIA, promote=False, wait_boundary=False)
    assert res["generation"] == g2 and res["state"] == "READY"
    oks = [p10.sys.gens.promote(p10.spec, MEDIA, g2) for _ in range(2)]
    assert oks == [True, False]
    assert int(p10.sys.gens.partition(p10.spec, MEDIA)[0]["active_epoch"]) == epoch0 + 1
    assert _one_active(p10, MEDIA) == g2
    _, name = active_collection(p10, MEDIA)
    assert p10.index.count(name, subject_seq_eq(MEDIA, s.subject_id, e["seq"])) == s.segment_count


def test_r7b_stale_upsert_during_catch_up_never_promoted_with_s(world) -> None:
    """R-7 (b): during the rebuild of the target role, T is retired (DELETE s+1 applied to every routed
    generation) and a stale pass then lands T's seq-s points in the new generation; g is promoted only
    with T absent and no match is made with T."""
    p10, (tid, ver, _) = world
    t = p10.target(tid, ver)
    stale = scroll(p10, t, int(entry(p10, t)["seq"]))
    state = {"done": False}

    def inject(generation, bucket):
        if state["done"] or bucket != 63:
            return
        state["done"] = True
        p10.s.targets.retire(tid, reason="R-7", actor="test")
        p10.sys.indexer.ensure(p10.target(tid, ver))               # DELETE s+1 to every routed generation
        row = p10.sys.gens.row(p10.spec, TARGET, generation)
        p10.index.upsert(row["collection"], stale)                 # the stale pass lands in g

    p10.hooks.on("builder.after_bucket", inject)
    res = p10.sys.rebuild(p10.spec, TARGET, promote=True, wait_boundary=False)
    p10.hooks.clear()
    g = res["generation"]
    row = p10.sys.gens.row(p10.spec, TARGET, g)
    present = p10.index.count(row["collection"], subject_filter(TARGET, t.subject_id))
    assert state["done"]
    if res.get("promoted"):
        assert present == 0, "promoted with the stale points of a retired subject"
    else:
        p10.drive()
        p10.sys.rebuild(p10.spec, TARGET, promote=True, wait_boundary=False)
    act = _one_active(p10, TARGET)
    arow = p10.sys.gens.row(p10.spec, TARGET, act)
    assert p10.index.count(arow["collection"], subject_filter(TARGET, t.subject_id)) == 0
    m = p10.ingest_media(*synth.clip_of(31, 40.0, 90.0))
    p10.drive()
    assert not [r for r in matches(p10) if r["content_id"] == m]


# --- §9e capacity ----------------------------------------------------------------------------


def test_cap2_cap6_counted_states_and_release_only_by_audit(world, monkeypatch) -> None:
    """CAP-2: points of a FAILED generation whose collection still exists are counted, the ledger is ≥ Σ
    exact counts at every step. CAP-6: promotion leaves the ledger unchanged; it decreases only by an
    audit after the old collection's deletion is confirmed."""
    p10, _ = world

    def resident() -> int:
        return sum(p10.index.count(c) for c in p10.index.created if p10.index.collection_exists(c))

    real = p10.sys.builder.validate
    monkeypatch.setattr(p10.sys.builder, "validate", lambda *a, **k: {"pass": False, "lag": False,
                                                                    "failures": ["injected"]})
    res = p10.sys.rebuild(p10.spec, MEDIA, promote=False, wait_boundary=False)
    monkeypatch.setattr(p10.sys.builder, "validate", real)
    assert res["state"] == "FAILED"
    failed = p10.sys.gens.row(p10.spec, MEDIA, res["generation"])
    assert p10.index.collection_exists(failed["collection"])
    a = p10.sys.capacity.audit()
    assert a.bound >= resident() and p10.index.count(failed["collection"]) > 0
    assert any(str(res["generation"]) in h for h in p10.sys.capacity.read()["holders"])   # still held
    p10.sys.drop_failed_generation(p10.spec, MEDIA, res["generation"], actor="test")
    p10.sys.capacity.audit()
    assert p10.sys.capacity.read()["reserved"] >= resident()
    # CAP-6
    res = p10.sys.rebuild(p10.spec, MEDIA, promote=False, wait_boundary=False)
    before = p10.sys.capacity.read()["reserved"]
    assert p10.sys.gens.promote(p10.spec, MEDIA, res["generation"])
    assert p10.sys.capacity.read()["reserved"] == before          # unchanged at promotion
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"index": p10.sys.p.settings.index.model_copy(
        update={"drain_grace_s": 0.0})})
    import time

    time.sleep(2.5)
    p10.sys.generations_pass()                                      # DRAINING -> RETIRED, collection deleted
    assert p10.sys.capacity.read()["reserved"] == before           # not lowered by retirement itself
    p10.sys.capacity.audit()
    after = p10.sys.capacity.read()["reserved"]
    assert after < before and after >= resident()


# --- §9 G-13 ---------------------------------------------------------------------------------


def test_g13_verification_timeout_incomplete_nothing_dropped(world) -> None:
    """G-13: a tiny verification timeout ⇒ every candidate FAILED ``verification_timeout`` (attempted,
    recorded), the run INCOMPLETE ``verification_failed``; nothing silently dropped, no decision."""
    p10, _ = world
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"matching": p10.sys.p.settings.matching.model_copy(
        update={"verify_timeout_s": 1e-9})})
    m = p10.ingest_media(*synth.clip_of(31, 40.0, 90.0))
    p10.drive()
    ms = p10.media(m)
    row = p10.sys.tasks.row(ms.subject_id, p10.spec, KIND_A)
    cands = p10.s.db.rows("SELECT state, reason FROM {ks}.match_candidates WHERE subject_id = ? AND spec_digest = ? "
                          "AND kind = ? ALLOW FILTERING", ms.subject_id, p10.spec, KIND_A)
    assert cands and all(c["state"] == "FAILED" and c["reason"] == "verification_timeout" for c in cands)
    assert row["state"] == "INCOMPLETE" and row["incomplete_reason"] == f"verification_failed={len(cands)}"
    assert not [r for r in matches(p10) if r["content_id"] == m]
    st = p10.sys.recovery.settled(ms.subject_id, p10.spec, [KIND_A])
    assert st["status"] == "provisional"


# --- §9b H-7 ---------------------------------------------------------------------------------


def test_h7_targets_without_stream_events_discovered_rev_once(world) -> None:
    """H-7: targets registered/retired without any stream event reaching P10 (P9 holds them) are found
    by the targets scan; the target-set revision bumps once per change (not per scan)."""
    p10, _ = world
    rev0 = p10.sys.lowinfo.observe(p10.spec)
    tid, ver, _ = p10.register_target(*synth.film(35, 60), title="X")
    x = p10.target(tid, ver)
    p10.sys.discovery.read_hints = lambda **_: {"stopped": 1}
    p10.sys.discovery.scan_targets()
    assert entry(p10, x) is not None and p10.sys.tasks.row(x.subject_id, p10.spec, KIND_B) is not None
    rev1 = p10.sys.lowinfo.observe(p10.spec)
    p10.sys.discovery.scan_targets()
    assert p10.sys.lowinfo.observe(p10.spec) == rev1 == rev0 + 1
    p10.s.targets.retire(tid, reason="H-7", actor="test")
    p10.sys.discovery.scan_targets()
    assert entry(p10, p10.target(tid, ver))["desired_op"] == "DELETE"
    assert p10.sys.lowinfo.observe(p10.spec) == rev1 + 1
