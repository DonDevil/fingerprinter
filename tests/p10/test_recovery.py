"""Index-integrity recovery and task failure semantics on the real stack (validation §9a.1-§9a.3, §9, §9d).

Non-protocol losses are injected by deleting a subject's current points
directly in Qdrant. Interleavings are forced with the implementation's hook
points (``matcher.before_completion_lwt`` = P1/P5, ``reconciler.*``).
Real cached L-PAIRS vectors give true matches; synthetic vectors elsewhere.
"""

from __future__ import annotations

import threading

import pytest

from fingerprinter.p10ctx import KIND_A, KIND_B
from fingerprinter.recovery import CLOSED, OPEN, PENDING, RESOLVED
from fingerprinter.vindex import subject_seq_eq
from tests.p10.conftest import VECTORS
from tests.p10.test_flow import clip, film

pytestmark = pytest.mark.p10_integration


@pytest.fixture(autouse=True)
def _need_vectors():
    if not (VECTORS / "l-pairs").exists():
        pytest.skip("cached L-PAIRS vectors not present")


def occurrences(p10, subject_id):
    return sorted((r for r in p10.sys.recovery.occurrences(p10.spec) if r["subject_id"] == subject_id),
                  key=lambda r: r["occurrence"])


def lose(p10, s):
    """Non-protocol loss: delete S's current points from the ACTIVE generation directly."""
    ptr = p10.sys.gens.active(p10.spec, s.role, fresh=True)
    e = p10.sys.indexer.entry(p10.spec, s.role, s.bucket, s.subject_id)
    p10.index.delete_by_filter(ptr.collection, subject_seq_eq(s.role, s.subject_id, int(e["seq"])))
    assert p10.index.count(ptr.collection, subject_seq_eq(s.role, s.subject_id, int(e["seq"]))) == 0
    return ptr.generation


def has_match(p10, cid, tid) -> bool:
    return any(r["content_id"] == cid and r["target_id"] == tid for r in p10.s.db.rows("SELECT * FROM {ks}.matches"))


@pytest.fixture
def world(p10):
    t = p10.register_target(*film("sintel"), title="Sintel")
    p10.bootstrap()
    p10.drive()
    return p10, t


def test_r17_r25a_loss_before_search_recovered_by_lost_subjects_rerun(world) -> None:
    """T's points lost; a matching clip M runs to DONE without finding T (no race); verification opens
    an occurrence; repair; T's target_vs_media is bumped; its run 2 finds (M, T); CLOSED RERUN_DONE."""
    p10, (tid, ver, _) = world
    t = p10.target(tid, ver)
    g = lose(p10, t)
    m = p10.ingest_media(*clip("lp-000-sintel-re-encode.mp4"))
    p10.sys.discovery.scan_reps(p10.spec)   # discovery only: no verification pass yet
    p10.run_matcher()
    ms = p10.media(m)
    ta = p10.sys.tasks.row(ms.subject_id, p10.spec, KIND_A)
    assert ta["state"] == "DONE" and not has_match(p10, m, tid)       # M DONE, pair hidden by the loss
    assert p10.sys.recovery.settled(ms.subject_id, p10.spec, [KIND_A])["status"] == "settled"  # nothing known yet
    p10.sys.verify_pass()                                               # detection
    occ = occurrences(p10, t.subject_id)
    assert len(occ) == 1 and occ[0]["state"] == OPEN and occ[0]["generation"] == g
    assert p10.sys.recovery.settled(ms.subject_id, p10.spec, [KIND_A])["status"] == "provisional"
    p10.drive()
    occ = occurrences(p10, t.subject_id)
    assert occ[0]["state"] == CLOSED and occ[0]["close_reason"] == "RERUN_DONE" and occ[0]["resolution"] == RESOLVED
    tb = p10.sys.tasks.row(t.subject_id, p10.spec, KIND_B)
    assert tb["run"] == 2 and tb["done_ok_run"] == 2 and tb["rerun_reason"] == "index_repair"
    assert tb["recovery_runs"] == {occ[0]["occurrence_id"]: 2}
    assert has_match(p10, m, tid)                                       # re-covered by T's own rerun
    assert p10.sys.tasks.row(ms.subject_id, p10.spec, KIND_A)["run"] == 1  # M untouched (R4)
    assert p10.sys.recovery.settled(ms.subject_id, p10.spec, [KIND_A])["status"] == "settled"


def test_r26a_other_subjects_completion_races_detection(world) -> None:
    """M blocked at P1 (before its completion LWT) while the verifier records T's loss: M's DONE succeeds
    (expected); the occurrence was durable first; closure only after T's run 2; (M, T) found by it."""
    p10, (tid, ver, _) = world
    t = p10.target(tid, ver)
    lose(p10, t)
    m = p10.ingest_media(*clip("lp-000-sintel-re-encode.mp4"))
    p10.sys.discovery.scan_reps(p10.spec)
    at_p1, release = threading.Event(), threading.Event()
    order: list[str] = []

    def p1(**ctx):
        if ctx["kind"] == KIND_A:
            order.append("M_at_P1")
            at_p1.set()
            release.wait(30)

    p10.hooks.on("matcher.before_completion_lwt", p1)
    worker = threading.Thread(target=p10.run_matcher)
    worker.start()
    assert at_p1.wait(60), ([(p, c.get("kind")) for _, p, c in p10.hooks.log][-12:],
                            [(r["kind"], r["state"], r.get("waiting_reason")) for r in p10.sys.tasks.all()])
    p10.sys.verify_pass()
    occ = occurrences(p10, t.subject_id)
    assert occ and occ[0]["state"] == OPEN
    order.append("occurrence_durable")
    release.set()
    worker.join(60)
    p10.hooks.clear()
    ms = p10.media(m)
    assert order == ["M_at_P1", "occurrence_durable"]
    assert p10.sys.tasks.row(ms.subject_id, p10.spec, KIND_A)["state"] == "DONE"   # not gated (R4)
    assert not has_match(p10, m, tid)
    p10.drive()
    occ = occurrences(p10, t.subject_id)
    assert occ[0]["close_reason"] == "RERUN_DONE"
    assert has_match(p10, m, tid)
    assert p10.sys.tasks.row(t.subject_id, p10.spec, KIND_B)["run"] == 2


def test_r26b_own_run_races_the_bump(world) -> None:
    """T's own re-opened run blocked at P5; the bump comes first: run 1's completion fails on `run`,
    the obligation requires run 2 and only run 2 closes it (S5)."""
    p10, (tid, ver, _) = world
    t = p10.target(tid, ver)
    p10.sys.tasks.rerun(t, KIND_B, reason="audited_rerun", actor="test")   # run 2 (audited)
    lose(p10, t)
    at_p5, release = threading.Event(), threading.Event()

    def p5(**ctx):
        if ctx["kind"] == KIND_B and not release.is_set():
            at_p5.set()
            release.wait(30)

    p10.hooks.on("matcher.before_completion_lwt", p5)
    outcomes: dict = {}
    worker = threading.Thread(target=lambda: outcomes.update(p10.run_matcher()))
    worker.start()
    assert at_p5.wait(60)
    p10.sys.verify_pass()
    occ = occurrences(p10, t.subject_id)[0]
    from fingerprinter.recovery import Occurrence

    o = Occurrence(occ["spec_digest"], occ["role"], occ["bucket"], occ["subject_id"], occ["generation"],
                   occ["occurrence"], occ["occurrence_id"], occ)
    p10.sys.recovery.step(occ)  # mark, repair, bump (order (i): bump first)
    row = p10.sys.recovery.read(o)
    assert row["state"] in ("RERUN_PENDING",) and row["required_run"] == {KIND_B: 3}
    release.set()
    worker.join(120)
    assert not worker.is_alive(), ([(p, c.get("kind"), c.get("run")) for _, p, c in p10.hooks.log][-15:],
                                   [(r["kind"], r["state"], r["run"], r.get("waiting_reason"), r.get("done_run"))
                                    for r in p10.sys.tasks.all()])
    p10.hooks.clear()
    tb = p10.sys.tasks.row(t.subject_id, p10.spec, KIND_B)
    assert outcomes.get("superseded", 0) >= 1                  # run 2's completion LWT was rejected on `run`
    assert tb["run"] == 3 and tb.get("done_run") in (1, 3)     # run 2 never recorded as finished
    p10.drive()
    row = p10.sys.recovery.read(o)
    assert row["state"] == CLOSED and row["resolved_by"] == {KIND_B: 3}


def test_r27_repeated_loss_needs_a_new_run(world) -> None:
    p10, (tid, ver, _) = world
    t = p10.target(tid, ver)
    lose(p10, t)
    p10.sys.verify_pass()
    p10.drive()
    first = occurrences(p10, t.subject_id)
    assert len(first) == 1 and first[0]["close_reason"] == "RERUN_DONE"
    snapshot = dict(first[0])
    lose(p10, t)
    m2 = p10.ingest_media(*clip("lp-000-sintel-re-encode.mp4"))
    p10.sys.discovery.scan_reps(p10.spec)
    p10.run_matcher()
    p10.sys.verify_pass()
    occ = occurrences(p10, t.subject_id)
    assert len(occ) == 2 and occ[1]["occurrence"] == 2 and occ[1]["occurrence_id"] != occ[0]["occurrence_id"]
    assert occ[1]["state"] == OPEN and not occ[1]["required_run"] and occ[1]["resolution"] == PENDING
    assert occ[0] == snapshot                                     # row 1 byte-for-byte unchanged
    p10.drive()
    occ = occurrences(p10, t.subject_id)
    tb = p10.sys.tasks.row(t.subject_id, p10.spec, KIND_B)
    assert occ[1]["required_run"] == {KIND_B: 3} and occ[1]["resolved_by"] == {KIND_B: 3}
    assert tb["recovery_runs"] == {occ[0]["occurrence_id"]: 2, occ[1]["occurrence_id"]: 3}
    assert has_match(p10, m2, tid)
    assert p10.sys.recovery.counters(p10.spec) == (0, 0)


def test_r29_unresolved_close_keeps_negatives_provisional_until_its_own_resolve(world) -> None:
    """u1/v2: T's rerun ends INCOMPLETE (cap 1 with two candidates) ⇒ CLOSED RERUN_INCOMPLETE,
    unresolved; an unrelated subject is provisional; a later DONE run (raised cap) resolves it LATER_RUN."""
    p10, (tid, ver, _) = world
    t = p10.target(tid, ver)
    p10.ingest_media(*clip("lp-000-sintel-re-encode.mp4"))
    p10.ingest_media(*clip("lp-035-sintel-re-encode.webm"))
    x = p10.ingest_media(*clip("lp-029-elephants-dream-pillarbox.webm"))   # the settlement probe
    p10.drive()
    p10.s.db.run("UPDATE {ks}.match_tasks SET cap_override = 1 WHERE subject_id = ? AND spec_digest = ? AND kind = ?",
                 t.subject_id, p10.spec, KIND_B)
    lose(p10, t)
    p10.sys.verify_pass()
    p10.drive()
    occ = occurrences(p10, t.subject_id)[0]
    assert occ["state"] == CLOSED and occ["close_reason"] == "RERUN_INCOMPLETE" and occ["resolution"] == PENDING
    probe = p10.sys.recovery.settled(p10.media(x).subject_id, p10.spec, [KIND_A])
    assert probe["status"] == "provisional" and probe["recovery_unresolved"] == 1
    p10.sys.tasks.rerun(t, KIND_B, reason="raise_cap", actor="test", cap=None)   # `match retry --raise-cap`
    p10.drive()
    occ = occurrences(p10, t.subject_id)[0]
    assert occ["resolution"] == RESOLVED and occ["resolution_reason"] == "LATER_RUN"
    assert occ["resolved_by"][KIND_B] >= occ["required_run"][KIND_B]
    assert p10.sys.recovery.settled(p10.media(x).subject_id, p10.spec, [KIND_A])["status"] == "settled"


def test_g10_corrupt_candidate_fails_only_the_candidate(world) -> None:
    p10, (tid, ver, _) = world
    m = p10.ingest_media(*clip("lp-000-sintel-re-encode.mp4"))
    t = p10.target(tid, ver)
    bucket, key = t.artifact_uri.removeprefix("s3://").split("/", 1)
    data = b"".join(p10.s.objects.stream(bucket, key))
    import io

    p10.s.objects.client.put_object(bucket, key, io.BytesIO(data[:-10] + b"0123456789"), len(data))
    p10.sys.p.artifacts._lru.clear()
    p10.drive()
    ms = p10.media(m)
    ta = p10.sys.tasks.row(ms.subject_id, p10.spec, KIND_A)
    assert ta["state"] == "INCOMPLETE" and ta["incomplete_reason"].startswith("verification_failed")
    cands = p10.s.db.rows("SELECT state, reason FROM {ks}.match_candidates WHERE subject_id = ? ALLOW FILTERING",
                          ms.subject_id)
    assert any(c["state"] == "FAILED" and c["reason"] == "artifact_corrupt" for c in cands)
    # the subject's own corrupt artifact: its own task FAILED (rerun of T)
    p10.sys.tasks.rerun(t, KIND_B, reason="test", actor="test")
    p10.run_matcher()
    tb = p10.sys.tasks.row(t.subject_id, p10.spec, KIND_B)
    assert tb["state"] == "FAILED" and tb["failure"].startswith("artifact_corrupt")


def test_g15_saturation_is_incomplete_never_done(p10) -> None:
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"matching": p10.sys.p.settings.matching.model_copy(
        update={"k0": 1, "k_max_a": 1, "floor_a": -0.9})})
    p10.register_target(*film("sintel"), title="Sintel")
    p10.register_target(*film("elephants-dream"), title="ED")
    m = p10.ingest_media(*clip("lp-000-sintel-re-encode.mp4"))
    p10.bootstrap()
    p10.drive()
    ta = p10.sys.tasks.row(p10.media(m).subject_id, p10.spec, KIND_A)
    assert ta["state"] == "INCOMPLETE" and ta["incomplete_reason"].startswith("saturated_segments")
    st = p10.sys.recovery.settled(p10.media(m).subject_id, p10.spec, [KIND_A])
    assert st["status"] == "provisional" and any("saturated_segments" in r for r in st["reasons"])


def test_k1_k6_k9_first_decision_final_and_ids(world) -> None:
    p10, (tid, ver, _) = world
    m = p10.ingest_media(*clip("lp-000-sintel-re-encode.mp4"))
    p10.drive()
    rows = list(p10.s.db.rows("SELECT * FROM {ks}.matches"))
    assert len(rows) == 1
    from antipiracy_contracts.ids import MatchId, RepresentationId

    r = rows[0]
    assert r["match_id"] == str(MatchId.of(RepresentationId(r["representation_id"]),
                                           RepresentationId(r["target_representation_id"]), r["technique_name"],
                                           r["technique_version"]))
    by_id = p10.s.db.one("SELECT * FROM {ks}.matches_by_id WHERE match_id = ?", r["match_id"])
    assert by_id["content_id"] == m and by_id["target_id"] == tid
    # K-1/K-9: concurrent re-commits of the same match_id are no-ops; a different later result is recorded
    tech = p10.sys.techniques.get("2.1")
    from fingerprinter.verify import verify

    ms, t = p10.media(m), p10.target(tid, ver)
    res = verify(p10.sys.p.artifacts.load_subject(ms), p10.sys.p.artifacts.load_subject(t), tech.params)
    res.score = res.score - 0.5  # a "different later result" (as a different exclusion set would give)
    outs = []
    ths = [threading.Thread(target=lambda: outs.append(p10.sys.committer.commit(ms, t, tech, res, {"kind": "k9"})))
           for _ in range(3)]
    for th in ths:
        th.start()
    for th in ths:
        th.join()
    assert all(o == (r["match_id"], False) for o in outs)
    assert len(list(p10.s.db.rows("SELECT * FROM {ks}.matches"))) == 1
    assert p10.s.db.rows("SELECT * FROM {ks}.matches")[0]["score"] == r["score"]
