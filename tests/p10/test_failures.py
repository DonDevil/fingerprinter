"""Failure and recovery of the matcher (validation §9 G-1, G-9, G-11, G-14) on the real stack.

G-1 makes the index unreachable from the matcher's side (connection refused
on a dead port — what a stopped container looks like to the client) and,
separately, fails exactly one segment query of a search step. G-9 fails
Scylla for the commit right after its intent. G-11 crashes a worker after a
verification page and lets its lease expire (shifted clock). G-14 runs two
matcher "hosts" (separate P10System instances, separate instance ids) on
the same durable state.
"""

from __future__ import annotations

import threading

import pytest

from fingerprinter.errors import StorageUnavailableError
from fingerprinter.p10ctx import KIND_A, P10
from fingerprinter.p10system import P10System
from fingerprinter.vindex import IndexUnavailableError, QdrantIndex
from tests.p10.conftest import VECTORS
from tests.p10.test_flow import clip, film

pytestmark = pytest.mark.p10_integration


@pytest.fixture(autouse=True)
def _need_vectors():
    if not (VECTORS / "l-pairs").exists():
        pytest.skip("cached L-PAIRS vectors not present")


def matches(p10):
    return list(p10.s.db.rows("SELECT * FROM {ks}.matches"))


@pytest.fixture
def world(p10):
    p10.register_target(*film("sintel"), title="Sintel")
    p10.bootstrap()
    p10.drive()
    return p10


def test_g1_index_unreachable_waits_then_resumes(world) -> None:
    p10 = world
    m = p10.ingest_media(*clip("lp-000-sintel-re-encode.mp4"))
    real = p10.sys.p.index
    p10.sys.p.index = QdrantIndex("http://127.0.0.1:1", None, timeout_s=1)   # nothing listens: unavailable
    p10.sys.discovery.scan_reps(p10.spec)
    p10.run_matcher()
    t = p10.sys.tasks.row(p10.media(m).subject_id, p10.spec, KIND_A)
    assert t["state"] == "WAITING_INDEX" and t["waiting_reason"].startswith("unavailable")
    assert not [r for r in matches(p10) if r["content_id"] == m]          # no decision, no fallback
    p10.sys.p.index = real
    p10.drive()
    t = p10.sys.tasks.row(p10.media(m).subject_id, p10.spec, KIND_A)
    assert t["state"] == "DONE" and len([r for r in matches(p10) if r["content_id"] == m]) == 1


def test_g1b_one_failed_segment_query_fails_the_step(world) -> None:
    p10 = world
    m = p10.ingest_media(*clip("lp-000-sintel-re-encode.mp4"))
    real = p10.sys.p.index.search_batch
    calls = {"n": 0}

    def flaky(name, reqs, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise IndexUnavailableError("injected: one query of the step dropped")
        return real(name, reqs, **kw)

    p10.sys.p.index.search_batch = flaky
    p10.sys.discovery.scan_reps(p10.spec)
    p10.run_matcher()
    t = p10.sys.tasks.row(p10.media(m).subject_id, p10.spec, KIND_A)
    assert t["state"] == "WAITING_INDEX" and t.get("searched_run") is None    # never SEARCHED with zero hits
    p10.drive()
    assert p10.sys.tasks.row(p10.media(m).subject_id, p10.spec, KIND_A)["state"] == "DONE"
    assert len([r for r in matches(p10) if r["content_id"] == m]) == 1


def test_g9_scylla_failure_after_commit_intent(world) -> None:
    p10 = world
    m = p10.ingest_media(*clip("lp-000-sintel-re-encode.mp4"))
    fired = {"n": 0}

    def fail_once(**_):
        if fired["n"] == 0:
            fired["n"] += 1
            raise StorageUnavailableError("injected: Scylla unavailable after the intent")

    p10.hooks.on("committer.after_intent", fail_once)
    p10.sys.discovery.scan_reps(p10.spec)
    p10.run_matcher()
    p10.hooks.clear()
    assert p10.sys.committer.intents(), "the intent survives the failure"
    p10.drive()
    assert not p10.sys.committer.intents()
    rows = [r for r in matches(p10) if r["content_id"] == m]
    assert len(rows) == 1 and rows[0]["verdict"] == "confirmed"           # no lost, no duplicated match


def test_g11_worker_crash_mid_run_resumes_after_lease(world) -> None:
    p10 = world
    m = p10.ingest_media(*clip("lp-000-sintel-re-encode.mp4"))

    class Killed(BaseException):
        pass

    def kill(**ctx):
        if ctx.get("kind") == KIND_A:
            raise Killed

    p10.hooks.on("matcher.after_page", kill)
    p10.sys.discovery.scan_reps(p10.spec)
    with pytest.raises(Killed):
        p10.sys.matcher.run_task(p10.media(m).subject_id, p10.spec, KIND_A)
    p10.hooks.clear()
    t = p10.sys.tasks.row(p10.media(m).subject_id, p10.spec, KIND_A)
    assert t["state"] == "VERIFYING"
    p10.s.clock.advance(seconds=30)            # past the task lease
    p10.drive()
    t = p10.sys.tasks.row(p10.media(m).subject_id, p10.spec, KIND_A)
    assert t["state"] == "DONE" and t["run"] == 1
    assert len([r for r in matches(p10) if r["content_id"] == m]) == 1


def test_g14_two_matcher_hosts(world) -> None:
    p10 = world
    clips = ["lp-000-sintel-re-encode.mp4", "lp-035-sintel-re-encode.webm", "lp-029-elephants-dream-pillarbox.webm"]
    cids = [p10.ingest_media(*clip(c)) for c in clips]
    p10.sys.discovery.scan_reps(p10.spec)
    from fingerprinter.store import Store

    store2 = Store(p10.s.db, p10.s.coord, p10.s.settings, role="host2")
    store2.clock = p10.s.clock
    host2 = P10System(P10(p10.sys.p.settings, p10.s.db, p10.s.coord, p10.s.objects, store2, p10.index))
    host2.techniques = p10.sys.techniques
    host2.matcher.techniques = p10.sys.techniques
    outs: list = []
    ths = [threading.Thread(target=lambda h=h: outs.append(h.matcher_loop(threading.Event(), until_idle=True)))
           for h in (p10.sys, host2)]
    for t in ths:
        t.start()
    for t in ths:
        t.join(300)
    p10.drive()
    rows = matches(p10)
    assert len({r["match_id"] for r in rows}) == len(rows)
    assert {r["content_id"] for r in rows} >= set(cids[:2])
    for c in cids:
        assert p10.sys.tasks.row(p10.media(c).subject_id, p10.spec, KIND_A)["state"] == "DONE"
