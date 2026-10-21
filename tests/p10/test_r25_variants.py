"""R-25 (b)-(d): non-protocol point loss during matching (validation §9a.1, design §12.10) — real stack.

Same conventions as ``test_recovery.py``: a loss is a direct Qdrant delete of
a subject's current points; M is a provenance-true clip of target T whose
search runs while T's points are missing, so M's DONE hides (M, T) and only
T's own post-repair rerun may find it.
"""

from __future__ import annotations

import dataclasses
import time
from datetime import timedelta

import pytest

from fingerprinter.coord import utc
from fingerprinter.p10ctx import BUCKETS, KIND_A, KIND_B
from fingerprinter.recovery import CLOSED, OPEN, RESOLVED
from fingerprinter.vindex import SUBJECT_FIELD, TARGET, subject_seq_eq
from tests.p10.test_flow import clip
from tests.p10.test_recovery import _need_vectors, has_match, lose, occurrences, world  # noqa: F401

pytestmark = pytest.mark.p10_integration

CLIP = "lp-000-sintel-re-encode.mp4"


def hidden_pair(p10, tid):
    """M searched while T's points are missing: DONE without (M, T)."""
    m = p10.ingest_media(*clip(CLIP))
    p10.sys.discovery.scan_reps(p10.spec)
    p10.run_matcher()
    ms = p10.media(m)
    assert p10.sys.tasks.row(ms.subject_id, p10.spec, KIND_A)["state"] == "DONE" and not has_match(p10, m, tid)
    return m, ms


def recovered(p10, t, m, ms, occ) -> None:
    """The common R-25 pass condition once the occurrence is driven to CLOSED."""
    assert occ["state"] == CLOSED and occ["close_reason"] == "RERUN_DONE" and occ["resolution"] == RESOLVED, occ
    tb = p10.sys.tasks.row(t.subject_id, p10.spec, KIND_B)
    assert tb["rerun_reason"] == "index_repair" and tb["done_ok_run"] == tb["run"] >= 2
    assert occ["occurrence_id"] in tb["recovery_runs"]                 # the bump was keyed by this occurrence
    assert has_match(p10, m, t.target_id)                              # found by T's own rerun
    assert p10.sys.tasks.row(ms.subject_id, p10.spec, KIND_A)["run"] == 1   # M's DONE untouched (R4)
    assert p10.sys.recovery.counters(p10.spec) == (0, 0)
    assert p10.sys.recovery.settled(ms.subject_id, p10.spec, [KIND_A])["status"] == "settled"


def test_r25b_new_intent_before_verification_records_the_gap_before_healing_it(world, monkeypatch) -> None:
    """R-25 (b): as (a), but before any verification pass T gets a new intent (re-UPSERT at s+1) whose
    apply would re-create T's points. The apply's pre-count makes the occurrence durable **before** its
    upsert (call order), so the healed gap is still recovered by T's rerun."""
    p10, (tid, ver, _) = world
    t = p10.target(tid, ver)
    g = lose(p10, t)
    m, ms = hidden_pair(p10, tid)
    s = int(p10.sys.indexer.entry(p10.spec, TARGET, t.bucket, t.subject_id)["seq"])
    order: list[str] = []
    real_detect, real_upsert = p10.sys.indexer.recovery.detect, p10.index.upsert

    def detect(*a, **kw):
        occ = real_detect(*a, **kw)
        order.append(f"detect:{kw.get('detected_by')}")
        return occ

    def upsert(name, points, **kw):
        if any(p.payload.get(SUBJECT_FIELD[TARGET]) == t.subject_id for p in points):
            order.append("upsert")
        return real_upsert(name, points, **kw)

    monkeypatch.setattr(p10.sys.indexer.recovery, "detect", detect)
    monkeypatch.setattr(p10.index, "upsert", upsert)
    real_desired = p10.sys.indexer.desired
    monkeypatch.setattr(p10.sys.indexer, "desired",   # e.g. a lowinfo_rev change: a new desired version
                        lambda subj: (lambda op, v: (op, v + "+r25b") if subj.subject_id == t.subject_id
                                      else (op, v))(*real_desired(subj)))
    res = p10.sys.indexer.ensure(t)
    row = p10.sys.indexer.entry(p10.spec, TARGET, t.bucket, t.subject_id)
    assert int(row["seq"]) == s + 1 and row["applied"].get(g) == s + 1, (res, row)
    assert order[:2] == ["detect:apply_precount", "upsert"], order    # durable before the heal
    ptr = p10.sys.gens.active(p10.spec, TARGET, fresh=True)
    assert p10.index.count(ptr.collection, subject_seq_eq(TARGET, t.subject_id, s + 1)) == t.segment_count
    occ = occurrences(p10, t.subject_id)
    assert len(occ) == 1 and occ[0]["state"] == OPEN and occ[0]["detected_by"] == "apply_precount"
    assert occ[0]["generation"] == g
    assert p10.sys.recovery.settled(ms.subject_id, p10.spec, [KIND_A])["status"] == "provisional"
    p10.drive()
    recovered(p10, t, m, ms, occurrences(p10, t.subject_id)[0])


def test_r25c_loss_in_the_draining_generation_during_its_horizon(world, monkeypatch) -> None:
    """R-25 (c): g′ promoted; T's points are deleted from the now DRAINING g during its search horizon
    and a reader holding the cached g pointer searches g (M DONE, pair hidden). g is not RETIRED before
    a verification pass that started after the horizon; that pass detects T in g; T's rerun finds (M, T);
    g is retired only once the occurrence no longer holds it."""
    p10, (tid, ver, _) = world
    t = p10.target(tid, ver)
    old = p10.sys.gens.active(p10.spec, TARGET, fresh=True)
    # a horizon long enough for M's whole step to run on the cached pointer inside it
    ix = p10.sys.p.settings.index.model_copy(update={"active_ttl_s": 20.0})
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"index": ix})
    res = p10.sys.rebuild(p10.spec, TARGET, promote=True, wait_boundary=False)
    assert res["state"] == "READY" and res["promoted"], res
    assert p10.sys.gens.row(p10.spec, TARGET, old.generation)["state"] == "DRAINING"
    e = p10.sys.indexer.entry(p10.spec, TARGET, t.bucket, t.subject_id)
    p10.index.delete_by_filter(old.collection, subject_seq_eq(TARGET, t.subject_id, int(e["seq"])))
    real_active = p10.sys.searcher.gens.active

    def cached(spec, role, **kw):     # a reader whose pointer still names g (read before the promotion)
        if role == TARGET:
            return dataclasses.replace(old, read_mono=time.monotonic())
        return real_active(spec, role, **kw)

    monkeypatch.setattr(p10.sys.searcher.gens, "active", cached)
    m, ms = hidden_pair(p10, tid)
    monkeypatch.setattr(p10.sys.searcher.gens, "active", real_active)
    row = p10.sys.gens.row(p10.spec, TARGET, old.generation)
    horizon = utc(row["drain_horizon_at"])
    assert p10.sys.p.now() < horizon
    p10.sys.generations_pass()                                         # inside the horizon: never retired
    assert p10.sys.gens.row(p10.spec, TARGET, old.generation)["state"] == "DRAINING"
    ix = p10.sys.p.settings.index.model_copy(update={"drain_grace_s": 0.5})
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"index": ix})
    while p10.sys.p.now() < horizon + timedelta(seconds=0.6):
        time.sleep(0.2)
    p10.sys.generations_pass()                                         # the post-horizon pass detects T in g
    occ = [o for o in occurrences(p10, t.subject_id) if o["generation"] == old.generation]
    assert len(occ) == 1 and occ[0]["detected_by"] == "verification", occ
    g_row = p10.sys.gens.row(p10.spec, TARGET, old.generation)
    assert utc(g_row["verify_started_at"]) >= horizon
    for _ in range(4):
        p10.drive()
        p10.sys.generations_pass()
    occ = [o for o in occurrences(p10, t.subject_id) if o["generation"] == old.generation]
    recovered(p10, t, m, ms, occ[0])
    assert p10.sys.gens.row(p10.spec, TARGET, old.generation)["state"] == "RETIRED"
    assert not p10.index.collection_exists(old.collection)


def test_r25d_lost_collection_fallback_obligations_recover_the_hidden_pair(world) -> None:
    """R-25 (d): as (a), then g's whole target collection is deleted before any audit or verification can
    count T. After a rebuild supersedes g, the lost DRAINING g yields one unauditable-fallback occurrence
    per subject applied to g, all durable before g is RETIRED; T's rerun finds (M, T)."""
    p10, (tid, ver, _) = world
    t = p10.target(tid, ver)
    g = lose(p10, t)
    m, ms = hidden_pair(p10, tid)
    old = p10.sys.gens.active(p10.spec, TARGET, fresh=True)
    assert old.generation == g
    applied = [r for b in range(BUCKETS)
               for r in p10.s.db.rows("SELECT * FROM {ks}.index_entries WHERE spec_digest = ? AND role = ? "
                                      "AND bucket = ?", p10.spec, TARGET, b) if (r["applied"] or {}).get(g)]
    p10.index.delete_collection(old.collection)
    res = p10.sys.rebuild(p10.spec, TARGET, promote=True, wait_boundary=False)
    assert res["state"] == "READY" and res["promoted"], res
    ix = p10.sys.p.settings.index.model_copy(update={"drain_grace_s": 0.0})
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"index": ix})
    horizon = utc(p10.sys.gens.row(p10.spec, TARGET, g)["drain_horizon_at"])
    while p10.sys.p.now() < horizon + timedelta(seconds=0.2):
        time.sleep(0.2)
    p10.sys.generations_pass()
    assert p10.sys.gens.row(p10.spec, TARGET, g)["state"] == "RETIRED"
    occ = [o for o in p10.sys.recovery.occurrences(p10.spec) if o["role"] == TARGET and o["generation"] == g]
    assert {o["subject_id"] for o in occ} == {r["subject_id"] for r in applied}
    assert all(o["detected_by"] == "unauditable" for o in occ)
    p10.drive(8)
    mine = [o for o in occ if o["subject_id"] == t.subject_id]
    assert len(mine) == 1
    recovered(p10, t, m, ms, next(o for o in occurrences(p10, t.subject_id) if o["generation"] == g))
