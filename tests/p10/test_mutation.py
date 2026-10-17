"""Index mutation protocol, generations and capacity on the real stack (validation §9a R-*, §9e CAP-*).

Fault injection uses the named hook points of the implementation (no-ops in
production): a hook that raises models a crash at that boundary; direct
Qdrant calls model a stale or paused worker landing an older operation, or
a non-protocol loss. Each test ends with the reconciler and checks the
common pass condition parts that apply.
"""

from __future__ import annotations

import pytest

from fingerprinter.indexing import APPLIED, BLOCKED, PENDING, WaitingIndexError
from fingerprinter.vindex import MEDIA, Point, point_id, subject_seq_eq, subject_seq_lt, subject_seq_lte
from tests.p10 import synth

pytestmark = pytest.mark.p10_integration


class CrashError(Exception):
    pass


def crash(p10, point: str) -> None:
    def h(**_):
        raise CrashError(point)
    p10.hooks.on(point, h)


def entry(p10, s):
    return p10.sys.indexer.entry(p10.spec, s.role, s.bucket, s.subject_id)


def active_collection(p10, role: str) -> tuple[int, str]:
    ptr = p10.sys.gens.active(p10.spec, role, fresh=True)
    return ptr.generation, ptr.collection


def count(p10, s, seq: int | None = None) -> int:
    _, name = active_collection(p10, s.role)
    e = entry(p10, s)
    return p10.index.count(name, subject_seq_eq(s.role, s.subject_id, int(seq if seq is not None else e["seq"])))


@pytest.fixture
def ready(p10):
    """An index with ACTIVE generations of both roles (empty start)."""
    t = p10.register_target(*synth.film(1), title="T")
    p10.bootstrap()
    return p10, t


def test_r2_crash_after_intent_before_qdrant(ready) -> None:
    p10, _ = ready
    cid = p10.ingest_media(*synth.random_subject(2))
    s = p10.media(cid)
    crash(p10, "indexer.after_intent")  # crash right after the intent LWT, before any Qdrant call
    with pytest.raises(CrashError):
        p10.sys.indexer.ensure(s)
    assert entry(p10, s)["state"] == PENDING and count(p10, s) == 0
    p10.hooks.clear()
    p10.reconcile()
    e = entry(p10, s)
    g, _ = active_collection(p10, MEDIA)
    assert e["state"] == APPLIED and e["applied"][g] == e["seq"] and count(p10, s) == s.segment_count


def test_r3_crash_after_qdrant_before_bookkeeping_no_duplicates(ready) -> None:
    p10, _ = ready
    s = p10.media(p10.ingest_media(*synth.random_subject(3)))
    crash(p10, "indexer.before_bookkeeping")
    with pytest.raises(CrashError):
        p10.sys.indexer.ensure(s)
    assert entry(p10, s)["state"] == PENDING and count(p10, s) == s.segment_count
    p10.hooks.clear()
    p10.reconcile()
    _, name = active_collection(p10, MEDIA)
    assert entry(p10, s)["state"] == APPLIED
    assert p10.index.count(name, {"must": [{"key": "rep", "match": {"value": s.subject_id}}]}) == s.segment_count


def test_r4_two_routed_generations_pending_until_both(ready) -> None:
    p10, _ = ready
    s = p10.media(p10.ingest_media(*synth.random_subject(4)))
    g_active, _ = active_collection(p10, MEDIA)
    # a second routed generation (CREATED/BUILDING) via admission + create, as a rebuild would
    from fingerprinter.capacity import holder_of
    from fingerprinter.vindex import GenerationParams

    g2 = p10.sys.gens.next_number(p10.spec, MEDIA)
    assert p10.sys.capacity.admit_rebuild(holder_of(p10.spec, MEDIA, g2), 1000)
    assert p10.sys.gens.create(p10.spec, MEDIA, g2, GenerationParams(), 99, 1000)
    row = p10.sys.gens.row(p10.spec, MEDIA, g2)
    p10.index.create_collection(row["collection"], GenerationParams(), MEDIA)
    p10.sys.indexer.write_intent(s)
    res = p10.sys.indexer.apply(p10.spec, MEDIA, s.bucket, s.subject_id, only_gens={g_active})
    assert res.state == PENDING and entry(p10, s)["applied"] == {g_active: entry(p10, s)["seq"]}
    p10.sys.builder.catch_up(p10.spec, MEDIA, g2)
    e = entry(p10, s)
    assert e["state"] == APPLIED and e["applied"][g2] == e["seq"]
    assert p10.index.count(row["collection"], subject_seq_eq(MEDIA, s.subject_id, e["seq"])) == s.segment_count


def test_r5_interrupted_delete_repeated(ready) -> None:
    p10, (tid, ver, _) = ready
    s = p10.target(tid, ver)
    assert count(p10, s) == s.segment_count
    p10.s.targets.retire(tid, reason="test", actor="test")
    s2 = p10.target(tid, ver)
    assert not s2.live
    crash(p10, "indexer.before_delete")
    with pytest.raises(CrashError):
        p10.sys.indexer.ensure(s2)
    assert entry(p10, s2)["state"] == PENDING and entry(p10, s2)["desired_op"] == "DELETE"
    p10.hooks.clear()
    p10.reconcile()
    _, name = active_collection(p10, "target")
    assert p10.index.count(name, {"must": [{"key": "trep", "match": {"value": s.subject_id}}]}) == 0
    assert entry(p10, s2)["state"] == APPLIED


def _stale_points(s, seq: int, vectors) -> list[Point]:
    return [Point(point_id(s.subject_id, seq, k), vectors[k], {"rep": s.subject_id, "cid": s.content_id,
                                                              "spec": s.spec_digest, "k": k, "t": 5.0 * k, "seq": seq})
            for k in range(len(vectors))]


def test_r22_r23_stale_delete_and_upsert_cannot_remove_current_points(ready) -> None:
    """S2 (C-12): after UPSERT u is applied, a stale UPSERT of an older seq then a stale DELETE of
    seq u-1 land; the seq-u points survive throughout; verification removes the older extras."""
    p10, _ = ready
    vec, pts = synth.random_subject(5)
    s = p10.media(p10.ingest_media(vec, pts))
    p10.sys.indexer.ensure(s)
    e = entry(p10, s)
    u = int(e["seq"])
    _, name = active_collection(p10, MEDIA)
    # injected (C-14q): a worker C holding UPSERT u-2 and a worker A holding DELETE u-1 land late
    p10.index.upsert(name, _stale_points(s, u + 0 - 2 if u > 2 else 0, vec))
    assert count(p10, s, u) == s.segment_count
    p10.index.delete_by_filter(name, subject_seq_lte(MEDIA, s.subject_id, u - 1))      # R-22 / R-23
    assert count(p10, s, u) == s.segment_count                                          # S2
    p10.index.upsert(name, _stale_points(s, max(u - 1, 0), vec))                        # stale UPSERT after
    g = p10.sys.gens.active(p10.spec, MEDIA, fresh=True).generation
    res = p10.sys.checker.verify_generation(p10.spec, MEDIA, g)
    assert res["stale"] == s.segment_count and res["short"] == 0
    assert p10.index.count(name, subject_seq_lt(MEDIA, s.subject_id, u)) == 0
    assert count(p10, s, u) == s.segment_count


def test_r16_orphan_of_dead_subject_removed(ready) -> None:
    p10, _ = ready
    _, name = active_collection(p10, MEDIA)
    vec, _ = synth.random_subject(6, 2)
    fake = [Point(point_id("rep_nonexistent", 1, k), vec[k], {"rep": "rep_nonexistent", "cid": "cnt_none",
                                                              "spec": p10.spec, "k": k, "t": 0.0, "seq": 1})
            for k in range(2)]
    p10.index.upsert(name, fake)
    g = p10.sys.gens.active(p10.spec, MEDIA, fresh=True).generation
    res = p10.sys.checker.verify_generation(p10.spec, MEDIA, g)
    assert res["orphans"] == 2
    assert p10.index.count(name, {"must": [{"key": "rep", "match": {"value": "rep_nonexistent"}}]}) == 0


def test_r11_write_to_retired_generation_is_noop(ready) -> None:
    """A routed-set read before retirement; the write meets 'not found' and is a no-op only because
    Scylla says RETIRED (generation numbers are never reused)."""
    p10, _ = ready
    s = p10.media(p10.ingest_media(*synth.random_subject(7)))
    res = p10.sys.rebuild(p10.spec, MEDIA, promote=True, wait_boundary=False)
    assert res.get("promoted")
    old = res["generation"] - 1
    row = p10.sys.gens.row(p10.spec, MEDIA, old)
    assert row["state"] == "DRAINING"
    p10.index.delete_collection(row["collection"])
    p10.s.db.run("UPDATE {ks}.index_generations SET state = 'RETIRED' WHERE spec_digest = ? AND role = ? "
                 "AND generation = ?", p10.spec, MEDIA, old)
    p10.sys.indexer.write_intent(s)
    out = p10.sys.indexer.apply(p10.spec, MEDIA, s.bucket, s.subject_id, force_gens={old, res["generation"]})
    assert out.state in (APPLIED, PENDING)
    assert not p10.index.collection_exists(row["collection"])  # not re-created
    assert p10.sys.gens.next_number(p10.spec, MEDIA) > res["generation"]


def test_cap1_capacity_refusal_before_any_write(p10) -> None:
    p10.sys.p.settings = p10.sys.p.settings.model_copy(update={"index": p10.sys.p.settings.index.model_copy(
        update={"max_points": 250_000, "capacity_margin_points": 249_980})})
    p10.sys.capacity.ix = p10.sys.p.settings.index
    t = p10.register_target(*synth.film(8, 60), title="T")
    p10.bootstrap(roles=("media",))
    # limit 20: the media role (the target's reference content, 12 points -> 13 projected) is admitted;
    # the target role (13 more) is not
    res = p10.sys.builder.build(p10.spec, "target")
    assert res["state"] == "CAPACITY_BLOCKED"
    _, gens = p10.sys.gens.partition(p10.spec, "target")
    assert not gens  # CAP-3: no generation row, no collection
    s = p10.media(p10.ingest_media(*synth.random_subject(9, 20)))
    with pytest.raises(WaitingIndexError) as exc:
        p10.sys.indexer.ensure(s)
    assert exc.value.reason == "capacity"
    e = entry(p10, s)
    assert e["state"] == BLOCKED and e["blocked_reason"] == "capacity"
    _, name = active_collection(p10, MEDIA)
    assert p10.index.count(name, {"must": [{"key": "rep", "match": {"value": s.subject_id}}]}) == 0  # no write
    assert t


def test_cap_audit_is_the_only_lowering_path(ready) -> None:
    p10, _ = ready
    before = p10.sys.capacity.read()["reserved"]
    s = p10.media(p10.ingest_media(*synth.random_subject(10, 8)))
    p10.sys.indexer.ensure(s)
    mid = p10.sys.capacity.read()["reserved"]
    assert mid >= before + 8
    p10.sys.indexer.ensure(s)  # nothing to apply: no reservation
    assert p10.sys.capacity.read()["reserved"] == mid
    res = p10.sys.capacity.audit()
    total = sum(p10.index.count(c) for c in p10.index.created if p10.index.collection_exists(c))
    assert res.applied and res.bound >= total  # the ledger never goes below the resident points
