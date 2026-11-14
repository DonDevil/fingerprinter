"""Rebuild, promotion and occurrence-fence scenarios on the real stack (validation §9a R-6, R-12, R-13, R-28; §9d K-2)."""

from __future__ import annotations

import pytest

from fingerprinter.generations import BUILDING, READY
from fingerprinter.p10ctx import KIND_B
from fingerprinter.recovery import CLOSED, OPEN, REPAIRED, Occurrence
from fingerprinter.vindex import MEDIA, subject_seq_eq
from tests.p10 import synth

pytestmark = pytest.mark.p10_integration


class KilledError(BaseException):
    pass


def test_r6_g4_interrupted_build_adopted_same_manifest(p10) -> None:
    p10.register_target(*synth.film(50, 120), title="T")
    for i in range(6):
        p10.ingest_media(*synth.random_subject(60 + i, 5))
    p10.bootstrap(roles=("target",))
    seen = {"n": 0}

    def kill(**ctx):
        seen["n"] += 1
        if seen["n"] == 20:          # mid-way through the 64 buckets of the media build
            raise KilledError

    p10.hooks.on("builder.after_bucket", kill)
    with pytest.raises(KilledError):
        p10.sys.builder.build(p10.spec, MEDIA, wait_boundary=False)
    p10.hooks.clear()
    _, gens = p10.sys.gens.partition(p10.spec, MEDIA)
    g = max(gens)
    assert gens[g]["state"] == BUILDING and len(gens[g]["build_cursor"]) == 20
    p10.s.clock.advance(seconds=60)                   # the build lease expires
    res = p10.sys.builder.build(p10.spec, MEDIA, wait_boundary=False)
    assert res["state"] == READY and res["generation"] == g     # adopted, not a new generation
    assert res["manifest_digest"] == p10.sys.builder.manifest_digest(p10.spec, MEDIA)


def test_r12_g12_live_writes_during_rebuild_reach_the_new_generation(p10) -> None:
    p10.register_target(*synth.film(51, 120), title="T")
    p10.ingest_media(*synth.random_subject(70, 5))
    p10.bootstrap()
    added: list[str] = []

    def write_during_build(**ctx):
        if ctx.get("bucket") == 10 and not added:     # a mutator after CREATED: routes to both generations
            cid = p10.ingest_media(*synth.random_subject(71, 5))
            p10.sys.indexer.ensure(p10.media(cid))
            added.append(cid)

    p10.hooks.on("builder.after_bucket", write_during_build)
    res = p10.sys.rebuild(p10.spec, MEDIA, promote=True, wait_boundary=False)
    p10.hooks.clear()
    assert res.get("promoted"), res.get("failures")
    s = p10.media(added[0])
    e = p10.sys.indexer.entry(p10.spec, MEDIA, s.bucket, s.subject_id)
    new = p10.sys.gens.row(p10.spec, MEDIA, res["generation"])
    old = p10.sys.gens.row(p10.spec, MEDIA, res["generation"] - 1)
    for row in (new, old):                             # in both generations at its newest seq
        assert p10.index.count(row["collection"], subject_seq_eq(MEDIA, s.subject_id, e["seq"])) == s.segment_count


def test_r13_lease_takeover_fences_the_stale_builder(p10) -> None:
    p10.ingest_media(*synth.random_subject(72, 5))
    e1 = p10.sys.gens.acquire_lease(p10.spec, MEDIA)
    e1b = p10.sys.gens.acquire_lease(p10.spec, MEDIA)      # the same owner re-acquiring gets a new epoch,
    assert e1 is not None and e1b == e1 + 1                  # which fences its own older operations
    e1 = e1b
    other = type(p10.sys)(type(p10.sys.p)(p10.sys.p.settings, p10.s.db, p10.s.coord, p10.s.objects,
                                         type(p10.s.store)(p10.s.db, p10.s.coord, p10.s.settings, role="b2"),
                                         p10.index))
    other.p.store.clock = p10.s.clock
    assert other.gens.acquire_lease(p10.spec, MEDIA) is None                         # held
    p10.s.clock.advance(seconds=60)
    e2 = other.gens.acquire_lease(p10.spec, MEDIA)
    assert e2 == e1 + 1
    # the stale builder's transitions are conditioned on build_epoch: create a generation under e2
    res = other.builder.build(p10.spec, MEDIA, wait_boundary=False)
    assert res["state"] == READY
    g = res["generation"]
    assert not p10.sys.gens.transition(p10.spec, MEDIA, g, READY, "VALIDATING", build_epoch=e1)  # stale: rejected


def test_r28_loss_after_confirming_count_is_not_hidden(p10) -> None:
    """C-14b: T's points deleted again after the repairer's confirming count (P7) and a detector merges
    before `repaired`: the REPAIRED LWT fails on `detections` and the repair is repeated."""
    tid, ver, _ = p10.register_target(*synth.film(52, 120), title="T")
    p10.bootstrap()
    p10.drive()
    t = p10.target(tid, ver)
    ptr = p10.sys.gens.active(p10.spec, "target", fresh=True)

    def lose():
        e = p10.sys.indexer.entry(p10.spec, "target", t.bucket, t.subject_id)
        p10.index.delete_by_filter(ptr.collection, subject_seq_eq("target", t.subject_id, int(e["seq"])))

    lose()
    p10.sys.verify_pass()
    occ_row = p10.sys.recovery.occurrences(p10.spec)[0]
    o = Occurrence(occ_row["spec_digest"], occ_row["role"], occ_row["bucket"], occ_row["subject_id"],
                   occ_row["generation"], occ_row["occurrence"], occ_row["occurrence_id"], occ_row)
    fired = {"n": 0}

    def at_p7(**_):
        if fired["n"] == 0:
            fired["n"] += 1
            lose()                                   # second loss after the confirming count
            p10.sys.verify_pass()                    # (a) a detector merges before `repaired`

    p10.hooks.on("reconciler.after_confirm_before_repaired", at_p7)
    p10.sys.recovery.step(p10.sys.recovery.read(o))
    p10.hooks.clear()
    row = p10.sys.recovery.read(o)
    assert int(row["detections"]) >= 1
    # the first `repaired` attempt failed on detections; whatever the step reached, the points are back
    assert row["state"] in (OPEN, REPAIRED, "RERUN_PENDING")
    p10.drive()
    rows = [r for r in p10.sys.recovery.occurrences(p10.spec) if r["subject_id"] == t.subject_id]
    assert all(r["state"] == CLOSED for r in rows)
    e = p10.sys.indexer.entry(p10.spec, "target", t.bucket, t.subject_id)
    assert p10.index.count(ptr.collection, subject_seq_eq("target", t.subject_id, int(e["seq"]))) == t.segment_count
    tb = p10.sys.tasks.row(t.subject_id, p10.spec, KIND_B)
    assert tb["done_ok_run"] >= max(int(v) for r in rows for v in (r["required_run"] or {}).values())


def test_k2_two_specs_two_match_ids(p10) -> None:
    from antipiracy_contracts.ids import MatchId, RepresentationId

    from fingerprinter.spec import dinov2_seg_v1, dinov2_seg_v2

    rep = {}
    for spec in (dinov2_seg_v1(), dinov2_seg_v2()):
        rep[str(spec.digest)] = (str(spec.representation_id("cnt_" + "a" * 32)),
                                 str(spec.target_representation_id("tgt_" + "b" * 32, 1)))
    ids = {MatchId.of(RepresentationId(m), RepresentationId(t), "dinov2-seg-temporal", "2.1") for m, t in rep.values()}
    assert len(ids) == 2   # the matches key contains both spec digests: two rows, never one (C-6)
