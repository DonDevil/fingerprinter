"""Spec lifecycle, FP16 gating, backfill and retirement (design §3.7, §11; TR-5, TR-6, TP-4, TP-6)."""

from __future__ import annotations

import json

import pytest

from fingerprinter.backfill import Backfill, BackfillError
from fingerprinter.publish import READY
from fingerprinter.spec import Spec, dinov2_seg_v1
from fingerprinter.specs import SpecError, SpecLifecycle
from tests.v2.conftest import ensure_group, integration, make_encoder
from tests.v2.helpers import make_video, request_env

pytestmark = integration


def _spec_v2() -> Spec:
    """A second encodable FP32 spec (4 s segments): a different identity, same runtime."""
    obj = json.loads(dinov2_seg_v1().canonical)
    obj["sampling"]["segment_duration_s"] = 4.0
    return Spec.from_json(json.dumps(obj))


def _crawl(stack, web, tmp_path, names):
    ensure_group(stack, READY)
    cids = {}
    for i, n in enumerate(names):
        v = make_video(web.httpd.root / f"{n}.mp4", seconds=9, seed=i + 1)
        env = request_env(v.read_bytes(), web.url("f", f"{n}.mp4"))
        stack.requests.upsert_event(env)
        cids[n] = str(env.payload.content.content_id)
    enc, _ = make_encoder(stack, tmp_path)
    assert enc.run(idle_exit_s=2) == 0
    return cids


def test_tp4_fp16_spec_is_gated_not_claimable_not_default(stack, tmp_path):
    life = SpecLifecycle(stack.store, stack.objects)
    fp16 = dinov2_seg_v1(compute_dtype="float16")
    assert life.add(fp16, actor="op") == "gated"
    with pytest.raises(SpecError):
        life.set_default(str(fp16.digest), actor="op")
    bad = tmp_path / "ev.json"
    bad.write_text(json.dumps({"spec_digest": str(fp16.digest), "precision_gate": "PASS",
                               "labelled_decision_gate": "NOT MEASURED"}))
    with pytest.raises(SpecError):
        life.enable(str(fp16.digest), evidence=bad, actor="op")
    assert stack.store.spec_row(str(fp16.digest))["status"] == "gated"
    with pytest.raises(BackfillError):
        Backfill(stack.store, stack.requests).create(str(stack.spec.digest), str(fp16.digest), actor="op")
    enc, _ = make_encoder(stack, tmp_path)
    assert enc.start() == 0
    assert "spec status gated" in enc.refused[str(fp16.digest)]


def test_tp6_cpu_encoder_never_claims_fp16_work(stack, tmp_path):
    life = SpecLifecycle(stack.store, stack.objects)
    fp16 = dinov2_seg_v1(compute_dtype="float16")
    life.add(fp16, actor="op")
    ok = tmp_path / "ev.json"
    ok.write_text(json.dumps({"spec_digest": str(fp16.digest), "precision_gate": "PASS",
                              "labelled_decision_gate": "PASS"}))
    life.enable(str(fp16.digest), evidence=ok, actor="op")  # synthetic evidence, test only
    assert stack.store.spec_row(str(fp16.digest))["status"] == "active"
    env = request_env(b"fp16 work", "https://a.example/x.mp4", required=fp16.contract)
    stack.requests.upsert_event(env)
    enc, _ = make_encoder(stack, tmp_path)
    assert enc.start() == 0
    assert "float16 spec on a CPU encoder" in enc.refused[str(fp16.digest)]
    stack.requests.schedule()
    assert enc._claim() is None  # popped, not ours, handed back
    r = stack.requests.get(str(env.payload.content.content_id), str(fp16.digest))
    assert r["state"] == "PENDING" and r["claims"] == 0


def test_tr5_backfill_interrupted_resumed_bounded_and_nondestructive(stack, web, tmp_path):
    cids = _crawl(stack, web, tmp_path, ["a", "b", "c", "d"])
    old = str(stack.spec.digest)
    v2 = _spec_v2()
    life = SpecLifecycle(stack.store, stack.objects)
    life.add(v2, actor="op")
    small = stack.fresh(backfill=stack.settings.backfill.model_copy(update={"max_in_flight": 2}))
    bf = Backfill(small.store, small.requests)
    job = bf.create(old, str(v2.digest), actor="op")
    counts = bf.run(job, actor="op", max_items=1)  # interrupted runner
    assert counts["enqueued"] == 1
    counts = bf.run(job, actor="op", wait_s=0.05, max_wait_s=0.2)  # bounded: stops at max_in_flight
    assert counts["enqueued"] == 1
    st = bf.status(job)
    assert st["requests"].get("PENDING") == 2 and not st["complete"]
    enc, _ = make_encoder(stack, tmp_path)
    enc.run(idle_exit_s=2)
    bf.run(job, actor="op")  # resumes from the bucket cursors; re-running is idempotent
    enc, _ = make_encoder(stack, tmp_path)
    enc.run(idle_exit_s=2)
    st = bf.status(job)
    assert st["complete"] and st["requests"] == {"READY": 4}
    for cid in cids.values():  # both specs coexist; nothing of the old spec was touched
        assert stack.store.representation(cid, old)["state"] == "READY"
        assert stack.store.representation(cid, str(v2.digest))["state"] == "READY"
    reqs = [r for r in stack.requests.all() if r["spec_key"] == str(v2.digest)]
    assert all(r["origin"] == "backfill" and r["first_event_id"] is None for r in reqs)


def test_tr5_pause_resume_cancel(stack, web, tmp_path):
    _crawl(stack, web, tmp_path, ["p", "q"])
    v2 = _spec_v2()
    SpecLifecycle(stack.store, stack.objects).add(v2, actor="op")
    bf = Backfill(stack.store, stack.requests)
    job = bf.create(str(stack.spec.digest), str(v2.digest), actor="op")
    bf.pause(job, actor="op")
    with pytest.raises(BackfillError):
        bf.run(job, actor="op")
    bf.resume(job, actor="op")
    assert bf.run(job, actor="op", max_items=1)["enqueued"] == 1
    bf.cancel(job, actor="op")
    with pytest.raises(BackfillError):
        bf.run(job, actor="op")
    assert bf.job(job)["state"] == "CANCELLED"


def test_d11_vanished_source_reported_and_old_representation_kept(stack, web, tmp_path):
    cids = _crawl(stack, web, tmp_path, ["keep", "gone"])
    (web.httpd.root / "gone.mp4").unlink()  # candidate bytes were not archived
    v2 = _spec_v2()
    SpecLifecycle(stack.store, stack.objects).add(v2, actor="op")
    bf = Backfill(stack.store, stack.requests)
    job = bf.create(str(stack.spec.digest), str(v2.digest), actor="op")
    bf.run(job, actor="op")
    enc, _ = make_encoder(stack, tmp_path)
    enc.run(idle_exit_s=2)
    st = bf.status(job)
    assert st["requests"] == {"READY": 1, "FAILED": 1}
    (lost,) = st["source_lost"]
    assert lost["content_id"] == cids["gone"] and lost["failure"] == "source_unreachable"
    assert not st["complete"] or st["failed"]  # never reported as processed
    assert stack.store.representation(cids["gone"], str(stack.spec.digest))["state"] == "READY"


def test_tr6_retire_guards_and_grace(stack, web, tmp_path):
    cids = _crawl(stack, web, tmp_path, ["r"])
    life = SpecLifecycle(stack.store, stack.objects)
    old = str(stack.spec.digest)
    with pytest.raises(SpecError, match="default"):
        life.retire(old, actor="op")
    v2 = _spec_v2()
    life.add(v2, actor="op")
    bf = Backfill(stack.store, stack.requests)
    job = bf.create(old, str(v2.digest), actor="op")
    life.set_default(str(v2.digest), actor="op")
    with pytest.raises(SpecError, match="backfill"):
        life.retire(old, actor="op")
    bf.cancel(job, actor="op")
    stack.db.run("INSERT INTO {ks}.retire_guard (spec_digest, holder) VALUES (?, 'p10-index')", old)
    with pytest.raises(SpecError, match="retire_guard"):
        life.retire(old, actor="op")
    stack.db.run("DELETE FROM {ks}.retire_guard WHERE spec_digest = ? AND holder = 'p10-index'", old)
    # an active target whose only READY binding is the old spec blocks retirement
    reg = stack.targets.register(web.httpd.root / "r.mp4", title="R", kind="movie", actor="op")
    assert reg.reused_representation is False  # registered under the new default (v2)
    stack.db.run("INSERT INTO {ks}.target_representations (target_id, version, spec_digest, state, "
                 "content_id, representation_id) VALUES (?,?,?,'READY',?,?)", reg.target_id, reg.version,
                 old, cids["r"], str(stack.spec.representation_id(cids["r"])))
    stack.db.lwt("UPDATE {ks}.targets SET state = 'READY' WHERE target_id = ? AND version = ? IF EXISTS",
                 reg.target_id, reg.version)
    with pytest.raises(SpecError, match="only READY binding"):
        life.retire(old, actor="op")
    stack.targets.retire(reg.target_id, reason="test", actor="op")
    assert life.retire(old, actor="op") == 1
    rep = stack.store.representation(cids["r"], old)
    assert rep["state"] == "RETIRED"
    assert life.retire_sweep() == 0  # within retire_grace_s
    stack.clock.advance(seconds=stack.settings.retire_grace_s + 1)
    assert life.retire_sweep() == 1


def test_target_registered_after_default_change_gets_its_spec(stack, web, tmp_path):
    cids = _crawl(stack, web, tmp_path, ["z"])
    life = SpecLifecycle(stack.store, stack.objects)
    v2 = _spec_v2()
    life.add(v2, actor="op")
    life.set_default(str(v2.digest), actor="op")
    reg = stack.targets.register(web.httpd.root / "z.mp4", title="Z", kind="movie", actor="op")
    assert reg.content_id == cids["z"]
    req = stack.requests.get(cids["z"], str(v2.digest))
    assert req is not None and req["state"] == "PENDING" and req["origin"] == "target"
    enc, _ = make_encoder(stack, tmp_path)
    enc.run(idle_exit_s=2)
    assert stack.targets.target(reg.target_id, reg.version)["state"] == "READY"
    (b,) = stack.targets.bindings(reg.target_id)
    assert b["spec_digest"] == str(v2.digest) and b["state"] == "READY"
