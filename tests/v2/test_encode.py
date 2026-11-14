"""Functional encoding on the real stack, CPU device (G-4) and the S1/S2 reuse
exit gate (G-1, TR-1…TR-3)."""

from __future__ import annotations

import shutil

import numpy as np
from antipiracy_contracts.events import decode_event_as
from antipiracy_contracts.events.fingerprinting import RepresentationReady

from fingerprinter import artifact
from fingerprinter.intake import Intake
from fingerprinter.objects import parse_uri
from fingerprinter.publish import INTAKE, READY
from tests.v2.conftest import ensure_group, integration, make_encoder
from tests.v2.helpers import make_video, request_env, xadd_request

pytestmark = integration


def _ready_events(stack):
    entries = stack.coord.client.xrange(stack.coord.stream(READY))
    return [decode_event_as(f[b"envelope"], RepresentationReady) for _, f in entries]


def _videos(web, names, seconds=12):
    out = {}
    for i, name in enumerate(names):
        p = make_video(web.httpd.root / f"{name}.mp4", seconds=seconds, seed=i + 1)
        out[name] = p.read_bytes()
    return out


def test_encode_end_to_end_cpu(stack, web, tmp_path):
    ensure_group(stack, READY)
    data = _videos(web, ["a"])["a"]
    env = request_env(data, web.url("f", "a.mp4"), referer="https://watch.example/page")
    stack.requests.upsert_event(env)
    enc, view = make_encoder(stack, tmp_path)
    assert enc.run(idle_exit_s=2) == 0
    (req,) = stack.requests.all()
    assert req["state"] == "READY"
    cid = req["content_id"]
    rep = stack.store.representation(cid, str(stack.spec.digest))
    assert rep["representation_id"] == str(stack.spec.representation_id(cid))
    assert rep["f_index"] and rep["f_request"] and rep["f_targets"] and rep["ready_event_published_at"]
    bucket, key = parse_uri(rep["artifact_uri"])
    blob = stack.objects.read_verified(bucket, key, rep["artifact_size"], rep["artifact_sha256"])
    arrays, manifest = artifact.load(blob)
    # real PTS (time_base 1/12288 for this H.264 fixture), not index × 5
    assert arrays["segment_index"].tolist() == [0, 1, 2]
    assert arrays["pts"].tolist() == [0, 61440, 122880]
    assert arrays["time_base"].tolist() == [1, 12288]
    assert arrays["vectors"].shape == (3, 768) and arrays["vectors"].dtype == np.float32
    assert np.allclose(np.linalg.norm(arrays["vectors"], axis=1), 1, atol=1e-5)
    assert manifest["compute_dtype"] == "float32" and manifest["device"] == "cpu"
    (ev,) = _ready_events(stack)
    assert ev.payload.representation.representation_id == rep["representation_id"]
    assert ev.payload.representation.segment_count == 3
    assert ev.causation_id == env.event_id
    assert list(stack.store.intents()) == []
    assert stack.db.one("SELECT * FROM {ks}.representations_by_spec WHERE spec_digest = ? AND bucket = ?",
                        str(stack.spec.digest), stack.store.bucket_of(cid))
    seen = [h for p, h in web.requests if p.endswith("a.mp4")]
    assert seen and seen[0].get("Accept-Encoding") == "identity"


def test_existing_representation_reused_and_duplicates_safe(stack, web, tmp_path):
    ensure_group(stack, READY)
    data = _videos(web, ["a"])["a"]
    stack.requests.upsert_event(request_env(data, web.url("f", "a.mp4")))
    enc, _ = make_encoder(stack, tmp_path)
    enc.run(idle_exit_s=2)
    # duplicate request under a new event id and another locator: no second encode
    shutil.copy(web.httpd.root / "a.mp4", web.httpd.root / "a2.mp4")
    out = stack.requests.upsert_event(request_env(data, web.url("f", "a2.mp4")))
    assert out == "ready_republished"
    enc2, _ = make_encoder(stack, tmp_path)
    enc2.run(idle_exit_s=2)
    reps = stack.db.rows("SELECT * FROM {ks}.representations")
    assert len(reps) == 1
    assert len(list(stack.objects.list(stack.objects.cfg.representations_bucket, "rep/"))) == 1
    assert len(_ready_events(stack)) == 2  # republished, same representation_id
    assert len({e.payload.representation.representation_id for e in _ready_events(stack)}) == 1


def _s1_s2(stack, web, tmp_path, *, restart_mid_s2=False, replay_s1=False):
    ensure_group(stack, READY)
    data = _videos(web, list("ABCDEFG"), seconds=6)
    intake = Intake(stack.store, stack.requests)
    intake.init("from-start", actor="test")
    assert intake.acquire()
    stream = stack.coord.stream(INTAKE)
    s1 = [request_env(data[n], web.url("f", f"{n}.mp4")) for n in "ABCDE"]
    for env in s1:
        xadd_request(stack.coord.client, stream, env)
    while not intake.cycle().idle:
        pass
    enc, _ = make_encoder(stack, tmp_path)
    assert enc.run(idle_exit_s=2) == 0
    # S2: C–G, where C–E are the same bytes at new URLs
    for n in "CDE":
        shutil.copy(web.httpd.root / f"{n}.mp4", web.httpd.root / f"{n}-mirror.mp4")
    s2 = [request_env(data[n], web.url("f", f"{n}-mirror.mp4")) for n in "CDE"]
    s2 += [request_env(data[n], web.url("f", f"{n}.mp4")) for n in "FG"]
    for i, env in enumerate(s2):
        xadd_request(stack.coord.client, stream, env)
        if restart_mid_s2 and i == 2:
            intake.cycle()
            intake = Intake(stack.fresh("intake").store, stack.requests)  # new process
            stack.clock.advance(seconds=stack.settings.intake.lease_ttl_s + 1)
            assert intake.acquire()
    if replay_s1:  # crawler2 relay --replay-from: S1 again with new stream IDs
        for env in s1:
            xadd_request(stack.coord.client, stream, env)
    while not intake.cycle().idle:
        pass
    enc2, _ = make_encoder(stack, tmp_path)
    assert enc2.run(idle_exit_s=2) == 0
    return enc, enc2


def _assert_seven(stack):
    reps = stack.db.rows("SELECT * FROM {ks}.representations")
    assert len(reps) == 7
    objects = list(stack.objects.list(stack.objects.cfg.representations_bucket, "rep/"))
    assert len(objects) == 7
    reqs = stack.requests.all()
    assert len(reqs) == 7 and {r["state"] for r in reqs} == {"READY"}
    assert sum(r["claims"] for r in reqs) == 7  # one claim per content: 7 encodes, not 10


def test_tr1_s1_s2_reuse_exit_gate(stack, web, tmp_path):
    enc1, enc2 = _s1_s2(stack, web, tmp_path)
    assert enc1.stats["won"] == 5 and enc2.stats["won"] == 2
    _assert_seven(stack)
    ready_ids = {e.payload.representation.representation_id for e in _ready_events(stack)}
    assert len(ready_ids) == 7  # every content is ready for crawler2's projection


def test_tr2_s1_s2_with_replay_of_s1(stack, web, tmp_path):
    enc1, enc2 = _s1_s2(stack, web, tmp_path, replay_s1=True)
    assert enc1.stats["won"] + enc2.stats["won"] == 7
    _assert_seven(stack)


def test_tr3_s1_s2_with_intake_restart(stack, web, tmp_path):
    enc1, enc2 = _s1_s2(stack, web, tmp_path, restart_mid_s2=True)
    assert enc1.stats["won"] + enc2.stats["won"] == 7
    _assert_seven(stack)


def test_attempt_log_written(stack, web, tmp_path):
    data = _videos(web, ["l"])["l"]
    stack.requests.upsert_event(request_env(data, web.url("f", "l.mp4")))
    enc, _ = make_encoder(stack, tmp_path)
    enc.run(idle_exit_s=2)
    (req,) = stack.requests.all()
    rows = stack.db.rows("SELECT * FROM {ks}.encode_attempt_log WHERE content_id = ? AND spec_key = ?",
                         req["content_id"], "default")
    assert len(rows) == 1 and rows[0]["outcome"] == "done" and rows[0]["host_id"] == enc.settings.host_id
