"""Target manager (design §10): registration, reuse, binding, TR-4, TR-7, retire/purge."""

from __future__ import annotations

import os
import shutil
import threading

import pytest

from fingerprinter.coord import uuid7
from fingerprinter.objects import ref_key
from fingerprinter.publish import FAILED, READY
from fingerprinter.targets import TargetError
from tests.v2.conftest import ensure_group, integration, make_encoder
from tests.v2.helpers import make_video, request_env
from tests.v2.test_commit import _art

pytestmark = integration


class Crash(Exception):
    pass


def _file(tmp_path, name="ref.bin", data=None):
    p = tmp_path / name
    p.write_bytes(data if data is not None else os.urandom(300_000))
    return p


def _keys(stack, prefix):
    return sorted(o.key for o in stack.objects.list(stack.objects.cfg.targets_bucket, prefix))


def test_register_creates_ref_target_request_and_pending_binding(stack, tmp_path):
    reg = stack.targets.register(_file(tmp_path), title="Film", kind="movie", aliases=("F",),
                                 release_year=2020, actor="op")
    assert reg.version == 1 and not reg.reused_object and not reg.reused_representation
    assert _keys(stack, "ref/") == [ref_key(reg.sha256)] and _keys(stack, "staging/") == []
    info = stack.objects.stat(stack.objects.cfg.targets_bucket, ref_key(reg.sha256))
    assert info.metadata["sha256"] == reg.sha256 and info.size == reg.size
    req = stack.requests.get(reg.content_id, "default")
    assert req["origin"] == "target" and req["priority"] == 90 and req["first_event_id"] is None
    (src,) = req["sources"]
    assert src == "obj:" + ref_key(reg.sha256)
    (b,) = stack.targets.bindings(reg.target_id)
    assert b["state"] == "PENDING"
    assert list(stack.store.intents()) == []


def test_tr4_repeated_registration_is_idempotent_new_bytes_new_version(stack, tmp_path):
    f = _file(tmp_path)
    a = stack.targets.register(f, title="T", kind="movie", actor="op")
    b = stack.targets.register(f, title="T", kind="movie", target_id=a.target_id, actor="op")
    assert b.idempotent and (b.target_id, b.version) == (a.target_id, 1)
    c = stack.targets.register(_file(tmp_path, "v2.bin"), title="T", kind="movie",
                               target_id=a.target_id, actor="op")
    assert c.version == 2 and not c.idempotent


def test_tr4_target_bytes_equal_crawled_content_reused_without_encode(stack, web, tmp_path):
    ensure_group(stack, READY)
    video = make_video(web.httpd.root / "c.mp4", seconds=6)
    stack.requests.upsert_event(request_env(video.read_bytes(), web.url("f", "c.mp4")))
    enc, _ = make_encoder(stack, tmp_path)
    enc.run(idle_exit_s=2)
    claims_before = sum(r["claims"] for r in stack.requests.all())
    reg = stack.targets.register(video, title="C", kind="movie", actor="op")
    assert reg.reused_representation and reg.state == "READY"
    (b,) = stack.targets.bindings(reg.target_id)
    assert b["state"] == "READY" and b["representation_id"] == str(stack.spec.representation_id(reg.content_id))
    assert sum(r["claims"] for r in stack.requests.all()) == claims_before  # no encode
    t = stack.targets.target(reg.target_id, reg.version)
    assert t["registered_envelope"] and t["registered_event_published_at"] is None  # held
    assert any(i.kind == "target_event" for i in stack.store.intents())


def test_tr4_registration_concurrent_with_commit_binds_exactly_once(stack, tmp_path):
    ensure_group(stack, READY)
    data = os.urandom(50_000)
    f = _file(tmp_path, "race.bin", data)
    env = request_env(data, "https://a.example/race.mp4")
    stack.requests.upsert_event(env)
    cid = str(env.payload.content.content_id)
    row = stack.requests.claim(cid, "default", uuid7())
    barrier = threading.Barrier(2)
    other = stack.fresh()

    def commit():
        barrier.wait()
        other.committer.commit(row, stack.spec, _art())

    def register():
        barrier.wait()
        stack.targets.register(f, title="R", kind="movie", actor="op")

    ts = [threading.Thread(target=commit), threading.Thread(target=register)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    stack.reconciler.intent_pass(grace_s=0)
    (t,) = stack.targets.list()
    (b,) = stack.targets.bindings(t["target_id"])
    assert b["state"] == "READY" and t["state"] == "READY"
    events = [i for i in stack.store.intents() if i.kind == "target_event"]
    assert len(events) == 1  # one registration event


def test_tr7_crash_after_staging(stack, tmp_path):
    f = _file(tmp_path)

    def hook(point, **ctx):
        if point == "after_staging":
            raise Crash(point)

    stack.targets.hook = hook
    with pytest.raises(Crash):
        stack.targets.register(f, title="S", kind="movie", actor="op")
    assert len(_keys(stack, "staging/")) == 1 and _keys(stack, "ref/") == []
    stack.reconciler.intent_pass(grace_s=0)
    assert _keys(stack, "staging/") == [] and list(stack.store.intents()) == []
    stack.targets.hook = lambda p, **c: None
    reg = stack.targets.register(f, title="S", kind="movie", actor="op")
    assert _keys(stack, "ref/") == [ref_key(reg.sha256)]


def test_tr7_crash_after_promotion_before_facts(stack, tmp_path):
    f = _file(tmp_path)

    def hook(point, **ctx):
        if point == "after_promote":
            raise Crash(point)

    stack.targets.hook = hook
    with pytest.raises(Crash):
        stack.targets.register(f, title="P", kind="movie", actor="op")
    stack.reconciler.intent_pass(grace_s=0)
    assert _keys(stack, "staging/") == []
    assert len(_keys(stack, "ref/")) == 1 and stack.targets.list() == []
    # unreferenced ref/<D> is an orphan after the grace…
    assert stack.reconciler.sweep(grace_s=3600)["deleted"] == 0
    assert stack.reconciler.sweep(grace_s=0)["deleted"] == 1
    # …and a retried registration before the sweep reuses the verified object
    with pytest.raises(Crash):
        stack.targets.register(f, title="P", kind="movie", actor="op")
    stack.targets.hook = lambda p, **c: None
    stack.reconciler.intent_pass(grace_s=0)
    reg = stack.targets.register(f, title="P", kind="movie", actor="op")
    assert reg.reused_object
    assert stack.reconciler.sweep(grace_s=0)["deleted"] == 0
    assert _keys(stack, "ref/") == [ref_key(reg.sha256)] and _keys(stack, "staging/") == []


def test_tr7_file_changed_between_passes_refused(stack, tmp_path):
    f = _file(tmp_path)

    def hook(point, **ctx):
        if point == "after_register_intent":
            f.write_bytes(os.urandom(300_000))

    stack.targets.hook = hook
    with pytest.raises(TargetError, match="changed"):
        stack.targets.register(f, title="X", kind="movie", actor="op")
    assert _keys(stack, "staging/") == [] and _keys(stack, "ref/") == []
    assert stack.targets.list() == [] and list(stack.store.intents()) == []


def test_tr7_concurrent_same_bytes_registrations(stack, tmp_path):
    f = _file(tmp_path)
    results, errors = [], []

    def go(i, target_id=None):
        try:
            results.append(stack.fresh().targets.register(f, title=f"C{i}", kind="movie",
                                                          target_id=target_id, actor="op"))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    ts = [threading.Thread(target=go, args=(i,)) for i in range(3)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert not errors and len({r.target_id for r in results}) == 3
    assert len(_keys(stack, "ref/")) == 1 and _keys(stack, "staging/") == []
    # same --target-id concurrently: one version
    tid = results[0].target_id
    results.clear()
    ts = [threading.Thread(target=go, args=(i, tid)) for i in range(3)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert not errors and {(r.target_id, r.version) for r in results} == {(tid, 1)}


def test_tr7_multipart_copy_above_single_copy_limit(stack, tmp_path):
    f = _file(tmp_path, "big.bin", os.urandom(12 * 1024 * 1024))
    stack.objects.single_copy_limit = 8 * 1024 * 1024  # exercise the >5 GiB path on 12 MiB
    stack.objects.copy_part_bytes = 5 * 1024 * 1024
    calls = []
    real = stack.objects.client.compose_object
    stack.objects.client.compose_object = lambda *a, **k: calls.append(len(a[2])) or real(*a, **k)
    reg = stack.targets.register(f, title="Big", kind="movie", actor="op")
    assert calls == [3]  # 5 + 5 + 2 MiB server-side part copies
    info = stack.objects.stat(stack.objects.cfg.targets_bucket, ref_key(reg.sha256))
    assert info.size == 12 * 1024 * 1024 and info.metadata["sha256"] == reg.sha256
    data = b"".join(stack.objects.stream(stack.objects.cfg.targets_bucket, ref_key(reg.sha256)))
    assert __import__("hashlib").sha256(data).hexdigest() == reg.sha256


@pytest.mark.slow
@pytest.mark.skipif(os.environ.get("RUN_P9_SLOW") != "1", reason="writes ~15 GiB to scratch/MinIO (RUN_P9_SLOW=1)")
def test_tr7_real_multipart_copy_above_5gib(stack, tmp_path):
    """TR-7 at real scale: a reference > 5 GiB is promoted by multipart copy with the production limits."""
    from fingerprinter.objects import S3_SINGLE_COPY_LIMIT

    size = S3_SINGLE_COPY_LIMIT + 64 * 1024**2
    f = tmp_path / "huge.bin"
    block = os.urandom(64 * 1024**2)  # repeated random block: real bytes, not a sparse file
    with f.open("wb") as fh:
        for _ in range(size // len(block)):
            fh.write(block)
    assert f.stat().st_size == size
    assert stack.objects.single_copy_limit == S3_SINGLE_COPY_LIMIT  # production limit, not lowered
    calls = []
    real = stack.objects.client.compose_object
    stack.objects.client.compose_object = lambda *a, **k: calls.append(len(a[2])) or real(*a, **k)
    reg = stack.targets.register(f, title="Huge", kind="movie", actor="op")
    assert reg.size == size
    assert calls == [-(-size // stack.objects.copy_part_bytes)]  # one compose of server-side part copies
    info = stack.objects.stat(stack.objects.cfg.targets_bucket, ref_key(reg.sha256))
    assert info.size == size and info.metadata["sha256"] == reg.sha256
    assert _keys(stack, "staging/") == []
    sha = __import__("hashlib").sha256()
    for chunk in stack.objects.stream(stack.objects.cfg.targets_bucket, ref_key(reg.sha256)):
        sha.update(chunk)
    assert sha.hexdigest() == reg.sha256
    f.unlink()
    stack.targets.retire(reg.target_id, reason="test", actor="op")
    assert stack.targets.purge(reg.target_id, actor="op") == [reg.sha256]
    assert _keys(stack, "ref/") == []


def test_tr7_purge_keeps_shared_reference(stack, tmp_path):
    f = _file(tmp_path)
    a = stack.targets.register(f, title="A", kind="movie", actor="op")
    b = stack.targets.register(f, title="B", kind="movie", actor="op")
    with pytest.raises(TargetError):
        stack.targets.purge(a.target_id, actor="op")  # must be retired first
    stack.targets.retire(a.target_id, reason="done", actor="op")
    assert stack.targets.purge(a.target_id, actor="op") == []
    assert _keys(stack, "ref/") == [ref_key(a.sha256)]  # still referenced by B
    stack.targets.retire(b.target_id, reason="done", actor="op")
    assert stack.targets.purge(b.target_id, actor="op") == [a.sha256]
    assert _keys(stack, "ref/") == []


def test_size_limit_checked_before_upload(stack, tmp_path):
    f = _file(tmp_path, "s.bin", b"x" * 1000)
    small = stack.fresh(acquire=stack.settings.acquire.model_copy(update={"max_bytes": 999}))
    from fingerprinter.targets import TargetManager

    with pytest.raises(TargetError, match="limit"):
        TargetManager(small).register(f, title="S", kind="movie", actor="op")
    assert _keys(stack, "") == []


def test_target_build_failure_visible_and_no_encode_failed(stack, web, tmp_path):
    ensure_group(stack, READY)
    ensure_group(stack, FAILED)
    reg = stack.targets.register(_file(tmp_path, "notvideo.bin"), title="Bad", kind="movie", actor="op")
    enc, _ = make_encoder(stack, tmp_path)
    enc.run(idle_exit_s=2)
    t = stack.targets.target(reg.target_id, reg.version)
    assert t["state"] == "BUILD_FAILED" and t["failure"].startswith("unsupported_format")
    assert stack.coord.client.xlen(stack.coord.stream(FAILED)) == 0
    assert stack.targets.show(reg.target_id)["versions"][0]["state"] == "BUILD_FAILED"


def test_target_encoded_from_internal_reference(stack, web, tmp_path):
    ensure_group(stack, READY)
    video = make_video(tmp_path / "ref.mp4", seconds=6, seed=5)
    reg = stack.targets.register(video, title="Ref", kind="movie", actor="op")
    enc, _ = make_encoder(stack, tmp_path)
    enc.run(idle_exit_s=2)
    t = stack.targets.target(reg.target_id, reg.version)
    assert t["state"] == "READY"
    (b,) = stack.targets.bindings(reg.target_id)
    assert b["state"] == "READY" and b["target_representation_id"]
    assert web.requests == []  # never fetched over HTTP
    shutil.rmtree(tmp_path / "scratch-h1", ignore_errors=True)
