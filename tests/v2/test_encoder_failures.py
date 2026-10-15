"""Encoder failure classes, leases and exit codes (design §12, TX-1…TX-11).

CPU tests inject faults through the embedder hook (classification logic);
``gpu`` tests run on the real RTX 2050 (skipped, i.e. reported BLOCKED, if CUDA
is unavailable) and provoke real CUDA OOM by reserving VRAM.
"""

from __future__ import annotations

import json
import threading
import time

import numpy as np
import pytest

from fingerprinter import artifact
from fingerprinter.encoder import (
    EXIT_CONFIG,
    EXIT_CONTROLLED,
    EXIT_DEVICE,
    EXIT_GPU_LEASE_HELD,
    EXIT_OK,
    EXIT_UNAVAILABLE,
)
from fingerprinter.errors import DeviceError
from fingerprinter.model import OutOfMemory
from fingerprinter.objects import parse_uri
from fingerprinter.publish import READY
from fingerprinter.runtime import RuntimeProfile
from fingerprinter.spec import RUNTIME_PROFILE
from tests.v2.conftest import ensure_group, fingerprint, integration, make_encoder
from tests.v2.helpers import make_video, request_env

pytestmark = integration


def _cuda() -> bool:
    import torch

    return torch.cuda.is_available()


gpu = pytest.mark.skipif(not _cuda(), reason="GPU unavailable: BLOCKED")


def _admit_cuda(stack):
    """rt-1 with both device variants, cuda admitted (the CUDA golden check runs in TX-2)."""
    stack.db.run("DELETE FROM {ks}.runtime_profiles WHERE profile = ?", RUNTIME_PROFILE)
    profile = RuntimeProfile.from_fingerprints(RUNTIME_PROFILE, fingerprint("cpu"), fingerprint("cuda"))
    stack.store.put_profile(profile, actor="test")
    stack.store.admit_variant(RUNTIME_PROFILE, "cuda", actor="test", evidence="TX-2 golden check")


def _requests(stack, web, n, seconds=12):
    ensure_group(stack, READY)
    cids = []
    for i in range(n):
        v = make_video(web.httpd.root / f"v{i}.mp4", seconds=seconds, seed=i + 1)
        env = request_env(v.read_bytes(), web.url("f", f"v{i}.mp4"))
        stack.requests.upsert_event(env)
        cids.append(str(env.payload.content.content_id))
    return cids


def _vectors(stack, cid):
    rep = stack.store.representation(cid, str(stack.spec.digest))
    b, k = parse_uri(rep["artifact_uri"])
    arrays, manifest = artifact.load(stack.objects.read_verified(b, k, rep["artifact_size"], rep["artifact_sha256"]))
    return arrays, manifest


def _reports(enc):
    path = enc.scratch / "exit_reports.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()]


# --- TX-1 startup refusals -----------------------------------------------------------


def test_tx1_cuda_unavailable_typed_exit(stack, tmp_path, monkeypatch):
    import torch

    from fingerprinter.encoder import Encoder

    _, view = make_encoder(stack, tmp_path)
    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
    enc = Encoder(view, device="cuda", golden_check=False, calibrate=False, embedders={},
                  fingerprint=fingerprint("cpu"))
    assert enc.run(idle_exit_s=1) == EXIT_UNAVAILABLE
    assert _reports(enc)[-1]["reason"] == "cuda_unavailable"


def test_tx1_model_missing_typed_exit(stack, tmp_path):
    from fingerprinter.encoder import Encoder

    enc, view = make_encoder(stack, tmp_path)
    settings = view.settings.model_copy(update={"model_cache_dir": tmp_path / "empty-hf-cache"})
    view.settings = settings
    bare = Encoder(view, device="cpu", golden_check=False, calibrate=False, fingerprint=fingerprint("cpu"))
    assert bare.run(idle_exit_s=1) == EXIT_UNAVAILABLE
    assert "ModelUnavailableError" in _reports(bare)[-1]["reason"]


def test_tx1_runtime_incompatible_refuses_all_work(stack, tmp_path):
    from dataclasses import replace

    from fingerprinter.encoder import Encoder

    drifted = replace(fingerprint("cpu"), common={**fingerprint("cpu").common, "torch": "9.9.9"})
    _, view = make_encoder(stack, tmp_path)
    enc = Encoder(view, device="cpu", golden_check=False, calibrate=False, fingerprint=drifted,
                  embedders={})
    assert enc.run(idle_exit_s=1) == EXIT_CONFIG
    assert "runtime_incompatible" in enc.refused[str(stack.spec.digest)]


def test_golden_check_failure_refuses_device(stack, tmp_path, monkeypatch):
    import fingerprinter.encoder as encmod

    bad = tmp_path / "golden.npy"
    np.save(bad, -np.load(encmod.GOLDEN_CPU))
    monkeypatch.setattr(encmod, "GOLDEN_CPU", bad)
    enc, _ = make_encoder(stack, tmp_path, golden=True)
    assert enc.run(idle_exit_s=1) == EXIT_CONFIG
    assert "golden_check_failed" in _reports(enc)[-1]["reason"]


def test_cpu_golden_check_passes(stack, tmp_path):
    enc, _ = make_encoder(stack, tmp_path, golden=True)
    assert enc.start() == EXIT_OK and enc.golden_min_cos >= 0.9999


# --- TX-6/TX-7/TX-8 failure classes (fault injection, CPU) --------------------------


def test_tx6_batch_oom_halves_and_retries_same_items(stack, web, tmp_path):
    (cid,) = _requests(stack, web, 1, seconds=40)  # 8 segments
    clean, _ = make_encoder(stack, tmp_path, host_id="clean")
    clean.run(idle_exit_s=2)
    ref, _ = _vectors(stack, cid)
    # second content: same bytes impossible (would be reused) → re-encode via a new spec-free path:
    stack.db.run("DELETE FROM {ks}.representations WHERE content_id = ?", cid)
    stack.db.lwt("UPDATE {ks}.encode_requests SET state = 'FAILED' WHERE content_id = ? AND spec_key = 'default' IF EXISTS", cid)
    assert stack.requests.retry(cid, "default", actor="test")
    enc, _ = make_encoder(stack, tmp_path, host_id="oom", cpu_batch=8)
    seen = []

    def fault(b):
        seen.append(b)
        if b > 2:
            raise OutOfMemory("injected batch OOM")

    enc.embedders["float32"].fault = fault
    try:
        assert enc.run(idle_exit_s=2) == EXIT_OK
    finally:
        enc.embedders["float32"].fault = None
    assert enc.batch <= 2 and max(seen) > 2 and seen[-1] <= 2  # halved from the failing size
    got, _ = _vectors(stack, cid)
    assert got["segment_index"].tolist() == ref["segment_index"].tolist()  # no loss, no duplicate
    assert got["pts"].tolist() == ref["pts"].tolist()
    cos = (got["vectors"] * ref["vectors"]).sum(axis=1)
    assert cos.min() >= 0.9999


def test_tx7_single_item_oom_controlled_exit_then_restart_completes(stack, web, tmp_path):
    poison, other = _requests(stack, web, 2, seconds=12)
    enc, _ = make_encoder(stack, tmp_path, host_id="h1", cpu_batch=1, decode_threads=2)
    blocker = threading.Event()

    def fault(b):
        blocker.wait(5)  # let both attempts get in flight first
        raise OutOfMemory("injected single-item OOM")

    enc.embedders["float32"].fault = fault
    threading.Timer(1.5, blocker.set).start()
    try:
        assert enc.run(idle_exit_s=10) == EXIT_CONTROLLED
    finally:
        enc.embedders["float32"].fault = None
    assert _reports(enc)[-1]["reason"] == "gpu_oom_single"
    rows = {r["content_id"]: r for r in stack.requests.all()}
    failed = [r for r in rows.values() if r["last_detail"] == "gpu_oom_single"]
    assert len(failed) == 1 and failed[0]["failures"] == 1 and failed[0]["state"] == "PENDING"
    assert failed[0]["last_retryable"] is True
    released = [r for r in rows.values() if r is not failed[0]]
    assert all(r["failures"] == 0 for r in released), [(r["state"], r["failures"], r["last_failure"], r["last_detail"], r["claims"]) for r in rows.values()]  # released, not attributed
    assert {r["state"] for r in rows.values()} == {"PENDING"}
    # supervisor restart: remaining work completes (backoff elapsed for the OOM item)
    stack.clock.advance(seconds=stack.settings.encoder.backoff_base_s + 1)
    again, _ = make_encoder(stack, tmp_path, host_id="h1")
    assert again.run(idle_exit_s=2) == EXIT_OK
    assert {r["state"] for r in stack.requests.all()} == {"READY"}


def test_tx8_device_error_exit_70_releases_without_cpu_switch(stack, web, tmp_path):
    _requests(stack, web, 1)
    enc, _ = make_encoder(stack, tmp_path, host_id="h1")

    def fault(b):
        raise DeviceError("CUDA error: an illegal memory access was encountered")

    enc.embedders["float32"].fault = fault
    try:
        assert enc.run(idle_exit_s=5) == EXIT_DEVICE
    finally:
        enc.embedders["float32"].fault = None
    (r,) = stack.requests.all()
    assert r["state"] == "PENDING" and r["failures"] == 0 and r["last_detail"] == "released:device_error"
    assert enc.device == "cpu" and _reports(enc)[-1]["exit_code"] == EXIT_DEVICE


def test_tx9_bounded_queue_under_slow_inference(stack, web, tmp_path):
    _requests(stack, web, 2, seconds=60)  # 24 segments
    enc, _ = make_encoder(stack, tmp_path, cpu_batch=2, queue_items=4)
    depths = []
    real_put = enc.queue.put

    def put(item, *a, **k):
        real_put(item, *a, **k)
        depths.append(enc.queue.qsize())

    enc.queue.put = put

    def slow(b):
        time.sleep(0.05)

    enc.embedders["float32"].fault = slow
    try:
        assert enc.run(idle_exit_s=3) == EXIT_OK
    finally:
        enc.embedders["float32"].fault = None
    assert max(depths) <= 4 + 2  # cap Q plus one in-flight put per decode thread
    assert {r["state"] for r in stack.requests.all()} == {"READY"}


def test_tx11_attempt_timeout_is_retryable_and_cleaned(stack, web, tmp_path):
    _requests(stack, web, 1, seconds=30)
    enc, view = make_encoder(stack, tmp_path, attempt_timeout_s=1.0)

    def slow(b):
        time.sleep(0.6)

    enc.embedders["float32"].fault = slow
    try:
        enc.run(idle_exit_s=4, max_attempts=1)
    finally:
        enc.embedders["float32"].fault = None
    (r,) = stack.requests.all()
    assert r["state"] == "PENDING" and r["last_failure"] == "timeout" and r["last_retryable"]
    assert r["failures"] == 1 and r["attempt_id"] is None
    assert not any(p.is_dir() and p.name != "exit_reports.jsonl" for p in enc.scratch.iterdir())
    assert list(stack.objects.list(stack.objects.cfg.representations_bucket, "rep/")) == []


def test_tx5_sigterm_drains_commits_finished_releases_rest(stack, web, tmp_path):
    _requests(stack, web, 3, seconds=20)
    enc, _ = make_encoder(stack, tmp_path, decode_threads=1, shutdown_grace_s=30)
    first_done = threading.Event()
    real = enc.s.committer.commit

    def commit(*a, **k):
        out = real(*a, **k)
        first_done.set()
        enc.draining.set()  # SIGTERM arrives after the first commit
        return out

    enc.s.committer.commit = commit
    assert enc.run(idle_exit_s=10) == EXIT_OK
    states = sorted(r["state"] for r in stack.requests.all())
    assert states.count("READY") >= 1 and "IN_PROGRESS" not in states
    assert all(r["failures"] == 0 for r in stack.requests.all())


def test_tx5_service_stop_also_signalling_ffmpeg_releases_not_fails(stack, web, tmp_path, monkeypatch):
    """A service stop signals the whole cgroup: ffmpeg dies of SIGTERM while the encoder drains.

    Found by SOAK-1000 run 1: such attempts became permanent ``decode_error``
    failures of valid media. They must be released (no failure counted).
    """
    import signal
    import subprocess

    _requests(stack, web, 1, seconds=60)
    procs: list[subprocess.Popen] = []
    real_popen = subprocess.Popen

    def popen(*a, **k):
        p = real_popen(*a, **k)
        procs.append(p)
        return p

    monkeypatch.setattr(subprocess, "Popen", popen)
    enc, _ = make_encoder(stack, tmp_path, decode_threads=1, shutdown_grace_s=10)
    fired: list[int] = []

    def stop(b):
        if not fired:
            fired.append(b)
            enc.draining.set()  # the encoder's own SIGTERM ...
            procs[-1].send_signal(signal.SIGTERM)  # ... and the same stop reaching ffmpeg
            procs[-1].wait(10)

    enc.embedders["float32"].fault = stop
    try:
        assert enc.run(idle_exit_s=5) == EXIT_OK
    finally:
        enc.embedders["float32"].fault = None
    assert fired
    (r,) = stack.requests.all()
    assert r["state"] == "PENDING" and r["failures"] == 0 and r["attempt_id"] is None
    assert r["last_failure"] is None


def test_claim_crash_is_a_controlled_exit_not_a_dead_worker(stack, web, tmp_path):
    """Found by SOAK-1000 run 1: an exception escaping the claim killed a worker thread silently."""
    _requests(stack, web, 1)
    enc, _ = make_encoder(stack, tmp_path)

    def broken(*a, **k):
        raise RuntimeError("injected claim bug")

    enc.s.requests.claim = broken
    assert enc.run(idle_exit_s=5) == EXIT_CONTROLLED
    assert _reports(enc)[-1]["reason"] == "claim_crashed"
    (r,) = stack.requests.all()
    assert r["state"] == "PENDING" and r["failures"] == 0


# --- real GPU ----------------------------------------------------------------------------


@gpu
def test_tx2_th8_cpu_and_cuda_fp32_artifacts_agree(stack, web, tmp_path):
    _admit_cuda(stack)
    (cid,) = _requests(stack, web, 1, seconds=30)
    cpu, _ = make_encoder(stack, tmp_path, host_id="cpu-host")
    cpu.run(idle_exit_s=2)
    a, ma = _vectors(stack, cid)
    stack.db.run("DELETE FROM {ks}.representations WHERE content_id = ?", cid)
    stack.db.lwt("UPDATE {ks}.encode_requests SET state = 'FAILED' WHERE content_id = ? AND spec_key = 'default' IF EXISTS", cid)
    stack.requests.retry(cid, "default", actor="test")
    cuda, _ = make_encoder(stack, tmp_path, device="cuda", host_id="gpu-host", golden=True)
    assert cuda.run(idle_exit_s=2) == EXIT_OK
    assert cuda.golden_min_cos >= 0.999
    b, mb = _vectors(stack, cid)
    assert ma["device"] == "cpu" and mb["device"] == "cuda"
    assert a["segment_index"].tolist() == b["segment_index"].tolist()
    assert a["pts"].tolist() == b["pts"].tolist()
    assert float((a["vectors"] * b["vectors"]).sum(axis=1).min()) >= 0.999


@gpu
def test_tx3_second_gpu_owner_exits_73(stack, tmp_path):
    _admit_cuda(stack)
    one, _ = make_encoder(stack, tmp_path, device="cuda", host_id="g1")
    assert one.start() == EXIT_OK
    two, _ = make_encoder(stack, tmp_path, device="cuda", host_id="g1")
    assert two.run(idle_exit_s=1) == EXIT_GPU_LEASE_HELD
    one._release_gpu()
    three, _ = make_encoder(stack, tmp_path, device="cuda", host_id="g1")
    assert three.start() == EXIT_OK
    three._release_gpu()


@gpu
def test_tx4_gpu_lease_lost_controlled_exit(stack, web, tmp_path):
    _admit_cuda(stack)
    _requests(stack, web, 2, seconds=40)
    enc, _ = make_encoder(stack, tmp_path, device="cuda", host_id="g1", gpu_lease_renew_ms=300)

    def steal(b):
        stack.coord.client.set(enc.gpu_key, "intruder")  # lease taken over (e.g. Redis restart)
        time.sleep(0.2)

    orig_start = enc._start

    def start():  # arm the fault after startup: TX-4 is lease loss while encoding
        orig_start()
        enc.embedders["float32"].fault = steal

    enc._start = start
    try:
        assert enc.run(idle_exit_s=10) == EXIT_CONTROLLED
    finally:
        enc.embedders["float32"].fault = None
    assert _reports(enc)[-1]["reason"] == "gpu_lease_lost"
    assert all(r["state"] in ("PENDING", "READY") and r["failures"] == 0 for r in stack.requests.all())
    assert stack.coord.holder(enc.gpu_key) == "intruder"  # never released someone else's lease


@gpu
def test_tx6_real_cuda_oom_recovered_by_halving(stack, web, tmp_path):
    import torch

    _admit_cuda(stack)
    (cid,) = _requests(stack, web, 1, seconds=120)  # 24 segments
    enc, _ = make_encoder(stack, tmp_path, device="cuda", host_id="oomhost", calibrate=False)
    enc.do_calibrate = False
    orig_start = enc._start
    holder = {}

    def start():
        orig_start()  # model already resident on the GPU
        torch.cuda.empty_cache()
        free, _ = torch.cuda.mem_get_info()
        holder["hog"] = torch.empty(int(free - 160 * 1024**2), dtype=torch.uint8, device="cuda")
        enc.batch = 64  # cannot fit in ~160 MiB of remaining VRAM

    enc._start = start
    try:
        assert enc.run(idle_exit_s=3) == EXIT_OK
    finally:
        holder.clear()
        torch.cuda.empty_cache()
    assert enc.batch < 24  # halved on a real torch.cuda.OutOfMemoryError
    r = stack.requests.get(cid, "default")
    assert r["state"] == "READY" and r["failures"] == 0
    arrays, _ = _vectors(stack, cid)
    assert len(arrays["segment_index"]) == len(set(arrays["segment_index"].tolist())) == 24


@gpu
def test_gpu_calibration_records_measured_batch(stack, tmp_path):
    """OF-2: the stored batch is the measured-throughput selection with its report."""
    _admit_cuda(stack)
    enc, _ = make_encoder(stack, tmp_path, device="cuda", host_id="calib", max_batch=64,
                          calibration_frames=32)
    enc.do_calibrate = True
    assert enc.start() == EXIT_OK
    row = stack.db.one("SELECT * FROM {ks}.encoder_hosts WHERE host_id = 'calib' ALLOW FILTERING")
    rep = json.loads(row["calibration"])
    assert rep["method"] == "throughput/v1" and enc.batch == row["b_start"] == rep["selected_batch"]
    assert row["b_max"] == rep["b_max_fitting"] >= rep["selected_batch"]
    measured = {c["batch"]: c for c in rep["candidates"] if c["eligible"]}
    assert all(len(c["frames_per_s"]) >= 3 and c["peak_vram_bytes"] > 0 for c in measured.values())
    best = max(c["median"] for c in measured.values())
    assert rep["selected_frames_per_s"] >= (1 - rep["parameters"]["tolerance"]) * best
    assert rep["profile"]["gpu"]["name"] and rep["profile"]["spec_digest"] == str(stack.spec.digest)
    enc._release_gpu()
    # a second start on the same host reuses the stored result (no recalibration)
    again, _ = make_encoder(stack, tmp_path, device="cuda", host_id="calib")
    again.do_calibrate = True
    again.calibrate = None  # would raise if called
    assert again.start() == EXIT_OK and again.batch == row["b_start"]
    again._release_gpu()


def test_superseded_bmax_half_row_is_recalibrated(stack, tmp_path):
    from fingerprinter.encoder import Encoder

    enc, _ = make_encoder(stack, tmp_path, host_id="old-rule")
    enc.device = "cuda"
    enc.allowed = {str(stack.spec.digest): stack.spec}
    stack.db.run(
        "INSERT INTO {ks}.encoder_hosts (host_id, device, spec_digest, compute_dtype, b_max, b_start) "
        "VALUES ('old-rule', 'cuda', ?, 'float32', 64, 32)", str(stack.spec.digest))
    calls = []
    enc.calibrate = lambda spec: calls.append(spec) or {
        "method": "throughput/v1", "selected_batch": 8, "selected_frames_per_s": 41.0,
        "b_max_fitting": 64, "selection_reason": "test"}
    assert Encoder._calibrated_batch(enc) == 8 and len(calls) == 1
    row = stack.db.one("SELECT * FROM {ks}.encoder_hosts WHERE host_id = 'old-rule' ALLOW FILTERING")
    assert row["b_start"] == 8 and json.loads(row["calibration"])["method"] == "throughput/v1"


class _FailingEmbedder:
    def __init__(self, exc):
        self.exc = exc

    def embed(self, batch):
        raise self.exc

    def free(self):
        pass

    def vram(self):
        return {}

    def sync(self):
        pass

    def reset_peak(self):
        pass


@pytest.mark.parametrize("exc,code,reason", [
    (OutOfMemory("fake"), EXIT_CONTROLLED, "calibration: batch 1 does not fit"),
    (DeviceError("fake xid"), EXIT_DEVICE, "calibration: device_error"),
])
def test_calibration_failure_is_a_typed_controlled_exit(stack, tmp_path, exc, code, reason):
    from fingerprinter.encoder import ControlledExit

    enc, _ = make_encoder(stack, tmp_path, host_id="calfail")
    enc.device = "cuda"
    enc.embedders = {"float32": _FailingEmbedder(exc)}
    assert stack.coord.acquire(enc.gpu_key, enc.gpu_token, 30_000)  # as _start holds it
    with pytest.raises(ControlledExit) as e:
        enc.calibrate(stack.spec)
    assert e.value.code == code and e.value.reason.startswith(reason)
    assert stack.coord.client.get(enc.gpu_key) is None  # GPU lease released


@gpu
def test_gpu_lease_kept_alive_during_slow_startup(stack, tmp_path):
    """Found by TX-3 after OF-2: calibration outlasts the GPU lease TTL; the lease
    must be renewed between acquisition and the supervisor (no second owner)."""
    _admit_cuda(stack)
    one, _ = make_encoder(stack, tmp_path, device="cuda", host_id="slow", gpu_lease_ms=1500,
                          gpu_lease_renew_ms=300)
    one._calibrated_batch = lambda: time.sleep(4) or 2  # longer than 2 × the lease TTL
    assert one.start() == EXIT_OK and one.batch == 2
    assert stack.coord.holder(one.gpu_key) == one.gpu_token
    two, _ = make_encoder(stack, tmp_path, device="cuda", host_id="slow")
    assert two.run(idle_exit_s=1) == EXIT_GPU_LEASE_HELD
    one._release_gpu()


@gpu
def test_gpu_lease_lost_during_startup_is_a_controlled_exit(stack, tmp_path):
    _admit_cuda(stack)
    enc, _ = make_encoder(stack, tmp_path, device="cuda", host_id="stolen", gpu_lease_ms=1500,
                          gpu_lease_renew_ms=300)

    def steal():
        stack.coord.client.set(enc.gpu_key, "someone-else")
        time.sleep(1.5)
        return 2

    enc._calibrated_batch = steal
    assert enc.run(idle_exit_s=1) == EXIT_CONTROLLED
    assert _reports(enc)[-1]["reason"] == "gpu_lease_lost during startup"
    assert stack.coord.holder(enc.gpu_key) == "someone-else"  # never released someone else's lease
