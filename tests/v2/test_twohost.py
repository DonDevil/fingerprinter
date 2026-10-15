"""Simulated two-host operation (design §15.11, P9-D18, TH-1…TH-8).

Two (or more) **separate encoder processes** with their own ``host_id``, scratch
directory and process space share the real Scylla/Redis/MinIO. Simulated only:
one machine, no network partition (real multi-machine validation is P14).
Deviation (recorded in design §22): the CPU "host" is a second host process, not
a container — a CPU container with a different ffmpeg/torch build would not match
runtime profile ``rt-1`` and must be refused (fail closed, §3.5).
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from fingerprinter.publish import READY
from tests.v2.conftest import ensure_group, integration, settings_env
from tests.v2.helpers import make_video, request_env
from tests.v2.test_encoder_failures import _admit_cuda, _cuda

pytestmark = [integration, pytest.mark.slow]
ROOT = Path(__file__).resolve().parents[2]


def _spawn(stack, tmp_path, host, device="cpu", idle=8.0, **extra):
    enc_cfg = stack.settings.encoder.model_copy(update={
        "decode_threads": 2, "poll_interval_s": 0.2, "attempt_lease_s": 20.0,
        "attempt_lease_renew_s": 5.0, "lease_ms": 15_000,
    })
    acq = stack.settings.acquire.model_copy(update={"ssrf_allow_cidrs": ["127.0.0.0/8"]})
    s = stack.settings.model_copy(update={
        "host_id": host, "scratch_dir": tmp_path / f"scratch-{host}", "encoder": enc_cfg,
        "acquire": acq, "roles": ["encoder", "scheduler"],
    })
    log = open(tmp_path / f"{host}.log", "w")  # noqa: SIM115
    proc = subprocess.Popen(  # noqa: S603
        [sys.executable, "-m", "fingerprinter", "encoder", "run", "--device", device,
         "--idle-exit-s", str(idle)],
        cwd=ROOT, env=settings_env(s, **extra), stdout=log, stderr=subprocess.STDOUT,
    )
    proc.scratch = s.scratch_dir
    return proc


def _work(stack, web, n, seconds=20):
    ensure_group(stack, READY)
    cids = []
    for i in range(n):
        v = make_video(web.httpd.root / f"w{i}.mp4", seconds=seconds, seed=i + 1)
        env = request_env(v.read_bytes(), web.url("f", f"w{i}.mp4"))
        stack.requests.upsert_event(env)
        cids.append(str(env.payload.content.content_id))
    return cids


def _wait(procs, timeout=900):
    deadline = time.monotonic() + timeout
    for p in procs:
        p.wait(timeout=max(1, deadline - time.monotonic()))
    return [p.returncode for p in procs]


def _assert_all_ready_once(stack, cids):
    reqs = stack.requests.all()
    assert {r["state"] for r in reqs} == {"READY"}, [(r["state"], r["last_detail"]) for r in reqs]
    reps = stack.db.rows("SELECT * FROM {ks}.representations")
    assert sorted(r["content_id"] for r in reps) == sorted(cids)  # one authoritative row each
    objs = list(stack.objects.list(stack.objects.cfg.representations_bucket, "rep/"))
    stack.reconciler.intent_pass(grace_s=0)
    stack.reconciler.sweep(grace_s=0)
    objs = list(stack.objects.list(stack.objects.cfg.representations_bucket, "rep/"))
    assert len(objs) == len(cids)
    assert list(stack.store.intents()) == []
    return reps


def test_th1_th2_th3_th7_two_cpu_hosts_share_state_first_committer_wins(stack, web, tmp_path):
    cids = _work(stack, web, 8)
    a = _spawn(stack, tmp_path, "host-a")
    b = _spawn(stack, tmp_path, "host-b")
    # duplicate requests arrive while both run (TH-2)
    for i in range(3):
        data = (web.httpd.root / f"w{i}.mp4").read_bytes()
        os.link(web.httpd.root / f"w{i}.mp4", web.httpd.root / f"dup{i}.mp4")
        stack.requests.upsert_event(request_env(data, web.url("f", f"dup{i}.mp4")))
    assert _wait([a, b]) == [0, 0]
    reps = _assert_all_ready_once(stack, cids)
    hosts = {json.loads(r["runtime_fingerprint"])["device"] for r in reps}
    assert hosts == {"cpu"}
    workers = {r["artifact_attempt_id"] for r in reps}
    assert len(workers) == len(cids)
    # TH-7: each process used only its own scratch, nothing left behind
    for p in (a, b):
        assert all(e.name == "exit_reports.jsonl" for e in Path(p.scratch).iterdir())


def test_th5_th6_kill9_one_host_no_lost_work(stack, web, tmp_path):
    cids = _work(stack, web, 6, seconds=40)
    a = _spawn(stack, tmp_path, "victim", idle=60)
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:  # wait until the victim owns work
        if any(r["state"] == "IN_PROGRESS" for r in stack.requests.all()):
            break
        time.sleep(0.5)
    a.send_signal(signal.SIGKILL)  # class 3: uncontrolled, nothing runs in-process
    a.wait()
    assert a.returncode == -signal.SIGKILL
    owned = [r for r in stack.requests.all() if r["state"] == "IN_PROGRESS"]
    assert owned, "victim held no attempt when killed"
    # the survivor must outlive the victim's attempt leases (20 s): abandoned work becomes
    # claimable only after lease expiry, never earlier (fencing)
    b = _spawn(stack, tmp_path, "survivor", idle=45)
    assert _wait([b]) == [0]
    _assert_all_ready_once(stack, cids)  # abandoned leases expired → requeued → done
    killed = {r["content_id"] for r in owned}
    after = {r["content_id"]: r for r in stack.requests.all()}
    assert all(after[c]["failures"] == 0 and after[c]["claims"] >= 2 for c in killed)


@pytest.mark.skipif(not _cuda(), reason="GPU unavailable: BLOCKED")
def test_th4_th8_gpu_host_and_cpu_host_together(stack, web, tmp_path):
    _admit_cuda(stack)
    cids = _work(stack, web, 8, seconds=30)
    g = _spawn(stack, tmp_path, "gpu-host", device="cuda")
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline and not any(
        r["state"] != "PENDING" for r in stack.requests.all()
    ):
        time.sleep(0.5)  # GPU host started (calibrated, golden check passed) and claims
    g2 = _spawn(stack, tmp_path, "gpu-host", device="cuda")  # same host: lease held → 73
    c = _spawn(stack, tmp_path, "cpu-host", device="cpu")
    codes = _wait([g, g2, c])
    assert sorted(codes) == [0, 0, 73]
    reps = _assert_all_ready_once(stack, cids)
    devices = {r["device"] for r in reps}
    assert devices <= {"cpu", "cuda"} and "cuda" in devices
