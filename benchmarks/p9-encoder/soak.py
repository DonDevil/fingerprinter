"""Observable soak gate harness (design §13.3, P9-D25, B-SOAK).

Runs the whole fingerprinter on a corpus against the real stack and evaluates
the eight soak criteria. **The gate is SOAK-1000** (data-preparation §2.4);
any other corpus yields a *harness run* whose report says ``gate_eligible:
false`` — it never counts as the soak gate.

Processes: intake (thread) reading crawler2-shaped ``encode.requested`` events;
encoder **subprocesses** under a supervisor (restart after controlled exits 75
and 70; 73 = another owner, no restart; anything else, including signals, is
an **uncontrolled termination**); reconciler + sweeper every ``--reconcile-s``;
a ``media-registry`` group consumer that acknowledges published events (as
crawler2 would); RSS/VRAM sampling per process.

    . scripts/p9-env.sh
    env/bin/python benchmarks/p9-encoder/soak.py \
        --corpus ~/anti_piracy/datasets/p9/synthetic-mini --devices cuda,cpu \
        --orphan-grace-s 240 --attempt-timeout-s 180 --out benchmarks/p9-encoder/results/soak-x.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import signal
import statistics
import subprocess
import sys
import threading
import time
import uuid
from datetime import UTC, datetime
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from fingerprinter.config import Settings  # noqa: E402
from fingerprinter.errors import StorageUnavailableError  # noqa: E402
from fingerprinter.intake import Intake  # noqa: E402
from fingerprinter.publish import FAILED, INTAKE, READY  # noqa: E402
from fingerprinter.runtime import RuntimeFingerprint, RuntimeProfile  # noqa: E402
from fingerprinter.services import Services  # noqa: E402
from fingerprinter.spec import RUNTIME_PROFILE, dinov2_seg_v2  # noqa: E402
from tests.v2.conftest import settings_env, wipe  # noqa: E402
from tests.v2.helpers import request_env, xadd_request  # noqa: E402

CONTROLLED = {75: "controlled", 70: "device_error", 73: "gpu_lease_held"}


class _Quiet(SimpleHTTPRequestHandler):
    def log_message(self, *a: Any) -> None:
        pass


def _rss(pid: int) -> int:
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except OSError:
        pass
    return 0


def _vram() -> dict[int, int]:
    try:
        out = subprocess.run(  # noqa: S603
            ["nvidia-smi", "--query-compute-apps=pid,used_memory", "--format=csv,noheader,nounits"],  # noqa: S607
            capture_output=True, text=True, timeout=10, check=True,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return {}
    res = {}
    for line in out.strip().splitlines():
        pid, mem = (x.strip() for x in line.split(","))
        res[int(pid)] = int(mem) * 1024**2
    return res


class Supervisor:
    def __init__(self, settings: Settings, device: str, host: str, scratch: Path, logs: Path,
                 idle_exit_s: float) -> None:
        self.settings, self.device, self.host = settings, device, host
        self.scratch, self.logs, self.idle = scratch, logs, idle_exit_s
        self.proc: subprocess.Popen[bytes] | None = None
        self.exits: list[dict[str, Any]] = []
        self.samples: list[dict[str, Any]] = []
        self.restarts = 0
        self.stop = False

    def spawn(self) -> None:
        s = self.settings.model_copy(update={"host_id": self.host, "scratch_dir": self.scratch,
                                             "roles": ["encoder", "scheduler"]})
        log = (self.logs / f"{self.host}-{len(self.exits)}.log").open("w")
        self.proc = subprocess.Popen(  # noqa: S603
            [sys.executable, "-m", "fingerprinter", "encoder", "run", "--device", self.device,
             "--idle-exit-s", str(self.idle)],
            cwd=ROOT, env=settings_env(s), stdout=log, stderr=subprocess.STDOUT,
        )
        self.started = time.monotonic()

    def poll(self, done: bool) -> bool:
        """True while this supervisor still has a live or restartable process."""
        assert self.proc is not None
        code = self.proc.poll()
        if code is None:
            return True
        if self.exits and self.exits[-1]["pid"] == self.proc.pid:
            return False  # this exit is already recorded and handled
        report = None
        rep_file = self.scratch / "exit_reports.jsonl"
        if rep_file.exists():
            lines = rep_file.read_text().splitlines()
            if lines:
                last = json.loads(lines[-1])
                if last.get("pid") == self.proc.pid:
                    report = last
        kind = "normal" if code == 0 else CONTROLLED.get(code, "uncontrolled")
        if code < 0:
            kind = "uncontrolled"
        self.exits.append({"host": self.host, "pid": self.proc.pid, "code": code, "kind": kind,
                           "reason": report.get("reason") if report else None,
                           "reported": report is not None,
                           "uptime_s": round(time.monotonic() - self.started, 1)})
        if code == 0 and done:
            return False
        if code == 73 or self.stop:
            return False
        if code == 70:
            ok = subprocess.run([sys.executable, "-c", "import torch,sys;sys.exit(0 if torch.cuda.is_available() else 1)"],  # noqa: S603
                                check=False).returncode == 0
            if not ok:
                return False
        self.restarts += 1
        self.spawn()
        return True

    def sample(self, vram: dict[int, int]) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.samples.append({"t": time.time(), "rss": _rss(self.proc.pid),
                                 "vram": vram.get(self.proc.pid, 0)})


def _trend(values: list[int]) -> dict[str, Any]:
    if len(values) < 8:
        return {"samples": len(values), "upward_trend": None, "note": "too few samples"}
    q = len(values) // 4
    second, last = statistics.median(values[q : 2 * q]), statistics.median(values[3 * q :])
    return {"samples": len(values), "peak": max(values), "median_q2": second, "median_q4": last,
            "upward_trend": bool(second and last > 1.10 * second)}


def main() -> int:  # noqa: PLR0912, PLR0915
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", type=Path, required=True)
    ap.add_argument("--devices", default="cuda,cpu")
    ap.add_argument("--orphan-grace-s", type=float, default=900.0)
    ap.add_argument("--attempt-timeout-s", type=float, default=600.0)
    ap.add_argument("--attempt-lease-s", type=float, default=600.0)
    ap.add_argument("--reconcile-s", type=float, default=30.0)
    ap.add_argument("--max-hours", type=float, default=12.0)
    ap.add_argument("--keyspace", default="fingerprinter_soak")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    corpus = args.corpus.expanduser()
    manifest = json.loads((corpus / "manifest.json").read_text())
    tag = uuid.uuid4().hex[:8]
    base = Settings.from_env()
    enc = base.encoder.model_copy(update={"attempt_timeout_s": args.attempt_timeout_s,
                                          "attempt_lease_s": args.attempt_lease_s,
                                          "attempt_lease_renew_s": args.attempt_lease_s / 5,
                                          "poll_interval_s": 0.5})
    settings = base.model_copy(update={
        "scylla": base.scylla.model_copy(update={"keyspace": args.keyspace}),
        "redis": base.redis.model_copy(update={"prefix": f"fpsoak:{tag}:", "stream_prefix": f"fpsoak-events:{tag}:"}),
        "minio": base.minio.model_copy(update={"representations_bucket": f"fp-soak-{tag}-reps",
                                               "targets_bucket": f"fp-soak-{tag}-targets"}),
        "acquire": base.acquire.model_copy(update={"ssrf_allow_cidrs": ["127.0.0.0/8"]}),
        "intake": base.intake.model_copy(update={"reader": f"soak-{tag}"}),
        "encoder": enc,
        "orphan_grace_s": args.orphan_grace_s,
    })
    Settings.model_validate(settings.model_dump())  # grace > timeout enforced
    svc = Services.connect(settings, role="soak", migrate_schema=True)
    wipe(svc.db)
    svc.objects.ensure_buckets()
    svc.pub.seed()
    devices = [d for d in args.devices.split(",") if d]
    prints = [RuntimeFingerprint.capture(d) for d in sorted(set(devices))]
    svc.store.put_profile(RuntimeProfile.from_fingerprints(RUNTIME_PROFILE, *prints), actor="soak")
    if "cuda" in devices:
        svc.store.admit_variant(RUNTIME_PROFILE, "cuda", actor="soak", evidence="startup golden check per process")
    spec = dinov2_seg_v2()
    svc.store.add_spec(spec, status="active", actor="soak", origin_profile=RUNTIME_PROFILE)
    svc.store.approve_compat(str(spec.digest), RUNTIME_PROFILE, actor="soak", evidence="origin")
    svc.store.set_role(str(spec.digest), actor="soak")
    client = svc.coord.client
    for name in (READY, FAILED):
        client.xgroup_create(svc.coord.stream(name), "media-registry", id="0", mkstream=True)

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), partial(_Quiet, directory=str(corpus)))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{httpd.server_address[1]}"
    intake = Intake(svc.store, svc.requests)
    intake.init("from-start", actor="soak")
    assert intake.acquire()
    expected: dict[str, dict[str, Any]] = {}
    for item in manifest["items"]:
        env = request_env("sha256:" + item["sha256"], f"{url}/{item['file']}")
        xadd_request(client, svc.coord.stream(INTAKE), env)
        expected[str(env.payload.content.content_id)] = item
    while not intake.cycle().idle:
        pass

    run_dir = args.out.with_suffix("")
    logs = run_dir / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    sups = [Supervisor(settings, d, f"soak-{d}-{i}", run_dir / f"scratch-{d}-{i}", logs, idle_exit_s=60)
            for i, d in enumerate(devices)]
    t0 = time.monotonic()
    started_at = datetime.now(UTC)
    for s in sups:
        s.spawn()
    acked = 0
    last_rec = 0.0
    reconcile_runs = []
    deadline = t0 + args.max_hours * 3600

    def terminal() -> bool:
        return all(r["state"] in ("READY", "FAILED") for r in svc.requests.all())

    while time.monotonic() < deadline:
        time.sleep(2)
        vram = _vram()
        for s in sups:
            s.sample(vram)
        for name in (READY, FAILED):  # crawler2-like consumer acknowledges
            for _, entries in client.xreadgroup("media-registry", "soak", {svc.coord.stream(name): ">"}, count=500) or []:
                ids = [i for i, _ in entries]
                if ids:
                    client.xack(svc.coord.stream(name), "media-registry", *ids)
                    acked += len(ids)
        if time.monotonic() - last_rec > args.reconcile_s:
            last_rec = time.monotonic()
            try:  # as `reconcile run` does (X-10): a storage failure ends the pass, not the loop
                r = svc.reconciler.intent_pass()
            except StorageUnavailableError as exc:
                reconcile_runs.append({"t": round(last_rec - t0), "storage_error": str(exc)[:200]})
            else:
                reconcile_runs.append({"t": round(last_rec - t0), "completed": r.completed, "held": r.held,
                                       "repairs": r.repairs})
        done = terminal()
        alive = [s.poll(done) for s in sups]
        if done and not any(alive):
            break
    drained_s = time.monotonic() - t0
    for s in sups:
        s.stop = True
        if s.proc and s.proc.poll() is None:
            s.proc.send_signal(signal.SIGTERM)
            s.proc.wait(120)
            s.poll(True)
    # criterion 2: drain + 2 × lease, then everything must be terminal
    time.sleep(2 * settings.encoder.attempt_lease_s)
    svc.requests.schedule()
    rows = svc.requests.all()
    # criterion 7: reconcile, then sweep with the soak's recorded (shortened) grace
    for _ in range(3):
        svc.reconciler.intent_pass(grace_s=0)
    wait_grace = max(0.0, args.orphan_grace_s - (time.monotonic() - t0 - drained_s))
    time.sleep(min(wait_grace, args.orphan_grace_s))
    sweep = svc.reconciler.sweep()
    open_intents = list(svc.store.intents())
    reps = svc.db.rows("SELECT content_id, spec_digest, artifact_attempt_id FROM {ks}.representations")
    objects = list(svc.objects.list(settings.minio.representations_bucket, "rep/"))
    referenced = {f"rep/{spec.hex}/{r['content_id']}/{r['artifact_attempt_id']}.npz" for r in reps}
    unreferenced = [o.key for o in objects if o.key not in referenced]

    exits = [e for s in sups for e in s.exits]
    states: dict[str, int] = {}
    wrong_expect = []
    oom_single = []
    for r in rows:
        states[r["state"]] = states.get(r["state"], 0) + 1
        item = expected.get(r["content_id"], {})
        exp = item.get("expect", "READY")
        if exp == "READY" and r["state"] != "READY":
            wrong_expect.append({"file": item.get("file"), "state": r["state"], "failure": r["last_failure"], "detail": r["last_detail"]})
        if exp.startswith("FAILED:"):
            classes = exp.split(":", 1)[1].split("|")
            if r["state"] != "FAILED" or r["last_failure"] not in classes:
                wrong_expect.append({"file": item.get("file"), "state": r["state"], "failure": r["last_failure"], "expected": classes})
        if r["last_detail"] == "gpu_oom_single":
            oom_single.append({"state": r["state"], "retryable": r["last_retryable"]})
    leases_left = [k.decode() for k in client.scan_iter(match=f"fpsoak:{tag}:gpu:*")]
    rss = {s.host: _trend([x["rss"] for x in s.samples]) for s in sups}
    vram_t = {s.host: _trend([x["vram"] for x in s.samples if x["vram"]]) for s in sups if s.device == "cuda"}
    limit = settings.encoder.max_rss_bytes
    criteria = {
        "1_zero_uncontrolled_terminations": not [e for e in exits if e["kind"] == "uncontrolled"],
        "2_zero_lost_requests": states.get("PENDING", 0) == 0 and states.get("IN_PROGRESS", 0) == 0
        and len(rows) == len(expected),
        "3_controlled_exits_reported_and_recovered": all(e["reported"] for e in exits if e["kind"] in ("controlled", "device_error")),
        "4_single_item_oom_items_retryable": all(o["state"] == "READY" or o["retryable"] for o in oom_single),
        "5_leases_released_or_expired": not leases_left and not [r for r in rows if r["state"] == "IN_PROGRESS"],
        "6_rss_vram_bounded": all(t.get("upward_trend") is not True and t.get("peak", 0) < limit for t in rss.values())
        and all(t.get("upward_trend") is not True for t in vram_t.values()),
        "7_zero_orphans_zero_open_intents": not unreferenced and not open_intents,
        "8_expected_failures_no_crash": not wrong_expect,
    }
    gate_eligible = manifest.get("corpus") == "SOAK-1000" and len(manifest["items"]) >= 1000
    import torch  # noqa: PLC0415

    report = {
        "benchmark": "B-SOAK",
        "gate": "G-8 soak (design §13.3)",
        "gate_eligible": gate_eligible,
        "gate_status": ("PASS" if all(criteria.values()) else "FAIL") if gate_eligible else "NOT MEASURED (corpus is not SOAK-1000)",
        "harness_result": "PASS" if all(criteria.values()) else "FAIL",
        "criteria": criteria,
        "started_at": started_at.isoformat(),
        "duration_s": round(time.monotonic() - t0, 1),
        "drain_s": round(drained_s, 1),
        "corpus": {"name": manifest.get("corpus"), "items": len(manifest["items"]), "synthetic": manifest.get("synthetic", False),
                   "manifest_sha256": hashlib.sha256((corpus / "manifest.json").read_bytes()).hexdigest()},
        "config": {"devices": devices, "orphan_grace_s": args.orphan_grace_s, "attempt_timeout_s": args.attempt_timeout_s,
                   "attempt_lease_s": settings.encoder.attempt_lease_s, "max_rss_bytes": limit,
                   "note": "orphan_grace_s and attempt_timeout_s shortened for observability (design §13.3 #7)"},
        "environment": {"host": platform.node(), "cpu": platform.processor() or platform.machine(), "cores": os.cpu_count(),
                        "torch": torch.__version__, "cuda": torch.version.cuda,
                        "gpu": torch.cuda.get_device_name(0) if torch.cuda.is_available() else None,
                        "spec": str(spec.digest), "runtime_profile": RUNTIME_PROFILE},
        "requests": states,
        "unexpected_outcomes": wrong_expect,
        "exits": exits,
        "restarts": {s.host: s.restarts for s in sups},
        "oom_single": oom_single,
        "leases_left": leases_left,
        "rss": rss,
        "vram": vram_t,
        "sweep": sweep,
        "open_intents": len(open_intents),
        "unreferenced_objects": unreferenced,
        "events_acked": acked,
        "reconcile_runs": reconcile_runs[-20:],
        "reconcile_storage_errors": sum("storage_error" in r for r in reconcile_runs),
        "stats": {"representations": len(reps), "objects": len(objects)},
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=1, default=str) + "\n")
    print(json.dumps({k: report[k] for k in ("gate_status", "harness_result", "criteria", "requests", "duration_s")}, indent=1))
    httpd.shutdown()
    for name in ("q", "lease"):
        for key in client.scan_iter(match=f"fpsoak:{tag}:{name}*"):
            client.delete(key)
    return 0 if all(criteria.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
