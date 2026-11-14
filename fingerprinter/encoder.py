"""The encoder process (design §12, P9-D16/D25).

::

    claim/worker threads (D) → acquire (typed source) → probe → decode (real PTS)
        → preprocess/v1 → bounded queue (Q items; producers block when full)
    inference thread (1, owns the device) → batches ≤ B across attempts → CLS → L2
    worker → artifact → commit protocol (commit.py)
    supervisor thread → durable attempt-lease renewal, Redis work/GPU lease
        renewal, attempt wall-clock limit, RSS watchdog

Failure classes and exit codes (§12.4)::

    1 batch OOM (B > 1)      halve B, retry the same in-memory items     (no exit)
    2 single-item OOM (B=1)  item's attempt → attributed internal_error/gpu_oom_single;
                             release the rest; release GPU lease; exit 75 (controlled)
    3 process crash          leases expire → requeue (uncontrolled; nothing in-process)
    4 device error           release all (device_error); exit 70; no CPU switch
    5 RSS over limit         release all; exit 75 (controlled)
    GPU lease lost           release all; exit 75 (controlled)
    GPU lease held at start  exit 73, before the model is loaded
    CUDA/model unavailable   exit 69 (typed); runtime/golden refusal exit 78

A controlled exit is always reported (log line ``controlled_exit`` with the
reason, metric ``encoder_controlled_exits_total``, ``exit_report.json`` in the
scratch dir) so the soak gate can tell it from an uncontrolled termination.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import shutil
import signal
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

import numpy as np
from antipiracy_contracts.events.fingerprinting import EncodeFailure as F

from fingerprinter import artifact, calibration, metrics
from fingerprinter.commit import Artifact
from fingerprinter.coord import uuid7
from fingerprinter.errors import (
    DeviceError,
    EncodeError,
    FingerprinterError,
    LeaseLostError,
    SingleItemOOMError,
    StorageUnavailableError,
)
from fingerprinter.media import decode, preprocess, probe
from fingerprinter.model import (
    CudaUnavailableError,
    Embedder,
    ModelUnavailableError,
    OutOfMemory,
    golden_inputs,
)
from fingerprinter.runtime import RuntimeFingerprint
from fingerprinter.services import Services
from fingerprinter.sources import Acquirer
from fingerprinter.spec import Spec
from fingerprinter.store import parse_member

log = logging.getLogger("fingerprinter.encoder")

EXIT_OK = 0
EXIT_UNAVAILABLE = 69
EXIT_DEVICE = 70
EXIT_GPU_LEASE_HELD = 73
EXIT_CONTROLLED = 75
EXIT_CONFIG = 78

GOLDEN_DIR = Path(__file__).parent / "golden"
GOLDEN_CPU = GOLDEN_DIR / "cpu_fp32.npy"
GOLDEN_MIN_COS = {"cpu": 0.9999, "cuda": 0.999}


class ControlledExit(Exception):
    def __init__(self, code: int, reason: str) -> None:
        super().__init__(reason)
        self.code = code
        self.reason = reason


@dataclass(eq=False)
class Attempt:
    row: dict[str, Any]
    spec: Spec
    started: float = field(default_factory=time.monotonic)
    cancel: threading.Event = field(default_factory=threading.Event)
    done: threading.Event = field(default_factory=threading.Event)
    vectors: dict[int, np.ndarray] = field(default_factory=dict)
    expected: int | None = None
    superseded: bool = False
    timed_out: bool = False
    released: bool = False
    release_lock: threading.Lock = field(default_factory=threading.Lock)
    error: BaseException | None = None
    redis_lease: str = ""
    lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def attempt_id(self) -> str:
        return str(self.row["attempt_id"])

    def add(self, k: int, v: np.ndarray) -> None:
        with self.lock:
            self.vectors[k] = v
            if self.expected is not None and len(self.vectors) >= self.expected:
                self.done.set()

    def finish_decode(self, count: int) -> None:
        with self.lock:
            self.expected = count
            if len(self.vectors) >= count:
                self.done.set()


@dataclass
class Item:
    attempt: Attempt
    k: int
    tensor: np.ndarray


class Encoder:
    def __init__(
        self,
        services: Services,
        *,
        device: str | None = None,
        golden_check: bool = True,
        calibrate: bool = True,
        embedders: dict[str, Embedder] | None = None,
        fingerprint: RuntimeFingerprint | None = None,
    ) -> None:
        self.s = services
        self.settings = services.settings
        self.cfg = services.settings.encoder
        self.device_req = device or self.cfg.device
        self.device = ""
        self.golden_check = golden_check
        self.do_calibrate = calibrate
        self.embedders: dict[str, Embedder] = embedders if embedders is not None else {}
        self._given_fingerprint = fingerprint
        self.allowed: dict[str, Spec] = {}
        self.refused: dict[str, str] = {}
        self.batch = 1
        self.queue: queue.Queue[Item] = queue.Queue()
        self.inflight: set[Attempt] = set()
        self.inflight_lock = threading.Lock()
        self.stopping = threading.Event()
        self.draining = threading.Event()
        self.exit: ControlledExit | None = None
        self.gpu_token = uuid7()
        self.gpu_key = services.coord.key("gpu", self.settings.host_id)
        self.scratch = Path(self.settings.scratch_dir)
        self.acquirer = Acquirer(self.settings, services.objects)
        self.fault_hooks: dict[str, Any] = {}
        """Test hooks: ``before_commit(attempt)``; embedder ``fault(batch_size)``."""
        self.completed = 0
        self.stats: dict[str, int] = {"won": 0, "lost": 0, "reused": 0, "failed": 0, "released": 0}
        self.started_at = time.monotonic()

    # --- startup ----------------------------------------------------------------

    def start(self) -> int:
        try:
            self._start()
        except ControlledExit as exc:
            self._report(exc.code, exc.reason)
            return exc.code
        return EXIT_OK

    def _resolve_device(self) -> str:
        import torch

        if self.device_req == "auto":
            return "cuda" if torch.cuda.is_available() else "cpu"
        return self.device_req

    def _start(self) -> None:
        self.device = self._resolve_device()
        if self.device == "cuda":
            import torch

            if not torch.cuda.is_available():
                raise ControlledExit(EXIT_UNAVAILABLE, "cuda_unavailable")
        self._clean_scratch()
        fp = self._given_fingerprint or RuntimeFingerprint.capture(
            self.device, self.settings.model_cache_dir
        )
        self.fingerprint = fp
        for row in self.s.store.specs():
            digest = row["spec_digest"]
            if row["status"] != "active":
                self.refused[digest] = f"spec status {row['status']}"
                continue
            spec = self.s.store.spec(digest)
            if spec.compute_dtype == "float16" and self.device != "cuda":
                self.refused[digest] = "float16 spec on a CPU encoder"
                continue
            reason = self.s.store.runtime_check(digest, fp)
            if reason:
                self.refused[digest] = f"runtime_incompatible: {reason}"
                metrics.RUNTIME_INCOMPATIBLE.inc()
                log.error("runtime_incompatible spec=%s reason=%s", digest, reason)
                continue
            self.allowed[digest] = spec
        if not self.allowed:
            raise ControlledExit(EXIT_CONFIG, "runtime_incompatible: no spec may be produced here")
        if self.device == "cuda":
            if not self.s.coord.acquire(self.gpu_key, self.gpu_token, self.cfg.gpu_lease_ms):
                raise ControlledExit(EXIT_GPU_LEASE_HELD, "gpu_lease_held")
        # Model load, golden check and calibration run before the supervisor exists;
        # keep the GPU lease alive across them (calibration alone takes minutes).
        keeper = _StartupLeaseKeeper(self) if self.device == "cuda" else None
        try:
            try:
                for spec in self.allowed.values():
                    if spec.compute_dtype not in self.embedders:
                        self.embedders[spec.compute_dtype] = Embedder(
                            self.device,
                            spec.compute_dtype,
                            threads=self.cfg.inference_threads,
                            cache_dir=self.settings.model_cache_dir,
                            local_files_only=self.cfg.local_files_only,
                        )
            except (CudaUnavailableError, ModelUnavailableError) as exc:
                self._release_gpu()
                raise ControlledExit(EXIT_UNAVAILABLE, f"{type(exc).__name__}: {exc}") from exc
            if self.golden_check and "float32" in self.embedders:
                self._golden()
            self.batch = self._calibrated_batch() if self.do_calibrate else (
                self.cfg.cpu_batch if self.device == "cpu" else 1
            )
        finally:
            if keeper is not None:
                keeper.stop()
        if keeper is not None and keeper.lost:
            raise ControlledExit(EXIT_CONTROLLED, "gpu_lease_lost during startup")
        metrics.BATCH.set(self.batch)

    def _golden(self) -> None:
        if not GOLDEN_CPU.exists():
            raise ControlledExit(EXIT_CONFIG, "golden vectors missing")
        golden = np.load(GOLDEN_CPU)
        vec = self.embedders["float32"].embed(golden_inputs(len(golden)))
        cos = float((vec * golden).sum(axis=1).min())
        if cos < GOLDEN_MIN_COS[self.device]:
            metrics.GOLDEN.labels(result="fail").inc()
            self._release_gpu()
            raise ControlledExit(EXIT_CONFIG, f"golden_check_failed min_cos={cos:.6f}")
        metrics.GOLDEN.labels(result="pass").inc()
        self.golden_min_cos = cos

    def _calibrated_batch(self) -> int:
        """Design §12.3 / OF-2: the stored ``throughput/v1`` result for this (host,
        device, spec, dtype), else a fresh calibration. Rows of the superseded
        ``B_max/2`` rule (no ``calibration`` report) are recalibrated."""
        if self.device == "cpu":
            return self.cfg.cpu_batch
        store = self.s.store
        spec = next(iter(self.allowed.values()))
        row = store.db.one(
            "SELECT * FROM {ks}.encoder_hosts WHERE host_id = ? AND device = ? AND spec_digest = ? "
            "AND compute_dtype = ?",
            self.settings.host_id, self.device, str(spec.digest), spec.compute_dtype,
        )
        if row is not None and row["b_start"] and _method(row.get("calibration")) == calibration.METHOD:
            return int(row["b_stable"] or row["b_start"])
        report = self.calibrate(spec)
        b_start = int(report["selected_batch"])
        store.db.run(
            "INSERT INTO {ks}.encoder_hosts (host_id, device, spec_digest, compute_dtype, b_max, "
            "b_start, b_stable, oom_recovered, oom_single, calibrated_at, calibration) "
            "VALUES (?,?,?,?,?,?,null,0,0,?,?)",
            self.settings.host_id, self.device, str(spec.digest), spec.compute_dtype,
            int(report["b_max_fitting"]), b_start, store.now(), json.dumps(report, sort_keys=True),
        )
        metrics.BATCH_MAX.set(report["b_max_fitting"])
        log.info("calibrated batch=%d frames_per_s=%.2f b_max=%d reason=%s", b_start,
                 report["selected_frames_per_s"], report["b_max_fitting"], report["selection_reason"])
        return b_start

    def calibrate(self, spec: Spec) -> dict[str, Any]:
        """Run ``calibration.calibrate`` on this device; failures leave through the
        typed controlled exits (75: nothing fits, 70: device error), GPU lease released."""
        emb = self.embedders[spec.compute_dtype]
        try:
            report = calibration.calibrate(
                emb, golden_inputs(8), max_batch=self.cfg.max_batch,
                frames=self.cfg.calibration_frames, reps=self.cfg.calibration_reps,
                tolerance=self.cfg.calibration_tolerance, sync=emb.sync, reset_peak=emb.reset_peak,
            )
        except calibration.CalibrationError as exc:
            self._release_gpu()
            raise ControlledExit(EXIT_CONTROLLED, f"calibration: {exc}") from exc
        except DeviceError as exc:
            self._release_gpu()
            raise ControlledExit(EXIT_DEVICE, f"calibration: device_error {exc}") from exc
        fp = getattr(self, "fingerprint", None)
        report["profile"] = {
            "host_id": self.settings.host_id,
            "device": self.device,
            "spec_digest": str(spec.digest),
            "compute_dtype": spec.compute_dtype,
            "runtime_profile": spec.document.runtime_profile,
            "fingerprint": json.loads(fp.to_json()) if fp is not None else None,
            "gpu": _gpu_info(),
        }
        return report

    def _clean_scratch(self) -> None:
        self.scratch.mkdir(parents=True, exist_ok=True)
        for entry in self.scratch.iterdir():
            owner = entry / "owner"
            try:
                pid = int(owner.read_text()) if owner.exists() else None
            except ValueError:
                pid = None
            if pid is not None and pid != os.getpid() and _pid_alive(pid):
                continue
            if entry.is_dir():
                shutil.rmtree(entry, ignore_errors=True)

    # --- run --------------------------------------------------------------------

    def run(self, *, max_attempts: int | None = None, idle_exit_s: float | None = None,
            install_signals: bool = False) -> int:
        code = self.start()
        if code != EXIT_OK:
            return code
        if install_signals:
            signal.signal(signal.SIGTERM, lambda *_: self.draining.set())
            signal.signal(signal.SIGINT, lambda *_: self.draining.set())
        self.max_attempts = max_attempts
        self.idle_exit_s = idle_exit_s
        self.last_work = time.monotonic()
        threads = [threading.Thread(target=self._inference_loop, name="inference", daemon=True),
                   threading.Thread(target=self._supervisor, name="supervisor", daemon=True)]
        workers = [threading.Thread(target=self._worker, name=f"worker-{i}", daemon=True)
                   for i in range(max(1, self.cfg.decode_threads))]
        for t in threads + workers:
            t.start()
        while any(t.is_alive() for t in workers) and not self.stopping.is_set():
            time.sleep(0.2)
            if self.draining.is_set():
                self._drain()
                break
        if self.exit is not None:
            self._shutdown_controlled(self.exit)
            for t in workers:  # let cancelled workers finish their own releases
                t.join(timeout=self.cfg.shutdown_grace_s)
            return self.exit.code
        self.stopping.set()
        for t in workers:
            t.join(timeout=self.cfg.shutdown_grace_s)
        self._release_gpu()
        self._report(EXIT_OK, "normal")
        return EXIT_OK

    def _drain(self) -> None:
        """SIGTERM: stop claiming, let in-flight attempts finish up to the grace, release the rest."""
        deadline = time.monotonic() + self.cfg.shutdown_grace_s
        while time.monotonic() < deadline:
            with self.inflight_lock:
                if not self.inflight:
                    break
            time.sleep(0.2)
        self.stopping.set()
        self._release_all("shutdown")

    def _shutdown_controlled(self, exit_: ControlledExit) -> None:
        self.stopping.set()
        self._release_all(exit_.reason)
        self._release_gpu()
        metrics.CONTROLLED_EXITS.labels(reason=exit_.reason).inc()
        log.error("controlled_exit code=%s reason=%s", exit_.code, exit_.reason)
        self._report(exit_.code, exit_.reason)

    def _controlled(self, code: int, reason: str) -> None:
        if self.exit is None:
            self.exit = ControlledExit(code, reason)
        self.stopping.set()
        for a in list(self.inflight):
            a.cancel.set()

    def _release_all(self, reason: str) -> None:
        with self.inflight_lock:
            attempts = list(self.inflight)
        for a in attempts:
            a.cancel.set()
            self._release(a, reason)

    def _release(self, a: Attempt, reason: str) -> None:
        """IN_PROGRESS → PENDING without an attributed failure (fenced; idempotent)."""
        # Held across the whole release so a second caller (main thread vs worker)
        # waits until the transition has landed instead of returning early.
        with a.release_lock:
            if a.released or a.done.is_set():
                return
            a.released = True
            try:
                if self.s.requests.release(a.row, reason=reason):
                    self.stats["released"] += 1
            except StorageUnavailableError:
                pass  # left to lease expiry
            try:
                self.s.coord.release(self.s.coord.work_lease_key(_member(a.row)), a.redis_lease)
            except StorageUnavailableError:
                pass

    def _release_gpu(self) -> None:
        if self.device == "cuda":
            try:
                self.s.coord.release(self.gpu_key, self.gpu_token)
            except StorageUnavailableError:
                pass

    def _report(self, code: int, reason: str) -> None:
        report = {
            "host_id": self.settings.host_id,
            "pid": os.getpid(),
            "device": self.device,
            "exit_code": code,
            "reason": reason,
            "controlled": code != EXIT_OK,
            "stats": self.stats,
            "batch": self.batch,
            "refused_specs": self.refused,
            "uptime_s": round(time.monotonic() - self.started_at, 1),
        }
        try:
            self.scratch.mkdir(parents=True, exist_ok=True)
            with (self.scratch / "exit_reports.jsonl").open("a") as fh:
                fh.write(json.dumps(report, sort_keys=True) + "\n")
        except OSError:
            pass
        self.last_report = report

    # --- claim + attempt (worker threads) ---------------------------------------

    def _worker(self) -> None:
        while not self.stopping.is_set() and not self.draining.is_set():
            if self.max_attempts is not None and self.completed >= self.max_attempts:
                return
            try:
                attempt = self._claim()
            except StorageUnavailableError:
                time.sleep(self.cfg.poll_interval_s)
                continue
            except Exception:  # noqa: BLE001 - a dead worker would leave the process running degraded or exit 0
                log.error("claim_crashed %s", traceback.format_exc())
                self._controlled(EXIT_CONTROLLED, "claim_crashed")
                return
            if attempt is None:
                if self.idle_exit_s is not None and time.monotonic() - self.last_work > self.idle_exit_s:
                    with self.inflight_lock:
                        if not self.inflight:
                            return
                time.sleep(self.cfg.poll_interval_s)
                continue
            self.last_work = time.monotonic()
            try:
                self._process(attempt)
            except ControlledExit as exc:
                self._controlled(exc.code, exc.reason)
            except Exception:  # noqa: BLE001 - a bug must not kill the process silently
                log.error("attempt_crashed %s", traceback.format_exc())
                self._attempt_fail(attempt, EncodeError(F.INTERNAL_ERROR, "unexpected", retryable=True))
            finally:
                with self.inflight_lock:
                    self.inflight.discard(attempt)
                self._log_attempt(attempt)
                self.completed += 1
                self.last_work = time.monotonic()
                shutil.rmtree(self.scratch / attempt.attempt_id, ignore_errors=True)

    def _claim(self) -> Attempt | None:
        coord = self.s.coord
        attempt_id = uuid7()
        popped = coord.pop_and_lease(attempt_id, self.cfg.lease_ms)
        if popped is None and "scheduler" in self.settings.roles:
            if self.s.requests.schedule()["enqueued"]:
                self.last_work = time.monotonic()  # fresh work is not idleness
                popped = coord.pop_and_lease(attempt_id, self.cfg.lease_ms)
        if popped is None:
            return None
        member, mine = popped
        if not mine:
            return None  # someone else holds the work lease (avoidance only)
        cid, key = parse_member(member)
        row = self.s.requests.get(cid, key, serial=False)
        if row is None or row["state"] != "PENDING":
            coord.release(coord.work_lease_key(member), attempt_id)
            return None
        spec = self.allowed.get(row["resolved_spec"])
        if spec is None:
            # Not ours to produce (gated, runtime, device): give it back for another encoder.
            coord.release(coord.work_lease_key(member), attempt_id)
            coord.enqueue(member, time.time() * 1000 + 60_000 + 1e15)
            return None
        claimed = self.s.requests.claim(cid, key, attempt_id)
        if claimed is None:
            coord.release(coord.work_lease_key(member), attempt_id)
            return None
        attempt = Attempt(claimed, spec, redis_lease=attempt_id)
        with self.inflight_lock:
            stopping = self.stopping.is_set() or self.draining.is_set()
            if not stopping:
                self.inflight.add(attempt)
        if stopping:
            # A claim that landed after a controlled exit/drain began: the release
            # sweep may already have run, so give it back here (no failure counted).
            self._release(attempt, self.exit.reason if self.exit else "shutdown")
            return None
        return attempt

    def _process(self, a: Attempt) -> None:
        req, spec = self.s.requests, a.spec
        rep = self.s.store.representation(a.row["content_id"], str(spec.digest), serial=True)
        if rep is not None:  # reuse: no encode
            req.mark_ready(a.row, rep["representation_id"])
            req._republish_ready(rep, cause=a.row.get("first_event_id"))  # noqa: SLF001
            self.stats["reused"] += 1
            metrics.ENCODES.labels(outcome="reused").inc()
            return
        work = self.scratch / a.attempt_id
        work.mkdir(parents=True, exist_ok=True)
        (work / "owner").write_text(str(os.getpid()))
        t0 = time.monotonic()
        acquired, bad, err, retry_err = None, {}, None, None
        digest_hex = str(a.row["content_digest"]).removeprefix("sha256:")
        for key, doc_text in _ordered_sources(a.row):
            if a.cancel.is_set():
                break
            try:
                acquired = self.acquirer.acquire(key, json.loads(doc_text), digest_hex, work / "media")
                break
            except EncodeError as exc:
                err = exc
                if exc.retryable:
                    retry_err = exc
                if exc.scope == "source" and not exc.retryable:
                    bad[key] = f"{exc.failure.value}:{exc.detail}"
                if exc.scope == "content":
                    break
        if a.cancel.is_set():
            return self._cancelled(a)
        if acquired is None:
            assert err is not None
            if err.scope != "content" and retry_err is not None:
                err = retry_err  # a source may still answer later: retry with backoff
            return self._attempt_fail(a, err, bad)
        metrics.STAGE.labels(stage="acquire").observe(time.monotonic() - t0)
        try:
            pr = probe(acquired.path)
            t1 = time.monotonic()
            count, metas = 0, []
            for frame in decode(acquired.path, spec, cancel=a.cancel):
                if a.cancel.is_set():
                    break
                metas.append((frame.segment_index, frame.pts, frame.pts_s))
                tb = frame.time_base
                self._put(Item(a, frame.segment_index, preprocess(frame.rgb)), a)
                count += 1
            if a.cancel.is_set():
                return self._cancelled(a)
            a.finish_decode(count)
            metrics.STAGE.labels(stage="decode").observe(time.monotonic() - t1)
            while not a.done.wait(0.2):
                if a.cancel.is_set() or self.stopping.is_set():
                    return self._cancelled(a)
            if isinstance(a.error, EncodeError):
                raise a.error
            if a.error is not None:
                return None  # e.g. single-item OOM: already attributed by the inference thread
        except EncodeError as exc:
            if a.cancel.is_set() or self.stopping.is_set() or self.draining.is_set():
                # stop/timeout/supersede decide, not the decoder: a service stop also
                # signals ffmpeg, whose failure then says nothing about the media
                return self._cancelled(a)
            return self._attempt_fail(a, exc, bad)
        if a.cancel.is_set() or a.superseded:
            return self._cancelled(a)
        ks = np.array([m[0] for m in metas])
        seg = artifact.Segments(
            segment_index=ks,
            pts=np.array([m[1] for m in metas], dtype=np.int64),
            time_base=tb,
            pts_s=np.array([m[2] for m in metas], dtype=np.float64),
            vectors=np.stack([a.vectors[k] for k in ks]),
            segment_duration_s=spec.document.sampling.segment_duration_s,
        )
        manifest = {
            "representation_id": str(spec.representation_id(a.row["content_id"])),
            "content_id": a.row["content_id"],
            "spec_digest": str(spec.digest),
            "runtime_fingerprint": json.loads(self.fingerprint.to_json()),
            "device": self.device,
            "compute_dtype": spec.compute_dtype,
            "batch_size": self.batch,
            "worker": self.s.store.instance,
            "attempt_id": a.attempt_id,
            "stream": pr.to_json(),
            "source": {"key": acquired.source_key, "redirects": acquired.redirects},
        }
        data, sha = artifact.build(seg, manifest)
        hook = self.fault_hooks.get("before_commit")
        if hook:
            hook(a)
        if a.superseded or time.monotonic() - a.started > self.cfg.attempt_timeout_s:
            return self._cancelled(a)
        t2 = time.monotonic()
        result = self.s.committer.commit(
            a.row,
            spec,
            Artifact(data, sha, len(ks), pr.duration_s or float(seg.pts_s[-1]),
                     self.fingerprint.to_json(), self.device, spec.compute_dtype),
        )
        metrics.STAGE.labels(stage="commit").observe(time.monotonic() - t2)
        metrics.ENCODES.labels(outcome=result).inc()
        self.stats[result] += 1
        a.done.set()

    def _log_attempt(self, a: Attempt) -> None:
        """Observability history (``encode_attempt_log``, 90-day TTL; never read for
        scheduling or recovery)."""
        if a.released:
            outcome = "released"
        elif a.superseded:
            outcome = "superseded"
        elif a.timed_out:
            outcome = "timeout"
        elif a.error is not None:
            outcome = f"error:{type(a.error).__name__}"
        else:
            outcome = "done"
        try:
            self.s.store.db.run(
                "INSERT INTO {ks}.encode_attempt_log (content_id, spec_key, attempt_id, host_id, "
                "device, outcome, detail, ended_at) VALUES (?,?,?,?,?,?,?,?)",
                a.row["content_id"], a.row["spec_key"], a.attempt_id, self.settings.host_id,
                self.device, outcome, f"{time.monotonic() - a.started:.1f}s", self.s.store.now(),
            )
        except StorageUnavailableError:
            pass

    def _put(self, item: Item, a: Attempt) -> None:
        cap = self.cfg.queue_items or 4 * max(self.batch, 1)
        while True:
            if a.cancel.is_set():
                return
            if self.queue.qsize() < cap:
                self.queue.put(item)
                metrics.QUEUE_DEPTH.set(self.queue.qsize())
                return
            time.sleep(0.01)

    def _cancelled(self, a: Attempt) -> None:
        if a.timed_out:
            self._attempt_fail(a, EncodeError(F.TIMEOUT, "attempt_timeout", retryable=True))
        elif a.superseded:
            log.warning("attempt_superseded %s", a.attempt_id)
        elif self.stopping.is_set() or self.draining.is_set():
            # Controlled exit or drain: release here, before this worker drops the
            # attempt from the in-flight set (the main thread may not see it any more).
            self._release(a, self.exit.reason if self.exit else "shutdown")

    def _attempt_fail(self, a: Attempt, err: EncodeError, bad: dict[str, str] | None = None) -> None:
        if a.superseded:
            return
        try:
            state = self.s.requests.fail(a.row, err, None, bad=bad or {})
        except StorageUnavailableError:
            return  # lease expiry requeues it
        a.done.set()
        if state == "FAILED":
            self.stats["failed"] += 1
        metrics.ENCODES.labels(outcome=f"failed_{err.failure.value}").inc()

    # --- inference thread -------------------------------------------------------

    def _inference_loop(self) -> None:
        stable = 0
        while not self.stopping.is_set():
            try:
                first = self.queue.get(timeout=0.2)
            except queue.Empty:
                continue
            items = [first]
            dtype = first.attempt.spec.compute_dtype
            while len(items) < self.batch:
                try:
                    nxt = self.queue.get_nowait()
                except queue.Empty:
                    break
                if nxt.attempt.spec.compute_dtype != dtype:
                    self.queue.put(nxt)
                    break
                items.append(nxt)
            metrics.QUEUE_DEPTH.set(self.queue.qsize())
            items = [i for i in items if not i.attempt.cancel.is_set()]
            emb = self.embedders[dtype]
            t0 = time.monotonic()
            while items:
                batch = items[: self.batch]
                try:
                    vecs = emb.embed(np.stack([i.tensor for i in batch]))
                except OutOfMemory:
                    emb.free()
                    if len(batch) > 1:
                        self.batch = max(1, len(batch) // 2)
                        metrics.OOM_RECOVERED.inc()
                        metrics.BATCH.set(self.batch)
                        stable = 0
                        continue  # same items, smaller batch: no loss, no duplicate
                    metrics.OOM_SINGLE.inc()
                    self._single_item_oom(batch[0].attempt)
                    return
                except DeviceError as exc:
                    log.error("device_error %s", exc)
                    self._controlled(EXIT_DEVICE, "device_error")
                    return
                except Exception as exc:  # noqa: BLE001
                    for i in batch:
                        i.attempt.error = EncodeError(F.INTERNAL_ERROR, f"inference: {type(exc).__name__}", retryable=True)
                        i.attempt.done.set()
                    items = items[len(batch):]
                    continue
                for i, v in zip(batch, vecs, strict=True):
                    i.attempt.add(i.k, v)
                metrics.FRAMES.labels(device=self.device).inc(len(batch))
                items = items[len(batch):]
                stable += 1
            metrics.STAGE.labels(stage="infer").observe(time.monotonic() - t0)
            if self.device == "cuda":
                for kind, value in self.embedders[dtype].vram().items():
                    metrics.VRAM.labels(kind=kind).set(value)

    def _single_item_oom(self, a: Attempt) -> None:
        """Class 2: attribute the failure to this item's request, then exit 75."""
        a.error = SingleItemOOMError("gpu_oom_single")
        try:
            self.s.requests.fail(a.row, EncodeError(F.INTERNAL_ERROR, "gpu_oom_single", retryable=True), None)
        except StorageUnavailableError:
            pass
        a.done.set()
        with self.inflight_lock:
            self.inflight.discard(a)
        self._controlled(EXIT_CONTROLLED, "gpu_oom_single")

    # --- supervisor ---------------------------------------------------------------

    def _supervisor(self) -> None:
        last_durable = last_redis = last_gpu = time.monotonic()
        while not self.stopping.is_set():
            time.sleep(0.2)
            now = time.monotonic()
            with self.inflight_lock:
                attempts = list(self.inflight)
            for a in attempts:
                if now - a.started > self.cfg.attempt_timeout_s and not a.timed_out:
                    a.timed_out = True
                    a.cancel.set()
            if now - last_durable >= self.cfg.attempt_lease_renew_s:
                last_durable = now
                for a in attempts:
                    if a.done.is_set():
                        continue
                    try:
                        if not self.s.requests.renew(a.row):
                            a.superseded = True
                            a.cancel.set()
                    except StorageUnavailableError:
                        pass
            if now - last_redis >= self.cfg.lease_ms / 3000:
                last_redis = now
                for a in attempts:
                    try:
                        self.s.coord.renew(self.s.coord.work_lease_key(_member(a.row)), a.redis_lease, self.cfg.lease_ms)
                    except StorageUnavailableError:
                        pass
            if self.device == "cuda" and now - last_gpu >= self.cfg.gpu_lease_renew_ms / 1000:
                last_gpu = now
                try:
                    ok = self.s.coord.renew(self.gpu_key, self.gpu_token, self.cfg.gpu_lease_ms)
                except StorageUnavailableError:
                    ok = False
                if not ok:
                    self._controlled(EXIT_CONTROLLED, "gpu_lease_lost")
                    return
            rss = _rss()
            metrics.RSS.set(rss)
            if rss > self.cfg.max_rss_bytes:
                self._controlled(EXIT_CONTROLLED, "rss_limit")
                return


def _member(row: dict[str, Any]) -> str:
    return f"{row['content_id']}|{row['spec_key']}"


def _ordered_sources(row: dict[str, Any]) -> list[tuple[str, str]]:
    bad = set((row.get("bad_sources") or {}).keys())
    tried = set(row.get("tried_sources") or ())
    items = [(k, v) for k, v in (row.get("sources") or {}).items() if k not in bad]
    # internal (verified reference media) first, then untried external sources first.
    return sorted(items, key=lambda kv: (not kv[0].startswith("obj:"), kv[0] in tried))


def _rss() -> int:
    try:
        with open("/proc/self/statm") as fh:
            return int(fh.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    except OSError:
        return 0


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


__all__ = [
    "EXIT_CONFIG", "EXIT_CONTROLLED", "EXIT_DEVICE", "EXIT_GPU_LEASE_HELD", "EXIT_OK",
    "EXIT_UNAVAILABLE", "ControlledExit", "Encoder", "FingerprinterError", "LeaseLostError",
    "timedelta",
]


def _method(report: str | None) -> str | None:
    try:
        return json.loads(report).get("method") if report else None
    except ValueError:
        return None


def _gpu_info() -> dict[str, Any] | None:
    import torch

    if not torch.cuda.is_available():
        return None
    p = torch.cuda.get_device_properties(0)
    return {"name": p.name, "total_memory": int(p.total_memory),
            "capability": f"{p.major}.{p.minor}", "torch": torch.__version__,
            "cuda": torch.version.cuda}


class _StartupLeaseKeeper:
    """Renews the GPU lease from acquisition until the supervisor takes over."""

    def __init__(self, enc: Encoder) -> None:
        self.enc = enc
        self.lost = False
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="gpu-lease-startup", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        cfg = self.enc.cfg
        while not self._stop.wait(cfg.gpu_lease_renew_ms / 1000):
            try:
                if not self.enc.s.coord.renew(self.enc.gpu_key, self.enc.gpu_token, cfg.gpu_lease_ms):
                    self.lost = True
                    log.error("gpu_lease_lost during startup")
                    return
            except StorageUnavailableError:
                pass  # the lease TTL decides; a later renewal reports a loss

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
