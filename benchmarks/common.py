"""Shared benchmark instrumentation: timing, resource sampling, environment
capture, latency statistics, and result persistence. Stdlib-only (plus
numpy, already a project dependency) — no psutil or other new dependency
was added to the project for this. See module docstring in
`benchmarks/__init__.py` for why this package exists outside production
code.

CPU/RSS are read directly from /proc (Linux-only, matches the "hardware"
section of the phase-11 doc: this project only runs on Linux dev/CI hosts
today). GPU is queried via `nvidia-smi` subprocess calls, which work
independent of whether torch can actually see CUDA — see the phase-11 doc
for why this development machine's GPU is unusable by torch despite being
physically present.
"""
from __future__ import annotations

import json
import os
import platform
import statistics
import subprocess
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence

RESULTS_DIR = Path(__file__).parent / "results"
_CLK_TCK = os.sysconf("SC_CLK_TCK")


# ---------------------------------------------------------------------------
# Timing
# ---------------------------------------------------------------------------


class Stopwatch:
    """`with Stopwatch() as sw: ...` then `sw.elapsed_s`. Uses
    `time.monotonic()` — never affected by wall-clock adjustments, which
    matters for anything timed across a lock-wait or subprocess call."""

    def __enter__(self) -> "Stopwatch":
        self._start = time.monotonic()
        self.elapsed_s: float = 0.0
        return self

    def __exit__(self, *exc) -> None:
        self.elapsed_s = time.monotonic() - self._start


# ---------------------------------------------------------------------------
# Latency statistics
# ---------------------------------------------------------------------------


@dataclass
class LatencyStats:
    count: int
    mean_s: Optional[float]
    median_s: Optional[float]
    p50_s: Optional[float]
    p95_s: Optional[float]
    p99_s: Optional[float]
    min_s: Optional[float]
    max_s: Optional[float]
    stdev_s: Optional[float]
    small_sample_warning: Optional[str] = None

    @staticmethod
    def from_samples(samples: Sequence[float]) -> "LatencyStats":
        n = len(samples)
        if n == 0:
            return LatencyStats(0, None, None, None, None, None, None, None, None, "no samples collected")
        ordered = sorted(samples)

        def pct(p: float) -> float:
            # Nearest-rank percentile — simple and deterministic; with the
            # small sample counts this benchmark suite uses, any percentile
            # method is approximate. See small_sample_warning below.
            k = max(0, min(n - 1, int(round(p * (n - 1)))))
            return ordered[k]

        warning = None
        if n < 10:
            warning = f"n={n} is too small for p95/p99 to be statistically meaningful; reported for completeness only"
        elif n < 30:
            warning = f"n={n} is small; p99 in particular should be treated as indicative, not precise"

        return LatencyStats(
            count=n,
            mean_s=statistics.fmean(samples),
            median_s=statistics.median(samples),
            p50_s=pct(0.50),
            p95_s=pct(0.95),
            p99_s=pct(0.99),
            min_s=ordered[0],
            max_s=ordered[-1],
            stdev_s=statistics.pstdev(samples) if n > 1 else 0.0,
            small_sample_warning=warning,
        )


# ---------------------------------------------------------------------------
# /proc-based CPU + RSS sampling
# ---------------------------------------------------------------------------


def _read_proc_stat_total_idle() -> Optional[tuple]:
    """Returns (total_ticks, idle_ticks) from the aggregate `cpu` line of
    /proc/stat, across all logical CPUs."""
    try:
        with open("/proc/stat") as f:
            fields = f.readline().split()
    except OSError:
        return None
    values = [int(x) for x in fields[1:]]
    total = sum(values)
    idle = values[3] + (values[4] if len(values) > 4 else 0)  # idle + iowait
    return total, idle


def _read_proc_pid_cpu_ticks(pid: int) -> Optional[int]:
    try:
        with open(f"/proc/{pid}/stat") as f:
            raw = f.read()
    except OSError:
        return None
    # comm field may contain spaces/parens; split after the last ')'
    tail = raw[raw.rfind(")") + 2 :].split()
    utime, stime = int(tail[11]), int(tail[12])
    return utime + stime


def _read_proc_pid_rss_kib(pid: int) -> Optional[int]:
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except OSError:
        return None
    return None


@dataclass
class ResourceSummary:
    sample_count: int
    duration_s: float
    process_cpu_percent_mean: Optional[float]
    process_cpu_percent_peak: Optional[float]
    system_cpu_percent_mean: Optional[float]
    system_cpu_percent_peak: Optional[float]
    process_rss_mib_mean: Optional[float]
    process_rss_mib_peak: Optional[float]
    gpu_util_percent_mean: Optional[float]
    gpu_util_percent_peak: Optional[float]
    gpu_vram_mib_mean: Optional[float]
    gpu_vram_mib_peak: Optional[float]
    note: Optional[str] = None


class ResourceSampler:
    """Background-thread sampler of process CPU%/RSS (summed across the
    given pids, which may span multiple worker processes) and system-wide
    CPU%, plus best-effort GPU utilization/VRAM via `nvidia-smi`.

    Not a profiler: this is coarse periodic sampling (default 0.25s), good
    enough to characterize sustained load during a benchmark run, not to
    attribute cost to a specific line of code.
    """

    def __init__(self, pids: Sequence[int], interval_s: float = 0.25, gpu: bool = True):
        self._pids = list(pids)
        self._interval_s = interval_s
        self._gpu = gpu and _nvidia_smi_available()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._samples: List[dict] = []
        self._start_time = 0.0

    def add_pid(self, pid: int) -> None:
        if pid not in self._pids:
            self._pids.append(pid)

    def start(self) -> None:
        self._start_time = time.monotonic()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> ResourceSummary:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        return self._summarize()

    def _run(self) -> None:
        prev_pid_ticks: Dict[int, int] = {}
        prev_total_idle = _read_proc_stat_total_idle()
        prev_wall = time.monotonic()
        while not self._stop.is_set():
            time.sleep(self._interval_s)
            now_wall = time.monotonic()
            wall_delta = now_wall - prev_wall

            proc_ticks_sum = 0
            rss_sum_kib = 0
            for pid in self._pids:
                ticks = _read_proc_pid_cpu_ticks(pid)
                rss = _read_proc_pid_rss_kib(pid)
                if ticks is not None:
                    prev = prev_pid_ticks.get(pid, ticks)
                    proc_ticks_sum += max(0, ticks - prev)
                    prev_pid_ticks[pid] = ticks
                if rss is not None:
                    rss_sum_kib += rss

            proc_cpu_pct = None
            if wall_delta > 0:
                proc_cpu_pct = (proc_ticks_sum / _CLK_TCK) / wall_delta * 100.0

            total_idle_now = _read_proc_stat_total_idle()
            sys_cpu_pct = None
            if total_idle_now is not None and prev_total_idle is not None:
                total_now, idle_now = total_idle_now
                total_prev, idle_prev = prev_total_idle
                total_delta = total_now - total_prev
                idle_delta = idle_now - idle_prev
                if total_delta > 0:
                    sys_cpu_pct = (1.0 - idle_delta / total_delta) * 100.0
            prev_total_idle = total_idle_now
            prev_wall = now_wall

            gpu_util, gpu_vram = (None, None)
            if self._gpu:
                gpu_util, gpu_vram = _query_nvidia_smi()

            self._samples.append(
                {
                    "t": now_wall - self._start_time,
                    "process_cpu_percent": proc_cpu_pct,
                    "system_cpu_percent": sys_cpu_pct,
                    "process_rss_mib": rss_sum_kib / 1024.0 if rss_sum_kib else None,
                    "gpu_util_percent": gpu_util,
                    "gpu_vram_mib": gpu_vram,
                }
            )

    def _summarize(self) -> ResourceSummary:
        if not self._samples:
            return ResourceSummary(0, 0.0, None, None, None, None, None, None, None, None, None, None, "no samples")

        cpu = [s["process_cpu_percent"] for s in self._samples if s["process_cpu_percent"] is not None]
        syscpu = [s["system_cpu_percent"] for s in self._samples if s["system_cpu_percent"] is not None]
        rss = [s["process_rss_mib"] for s in self._samples if s["process_rss_mib"] is not None]
        gu = [s["gpu_util_percent"] for s in self._samples if s["gpu_util_percent"] is not None]
        gv = [s["gpu_vram_mib"] for s in self._samples if s["gpu_vram_mib"] is not None]

        return ResourceSummary(
            sample_count=len(self._samples),
            duration_s=self._samples[-1]["t"] if self._samples else 0.0,
            process_cpu_percent_mean=statistics.fmean(cpu) if cpu else None,
            process_cpu_percent_peak=max(cpu) if cpu else None,
            system_cpu_percent_mean=statistics.fmean(syscpu) if syscpu else None,
            system_cpu_percent_peak=max(syscpu) if syscpu else None,
            process_rss_mib_mean=statistics.fmean(rss) if rss else None,
            process_rss_mib_peak=max(rss) if rss else None,
            gpu_util_percent_mean=statistics.fmean(gu) if gu else None,
            gpu_util_percent_peak=max(gu) if gu else None,
            gpu_vram_mib_mean=statistics.fmean(gv) if gv else None,
            gpu_vram_mib_peak=max(gv) if gv else None,
            note=None if self._gpu else "nvidia-smi unavailable or GPU sampling disabled",
        )


_nvidia_smi_checked = False
_nvidia_smi_ok = False


def _nvidia_smi_available() -> bool:
    global _nvidia_smi_checked, _nvidia_smi_ok
    if not _nvidia_smi_checked:
        _nvidia_smi_checked = True
        try:
            subprocess.run(["nvidia-smi", "-L"], capture_output=True, timeout=3, check=True)
            _nvidia_smi_ok = True
        except Exception:
            _nvidia_smi_ok = False
    return _nvidia_smi_ok


def _query_nvidia_smi() -> tuple:
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=utilization.gpu,memory.used", "--format=csv,noheader,nounits"],
            capture_output=True,
            timeout=3,
            text=True,
            check=True,
        )
        util_str, mem_str = out.stdout.strip().split(",")
        return float(util_str.strip()), float(mem_str.strip())
    except Exception:
        return None, None


def proc_peak_rss_mib(pid: Optional[int] = None) -> Optional[float]:
    """One-shot peak RSS read (VmHWM, the kernel-tracked high-water mark) —
    cheaper than sampling when only the final peak is needed."""
    pid = pid or os.getpid()
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        return None
    return None


# ---------------------------------------------------------------------------
# Environment capture
# ---------------------------------------------------------------------------


def _cmd(args: List[str]) -> Optional[str]:
    try:
        out = subprocess.run(args, capture_output=True, timeout=5, text=True, check=True)
        return out.stdout.strip()
    except Exception:
        return None


def git_revision() -> Optional[str]:
    return _cmd(["git", "rev-parse", "HEAD"])


def git_dirty() -> Optional[bool]:
    out = _cmd(["git", "status", "--porcelain"])
    return None if out is None else bool(out)


def cpu_model() -> Optional[str]:
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        return None
    return None


def total_ram_mib() -> Optional[float]:
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        return None
    return None


def available_ram_mib() -> Optional[float]:
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        return None
    return None


def gpu_name() -> Optional[str]:
    return _cmd(["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"])


def redis_server_info(redis_client) -> Dict[str, object]:
    try:
        info = redis_client.info()
        return {
            "redis_version": info.get("redis_version"),
            "connected_clients": info.get("connected_clients"),
            "used_memory_human": info.get("used_memory_human"),
            "used_memory": info.get("used_memory"),
            "total_commands_processed": info.get("total_commands_processed"),
            "instantaneous_ops_per_sec": info.get("instantaneous_ops_per_sec"),
            "uptime_in_seconds": info.get("uptime_in_seconds"),
        }
    except Exception as exc:
        return {"error": str(exc)}


def redis_info_delta(before: Dict[str, object], after: Dict[str, object]) -> Dict[str, object]:
    delta = {}
    for key in ("total_commands_processed",):
        b, a = before.get(key), after.get(key)
        if isinstance(b, (int, float)) and isinstance(a, (int, float)):
            delta[f"{key}_delta"] = a - b
    for key in ("used_memory",):
        b, a = before.get(key), after.get(key)
        if isinstance(b, (int, float)) and isinstance(a, (int, float)):
            delta[f"{key}_delta_bytes"] = a - b
    return delta


def software_versions() -> Dict[str, Optional[str]]:
    versions: Dict[str, Optional[str]] = {
        "python": platform.python_version(),
    }
    try:
        import torch

        versions["torch"] = torch.__version__
        versions["torch_cuda_available"] = str(torch.cuda.is_available())
    except Exception as exc:
        versions["torch"] = f"import failed: {exc}"

    try:
        import transformers

        versions["transformers"] = transformers.__version__
    except Exception as exc:
        versions["transformers"] = f"import failed: {exc}"

    try:
        import redis as redis_pkg

        versions["redis_py"] = redis_pkg.__version__
    except Exception as exc:
        versions["redis_py"] = f"import failed: {exc}"

    versions["ffmpeg"] = (_cmd(["ffmpeg", "-version"]) or "").splitlines()[0] if _cmd(["ffmpeg", "-version"]) else None
    return versions


def environment_snapshot() -> Dict[str, object]:
    return {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "git_revision": git_revision(),
        "git_dirty": git_dirty(),
        "hardware": {
            "cpu_model": cpu_model(),
            "logical_cpus": os.cpu_count(),
            "total_ram_mib": total_ram_mib(),
            "gpu": gpu_name(),
        },
        "os": {
            "system": platform.system(),
            "release": platform.release(),
            "platform": platform.platform(),
        },
        "software": software_versions(),
    }


# ---------------------------------------------------------------------------
# Result persistence
# ---------------------------------------------------------------------------


def _to_jsonable(obj):
    if hasattr(obj, "__dict__") and hasattr(obj, "__dataclass_fields__"):
        return asdict(obj)
    if isinstance(obj, Path):
        return str(obj)
    raise TypeError(f"not JSON-serializable: {type(obj)}")


def save_result(workload: str, payload: Dict[str, object], results_dir: Path = RESULTS_DIR) -> Path:
    """Writes `payload` as pretty JSON to a uniquely-named file — never
    overwrites a previous run's result."""
    results_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%S")
    short_id = uuid.uuid4().hex[:8]
    path = results_dir / f"{workload}_{stamp}_{short_id}.json"
    with open(path, "w") as f:
        json.dump(payload, f, indent=2, default=_to_jsonable, sort_keys=False)
    return path
