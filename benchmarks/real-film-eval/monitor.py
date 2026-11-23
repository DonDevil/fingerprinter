"""Single shared system monitor + thermal guard for the real-film evaluation (FP-08).

One process, stdlib only, one monotonic+wall timestamp per sample, JSONL output. It also maintains
``thermal_state.json`` which the orchestrator consults (``wait_until_cool``) before starting GPU/CPU
heavy batches. It never changes any thermal/power setting; it only reads sensors.

Metrics per sample (every ``--interval`` s):
  cpu: system utilisation %, load, per-watched-process CPU % and RSS/RssAnon (children of --root-pid)
  mem: MemAvailable, Cached, SwapUsed (kept distinct from process RSS/RssAnon and GPU memory)
  gpu: util, mem used, temp, sm clock, power, throttle-reason bitmask (nvidia-smi)
  temps: k10temp Tctl, NVMe composite, GPU
  disk: per device read/write MB/s, IOPS, util %, avg await (ms) from /proc/diskstats
  fs: free bytes on watched mounts, size of the experiment dir
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import time
from pathlib import Path

# Conservative limits from the actual hardware (nvme max 83.85 C / crit 87.85 C; GPU target 87 C,
# slowdown 97 C; Ryzen 7 7435HS Tmax 95 C). pause = stop starting new work; resume = hysteresis.
LIMITS = {"cpu_tctl": (90.0, 80.0), "gpu": (83.0, 74.0), "nvme": (75.0, 65.0)}
SECTOR = 512


def read(p: str) -> str | None:
    try:
        return Path(p).read_text().strip()
    except OSError:
        return None


def hwmon_temp(name: str, label: str | None = None) -> float | None:
    for h in sorted(Path("/sys/class/hwmon").glob("hwmon*")):
        if read(f"{h}/name") != name:
            continue
        for t in sorted(h.glob("temp*_input")):
            if label is None or read(str(t).replace("_input", "_label")) == label:
                v = read(str(t))
                return int(v) / 1000 if v else None
    return None


def cpu_times() -> tuple[int, int]:
    f = [int(x) for x in open("/proc/stat").readline().split()[1:]]
    return sum(f), f[3] + f[4]


def diskstats(devs: list[str]) -> dict[str, tuple[int, ...]]:
    out = {}
    for line in open("/proc/diskstats"):
        p = line.split()
        if p[2] in devs:
            out[p[2]] = tuple(int(x) for x in p[3:14])
    return out


def descendants(root: int) -> list[int]:
    kids: dict[int, list[int]] = {}
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        try:
            ppid = int(open(f"/proc/{d}/stat").read().rsplit(")", 1)[1].split()[1])
        except (OSError, IndexError, ValueError):
            continue
        kids.setdefault(ppid, []).append(int(d))
    out, stack = [], [root]
    while stack:
        p = stack.pop()
        out.append(p)
        stack.extend(kids.get(p, []))
    return out


def proc_stat(pid: int) -> tuple[str, int, int, int] | None:
    try:
        s = open(f"/proc/{pid}/stat").read()
        comm = s[s.index("(") + 1:s.rindex(")")]
        f = s.rsplit(")", 1)[1].split()
        ticks = int(f[11]) + int(f[12])
        rss = anon = 0
        for line in open(f"/proc/{pid}/status"):
            if line.startswith("VmRSS"):
                rss = int(line.split()[1]) * 1024
            elif line.startswith("RssAnon"):
                anon = int(line.split()[1]) * 1024
        return comm, ticks, rss, anon
    except (OSError, ValueError, IndexError):
        return None


def gpu_sample() -> dict:
    try:
        o = subprocess.run(["nvidia-smi", "--query-gpu=utilization.gpu,memory.used,temperature.gpu,clocks.sm,power.draw,"
                            "clocks_throttle_reasons.active", "--format=csv,noheader,nounits"],
                           capture_output=True, text=True, timeout=5).stdout.strip().split(", ")
        return {"util": float(o[0]), "mem_mib": float(o[1]), "temp": float(o[2]), "sm_mhz": float(o[3]),
                "power_w": float(o[4]) if o[4] not in ("[N/A]", "") else None, "throttle": o[5]}
    except Exception:  # noqa: BLE001 - monitor must never die
        return {}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--state", required=True)
    ap.add_argument("--root-pid", type=int, default=0)
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--devs", default="sda,nvme0n1")
    ap.add_argument("--watch-dir", default=".")
    a = ap.parse_args()
    devs = a.devs.split(",")
    stop = False

    def _s(*_):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGTERM, _s)
    signal.signal(signal.SIGINT, _s)
    clk = os.sysconf("SC_CLK_TCK")
    prev_cpu, prev_disk, prev_t = cpu_times(), diskstats(devs), time.monotonic()
    prev_proc: dict[int, int] = {}
    hot_since: dict[str, bool] = {k: False for k in LIMITS}
    out = open(a.out, "a", buffering=1)
    n = 0
    while not stop:
        time.sleep(a.interval)
        now, wall = time.monotonic(), time.time()
        dt = now - prev_t
        c = cpu_times()
        cpu_pct = 100 * (1 - (c[1] - prev_cpu[1]) / max(c[0] - prev_cpu[0], 1))
        d = diskstats(devs)
        disk = {}
        for k, v in d.items():
            p = prev_disk.get(k, v)
            rd_io, rd_sec, rd_ms, wr_io, wr_sec, wr_ms, _, io_ms = v[0], v[2], v[3], v[4], v[6], v[7], v[8], v[9]
            disk[k] = {"read_mb_s": round((rd_sec - p[2]) * SECTOR / dt / 1e6, 2),
                       "write_mb_s": round((wr_sec - p[6]) * SECTOR / dt / 1e6, 2),
                       "read_iops": round((rd_io - p[0]) / dt, 1), "write_iops": round((wr_io - p[4]) / dt, 1),
                       "util_pct": round(min(100.0, (io_ms - p[9]) / dt / 10), 1),
                       "r_await_ms": round((rd_ms - p[3]) / max(rd_io - p[0], 1), 2),
                       "w_await_ms": round((wr_ms - p[7]) / max(wr_io - p[4], 1), 2)}
        mem = {l.split(":")[0]: int(l.split()[1]) * 1024 for l in open("/proc/meminfo")
               if l.split(":")[0] in ("MemTotal", "MemAvailable", "Cached", "SwapTotal", "SwapFree")}
        procs, tot_rss, tot_anon, nproc = [], 0, 0, 0
        cur_proc: dict[int, int] = {}
        if a.root_pid and os.path.exists(f"/proc/{a.root_pid}"):
            for pid in descendants(a.root_pid):
                s = proc_stat(pid)
                if not s:
                    continue
                nproc += 1
                cur_proc[pid] = s[1]
                tot_rss += s[2]
                tot_anon += s[3]
                pc = 100 * (s[1] - prev_proc.get(pid, s[1])) / clk / dt
                if pc >= 5 or s[2] > 500e6:
                    procs.append({"pid": pid, "comm": s[0], "cpu_pct": round(pc, 1), "rss_mb": round(s[2] / 1e6),
                                  "anon_mb": round(s[3] / 1e6)})
        g = gpu_sample()
        t = {"cpu_tctl": hwmon_temp("k10temp", "Tctl"), "nvme": hwmon_temp("nvme", "Composite"), "gpu": g.get("temp")}
        fs = {}
        for m in ("/", "/media/dhanush/Media"):
            try:
                st = os.statvfs(m)
                fs[m] = st.f_bavail * st.f_frsize
            except OSError:
                pass
        rec = {"t_wall": round(wall, 3), "t_mono": round(now, 3), "cpu_pct": round(cpu_pct, 1),
               "load1": os.getloadavg()[0], "mem": mem, "swap_used": mem.get("SwapTotal", 0) - mem.get("SwapFree", 0),
               "tree": {"nproc": nproc, "rss_mb": round(tot_rss / 1e6), "rss_anon_mb": round(tot_anon / 1e6), "top": procs[:6]},
               "gpu": g, "temps": t, "disk": disk, "fs_free": fs}
        out.write(json.dumps(rec, separators=(",", ":")) + "\n")
        # thermal guard with hysteresis
        state = {}
        for k, (hi, lo) in LIMITS.items():
            v = t.get(k)
            if v is None:
                continue
            if v >= hi:
                hot_since[k] = True
            elif v <= lo:
                hot_since[k] = False
            state[k] = {"temp": v, "hot": hot_since[k], "pause_at": hi, "resume_at": lo}
        thr = int(g.get("throttle", "0x0"), 16) if g.get("throttle", "").startswith("0x") else 0
        # 0x1 idle, 0x4 sw power cap (benign for a laptop GPU), 0x8 hw slowdown, 0x20 sw thermal, 0x40 hw thermal, 0x80 power brake
        state["gpu_thermal_throttle"] = bool(thr & (0x08 | 0x20 | 0x40 | 0x80))
        state["ok"] = not any(v["hot"] for k, v in state.items() if isinstance(v, dict)) and not state["gpu_thermal_throttle"]
        state["t_wall"] = wall
        tmp = a.state + ".tmp"
        Path(tmp).write_text(json.dumps(state))
        os.replace(tmp, a.state)
        prev_cpu, prev_disk, prev_t, prev_proc = c, d, now, cur_proc
        n += 1
    out.close()


if __name__ == "__main__":
    main()
