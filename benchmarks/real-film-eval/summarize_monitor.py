"""FP-08: summarise system.jsonl (one shared monitor) and per-stage encoder stats into metrics/monitor_summary.json."""
import glob
import json

import numpy as np

from common import d, jdump

rows = [json.loads(l) for l in open(d("metrics", "system.jsonl"))]
def col(f):
    return np.array([x for x in (f(r) for r in rows) if x is not None], float)
g = lambda k: col(lambda r: r["gpu"].get(k))
t = lambda k: col(lambda r: r["temps"].get(k))
thr = [r["gpu"].get("throttle") for r in rows if r["gpu"]]
from collections import Counter
s = {"samples": len(rows), "interval_s": 2, "span_h": round((rows[-1]["t_wall"] - rows[0]["t_wall"]) / 3600, 2),
     "cpu_tctl_c": {"max": t("cpu_tctl").max(), "p95": np.percentile(t("cpu_tctl"), 95), "frac_ge_90": float((t("cpu_tctl") >= 90).mean())},
     "gpu_temp_c": {"max": t("gpu").max(), "p95": np.percentile(t("gpu"), 95)}, "nvme_temp_c_max": t("nvme").max(),
     "gpu_util_pct_mean_when_active": float(g("util")[g("util") > 20].mean()), "gpu_mem_mib_max": g("mem_mib").max(),
     "gpu_power_w_max": g("power_w").max(), "gpu_sm_mhz_max": g("sm_mhz").max(), "gpu_throttle_reason_counts": dict(Counter(thr)),
     "cpu_pct_system_max": col(lambda r: r["cpu_pct"]).max(), "swap_used_mb_max": col(lambda r: r["swap_used"]).max() / 1e6,
     "tree_rss_mb_max": col(lambda r: r["tree"]["rss_mb"]).max(), "tree_rss_anon_mb_max": col(lambda r: r["tree"]["rss_anon_mb"]).max(),
     "mem_available_gb_min": col(lambda r: r["mem"]["MemAvailable"]).min() / 1e9,
     "sda_read_mb_s": {"max": col(lambda r: r["disk"]["sda"]["read_mb_s"]).max(), "mean_when_active": float(col(lambda r: r["disk"]["sda"]["read_mb_s"])[col(lambda r: r["disk"]["sda"]["read_mb_s"]) > 1].mean())},
     "sda_util_pct_max": col(lambda r: r["disk"]["sda"]["util_pct"]).max(),
     "nvme_write_mb_s_max": col(lambda r: r["disk"]["nvme0n1"]["write_mb_s"]).max(),
     "sda_r_await_ms_max": col(lambda r: r["disk"]["sda"]["r_await_ms"]).max(),
     "root_free_gb_min": col(lambda r: r["fs_free"].get("/")).min() / 1e9}
enc = {"full": [json.load(open(f)) for f in sorted(glob.glob(str(d("metrics", "embed_full") / "*.json")))],
       "clips": [json.load(open(f)) for f in glob.glob(str(d("metrics", "embed_clips") / "*.json"))]}
def agg(L):
    return {"units": len(L), "frames_embedded": sum(x["embedded"] for x in L), "failed_frames": sum(x["failed_frames"] for x in L),
            "decode_errors": sum(1 for x in L if x["error"]), "gpu_inference_s": round(sum(x["gpu_inference_s"] for x in L), 1),
            "preprocess_s": round(sum(x["preprocess_s"] for x in L), 1), "decode_wait_s": round(sum(x["decode_wait_s"] for x in L), 1),
            "thermal_wait_s": round(sum(x["thermal_wait_s"] + x.get("thermal_wait_decode_s", 0) for x in L), 1),
            "wall_s": round(sum(x["wall_s"] for x in L), 1), "gpu_only_fps": round(sum(x["embedded"] for x in L) / max(sum(x["gpu_inference_s"] for x in L), 1e-9), 1),
            "end_to_end_fps": round(sum(x["embedded"] for x in L) / max(sum(x["wall_s"] for x in L), 1e-9), 1),
            "vram_max_allocated_mb": round(max(x["vram"]["max_allocated"] for x in L) / 1e6), "vram_max_reserved_mb": round(max(x["vram"]["max_reserved"] for x in L) / 1e6)}
s["encoder"] = {k: agg(v) for k, v in enc.items()}
jdump(s, d("metrics", "monitor_summary.json"))
print(json.dumps(s, indent=1, default=float))
