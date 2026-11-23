"""Controlled positives: transformed variants of each excerpt, re-encoded on the GPU (NVDEC decode + NVENC encode).
Every clip is traceable: clip_id -> excerpt_id -> (movie_id, start_s). Clips are regenerable scratch, never committed."""
from __future__ import annotations

import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

from common import cfg_fp, d, jdump, jload, mark_done, stage_done, wait_until_cool, SRC_DIR  # noqa: F401

N = "h264_nvenc"
TRANSFORMS = {
    "t00_reencode":  {"vf": "null", "enc": [N, "-cq", "19"]},
    "t01_res480":    {"vf": "scale=-2:480", "enc": [N, "-cq", "26"]},
    "t02_lowbitrate": {"vf": "scale=-2:360", "enc": [N, "-b:v", "250k", "-maxrate", "300k", "-bufsize", "600k"]},
    "t03_hevc":      {"vf": "scale=-2:576", "enc": ["hevc_nvenc", "-cq", "32"]},
    "t04_crop":      {"vf": "crop=iw*0.85:ih*0.85", "enc": [N, "-cq", "23"]},
    "t05_aspect43":  {"vf": "scale=iw*0.75:ih,setsar=1", "enc": [N, "-cq", "23"]},
    "t06_brightness": {"vf": "eq=brightness=0.12:contrast=1.25", "enc": [N, "-cq", "23"]},
    "t07_color":     {"vf": "eq=saturation=0.5,hue=h=20", "enc": [N, "-cq", "23"]},
    "t08_overlay":   {"vf": "drawbox=x=iw*0.78:y=ih*0.05:w=iw*0.18:h=ih*0.1:color=white@0.6:t=fill,drawbox=x=0:y=ih*0.9:w=iw:h=ih*0.1:color=black@0.7:t=fill", "enc": [N, "-cq", "23"]},
    "t09_fps15":     {"vf": "fps=15", "enc": [N, "-cq", "23"]},
    "t10_mirror":    {"vf": "hflip", "enc": [N, "-cq", "23"]},
    "t11_combo":     {"vf": "scale=-2:480,crop=iw*0.9:ih*0.9,eq=brightness=0.08:contrast=1.15", "enc": [N, "-b:v", "400k", "-maxrate", "500k", "-bufsize", "1000k"]},
}


def make_clip(src: str, ex: dict, name: str, out) -> dict:
    if out.exists() and out.stat().st_size > 1000:
        return {"cached": True}
    t = time.time()
    wait_until_cool()
    cmd = ["ffmpeg", "-nostdin", "-loglevel", "error", "-y", "-hwaccel", "cuda", "-ss", str(ex["start_s"]), "-t", str(ex["duration_s"]),
           "-i", src, "-an", "-sn", "-vf", TRANSFORMS[name]["vf"], "-c:v", *TRANSFORMS[name]["enc"], str(out)]
    p = subprocess.run(cmd, capture_output=True, text=True)
    return {"ok": p.returncode == 0 and out.exists(), "err": p.stderr[-300:], "seconds": round(time.time() - t, 2)}


def run(emb=None) -> None:
    exs = jload(d("manifests", "excerpts.json"))
    paths = jload(d("manifests", "filename_map.LOCAL.json"))
    fp = cfg_fp({"v": 1, "t": TRANSFORMS, "e": [e["excerpt_id"] for e in exs]})
    if stage_done("clips", fp):
        return
    jobs = [(e, t) for e in exs for t in TRANSFORMS]
    # 2 concurrent NVENC sessions; sources are read from the HDD, but each excerpt is a short contiguous read
    with ThreadPoolExecutor(2) as pool:
        res = list(pool.map(lambda j: make_clip(paths[j[0]["movie_id"]], j[0], j[1], d("segments", "clips", f"{j[0]['excerpt_id']}__{j[1]}.mp4")), jobs))
    man = [{"clip_id": f"{e['excerpt_id']}__{t}", "excerpt_id": e["excerpt_id"], "movie_id": e["movie_id"], "transform": t,
            "vf": TRANSFORMS[t]["vf"], "encoder": TRANSFORMS[t]["enc"], "path": f"segments/clips/{e['excerpt_id']}__{t}.mp4",
            "result": r, "label": "same_underlying_segment_transformed"} for (e, t), r in zip(jobs, res)]
    jdump(man, d("manifests", "clips.json"))
    bad = [m for m in man if not (m["result"].get("ok") or m["result"].get("cached"))]
    mark_done("clips", fp, {"n": len(man), "failed": len(bad)})
    print(len(man), "clips,", len(bad), "failed")
