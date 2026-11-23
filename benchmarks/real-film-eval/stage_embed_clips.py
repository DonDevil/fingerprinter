"""Embed every transformed clip at the 0.1 s grid (5 fps + 2 fps frames, nested 1 fps / 0.2 fps), and the original excerpt windows."""
from __future__ import annotations

import time
from pathlib import Path

import numpy as np

from common import DATA, cfg_fp, d, jdump, jload, mark_done, stage_done
from encode import encode_video, validate

RATES = ["5fps", "2fps"]


def run(emb) -> None:
    clips, exs = jload(d("manifests", "clips.json")), jload(d("manifests", "excerpts.json"))
    paths = jload(d("manifests", "filename_map.LOCAL.json"))
    fp = cfg_fp({"v": 1, "rates": RATES, "n": len(clips), "e": len(exs)})
    if stage_done("embed_clips", fp):
        return
    jobs = [("win", e["excerpt_id"], Path(paths[e["movie_id"]]), (e["start_s"], e["duration_s"])) for e in exs]
    jobs += [("clip", c["clip_id"], DATA / c["path"], None) for c in clips if c["result"].get("ok") or c["result"].get("cached")]
    for kind, name, path, win in jobs:
        out = d("embeddings", "clips" if kind == "clip" else "windows", f"{name}.npz")
        if out.exists():
            continue
        r = encode_video(path, emb, RATES, window=win)
        np.savez(out, k=r["k"], pts=r["pts"], vec=r["vec"])
        jdump({**r["stats"], "name": name, "kind": kind, "validation": validate(r["vec"])}, d("metrics", "embed_clips", f"{name}.json"))
    mark_done("embed_clips", fp, {"jobs": len(jobs)})
