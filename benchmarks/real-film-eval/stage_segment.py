"""Scene/segment detection from the production 1 fps embeddings (no extra decode pass).

A boundary is a drop of the cosine between consecutive 1 s samples below ``SCENE_COS``; segments are then merged up to
``MIN_S`` and split down to ``MAX_S`` (at the weakest consecutive similarity). Timestamps are source seconds."""
from __future__ import annotations

import numpy as np

from common import cfg_fp, d, jdump, jload, mark_done, stage_done

SCENE_COS, MIN_S, MAX_S = 0.60, 30, 120


def segment_film(pts: np.ndarray, vec: np.ndarray) -> list[tuple[float, float]]:
    n = len(pts)
    if n < 2:
        return []
    cs = (vec[1:] * vec[:-1]).sum(1)
    cuts = [0] + [i + 1 for i in np.where(cs < SCENE_COS)[0]] + [n]
    segs = [[cuts[i], cuts[i + 1]] for i in range(len(cuts) - 1)]
    merged: list[list[int]] = []
    for s in segs:  # merge short fragments into the previous one (or next at the start)
        if merged and (s[1] - s[0] < MIN_S or merged[-1][1] - merged[-1][0] < MIN_S):
            merged[-1][1] = s[1]
        else:
            merged.append(s)
    if len(merged) > 1 and merged[-1][1] - merged[-1][0] < MIN_S:
        merged[-2][1] = merged[-1][1]
        merged.pop()
    out: list[tuple[int, int]] = []
    def split(a: int, b: int) -> None:
        if b - a <= MAX_S:
            out.append((a, b)); return
        lo, hi = a + MIN_S, min(b - MIN_S, a + MAX_S)
        j = lo + int(np.argmin(cs[lo - 1:hi - 1])) if hi > lo else (a + b) // 2
        split(a, j); split(j, b)
    for a, b in merged:
        split(a, b)
    return [(float(pts[a]), float(pts[b - 1]) + 1.0) for a, b in out]


def run(emb=None) -> None:
    movies = jload(d("manifests", "movies.json"))
    fp = cfg_fp({"v": 1, "cos": SCENE_COS, "min": MIN_S, "max": MAX_S, "m": [m["sha256"] for m in movies]})
    if stage_done("segment", fp):
        return
    rows = []
    for m in movies:
        z = np.load(d("embeddings", "full", f"{m['movie_id']}.npz"))
        order = np.argsort(z["pts"]); pts, vec = z["pts"][order], z["vec"][order]
        for i, (a, b) in enumerate(segment_film(pts, vec)):
            rows.append({"movie_id": m["movie_id"], "segment_id": f"{m['movie_id']}_s{i:04d}", "start_s": round(a, 3),
                         "end_s": round(b, 3), "duration_s": round(b - a, 3), "source_sha256": m["sha256"],
                         "config": {"method": "emb-cosine-1fps", "scene_cos": SCENE_COS, "min_s": MIN_S, "max_s": MAX_S},
                         "segment_file": None})  # timestamp-based; no temporary segment file is written
    jdump(rows, d("manifests", "segments.json"))
    dur = np.array([r["duration_s"] for r in rows])
    mark_done("segment", fp, {"n": len(rows), "dur_pcts": np.percentile(dur, [0, 10, 50, 90, 100]).tolist()})
    print(len(rows), "segments; duration pcts", np.percentile(dur, [0, 10, 50, 90, 100]))
