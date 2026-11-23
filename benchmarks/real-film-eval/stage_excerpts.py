"""Pick evaluation excerpts: 4 per film, 60 s, integer start, inside a detected scene segment, non-degenerate, >=120 s apart."""
from __future__ import annotations

import numpy as np

from common import cfg_fp, d, jdump, jload, mark_done, stage_done

N_PER_FILM, DUR, QUANTILES = 4, 60, (0.15, 0.35, 0.55, 0.75)


def run(emb=None) -> None:
    movies = {m["movie_id"]: m for m in jload(d("manifests", "movies.json"))}
    segs = jload(d("manifests", "segments.json"))
    fp = cfg_fp({"v": 1, "n": N_PER_FILM, "dur": DUR, "q": QUANTILES, "s": len(segs)})
    if stage_done("excerpts", fp):
        return
    ex = []
    for mid, m in movies.items():
        z = np.load(d("embeddings", "full", f"{mid}.npz")); o = np.argsort(z["pts"]); pts, vec = z["pts"][o], z["vec"][o]
        cand = [s for s in segs if s["movie_id"] == mid and s["duration_s"] >= DUR]
        for q in QUANTILES:
            target_t = q * m["duration_s"]
            for s in sorted(cand, key=lambda s: abs((s["start_s"] + s["end_s"]) / 2 - target_t)):
                start = int(np.ceil(s["start_s"] + (s["duration_s"] - DUR) / 2))
                sel = (pts >= start) & (pts < start + DUR)
                if sel.sum() < DUR * 0.9:
                    continue
                v = vec[sel]
                if float((v @ v.T).mean()) > 0.93:  # near-static / low-information excerpt
                    continue
                if any(e["movie_id"] == mid and abs(e["start_s"] - start) < 2 * DUR for e in ex):
                    continue
                ex.append({"excerpt_id": f"{mid}_x{len([e for e in ex if e['movie_id'] == mid])}", "movie_id": mid,
                           "segment_id": s["segment_id"], "start_s": start, "duration_s": DUR, "source_sha256": m["sha256"],
                           "self_similarity_mean": round(float((v @ v.T).mean()), 4)})
                break
    jdump(ex, d("manifests", "excerpts.json"))
    mark_done("excerpts", fp, {"n": len(ex)})
    print(len(ex), "excerpts")
