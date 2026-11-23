"""FP-04: mine cross-film hard-negative CANDIDATES. High similarity is only a candidate; labels come from review."""
from __future__ import annotations

import json
import subprocess

import numpy as np
import torch

from common import SRC_DIR, cfg_fp, d, jdump, jload, mark_done, stage_done  # noqa: F401
from fingerprinter.verify import VerifierParams, best_score, verify
from rfdata import load, params_for

TOP_PAIRS, MAX_PER_SEG = 240, 2


def run(emb=None) -> None:
    movies = jload(d("manifests", "movies.json")); segs = jload(d("manifests", "segments.json"))
    fp = cfg_fp({"v": 1, "top": TOP_PAIRS, "mps": MAX_PER_SEG, "s": len(segs)})
    if stage_done("hardneg", fp):
        return
    vecs, pts, mov = [], [], []
    for i, m in enumerate(movies):
        z = np.load(d("embeddings", "full", f"{m['movie_id']}.npz")); o = np.argsort(z["pts"])
        vecs.append(z["vec"][o]); pts.append(z["pts"][o]); mov.append(np.full(len(o), i))
    V = torch.from_numpy(np.concatenate(vecs)).cuda(); P = np.concatenate(pts); M = np.concatenate(mov)
    sid = np.full(len(P), -1, np.int64)
    for j, s in enumerate(segs):
        mi = int(s["movie_id"].split("_")[1]) - 1
        sid[(M == mi) & (P >= s["start_s"]) & (P < s["end_s"])] = j
    ok = sid >= 0
    S = torch.from_numpy(sid).cuda(); nS = len(segs)
    sym = np.zeros((nS, nS), np.float32)
    for j, s in enumerate(segs):
        idx = np.where(sid == j)[0]
        if len(idx) == 0:
            continue
        sims = V[idx] @ V.T  # (a, N)
        red = torch.full((len(idx), nS), -1.0, device="cuda").scatter_reduce(1, S.clamp(min=0).expand(len(idx), -1), sims.masked_fill((S < 0)[None, :], -1.0), "amax")
        sym[j] = red.mean(0).cpu().numpy()  # A->B chamfer-mean
    sym = np.minimum(sym, sym.T)  # require similarity in both directions
    seg_movie = np.array([int(s["movie_id"].split("_")[1]) for s in segs])
    sym[seg_movie[:, None] == seg_movie[None, :]] = -1
    iu = np.dstack(np.unravel_index(np.argsort(-np.triu(sym).ravel()), sym.shape))[0]
    chosen, used = [], {}
    for a, b in iu:
        if a >= b:
            continue
        if used.get(a, 0) >= MAX_PER_SEG or used.get(b, 0) >= MAX_PER_SEG:
            continue
        chosen.append((int(a), int(b), float(sym[a, b]))); used[a] = used.get(a, 0) + 1; used[b] = used.get(b, 0) + 1
        if len(chosen) >= TOP_PAIRS:
            break
    rows = []
    sv = {}
    def seg_vec(j, rate):
        if (j, rate) not in sv:
            s = segs[j]; z = np.load(d("embeddings", "full", f"{s['movie_id']}.npz")); o = np.argsort(z["pts"])
            pt, v = z["pts"][o], z["vec"][o]
            sel = (pt >= s["start_s"]) & (pt < s["end_s"])
            from fingerprinter.verify import Vectors
            m = 1 if rate == "1fps" else 5
            kk = np.arange(len(pt))[sel]; kk = kk[(z["k"][o][sel] % m) == 0]
            vv = v[kk]; tt = pt[kk]
            sv[(j, rate)] = Vectors.from_arrays({"segment_index": np.arange(len(kk)), "pts_s": (tt - tt[0]) if len(tt) else tt, "vectors": vv})
        return sv[(j, rate)]
    for n, (a, b, sc) in enumerate(chosen):
        r = {"pair_id": f"hn_{n:04d}", "movie_a": segs[a]["movie_id"], "segment_a": segs[a]["segment_id"], "start_a": segs[a]["start_s"],
             "end_a": segs[a]["end_s"], "movie_b": segs[b]["movie_id"], "segment_b": segs[b]["segment_id"], "start_b": segs[b]["start_s"],
             "end_b": segs[b]["end_s"], "embedding_similarity_chamfer_1fps": round(sc, 4), "sampling_config": {"embedding": "dinov2-base fp32 1fps"}}
        for rate in ("0.2fps", "1fps"):
            p, _, info = params_for(rate)
            va, vb = seg_vec(a, rate), seg_vec(b, rate)
            if len(va) == 0 or len(vb) == 0:
                r[f"matcher_{rate}"] = None; continue
            res = verify(va, vb, p)
            sc2 = best_score(va, vb, p)
            r[f"matcher_{rate}"] = {"verdict": res.verdict, "score": res.score, "best_score_ignoring_L_tau": sc2[0], "best_run_len": sc2[1],
                                    "registered_params": info["registered_parameters"]}
        r.update(proposed_label="hard_negative_candidate", verification_status="unreviewed",
                 reason="different films by construction; content not yet visually verified")
        rows.append(r)
    jdump(rows, d("pairs", "hard-negatives", "candidates.json"))
    mark_done("hardneg", fp, {"n": len(rows)})
    print(len(rows), "candidates; matcher confirmed @1fps:", sum(1 for r in rows if (r.get("matcher_1fps") or {}).get("verdict") == "confirmed"))
