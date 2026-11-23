"""Embedding validation: NaN/inf/dim/norm for every artifact; CPU vs GPU agreement; software vs NVDEC decode equality."""
from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

import numpy as np

from common import d, jdump, jload, mark_done
from encode import test_spec, validate


def run(emb) -> None:
    res = {"artifacts": {}}
    for p in sorted((d("embeddings") ).rglob("*.npz")):
        z = np.load(p)
        res["artifacts"][str(p.relative_to(d("embeddings")))] = validate(z["vec"])
    bad = {k: v for k, v in res["artifacts"].items() if v["nan"] or v["inf"] or not v["dim_ok"] or not v["norm_ok"]}
    res["n_artifacts"], res["n_bad"], res["bad"] = len(res["artifacts"]), len(bad), bad
    # CPU vs GPU on a representative sample: 32 frames from 4 films
    from fingerprinter.media import decode, preprocess
    from fingerprinter.model import Embedder
    paths = jload(d("manifests", "filename_map.LOCAL.json"))
    cpu = Embedder("cpu", "float32", threads=4)
    xs = []
    import encode as E
    for mid in ("movie_002", "movie_005", "movie_007", "movie_010"):
        E.WINDOW = (900, 20)
        E.use_gpu_decode()
        for i, f in enumerate(decode(Path(paths[mid]), test_spec(2.5))):
            xs.append(preprocess(f.rgb))
            if i >= 7:
                break
    x = np.stack(xs)
    g = np.concatenate([emb.embed(x[i:i + 8]) for i in range(0, len(x), 8)]); c = np.concatenate([cpu.embed(x[i:i + 4]) for i in range(0, len(x), 4)])
    cos = (g * c).sum(1)
    res["cpu_gpu"] = {"n_frames": len(x), "cos_min": float(cos.min()), "cos_mean": float(cos.mean()), "max_abs_diff": float(np.abs(g - c).max())}
    # determinism: the same batch twice on the GPU
    g2 = np.concatenate([emb.embed(x[i:i + 8]) for i in range(0, len(x), 8)])
    res["gpu_repeat_max_abs_diff"] = float(np.abs(g - g2).max())
    # software vs NVDEC decode, 30 s of two films, md5 of the production-filtered raw frames
    from fingerprinter.media import decode_command
    import fingerprinter.media as M
    dec = {}
    for mid in ("movie_003", "movie_008"):
        orig = getattr(M.decode_command, "__wrapped_orig__", None)
        base = [] 
        E.WINDOW = None
        spec = test_spec(0.1)
        from fingerprinter.media import decode_command as dc
        cmd = dc(Path(paths[mid]), spec)       # NVDEC wrapped command
        i = cmd.index("-i"); sw = [a for j, a in enumerate(cmd) if not (cmd[j] == "-hwaccel" or (j and cmd[j - 1] == "-hwaccel"))]
        hs = []
        for c_ in (cmd, sw):
            i = c_.index("-i")
            r = subprocess.run(c_[:i] + ["-ss", "1500", "-t", "30"] + c_[i:], capture_output=True)
            hs.append((hashlib.md5(r.stdout).hexdigest(), len(r.stdout)))
        dec[mid] = {"nvdec": hs[0], "software": hs[1], "identical": hs[0] == hs[1]}
    res["decode_equality"] = dec
    jdump(res, d("metrics", "validation.json"))
    mark_done("validate", "v1", {"bad": len(bad)})
    print({k: res[k] for k in ("n_artifacts", "n_bad", "cpu_gpu", "gpu_repeat_max_abs_diff", "decode_equality")})
